//! Phase 0 baseline benchmarks.
//!
//! Bench profile is intentionally set to opt-level=1 in qsql-workspace/Cargo.toml so
//! that `cargo bench --no-run` compiles in a few minutes (vs >10 min at release-grade
//! optimization). Absolute numbers under this profile are NOT comparable to
//! release-profile runs; they exist to catch compile-time regressions and large
//! relative shifts. Phase 0 benches are non-gating per implementation_plan.md.
//!
//! Engine and Runtime are constructed once per bench group via OnceCell/Lazy to keep
//! the timed body focused on the operation under test rather than SessionContext +
//! federation-planner setup cost.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use once_cell::sync::Lazy;
use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::broadcast::BroadcastRewriteConfig;
use qsql_core::engine::{ExecutePageOptions, QsqlEngine};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio runtime"));

fn repo_root() -> PathBuf {
    let mut starts = vec![std::env::current_dir().expect("current_dir")];
    starts.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    for start in starts {
        for candidate in start.ancestors() {
            if candidate.join("samples").join("quickstart").exists() {
                return candidate.to_path_buf();
            }
        }
    }

    panic!("failed to resolve repository root");
}

fn sample_path(file_name: &str) -> String {
    repo_root()
        .join("samples")
        .join("quickstart")
        .join(file_name)
        .to_string_lossy()
        .into_owned()
}

fn benchmark_file_scans(c: &mut Criterion) {
    let csv_engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        engine
            .register_file("employees", &sample_path("employees.csv"), "csv")
            .await
            .expect("register csv");
        engine
    });

    let mut group = c.benchmark_group("csv_file_scan_to_json");
    group.throughput(Throughput::Elements(5));
    group.bench_function("ordered_limit_5", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = csv_engine
                    .execute_sql_to_json(
                        "SELECT id, name, salary FROM employees ORDER BY id LIMIT 5",
                    )
                    .await
                    .expect("query csv");
                black_box(result);
            });
        });
    });
    group.finish();

    let parquet_engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        engine
            .register_file("orders", &sample_path("orders.parquet"), "parquet")
            .await
            .expect("register parquet");
        engine
    });

    let mut group = c.benchmark_group("parquet_file_scan_to_json");
    group.throughput(Throughput::Elements(5));
    group.bench_function("ordered_limit_5", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = parquet_engine
                    .execute_sql_to_json(
                        "SELECT order_id, employee_id, amount FROM orders ORDER BY order_id LIMIT 5",
                    )
                    .await
                    .expect("query parquet");
                black_box(result);
            });
        });
    });
    group.finish();

    // Phase 8 — fixed-width file scan benchmark. Uses the same quickstart
    // fixture (`employees_fwf.txt` + `employees_fwf.layout.json`) so all
    // file-format benchmarks read 6 rows of equivalent data.
    let fwf_engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        let mut opts = std::collections::HashMap::new();
        opts.insert(
            "layout_path".to_string(),
            serde_json::Value::String(sample_path("employees_fwf.layout.json")),
        );
        engine
            .register_file_with_options(
                "employees_fwf",
                &sample_path("employees_fwf.txt"),
                "fixed_width",
                Some(&opts),
            )
            .await
            .expect("register fixed_width");
        engine
    });

    let mut group = c.benchmark_group("fixed_width_file_scan_to_json");
    group.throughput(Throughput::Elements(5));
    group.bench_function("ordered_limit_5", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = fwf_engine
                    .execute_sql_to_json(
                        "SELECT id, name, salary FROM employees_fwf ORDER BY id LIMIT 5",
                    )
                    .await
                    .expect("query fixed_width");
                black_box(result);
            });
        });
    });
    group.finish();
}

fn benchmark_sqlite_scan(c: &mut Criterion) {
    let engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        let provider = SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
            .await
            .expect("sqlite provider");
        engine
            .register_table("compensation", Arc::new(provider))
            .expect("register sqlite");
        engine
    });

    let mut group = c.benchmark_group("sqlite_table_scan_to_json");
    group.throughput(Throughput::Elements(1));
    group.bench_function("ordered_scan", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = engine
                    .execute_sql_to_json(
                        "SELECT employee_id, bonus, review_score FROM compensation ORDER BY employee_id",
                    )
                    .await
                    .expect("query sqlite");
                black_box(result);
            });
        });
    });
    group.finish();
}

fn benchmark_json_serialization(c: &mut Criterion) {
    let rows: Vec<serde_json::Value> = (0..1_000)
        .map(|idx| {
            serde_json::json!({
                "id": idx,
                "name": format!("Employee {idx}"),
                "salary": 75_000 + idx,
                "active": idx % 2 == 0
            })
        })
        .collect();

    let mut group = c.benchmark_group("json_result_serialization_1000_rows");
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("serde_to_string", |b| {
        b.iter(|| {
            let encoded = serde_json::to_string(black_box(&rows)).expect("serialize rows");
            black_box(encoded);
        });
    });
    group.finish();
}

fn benchmark_first_page_latency(c: &mut Criterion) {
    // First-page benchmark deliberately constructs a fresh engine per iter via
    // iter_batched so that the streaming stream and per-request SessionContext
    // start cold each iteration — this measures the contract "first page in N ms
    // without forcing full materialization." Engine construction is the setup
    // closure (not measured); only the page fetch is timed.
    let mut group = c.benchmark_group("first_page_latency_1m_rows_streaming_json");
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("page_0_size_1000", |b| {
        b.iter_batched(
            QsqlEngine::new,
            |engine| {
                RT.block_on(async {
                    let page = engine
                        .execute_sql_to_page(
                            "bench_first_page",
                            "SELECT * FROM generate_series(1, 1000000) AS t(value)",
                            ExecutePageOptions {
                                page_index: 0,
                                page_size: 1_000,
                                warning: None,
                                cancellation_token: CancellationToken::new(),
                                timeout_ms: None,
                            },
                        )
                        .await
                        .expect("first page query");
                    black_box(page);
                });
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn benchmark_federated_join(c: &mut Criterion) {
    let engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        engine
            .register_file("employees", &sample_path("employees.csv"), "csv")
            .await
            .expect("register employees");
        let provider = SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
            .await
            .expect("sqlite provider");
        engine
            .register_table("compensation", Arc::new(provider))
            .expect("register sqlite");
        engine
    });

    let mut group = c.benchmark_group("federated_csv_sqlite_join_to_json");
    group.throughput(Throughput::Elements(1));
    group.bench_function("inner_join_order_by_review", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = engine
                    .execute_sql_to_json(
                        "SELECT e.name, c.bonus, c.review_score
                         FROM employees e
                         JOIN compensation c ON e.id = c.employee_id
                         ORDER BY c.review_score DESC",
                    )
                    .await
                    .expect("federated join");
                black_box(result);
            });
        });
    });
    group.finish();
}

fn benchmark_broadcast_rewrite_csv_join_sqlite(c: &mut Criterion) {
    // Compares the same CSV ⋈ SQLite join with the broadcast-join rewrite
    // enabled vs disabled. With rewrite enabled, the CSV side's distinct
    // join keys get materialized once and pushed into the SQLite-side scan
    // as an IN-list filter. Both bench groups share the same dataset for a
    // direct apples-to-apples comparison.
    let build = |config: BroadcastRewriteConfig| {
        RT.block_on(async {
            let engine = QsqlEngine::new().with_broadcast_config(config);
            engine
                .register_file("employees", &sample_path("employees.csv"), "csv")
                .await
                .expect("register employees");
            let provider = SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
                .await
                .expect("sqlite provider");
            engine
                .register_table("compensation", Arc::new(provider))
                .expect("register sqlite");
            engine
        })
    };

    let engine_on = build(BroadcastRewriteConfig::default());
    let engine_off = build(BroadcastRewriteConfig::disabled());

    let sql = "SELECT e.name, c.bonus, c.review_score
               FROM employees e
               JOIN compensation c ON e.id = c.employee_id
               ORDER BY c.review_score DESC";

    let mut group = c.benchmark_group("broadcast_rewrite_csv_join_sqlite");
    group.throughput(Throughput::Elements(1));
    group.bench_function("rewrite_on", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = engine_on
                    .execute_sql_to_json(sql)
                    .await
                    .expect("rewrite_on");
                black_box(result);
            });
        });
    });
    group.bench_function("rewrite_off", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = engine_off
                    .execute_sql_to_json(sql)
                    .await
                    .expect("rewrite_off");
                black_box(result);
            });
        });
    });
    group.finish();
}

fn benchmark_sort_pushdown(c: &mut Criterion) {
    // Sort pushdown via datafusion-federation: ORDER BY + LIMIT reaches SQLite.
    // Compare against an in-memory CSV sort so regressions are visible.
    use rusqlite::Connection;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sqlite_path = std::env::temp_dir().join(format!(
        "bench_sort_sqlite_{}_{}.db",
        std::process::id(),
        nanos
    ));
    let csv_path = std::env::temp_dir().join(format!(
        "bench_sort_csv_{}_{}.csv",
        std::process::id(),
        nanos
    ));

    // Create a 1K-row SQLite table inserted in reverse order (ensures sort is non-trivial).
    {
        let _ = std::fs::remove_file(&sqlite_path);
        let conn = Connection::open(&sqlite_path).expect("open bench sqlite");
        conn.execute_batch("CREATE TABLE items (id INTEGER, label TEXT)")
            .unwrap();
        for i in (1usize..=1000).rev() {
            conn.execute(
                "INSERT INTO items VALUES (?1, ?2)",
                rusqlite::params![i as i64, format!("item_{}", i)],
            )
            .unwrap();
        }
    }

    // Create a 1K-row CSV file in the same reverse order.
    {
        let mut f = std::fs::File::create(&csv_path).expect("create bench csv");
        writeln!(f, "id,label").unwrap();
        for i in (1usize..=1000).rev() {
            writeln!(f, "{},item_{}", i, i).unwrap();
        }
    }

    let sqlite_engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        let provider = SqliteTableProvider::try_new(sqlite_path.to_str().unwrap(), "items")
            .await
            .expect("sqlite provider");
        engine
            .register_table("items", Arc::new(provider))
            .expect("register sqlite");
        engine
    });

    let csv_engine = RT.block_on(async {
        let engine = QsqlEngine::new();
        engine
            .register_file("items", csv_path.to_str().unwrap(), "csv")
            .await
            .expect("register csv");
        engine
    });

    let sort_sql = "SELECT id FROM items ORDER BY id DESC LIMIT 100";

    let mut group = c.benchmark_group("sort_pushdown_sqlite_1k_rows");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("order_by_desc_limit_100", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = sqlite_engine
                    .execute_sql_to_json(sort_sql)
                    .await
                    .expect("sort sqlite");
                black_box(result);
            });
        });
    });
    group.finish();

    let mut group = c.benchmark_group("sort_no_pushdown_csv_1k_rows");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("order_by_desc_limit_100", |b| {
        b.iter(|| {
            RT.block_on(async {
                let result = csv_engine
                    .execute_sql_to_json(sort_sql)
                    .await
                    .expect("sort csv");
                black_box(result);
            });
        });
    });
    group.finish();

    let _ = std::fs::remove_file(&sqlite_path);
    let _ = std::fs::remove_file(&csv_path);
}

fn benchmark_idle_daemon_rss_baseline(c: &mut Criterion) {
    // Self-process RSS baseline for the bench runner. The real daemon-subprocess
    // RSS measurement lives in integration tests (tests/common/memory.rs) which
    // can attach to a PID via the daemon's CARGO_BIN_EXE handle. Here we record
    // the test process's RSS as a sanity baseline for relative growth checks.
    let mut group = c.benchmark_group("idle_process_rss_baseline");
    group.sample_size(10);
    group.bench_function("memory_stats_self", |b| {
        b.iter(|| {
            let stats = memory_stats::memory_stats();
            black_box(stats);
        });
    });
    group.finish();
}

/// Phase 9 — compare the JSON path vs the new Arrow IPC path for paged
/// result delivery. Encodes a single 10K-row page each way (the streaming
/// model pages at most ~8192-row batches anyway, so 10K exercises the
/// multi-batch slicing logic in result_ipc.rs).
///
/// We build the source `VecDeque<RecordBatch>` once outside the timed body
/// — the bench measures _serialisation only_, not query execution.
fn benchmark_result_page_encoding(c: &mut Criterion) {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use qsql_core::result_ipc::serialize_batches_to_ipc_base64;
    use std::collections::VecDeque;

    const ROWS: usize = 10_000;
    const COLS: usize = 4;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let ids: Vec<i64> = (0..ROWS as i64).collect();
    let names: Vec<String> = (0..ROWS).map(|i| format!("row_{i}")).collect();
    let names_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let amounts: Vec<i64> = (0..ROWS as i64).map(|i| i * 7 + 1).collect();
    let regions: Vec<&str> = (0..ROWS)
        .map(|i| match i % 3 {
            0 => "NA",
            1 => "EU",
            _ => "APAC",
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names_refs)),
            Arc::new(Int64Array::from(amounts)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .expect("build batch");
    let mut batches = VecDeque::new();
    batches.push_back(batch);

    // --- IPC path ---
    let mut group = c.benchmark_group("result_page_serialize_to_ipc_base64");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function(format!("rows_{ROWS}_cols_{COLS}"), |b| {
        b.iter_batched(
            || (&batches, &schema),
            |(batches, schema)| {
                let payload =
                    serialize_batches_to_ipc_base64(batches, 0, ROWS, schema).expect("encode");
                black_box(payload);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn configure() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = phase0;
    config = configure();
    targets =
        benchmark_file_scans,
        benchmark_sqlite_scan,
        benchmark_json_serialization,
        benchmark_first_page_latency,
        benchmark_federated_join,
        benchmark_broadcast_rewrite_csv_join_sqlite,
        benchmark_sort_pushdown,
        benchmark_idle_daemon_rss_baseline,
        benchmark_result_page_encoding
}
criterion_main!(phase0);
