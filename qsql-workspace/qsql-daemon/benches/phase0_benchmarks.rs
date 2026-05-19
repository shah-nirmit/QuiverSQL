use criterion::{black_box, criterion_group, criterion_main, Criterion};
use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::engine::QsqlEngine;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

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

async fn register_quickstart_files(engine: &QsqlEngine) {
    engine
        .register_file("employees", &sample_path("employees.csv"), "csv")
        .await
        .expect("register employees");
    engine
        .register_file("orders", &sample_path("orders.parquet"), "parquet")
        .await
        .expect("register orders");
}

fn benchmark_file_scans(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("csv_file_scan_to_json", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = QsqlEngine::new();
                engine
                    .register_file("employees", &sample_path("employees.csv"), "csv")
                    .await
                    .expect("register csv");
                let result = engine
                    .execute_sql_to_json(
                        "SELECT id, name, salary FROM employees ORDER BY id LIMIT 5",
                    )
                    .await
                    .expect("query csv");
                black_box(result);
            });
        });
    });

    c.bench_function("parquet_file_scan_to_json", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = QsqlEngine::new();
                engine
                    .register_file("orders", &sample_path("orders.parquet"), "parquet")
                    .await
                    .expect("register parquet");
                let result = engine
                    .execute_sql_to_json(
                        "SELECT order_id, employee_id, amount FROM orders ORDER BY order_id LIMIT 5",
                    )
                    .await
                    .expect("query parquet");
                black_box(result);
            });
        });
    });
}

fn benchmark_sqlite_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("sqlite_table_scan_to_json", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = QsqlEngine::new();
                let provider =
                    SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
                        .expect("sqlite provider");
                engine
                    .register_table("compensation", Arc::new(provider))
                    .expect("register sqlite");
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

    c.bench_function("json_result_serialization_1000_rows", |b| {
        b.iter(|| {
            let encoded = serde_json::to_string(black_box(&rows)).expect("serialize rows");
            black_box(encoded);
        });
    });
}

fn benchmark_first_page_placeholder(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("first_page_latency_placeholder_full_collect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = QsqlEngine::new();
                register_quickstart_files(&engine).await;
                let result = engine
                    .execute_sql_to_json(
                        "SELECT e.name, o.product, o.amount
                         FROM employees e
                         JOIN orders o ON e.id = o.employee_id
                         ORDER BY o.amount DESC
                         LIMIT 1000",
                    )
                    .await
                    .expect("first page placeholder query");
                black_box(result);
            });
        });
    });
}

fn benchmark_federated_join(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("federated_csv_sqlite_join_to_json", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = QsqlEngine::new();
                engine
                    .register_file("employees", &sample_path("employees.csv"), "csv")
                    .await
                    .expect("register employees");
                let provider =
                    SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
                        .expect("sqlite provider");
                engine
                    .register_table("compensation", Arc::new(provider))
                    .expect("register sqlite");
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
        benchmark_first_page_placeholder,
        benchmark_federated_join
}
criterion_main!(phase0);
