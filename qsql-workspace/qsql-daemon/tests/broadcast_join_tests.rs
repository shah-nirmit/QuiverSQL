//! Integration parity tests for the broadcast-join rewrite.
//!
//! For each local-to-federated join pattern we exercise, the test:
//! 1. Runs the query with `BroadcastRewriteConfig::default()` (rewrite ON).
//! 2. Runs the same query with `BroadcastRewriteConfig::disabled()` (OFF).
//! 3. Sorts both result sets and asserts byte-for-byte row equality.
//! 4. Confirms the explain output reports `applied.len() == 1` in case 1.
//!
//! Parity is the load-bearing contract: the rewrite must never change the
//! result set; it should only narrow the remote scan to make the same join
//! faster.
//!
//! The remote-side SqliteTableProvider is wrapped in a `GuardedTableProvider`
//! before registration, because that wrapper is the federation marker the
//! rewrite uses to classify a side as "remote." The daemon does the same wrap
//! in `register_schema_table`; integration tests do it inline.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::datasource::TableProvider;
use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::broadcast::{BroadcastRewriteConfig, SkipReason};
use qsql_core::engine::QsqlEngine;
use qsql_core::GuardedTableProvider;
use rusqlite::Connection;

// Atomic counter keeps temp filenames unique even when nanosecond clock ticks
// at coarser resolution (Windows). Without this, parallel tests can collide on
// the same suffix and share a CSV/SQLite file, producing cross-test pollution.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", std::process::id(), nanos, seq)
}

fn create_csv(rows: &[(i64, &str, &str)]) -> String {
    let path =
        std::env::temp_dir().join(format!("test_qsql_broadcast_users_{}.csv", temp_suffix()));
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "id,name,role").unwrap();
    for (id, name, role) in rows {
        writeln!(file, "{id},{name},{role}").unwrap();
    }
    path.to_str().unwrap().to_string()
}

async fn create_sqlite_compensation(rows: &[(i64, f64, i64)]) -> String {
    let path = std::env::temp_dir().join(format!("test_qsql_broadcast_comp_{}.db", temp_suffix()));
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "CREATE TABLE compensation (employee_id INTEGER, salary REAL, bonus INTEGER)",
        [],
    )
    .unwrap();
    for (emp_id, salary, bonus) in rows {
        conn.execute(
            "INSERT INTO compensation (employee_id, salary, bonus) VALUES (?1, ?2, ?3)",
            rusqlite::params![emp_id, salary, bonus],
        )
        .unwrap();
    }
    path.to_str().unwrap().to_string()
}

async fn build_engine(
    csv_path: &str,
    sqlite_path: &str,
    config: BroadcastRewriteConfig,
) -> QsqlEngine {
    let engine = QsqlEngine::new().with_broadcast_config(config);
    engine
        .register_file("users", csv_path, "csv")
        .await
        .expect("register users csv");
    let sqlite = SqliteTableProvider::try_new(sqlite_path.to_string(), "compensation")
        .await
        .expect("sqlite provider");
    // Wrap in GuardedTableProvider so the broadcast rewrite classifies this
    // side as Federated. The daemon does the same wrap during JIT
    // registration; tests bypass that path so we wrap inline.
    let guarded: Arc<dyn TableProvider> =
        Arc::new(GuardedTableProvider::new("compensation", Arc::new(sqlite)));
    engine
        .register_table("compensation", guarded)
        .expect("register compensation");
    engine
}

async fn collect_sorted_rows(engine: &QsqlEngine, sql: &str) -> Vec<String> {
    let value = engine
        .execute_sql_to_json(sql)
        .await
        .expect("execute query");
    let arr = value.as_array().expect("expected JSON array").clone();
    let mut rendered: Vec<String> = arr
        .into_iter()
        .map(|v| serde_json::to_string(&v).expect("serialize row"))
        .collect();
    rendered.sort();
    rendered
}

const PARITY_SQL: &str = "
    SELECT u.id, u.name, c.bonus
    FROM users u
    JOIN compensation c ON u.id = c.employee_id
";

#[tokio::test]
async fn csv_join_sqlite_parity_with_and_without_rewrite() {
    let csv = create_csv(&[
        (1, "Alice", "Engineer"),
        (2, "Bob", "Manager"),
        (3, "Charlie", "Designer"),
    ]);
    let sqlite = create_sqlite_compensation(&[
        (1, 120000.0, 8000),
        (2, 95000.0, 5000),
        (3, 80000.0, 4000),
        (4, 70000.0, 2000), // no matching CSV row — must not appear in either result
    ])
    .await;

    let engine_on = build_engine(&csv, &sqlite, BroadcastRewriteConfig::default()).await;
    let engine_off = build_engine(&csv, &sqlite, BroadcastRewriteConfig::disabled()).await;

    let rows_on = collect_sorted_rows(&engine_on, PARITY_SQL).await;
    let rows_off = collect_sorted_rows(&engine_off, PARITY_SQL).await;
    assert_eq!(
        rows_on, rows_off,
        "broadcast rewrite must not change result set"
    );
    assert_eq!(rows_on.len(), 3, "expected 3 inner-join matches");

    // Explain output should report the rewrite when enabled.
    let (_plan_on, info_on) = engine_on
        .get_logical_plan_with_broadcast(PARITY_SQL)
        .await
        .expect("explain rewrite_on");
    assert_eq!(
        info_on.applied.len(),
        1,
        "expected one applied rewrite, got: {info_on:?}"
    );
    assert_eq!(info_on.applied[0].predicate_value_count, 3);

    let (_plan_off, info_off) = engine_off
        .get_logical_plan_with_broadcast(PARITY_SQL)
        .await
        .expect("explain rewrite_off");
    assert!(
        info_off.applied.is_empty(),
        "disabled config should never apply rewrites: {info_off:?}"
    );
    assert_eq!(info_off.considered, 0);
}

#[tokio::test]
async fn large_local_side_exceeds_cap_falls_back_to_unrewritten_plan() {
    let mut rows = Vec::with_capacity(50);
    for i in 0..50 {
        rows.push((i, "x", "y"));
    }
    let owned: Vec<(i64, &str, &str)> = rows.iter().map(|r| (r.0, r.1, r.2)).collect();
    let csv = create_csv(&owned);

    let sqlite = create_sqlite_compensation(&[(0, 1.0, 100), (1, 2.0, 200), (49, 3.0, 300)]).await;

    let config = BroadcastRewriteConfig {
        enabled: true,
        max_local_rows: 10,
        max_local_bytes: qsql_core::broadcast::DEFAULT_MAX_LOCAL_BYTES,
    };
    let engine_capped = build_engine(&csv, &sqlite, config.clone()).await;
    let engine_off = build_engine(&csv, &sqlite, BroadcastRewriteConfig::disabled()).await;

    let rows_capped = collect_sorted_rows(&engine_capped, PARITY_SQL).await;
    let rows_off = collect_sorted_rows(&engine_off, PARITY_SQL).await;
    assert_eq!(
        rows_capped, rows_off,
        "cap fallback must produce identical rows to the unrewritten plan"
    );

    let (_plan, info) = engine_capped
        .get_logical_plan_with_broadcast(PARITY_SQL)
        .await
        .expect("explain over-cap");
    assert!(
        info.applied.is_empty(),
        "over-cap rewrite must abort: {info:?}"
    );
    assert!(
        info.skipped
            .iter()
            .any(|s| s.reason == SkipReason::LocalSideMaterializationExceededCap),
        "expected LocalSideMaterializationExceededCap skip: {info:?}"
    );
}

#[tokio::test]
async fn empty_local_side_returns_zero_rows_with_rewrite_marked_applied() {
    let csv = create_csv(&[]);
    let sqlite = create_sqlite_compensation(&[(1, 1.0, 100), (2, 2.0, 200)]).await;

    let engine_on = build_engine(&csv, &sqlite, BroadcastRewriteConfig::default()).await;
    let engine_off = build_engine(&csv, &sqlite, BroadcastRewriteConfig::disabled()).await;

    let rows_on = collect_sorted_rows(&engine_on, PARITY_SQL).await;
    let rows_off = collect_sorted_rows(&engine_off, PARITY_SQL).await;
    assert_eq!(rows_on, rows_off);
    assert!(rows_on.is_empty(), "empty inner join should produce 0 rows");

    let (_plan, info) = engine_on
        .get_logical_plan_with_broadcast(PARITY_SQL)
        .await
        .expect("explain empty-side");
    assert_eq!(
        info.applied.len(),
        1,
        "empty-side rewrite still counts as applied: {info:?}"
    );
    assert_eq!(info.applied[0].predicate_value_count, 0);
}

#[tokio::test]
async fn left_join_is_not_eligible_for_rewrite() {
    let csv = create_csv(&[(1, "Alice", "E"), (2, "Bob", "M")]);
    let sqlite = create_sqlite_compensation(&[(1, 1.0, 10), (2, 2.0, 20), (3, 3.0, 30)]).await;
    let engine = build_engine(&csv, &sqlite, BroadcastRewriteConfig::default()).await;

    let sql = "SELECT u.id, u.name, c.bonus
               FROM users u LEFT JOIN compensation c ON u.id = c.employee_id";
    let (_plan, info) = engine
        .get_logical_plan_with_broadcast(sql)
        .await
        .expect("explain left join");
    assert!(info.applied.is_empty(), "LEFT JOIN must not rewrite");
    assert!(
        info.skipped
            .iter()
            .any(|s| s.reason == SkipReason::NotInnerEquiJoin),
        "expected NotInnerEquiJoin skip: {info:?}"
    );
}
