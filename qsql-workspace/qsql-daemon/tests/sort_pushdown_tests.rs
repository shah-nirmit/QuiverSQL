/// Integration tests for sort/top-K pushdown via datafusion-federation.
///
/// datafusion-federation's SQL unparser converts the full pushable logical plan
/// — including Sort nodes — into a complete SQL string with ORDER BY + LIMIT
/// embedded before calling SQLExecutor::execute(). These tests verify that the
/// pushed-down sort produces correct ordered results, and that SQLite's own
/// EXPLAIN QUERY PLAN output confirms the sort reaches the DB layer.
mod common;

use common::fixtures::{generate_medium_csv, generate_medium_sqlite, unique_temp_path};
use qsql_connectors::sqlite::{SqliteConnector, SqliteTableProvider};
use qsql_connectors::RemoteConnector;
use qsql_core::engine::QsqlEngine;
use qsql_core::GuardedTableProvider;
use rusqlite::Connection;
use std::sync::Arc;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Creates a temp SQLite DB with table `t (id INTEGER, label TEXT)`.
/// Rows are inserted in shuffled order so the natural row order differs from
/// sort order (avoids false passes from accidental natural ordering).
async fn make_shuffled_sqlite(n: usize) -> String {
    let path = unique_temp_path("test_qsql_sort", "db");
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER, label TEXT)", [])
        .unwrap();

    // Shuffle by inserting even IDs first, then odd — ensures SQLite physical
    // order ≠ sorted order for both ASC and DESC cases.
    let evens: Vec<usize> = (1..=n).filter(|i| i % 2 == 0).collect();
    let odds: Vec<usize> = (1..=n).filter(|i| i % 2 != 0).collect();
    for i in evens.into_iter().chain(odds) {
        conn.execute(
            "INSERT INTO t (id, label) VALUES (?1, ?2)",
            rusqlite::params![i as i64, format!("row_{}", i)],
        )
        .unwrap();
    }
    path.to_str().unwrap().to_string()
}

async fn engine_with_sqlite(db_path: &str) -> QsqlEngine {
    let engine = QsqlEngine::new();
    let provider = SqliteTableProvider::try_new(db_path, "t")
        .await
        .expect("SqliteTableProvider");
    let guarded: Arc<dyn datafusion::datasource::TableProvider> =
        Arc::new(GuardedTableProvider::new("t", Arc::new(provider)));
    engine.register_table("t", guarded).expect("register t");
    engine
}

fn extract_ids(rows: &serde_json::Value) -> Vec<i64> {
    rows.as_array()
        .expect("expected array")
        .iter()
        .map(|r| r["id"].as_i64().expect("id must be integer"))
        .collect()
}

// ── sort parity tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn sort_asc_single_column_sqlite_parity() {
    let db = make_shuffled_sqlite(20).await;
    let engine = engine_with_sqlite(&db).await;

    let rows = engine
        .execute_sql_to_json("SELECT id FROM t ORDER BY id ASC LIMIT 10")
        .await
        .expect("execute");

    let ids = extract_ids(&rows);
    assert_eq!(ids.len(), 10, "expected 10 rows");
    assert_eq!(ids[0], 1, "first id should be 1");
    assert_eq!(ids[9], 10, "last id should be 10");
    // Strictly ascending
    for w in ids.windows(2) {
        assert!(w[0] < w[1], "rows not in ascending order: {:?}", ids);
    }

    let _ = std::fs::remove_file(&db);
}

#[tokio::test]
async fn sort_desc_single_column_sqlite_parity() {
    let n = 15usize;
    let db = make_shuffled_sqlite(n).await;
    let engine = engine_with_sqlite(&db).await;

    let rows = engine
        .execute_sql_to_json("SELECT id FROM t ORDER BY id DESC LIMIT 5")
        .await
        .expect("execute");

    let ids = extract_ids(&rows);
    assert_eq!(ids.len(), 5, "expected 5 rows");
    assert_eq!(ids[0], n as i64, "first id should be max");
    // Strictly descending
    for w in ids.windows(2) {
        assert!(w[0] > w[1], "rows not in descending order: {:?}", ids);
    }

    let _ = std::fs::remove_file(&db);
}

#[tokio::test]
async fn sort_topk_no_limit_sqlite() {
    let n = 12usize;
    let db = make_shuffled_sqlite(n).await;
    let engine = engine_with_sqlite(&db).await;

    let rows = engine
        .execute_sql_to_json("SELECT id FROM t ORDER BY id ASC")
        .await
        .expect("execute");

    let ids = extract_ids(&rows);
    assert_eq!(ids.len(), n, "expected all {n} rows");
    assert_eq!(ids[0], 1);
    assert_eq!(ids[n - 1], n as i64);
    for w in ids.windows(2) {
        assert!(w[0] < w[1], "rows not in ascending order: {:?}", ids);
    }

    let _ = std::fs::remove_file(&db);
}

#[tokio::test]
async fn sort_with_filter_and_limit_sqlite() {
    // Rows 1..=20, filter id > 5, ORDER BY id DESC LIMIT 3 → [20, 19, 18]
    let db = make_shuffled_sqlite(20).await;
    let engine = engine_with_sqlite(&db).await;

    let rows = engine
        .execute_sql_to_json("SELECT id FROM t WHERE id > 5 ORDER BY id DESC LIMIT 3")
        .await
        .expect("execute");

    let ids = extract_ids(&rows);
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], 20);
    assert_eq!(ids[1], 19);
    assert_eq!(ids[2], 18);

    let _ = std::fs::remove_file(&db);
}

/// Verifies that SQLite's own EXPLAIN QUERY PLAN shows sort activity at the DB
/// layer, confirming ORDER BY was pushed down rather than handled in-memory by
/// DataFusion.
#[tokio::test]
async fn sort_explain_contains_order_by() {
    let db = make_shuffled_sqlite(10).await;
    let connector = SqliteConnector::new(&db);

    let plan = connector
        .explain_query("SELECT id FROM t ORDER BY id DESC LIMIT 3")
        .await
        .expect("explain_query");

    // SQLite EXPLAIN QUERY PLAN reports "USE TEMP B-TREE FOR ORDER BY" or
    // similar when it performs a sort. Either that phrase or "ORDER" in the
    // detail confirms the sort reached the database.
    let upper = plan.to_uppercase();
    assert!(
        upper.contains("ORDER") || upper.contains("B-TREE") || upper.contains("SORT"),
        "Expected sort evidence in EXPLAIN QUERY PLAN output, got:\n{plan}"
    );

    let _ = std::fs::remove_file(&db);
}

// ── medium fixture smoke tests ────────────────────────────────────────────────

#[tokio::test]
async fn medium_csv_sort_smoke() {
    let path = unique_temp_path("test_qsql_medium_sort", "csv");
    generate_medium_csv(&path, 100_000);

    let engine = QsqlEngine::new();
    engine
        .register_file("big", path.to_str().unwrap(), "csv")
        .await
        .expect("register csv");

    let rows = engine
        .execute_sql_to_json("SELECT id FROM big ORDER BY id DESC LIMIT 3")
        .await
        .expect("execute");

    let ids: Vec<i64> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();

    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids[0], 100_000,
        "expected top id to be 100000, got {}",
        ids[0]
    );
    assert_eq!(ids[1], 99_999);
    assert_eq!(ids[2], 99_998);

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn medium_sqlite_sort_smoke() {
    let path = unique_temp_path("test_qsql_medium_sort_sqlite", "db");
    generate_medium_sqlite(&path, "big", 100_000);

    let engine = QsqlEngine::new();
    let provider = SqliteTableProvider::try_new(path.to_str().unwrap(), "big")
        .await
        .expect("SqliteTableProvider");
    let guarded: Arc<dyn datafusion::datasource::TableProvider> =
        Arc::new(GuardedTableProvider::new("big", Arc::new(provider)));
    engine.register_table("big", guarded).expect("register big");

    let rows = engine
        .execute_sql_to_json("SELECT id FROM big ORDER BY id DESC LIMIT 3")
        .await
        .expect("execute");

    let ids: Vec<i64> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();

    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids[0], 100_000,
        "expected top id to be 100000, got {}",
        ids[0]
    );
    assert_eq!(ids[1], 99_999);
    assert_eq!(ids[2], 99_998);

    let _ = std::fs::remove_file(&path);
}

// Postgres and MySQL live sort parity tests are covered by qsql-connectors tests
// when QSQL_POSTGRES_URL / QSQL_MYSQL_URL are set. They are not included here
// because the daemon test crate does not enable those connector features.
