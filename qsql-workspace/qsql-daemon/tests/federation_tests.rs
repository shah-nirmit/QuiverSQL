use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::engine::QsqlEngine;
use rusqlite::Connection;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

fn create_temp_csv() -> String {
    let path =
        std::env::temp_dir().join(format!("test_qsql_federated_users_{}.csv", temp_suffix()));
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "id,name,role").unwrap();
    writeln!(file, "1,Alice,Engineer").unwrap();
    writeln!(file, "2,Bob,Manager").unwrap();
    writeln!(file, "3,Charlie,Designer").unwrap();
    path.to_str().unwrap().to_string()
}

async fn create_temp_sqlite() -> String {
    let path =
        std::env::temp_dir().join(format!("test_qsql_federated_salaries_{}.db", temp_suffix()));
    let _ = std::fs::remove_file(&path);

    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "CREATE TABLE compensation (employee_id INTEGER, salary REAL)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO compensation (employee_id, salary) VALUES (1, 120000.0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO compensation (employee_id, salary) VALUES (2, 95000.0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO compensation (employee_id, salary) VALUES (3, 80000.0)",
        [],
    )
    .unwrap();

    path.to_str().unwrap().to_string()
}

async fn create_temp_sqlite_sales() -> String {
    let path = std::env::temp_dir().join(format!("test_qsql_federated_sales_{}.db", temp_suffix()));
    let _ = std::fs::remove_file(&path);

    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "CREATE TABLE customers (id INTEGER, name TEXT, region TEXT)",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, total REAL)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO customers (id, name, region) VALUES
         (1, 'Acme', 'west'), (2, 'Globex', 'east')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO orders (id, customer_id, total) VALUES
         (10, 1, 125.0), (11, 1, 90.0), (12, 2, 60.0)",
        [],
    )
    .unwrap();

    path.to_str().unwrap().to_string()
}

#[tokio::test]
async fn test_federated_cross_source_join() {
    let engine = QsqlEngine::new();

    // 1. Register CSV File (Local Users)
    let csv_path = create_temp_csv();
    engine
        .register_file("users", &csv_path, "csv")
        .await
        .unwrap();

    // 2. Register SQLite Database Table (Compensation) via custom connector
    let db_path = create_temp_sqlite().await;
    let provider = SqliteTableProvider::try_new(&db_path, "compensation")
        .await
        .unwrap();
    engine.register_table("comp", Arc::new(provider)).unwrap();

    // 3. Execute federated JOIN query
    let sql = "
        SELECT u.name, u.role, c.salary
        FROM users u
        JOIN comp c ON u.id = c.employee_id
        WHERE c.salary > 90000.00
        ORDER BY c.salary DESC
    ";

    let result = engine.execute_sql_to_json(sql).await.unwrap();

    // 4. Assert correct merged outputs
    assert!(result.is_array());
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0]["name"], "Alice");
    assert_eq!(rows[0]["role"], "Engineer");
    assert_eq!(rows[0]["salary"], 120000.00);

    assert_eq!(rows[1]["name"], "Bob");
    assert_eq!(rows[1]["role"], "Manager");
    assert_eq!(rows[1]["salary"], 95000.00);

    // Clean up temporary files
    let _ = std::fs::remove_file(csv_path);
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_same_source_sqlite_join_uses_federation_optimizer() {
    let engine = QsqlEngine::new();
    let db_path = create_temp_sqlite_sales().await;

    let customers = SqliteTableProvider::try_new(&db_path, "customers")
        .await
        .unwrap();
    let orders = SqliteTableProvider::try_new(&db_path, "orders")
        .await
        .unwrap();
    engine
        .register_table("customers", Arc::new(customers))
        .unwrap();
    engine.register_table("orders", Arc::new(orders)).unwrap();

    let sql = "
        SELECT c.name, SUM(o.total) AS order_total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        WHERE c.region = 'west'
        GROUP BY c.name
    ";

    let plan = engine.get_logical_plan(sql).await.unwrap();
    let plan_text = format!("{plan:?}");
    assert!(
        plan_text.contains("Federated"),
        "expected same-source SQLite subplan to be federated, got:\n{plan_text}"
    );

    let result = engine.execute_sql_to_json(sql).await.unwrap();
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "Acme");
    assert_eq!(rows[0]["order_total"], 215.0);

    let _ = std::fs::remove_file(db_path);
}
