use qsql_connectors::sqlite::{SqliteConnector, SqliteTableProvider};
use qsql_connectors::RemoteConnector;
use qsql_core::engine::QsqlEngine;
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

    let connector = SqliteConnector::new(path.to_str().unwrap());
    // Create table and insert rows using our connector execution path!
    connector
        .execute_query("CREATE TABLE compensation (employee_id INTEGER, salary REAL)")
        .await
        .unwrap();
    connector
        .execute_query("INSERT INTO compensation (employee_id, salary) VALUES (1, 120000.0) ")
        .await
        .unwrap();
    connector
        .execute_query("INSERT INTO compensation (employee_id, salary) VALUES (2, 95000.0) ")
        .await
        .unwrap();
    connector
        .execute_query("INSERT INTO compensation (employee_id, salary) VALUES (3, 80000.0) ")
        .await
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
    let provider = SqliteTableProvider::try_new(&db_path, "compensation").unwrap();
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
