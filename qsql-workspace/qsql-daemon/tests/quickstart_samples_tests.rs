use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::engine::QsqlEngine;
use std::path::PathBuf;
use std::sync::Arc;

fn sample_path(file_name: &str) -> String {
    repo_root()
        .join("samples")
        .join("quickstart")
        .join(file_name)
        .to_string_lossy()
        .into_owned()
}

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

#[tokio::test]
async fn test_quickstart_samples_are_queryable() {
    let engine = QsqlEngine::new();

    engine
        .register_file("employees", &sample_path("employees.csv"), "csv")
        .await
        .unwrap();
    engine
        .register_file("departments", &sample_path("departments.ndjson"), "json")
        .await
        .unwrap();
    engine
        .register_file("projects", &sample_path("projects.json"), "json")
        .await
        .unwrap();
    engine
        .register_file("orders", &sample_path("orders.parquet"), "parquet")
        .await
        .unwrap();

    let compensation = SqliteTableProvider::try_new(sample_path("demo.sqlite"), "compensation")
        .await
        .unwrap();
    engine
        .register_table("compensation", Arc::new(compensation))
        .unwrap();

    let high_earners = engine
        .execute_sql_to_json(
            "SELECT name, role, salary FROM employees WHERE salary > 90000 ORDER BY salary DESC",
        )
        .await
        .unwrap();
    assert_eq!(high_earners.as_array().unwrap().len(), 3);

    let departments = engine
        .execute_sql_to_json("SELECT name FROM departments ORDER BY id")
        .await
        .unwrap();
    assert_eq!(departments.as_array().unwrap().len(), 4);

    let projects = engine
        .execute_sql_to_json("SELECT project_name FROM projects WHERE status = 'active'")
        .await
        .unwrap();
    assert_eq!(projects.as_array().unwrap().len(), 2);

    let shipped_orders = engine
        .execute_sql_to_json("SELECT product FROM orders WHERE shipped = true")
        .await
        .unwrap();
    assert_eq!(shipped_orders.as_array().unwrap().len(), 4);

    let federated = engine
        .execute_sql_to_json(
            "SELECT e.name, c.band
             FROM employees e
             JOIN compensation c ON e.id = c.employee_id
             WHERE c.review_score >= 4.7
             ORDER BY c.review_score DESC",
        )
        .await
        .unwrap();
    assert_eq!(federated.as_array().unwrap().len(), 2);
}
