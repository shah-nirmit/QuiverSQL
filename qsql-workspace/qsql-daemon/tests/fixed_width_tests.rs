//! Phase 8 — daemon-level integration coverage for fixed-width files.
//!
//! Tests in this file confirm the end-to-end registration path
//! (`register_file_with_options(format: "fixed_width", options: { layout_path })`)
//! produces a queryable table whose rows match the CSV equivalent of the
//! same data. The shared fixture is the pair
//! `samples/quickstart/employees_fwf.txt` + `employees_fwf.layout.json`,
//! which mirrors `employees.csv` row-for-row.

mod common;

use qsql_core::engine::QsqlEngine;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

use common::fixtures::{generate_medium_fwf, unique_temp_path};

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

fn fixed_width_options() -> HashMap<String, Value> {
    let mut m = HashMap::new();
    m.insert(
        "layout_path".to_string(),
        Value::String(sample_path("employees_fwf.layout.json")),
    );
    m
}

#[tokio::test]
async fn fixed_width_table_registers_and_queries() {
    let engine = QsqlEngine::new();
    let options = fixed_width_options();
    engine
        .register_file_with_options(
            "employees_fwf",
            &sample_path("employees_fwf.txt"),
            "fixed_width",
            Some(&options),
        )
        .await
        .expect("fixed-width registration should succeed");

    // Simple count.
    let rows = engine
        .execute_sql_to_json("SELECT id FROM employees_fwf")
        .await
        .unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        6,
        "expected 6 rows from the sample fixed-width file"
    );

    // Range query exercises projection + integer coercion.
    let high = engine
        .execute_sql_to_json(
            "SELECT id, name, salary FROM employees_fwf WHERE salary > 90000 ORDER BY salary DESC",
        )
        .await
        .unwrap();
    let high = high.as_array().unwrap();
    assert_eq!(high.len(), 3, "Finn, Alice, Carla earn above 90k");
    // The TRIM on string columns means the column value should be the
    // unpadded name, not the raw 24-byte slice.
    assert_eq!(high[0]["name"], "Finn Morgan");
    assert_eq!(high[0]["salary"], 105000);
    assert_eq!(high[1]["name"], "Alice Rao");
    assert_eq!(high[2]["name"], "Carla Mendes");
}

#[tokio::test]
async fn fixed_width_matches_csv_equivalent_row_for_row() {
    let engine = QsqlEngine::new();
    let options = fixed_width_options();

    engine
        .register_file_with_options(
            "employees_fwf",
            &sample_path("employees_fwf.txt"),
            "fixed_width",
            Some(&options),
        )
        .await
        .unwrap();
    engine
        .register_file("employees_csv", &sample_path("employees.csv"), "csv")
        .await
        .unwrap();

    // Both queries select the same columns in the same order.
    let select = "SELECT id, name, department_id, role, salary, location \
                  FROM {} ORDER BY id";
    let fwf = engine
        .execute_sql_to_json(&select.replace("{}", "employees_fwf"))
        .await
        .unwrap();
    let csv = engine
        .execute_sql_to_json(&select.replace("{}", "employees_csv"))
        .await
        .unwrap();

    let fwf_rows = fwf.as_array().unwrap();
    let csv_rows = csv.as_array().unwrap();
    assert_eq!(fwf_rows.len(), csv_rows.len(), "row counts must match");

    for (i, (a, b)) in fwf_rows.iter().zip(csv_rows.iter()).enumerate() {
        assert_eq!(
            a["id"], b["id"],
            "row {i}: id mismatch (fwf={}, csv={})",
            a["id"], b["id"]
        );
        assert_eq!(a["name"], b["name"], "row {i}: name mismatch");
        assert_eq!(
            a["department_id"], b["department_id"],
            "row {i}: department_id mismatch"
        );
        assert_eq!(a["role"], b["role"], "row {i}: role mismatch");
        assert_eq!(a["salary"], b["salary"], "row {i}: salary mismatch");
        assert_eq!(a["location"], b["location"], "row {i}: location mismatch");
    }
}

#[tokio::test]
async fn fixed_width_registration_errors_when_layout_path_missing() {
    let engine = QsqlEngine::new();
    let err = engine
        .register_file_with_options(
            "no_layout",
            &sample_path("employees_fwf.txt"),
            "fixed_width",
            None, // no options at all
        )
        .await
        .unwrap_err();
    assert!(
        err.contains("layout_path"),
        "expected layout_path-missing error, got: {err}"
    );
}

#[tokio::test]
async fn medium_fwf_sort_smoke() {
    // Phase 8E parity with the existing CSV/SQLite medium-fixture smoke
    // tests: generate a 100K-row fixed-width file at test time, run
    // ORDER BY id DESC LIMIT 3, assert the largest id comes first.
    const ROWS: usize = 100_000;
    let data_path = unique_temp_path("medium_fwf", "fwf");
    let layout_path = generate_medium_fwf(&data_path, ROWS);

    let engine = QsqlEngine::new();
    let mut opts = HashMap::new();
    opts.insert(
        "layout_path".to_string(),
        Value::String(layout_path.to_string_lossy().into_owned()),
    );
    engine
        .register_file_with_options(
            "big",
            data_path.to_str().unwrap(),
            "fixed_width",
            Some(&opts),
        )
        .await
        .unwrap();

    let top = engine
        .execute_sql_to_json("SELECT id FROM big ORDER BY id DESC LIMIT 3")
        .await
        .unwrap();
    let rows = top.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"], 100_000);
    assert_eq!(rows[1]["id"], 99_999);
    assert_eq!(rows[2]["id"], 99_998);

    // Cleanup so we don't accumulate temp files across runs.
    let _ = std::fs::remove_file(&data_path);
    let _ = std::fs::remove_file(&layout_path);
}

#[tokio::test]
async fn fixed_width_registration_errors_when_layout_path_blank() {
    let engine = QsqlEngine::new();
    let mut opts = HashMap::new();
    opts.insert(
        "layout_path".to_string(),
        Value::String("/definitely/not/a/real/path.json".to_string()),
    );
    let err = engine
        .register_file_with_options(
            "bad_layout",
            &sample_path("employees_fwf.txt"),
            "fixed_width",
            Some(&opts),
        )
        .await
        .unwrap_err();
    // Daemon should propagate the file-not-found path so the message points
    // at the offending path; the wizard surfaces this to the user via the
    // existing showErrorMessage flow.
    assert!(err.contains("path.json"), "got: {err}");
}
