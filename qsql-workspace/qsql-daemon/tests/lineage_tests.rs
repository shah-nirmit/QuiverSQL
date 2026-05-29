//! Phase 10 — daemon-level integration coverage for the rich lineage API.
//!
//! These tests drive `QsqlEngine::get_query_lineage` end-to-end over the
//! quickstart sample CSV files for a multi-table JOIN with a SUM aggregate
//! and confirm the new output_columns / joins / aggregates / aliases
//! fields propagate cleanly through the daemon-facing engine API. The
//! integration coverage complements the per-clause unit tests in
//! `qsql-core::engine::tests` by exercising the wire-facing call site
//! with real fixture files.

use qsql_core::engine::QsqlEngine;
use std::path::PathBuf;

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
async fn lineage_join_query_carries_output_columns_joins_and_relations() {
    let engine = QsqlEngine::new();
    engine
        .register_file("employees", &sample_path("employees.csv"), "csv")
        .await
        .expect("register employees.csv");
    engine
        .register_file("departments", &sample_path("departments.ndjson"), "ndjson")
        .await
        .expect("register departments.ndjson");

    let lineage = engine
        .get_query_lineage(
            "SELECT e.name, d.name AS department_name \
             FROM employees e \
             INNER JOIN departments d ON e.department_id = d.id",
        )
        .await
        .expect("lineage on a multi-table join should succeed");

    // Legacy fields still present and accurate.
    assert!(lineage.tables.contains(&"employees".to_string()));
    assert!(lineage.tables.contains(&"departments".to_string()));
    assert_eq!(
        lineage.relations.len(),
        2,
        "one entry per source table: {:?}",
        lineage.relations
    );

    // Phase 10 — rich fields.
    assert_eq!(
        lineage.output_columns.len(),
        2,
        "two SELECT-list entries: {:?}",
        lineage.output_columns
    );
    assert_eq!(lineage.output_columns[0].name, "name");
    assert_eq!(lineage.output_columns[1].name, "department_name");

    assert_eq!(
        lineage.joins.len(),
        1,
        "single Inner join recorded: {:?}",
        lineage.joins
    );
    assert_eq!(lineage.joins[0].kind, "Inner");
    let tables_in_join = [
        lineage.joins[0].left_table.as_str(),
        lineage.joins[0].right_table.as_str(),
    ];
    assert!(
        tables_in_join.contains(&"employees") && tables_in_join.contains(&"departments"),
        "join records both underlying tables: {:?}",
        lineage.joins[0]
    );
    assert!(
        !lineage.joins[0].on.is_empty(),
        "join ON-clause keys present: {:?}",
        lineage.joins[0].on
    );

    // No aggregates / aliases for this shape.
    assert!(lineage.aggregates.is_empty());
    assert!(
        lineage.aliases.is_empty()
            || lineage
                .aliases
                .values()
                .all(|v| v == "employees" || v == "departments"),
        "aliases either absent or point at the base relations: {:?}",
        lineage.aliases
    );
}

#[tokio::test]
async fn lineage_group_by_aggregate_carries_function_and_inputs() {
    let engine = QsqlEngine::new();
    engine
        .register_file("employees", &sample_path("employees.csv"), "csv")
        .await
        .expect("register employees.csv");

    let lineage = engine
        .get_query_lineage(
            "SELECT department_id, SUM(salary) AS total_salary \
             FROM employees \
             GROUP BY department_id",
        )
        .await
        .expect("lineage on a GROUP BY should succeed");

    assert_eq!(
        lineage.aggregates.len(),
        1,
        "single SUM aggregate: {:?}",
        lineage.aggregates
    );
    let agg = &lineage.aggregates[0];
    assert_eq!(agg.function, "SUM");
    assert_eq!(
        agg.alias.as_deref(),
        Some("total_salary"),
        "the user-supplied alias is preserved end-to-end"
    );
    assert_eq!(agg.inputs.len(), 1);
    assert_eq!(agg.inputs[0].column, "salary");
}

#[tokio::test]
async fn lineage_legacy_simple_query_omits_rich_fields_on_the_wire() {
    // Regression guard: for plain `SELECT col FROM table`, the new fields
    // all skip-if-empty, so the serialised JSON the daemon returns stays
    // byte-identical to Phase 9 clients. The behaviour is exercised at the
    // type level here — we serialise to `serde_json::Value` and assert
    // none of the new keys are present.
    let engine = QsqlEngine::new();
    engine
        .register_file("employees", &sample_path("employees.csv"), "csv")
        .await
        .expect("register employees.csv");

    let lineage = engine
        .get_query_lineage("SELECT name FROM employees")
        .await
        .unwrap();
    let value = serde_json::to_value(&lineage).expect("lineage serialises");
    assert!(value.get("tables").is_some(), "legacy `tables` present");
    assert!(
        value.get("relations").is_some(),
        "legacy `relations` present"
    );
    // The new fields are present *only* when populated; the single-table
    // simple-SELECT shape populates at least `output_columns`, so we
    // assert that's the only new field on the wire.
    let wire_keys: std::collections::HashSet<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(|s| s.as_str())
        .collect();
    assert!(!wire_keys.contains("joins"));
    assert!(!wire_keys.contains("aggregates"));
    assert!(!wire_keys.contains("aliases"));
}
