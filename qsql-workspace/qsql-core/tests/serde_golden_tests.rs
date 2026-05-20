use qsql_core::models::*;
use serde_json::json;

#[test]
fn test_source_kind_golden() {
    let source_kind = SourceKind::FixedWidth;
    let serialized = serde_json::to_value(&source_kind).unwrap();
    assert_eq!(serialized, json!("fixed_width"));

    let deserialized: SourceKind = serde_json::from_value(json!("fixed_width")).unwrap();
    assert_eq!(deserialized, SourceKind::FixedWidth);

    let all_kinds = vec![
        (SourceKind::Csv, "csv"),
        (SourceKind::Parquet, "parquet"),
        (SourceKind::Json, "json"),
        (SourceKind::Ndjson, "ndjson"),
        (SourceKind::Sqlite, "sqlite"),
        (SourceKind::FixedWidth, "fixed_width"),
        (SourceKind::Postgres, "postgres"),
        (SourceKind::Mysql, "mysql"),
        (SourceKind::Mariadb, "mariadb"),
    ];

    for (kind, expected) in all_kinds {
        assert_eq!(serde_json::to_value(&kind).unwrap(), json!(expected));
        let de: SourceKind = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(de, kind);
    }
}

#[test]
fn test_source_profile_golden() {
    let profile = SourceProfile {
        name: "my_csv".to_string(),
        kind: SourceKind::Csv,
        connection_details: json!({ "path": "/data/file.csv" }),
    };

    let expected = json!({
        "name": "my_csv",
        "kind": "csv",
        "connection_details": {
            "path": "/data/file.csv"
        }
    });

    assert_eq!(serde_json::to_value(&profile).unwrap(), expected);
    let deserialized: SourceProfile = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, profile);
}

#[test]
fn test_table_ref_golden() {
    let table_ref = TableRef {
        source_name: "my_postgres".to_string(),
        table_name: "users".to_string(),
    };

    let expected = json!({
        "source_name": "my_postgres",
        "table_name": "users"
    });

    assert_eq!(serde_json::to_value(&table_ref).unwrap(), expected);
    let deserialized: TableRef = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, table_ref);
}

#[test]
fn test_connector_capabilities_golden() {
    let capabilities = ConnectorCapabilities {
        projection: true,
        filter: false,
        limit: true,
        aggregate: false,
        joins: true,
        dialect_name: "postgres".to_string(),
    };

    let expected = json!({
        "projection": true,
        "filter": false,
        "limit": true,
        "aggregate": false,
        "joins": true,
        "dialect_name": "postgres"
    });

    assert_eq!(serde_json::to_value(&capabilities).unwrap(), expected);
    let deserialized: ConnectorCapabilities = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, capabilities);
}

#[test]
fn test_schema_field_and_schema_golden() {
    let field1 = SchemaField {
        name: "id".to_string(),
        data_type: "Int64".to_string(),
        nullable: false,
    };
    let field2 = SchemaField {
        name: "name".to_string(),
        data_type: "Utf8".to_string(),
        nullable: true,
    };
    let schema = Schema {
        fields: vec![field1, field2],
    };

    let expected = json!({
        "fields": [
            {
                "name": "id",
                "data_type": "Int64",
                "nullable": false
            },
            {
                "name": "name",
                "data_type": "Utf8",
                "nullable": true
            }
        ]
    });

    assert_eq!(serde_json::to_value(&schema).unwrap(), expected);
    let deserialized: Schema = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, schema);
}

#[test]
fn test_query_page_golden() {
    let page = QueryPage {
        query_id: "q_123".to_string(),
        schema: Schema {
            fields: vec![SchemaField {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }],
        },
        page_index: 2,
        page_size: 100,
        is_last: true,
        data: vec![
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "name": "Bob" }),
        ],
        metrics: PerformanceMetrics {
            planning_time_ms: 3,
            execution_time_ms: 8,
            first_page_time_ms: 11,
            rows_produced: 202,
            rows_returned: 2,
        },
        warning: Some("Page size was clamped.".to_string()),
    };

    let expected = json!({
        "query_id": "q_123",
        "schema": {
            "fields": [
                {
                    "name": "id",
                    "data_type": "Int64",
                    "nullable": false
                }
            ]
        },
        "page_index": 2,
        "page_size": 100,
        "is_last": true,
        "data": [
            { "id": 1, "name": "Alice" },
            { "id": 2, "name": "Bob" }
        ],
        "metrics": {
            "planning_time_ms": 3,
            "execution_time_ms": 8,
            "first_page_time_ms": 11,
            "rows_produced": 202,
            "rows_returned": 2
        },
        "warning": "Page size was clamped."
    });

    assert_eq!(serde_json::to_value(&page).unwrap(), expected);
    let deserialized: QueryPage = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, page);
}

#[test]
fn test_query_handle_golden() {
    let handle = QueryHandle {
        query_id: "q_12345".to_string(),
    };

    let expected = json!({
        "query_id": "q_12345"
    });

    assert_eq!(serde_json::to_value(&handle).unwrap(), expected);
    let deserialized: QueryHandle = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, handle);
}

#[test]
fn test_query_error_golden() {
    let query_error = QueryError {
        code: 4001,
        message: "Syntax error near SELECT".to_string(),
        details: Some("Expected token, found SELECT at line 1, col 5".to_string()),
    };

    let expected = json!({
        "code": 4001,
        "message": "Syntax error near SELECT",
        "details": "Expected token, found SELECT at line 1, col 5"
    });

    assert_eq!(serde_json::to_value(&query_error).unwrap(), expected);
    let deserialized: QueryError = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, query_error);

    // Test with details as None (should omit the key)
    let no_details = QueryError {
        code: 5000,
        message: "Internal server error".to_string(),
        details: None,
    };
    let expected_no_details = json!({
        "code": 5000,
        "message": "Internal server error"
    });
    assert_eq!(
        serde_json::to_value(&no_details).unwrap(),
        expected_no_details
    );
}

#[test]
fn test_explain_result_golden() {
    let result = ExplainResult {
        logical_plan: "Projection: id\n  TableScan: users".to_string(),
        physical_plan: "ProjectionExec: expr=[id]\n  MemoryExec: ...".to_string(),
    };

    let expected = json!({
        "logical_plan": "Projection: id\n  TableScan: users",
        "physical_plan": "ProjectionExec: expr=[id]\n  MemoryExec: ..."
    });

    assert_eq!(serde_json::to_value(&result).unwrap(), expected);
    let deserialized: ExplainResult = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, result);
}

#[test]
fn test_performance_metrics_golden() {
    let metrics = PerformanceMetrics {
        planning_time_ms: 12,
        execution_time_ms: 45,
        first_page_time_ms: 57,
        rows_produced: 1500,
        rows_returned: 1000,
    };

    let expected = json!({
        "planning_time_ms": 12,
        "execution_time_ms": 45,
        "first_page_time_ms": 57,
        "rows_produced": 1500,
        "rows_returned": 1000
    });

    assert_eq!(serde_json::to_value(&metrics).unwrap(), expected);
    let deserialized: PerformanceMetrics = serde_json::from_value(expected).unwrap();
    assert_eq!(deserialized, metrics);
}

#[test]
fn test_query_requests_and_cancel_result_golden() {
    let start = QueryStartRequest {
        sql: "SELECT * FROM employees".to_string(),
        page_size: Some(500),
        timeout_ms: Some(10_000),
    };
    let start_expected = json!({
        "sql": "SELECT * FROM employees",
        "page_size": 500,
        "timeout_ms": 10000
    });
    assert_eq!(serde_json::to_value(&start).unwrap(), start_expected);
    let start_de: QueryStartRequest = serde_json::from_value(start_expected).unwrap();
    assert_eq!(start_de, start);

    let page = QueryPageRequest {
        query_id: "q_1".to_string(),
        page_index: Some(1),
        page_size: Some(500),
    };
    let page_expected = json!({
        "query_id": "q_1",
        "page_index": 1,
        "page_size": 500
    });
    assert_eq!(serde_json::to_value(&page).unwrap(), page_expected);
    let page_de: QueryPageRequest = serde_json::from_value(page_expected).unwrap();
    assert_eq!(page_de, page);

    let cancel = QueryCancelRequest {
        query_id: "q_1".to_string(),
    };
    let cancel_expected = json!({ "query_id": "q_1" });
    assert_eq!(serde_json::to_value(&cancel).unwrap(), cancel_expected);
    let cancel_de: QueryCancelRequest = serde_json::from_value(cancel_expected).unwrap();
    assert_eq!(cancel_de, cancel);

    let cancel_result = QueryCancelResult {
        query_id: "q_1".to_string(),
        cancelled: true,
        message: "Query cancelled".to_string(),
    };
    let cancel_result_expected = json!({
        "query_id": "q_1",
        "cancelled": true,
        "message": "Query cancelled"
    });
    assert_eq!(
        serde_json::to_value(&cancel_result).unwrap(),
        cancel_result_expected
    );
    let cancel_result_de: QueryCancelResult =
        serde_json::from_value(cancel_result_expected).unwrap();
    assert_eq!(cancel_result_de, cancel_result);
}
