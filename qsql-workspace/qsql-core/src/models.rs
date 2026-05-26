use serde::{Deserialize, Serialize};

pub const DEFAULT_PAGE_SIZE: usize = 1_000;
pub const MAX_PAGE_SIZE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Csv,
    Parquet,
    Json,
    Ndjson,
    Sqlite,
    FixedWidth,
    Postgres,
    Mysql,
    Mariadb,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceProfile {
    pub name: String,
    pub kind: SourceKind,
    pub connection_details: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub source_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorCapabilities {
    pub projection: bool,
    pub filter: bool,
    pub limit: bool,
    pub aggregate: bool,
    pub joins: bool,
    pub dialect_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPage {
    pub query_id: String,
    pub schema: Schema,
    pub page_index: usize,
    pub page_size: usize,
    pub is_last: bool,
    pub data: Vec<serde_json::Value>,
    pub metrics: PerformanceMetrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryHandle {
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainResult {
    pub logical_plan: String,
    pub physical_plan: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainQueryRequest {
    pub sql: String,
    pub include_native: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainQueryResult {
    pub sql: String,
    pub federated_plan: PlanGraph,
    pub source_plans: serde_json::Value,
    pub raw: String,
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast_rewrites: Option<crate::broadcast::BroadcastRewriteInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanGraph {
    pub root_ids: Vec<String>,
    pub nodes: std::collections::HashMap<String, PlanNode>,
    pub node_count: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanNode {
    pub id: String,
    pub origin: String,
    pub node_type: String,
    pub label: String,
    pub children: Vec<String>,
    pub attributes: std::collections::HashMap<String, String>,
    pub metrics: PlanMetrics,
    pub source_ref: Option<String>,
    pub native_plan_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanMetrics {
    pub estimated_rows: Option<f64>,
    pub estimated_bytes: Option<f64>,
    pub startup_cost: Option<f64>,
    pub total_cost: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub planning_time_ms: u64,
    pub execution_time_ms: u64,
    pub first_page_time_ms: u64,
    pub rows_produced: u64,
    pub rows_returned: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStartRequest {
    pub sql: String,
    #[serde(default)]
    pub page_size: Option<usize>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPageRequest {
    pub query_id: String,
    #[serde(default)]
    pub page_index: Option<usize>,
    #[serde(default)]
    pub page_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCancelRequest {
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCancelResult {
    pub query_id: String,
    pub cancelled: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionResult {
    pub schema: Schema,
    pub data: Vec<serde_json::Value>,
    pub metrics: PerformanceMetrics,
}

pub fn normalize_page_size(
    page_size: Option<usize>,
) -> Result<(usize, Option<String>), QueryError> {
    match page_size {
        Some(0) => Err(QueryError {
            code: -32602,
            message: "page_size must be greater than zero".to_string(),
            details: None,
        }),
        Some(size) if size > MAX_PAGE_SIZE => Ok((
            MAX_PAGE_SIZE,
            Some(format!(
                "Requested page_size {size} exceeded the maximum {MAX_PAGE_SIZE}; using {MAX_PAGE_SIZE}."
            )),
        )),
        Some(size) => Ok((size, None)),
        None => Ok((DEFAULT_PAGE_SIZE, None)),
    }
}

pub fn build_query_page(
    query_id: impl Into<String>,
    result: &QueryExecutionResult,
    page_index: usize,
    page_size: usize,
    warning: Option<String>,
) -> QueryPage {
    let start = page_index.saturating_mul(page_size);
    let end = start.saturating_add(page_size).min(result.data.len());
    let data = if start >= result.data.len() {
        Vec::new()
    } else {
        result.data[start..end].to_vec()
    };

    let mut metrics = result.metrics.clone();
    metrics.rows_returned = data.len() as u64;

    QueryPage {
        query_id: query_id.into(),
        schema: result.schema.clone(),
        page_index,
        page_size,
        is_last: end >= result.data.len(),
        data,
        metrics,
        warning,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_result(rows: usize) -> QueryExecutionResult {
        QueryExecutionResult {
            schema: Schema {
                fields: vec![SchemaField {
                    name: "x".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                }],
            },
            data: (0..rows).map(|i| json!(i)).collect(),
            metrics: PerformanceMetrics {
                planning_time_ms: 1,
                execution_time_ms: 2,
                first_page_time_ms: 3,
                rows_produced: rows as u64,
                rows_returned: rows as u64,
            },
        }
    }

    #[test]
    fn normalize_page_size_defaults_to_default_page_size() {
        let (size, warn) = normalize_page_size(None).unwrap();
        assert_eq!(size, DEFAULT_PAGE_SIZE);
        assert!(warn.is_none());
    }

    #[test]
    fn normalize_page_size_zero_is_error() {
        assert!(normalize_page_size(Some(0)).is_err());
    }

    #[test]
    fn normalize_page_size_clamps_over_max() {
        let (size, warn) = normalize_page_size(Some(MAX_PAGE_SIZE + 1)).unwrap();
        assert_eq!(size, MAX_PAGE_SIZE);
        assert!(warn.is_some());
        assert!(warn.unwrap().contains(&(MAX_PAGE_SIZE + 1).to_string()));
    }

    #[test]
    fn normalize_page_size_exact_max_is_accepted() {
        let (size, warn) = normalize_page_size(Some(MAX_PAGE_SIZE)).unwrap();
        assert_eq!(size, MAX_PAGE_SIZE);
        assert!(warn.is_none());
    }

    #[test]
    fn normalize_page_size_normal_value_passes_through() {
        let (size, warn) = normalize_page_size(Some(500)).unwrap();
        assert_eq!(size, 500);
        assert!(warn.is_none());
    }

    #[test]
    fn build_query_page_first_page() {
        let result = make_result(10);
        let page = build_query_page("q1", &result, 0, 3, None);
        assert_eq!(page.data.len(), 3);
        assert_eq!(page.page_index, 0);
        assert!(!page.is_last);
        assert_eq!(page.metrics.rows_returned, 3);
    }

    #[test]
    fn build_query_page_last_partial_page() {
        let result = make_result(10);
        let page = build_query_page("q1", &result, 3, 3, None);
        // page 3: rows 9..10 → 1 row
        assert_eq!(page.data.len(), 1);
        assert!(page.is_last);
    }

    #[test]
    fn build_query_page_beyond_end_is_empty_and_last() {
        let result = make_result(5);
        let page = build_query_page("q1", &result, 10, 3, None);
        assert!(page.data.is_empty());
        assert!(page.is_last);
    }

    #[test]
    fn build_query_page_warning_is_propagated() {
        let result = make_result(1);
        let page = build_query_page("q1", &result, 0, 100, Some("capped".to_string()));
        assert_eq!(page.warning.as_deref(), Some("capped"));
    }

    #[test]
    fn serde_round_trip_catalog_source() {
        let src = CatalogSource {
            name: "db1".to_string(),
            kind: SourceKind::Postgres,
            connection_details: json!({"host": "localhost"}),
            schema: None,
            capabilities: None,
            status: "ok".to_string(),
            error: None,
            tables: Some(vec!["t1".to_string()]),
        };
        let json = serde_json::to_string(&src).unwrap();
        let back: CatalogSource = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, src.name);
        assert_eq!(back.tables, src.tables);
    }

    #[test]
    fn serde_round_trip_remove_source() {
        let req = RemoveSourceRequest { name: "s1".to_string() };
        let res = RemoveSourceResult { name: "s1".to_string(), removed: true };
        let req2: RemoveSourceRequest = serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        let res2: RemoveSourceResult = serde_json::from_str(&serde_json::to_string(&res).unwrap()).unwrap();
        assert_eq!(req2.name, "s1");
        assert!(res2.removed);
    }

    #[test]
    fn serde_round_trip_list_source_tables() {
        let req = ListSourceTablesRequest { name: "db".to_string(), offset: Some(5), limit: Some(10) };
        let result = ListSourceTablesResult {
            name: "db".to_string(),
            tables: vec!["a".to_string(), "b".to_string()],
            offset: 5,
            limit: 10,
            total_known: Some(20),
            truncated: false,
        };
        let req2: ListSourceTablesRequest = serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        let res2: ListSourceTablesResult = serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap();
        assert_eq!(req2.offset, Some(5));
        assert_eq!(res2.tables.len(), 2);
        assert_eq!(res2.total_known, Some(20));
    }

    #[test]
    fn serde_round_trip_get_source_metadata_request() {
        let req = GetSourceMetadataRequest { name: "src".to_string() };
        let req2: GetSourceMetadataRequest = serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        assert_eq!(req2.name, "src");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSource {
    pub name: String,
    pub kind: SourceKind,
    pub connection_details: serde_json::Value,
    pub schema: Option<Schema>,
    pub capabilities: Option<ConnectorCapabilities>,
    pub status: String,
    pub error: Option<String>,
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveSourceRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveSourceResult {
    pub name: String,
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetSourceMetadataRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSourceTablesRequest {
    pub name: String,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSourceTablesResult {
    pub name: String,
    pub tables: Vec<String>,
    pub offset: usize,
    pub limit: usize,
    pub total_known: Option<usize>,
    pub truncated: bool,
}
