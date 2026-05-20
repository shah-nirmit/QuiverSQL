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

pub fn normalize_page_size(page_size: Option<usize>) -> Result<(usize, Option<String>), QueryError> {
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


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSource {
    pub name: String,
    pub kind: SourceKind,
    pub connection_details: serde_json::Value,
    pub schema: Option<Schema>,
    pub capabilities: Option<ConnectorCapabilities>,
    pub status: String,
    pub error: Option<String>,
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
