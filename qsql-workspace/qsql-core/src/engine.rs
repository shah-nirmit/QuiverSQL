use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::options::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};
use datafusion::prelude::*;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::models::{
    build_query_page, normalize_page_size, CatalogSource, PerformanceMetrics, QueryError,
    QueryExecutionResult, QueryPage, Schema as QsqlSchema, SchemaField,
};

pub struct QsqlEngine {
    ctx: SessionContext,
    pub catalog: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, CatalogSource>>>,
}

pub struct ExecutePageOptions {
    pub page_index: usize,
    pub page_size: usize,
    pub warning: Option<String>,
    pub cancellation_token: CancellationToken,
    pub timeout_ms: Option<u64>,
}

impl QsqlEngine {
    pub fn get_catalog(&self) -> Vec<CatalogSource> {
        let catalog = self.catalog.lock().unwrap();
        catalog.values().cloned().collect()
    }

    pub fn get_source_metadata(&self, name: &str) -> Option<CatalogSource> {
        let catalog = self.catalog.lock().unwrap();
        catalog.get(name).cloned()
    }

    pub fn catalog_source(&self, source: CatalogSource) {
        let mut catalog = self.catalog.lock().unwrap();
        catalog.insert(source.name.clone(), source);
    }

    pub fn remove_source(&self, name: &str) -> Result<bool, String> {
        let deregistered = self
            .ctx
            .deregister_table(name)
            .map_err(|e| e.to_string())?
            .is_some();
        let mut catalog = self.catalog.lock().unwrap();
        let removed_from_catalog = catalog.remove(name).is_some();
        Ok(deregistered || removed_from_catalog)
    }

    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            catalog: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Executes a SQL query and returns the pretty-printed result as a string.
    pub async fn execute_sql_to_string(&self, sql: &str) -> Result<String, String> {
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;

        if batches.is_empty() {
            return Ok("No results or table successfully created.".to_string());
        }

        let formatted = pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string();

        Ok(formatted)
    }

    /// Executes a SQL query and returns the result as a JSON string.
    pub async fn execute_sql_to_json(&self, sql: &str) -> Result<serde_json::Value, String> {
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;

        if batches.is_empty() {
            return Ok(serde_json::json!([]));
        }

        let mut buf = Vec::new();
        {
            let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
            for batch in &batches {
                writer.write(batch).map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())?;
        }

        let json_str = String::from_utf8(buf).map_err(|e| e.to_string())?;
        let val: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
        Ok(val)
    }

    /// Executes a SQL query and returns a page-oriented result with schema and metrics.
    pub async fn execute_sql_to_page(
        &self,
        query_id: &str,
        sql: &str,
        options: ExecutePageOptions,
    ) -> Result<QueryPage, QueryError> {
        let (page_size, size_warning) = normalize_page_size(Some(options.page_size))?;
        let warning = options.warning.or(size_warning);
        let result = self
            .execute_sql_collect(sql, options.cancellation_token, options.timeout_ms)
            .await?;

        Ok(build_query_page(
            query_id.to_string(),
            &result,
            options.page_index,
            page_size,
            warning,
        ))
    }

    /// Executes a SQL query and returns all rows plus schema/metrics.
    /// The daemon uses this to cache row data for subsequent JSON pages.
    pub async fn execute_sql_collect(
        &self,
        sql: &str,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<QueryExecutionResult, QueryError> {
        if cancellation_token.is_cancelled() {
            return Err(query_cancelled_error());
        }

        if timeout_ms == Some(0) {
            return Err(query_timeout_error(0));
        }

        let execution = async {
            let planning_start = Instant::now();
            let df = self.ctx.sql(sql).await.map_err(query_execution_error)?;
            let schema = dataframe_schema_to_qsql_schema(df.schema());
            let planning_time_ms = elapsed_ms(planning_start);

            let execution_start = Instant::now();
            let batches = df.collect().await.map_err(query_execution_error)?;
            let execution_time_ms = elapsed_ms(execution_start);

            let data = record_batches_to_json_rows(&batches)?;
            let rows_produced = data.len() as u64;

            Ok(QueryExecutionResult {
                schema,
                data,
                metrics: PerformanceMetrics {
                    planning_time_ms,
                    execution_time_ms,
                    first_page_time_ms: planning_time_ms + execution_time_ms,
                    rows_produced,
                    rows_returned: rows_produced,
                },
            })
        };

        match timeout_ms {
            Some(timeout_ms) => {
                tokio::select! {
                    _ = cancellation_token.cancelled() => Err(query_cancelled_error()),
                    result = tokio::time::timeout(Duration::from_millis(timeout_ms), execution) => {
                        match result {
                            Ok(result) => result,
                            Err(_) => Err(query_timeout_error(timeout_ms)),
                        }
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = cancellation_token.cancelled() => Err(query_cancelled_error()),
                    result = execution => result,
                }
            }
        }
    }

    /// Registers a local file as a virtual table in the DataFusion context.
    pub async fn register_file(
        &self,
        table_name: &str,
        file_path: &str,
        format: &str,
    ) -> Result<String, String> {
        let kind = match format.to_lowercase().as_str() {
            "csv" => crate::models::SourceKind::Csv,
            "parquet" => crate::models::SourceKind::Parquet,
            "json" => crate::models::SourceKind::Json,
            "ndjson" => crate::models::SourceKind::Ndjson,
            _ => return Err(format!("Unsupported format: {}", format)),
        };

        match kind {
            crate::models::SourceKind::Csv => {
                self.ctx
                    .register_csv(table_name, file_path, CsvReadOptions::new())
                    .await
                    .map_err(|e| format!("Failed to register CSV: {}", e))?;
            }
            crate::models::SourceKind::Parquet => {
                self.ctx
                    .register_parquet(table_name, file_path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| format!("Failed to register Parquet: {}", e))?;
            }
            crate::models::SourceKind::Json | crate::models::SourceKind::Ndjson => {
                let file_extension = file_extension_filter(file_path, ".json");
                self.ctx
                    .register_json(
                        table_name,
                        file_path,
                        NdJsonReadOptions::default().file_extension(&file_extension),
                    )
                    .await
                    .map_err(|e| format!("Failed to register JSON: {}", e))?;
            }
            _ => unreachable!(),
        }

        let df = self
            .ctx
            .table(table_name)
            .await
            .map_err(|e| e.to_string())?;
        let arrow_schema: &datafusion::arrow::datatypes::Schema = df.schema().as_ref();
        let qsql_schema = arrow_schema_to_qsql_schema(arrow_schema);

        let source = CatalogSource {
            name: table_name.to_string(),
            kind,
            connection_details: serde_json::json!({
                "path": file_path,
                "format": format,
            }),
            schema: Some(qsql_schema),
            capabilities: None,
            status: "ready".to_string(),
            error: None,
        };
        self.catalog_source(source);

        Ok(format!(
            "Successfully registered '{}' as a virtual table.",
            table_name
        ))
    }

    /// Registers any DataFusion `TableProvider` under a given name.
    /// Used by `qsql-connectors` to inject remote sources (SQLite, Postgres, etc.)
    /// into the shared DataFusion session without creating a circular dependency.
    pub fn register_table(
        &self,
        table_name: &str,
        provider: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<String, String> {
        self.ctx
            .register_table(table_name, provider)
            .map_err(|e| format!("Failed to register table '{}': {}", table_name, e))?;
        Ok(format!(
            "Successfully registered '{}' as a federated table.",
            table_name
        ))
    }

    /// Extracts column-level query lineage from a SQL statement.
    pub async fn get_query_lineage(&self, sql: &str) -> Result<QueryLineage, String> {
        let plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;
        let plan = self
            .ctx
            .state()
            .optimize(&plan)
            .map_err(|e| e.to_string())?;

        let mut results = std::collections::HashMap::new();
        fn extract_lineage(
            plan: &datafusion::logical_expr::LogicalPlan,
            results: &mut std::collections::HashMap<String, std::collections::HashSet<String>>,
        ) {
            use datafusion::logical_expr::LogicalPlan;
            match plan {
                LogicalPlan::TableScan(scan) => {
                    let table_name = scan.table_name.table().to_string();
                    let entry = results.entry(table_name).or_default();
                    let schema = scan.source.schema();
                    if let Some(proj) = &scan.projection {
                        for &idx in proj {
                            if let Some(field) = schema.fields().get(idx) {
                                entry.insert(field.name().clone());
                            }
                        }
                    } else {
                        for field in schema.fields() {
                            entry.insert(field.name().clone());
                        }
                    }
                }
                _ => {
                    for input in plan.inputs() {
                        extract_lineage(input, results);
                    }
                }
            }
        }

        extract_lineage(&plan, &mut results);

        let mut tables = Vec::new();
        let mut relations = Vec::new();
        for (table_name, cols) in results {
            tables.push(table_name.clone());
            let mut columns: Vec<String> = cols.into_iter().collect();
            columns.sort();
            relations.push(LineageInfo {
                table_name,
                columns,
            });
        }
        tables.sort();
        relations.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(QueryLineage { tables, relations })
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn query_execution_error(error: impl ToString) -> QueryError {
    QueryError {
        code: -32001,
        message: error.to_string(),
        details: None,
    }
}

fn query_cancelled_error() -> QueryError {
    QueryError {
        code: -32002,
        message: "Query cancelled".to_string(),
        details: None,
    }
}

fn query_timeout_error(timeout_ms: u64) -> QueryError {
    QueryError {
        code: -32003,
        message: format!("Query timed out after {timeout_ms}ms"),
        details: None,
    }
}

fn dataframe_schema_to_qsql_schema(schema: &datafusion::common::DFSchema) -> QsqlSchema {
    QsqlSchema {
        fields: schema
            .fields()
            .iter()
            .map(|field| SchemaField {
                name: field.name().to_string(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}

fn record_batches_to_json_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<Vec<serde_json::Value>, QueryError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut buf = Vec::new();
    {
        let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch).map_err(query_execution_error)?;
        }
        writer.finish().map_err(query_execution_error)?;
    }

    let json_str = String::from_utf8(buf).map_err(query_execution_error)?;
    let val: serde_json::Value = serde_json::from_str(&json_str).map_err(query_execution_error)?;
    Ok(val.as_array().cloned().unwrap_or_default())
}

fn file_extension_filter(file_path: &str, default_extension: &str) -> String {
    std::path::Path::new(file_path)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| format!(".{extension}"))
        .unwrap_or_else(|| default_extension.to_string())
}

impl Default for QsqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct LineageInfo {
    pub table_name: String,
    pub columns: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct QueryLineage {
    pub tables: Vec<String>,
    pub relations: Vec<LineageInfo>,
}

pub fn arrow_schema_to_qsql_schema(schema: &datafusion::arrow::datatypes::Schema) -> QsqlSchema {
    QsqlSchema {
        fields: schema
            .fields()
            .iter()
            .map(|field| SchemaField {
                name: field.name().to_string(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_csv() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "test_qsql_emp_{}_{}.csv",
            std::process::id(),
            nanos
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "id,name,department,salary").unwrap();
        writeln!(file, "1,Alice,Engineering,100000").unwrap();
        writeln!(file, "2,Bob,Sales,80000").unwrap();
        path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_engine_lifecycle() {
        let engine = QsqlEngine::new();
        let res = engine
            .execute_sql_to_string("SELECT * FROM non_existent")
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_query_lineage_simple() {
        let engine = QsqlEngine::new();
        let csv_path = create_temp_csv();
        engine
            .register_file("employees", &csv_path, "csv")
            .await
            .unwrap();

        let lineage = engine
            .get_query_lineage("SELECT name, salary FROM employees")
            .await
            .unwrap();
        assert_eq!(lineage.tables, vec!["employees".to_string()]);
        assert_eq!(lineage.relations.len(), 1);
        assert_eq!(lineage.relations[0].table_name, "employees");
        assert_eq!(
            lineage.relations[0].columns,
            vec!["name".to_string(), "salary".to_string()]
        );

        // Clean up temp file
        let _ = std::fs::remove_file(csv_path);
    }

    #[tokio::test]
    async fn test_query_lineage_errors() {
        let engine = QsqlEngine::new();
        let res = engine
            .get_query_lineage("SELECT name FROM non_existent")
            .await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("non_existent"));
    }

    #[tokio::test]
    async fn test_execute_sql_to_page_includes_schema_and_metadata() {
        let engine = QsqlEngine::new();
        let csv_path = create_temp_csv();
        engine
            .register_file("employees", &csv_path, "csv")
            .await
            .unwrap();

        let page = engine
            .execute_sql_to_page(
                "q_test",
                "SELECT id, name FROM employees ORDER BY id",
                ExecutePageOptions {
                    page_index: 0,
                    page_size: 1,
                    warning: None,
                    cancellation_token: CancellationToken::new(),
                    timeout_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(page.query_id, "q_test");
        assert_eq!(page.page_index, 0);
        assert_eq!(page.page_size, 1);
        assert!(!page.is_last);
        assert_eq!(page.data.len(), 1);
        assert_eq!(page.schema.fields.len(), 2);
        assert_eq!(page.schema.fields[0].name, "id");
        assert_eq!(page.metrics.rows_produced, 2);
        assert_eq!(page.metrics.rows_returned, 1);

        let _ = std::fs::remove_file(csv_path);
    }

    #[tokio::test]
    async fn test_execute_sql_to_page_clamps_large_page_size() {
        let engine = QsqlEngine::new();
        let page = engine
            .execute_sql_to_page(
                "q_clamp",
                "SELECT 1 AS value",
                ExecutePageOptions {
                    page_index: 0,
                    page_size: crate::models::MAX_PAGE_SIZE + 1,
                    warning: None,
                    cancellation_token: CancellationToken::new(),
                    timeout_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(page.page_size, crate::models::MAX_PAGE_SIZE);
        assert!(page.warning.unwrap().contains("exceeded the maximum"));
    }

    #[tokio::test]
    async fn test_execute_sql_to_page_empty_result_is_last() {
        let engine = QsqlEngine::new();
        let page = engine
            .execute_sql_to_page(
                "q_empty",
                "SELECT 1 AS value WHERE false",
                ExecutePageOptions {
                    page_index: 0,
                    page_size: 100,
                    warning: None,
                    cancellation_token: CancellationToken::new(),
                    timeout_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(page.data.len(), 0);
        assert!(page.is_last);
        assert_eq!(page.metrics.rows_produced, 0);
    }

    #[tokio::test]
    async fn test_execute_sql_to_page_returns_cancellation_error() {
        let engine = QsqlEngine::new();
        let token = CancellationToken::new();
        token.cancel();

        let err = engine
            .execute_sql_to_page(
                "q_cancel",
                "SELECT 1",
                ExecutePageOptions {
                    page_index: 0,
                    page_size: 100,
                    warning: None,
                    cancellation_token: token,
                    timeout_ms: None,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, -32002);
        assert_eq!(err.message, "Query cancelled");
    }
}
