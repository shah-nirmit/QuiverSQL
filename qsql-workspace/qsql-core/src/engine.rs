use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::{MemorySchemaProvider, Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Statistics, TableReference};
use datafusion::error::DataFusionError;
use datafusion::execution::options::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, TableProviderFilterPushDown, TableType,
};
use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::*;
use datafusion_federation::{FederatedPlanNode, FederatedQueryPlanner};
use futures::StreamExt;
use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::broadcast::{apply_broadcast_rewrites, BroadcastRewriteConfig, BroadcastRewriteInfo};
use crate::models::{
    normalize_page_size, CatalogSource, PerformanceMetrics, QueryError, QueryExecutionResult,
    QueryPage, Schema as QsqlSchema, SchemaField,
};

const MAX_BUFFERED_RESULT_ROWS: usize = 100_000;
pub const DEFAULT_QUERY_MEMORY_LIMIT_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_REMOTE_SCAN_MAX_ROWS: usize = 1_000_000;
pub const DEFAULT_REMOTE_SCAN_MAX_BYTES: usize = 1024 * 1024 * 1024;

pub fn get_query_memory_limit_bytes() -> usize {
    crate::models::get_env_usize(
        "QSQL_QUERY_MEMORY_LIMIT_BYTES",
        DEFAULT_QUERY_MEMORY_LIMIT_BYTES,
    )
}
pub fn get_remote_scan_max_rows() -> usize {
    crate::models::get_env_usize("QSQL_REMOTE_SCAN_MAX_ROWS", DEFAULT_REMOTE_SCAN_MAX_ROWS)
}
pub fn get_remote_scan_max_bytes() -> usize {
    crate::models::get_env_usize("QSQL_REMOTE_SCAN_MAX_BYTES", DEFAULT_REMOTE_SCAN_MAX_BYTES)
}
pub fn get_max_buffered_result_rows() -> usize {
    crate::models::get_env_usize("QSQL_MAX_BUFFERED_RESULT_ROWS", MAX_BUFFERED_RESULT_ROWS)
}
pub fn get_max_buffered_result_bytes() -> usize {
    crate::models::get_env_usize(
        "QSQL_MAX_BUFFERED_RESULT_BYTES",
        DEFAULT_QUERY_MEMORY_LIMIT_BYTES,
    )
}

pub struct QsqlEngine {
    runtime: Arc<RuntimeEnv>,
    pub catalog: std::sync::Arc<RwLock<std::collections::HashMap<String, CatalogSource>>>,
    table_registry: std::sync::Arc<
        RwLock<std::collections::HashMap<RegisteredTableRef, Arc<dyn TableProvider>>>,
    >,
    broadcast_config: BroadcastRewriteConfig,
}

pub struct ExecutePageOptions {
    pub page_index: usize,
    pub page_size: usize,
    pub warning: Option<String>,
    pub cancellation_token: CancellationToken,
    pub timeout_ms: Option<u64>,
}

pub struct QueryResultHandle {
    schema: QsqlSchema,
    stream: SendableRecordBatchStream,
    batches: std::collections::VecDeque<RecordBatch>,
    buffered_rows: usize,
    buffered_bytes: usize,
    terminal: bool,
    planning_time_ms: u64,
    execution_start: Instant,
    first_page_time_ms: Option<u64>,
    broadcast_info: BroadcastRewriteInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegisteredTableRef {
    schema: Option<String>,
    table: String,
}

impl RegisteredTableRef {
    fn bare(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
        }
    }

    fn partial(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            table: table.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for ScanBudget {
    fn default() -> Self {
        Self {
            max_rows: get_remote_scan_max_rows(),
            max_bytes: get_remote_scan_max_bytes(),
        }
    }
}

#[derive(Clone)]
pub struct GuardedTableProvider {
    source_ref: String,
    inner: Arc<dyn TableProvider>,
    budget: ScanBudget,
}

impl fmt::Debug for GuardedTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuardedTableProvider")
            .field("source_ref", &self.source_ref)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl GuardedTableProvider {
    pub fn new(source_ref: impl Into<String>, inner: Arc<dyn TableProvider>) -> Self {
        Self {
            source_ref: source_ref.into(),
            inner,
            budget: ScanBudget::default(),
        }
    }

    pub fn with_budget(
        source_ref: impl Into<String>,
        inner: Arc<dyn TableProvider>,
        budget: ScanBudget,
    ) -> Self {
        Self {
            source_ref: source_ref.into(),
            inner,
            budget,
        }
    }

    pub fn source_ref(&self) -> &str {
        &self.source_ref
    }

    pub fn budget(&self) -> &ScanBudget {
        &self.budget
    }

    pub fn inner(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.inner)
    }

    fn check_budget(&self, limit: Option<usize>) -> Result<(), DataFusionError> {
        let Some(stats) = self.inner.statistics() else {
            return Ok(());
        };

        let effective_rows = stats
            .num_rows
            .get_value()
            .copied()
            .map(|rows| limit.map_or(rows, |limit| rows.min(limit)));
        if effective_rows.is_some_and(|rows| rows > self.budget.max_rows) {
            return Err(scan_budget_error(
                &self.source_ref,
                "rows",
                effective_rows.unwrap(),
                self.budget.max_rows,
            ));
        }

        let effective_bytes = estimate_effective_bytes(&stats, limit);
        if effective_bytes.is_some_and(|bytes| bytes > self.budget.max_bytes) {
            return Err(scan_budget_error(
                &self.source_ref,
                "bytes",
                effective_bytes.unwrap(),
                self.budget.max_bytes,
            ));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl TableProvider for GuardedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.check_budget(limit)?;
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

impl QueryResultHandle {
    /// Access the broadcast-rewrite outcome captured during planning. Returns
    /// a default (empty) record when the rewrite was disabled or when no
    /// eligible join was inspected.
    pub fn broadcast_info(&self) -> &BroadcastRewriteInfo {
        &self.broadcast_info
    }

    pub async fn page(
        &mut self,
        query_id: impl Into<String>,
        page_index: usize,
        page_size: usize,
        warning: Option<String>,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<QueryPage, QueryError> {
        let query_id = query_id.into();
        let end = page_index.saturating_add(1).saturating_mul(page_size);
        let read_target = end.saturating_add(1);
        self.read_until(read_target, cancellation_token, timeout_ms)
            .await?;

        let start = page_index.saturating_mul(page_size);
        let end = end.min(self.buffered_rows);
        let data = if start >= self.buffered_rows {
            Vec::new()
        } else {
            record_batches_to_json_rows(&self.batches, start, end.saturating_sub(start))?
        };

        if page_index == 0 && self.first_page_time_ms.is_none() {
            self.first_page_time_ms =
                Some(elapsed_ms(self.execution_start) + self.planning_time_ms);
        }

        let rows_produced = self.buffered_rows as u64;
        let first_page_time_ms = self
            .first_page_time_ms
            .unwrap_or_else(|| elapsed_ms(self.execution_start) + self.planning_time_ms);

        Ok(QueryPage {
            query_id,
            schema: self.schema.clone(),
            page_index,
            page_size,
            is_last: self.terminal && end >= self.buffered_rows,
            data,
            metrics: PerformanceMetrics {
                planning_time_ms: self.planning_time_ms,
                execution_time_ms: elapsed_ms(self.execution_start),
                first_page_time_ms,
                rows_produced,
                rows_returned: end.saturating_sub(start) as u64,
            },
            warning,
        })
    }

    pub async fn collect_all(
        mut self,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<QueryExecutionResult, QueryError> {
        self.read_to_end(cancellation_token, timeout_ms).await?;
        let rows = record_batches_to_json_rows(&self.batches, 0, self.buffered_rows)?;
        let rows_produced = rows.len() as u64;
        Ok(QueryExecutionResult {
            schema: self.schema,
            data: rows,
            metrics: PerformanceMetrics {
                planning_time_ms: self.planning_time_ms,
                execution_time_ms: elapsed_ms(self.execution_start),
                first_page_time_ms: self
                    .first_page_time_ms
                    .unwrap_or_else(|| elapsed_ms(self.execution_start) + self.planning_time_ms),
                rows_produced,
                rows_returned: rows_produced,
            },
        })
    }

    pub async fn collect_batches(
        mut self,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        self.read_to_end(cancellation_token, timeout_ms).await?;
        Ok(self.batches.into_iter().collect())
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    async fn read_until(
        &mut self,
        target_rows: usize,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<(), QueryError> {
        if cancellation_token.is_cancelled() {
            return Err(query_cancelled_error());
        }

        if timeout_ms == Some(0) {
            return Err(query_timeout_error(0));
        }

        let read = async {
            while !self.terminal && self.buffered_rows < target_rows {
                tokio::select! {
                    _ = cancellation_token.cancelled() => return Err(query_cancelled_error()),
                    batch = self.stream.next() => {
                        match batch {
                            Some(Ok(batch)) => self.push_batch(&batch)?,
                            Some(Err(error)) => return Err(query_execution_error(error)),
                            None => self.terminal = true,
                        }
                    }
                }
            }
            Ok(())
        };

        match timeout_ms {
            Some(timeout_ms) => {
                match tokio::time::timeout(Duration::from_millis(timeout_ms), read).await {
                    Ok(result) => result,
                    Err(_) => Err(query_timeout_error(timeout_ms)),
                }
            }
            None => read.await,
        }
    }

    async fn read_to_end(
        &mut self,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<(), QueryError> {
        self.read_until(usize::MAX, cancellation_token, timeout_ms)
            .await
    }

    fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), QueryError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.buffered_rows = self.buffered_rows.saturating_add(batch.num_rows());
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(batch.get_array_memory_size());
        let max_rows = get_max_buffered_result_rows();
        let max_bytes = get_max_buffered_result_bytes();
        if self.buffered_rows > max_rows {
            return Err(resource_limit_error(format!(
                "Result buffer exceeded {max_rows} rows; request a smaller page window or add a LIMIT/filter."
            )));
        }
        if self.buffered_bytes > max_bytes {
            return Err(resource_limit_error(format!(
                "Result buffer exceeded {max_bytes} bytes; request a smaller page window or add a LIMIT/filter."
            )));
        }
        self.batches.push_back(batch.clone());
        Ok(())
    }
}

impl QsqlEngine {
    pub fn get_catalog(&self) -> Vec<CatalogSource> {
        let catalog = self.catalog.read().unwrap();
        catalog.values().cloned().collect()
    }

    pub fn get_source_metadata(&self, name: &str) -> Option<CatalogSource> {
        let catalog = self.catalog.read().unwrap();
        catalog.get(name).cloned()
    }

    pub fn catalog_source(&self, source: CatalogSource) {
        let mut catalog = self.catalog.write().unwrap();
        catalog.insert(source.name.clone(), source);
    }

    pub fn remove_source(&self, name: &str) -> Result<bool, String> {
        let source = {
            let catalog = self.catalog.read().unwrap();
            catalog.get(name).cloned()
        };
        let deregistered = if source.as_ref().is_some_and(is_database_source) {
            self.deregister_schema(name)?
        } else {
            self.deregister_table(name)
        };
        let mut catalog = self.catalog.write().unwrap();
        let removed_from_catalog = catalog.remove(name).is_some();
        Ok(deregistered || removed_from_catalog)
    }

    pub fn new() -> Self {
        let runtime = qsql_runtime_env();
        Self {
            runtime,
            catalog: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            table_registry: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            broadcast_config: BroadcastRewriteConfig::default(),
        }
    }

    /// Override the broadcast-join rewrite configuration. Used by benches and
    /// integration tests to disable the rewrite for parity comparisons.
    pub fn with_broadcast_config(mut self, config: BroadcastRewriteConfig) -> Self {
        self.broadcast_config = config;
        self
    }

    pub fn broadcast_config(&self) -> &BroadcastRewriteConfig {
        &self.broadcast_config
    }

    fn execution_context(&self) -> Result<SessionContext, String> {
        let ctx = SessionContext::new_with_state(qsql_session_state(self.runtime.clone()));
        let snapshot = self
            .table_registry
            .read()
            .unwrap()
            .iter()
            .map(|(table_ref, provider)| (table_ref.clone(), Arc::clone(provider)))
            .collect::<Vec<_>>();

        for (table_ref, provider) in snapshot {
            match table_ref.schema {
                Some(schema) => {
                    ensure_schema_in_context(&ctx, &schema)?;
                    ctx.register_table(
                        TableReference::partial(schema, table_ref.table.clone()),
                        provider,
                    )
                    .map_err(|e| {
                        format!(
                            "Failed to register table snapshot '{}': {}",
                            table_ref.table, e
                        )
                    })?;
                }
                None => {
                    ctx.register_table(table_ref.table.clone(), provider)
                        .map_err(|e| {
                            format!(
                                "Failed to register table snapshot '{}': {}",
                                table_ref.table, e
                            )
                        })?;
                }
            }
        }

        Ok(ctx)
    }

    /// Executes a SQL query and returns the pretty-printed result as a string.
    pub async fn execute_sql_to_string(&self, sql: &str) -> Result<String, String> {
        let batches = self
            .start_query_stream(sql, CancellationToken::new(), None)
            .await
            .map_err(|e| e.message)?
            .collect_batches(CancellationToken::new(), None)
            .await
            .map_err(|e| e.message)?;

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
        let result = self
            .execute_sql_collect(sql, CancellationToken::new(), None)
            .await
            .map_err(|e| e.message)?;
        Ok(serde_json::Value::Array(result.data))
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
        let mut handle = self
            .start_query_stream(sql, options.cancellation_token.clone(), options.timeout_ms)
            .await?;
        handle
            .page(
                query_id.to_string(),
                options.page_index,
                page_size,
                warning,
                options.cancellation_token,
                options.timeout_ms,
            )
            .await
    }

    /// Executes a SQL query and returns all rows plus schema/metrics.
    /// Compatibility wrapper around the streaming execution path.
    pub async fn execute_sql_collect(
        &self,
        sql: &str,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<QueryExecutionResult, QueryError> {
        let handle = self
            .start_query_stream(sql, cancellation_token.clone(), timeout_ms)
            .await?;
        handle.collect_all(cancellation_token, timeout_ms).await
    }

    pub async fn start_query_stream(
        &self,
        sql: &str,
        cancellation_token: CancellationToken,
        timeout_ms: Option<u64>,
    ) -> Result<QueryResultHandle, QueryError> {
        if cancellation_token.is_cancelled() {
            return Err(query_cancelled_error());
        }

        if timeout_ms == Some(0) {
            return Err(query_timeout_error(0));
        }

        let cancellation_for_planning = cancellation_token.clone();
        let planning = async {
            let planning_start = Instant::now();
            let ctx = self.execution_context().map_err(query_execution_error)?;
            // Build, optimize, rewrite, re-optimize on rewrite, then plan
            // physically. The rewrite step is a no-op when broadcast is
            // disabled or when no eligible join is found.
            let logical = ctx
                .state()
                .create_logical_plan(sql)
                .await
                .map_err(query_execution_error)?;
            let optimized = ctx
                .state()
                .optimize(&logical)
                .map_err(query_execution_error)?;
            let (rewritten, broadcast_info) = apply_broadcast_rewrites(
                &ctx,
                optimized,
                &self.broadcast_config,
                cancellation_for_planning,
            )
            .await
            .map_err(query_execution_error)?;
            let final_plan = if broadcast_info.applied.is_empty() {
                rewritten
            } else {
                // Re-optimize so the injected IN-list filters get pushed into
                // the federated scans by DataFusion's normal pushdown pass.
                ctx.state()
                    .optimize(&rewritten)
                    .map_err(query_execution_error)?
            };
            let df = DataFrame::new(ctx.state(), final_plan);
            let schema = dataframe_schema_to_qsql_schema(df.schema());
            let stream = df.execute_stream().await.map_err(query_execution_error)?;
            let planning_time_ms = elapsed_ms(planning_start);
            Ok(QueryResultHandle {
                schema,
                stream,
                batches: std::collections::VecDeque::new(),
                buffered_rows: 0,
                buffered_bytes: 0,
                terminal: false,
                planning_time_ms,
                execution_start: Instant::now(),
                first_page_time_ms: None,
                broadcast_info,
            })
        };

        match timeout_ms {
            Some(timeout_ms) => {
                tokio::select! {
                    _ = cancellation_token.cancelled() => Err(query_cancelled_error()),
                    result = tokio::time::timeout(Duration::from_millis(timeout_ms), planning) => {
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
                    result = planning => result,
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

        let ctx = SessionContext::new_with_state(qsql_session_state(self.runtime.clone()));

        match kind {
            crate::models::SourceKind::Csv => {
                ctx.register_csv(table_name, file_path, CsvReadOptions::new())
                    .await
                    .map_err(|e| format!("Failed to register CSV: {}", e))?;
            }
            crate::models::SourceKind::Parquet => {
                ctx.register_parquet(table_name, file_path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| format!("Failed to register Parquet: {}", e))?;
            }
            crate::models::SourceKind::Json | crate::models::SourceKind::Ndjson => {
                let file_extension = file_extension_filter(file_path, ".json");
                ctx.register_json(
                    table_name,
                    file_path,
                    JsonReadOptions::default().file_extension(&file_extension),
                )
                .await
                .map_err(|e| format!("Failed to register JSON: {}", e))?;
            }
            _ => unreachable!(),
        }

        let provider = ctx
            .table_provider(table_name)
            .await
            .map_err(|e| e.to_string())?;
        let provider_schema = provider.schema();
        let qsql_schema = arrow_schema_to_qsql_schema(provider_schema.as_ref());

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
            tables: None,
        };
        self.catalog_source(source);
        self.register_table_entry(RegisteredTableRef::bare(table_name), provider);

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
        provider: Arc<dyn TableProvider>,
    ) -> Result<String, String> {
        self.register_table_entry(RegisteredTableRef::bare(table_name), provider);
        Ok(format!(
            "Successfully registered '{}' as a federated table.",
            table_name
        ))
    }

    pub fn register_schema_table(
        &self,
        schema_name: &str,
        table_name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<String, String> {
        if self.table_registered_in_schema(schema_name, table_name) {
            return Ok(format!(
                "Table '{}.{}' is already registered.",
                schema_name, table_name
            ));
        }
        let guarded = Arc::new(GuardedTableProvider::new(
            format!("{schema_name}.{table_name}"),
            provider,
        )) as Arc<dyn TableProvider>;
        self.register_table_entry(
            RegisteredTableRef::partial(schema_name, table_name),
            guarded,
        );
        Ok(format!(
            "Successfully registered '{}.{}' as a federated table.",
            schema_name, table_name
        ))
    }

    pub fn table_registered_in_schema(&self, schema_name: &str, table_name: &str) -> bool {
        self.table_registry
            .read()
            .unwrap()
            .contains_key(&RegisteredTableRef::partial(schema_name, table_name))
    }

    fn register_table_entry(
        &self,
        table_ref: RegisteredTableRef,
        provider: Arc<dyn TableProvider>,
    ) {
        self.table_registry
            .write()
            .unwrap()
            .insert(table_ref, provider);
    }

    fn deregister_schema(&self, schema_name: &str) -> Result<bool, String> {
        let mut registry = self.table_registry.write().unwrap();
        let before = registry.len();
        registry.retain(|table_ref, _| table_ref.schema.as_deref() != Some(schema_name));
        Ok(before != registry.len())
    }

    fn deregister_table(&self, table_name: &str) -> bool {
        self.table_registry
            .write()
            .unwrap()
            .remove(&RegisteredTableRef::bare(table_name))
            .is_some()
    }

    /// Returns the optimized logical plan without running the broadcast
    /// rewrite. Kept for callers (lineage, legacy explain paths) that only
    /// care about the unrewritten optimizer output. New callers that need
    /// the same view executed at query time should use
    /// [`Self::get_logical_plan_with_broadcast`].
    pub async fn get_logical_plan(
        &self,
        sql: &str,
    ) -> Result<datafusion::logical_expr::LogicalPlan, String> {
        let ctx = self.execution_context()?;
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;
        ctx.state().optimize(&plan).map_err(|e| e.to_string())
    }

    /// Returns the optimized logical plan AFTER the broadcast rewrite has
    /// been applied (and, if any joins were rewritten, after a second
    /// optimization pass that pushes the injected IN-list filters into the
    /// federated scans). Also returns the structured [`BroadcastRewriteInfo`]
    /// that explain/metrics surfaces consume.
    pub async fn get_logical_plan_with_broadcast(
        &self,
        sql: &str,
    ) -> Result<(datafusion::logical_expr::LogicalPlan, BroadcastRewriteInfo), String> {
        let ctx = self.execution_context()?;
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;
        let optimized = ctx.state().optimize(&plan).map_err(|e| e.to_string())?;
        let (rewritten, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &self.broadcast_config,
            CancellationToken::new(),
        )
        .await
        .map_err(|e| e.to_string())?;
        let final_plan = if info.applied.is_empty() {
            rewritten
        } else {
            ctx.state()
                .optimize(&rewritten)
                .map_err(|e| e.to_string())?
        };
        Ok((final_plan, info))
    }

    /// Produces the DataFusion physical plan for a logical plan that was
    /// already optimized + broadcast-rewritten. The physical plan is where
    /// `datafusion-federation` materialises the actual SQL string sent to each
    /// remote DBMS (embedded in the `fmt_as` of `VirtualExecutionPlan` /
    /// `SqlExec` leaves), which the explain endpoint then scrapes to surface
    /// the real pushed-down SQL — not the placeholder `SELECT *` we used
    /// before.
    pub async fn create_physical_plan_for_explain(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, String> {
        let ctx = self.execution_context()?;
        ctx.state()
            .create_physical_plan(plan)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn get_query_lineage(&self, sql: &str) -> Result<QueryLineage, String> {
        let ctx = self.execution_context()?;
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;
        let plan = ctx.state().optimize(&plan).map_err(|e| e.to_string())?;

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

fn ensure_schema_in_context(ctx: &SessionContext, schema_name: &str) -> Result<(), String> {
    let catalog_name = ctx
        .state()
        .config()
        .options()
        .catalog
        .default_catalog
        .clone();
    let catalog = ctx
        .catalog(&catalog_name)
        .ok_or_else(|| format!("Default catalog '{catalog_name}' not found"))?;

    if catalog.schema(schema_name).is_none() {
        catalog
            .register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))
            .map_err(|e| format!("Failed to register schema '{}': {}", schema_name, e))?;
    }

    Ok(())
}

fn qsql_session_state(runtime: Arc<RuntimeEnv>) -> SessionState {
    let mut rules = datafusion_federation::default_optimizer_rules();
    let guard = Arc::new(SingleScanFederationGuard);
    if let Some(pos) = rules
        .iter()
        .position(|rule| rule.name() == "federation_optimizer_rule")
    {
        rules.insert(pos + 1, guard);
    } else {
        rules.push(guard);
    }

    SessionStateBuilder::new()
        .with_runtime_env(runtime)
        .with_optimizer_rules(rules)
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
        .with_default_features()
        .build()
}

fn qsql_runtime_env() -> Arc<RuntimeEnv> {
    Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit(get_query_memory_limit_bytes(), 1.0)
            .build()
            .expect("QuiverSQL runtime env should initialize"),
    )
}

#[derive(Debug)]
struct SingleScanFederationGuard;

impl OptimizerRule for SingleScanFederationGuard {
    fn name(&self) -> &str {
        "qsql_single_scan_federation_guard"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Extension(Extension { node }) = &plan {
            if let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>() {
                if count_table_scans(federated.plan()) <= 1 {
                    return Ok(Transformed::yes(federated.plan().clone()));
                }
            }
        }
        Ok(Transformed::no(plan))
    }
}

fn count_table_scans(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::TableScan(_) => 1,
        _ => plan
            .inputs()
            .iter()
            .map(|input| count_table_scans(input))
            .sum(),
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn is_database_source(source: &CatalogSource) -> bool {
    matches!(
        source.kind,
        crate::models::SourceKind::Sqlite
            | crate::models::SourceKind::Postgres
            | crate::models::SourceKind::Mysql
            | crate::models::SourceKind::Mariadb
    ) && source.tables.is_some()
}

const SCAN_GUARD_SENTINEL: &str = "[QSQL_SCAN_GUARD] ";

fn query_execution_error(error: impl ToString) -> QueryError {
    let msg = error.to_string();
    if let Some(clean) = msg.strip_prefix(SCAN_GUARD_SENTINEL) {
        QueryError {
            code: crate::models::SCAN_GUARD_ERROR_CODE,
            message: clean.to_string(),
            details: Some("scan_guard".to_string()),
        }
    } else {
        QueryError {
            code: -32001,
            message: msg,
            details: None,
        }
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

fn resource_limit_error(message: String) -> QueryError {
    QueryError {
        code: -32020,
        message,
        details: Some("resource_limit".to_string()),
    }
}

fn estimate_effective_bytes(stats: &Statistics, limit: Option<usize>) -> Option<usize> {
    let bytes = stats.total_byte_size.get_value().copied()?;
    let rows = stats.num_rows.get_value().copied()?;
    let Some(limit) = limit else {
        return Some(bytes);
    };
    if rows == 0 || rows <= limit {
        return Some(bytes);
    }

    Some(bytes.saturating_mul(limit) / rows)
}

fn scan_budget_error(
    source_ref: &str,
    metric: &str,
    estimated: usize,
    limit: usize,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[QSQL_SCAN_GUARD] Remote scan '{source_ref}' estimated {estimated} {metric}, exceeding the configured budget of {limit} {metric}. Add a LIMIT, use tighter filters, or raise the source scan budget."
    ))
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
    batches: &std::collections::VecDeque<RecordBatch>,
    start_row: usize,
    row_count: usize,
) -> Result<Vec<serde_json::Value>, QueryError> {
    if row_count == 0 {
        return Ok(Vec::new());
    }

    let mut remaining_skip = start_row;
    let mut remaining_take = row_count;
    let mut rows = Vec::with_capacity(row_count);

    for batch in batches {
        if remaining_take == 0 {
            break;
        }

        let batch_rows = batch.num_rows();
        if remaining_skip >= batch_rows {
            remaining_skip -= batch_rows;
            continue;
        }

        let offset = remaining_skip;
        let length = remaining_take.min(batch_rows - offset);
        let slice = batch.slice(offset, length);
        rows.extend(record_batch_to_json_rows(&slice)?);
        remaining_skip = 0;
        remaining_take -= length;
    }

    Ok(rows)
}

fn record_batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, QueryError> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_index in 0..batch.num_rows() {
        let mut obj = serde_json::Map::with_capacity(batch.num_columns());
        for (column_index, field) in schema.fields().iter().enumerate() {
            let column = batch.column(column_index).as_ref();
            obj.insert(
                field.name().clone(),
                array_value_to_json(column, row_index).map_err(query_execution_error)?,
            );
        }
        rows.push(serde_json::Value::Object(obj));
    }

    Ok(rows)
}

fn array_value_to_json(
    array: &dyn Array,
    row_index: usize,
) -> Result<serde_json::Value, datafusion::arrow::error::ArrowError> {
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }

    macro_rules! downcast_number {
        ($array:expr, $row:expr, $ty:ty) => {
            if let Some(values) = $array.as_any().downcast_ref::<$ty>() {
                return Ok(serde_json::json!(values.value($row)));
            }
        };
    }

    downcast_number!(array, row_index, Int8Array);
    downcast_number!(array, row_index, Int16Array);
    downcast_number!(array, row_index, Int32Array);
    downcast_number!(array, row_index, Int64Array);
    downcast_number!(array, row_index, UInt8Array);
    downcast_number!(array, row_index, UInt16Array);
    downcast_number!(array, row_index, UInt32Array);
    downcast_number!(array, row_index, UInt64Array);
    downcast_number!(array, row_index, Float32Array);
    downcast_number!(array, row_index, Float64Array);

    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(serde_json::Value::Bool(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(serde_json::Value::String(
            values.value(row_index).to_string(),
        ));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(serde_json::Value::String(
            values.value(row_index).to_string(),
        ));
    }

    Ok(serde_json::Value::String(array_value_to_string(
        array, row_index,
    )?))
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
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::MemTable;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug)]
    struct StatsOnlyProvider {
        schema: SchemaRef,
        stats: Statistics,
    }

    #[async_trait::async_trait]
    impl TableProvider for StatsOnlyProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            panic!("guard should reject before delegating to inner scan")
        }

        fn statistics(&self) -> Option<Statistics> {
            Some(self.stats.clone())
        }
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_streaming_queries_can_cancel_half_under_load() {
        let engine = Arc::new(QsqlEngine::new());
        let mut tasks = Vec::new();

        for task_index in 0..32 {
            let engine = Arc::clone(&engine);
            tasks.push(tokio::spawn(async move {
                let token = CancellationToken::new();
                let mut handle = engine
                    .start_query_stream(
                        "SELECT * FROM generate_series(1, 1000000) AS t(value)",
                        token.clone(),
                        Some(5_000),
                    )
                    .await?;

                if task_index % 2 == 0 {
                    token.cancel();
                    let err = handle
                        .page(
                            format!("q_cancel_{task_index}"),
                            0,
                            1_000,
                            None,
                            token,
                            Some(5_000),
                        )
                        .await
                        .unwrap_err();
                    return Ok::<_, QueryError>((true, err.code, 0));
                }

                let page = handle
                    .page(
                        format!("q_keep_{task_index}"),
                        0,
                        1_000,
                        None,
                        token,
                        Some(5_000),
                    )
                    .await?;
                Ok((false, 0, page.data.len()))
            }));
        }

        let mut cancelled = 0;
        let mut completed = 0;
        for task in tasks {
            let (was_cancelled, code, row_count) = task.await.unwrap().unwrap();
            if was_cancelled {
                assert_eq!(code, -32002);
                cancelled += 1;
            } else {
                assert_eq!(row_count, 1_000);
                completed += 1;
            }
        }

        assert_eq!(cancelled, 16);
        assert_eq!(completed, 16);
    }

    #[tokio::test]
    async fn test_execute_sql_to_page_does_not_materialize_all_batches() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let make_batch = |start: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(start..start + 1_000))],
            )
            .unwrap()
        };
        let provider = MemTable::try_new(
            schema.clone(),
            vec![vec![make_batch(0), make_batch(1_000), make_batch(2_000)]],
        )
        .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("big_rows", Arc::new(provider))
            .unwrap();

        let page = engine
            .execute_sql_to_page(
                "q_stream",
                "SELECT value FROM big_rows",
                ExecutePageOptions {
                    page_index: 0,
                    page_size: 10,
                    warning: None,
                    cancellation_token: CancellationToken::new(),
                    timeout_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(page.data.len(), 10);
        assert!(
            page.metrics.rows_produced < 3_000,
            "first page should not force full materialization"
        );
    }

    #[tokio::test]
    async fn test_streaming_result_row_guard_returns_structured_error() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let batches = (0..=MAX_BUFFERED_RESULT_ROWS / 1_000)
            .map(|batch_idx| {
                let start = (batch_idx * 1_000) as i64;
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(start..start + 1_000))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let provider = MemTable::try_new(schema, vec![batches]).unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("too_many_rows", Arc::new(provider))
            .unwrap();

        let err = engine
            .execute_sql_to_page(
                "q_guard",
                "SELECT value FROM too_many_rows",
                ExecutePageOptions {
                    page_index: MAX_BUFFERED_RESULT_ROWS / 1_000,
                    page_size: 1_000,
                    warning: None,
                    cancellation_token: CancellationToken::new(),
                    timeout_ms: None,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, -32020);
        assert_eq!(err.details.as_deref(), Some("resource_limit"));
        assert!(err.message.contains("Result buffer exceeded"));
    }

    #[tokio::test]
    async fn test_execution_context_is_snapshot_not_shared_mutation() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let make_provider = |value: i64| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values([value]))],
            )
            .unwrap();
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap())
        };

        let engine = QsqlEngine::new();
        engine.register_table("first", make_provider(1)).unwrap();
        let snapshot = engine.execution_context().unwrap();
        engine.register_table("second", make_provider(2)).unwrap();

        assert!(snapshot.table_provider("first").await.is_ok());
        assert!(snapshot.table_provider("second").await.is_err());

        let rows = engine
            .execute_sql_to_json("SELECT value FROM second")
            .await
            .unwrap();
        assert_eq!(rows[0]["value"], 2);
    }

    #[tokio::test]
    async fn guarded_table_provider_rejects_over_budget_scan_estimates() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let stats = Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(11));
        let inner = Arc::new(StatsOnlyProvider { schema, stats }) as Arc<dyn TableProvider>;
        let guarded = GuardedTableProvider::with_budget(
            "pg_local.orders",
            inner,
            ScanBudget {
                max_rows: 10,
                max_bytes: DEFAULT_REMOTE_SCAN_MAX_BYTES,
            },
        );
        let engine = QsqlEngine::new();
        let ctx = engine.execution_context().unwrap();
        let state = ctx.state();

        let error = guarded.scan(&state, None, &[], None).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("pg_local.orders"));
        assert!(message.contains("LIMIT"));
        assert!(message.contains("tighter filters"));
        assert!(message.contains("raise the source scan budget"));
    }

    #[tokio::test]
    async fn guarded_provider_accessors() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
            as Arc<dyn TableProvider>;

        // new() constructor
        let g = GuardedTableProvider::new("mysource", Arc::clone(&mem));
        assert_eq!(g.source_ref(), "mysource");
        // budget() returns default
        assert_eq!(g.budget().max_rows, DEFAULT_REMOTE_SCAN_MAX_ROWS);
        // inner() returns same provider
        let _ = g.inner();

        // with_budget constructor
        let g2 = GuardedTableProvider::with_budget(
            "s2",
            Arc::clone(&mem),
            ScanBudget {
                max_rows: 5,
                max_bytes: 100,
            },
        );
        assert_eq!(g2.budget().max_rows, 5);
    }

    #[tokio::test]
    async fn guarded_provider_scan_within_budget_passes() {
        // No statistics → budget check is a no-op
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
            as Arc<dyn TableProvider>;
        let g = GuardedTableProvider::new("s", mem);
        let engine = QsqlEngine::new();
        let ctx = engine.execution_context().unwrap();
        // MemTable has no statistics, so budget check is skipped — scan returns Ok
        let result = g.scan(&ctx.state(), None, &[], None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn engine_catalog_operations() {
        use crate::models::{CatalogSource, SourceKind};
        let engine = QsqlEngine::new();

        // get_catalog is empty initially
        assert!(engine.get_catalog().is_empty());
        assert!(engine.get_source_metadata("missing").is_none());

        // catalog_source and get_source_metadata
        let src = CatalogSource {
            name: "db1".to_string(),
            kind: SourceKind::Sqlite,
            connection_details: serde_json::json!({}),
            schema: None,
            capabilities: None,
            status: "ready".to_string(),
            error: None,
            tables: Some(vec!["t1".to_string()]),
        };
        engine.catalog_source(src.clone());
        assert_eq!(engine.get_catalog().len(), 1);
        assert!(engine.get_source_metadata("db1").is_some());

        // remove_source removes it
        let removed = engine.remove_source("db1").unwrap();
        assert!(removed);
        assert!(engine.get_catalog().is_empty());

        // remove non-existent returns false
        let not_removed = engine.remove_source("not_here").unwrap();
        assert!(!not_removed);
    }

    #[tokio::test]
    async fn engine_register_schema_table_and_table_registered() {
        let engine = QsqlEngine::new();
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
            as Arc<dyn TableProvider>;

        assert!(!engine.table_registered_in_schema("myschema", "mytable"));
        engine
            .register_schema_table("myschema", "mytable", mem)
            .unwrap();
        assert!(engine.table_registered_in_schema("myschema", "mytable"));

        // Registering again returns "already registered" message
        let schema2 = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(Int64Array::from(vec![2i64]))],
        )
        .unwrap();
        let mem2 = Arc::new(MemTable::try_new(schema2, vec![vec![batch2]]).unwrap())
            as Arc<dyn TableProvider>;
        let msg = engine
            .register_schema_table("myschema", "mytable", mem2)
            .unwrap();
        assert!(msg.contains("already registered"));
    }

    #[tokio::test]
    async fn engine_with_broadcast_config() {
        let cfg = BroadcastRewriteConfig::disabled();
        let engine = QsqlEngine::new().with_broadcast_config(cfg);
        assert!(!engine.broadcast_config().enabled);
    }

    #[tokio::test]
    async fn engine_default_is_same_as_new() {
        let _engine: QsqlEngine = QsqlEngine::default();
        // Just verifies Default impl compiles and doesn't panic
    }

    #[tokio::test]
    async fn engine_execute_sql_to_string_success() {
        let engine = QsqlEngine::new();
        let result = engine
            .execute_sql_to_string("SELECT 42 AS answer")
            .await
            .unwrap();
        assert!(result.contains("42"));
    }

    #[tokio::test]
    async fn engine_execute_sql_collect_success() {
        let engine = QsqlEngine::new();
        let result = engine
            .execute_sql_collect("SELECT 1 AS v", CancellationToken::new(), None)
            .await
            .unwrap();
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0]["v"], 1);
    }

    #[tokio::test]
    async fn start_query_stream_immediate_timeout_returns_error() {
        let engine = QsqlEngine::new();
        let result = engine
            .start_query_stream("SELECT 1", CancellationToken::new(), Some(0))
            .await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code, -32003);
    }

    #[tokio::test]
    async fn handle_is_terminal_after_collecting_all() {
        let engine = QsqlEngine::new();
        let mut handle = engine
            .start_query_stream("SELECT 1 AS v", CancellationToken::new(), None)
            .await
            .unwrap();
        assert!(!handle.is_terminal());
        let _ = handle
            .page("q", 0, 100, None, CancellationToken::new(), None)
            .await
            .unwrap();
        assert!(handle.is_terminal());
    }

    #[tokio::test]
    async fn handle_collect_batches_returns_record_batches() {
        let engine = QsqlEngine::new();
        let handle = engine
            .start_query_stream("SELECT 1 AS v", CancellationToken::new(), None)
            .await
            .unwrap();
        let batches = handle
            .collect_batches(CancellationToken::new(), None)
            .await
            .unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn get_logical_plan_with_broadcast_returns_info() {
        let engine = QsqlEngine::new();
        let csv_path = create_temp_csv();
        engine.register_file("emp", &csv_path, "csv").await.unwrap();
        let (plan, info) = engine
            .get_logical_plan_with_broadcast("SELECT id FROM emp")
            .await
            .unwrap();
        let _ = plan;
        // No join, so considered=0
        assert_eq!(info.considered, 0);
        let _ = std::fs::remove_file(csv_path);
    }

    #[tokio::test]
    async fn register_file_unsupported_format_returns_error() {
        let engine = QsqlEngine::new();
        let err = engine
            .register_file("t", "/tmp/f.xyz", "xml")
            .await
            .unwrap_err();
        assert!(err.contains("Unsupported format"));
    }

    #[tokio::test]
    async fn file_extension_filter_with_and_without_extension() {
        // Has extension
        assert_eq!(file_extension_filter("data.json", ".ndjson"), ".json");
        // No extension fallback
        assert_eq!(file_extension_filter("data", ".csv"), ".csv");
    }

    #[test]
    fn arrow_schema_to_qsql_schema_roundtrip() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let qsql = arrow_schema_to_qsql_schema(&schema);
        assert_eq!(qsql.fields.len(), 2);
        assert_eq!(qsql.fields[0].name, "id");
        assert!(!qsql.fields[0].nullable);
        assert_eq!(qsql.fields[1].name, "name");
        assert!(qsql.fields[1].nullable);
    }

    #[test]
    fn estimate_effective_bytes_branches() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::stats::Precision;
        use datafusion::common::Statistics;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // No byte stats → None
        let stats_no_bytes = Statistics::new_unknown(&schema);
        assert!(estimate_effective_bytes(&stats_no_bytes, None).is_none());

        // Both rows and bytes known, no limit
        let stats = Statistics::new_unknown(&schema)
            .with_num_rows(Precision::Exact(100))
            .with_total_byte_size(Precision::Exact(1_000));
        assert_eq!(estimate_effective_bytes(&stats, None), Some(1_000));

        // rows=0 (div by zero guard)
        let stats_zero = Statistics::new_unknown(&schema)
            .with_num_rows(Precision::Exact(0))
            .with_total_byte_size(Precision::Exact(1_000));
        assert_eq!(estimate_effective_bytes(&stats_zero, Some(10)), Some(1_000));

        // limit <= rows → proportional
        let limited = estimate_effective_bytes(&stats, Some(10));
        assert_eq!(limited, Some(100)); // 1000 * 10 / 100

        // limit >= rows → full bytes
        let full = estimate_effective_bytes(&stats, Some(200));
        assert_eq!(full, Some(1_000));
    }

    #[tokio::test]
    async fn guarded_table_provider_rejects_over_budget_byte_estimates() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let stats = Statistics::new_unknown(&schema)
            .with_num_rows(Precision::Exact(100))
            .with_total_byte_size(Precision::Exact(2_048));
        let inner = Arc::new(StatsOnlyProvider { schema, stats }) as Arc<dyn TableProvider>;
        let guarded = GuardedTableProvider::with_budget(
            "pg_local.orders",
            inner,
            ScanBudget {
                max_rows: DEFAULT_REMOTE_SCAN_MAX_ROWS,
                max_bytes: 1_024,
            },
        );
        let engine = QsqlEngine::new();
        let ctx = engine.execution_context().unwrap();
        let state = ctx.state();

        let error = guarded.scan(&state, None, &[], None).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("pg_local.orders"));
        assert!(message.contains("bytes"));
        assert!(message.contains("LIMIT"));
        assert!(message.contains("raise the source scan budget"));
    }
}
