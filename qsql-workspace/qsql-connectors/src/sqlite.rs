//! SQLite connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion::sql::unparser::dialect::{Dialect, SqliteDialect};
use datafusion::sql::TableReference;
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::get_schema;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::{JoinPushDown, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use datafusion_table_providers::sqlite::sql_table::SQLiteTable;
use datafusion_table_providers::sqlite::DynSqliteConnectionPool;
use futures::TryStreamExt;
use rusqlite::Connection;
use std::sync::Arc;
use std::time::Duration;

use crate::sql::{quote_identifier, sql_capabilities, sql_type_to_arrow, SqlDialectKind};
use crate::{ConnectorResult, RemoteConnector};

/// A connector to a local SQLite database file.
#[derive(Debug)]
pub struct SqliteConnector {
    db_path: String,
}

impl SqliteConnector {
    pub fn new(db_path: impl Into<String>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }
}

#[async_trait]
impl RemoteConnector for SqliteConnector {
    fn connector_type(&self) -> &'static str {
        "sqlite"
    }

    async fn table_provider(
        &self,
        _schema: Option<&str>,
        table: &str,
        cached_schema: Option<SchemaRef>,
    ) -> ConnectorResult<Arc<dyn TableProvider>> {
        let provider =
            SqliteTableProvider::try_new_with_schema(self.db_path.clone(), table, cached_schema)
                .await?;
        Ok(Arc::new(provider))
    }

    async fn explain_query(&self, sql: &str) -> ConnectorResult<String> {
        let conn = rusqlite::Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to get SQLite connection: {}", e))?;

        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let mut stmt = conn
            .prepare(&explain_sql)
            .map_err(|e| format!("Failed to prepare explain query: {}", e))?;
        let column_names = stmt
            .column_names()
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let id_idx = column_names
            .iter()
            .position(|name| name == "id")
            .unwrap_or(0);
        let parent_idx = column_names
            .iter()
            .position(|name| name == "parent")
            .unwrap_or(1);
        let detail_idx = column_names
            .iter()
            .position(|name| name == "detail")
            .unwrap_or(3);

        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Failed to execute explain query: {}", e))?;

        let mut result = String::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let id: i32 = row.get(id_idx).unwrap_or(0);
            let parent: i32 = row.get(parent_idx).unwrap_or(0);
            let detail: String = row.get(detail_idx).unwrap_or_default();
            result.push_str(&format!(
                "id: {}, parent: {}, detail: {}\n",
                id, parent, detail
            ));
        }

        Ok(result)
    }

    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities {
        sql_capabilities(SqlDialectKind::Sqlite)
    }

    async fn list_tables(
        &self,
        _schema: Option<&str>,
        limit: usize,
    ) -> ConnectorResult<Vec<String>> {
        self.list_tables_page(None, 0, limit).await
    }

    async fn list_tables_page(
        &self,
        _schema: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> ConnectorResult<Vec<String>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| format!("Failed to open SQLite DB '{}': {}", db_path, e))?;

            let sql = format!(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT {} OFFSET {}",
                limit.max(1),
                offset
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("Failed to prepare table list query: {}", e))?;

            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Failed to list SQLite tables: {}", e))?;

            let mut tables = Vec::new();
            for row in rows {
                tables.push(row.map_err(|e| format!("Row error: {}", e))?);
            }
            Ok(tables)
        })
        .await
        .map_err(|e| format!("Thread join error: {}", e))?
    }
}

/// A DataFusion `TableProvider` backed by a single SQLite table.
#[derive(Debug)]
pub struct SqliteTableProvider {
    connector: Arc<SqliteConnector>,
    inner: Arc<dyn TableProvider>,
    table_name: String,
}

impl SqliteTableProvider {
    pub async fn try_new(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        Self::try_new_with_schema(db_path, table_name, None).await
    }

    pub async fn try_new_with_schema(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
        schema: Option<SchemaRef>,
    ) -> Result<Self, String> {
        let db_path = db_path.into();
        let table_name = table_name.into();
        let schema = schema.unwrap_or(introspect_sqlite_schema(&db_path, &table_name)?);
        let connector = Arc::new(SqliteConnector::new(db_path));
        let pool = SqliteConnectionPool::new(
            connector.db_path(),
            Mode::File,
            JoinPushDown::AllowedFor(connector.db_path().to_string()),
            Vec::new(),
            Duration::from_millis(5000),
        )
        .await
        .map_err(|e| format!("Failed to create SQLite provider pool: {e}"))?;
        let pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);
        let table_ref = TableReference::bare(table_name.clone());
        let fallback = Arc::new(SQLiteTable::new_with_schema(
            &pool,
            schema.clone(),
            table_ref.clone(),
        )) as Arc<dyn TableProvider>;
        let executor = Arc::new(QsqlSqliteFederationExecutor {
            db_path: connector.db_path().to_string(),
            pool,
        });
        let federation_provider = Arc::new(SQLFederationProvider::new(executor));
        let table_source = Arc::new(SQLTableSource::new_with_schema(
            federation_provider,
            RemoteTableRef::from(table_ref),
            schema.clone(),
        ));
        let inner = Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            fallback,
        ));

        Ok(Self {
            connector,
            inner,
            table_name,
        })
    }

    pub fn connector(&self) -> &Arc<SqliteConnector> {
        &self.connector
    }

    pub fn native_select_sql(&self) -> String {
        format!(
            "SELECT * FROM {}",
            quote_identifier(&self.table_name, SqlDialectKind::Sqlite)
        )
    }
}

struct QsqlSqliteFederationExecutor {
    db_path: String,
    pool: Arc<DynSqliteConnectionPool>,
}

impl std::fmt::Debug for QsqlSqliteFederationExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QsqlSqliteFederationExecutor")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SQLExecutor for QsqlSqliteFederationExecutor {
    fn name(&self) -> &str {
        "sqlite"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.db_path.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let query = apply_physical_filters(query, filters)?;
        let stream = futures::stream::once(get_stream(
            Arc::clone(&self.pool),
            query,
            Arc::clone(&schema),
        ))
        .try_flatten();
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        ))
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        let connector = SqliteConnector::new(&self.db_path);
        connector
            .list_tables(None, 5000)
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let conn = self.pool.connect().await.map_err(to_execution_error)?;
        get_schema(conn, &TableReference::from(table_name))
            .await
            .map_err(to_execution_error)
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

fn apply_physical_filters(
    query: &str,
    filters: &[Arc<dyn PhysicalExpr>],
) -> datafusion::error::Result<String> {
    if filters.is_empty() {
        return Ok(query.to_string());
    }

    let mut conditions = Vec::new();
    for filter in filters {
        if filter
            .as_any()
            .is::<datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr>()
        {
            continue;
        }
        let condition = physical_expr_to_sql(filter).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Unsupported SQLite federation filter pushdown: {filter:?}"
            ))
        })?;
        conditions.push(condition);
    }
    if conditions.is_empty() {
        return Ok(query.to_string());
    }
    Ok(insert_where_clause(query, &conditions.join(" AND ")))
}

fn physical_expr_to_sql(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
    };

    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        return Some(quote_identifier(column.name(), SqlDialectKind::Sqlite));
    }
    if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
        return scalar_value_to_sql(literal.value());
    }
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let left = physical_expr_to_sql(binary.left())?;
        let right = physical_expr_to_sql(binary.right())?;
        let op = operator_to_sql(binary.op())?;
        return Some(format!("({left} {op} {right})"));
    }
    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        let inner = physical_expr_to_sql(is_null.arg())?;
        return Some(format!("{inner} IS NULL"));
    }
    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        let inner = physical_expr_to_sql(is_not_null.arg())?;
        return Some(format!("{inner} IS NOT NULL"));
    }
    if let Some(not) = expr.as_any().downcast_ref::<NotExpr>() {
        let inner = physical_expr_to_sql(not.arg())?;
        return Some(format!("NOT ({inner})"));
    }
    None
}

fn operator_to_sql(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::Eq => Some("="),
        Operator::NotEq => Some("<>"),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        Operator::And => Some("AND"),
        Operator::Or => Some("OR"),
        _ => None,
    }
}

fn scalar_value_to_sql(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(if *value { "TRUE" } else { "FALSE" }.into()),
        ScalarValue::Int8(Some(value)) => Some(value.to_string()),
        ScalarValue::Int16(Some(value)) => Some(value.to_string()),
        ScalarValue::Int32(Some(value)) => Some(value.to_string()),
        ScalarValue::Int64(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt16(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt32(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt64(Some(value)) => Some(value.to_string()),
        ScalarValue::Float32(Some(value)) => Some(value.to_string()),
        ScalarValue::Float64(Some(value)) => Some(value.to_string()),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Some(format!("'{}'", value.replace('\'', "''")))
        }
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Some("NULL".to_string()),
        _ => None,
    }
}

fn insert_where_clause(query: &str, conditions: &str) -> String {
    let upper = query.to_uppercase();
    if let Some(where_pos) = upper.find(" WHERE ") {
        let after_where = where_pos + 7;
        let next_clause_pos = [" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "]
            .iter()
            .filter_map(|keyword| {
                upper[after_where..]
                    .find(keyword)
                    .map(|pos| after_where + pos)
            })
            .min()
            .unwrap_or(query.len());
        format!(
            "{} AND ({conditions}){}",
            &query[..next_clause_pos],
            &query[next_clause_pos..]
        )
    } else {
        let insert_pos = [" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "]
            .iter()
            .filter_map(|keyword| upper.find(keyword))
            .min()
            .unwrap_or(query.len());
        format!(
            "{} WHERE {conditions}{}",
            &query[..insert_pos],
            &query[insert_pos..]
        )
    }
}

fn introspect_sqlite_schema(db_path: &str, table_name: &str) -> Result<SchemaRef, String> {
    let conn = Connection::open(db_path).map_err(|e| format!("SQLite open error: {}", e))?;
    let pragma_sql = format!(
        "PRAGMA table_info({})",
        quote_identifier(table_name, SqlDialectKind::Sqlite)
    );
    let mut stmt = conn
        .prepare(&pragma_sql)
        .map_err(|e| format!("PRAGMA error: {}", e))?;

    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2).unwrap_or_default();
            let not_null: i64 = row.get(3).unwrap_or(0);
            Ok((name, col_type, not_null == 0))
        })
        .map_err(|e| format!("PRAGMA query error: {}", e))?;

    let mut fields = Vec::new();
    for row in rows {
        let (name, col_type, nullable) = row.map_err(|e| format!("Row error: {}", e))?;
        fields.push(Field::new(name, sql_type_to_arrow(&col_type), nullable));
    }

    if fields.is_empty() {
        return Err(format!(
            "Table '{}' not found or has no columns in '{}'",
            table_name, db_path
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qsql_core::QsqlEngine;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_sqlite(suffix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "test_qsql_sqlite_{}_{}_{}.db",
            suffix,
            std::process::id(),
            nanos
        ));
        let _ = std::fs::remove_file(&path);

        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, active BOOLEAN)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO products (name, price, active) VALUES ('Apple', 1.20, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO products (name, price, active) VALUES ('Banana', 0.80, 0)",
            [],
        )
        .unwrap();
        path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_sqlite_connector() {
        let path = create_temp_sqlite("conn");
        let connector = SqliteConnector::new(&path);
        let tables = connector.list_tables(None, 10).await.unwrap();

        assert_eq!(tables, vec!["products"]);
        assert!(!connector.capabilities().aggregate);
        assert!(!connector.capabilities().joins);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_sqlite_table_provider_schema() {
        let path = create_temp_sqlite("schema");
        let provider = SqliteTableProvider::try_new(&path, "products")
            .await
            .unwrap();
        let schema = provider.schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(
            *schema.field(0).data_type(),
            datafusion::arrow::datatypes::DataType::Int64
        );
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(
            *schema.field(1).data_type(),
            datafusion::arrow::datatypes::DataType::Utf8
        );
        assert_eq!(schema.field(2).name(), "price");
        assert_eq!(
            *schema.field(2).data_type(),
            datafusion::arrow::datatypes::DataType::Float64
        );
        assert_eq!(schema.field(3).name(), "active");
        assert_eq!(
            *schema.field(3).data_type(),
            datafusion::arrow::datatypes::DataType::Boolean
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_sqlite_table_provider_executes_through_datafusion() {
        let path = create_temp_sqlite("provider_exec");
        let provider = SqliteTableProvider::try_new(&path, "products")
            .await
            .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("products", Arc::new(provider))
            .unwrap();

        let rows = engine
            .execute_sql_to_json(
                "SELECT name, price FROM products WHERE price > 1.0 ORDER BY price LIMIT 1",
            )
            .await
            .unwrap();
        assert_eq!(rows[0]["name"], "Apple");

        let _ = std::fs::remove_file(path);
    }
}
