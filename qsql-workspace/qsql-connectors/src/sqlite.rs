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
        fields.push(Field::new(name, sql_type_to_arrow(&col_type)?, nullable));
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
    use datafusion::physical_expr::expressions::{
        BinaryExpr as PhysBinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
    };
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
            datafusion::arrow::datatypes::DataType::Int32
        );
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(
            *schema.field(1).data_type(),
            datafusion::arrow::datatypes::DataType::Utf8
        );
        assert_eq!(schema.field(2).name(), "price");
        assert_eq!(
            *schema.field(2).data_type(),
            datafusion::arrow::datatypes::DataType::Float32
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

    // --- explain_query ---

    #[tokio::test]
    async fn explain_query_returns_plan_for_valid_sql() {
        let path = create_temp_sqlite("explain");
        let connector = SqliteConnector::new(&path);
        let result = connector
            .explain_query("SELECT * FROM products")
            .await
            .unwrap();
        assert!(!result.is_empty(), "explain should return output");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn explain_query_errors_on_invalid_sql() {
        let path = create_temp_sqlite("explain_err");
        let connector = SqliteConnector::new(&path);
        let err = connector
            .explain_query("NOT VALID SQL !!!!")
            .await
            .unwrap_err();
        assert!(!err.message.is_empty());
        let _ = std::fs::remove_file(path);
    }

    // --- native_select_sql ---

    #[tokio::test]
    async fn native_select_sql_is_quoted_select_star() {
        let path = create_temp_sqlite("native_sql");
        let provider = SqliteTableProvider::try_new(&path, "products")
            .await
            .unwrap();
        assert_eq!(provider.native_select_sql(), "SELECT * FROM `products`");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn native_select_sql_quotes_special_chars() {
        let path = create_temp_sqlite("native_sql_special");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute("CREATE TABLE \"my table\" (id INTEGER)", [])
                .unwrap();
        }
        let provider = SqliteTableProvider::try_new(&path, "my table")
            .await
            .unwrap();
        assert_eq!(provider.native_select_sql(), "SELECT * FROM `my table`");
        let _ = std::fs::remove_file(path);
    }

    // --- connector accessors ---

    #[tokio::test]
    async fn connector_type_and_db_path() {
        let path = create_temp_sqlite("accessors");
        let connector = SqliteConnector::new(&path);
        assert_eq!(connector.connector_type(), "sqlite");
        assert_eq!(connector.db_path(), path);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn table_provider_connector_accessor() {
        let path = create_temp_sqlite("connector_acc");
        let provider = SqliteTableProvider::try_new(&path, "products")
            .await
            .unwrap();
        assert_eq!(provider.connector().db_path(), path);
        let _ = std::fs::remove_file(path);
    }

    // --- introspect_sqlite_schema ---

    #[test]
    fn introspect_sqlite_schema_returns_correct_fields() {
        let path = create_temp_sqlite("introspect");
        let schema = introspect_sqlite_schema(&path, "products").unwrap();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn introspect_sqlite_schema_errors_on_missing_table() {
        let path = create_temp_sqlite("introspect_missing");
        let err = introspect_sqlite_schema(&path, "no_such_table").unwrap_err();
        assert!(err.contains("no_such_table"), "error should name the table");
        let _ = std::fs::remove_file(path);
    }

    // --- operator_to_sql ---

    #[test]
    fn operator_to_sql_covers_all_supported() {
        assert_eq!(operator_to_sql(&Operator::Eq), Some("="));
        assert_eq!(operator_to_sql(&Operator::NotEq), Some("<>"));
        assert_eq!(operator_to_sql(&Operator::Lt), Some("<"));
        assert_eq!(operator_to_sql(&Operator::LtEq), Some("<="));
        assert_eq!(operator_to_sql(&Operator::Gt), Some(">"));
        assert_eq!(operator_to_sql(&Operator::GtEq), Some(">="));
        assert_eq!(operator_to_sql(&Operator::And), Some("AND"));
        assert_eq!(operator_to_sql(&Operator::Or), Some("OR"));
        assert_eq!(operator_to_sql(&Operator::Plus), None);
        assert_eq!(operator_to_sql(&Operator::Minus), None);
    }

    // --- scalar_value_to_sql ---

    #[test]
    fn scalar_value_to_sql_covers_numeric_and_text() {
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Int8(Some(1))),
            Some("1".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Int16(Some(-5))),
            Some("-5".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Int32(Some(100))),
            Some("100".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Int64(Some(999))),
            Some("999".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::UInt8(Some(2))),
            Some("2".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::UInt16(Some(3))),
            Some("3".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::UInt32(Some(4))),
            Some("4".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::UInt64(Some(5))),
            Some("5".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Float32(Some(1.5))),
            Some("1.5".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Float64(Some(2.5))),
            Some("2.5".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Utf8(Some("it's".into()))),
            Some("'it''s'".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::LargeUtf8(Some("big".into()))),
            Some("'big'".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Boolean(Some(true))),
            Some("TRUE".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Boolean(Some(false))),
            Some("FALSE".into())
        );
    }

    #[test]
    fn scalar_value_to_sql_null_variants_return_null() {
        assert_eq!(scalar_value_to_sql(&ScalarValue::Null), Some("NULL".into()));
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Int64(None)),
            Some("NULL".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Utf8(None)),
            Some("NULL".into())
        );
        assert_eq!(
            scalar_value_to_sql(&ScalarValue::Boolean(None)),
            Some("NULL".into())
        );
    }

    #[test]
    fn scalar_value_to_sql_unsupported_returns_none() {
        // Date32 is not in the match arms
        assert_eq!(scalar_value_to_sql(&ScalarValue::Date32(Some(1))), None);
    }

    // --- physical_expr_to_sql ---

    #[test]
    fn physical_expr_to_sql_column() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        assert_eq!(physical_expr_to_sql(&expr), Some("`id`".into()));
    }

    #[test]
    fn physical_expr_to_sql_literal() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(42))));
        assert_eq!(physical_expr_to_sql(&expr), Some("42".into()));
    }

    #[test]
    fn physical_expr_to_sql_binary_expr() {
        let left: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(5))));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(PhysBinaryExpr::new(left, Operator::Gt, right));
        assert_eq!(physical_expr_to_sql(&expr), Some("(`id` > 5)".into()));
    }

    #[test]
    fn physical_expr_to_sql_is_null_and_is_not_null() {
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("name", 1));
        let is_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::clone(&col)));
        let is_not_null: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(col));
        assert_eq!(
            physical_expr_to_sql(&is_null),
            Some("`name` IS NULL".into())
        );
        assert_eq!(
            physical_expr_to_sql(&is_not_null),
            Some("`name` IS NOT NULL".into())
        );
    }

    #[test]
    fn physical_expr_to_sql_not_expr() {
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let not_expr: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(col));
        assert_eq!(physical_expr_to_sql(&not_expr), Some("NOT (`id`)".into()));
    }

    // --- insert_where_clause ---

    #[test]
    fn insert_where_clause_bare_select() {
        let sql = insert_where_clause("SELECT * FROM t", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE x > 1");
    }

    #[test]
    fn insert_where_clause_appends_to_existing_where() {
        let sql = insert_where_clause("SELECT * FROM t WHERE a = 1", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE a = 1 AND (x > 1)");
    }

    #[test]
    fn insert_where_clause_before_order_by() {
        let sql = insert_where_clause("SELECT * FROM t ORDER BY id", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE x > 1 ORDER BY id");
    }

    #[test]
    fn insert_where_clause_before_limit() {
        let sql = insert_where_clause("SELECT * FROM t LIMIT 10", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE x > 1 LIMIT 10");
    }

    #[test]
    fn insert_where_clause_before_group_by() {
        let sql = insert_where_clause("SELECT * FROM t GROUP BY x", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE x > 1 GROUP BY x");
    }

    #[test]
    fn insert_where_clause_existing_where_before_order_by() {
        let sql = insert_where_clause("SELECT * FROM t WHERE a = 1 ORDER BY id", "x > 1");
        assert_eq!(sql, "SELECT * FROM t WHERE a = 1 AND (x > 1) ORDER BY id");
    }

    // --- apply_physical_filters ---

    #[test]
    fn apply_physical_filters_empty_is_identity() {
        let result = apply_physical_filters("SELECT * FROM t", &[]).unwrap();
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn apply_physical_filters_injects_condition() {
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(3))));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(PhysBinaryExpr::new(col, Operator::GtEq, lit));
        let result = apply_physical_filters("SELECT * FROM t", &[expr]).unwrap();
        assert_eq!(result, "SELECT * FROM t WHERE (`id` >= 3)");
    }

    // --- list_tables_page pagination ---

    #[tokio::test]
    async fn list_tables_page_offset_and_limit() {
        let path = create_temp_sqlite("page");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute("CREATE TABLE aaa (id INTEGER)", []).unwrap();
            conn.execute("CREATE TABLE bbb (id INTEGER)", []).unwrap();
            conn.execute("CREATE TABLE ccc (id INTEGER)", []).unwrap();
        }
        let connector = SqliteConnector::new(&path);
        let page = connector.list_tables_page(None, 1, 2).await.unwrap();
        // Alphabetical: aaa, bbb, ccc. offset=1, limit=2 → bbb, ccc
        // products is also there from create_temp_sqlite... actually wait,
        // create_temp_sqlite creates a separate db for "page" suffix.
        // The "page" db has aaa, bbb, ccc, products
        assert_eq!(page.len(), 2);
        let _ = std::fs::remove_file(path);
    }
}
