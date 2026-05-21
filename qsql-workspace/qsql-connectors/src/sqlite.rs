//! SQLite connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use rusqlite::{types::ValueRef, Connection};
use std::sync::Arc;

use crate::sql::{
    quote_identifier, sql_capabilities, sql_type_to_arrow, SqlDialectKind, SqlPushdownPlan,
    SqlTableProvider, SqlTableRef,
};
use crate::RemoteConnector;

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

    async fn explain_query(&self, sql: &str) -> Result<String, String> {
        let conn = rusqlite::Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to get SQLite connection: {}", e))?;

        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let mut stmt = conn
            .prepare(&explain_sql)
            .map_err(|e| format!("Failed to prepare explain query: {}", e))?;

        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Failed to execute explain query: {}", e))?;

        let mut result = String::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let id: i32 = row.get(0).unwrap_or(0);
            let parent: i32 = row.get(1).unwrap_or(0);
            let detail: String = row.get(3).unwrap_or_default();
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
    ) -> Result<Vec<String>, String> {
        let sql = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT {}",
            limit.max(1)
        );
        let rows = self.execute_query(&sql).await?;
        let mut tables = Vec::new();
        for row in rows {
            if let Some(name) = row.get("name").and_then(|v| v.as_str()) {
                tables.push(name.to_string());
            }
        }
        Ok(tables)
    }

    async fn execute_query(&self, sql: &str) -> Result<Vec<serde_json::Value>, String> {
        let db_path = self.db_path.clone();
        let sql = sql.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| format!("Failed to open SQLite DB '{}': {}", db_path, e))?;

            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("Failed to prepare SQL: {}", e))?;

            let column_count = stmt.column_count();
            let column_names: Vec<String> = (0..column_count)
                .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
                .collect();

            let rows = stmt
                .query_map([], |row| {
                    let mut obj = serde_json::Map::new();
                    for (i, name) in column_names.iter().enumerate() {
                        let val = match row.get_ref(i).unwrap_or(ValueRef::Null) {
                            ValueRef::Null => serde_json::Value::Null,
                            ValueRef::Integer(n) => serde_json::json!(n),
                            ValueRef::Real(f) => serde_json::json!(f),
                            ValueRef::Text(t) => {
                                serde_json::Value::String(String::from_utf8_lossy(t).into_owned())
                            }
                            ValueRef::Blob(b) => {
                                serde_json::Value::String(format!("<blob {} bytes>", b.len()))
                            }
                        };
                        obj.insert(name.clone(), val);
                    }
                    Ok(serde_json::Value::Object(obj))
                })
                .map_err(|e| format!("Query failed: {}", e))?;

            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(|e| format!("Row error: {}", e))?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| format!("Thread join error: {}", e))?
    }
}

/// A DataFusion `TableProvider` backed by a single SQLite table.
#[derive(Debug)]
pub struct SqliteTableProvider {
    connector: Arc<SqliteConnector>,
    inner: SqlTableProvider,
}

impl SqliteTableProvider {
    pub fn try_new(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        let db_path = db_path.into();
        let table_name = table_name.into();
        let schema = introspect_sqlite_schema(&db_path, &table_name)?;
        let connector = Arc::new(SqliteConnector::new(db_path));
        let inner = SqlTableProvider::new(
            connector.clone(),
            SqlDialectKind::Sqlite,
            SqlTableRef::bare(table_name),
            schema,
        );

        Ok(Self { connector, inner })
    }

    pub fn connector(&self) -> &Arc<SqliteConnector> {
        &self.connector
    }

    pub fn build_select_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SqlPushdownPlan, String> {
        self.inner.build_select_sql(projection, filters, limit)
    }

    pub fn last_sql(&self) -> Option<String> {
        self.inner.last_sql()
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    use datafusion::prelude::{col, lit};
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
        let res = connector
            .execute_query("SELECT name, price FROM products ORDER BY price DESC")
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0]["name"], "Apple");
        assert_eq!(res[0]["price"], 1.20);
        assert_eq!(connector.capabilities().aggregate, false);
        assert_eq!(connector.capabilities().joins, false);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_sqlite_table_provider_schema() {
        let path = create_temp_sqlite("schema");
        let provider = SqliteTableProvider::try_new(&path, "products").unwrap();
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

    #[test]
    fn test_sqlite_pushdown_emits_projection_filter_limit() {
        let path = create_temp_sqlite("pushdown_sql");
        let provider = SqliteTableProvider::try_new(&path, "products").unwrap();
        let plan = provider
            .build_select_sql(Some(&vec![1, 2]), &[col("price").gt(lit(1.0))], Some(1))
            .unwrap();

        assert_eq!(
            plan.sql,
            "SELECT `name`, `price` FROM `products` WHERE (`price` > 1.0) LIMIT 1"
        );
        assert!(!plan.sql.contains("SELECT *"));

        let _ = std::fs::remove_file(path);
    }
}
