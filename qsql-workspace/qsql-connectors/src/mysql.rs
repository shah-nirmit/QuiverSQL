//! MySQL and MariaDB connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::dialect::MySqlDialect;
use datafusion::sql::TableReference;
use datafusion_table_providers::mysql::{DynMySQLConnectionPool, MySQLTableFactory};
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use mysql_async::{prelude::Queryable, Opts, Pool};
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::{quote_identifier, sql_capabilities, sql_literal, SqlDialectKind};
use crate::{ConnectorResult, RemoteConnector};

#[derive(Debug)]
pub struct MySqlConnector {
    connection_string: String,
    dialect: SqlDialectKind,
}

impl MySqlConnector {
    pub fn new(connection_string: impl Into<String>, dialect: SqlDialectKind) -> Self {
        Self {
            connection_string: connection_string.into(),
            dialect,
        }
    }

    pub fn mysql(connection_string: impl Into<String>) -> Self {
        Self::new(connection_string, SqlDialectKind::Mysql)
    }

    pub fn mariadb(connection_string: impl Into<String>) -> Self {
        Self::new(connection_string, SqlDialectKind::Mariadb)
    }

    pub fn dialect(&self) -> SqlDialectKind {
        self.dialect
    }

    fn pool(&self) -> Result<Pool, String> {
        let opts = Opts::from_url(&self.connection_string)
            .map_err(|e| format!("Invalid MySQL/MariaDB connection URL: {e}"))?;
        Ok(Pool::new(opts))
    }
}

#[async_trait]
impl RemoteConnector for MySqlConnector {
    fn connector_type(&self) -> &'static str {
        match self.dialect {
            SqlDialectKind::Mariadb => "mariadb",
            _ => "mysql",
        }
    }

    async fn table_provider(
        &self,
        schema: Option<&str>,
        table: &str,
        cached_schema: Option<SchemaRef>,
    ) -> ConnectorResult<Arc<dyn TableProvider>> {
        let provider = MySqlTableProvider::try_new_with_schema(
            self.connection_string.clone(),
            self.dialect,
            schema.map(str::to_string),
            table,
            cached_schema,
        )
        .await?;
        Ok(Arc::new(provider))
    }

    async fn explain_query(&self, sql: &str) -> ConnectorResult<String> {
        use mysql_async::prelude::Queryable;
        let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
        let mut conn = self
            .pool()?
            .get_conn()
            .await
            .map_err(|e| format!("Failed to get MySQL connection: {}", e))?;

        let rows: Vec<mysql_async::Row> = conn
            .query(&explain_sql)
            .await
            .map_err(|e| format!("Explain failed: {}", e))?;

        if let Some(row) = rows.first() {
            if let Some(val) = row.get::<String, _>(0) {
                return Ok(val);
            }
        }
        Ok("{}".to_string())
    }

    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities {
        sql_capabilities(self.dialect)
    }

    async fn list_tables(
        &self,
        schema: Option<&str>,
        limit: usize,
    ) -> ConnectorResult<Vec<String>> {
        self.list_tables_page(schema, 0, limit).await
    }

    async fn list_tables_page(
        &self,
        schema: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> ConnectorResult<Vec<String>> {
        let schema_predicate = match schema {
            Some(schema) if !schema.trim().is_empty() => {
                format!("table_schema = {}", sql_literal(schema))
            }
            _ => "table_schema = DATABASE()".to_string(),
        };
        let sql = format!(
            "SELECT table_name FROM information_schema.tables WHERE {schema_predicate} AND table_type = 'BASE TABLE' ORDER BY table_name LIMIT {} OFFSET {}",
            limit.max(1),
            offset
        );
        let pool = self.pool()?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| format!("Failed to get MySQL connection: {}", e))?;
        let rows: Vec<mysql_async::Row> = conn
            .query(sql)
            .await
            .map_err(|e| format!("Failed to list tables: {}", e))?;

        let mut tables = Vec::new();
        for row in rows {
            if let Some(name) = row.get::<String, _>(0) {
                tables.push(name);
            }
        }
        Ok(tables)
    }
}

#[derive(Debug)]
pub struct MySqlTableProvider {
    connector: Arc<MySqlConnector>,
    inner: Arc<dyn TableProvider>,
    dialect: SqlDialectKind,
    schema_name: Option<String>,
    table_name: String,
}

impl MySqlTableProvider {
    pub async fn try_new(
        connection_string: impl Into<String>,
        dialect: SqlDialectKind,
        schema_name: Option<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        Self::try_new_with_schema(connection_string, dialect, schema_name, table_name, None).await
    }

    pub async fn try_new_with_schema(
        connection_string: impl Into<String>,
        dialect: SqlDialectKind,
        schema_name: Option<String>,
        table_name: impl Into<String>,
        schema: Option<SchemaRef>,
    ) -> Result<Self, String> {
        let connection_string = connection_string.into();
        let table_name = table_name.into();
        let connector = Arc::new(MySqlConnector::new(connection_string.clone(), dialect));
        let table_ref = match schema_name.as_deref() {
            Some(schema) if !schema.trim().is_empty() => {
                TableReference::partial(schema.to_string(), table_name.clone())
            }
            _ => TableReference::bare(table_name.clone()),
        };
        let inner = upstream_mysql_provider(&connection_string, table_ref, schema).await?;
        let schema_name = schema_name.filter(|schema| !schema.trim().is_empty());

        Ok(Self {
            connector,
            inner,
            dialect,
            schema_name,
            table_name,
        })
    }

    pub fn connector(&self) -> &Arc<MySqlConnector> {
        &self.connector
    }

    pub fn native_select_sql(&self) -> String {
        let table = quote_identifier(&self.table_name, self.dialect);
        match self.schema_name.as_deref() {
            Some(schema) => format!(
                "SELECT * FROM {}.{table}",
                quote_identifier(schema, self.dialect)
            ),
            None => format!("SELECT * FROM {table}"),
        }
    }
}

#[async_trait]
impl TableProvider for MySqlTableProvider {
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

async fn upstream_mysql_provider(
    connection_string: &str,
    table_ref: TableReference,
    schema: Option<SchemaRef>,
) -> Result<Arc<dyn TableProvider>, String> {
    let pool = Arc::new(
        MySQLConnectionPool::new(mysql_pool_params(connection_string))
            .await
            .map_err(|e| format!("Failed to create MySQL/MariaDB provider pool: {e}"))?,
    );

    if let Some(schema) = schema {
        let dyn_pool: Arc<DynMySQLConnectionPool> = pool;
        let table_provider = Arc::new(
            SqlTable::new_with_schema("mysql", &dyn_pool, schema, table_ref)
                .with_dialect(Arc::new(MySqlDialect {})),
        );
        return Ok(Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| format!("Failed to create MySQL/MariaDB federated provider: {e}"))?,
        ));
    }

    MySQLTableFactory::new(pool)
        .table_provider(table_ref)
        .await
        .map_err(|e| format!("Failed to create MySQL/MariaDB provider: {e}"))
}

fn mysql_pool_params(connection_string: &str) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "connection_string".to_string(),
        SecretString::from(connection_string.trim().to_string()),
    );
    if !connection_string.to_ascii_lowercase().contains("sslmode=") {
        params.insert(
            "sslmode".to_string(),
            SecretString::from("disabled".to_string()),
        );
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use qsql_core::QsqlEngine;
    use secrecy::ExposeSecret;
    use std::sync::Arc;

    async fn mysql_query_drop(connector: &MySqlConnector, sql: &str) {
        let pool = connector.pool().unwrap();
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(sql).await.unwrap();
    }

    #[test]
    fn mysql_pool_params_preserve_local_no_tls_default() {
        let params = mysql_pool_params("mysql://qsql_test:qsql_test@localhost:3306/qsql_test");

        assert_eq!(
            params["connection_string"].expose_secret(),
            "mysql://qsql_test:qsql_test@localhost:3306/qsql_test"
        );
        assert_eq!(params["sslmode"].expose_secret(), "disabled");
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_live_select_requires_env() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_phase4_mysql (id INT, name TEXT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_phase4_mysql").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_phase4_mysql VALUES (1, 'Alice'), (2, 'Bob')",
        )
        .await;

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_phase4_mysql")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_mysql", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT name FROM qsql_phase4_mysql WHERE id = 1 LIMIT 1")
            .await
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["name"], "Alice");
    }
    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_live_pushdown_scenarios() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_phase4_mysql_pushdowns (id INT, name TEXT, price FLOAT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_phase4_mysql_pushdowns").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_phase4_mysql_pushdowns VALUES (1, 'Alice', 10.0), (2, 'Bob', 20.5), (3, NULL, 5.0), (4, 'Dave', NULL)",
        )
        .await;

        let provider = MySqlTableProvider::try_new(
            url,
            SqlDialectKind::Mysql,
            None,
            "qsql_phase4_mysql_pushdowns",
        )
        .await
        .unwrap();

        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_mysql_pushdowns", Arc::new(provider))
            .unwrap();
        let rows1 = engine
            .execute_sql_to_json(
                "SELECT name FROM qsql_phase4_mysql_pushdowns WHERE price + 5 BETWEEN 11 AND 20",
            )
            .await
            .unwrap();
        assert_eq!(rows1.as_array().unwrap().len(), 1);
        assert_eq!(rows1[0]["name"], "Alice");

        let rows2 = engine
            .execute_sql_to_json(
                "SELECT name FROM qsql_phase4_mysql_pushdowns WHERE name IS NOT NULL",
            )
            .await
            .unwrap();
        assert_eq!(rows2.as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_live_complex_pushdown_scenarios() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_phase4_mysql_complex (id INT, category TEXT, score FLOAT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_phase4_mysql_complex").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_phase4_mysql_complex VALUES (1, 'Alpha', 9.5), (2, 'Beta', 8.0), (3, 'Gamma', 4.0), (4, 'Alpha', 3.0)",
        )
        .await;

        let provider = MySqlTableProvider::try_new(
            url,
            SqlDialectKind::Mysql,
            None,
            "qsql_phase4_mysql_complex",
        )
        .await
        .unwrap();

        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_mysql_complex", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json(
                "SELECT id FROM qsql_phase4_mysql_complex WHERE category LIKE 'A%' AND id IN (1, 3, 4) AND (score > 9.0 OR score < 5.0)",
            )
            .await
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
    }
}
