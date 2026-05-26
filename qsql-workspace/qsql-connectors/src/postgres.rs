//! Postgres connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion::sql::TableReference;
use datafusion_table_providers::postgres::{DynPostgresConnectionPool, PostgresTableFactory};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;

use crate::sql::{quote_identifier, sql_capabilities, SqlDialectKind};
use crate::{ConnectorResult, RemoteConnector};

#[derive(Debug)]
pub struct PostgresConnector {
    connection_string: String,
}

impl PostgresConnector {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
        }
    }

    async fn client(&self) -> Result<tokio_postgres::Client, String> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to Postgres: {e}"))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

#[async_trait]
impl RemoteConnector for PostgresConnector {
    fn connector_type(&self) -> &'static str {
        "postgres"
    }

    async fn table_provider(
        &self,
        schema: Option<&str>,
        table: &str,
        cached_schema: Option<SchemaRef>,
    ) -> ConnectorResult<Arc<dyn TableProvider>> {
        let provider = PostgresTableProvider::try_new_with_schema(
            self.connection_string.clone(),
            Some(schema.unwrap_or("public").to_string()),
            table,
            cached_schema,
        )
        .await?;
        Ok(Arc::new(provider))
    }

    async fn explain_query(&self, sql: &str) -> ConnectorResult<String> {
        let explain_sql = format!("EXPLAIN (FORMAT JSON, COSTS TRUE) {}", sql);
        let client = self.client().await?;
        let rows = client
            .query(&explain_sql, &[])
            .await
            .map_err(|e| format!("Explain failed: {}", e))?;

        if let Some(row) = rows.first() {
            let val: serde_json::Value = row.get(0);
            return Ok(serde_json::to_string(&val).unwrap_or_default());
        }
        Ok("[]".to_string())
    }

    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities {
        sql_capabilities(SqlDialectKind::Postgres)
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
        let schema_name = schema.unwrap_or("public");
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name LIMIT $2 OFFSET $3";
        let client = self.client().await?;
        let rows = client
            .query(sql, &[&schema_name, &limit, &offset])
            .await
            .map_err(|e| format!("Failed to list tables: {}", e))?;

        let mut tables = Vec::new();
        for row in rows {
            let name: String = row.get(0);
            tables.push(name);
        }
        Ok(tables)
    }
}

#[derive(Debug)]
pub struct PostgresTableProvider {
    connector: Arc<PostgresConnector>,
    inner: Arc<dyn TableProvider>,
    schema_name: String,
    table_name: String,
}

impl PostgresTableProvider {
    pub async fn try_new(
        connection_string: impl Into<String>,
        schema_name: Option<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        Self::try_new_with_schema(connection_string, schema_name, table_name, None).await
    }

    pub async fn try_new_with_schema(
        connection_string: impl Into<String>,
        schema_name: Option<String>,
        table_name: impl Into<String>,
        schema: Option<SchemaRef>,
    ) -> Result<Self, String> {
        let connection_string = connection_string.into();
        let table_name = table_name.into();
        let schema_name = schema_name.unwrap_or_else(|| "public".to_string());
        let connector = Arc::new(PostgresConnector::new(connection_string.clone()));
        let table_ref = TableReference::partial(schema_name.clone(), table_name.clone());
        let inner = upstream_postgres_provider(&connection_string, table_ref, schema).await?;

        Ok(Self {
            connector,
            inner,
            schema_name,
            table_name,
        })
    }

    pub fn connector(&self) -> &Arc<PostgresConnector> {
        &self.connector
    }

    pub fn native_select_sql(&self) -> String {
        format!(
            "SELECT * FROM {}.{}",
            quote_identifier(&self.schema_name, SqlDialectKind::Postgres),
            quote_identifier(&self.table_name, SqlDialectKind::Postgres)
        )
    }
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
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

async fn upstream_postgres_provider(
    connection_string: &str,
    table_ref: TableReference,
    schema: Option<SchemaRef>,
) -> Result<Arc<dyn TableProvider>, String> {
    let pool = Arc::new(
        PostgresConnectionPool::new(postgres_pool_params(connection_string)?)
            .await
            .map_err(|e| format!("Failed to create Postgres provider pool: {e}"))?,
    );

    if let Some(schema) = schema {
        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;
        let table_provider = Arc::new(
            SqlTable::new_with_schema("postgres", &dyn_pool, schema, table_ref)
                .with_dialect(Arc::new(PostgreSqlDialect {})),
        );
        return Ok(Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| format!("Failed to create Postgres federated provider: {e}"))?,
        ));
    }

    PostgresTableFactory::new(pool)
        .table_provider(table_ref)
        .await
        .map_err(|e| format!("Failed to create Postgres provider: {e}"))
}

fn postgres_pool_params(connection_string: &str) -> Result<HashMap<String, SecretString>, String> {
    let connection_string = connection_string.trim();
    let mut params = HashMap::new();

    if connection_string.starts_with("postgres://")
        || connection_string.starts_with("postgresql://")
    {
        let url = url::Url::parse(connection_string)
            .map_err(|e| format!("Invalid Postgres connection URL: {e}"))?;
        if let Some(host) = url.host_str() {
            params.insert("host".to_string(), SecretString::from(host.to_string()));
        }
        if let Some(port) = url.port() {
            params.insert("port".to_string(), SecretString::from(port.to_string()));
        }
        if !url.username().is_empty() {
            params.insert(
                "user".to_string(),
                SecretString::from(url.username().to_string()),
            );
        }
        if let Some(password) = url.password() {
            params.insert("pass".to_string(), SecretString::from(password.to_string()));
        }
        let db = url.path().trim_start_matches('/');
        if !db.is_empty() {
            params.insert("db".to_string(), SecretString::from(db.to_string()));
        }
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sslmode" | "sslrootcert" | "application_name" => {
                    params.insert(key.to_string(), SecretString::from(value.to_string()));
                }
                _ => {}
            }
        }
        params
            .entry("sslmode".to_string())
            .or_insert_with(|| SecretString::from("disable".to_string()));
    } else {
        params.insert(
            "connection_string".to_string(),
            SecretString::from(connection_string.to_string()),
        );
        if !connection_string.to_ascii_lowercase().contains("sslmode=") {
            params.insert(
                "sslmode".to_string(),
                SecretString::from("disable".to_string()),
            );
        }
    }

    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use qsql_core::QsqlEngine;
    use secrecy::ExposeSecret;
    use std::sync::Arc;

    #[test]
    fn postgres_url_params_preserve_local_no_tls_default() {
        let params =
            postgres_pool_params("postgres://qsql_test:qsql_test@localhost:5432/qsql_test")
                .unwrap();

        assert_eq!(params["host"].expose_secret(), "localhost");
        assert_eq!(params["port"].expose_secret(), "5432");
        assert_eq!(params["user"].expose_secret(), "qsql_test");
        assert_eq!(params["pass"].expose_secret(), "qsql_test");
        assert_eq!(params["db"].expose_secret(), "qsql_test");
        assert_eq!(params["sslmode"].expose_secret(), "disable");
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_live_select_requires_env() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url);
        let client = connector.client().await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_phase4_pg (id INT, name TEXT);
                 TRUNCATE qsql_phase4_pg;
                 INSERT INTO qsql_phase4_pg VALUES (1, 'Alice'), (2, 'Bob');",
            )
            .await
            .unwrap();

        let provider = PostgresTableProvider::try_new(
            connector.connection_string.clone(),
            Some("public".to_string()),
            "qsql_phase4_pg",
        )
        .await
        .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_pg", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT name FROM qsql_phase4_pg WHERE id = 1 LIMIT 1")
            .await
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["name"], "Alice");
    }
    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_live_pushdown_scenarios() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url);
        let client = connector.client().await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_phase4_pg_pushdowns (id INT, name TEXT, price FLOAT);
                 TRUNCATE qsql_phase4_pg_pushdowns;
                 INSERT INTO qsql_phase4_pg_pushdowns VALUES (1, 'Alice', 10.0), (2, 'Bob', 20.5), (3, NULL, 5.0), (4, 'Dave', NULL);",
            )
            .await
            .unwrap();

        let provider = PostgresTableProvider::try_new(
            connector.connection_string.clone(),
            Some("public".to_string()),
            "qsql_phase4_pg_pushdowns",
        )
        .await
        .unwrap();

        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_pg_pushdowns", Arc::new(provider))
            .unwrap();
        let rows1 = engine
            .execute_sql_to_json(
                "SELECT name FROM qsql_phase4_pg_pushdowns WHERE price + 5 BETWEEN 11 AND 20",
            )
            .await
            .unwrap();
        assert_eq!(rows1.as_array().unwrap().len(), 1);
        assert_eq!(rows1[0]["name"], "Alice");

        let rows2 = engine
            .execute_sql_to_json("SELECT name FROM qsql_phase4_pg_pushdowns WHERE name IS NOT NULL")
            .await
            .unwrap();
        assert_eq!(rows2.as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_live_complex_pushdown_scenarios() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url);
        let client = connector.client().await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_phase4_pg_complex (id INT, category TEXT, score FLOAT);
                 TRUNCATE qsql_phase4_pg_complex;
                 INSERT INTO qsql_phase4_pg_complex VALUES (1, 'Alpha', 9.5), (2, 'Beta', 8.0), (3, 'Gamma', 4.0), (4, 'Alpha', 3.0);",
            )
            .await
            .unwrap();

        let provider = PostgresTableProvider::try_new(
            connector.connection_string.clone(),
            Some("public".to_string()),
            "qsql_phase4_pg_complex",
        )
        .await
        .unwrap();

        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_phase4_pg_complex", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json(
                "SELECT id FROM qsql_phase4_pg_complex WHERE category LIKE 'A%' AND id IN (1, 3, 4) AND (score > 9.0 OR score < 5.0)",
            )
            .await
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
    }

    #[test]
    fn postgres_connector_type_is_postgres() {
        let connector = PostgresConnector::new("postgres://localhost/mydb");
        assert_eq!(connector.connector_type(), "postgres");
    }

    #[test]
    fn postgres_capabilities_reports_postgres_dialect() {
        let connector = PostgresConnector::new("postgres://localhost/mydb");
        let caps = connector.capabilities();
        assert!(caps.filter, "Postgres should support filter pushdown");
        assert_eq!(caps.dialect_name, "postgres");
    }

    #[test]
    fn postgres_pool_params_postgresql_scheme_is_handled() {
        let params =
            postgres_pool_params("postgresql://alice:pw@db.example.com:5433/myapp").unwrap();
        assert_eq!(params["host"].expose_secret(), "db.example.com");
        assert_eq!(params["port"].expose_secret(), "5433");
        assert_eq!(params["user"].expose_secret(), "alice");
        assert_eq!(params["pass"].expose_secret(), "pw");
        assert_eq!(params["db"].expose_secret(), "myapp");
    }

    #[test]
    fn postgres_pool_params_url_without_password() {
        let params = postgres_pool_params("postgres://alice@localhost/mydb").unwrap();
        assert_eq!(params["user"].expose_secret(), "alice");
        assert!(!params.contains_key("pass"), "no password should be set");
        assert_eq!(params["sslmode"].expose_secret(), "disable");
    }

    #[test]
    fn postgres_pool_params_key_value_string_is_passed_through() {
        let conn = "host=localhost port=5432 user=alice dbname=mydb";
        let params = postgres_pool_params(conn).unwrap();
        assert_eq!(params["connection_string"].expose_secret(), conn);
        assert_eq!(params["sslmode"].expose_secret(), "disable");
    }

    #[test]
    fn postgres_pool_params_malformed_url_returns_error() {
        let result = postgres_pool_params("postgres://not a valid[url");
        assert!(result.is_err(), "malformed URL should return an error");
    }
}
