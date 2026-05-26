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
    use datafusion::datasource::TableProvider;
    use qsql_core::broadcast::{BroadcastRewriteConfig, SkipReason};
    use qsql_core::GuardedTableProvider;
    use qsql_core::QsqlEngine;
    use secrecy::ExposeSecret;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    static PG_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn pg_temp_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = PG_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}", std::process::id(), nanos, seq)
    }

    fn pg_temp_csv(rows: &[(i64, &str, &str)]) -> String {
        let path =
            std::env::temp_dir().join(format!("test_qsql_pg_users_{}.csv", pg_temp_suffix()));
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "id,name,role").unwrap();
        for (id, name, role) in rows {
            writeln!(file, "{id},{name},{role}").unwrap();
        }
        path.to_str().unwrap().to_string()
    }

    async fn pg_collect_sorted_rows(engine: &QsqlEngine, sql: &str) -> Vec<String> {
        let value = engine
            .execute_sql_to_json(sql)
            .await
            .expect("execute query");
        let arr = value.as_array().expect("expected JSON array").clone();
        let mut rendered: Vec<String> = arr
            .into_iter()
            .map(|v| serde_json::to_string(&v).expect("serialize row"))
            .collect();
        rendered.sort();
        rendered
    }

    async fn pg_build_broadcast_engine(
        url: &str,
        table: &str,
        csv_path: &str,
        config: BroadcastRewriteConfig,
    ) -> QsqlEngine {
        let provider =
            PostgresTableProvider::try_new(url.to_string(), Some("public".to_string()), table)
                .await
                .unwrap();
        let guarded: Arc<dyn TableProvider> =
            Arc::new(GuardedTableProvider::new(table, Arc::new(provider)));
        let engine = QsqlEngine::new().with_broadcast_config(config);
        engine
            .register_file("users", csv_path, "csv")
            .await
            .expect("register csv");
        engine.register_table(table, guarded).unwrap();
        engine
    }

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

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_sort_asc_parity() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        let client = connector.client().await.unwrap();
        // Insert evens first, then odds — physical row order ≠ sort order
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_sort_pg_asc (id INT, label TEXT);
                 TRUNCATE qsql_sort_pg_asc;
                 INSERT INTO qsql_sort_pg_asc (id, label)
                 SELECT n, 'row_' || n FROM generate_series(2, 20, 2) AS t(n)
                 UNION ALL
                 SELECT n, 'row_' || n FROM generate_series(1, 19, 2) AS t(n);",
            )
            .await
            .unwrap();

        let provider =
            PostgresTableProvider::try_new(url, Some("public".to_string()), "qsql_sort_pg_asc")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_pg_asc", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_pg_asc ORDER BY id ASC LIMIT 10")
            .await
            .unwrap();

        let ids: Vec<i64> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids.len(), 10);
        assert_eq!(ids[0], 1, "first id should be 1");
        assert_eq!(ids[9], 10, "last id should be 10");
        for w in ids.windows(2) {
            assert!(w[0] < w[1], "rows not in ascending order: {:?}", ids);
        }
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_sort_desc_parity() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        let client = connector.client().await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_sort_pg_desc (id INT, label TEXT);
                 TRUNCATE qsql_sort_pg_desc;
                 INSERT INTO qsql_sort_pg_desc (id, label)
                 SELECT n, 'row_' || n FROM generate_series(2, 20, 2) AS t(n)
                 UNION ALL
                 SELECT n, 'row_' || n FROM generate_series(1, 19, 2) AS t(n);",
            )
            .await
            .unwrap();

        let provider =
            PostgresTableProvider::try_new(url, Some("public".to_string()), "qsql_sort_pg_desc")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_pg_desc", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_pg_desc ORDER BY id DESC LIMIT 5")
            .await
            .unwrap();

        let ids: Vec<i64> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids.len(), 5);
        assert_eq!(ids[0], 20, "first id should be max (20)");
        for w in ids.windows(2) {
            assert!(w[0] > w[1], "rows not in descending order: {:?}", ids);
        }
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_medium_sort_smoke() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        let client = connector.client().await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_sort_pg_medium (id INT, label TEXT);
                 TRUNCATE qsql_sort_pg_medium;
                 INSERT INTO qsql_sort_pg_medium (id, label)
                 SELECT n, 'item_' || n FROM generate_series(1, 1000) AS t(n);",
            )
            .await
            .unwrap();

        let provider =
            PostgresTableProvider::try_new(url, Some("public".to_string()), "qsql_sort_pg_medium")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_pg_medium", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_pg_medium ORDER BY id DESC LIMIT 3")
            .await
            .unwrap();

        let ids: Vec<i64> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], 1000, "expected top id to be 1000, got {}", ids[0]);
        assert_eq!(ids[1], 999);
        assert_eq!(ids[2], 998);
    }

    // ── broadcast join tests ──────────────────────────────────────────────────

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_broadcast_join_csv_parity() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_bcast_pg_parity
                   (employee_id INT, salary DOUBLE PRECISION, bonus INT);
                 TRUNCATE qsql_bcast_pg_parity;
                 INSERT INTO qsql_bcast_pg_parity VALUES
                   (1, 120000.0, 8000), (2, 95000.0, 5000),
                   (3, 80000.0, 4000), (4, 70000.0, 2000);",
            )
            .await
            .unwrap();

        let csv = pg_temp_csv(&[
            (1, "Alice", "Engineer"),
            (2, "Bob", "Manager"),
            (3, "Charlie", "Designer"),
        ]);
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   JOIN qsql_bcast_pg_parity c ON u.id = c.employee_id";

        let engine_on = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_parity",
            &csv,
            BroadcastRewriteConfig::default(),
        )
        .await;
        let engine_off = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_parity",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_on = pg_collect_sorted_rows(&engine_on, sql).await;
        let rows_off = pg_collect_sorted_rows(&engine_off, sql).await;
        assert_eq!(
            rows_on, rows_off,
            "broadcast rewrite must not change result set"
        );
        assert_eq!(rows_on.len(), 3, "expected 3 inner-join matches");

        let (_, info_on) = engine_on
            .get_logical_plan_with_broadcast(sql)
            .await
            .expect("explain rewrite_on");
        assert_eq!(
            info_on.applied.len(),
            1,
            "expected one applied rewrite: {info_on:?}"
        );
        assert_eq!(info_on.applied[0].predicate_value_count, 3);

        let (_, info_off) = engine_off
            .get_logical_plan_with_broadcast(sql)
            .await
            .expect("explain rewrite_off");
        assert!(
            info_off.applied.is_empty(),
            "disabled config must not apply rewrites: {info_off:?}"
        );
        assert_eq!(info_off.considered, 0);

        let _ = std::fs::remove_file(csv);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_broadcast_join_empty_local_side() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_bcast_pg_empty
                   (employee_id INT, salary DOUBLE PRECISION, bonus INT);
                 TRUNCATE qsql_bcast_pg_empty;
                 INSERT INTO qsql_bcast_pg_empty VALUES (1, 1.0, 100), (2, 2.0, 200);",
            )
            .await
            .unwrap();

        let csv = pg_temp_csv(&[]); // empty local side — only header row
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   JOIN qsql_bcast_pg_empty c ON u.id = c.employee_id";

        let engine_on = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_empty",
            &csv,
            BroadcastRewriteConfig::default(),
        )
        .await;
        let engine_off = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_empty",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_on = pg_collect_sorted_rows(&engine_on, sql).await;
        let rows_off = pg_collect_sorted_rows(&engine_off, sql).await;
        assert_eq!(rows_on, rows_off);
        assert!(rows_on.is_empty(), "empty inner join should produce 0 rows");

        let (_, info) = engine_on
            .get_logical_plan_with_broadcast(sql)
            .await
            .expect("explain empty-side");
        assert_eq!(
            info.applied.len(),
            1,
            "empty-side rewrite still counts as applied: {info:?}"
        );
        assert_eq!(info.applied[0].predicate_value_count, 0);

        let _ = std::fs::remove_file(csv);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_broadcast_join_large_local_exceeds_cap() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_bcast_pg_large
                   (employee_id INT, salary DOUBLE PRECISION, bonus INT);
                 TRUNCATE qsql_bcast_pg_large;
                 INSERT INTO qsql_bcast_pg_large VALUES
                   (0, 1.0, 100), (1, 2.0, 200), (49, 3.0, 300);",
            )
            .await
            .unwrap();

        // 50-row CSV exceeds max_local_rows=10 cap
        let csv_path =
            std::env::temp_dir().join(format!("test_qsql_pg_large_{}.csv", pg_temp_suffix()));
        {
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "id,name,role").unwrap();
            for i in 0..50i64 {
                writeln!(f, "{i},x,y").unwrap();
            }
        }
        let csv = csv_path.to_str().unwrap().to_string();
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   JOIN qsql_bcast_pg_large c ON u.id = c.employee_id";

        let config_capped = BroadcastRewriteConfig {
            enabled: true,
            max_local_rows: 10,
            max_local_bytes: qsql_core::broadcast::DEFAULT_MAX_LOCAL_BYTES,
        };
        let engine_capped =
            pg_build_broadcast_engine(&url, "qsql_bcast_pg_large", &csv, config_capped).await;
        let engine_off = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_large",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_capped = pg_collect_sorted_rows(&engine_capped, sql).await;
        let rows_off = pg_collect_sorted_rows(&engine_off, sql).await;
        assert_eq!(
            rows_capped, rows_off,
            "cap fallback must produce identical rows to the unrewritten plan"
        );

        let (_, info) = engine_capped
            .get_logical_plan_with_broadcast(sql)
            .await
            .expect("explain over-cap");
        assert!(
            info.applied.is_empty(),
            "over-cap rewrite must abort: {info:?}"
        );
        assert!(
            info.skipped
                .iter()
                .any(|s| s.reason == SkipReason::LocalSideMaterializationExceededCap),
            "expected LocalSideMaterializationExceededCap skip: {info:?}"
        );

        let _ = std::fs::remove_file(csv);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_broadcast_join_left_join_not_eligible() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_bcast_pg_left
                   (employee_id INT, salary DOUBLE PRECISION, bonus INT);
                 TRUNCATE qsql_bcast_pg_left;
                 INSERT INTO qsql_bcast_pg_left VALUES
                   (1, 1.0, 10), (2, 2.0, 20), (3, 3.0, 30);",
            )
            .await
            .unwrap();

        let csv = pg_temp_csv(&[(1, "Alice", "E"), (2, "Bob", "M")]);
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   LEFT JOIN qsql_bcast_pg_left c ON u.id = c.employee_id";
        let engine = pg_build_broadcast_engine(
            &url,
            "qsql_bcast_pg_left",
            &csv,
            BroadcastRewriteConfig::default(),
        )
        .await;

        let (_, info) = engine
            .get_logical_plan_with_broadcast(sql)
            .await
            .expect("explain left join");
        assert!(info.applied.is_empty(), "LEFT JOIN must not rewrite");
        assert!(
            info.skipped
                .iter()
                .any(|s| s.reason == SkipReason::NotInnerEquiJoin),
            "expected NotInnerEquiJoin skip: {info:?}"
        );

        let _ = std::fs::remove_file(csv);
    }

    // ── federation cross-source + same-source tests ───────────────────────────

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_federated_cross_source_join() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_fedj_pg_comp
                   (employee_id INT, salary DOUBLE PRECISION);
                 TRUNCATE qsql_fedj_pg_comp;
                 INSERT INTO qsql_fedj_pg_comp VALUES
                   (1, 120000.0), (2, 95000.0), (3, 80000.0);",
            )
            .await
            .unwrap();

        let csv = pg_temp_csv(&[
            (1, "Alice", "Engineer"),
            (2, "Bob", "Manager"),
            (3, "Charlie", "Designer"),
        ]);
        let engine = QsqlEngine::new();
        engine
            .register_file("users", &csv, "csv")
            .await
            .expect("register users csv");
        let provider = PostgresTableProvider::try_new(
            url.clone(),
            Some("public".to_string()),
            "qsql_fedj_pg_comp",
        )
        .await
        .unwrap();
        engine
            .register_table("qsql_fedj_pg_comp", Arc::new(provider))
            .unwrap();

        let sql = "
            SELECT u.name, u.role, c.salary
            FROM users u
            JOIN qsql_fedj_pg_comp c ON u.id = c.employee_id
            WHERE c.salary > 90000.0
            ORDER BY c.salary DESC
        ";
        let result = engine.execute_sql_to_json(sql).await.unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[0]["role"], "Engineer");
        assert_eq!(rows[0]["salary"], 120000.0);
        assert_eq!(rows[1]["name"], "Bob");
        assert_eq!(rows[1]["role"], "Manager");
        assert_eq!(rows[1]["salary"], 95000.0);

        let _ = std::fs::remove_file(csv);
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_postgres_tests),
        ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
    )]
    async fn postgres_same_source_join_uses_federation() {
        let url = std::env::var("QSQL_POSTGRES_URL")
            .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");

        let connector = PostgresConnector::new(url.clone());
        connector
            .client()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS qsql_fedj_pg_customers
                   (id INT, name TEXT, region TEXT);
                 CREATE TABLE IF NOT EXISTS qsql_fedj_pg_orders
                   (id INT, customer_id INT, total DOUBLE PRECISION);
                 TRUNCATE qsql_fedj_pg_customers;
                 TRUNCATE qsql_fedj_pg_orders;
                 INSERT INTO qsql_fedj_pg_customers VALUES
                   (1, 'Acme', 'west'), (2, 'Globex', 'east');
                 INSERT INTO qsql_fedj_pg_orders VALUES
                   (10, 1, 125.0), (11, 1, 90.0), (12, 2, 60.0);",
            )
            .await
            .unwrap();

        let engine = QsqlEngine::new();
        let customers = PostgresTableProvider::try_new(
            url.clone(),
            Some("public".to_string()),
            "qsql_fedj_pg_customers",
        )
        .await
        .unwrap();
        let orders = PostgresTableProvider::try_new(
            url.clone(),
            Some("public".to_string()),
            "qsql_fedj_pg_orders",
        )
        .await
        .unwrap();
        engine
            .register_table("qsql_fedj_pg_customers", Arc::new(customers))
            .unwrap();
        engine
            .register_table("qsql_fedj_pg_orders", Arc::new(orders))
            .unwrap();

        let sql = "
            SELECT c.name, SUM(o.total) AS order_total
            FROM qsql_fedj_pg_customers c
            JOIN qsql_fedj_pg_orders o ON c.id = o.customer_id
            WHERE c.region = 'west'
            GROUP BY c.name
        ";
        let plan = engine.get_logical_plan(sql).await.unwrap();
        let plan_text = format!("{plan:?}");
        assert!(
            plan_text.contains("Federated"),
            "expected same-source Postgres subplan to be federated, got:\n{plan_text}"
        );

        let result = engine.execute_sql_to_json(sql).await.unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Acme");
        assert_eq!(rows[0]["order_total"], 215.0);
    }
}
