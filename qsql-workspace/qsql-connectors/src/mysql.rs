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
    use datafusion::datasource::TableProvider;
    use qsql_core::broadcast::{BroadcastRewriteConfig, SkipReason};
    use qsql_core::GuardedTableProvider;
    use qsql_core::QsqlEngine;
    use secrecy::ExposeSecret;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    static MYSQL_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn mysql_temp_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = MYSQL_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}", std::process::id(), nanos, seq)
    }

    fn mysql_temp_csv(rows: &[(i64, &str, &str)]) -> String {
        let path =
            std::env::temp_dir().join(format!("test_qsql_mysql_users_{}.csv", mysql_temp_suffix()));
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "id,name,role").unwrap();
        for (id, name, role) in rows {
            writeln!(file, "{id},{name},{role}").unwrap();
        }
        path.to_str().unwrap().to_string()
    }

    async fn mysql_collect_sorted_rows(engine: &QsqlEngine, sql: &str) -> Vec<String> {
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

    async fn mysql_build_broadcast_engine(
        url: &str,
        table: &str,
        csv_path: &str,
        config: BroadcastRewriteConfig,
    ) -> QsqlEngine {
        let provider =
            MySqlTableProvider::try_new(url.to_string(), SqlDialectKind::Mysql, None, table)
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

    #[test]
    fn mysql_connector_type_for_mysql_dialect() {
        let connector = MySqlConnector::mysql("mysql://root:pw@localhost/mydb");
        assert_eq!(connector.connector_type(), "mysql");
    }

    #[test]
    fn mariadb_connector_type_is_mariadb() {
        let connector = MySqlConnector::mariadb("mysql://root:pw@localhost/mydb");
        assert_eq!(connector.connector_type(), "mariadb");
    }

    #[test]
    fn mariadb_constructor_sets_mariadb_dialect() {
        let connector = MySqlConnector::mariadb("mysql://root:pw@localhost/mydb");
        assert_eq!(connector.dialect(), SqlDialectKind::Mariadb);
    }

    #[test]
    fn mysql_constructor_sets_mysql_dialect() {
        let connector = MySqlConnector::mysql("mysql://root:pw@localhost/mydb");
        assert_eq!(connector.dialect(), SqlDialectKind::Mysql);
    }

    #[test]
    fn mysql_capabilities_reports_mysql_dialect() {
        let connector = MySqlConnector::mysql("mysql://localhost/mydb");
        let caps = connector.capabilities();
        assert!(caps.filter, "MySQL should support filter pushdown");
        assert_eq!(caps.dialect_name, "mysql");
    }

    #[test]
    fn mysql_pool_params_sslmode_not_duplicated_when_present() {
        let params = mysql_pool_params("mysql://root:pw@localhost/mydb?sslmode=required");
        assert!(
            !params.contains_key("sslmode"),
            "sslmode already in URL, should not be inserted separately"
        );
    }

    #[test]
    fn mysql_pool_with_malformed_url_returns_error() {
        let connector = MySqlConnector::mysql("not a valid url !!!");
        assert!(connector.pool().is_err(), "malformed URL should fail");
    }

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_sort_asc_parity() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_sort_mysql_asc (id INT, label VARCHAR(64))",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_sort_mysql_asc").await;
        // Insert evens first, then odds — physical row order ≠ sort order
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_sort_mysql_asc (id, label) VALUES \
             (2,'row_2'),(4,'row_4'),(6,'row_6'),(8,'row_8'),(10,'row_10'),\
             (12,'row_12'),(14,'row_14'),(16,'row_16'),(18,'row_18'),(20,'row_20')",
        )
        .await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_sort_mysql_asc (id, label) VALUES \
             (1,'row_1'),(3,'row_3'),(5,'row_5'),(7,'row_7'),(9,'row_9'),\
             (11,'row_11'),(13,'row_13'),(15,'row_15'),(17,'row_17'),(19,'row_19')",
        )
        .await;

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_sort_mysql_asc")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_mysql_asc", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_mysql_asc ORDER BY id ASC LIMIT 10")
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_sort_desc_parity() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_sort_mysql_desc (id INT, label VARCHAR(64))",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_sort_mysql_desc").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_sort_mysql_desc (id, label) VALUES \
             (2,'row_2'),(4,'row_4'),(6,'row_6'),(8,'row_8'),(10,'row_10'),\
             (12,'row_12'),(14,'row_14'),(16,'row_16'),(18,'row_18'),(20,'row_20')",
        )
        .await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_sort_mysql_desc (id, label) VALUES \
             (1,'row_1'),(3,'row_3'),(5,'row_5'),(7,'row_7'),(9,'row_9'),\
             (11,'row_11'),(13,'row_13'),(15,'row_15'),(17,'row_17'),(19,'row_19')",
        )
        .await;

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_sort_mysql_desc")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_mysql_desc", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_mysql_desc ORDER BY id DESC LIMIT 5")
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_medium_sort_smoke() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_sort_mysql_medium (id INT, label VARCHAR(64))",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_sort_mysql_medium").await;
        // Generate 1000 rows via recursive CTE (requires MySQL 8.0+ / MariaDB 10.2+)
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_sort_mysql_medium (id, label)
             WITH RECURSIVE gen(n) AS (
               SELECT 1 UNION ALL SELECT n + 1 FROM gen WHERE n < 1000
             )
             SELECT n, CONCAT('item_', n) FROM gen",
        )
        .await;

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_sort_mysql_medium")
                .await
                .unwrap();
        let engine = QsqlEngine::new();
        engine
            .register_table("qsql_sort_mysql_medium", Arc::new(provider))
            .unwrap();
        let rows = engine
            .execute_sql_to_json("SELECT id FROM qsql_sort_mysql_medium ORDER BY id DESC LIMIT 3")
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_broadcast_join_csv_parity() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_bcast_mysql_parity
               (employee_id INT, salary DOUBLE, bonus INT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_bcast_mysql_parity").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_bcast_mysql_parity VALUES
               (1, 120000.0, 8000), (2, 95000.0, 5000),
               (3, 80000.0, 4000), (4, 70000.0, 2000)",
        )
        .await;

        let csv = mysql_temp_csv(&[
            (1, "Alice", "Engineer"),
            (2, "Bob", "Manager"),
            (3, "Charlie", "Designer"),
        ]);
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   JOIN qsql_bcast_mysql_parity c ON u.id = c.employee_id";

        let engine_on = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_parity",
            &csv,
            BroadcastRewriteConfig::default(),
        )
        .await;
        let engine_off = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_parity",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_on = mysql_collect_sorted_rows(&engine_on, sql).await;
        let rows_off = mysql_collect_sorted_rows(&engine_off, sql).await;
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_broadcast_join_empty_local_side() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_bcast_mysql_empty
               (employee_id INT, salary DOUBLE, bonus INT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_bcast_mysql_empty").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_bcast_mysql_empty VALUES (1, 1.0, 100), (2, 2.0, 200)",
        )
        .await;

        let csv = mysql_temp_csv(&[]); // empty local side — only header row
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   JOIN qsql_bcast_mysql_empty c ON u.id = c.employee_id";

        let engine_on = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_empty",
            &csv,
            BroadcastRewriteConfig::default(),
        )
        .await;
        let engine_off = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_empty",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_on = mysql_collect_sorted_rows(&engine_on, sql).await;
        let rows_off = mysql_collect_sorted_rows(&engine_off, sql).await;
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_broadcast_join_large_local_exceeds_cap() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_bcast_mysql_large
               (employee_id INT, salary DOUBLE, bonus INT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_bcast_mysql_large").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_bcast_mysql_large VALUES
               (0, 1.0, 100), (1, 2.0, 200), (49, 3.0, 300)",
        )
        .await;

        // 50-row CSV exceeds max_local_rows=10 cap
        let csv_path =
            std::env::temp_dir().join(format!("test_qsql_mysql_large_{}.csv", mysql_temp_suffix()));
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
                   JOIN qsql_bcast_mysql_large c ON u.id = c.employee_id";

        let config_capped = BroadcastRewriteConfig {
            enabled: true,
            max_local_rows: 10,
            max_local_bytes: qsql_core::broadcast::DEFAULT_MAX_LOCAL_BYTES,
        };
        let engine_capped =
            mysql_build_broadcast_engine(&url, "qsql_bcast_mysql_large", &csv, config_capped).await;
        let engine_off = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_large",
            &csv,
            BroadcastRewriteConfig::disabled(),
        )
        .await;

        let rows_capped = mysql_collect_sorted_rows(&engine_capped, sql).await;
        let rows_off = mysql_collect_sorted_rows(&engine_off, sql).await;
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_broadcast_join_left_join_not_eligible() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_bcast_mysql_left
               (employee_id INT, salary DOUBLE, bonus INT)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_bcast_mysql_left").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_bcast_mysql_left VALUES
               (1, 1.0, 10), (2, 2.0, 20), (3, 3.0, 30)",
        )
        .await;

        let csv = mysql_temp_csv(&[(1, "Alice", "E"), (2, "Bob", "M")]);
        let sql = "SELECT u.id, u.name, c.bonus \
                   FROM users u \
                   LEFT JOIN qsql_bcast_mysql_left c ON u.id = c.employee_id";
        let engine = mysql_build_broadcast_engine(
            &url,
            "qsql_bcast_mysql_left",
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_federated_cross_source_join() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_fedj_mysql_comp
               (employee_id INT, salary DOUBLE)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_fedj_mysql_comp").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_fedj_mysql_comp VALUES
               (1, 120000.0), (2, 95000.0), (3, 80000.0)",
        )
        .await;

        let csv = mysql_temp_csv(&[
            (1, "Alice", "Engineer"),
            (2, "Bob", "Manager"),
            (3, "Charlie", "Designer"),
        ]);
        let engine = QsqlEngine::new();
        engine
            .register_file("users", &csv, "csv")
            .await
            .expect("register users csv");
        let provider = MySqlTableProvider::try_new(
            url.clone(),
            SqlDialectKind::Mysql,
            None,
            "qsql_fedj_mysql_comp",
        )
        .await
        .unwrap();
        engine
            .register_table("qsql_fedj_mysql_comp", Arc::new(provider))
            .unwrap();

        let sql = "
            SELECT u.name, u.role, c.salary
            FROM users u
            JOIN qsql_fedj_mysql_comp c ON u.id = c.employee_id
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
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_same_source_join_uses_federation() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_fedj_mysql_customers
               (id INT, name TEXT, region TEXT)",
        )
        .await;
        mysql_query_drop(
            &connector,
            "CREATE TABLE IF NOT EXISTS qsql_fedj_mysql_orders
               (id INT, customer_id INT, total DOUBLE)",
        )
        .await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_fedj_mysql_customers").await;
        mysql_query_drop(&connector, "TRUNCATE TABLE qsql_fedj_mysql_orders").await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_fedj_mysql_customers VALUES
               (1, 'Acme', 'west'), (2, 'Globex', 'east')",
        )
        .await;
        mysql_query_drop(
            &connector,
            "INSERT INTO qsql_fedj_mysql_orders VALUES
               (10, 1, 125.0), (11, 1, 90.0), (12, 2, 60.0)",
        )
        .await;

        let engine = QsqlEngine::new();
        let customers = MySqlTableProvider::try_new(
            url.clone(),
            SqlDialectKind::Mysql,
            None,
            "qsql_fedj_mysql_customers",
        )
        .await
        .unwrap();
        let orders = MySqlTableProvider::try_new(
            url.clone(),
            SqlDialectKind::Mysql,
            None,
            "qsql_fedj_mysql_orders",
        )
        .await
        .unwrap();
        engine
            .register_table("qsql_fedj_mysql_customers", Arc::new(customers))
            .unwrap();
        engine
            .register_table("qsql_fedj_mysql_orders", Arc::new(orders))
            .unwrap();

        let sql = "
            SELECT c.name, SUM(o.total) AS order_total
            FROM qsql_fedj_mysql_customers c
            JOIN qsql_fedj_mysql_orders o ON c.id = o.customer_id
            WHERE c.region = 'west'
            GROUP BY c.name
        ";
        let plan = engine.get_logical_plan(sql).await.unwrap();
        let plan_text = format!("{plan:?}");
        assert!(
            plan_text.contains("Federated"),
            "expected same-source MySQL subplan to be federated, got:\n{plan_text}"
        );

        let result = engine.execute_sql_to_json(sql).await.unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Acme");
        assert_eq!(rows[0]["order_total"], 215.0);
    }
}
