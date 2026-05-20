//! MySQL and MariaDB connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use mysql_async::{prelude::Queryable, Opts, Pool, Row, Value};
use std::sync::Arc;

use crate::sql::{
    schema_from_fields, sql_capabilities, sql_literal, SqlDialectKind, SqlPushdownPlan,
    SqlTableProvider, SqlTableRef,
};
use crate::RemoteConnector;

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

    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities {
        sql_capabilities(self.dialect)
    }

    async fn execute_query(&self, sql: &str) -> Result<Vec<serde_json::Value>, String> {
        let pool = self.pool()?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| format!("Failed to connect to MySQL/MariaDB: {e}"))?;
        let rows: Vec<Row> = conn
            .query(sql)
            .await
            .map_err(|e| format!("MySQL/MariaDB query failed: {e}"))?;
        drop(conn);
        pool.disconnect()
            .await
            .map_err(|e| format!("Failed to close MySQL/MariaDB connection pool: {e}"))?;
        Ok(rows.iter().map(mysql_row_to_json).collect())
    }
}

#[derive(Debug)]
pub struct MySqlTableProvider {
    connector: Arc<MySqlConnector>,
    inner: SqlTableProvider,
}

impl MySqlTableProvider {
    pub async fn try_new(
        connection_string: impl Into<String>,
        dialect: SqlDialectKind,
        schema_name: Option<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        let connection_string = connection_string.into();
        let table_name = table_name.into();
        let connector = Arc::new(MySqlConnector::new(connection_string, dialect));
        let schema =
            introspect_mysql_schema(&connector, schema_name.as_deref(), &table_name).await?;
        let table_ref = match schema_name {
            Some(schema) if !schema.trim().is_empty() => {
                SqlTableRef::with_schema(schema, table_name)
            }
            _ => SqlTableRef::bare(table_name),
        };
        let inner = SqlTableProvider::new(connector.clone(), dialect, table_ref, schema);

        Ok(Self { connector, inner })
    }

    pub fn connector(&self) -> &Arc<MySqlConnector> {
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
}

#[async_trait]
impl TableProvider for MySqlTableProvider {
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

async fn introspect_mysql_schema(
    connector: &MySqlConnector,
    schema_name: Option<&str>,
    table_name: &str,
) -> Result<SchemaRef, String> {
    let schema_predicate = match schema_name {
        Some(schema) if !schema.trim().is_empty() => {
            format!("TABLE_SCHEMA = {}", sql_literal(schema))
        }
        _ => "TABLE_SCHEMA = DATABASE()".to_string(),
    };
    let sql = format!(
        "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable \
         FROM information_schema.COLUMNS \
         WHERE {schema_predicate} AND TABLE_NAME = {} \
         ORDER BY ORDINAL_POSITION",
        sql_literal(table_name)
    );

    let rows = connector.execute_query(&sql).await?;
    if rows.is_empty() {
        return Err(format!(
            "Table '{}' not found or has no columns",
            match schema_name {
                Some(schema) if !schema.trim().is_empty() => format!("{schema}.{table_name}"),
                _ => table_name.to_string(),
            }
        ));
    }

    Ok(schema_from_fields(
        rows.into_iter()
            .filter_map(|row| {
                let name = row.get("column_name")?.as_str()?.to_string();
                let sql_type = row.get("data_type")?.as_str()?.to_string();
                let nullable = row
                    .get("is_nullable")
                    .and_then(|value| value.as_str())
                    .map(|value| value.eq_ignore_ascii_case("YES"))
                    .unwrap_or(true);
                Some((name, sql_type, nullable))
            })
            .collect(),
    ))
}

fn mysql_row_to_json(row: &Row) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (idx, column) in row.columns_ref().iter().enumerate() {
        let name = column.name_str().to_string();
        let value = row
            .as_ref(idx)
            .map(mysql_value_to_json)
            .unwrap_or(serde_json::Value::Null);
        obj.insert(name, value);
    }
    serde_json::Value::Object(obj)
}

fn mysql_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::NULL => serde_json::Value::Null,
        Value::Bytes(bytes) => {
            serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned())
        }
        Value::Int(value) => serde_json::json!(value),
        Value::UInt(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(*value as f64),
        Value::Double(value) => serde_json::json!(value),
        Value::Date(year, month, day, hour, minute, second, micros) => serde_json::Value::String(
            format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"),
        ),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            serde_json::Value::String(format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};
    use std::ops::Add;

    #[tokio::test]
    #[cfg_attr(
        not(qsql_live_mysql_tests),
        ignore = "requires a live MySQL/MariaDB database and QSQL_MYSQL_URL"
    )]
    async fn mysql_live_select_requires_env() {
        let url = std::env::var("QSQL_MYSQL_URL")
            .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");

        let connector = MySqlConnector::mysql(url.clone());
        connector
            .execute_query("CREATE TABLE IF NOT EXISTS qsql_phase4_mysql (id INT, name TEXT)")
            .await
            .unwrap();
        connector
            .execute_query("TRUNCATE TABLE qsql_phase4_mysql")
            .await
            .unwrap();
        connector
            .execute_query("INSERT INTO qsql_phase4_mysql VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .unwrap();

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_phase4_mysql")
                .await
                .unwrap();
        let sql = provider
            .build_select_sql(Some(&vec![1]), &[col("id").eq(lit(1_i64))], Some(1))
            .unwrap()
            .sql;

        assert_eq!(
            sql,
            "SELECT `name` FROM `qsql_phase4_mysql` WHERE (`id` = 1) LIMIT 1"
        );
        let rows = connector.execute_query(&sql).await.unwrap();
        assert_eq!(rows.len(), 1);
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
        connector
            .execute_query("CREATE TABLE IF NOT EXISTS qsql_phase4_mysql_pushdowns (id INT, name TEXT, price FLOAT)")
            .await
            .unwrap();
        connector
            .execute_query("TRUNCATE TABLE qsql_phase4_mysql_pushdowns")
            .await
            .unwrap();
        connector
            .execute_query("INSERT INTO qsql_phase4_mysql_pushdowns VALUES (1, 'Alice', 10.0), (2, 'Bob', 20.5), (3, NULL, 5.0), (4, 'Dave', NULL)")
            .await
            .unwrap();

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_phase4_mysql_pushdowns")
                .await
                .unwrap();

        // Test Between and Arithmetic
        let sql1 = provider
            .build_select_sql(None, &[col("price").add(lit(5.0)).between(lit(11.0), lit(20.0))], None)
            .unwrap()
            .sql;
        let rows1 = connector.execute_query(&sql1).await.unwrap();
        assert_eq!(rows1.len(), 1); // Alice
        assert_eq!(rows1[0]["name"], "Alice");

        // Test Not and IsNull
        let sql2 = provider
            .build_select_sql(None, &[datafusion::logical_expr::expr::Expr::Not(Box::new(col("name").is_null()))], None)
            .unwrap()
            .sql;
        let rows2 = connector.execute_query(&sql2).await.unwrap();
        assert_eq!(rows2.len(), 3); // Alice, Bob, Dave
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
        connector
            .execute_query("CREATE TABLE IF NOT EXISTS qsql_phase4_mysql_complex (id INT, category TEXT, score FLOAT)")
            .await
            .unwrap();
        connector
            .execute_query("TRUNCATE TABLE qsql_phase4_mysql_complex")
            .await
            .unwrap();
        connector
            .execute_query("INSERT INTO qsql_phase4_mysql_complex VALUES (1, 'Alpha', 9.5), (2, 'Beta', 8.0), (3, 'Gamma', 4.0), (4, 'Alpha', 3.0)")
            .await
            .unwrap();

        let provider =
            MySqlTableProvider::try_new(url, SqlDialectKind::Mysql, None, "qsql_phase4_mysql_complex")
                .await
                .unwrap();

        // category LIKE 'A%' AND id IN (1, 3, 4) AND (score > 9.0 OR score < 5.0)
        let sql = provider
            .build_select_sql(
                None,
                &[
                    col("category").like(lit("A%")),
                    col("id").in_list(vec![lit(1_i64), lit(3_i64), lit(4_i64)], false),
                    col("score").gt(lit(9.0)).or(col("score").lt(lit(5.0))),
                ],
                None,
            )
            .unwrap()
            .sql;
        
        let rows = connector.execute_query(&sql).await.unwrap();
        assert_eq!(rows.len(), 2); // id 1 (Alpha, 9.5) and id 4 (Alpha, 3.0)
    }
}
