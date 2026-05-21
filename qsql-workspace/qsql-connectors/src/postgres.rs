//! Postgres connector for QuiverSQL.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio_postgres::{types::Type, NoTls, Row};

use crate::sql::{
    schema_from_fields, sql_capabilities, SqlDialectKind, SqlPushdownPlan, SqlTableProvider,
    SqlTableRef,
};
use crate::RemoteConnector;

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

    async fn explain_query(&self, sql: &str) -> Result<String, String> {
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

    async fn execute_query(&self, sql: &str) -> Result<Vec<serde_json::Value>, String> {
        let client = self.client().await?;
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("Postgres query failed: {e}"))?;

        Ok(rows.iter().map(postgres_row_to_json).collect())
    }
}

#[derive(Debug)]
pub struct PostgresTableProvider {
    connector: Arc<PostgresConnector>,
    inner: SqlTableProvider,
}

impl PostgresTableProvider {
    pub async fn try_new(
        connection_string: impl Into<String>,
        schema_name: Option<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        let connection_string = connection_string.into();
        let table_name = table_name.into();
        let schema_name = schema_name.unwrap_or_else(|| "public".to_string());
        let connector = Arc::new(PostgresConnector::new(connection_string));
        let schema = introspect_postgres_schema(&connector, &schema_name, &table_name).await?;
        let inner = SqlTableProvider::new(
            connector.clone(),
            SqlDialectKind::Postgres,
            SqlTableRef::with_schema(schema_name, table_name),
            schema,
        );

        Ok(Self { connector, inner })
    }

    pub fn connector(&self) -> &Arc<PostgresConnector> {
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
impl TableProvider for PostgresTableProvider {
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

async fn introspect_postgres_schema(
    connector: &PostgresConnector,
    schema_name: &str,
    table_name: &str,
) -> Result<SchemaRef, String> {
    let client = connector.client().await?;
    let rows = client
        .query(
            r#"
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#,
            &[&schema_name, &table_name],
        )
        .await
        .map_err(|e| format!("Postgres schema introspection failed: {e}"))?;

    if rows.is_empty() {
        return Err(format!(
            "Table '{}.{}' not found or has no columns",
            schema_name, table_name
        ));
    }

    Ok(schema_from_fields(
        rows.into_iter()
            .map(|row| {
                let name: String = row.get(0);
                let sql_type: String = row.get(1);
                let is_nullable: String = row.get(2);
                (name, sql_type, is_nullable.eq_ignore_ascii_case("YES"))
            })
            .collect(),
    ))
}

fn postgres_row_to_json(row: &Row) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (idx, column) in row.columns().iter().enumerate() {
        let value = postgres_value_to_json(row, idx, column.type_());
        obj.insert(column.name().to_string(), value);
    }
    serde_json::Value::Object(obj)
}

fn postgres_value_to_json(row: &Row, idx: usize, ty: &Type) -> serde_json::Value {
    match *ty {
        Type::BOOL => row
            .try_get::<usize, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null),
        Type::INT2 => row
            .try_get::<usize, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        Type::INT4 => row
            .try_get::<usize, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        Type::INT8 => row
            .try_get::<usize, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        Type::FLOAT4 => row
            .try_get::<usize, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::json!(v as f64))
            .unwrap_or(serde_json::Value::Null),
        Type::FLOAT8 => row
            .try_get::<usize, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
            .try_get::<usize, Option<String>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
        _ => row
            .try_get::<usize, Option<String>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};
    use std::ops::Add;

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
        let sql = provider
            .build_select_sql(Some(&vec![1]), &[col("id").eq(lit(1_i64))], Some(1))
            .unwrap()
            .sql;

        assert_eq!(
            sql,
            "SELECT \"name\" FROM \"public\".\"qsql_phase4_pg\" WHERE (\"id\" = 1) LIMIT 1"
        );
        let rows = connector.execute_query(&sql).await.unwrap();
        assert_eq!(rows.len(), 1);
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

        // Test Between and Arithmetic
        let sql1 = provider
            .build_select_sql(None, &[col("price").add(lit(5.0)).between(lit(11.0), lit(20.0))], None)
            .unwrap()
            .sql;
        let rows1 = connector.execute_query(&sql1).await.unwrap();
        assert_eq!(rows1.len(), 1); // 10.0 + 5 = 15 (Between 10 and 20) => Alice
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
