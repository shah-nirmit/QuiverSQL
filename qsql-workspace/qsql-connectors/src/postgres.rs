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

    #[tokio::test]
    async fn postgres_live_select_is_env_gated() {
        let Ok(url) = std::env::var("QSQL_POSTGRES_URL") else {
            return;
        };

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
}
