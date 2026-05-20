//! Shared SQL connector pushdown support.
//!
//! This module follows the same shape as SpiceAI's DataFusion table provider
//! integration: DataFusion owns planning, while a connector-specific table
//! provider translates projection, supported filters, and limits into source
//! SQL.

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{BinaryExpr, InList, Like};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SqliteDialect};
use datafusion::sql::unparser::Unparser;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::RemoteConnector;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialectKind {
    Sqlite,
    Postgres,
    Mysql,
    Mariadb,
}

impl SqlDialectKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Mariadb => "mariadb",
        }
    }

    pub fn quote_char(self) -> char {
        match self {
            Self::Sqlite => '`',
            Self::Postgres => '"',
            Self::Mysql | Self::Mariadb => '`',
        }
    }

    fn unparse_filter(self, expr: &Expr) -> Result<String, String> {
        match self {
            Self::Sqlite => render_filter_with_dialect(&SqliteDialect {}, expr),
            Self::Postgres => render_filter_with_dialect(&PostgreSqlDialect {}, expr),
            Self::Mysql | Self::Mariadb => render_filter_with_dialect(&MySqlDialect {}, expr),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlTableRef {
    pub schema: Option<String>,
    pub table: String,
}

impl SqlTableRef {
    pub fn bare(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
        }
    }

    pub fn with_schema(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            table: table.into(),
        }
    }

    pub fn to_sql(&self, dialect: SqlDialectKind) -> String {
        match &self.schema {
            Some(schema) if !schema.trim().is_empty() => format!(
                "{}.{}",
                quote_identifier(schema, dialect),
                quote_identifier(&self.table, dialect)
            ),
            _ => quote_identifier(&self.table, dialect),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlPushdownPlan {
    pub sql: String,
    pub projected_columns: Vec<String>,
    pub filters: Vec<String>,
    pub limit: Option<usize>,
    pub unsupported_filters: Vec<String>,
}

pub struct SqlTableProvider {
    connector: Arc<dyn RemoteConnector>,
    dialect: SqlDialectKind,
    table_ref: SqlTableRef,
    schema: SchemaRef,
    last_sql: Arc<Mutex<Option<String>>>,
}

impl fmt::Debug for SqlTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTableProvider")
            .field("connector_type", &self.connector.connector_type())
            .field("dialect", &self.dialect)
            .field("table_ref", &self.table_ref)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl SqlTableProvider {
    pub fn new(
        connector: Arc<dyn RemoteConnector>,
        dialect: SqlDialectKind,
        table_ref: SqlTableRef,
        schema: SchemaRef,
    ) -> Self {
        Self {
            connector,
            dialect,
            table_ref,
            schema,
            last_sql: Arc::new(Mutex::new(None)),
        }
    }

    pub fn connector(&self) -> &Arc<dyn RemoteConnector> {
        &self.connector
    }

    pub fn dialect(&self) -> SqlDialectKind {
        self.dialect
    }

    pub fn table_ref(&self) -> &SqlTableRef {
        &self.table_ref
    }

    pub fn last_sql(&self) -> Option<String> {
        self.last_sql.lock().ok().and_then(|sql| sql.clone())
    }

    pub fn build_select_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SqlPushdownPlan, String> {
        build_select_sql(
            &self.schema,
            &self.table_ref,
            self.dialect,
            projection,
            filters,
            limit,
        )
    }
}

#[async_trait]
impl TableProvider for SqlTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let pushdown = self
            .build_select_sql(projection, filters, limit)
            .map_err(|e| DataFusionError::External(e.into()))?;

        if let Ok(mut last_sql) = self.last_sql.lock() {
            *last_sql = Some(pushdown.sql.clone());
        }

        let scan_schema = scan_schema(&self.schema, projection)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;
        let rows = self
            .connector
            .execute_query(&pushdown.sql)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let batch = json_rows_to_record_batch(&rows, scan_schema)
            .map_err(|e| DataFusionError::External(e.into()))?;
        let projected_batch = match projection {
            Some(indices) if indices.is_empty() => batch
                .project(indices)
                .map_err(|e| DataFusionError::ArrowError(e, None))?,
            _ => batch,
        };

        let projected_schema = projected_batch.schema();
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![projected_batch]],
            projected_schema,
            None,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(
                |filter| match render_supported_filter(self.dialect, filter) {
                    Ok(_) => TableProviderFilterPushDown::Exact,
                    Err(_) => TableProviderFilterPushDown::Unsupported,
                },
            )
            .collect())
    }
}

pub fn build_select_sql(
    schema: &SchemaRef,
    table_ref: &SqlTableRef,
    dialect: SqlDialectKind,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<SqlPushdownPlan, String> {
    let selected_indices = selected_scan_indices(schema, projection)?;
    let projected_columns = selected_indices
        .iter()
        .map(|index| schema.field(*index).name().clone())
        .collect::<Vec<_>>();
    let select_list = projected_columns
        .iter()
        .map(|name| quote_identifier(name, dialect))
        .collect::<Vec<_>>()
        .join(", ");

    let filter_sql = filters
        .iter()
        .map(|filter| render_supported_filter(dialect, filter))
        .collect::<Result<Vec<_>, _>>()?;

    let mut sql = format!("SELECT {select_list} FROM {}", table_ref.to_sql(dialect));
    if !filter_sql.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&filter_sql.join(" AND "));
    }
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }

    Ok(SqlPushdownPlan {
        sql,
        projected_columns,
        filters: filter_sql,
        limit,
        unsupported_filters: Vec::new(),
    })
}

pub fn sql_capabilities(dialect: SqlDialectKind) -> qsql_core::models::ConnectorCapabilities {
    qsql_core::models::ConnectorCapabilities {
        projection: true,
        filter: true,
        limit: true,
        aggregate: false,
        joins: false,
        dialect_name: dialect.name().to_string(),
    }
}

pub fn quote_identifier(identifier: &str, dialect: SqlDialectKind) -> String {
    let quote = dialect.quote_char();
    let escaped = identifier.replace(quote, &format!("{quote}{quote}"));
    format!("{quote}{escaped}{quote}")
}

pub fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn sql_type_to_arrow(sql_type: &str) -> DataType {
    let upper = sql_type.to_uppercase();
    if upper.contains("BOOL") {
        DataType::Boolean
    } else if upper.contains("BIGINT")
        || upper.contains("INT8")
        || upper.contains("INTEGER")
        || upper == "INT"
        || upper == "INT4"
        || upper == "INT2"
        || upper == "SMALLINT"
        || upper == "TINYINT"
        || upper == "MEDIUMINT"
        || upper == "SERIAL"
        || upper == "BIGSERIAL"
    {
        DataType::Int64
    } else if upper.contains("REAL")
        || upper.contains("FLOA")
        || upper.contains("DOUB")
        || upper.contains("NUM")
        || upper.contains("DEC")
    {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

pub fn json_rows_to_record_batch(
    rows: &[serde_json::Value],
    schema: SchemaRef,
) -> Result<RecordBatch, String> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in rows {
                    match row.get(field.name()) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(value) = n.as_i64().or_else(|| n.as_u64().map(|n| n as i64))
                            {
                                builder.append_value(value);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(serde_json::Value::String(s)) => match s.parse::<i64>() {
                            Ok(value) => builder.append_value(value),
                            Err(_) => builder.append_null(),
                        },
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            builder.append_value(
                                v.as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0),
                            );
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in rows {
                    match row.get(field.name()) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(value) = n.as_f64() {
                                builder.append_value(value);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(serde_json::Value::String(s)) => match s.parse::<f64>() {
                            Ok(value) => builder.append_value(value),
                            Err(_) => builder.append_null(),
                        },
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            builder.append_value(
                                v.as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0),
                            );
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in rows {
                    match row.get(field.name()) {
                        Some(serde_json::Value::Bool(b)) => builder.append_value(*b),
                        Some(serde_json::Value::Number(n)) => {
                            builder.append_value(n.as_i64().unwrap_or(0) != 0);
                        }
                        Some(serde_json::Value::String(s)) => match s.to_lowercase().as_str() {
                            "true" | "t" | "1" | "yes" => builder.append_value(true),
                            "false" | "f" | "0" | "no" => builder.append_value(false),
                            _ => builder.append_null(),
                        },
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(_) => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    match row.get(field.name()) {
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(serde_json::Value::String(s)) => builder.append_value(s),
                        Some(other) => builder.append_value(other.to_string()),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

fn selected_scan_indices(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<Vec<usize>, String> {
    match projection {
        Some(indices) if !indices.is_empty() => {
            validate_projection(schema, indices)?;
            Ok(indices.clone())
        }
        _ => Ok((0..schema.fields().len()).collect()),
    }
}

fn scan_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<SchemaRef, datafusion::arrow::error::ArrowError> {
    match projection {
        Some(indices) if !indices.is_empty() => Ok(Arc::new(schema.project(indices)?)),
        _ => Ok(schema.clone()),
    }
}

fn validate_projection(schema: &SchemaRef, projection: &[usize]) -> Result<(), String> {
    for index in projection {
        if *index >= schema.fields().len() {
            return Err(format!(
                "Projection index {index} out of bounds for schema with {} fields",
                schema.fields().len()
            ));
        }
    }
    Ok(())
}

fn render_supported_filter(dialect: SqlDialectKind, expr: &Expr) -> Result<String, String> {
    if !filter_supported(expr) {
        return Err(format!("Unsupported filter expression: {expr}"));
    }
    let normalized = strip_column_qualifiers(expr);
    dialect.unparse_filter(&normalized)
}

fn render_filter_with_dialect(dialect: &dyn Dialect, expr: &Expr) -> Result<String, String> {
    let unparser = Unparser::new(dialect);
    unparser
        .expr_to_sql(expr)
        .map(|expr| expr.to_string())
        .map_err(|e| e.to_string())
}

fn filter_supported(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => filter_supported(&alias.expr),
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And | Operator::Or => {
                filter_supported(&binary.left) && filter_supported(&binary.right)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                scalar_operand_supported(&binary.left) && scalar_operand_supported(&binary.right)
            }
            Operator::LikeMatch | Operator::NotLikeMatch => {
                scalar_operand_supported(&binary.left) && scalar_operand_supported(&binary.right)
            }
            _ => false,
        },
        Expr::Like(like) => {
            !like.case_insensitive
                && scalar_operand_supported(&like.expr)
                && scalar_operand_supported(&like.pattern)
        }
        Expr::InList(list) => {
            scalar_operand_supported(&list.expr)
                && list
                    .list
                    .iter()
                    .all(|expr| matches!(expr, Expr::Literal(_)))
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => scalar_operand_supported(expr),
        _ => false,
    }
}

fn scalar_operand_supported(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Alias(alias) => scalar_operand_supported(&alias.expr),
        _ => false,
    }
}

fn strip_column_qualifiers(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => strip_column_qualifiers(&alias.expr),
        Expr::Column(column) => Expr::Column(Column::new_unqualified(column.name.clone())),
        Expr::Literal(_) => expr.clone(),
        Expr::BinaryExpr(binary) => Expr::BinaryExpr(BinaryExpr::new(
            Box::new(strip_column_qualifiers(&binary.left)),
            binary.op,
            Box::new(strip_column_qualifiers(&binary.right)),
        )),
        Expr::Like(like) => Expr::Like(Like::new(
            like.negated,
            Box::new(strip_column_qualifiers(&like.expr)),
            Box::new(strip_column_qualifiers(&like.pattern)),
            like.escape_char,
            like.case_insensitive,
        )),
        Expr::InList(list) => Expr::InList(InList::new(
            Box::new(strip_column_qualifiers(&list.expr)),
            list.list.iter().map(strip_column_qualifiers).collect(),
            list.negated,
        )),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(strip_column_qualifiers(inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(strip_column_qualifiers(inner))),
        _ => expr.clone(),
    }
}

pub fn schema_from_fields(fields: Vec<(String, String, bool)>) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, sql_type, nullable)| {
                Field::new(name, sql_type_to_arrow(&sql_type), nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("select", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    #[test]
    fn emits_sqlite_projection_filter_and_limit() {
        let sql = build_select_sql(
            &schema(),
            &SqlTableRef::bare("products"),
            SqlDialectKind::Sqlite,
            Some(&vec![1, 2]),
            &[col("price").gt(lit(1.0))],
            Some(10),
        )
        .unwrap()
        .sql;

        assert_eq!(
            sql,
            "SELECT `select`, `price` FROM `products` WHERE (`price` > 1.0) LIMIT 10"
        );
    }

    #[test]
    fn emits_postgres_schema_qualified_sql() {
        let sql = build_select_sql(
            &schema(),
            &SqlTableRef::with_schema("sales", "Orders"),
            SqlDialectKind::Postgres,
            Some(&vec![0]),
            &[col("id").eq(lit(7_i64))],
            None,
        )
        .unwrap()
        .sql;

        assert_eq!(
            sql,
            "SELECT \"id\" FROM \"sales\".\"Orders\" WHERE (\"id\" = 7)"
        );
    }

    #[test]
    fn emits_mysql_identifier_quotes_and_escaped_string() {
        let sql = build_select_sql(
            &schema(),
            &SqlTableRef::with_schema("shop", "products"),
            SqlDialectKind::Mysql,
            Some(&vec![1]),
            &[col("select").eq(lit("O'Brien"))],
            None,
        )
        .unwrap()
        .sql;

        assert_eq!(
            sql,
            "SELECT `select` FROM `shop`.`products` WHERE (`select` = 'O''Brien')"
        );
    }

    #[test]
    fn rejects_unsupported_functions() {
        let expr = datafusion::prelude::length(col("select")).gt(lit(3_i64));
        let err = build_select_sql(
            &schema(),
            &SqlTableRef::bare("products"),
            SqlDialectKind::Sqlite,
            None,
            &[expr],
            None,
        )
        .unwrap_err();

        assert!(err.contains("Unsupported filter expression"));
    }
}
