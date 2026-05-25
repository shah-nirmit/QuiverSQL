//! Shared SQL connector utilities.
//!
//! Runtime pushdown and scan execution are delegated to
//! `datafusion-table-providers` and `datafusion-federation`. This module keeps
//! only QuiverSQL-owned dialect metadata needed for catalog capabilities,
//! source-native explain labels, and lightweight schema helpers.

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

pub fn native_select_all_sql(table_ref: &SqlTableRef, dialect: SqlDialectKind) -> String {
    format!("SELECT * FROM {}", table_ref.to_sql(dialect))
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

    #[test]
    fn quotes_reserved_and_mixed_case_identifiers() {
        assert_eq!(
            quote_identifier("select", SqlDialectKind::Postgres),
            "\"select\""
        );
        assert_eq!(
            quote_identifier("we`ird", SqlDialectKind::Mysql),
            "`we``ird`"
        );
    }

    #[test]
    fn native_select_all_uses_dialect_qualified_names() {
        assert_eq!(
            native_select_all_sql(
                &SqlTableRef::with_schema("sales", "Orders"),
                SqlDialectKind::Postgres
            ),
            "SELECT * FROM \"sales\".\"Orders\""
        );
        assert_eq!(
            native_select_all_sql(&SqlTableRef::bare("products"), SqlDialectKind::Sqlite),
            "SELECT * FROM `products`"
        );
    }
}
