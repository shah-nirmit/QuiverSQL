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

    #[test]
    fn dialect_name_covers_all_variants() {
        assert_eq!(SqlDialectKind::Sqlite.name(), "sqlite");
        assert_eq!(SqlDialectKind::Postgres.name(), "postgres");
        assert_eq!(SqlDialectKind::Mysql.name(), "mysql");
        assert_eq!(SqlDialectKind::Mariadb.name(), "mariadb");
    }

    #[test]
    fn dialect_quote_char_covers_all_variants() {
        assert_eq!(SqlDialectKind::Sqlite.quote_char(), '`');
        assert_eq!(SqlDialectKind::Postgres.quote_char(), '"');
        assert_eq!(SqlDialectKind::Mysql.quote_char(), '`');
        assert_eq!(SqlDialectKind::Mariadb.quote_char(), '`');
    }

    #[test]
    fn sql_table_ref_to_sql_whitespace_schema_falls_back_to_bare() {
        let r = SqlTableRef {
            schema: Some("  ".to_string()),
            table: "orders".to_string(),
        };
        assert_eq!(r.to_sql(SqlDialectKind::Sqlite), "`orders`");
    }

    #[test]
    fn sql_capabilities_reflects_dialect_name() {
        let caps = sql_capabilities(SqlDialectKind::Postgres);
        assert!(caps.projection);
        assert!(caps.filter);
        assert!(caps.limit);
        assert!(!caps.aggregate);
        assert!(!caps.joins);
        assert_eq!(caps.dialect_name, "postgres");
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(sql_literal("it's"), "'it''s'");
        assert_eq!(sql_literal("plain"), "'plain'");
        assert_eq!(sql_literal(""), "''");
    }

    #[test]
    fn sql_type_to_arrow_covers_all_branches() {
        assert_eq!(sql_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INT8"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INTEGER"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INT"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INT4"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INT2"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("SMALLINT"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("TINYINT"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("MEDIUMINT"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("SERIAL"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("BIGSERIAL"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("REAL"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("FLOAT4"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("NUMERIC"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("DECIMAL"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(sql_type_to_arrow("VARCHAR(255)"), DataType::Utf8);
    }

    #[test]
    fn schema_from_fields_builds_arrow_schema() {
        let schema = schema_from_fields(vec![
            ("id".to_string(), "INT".to_string(), false),
            ("name".to_string(), "TEXT".to_string(), true),
        ]);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());
    }
}
