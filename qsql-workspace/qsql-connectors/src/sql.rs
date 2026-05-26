//! Shared SQL connector utilities.
//!
//! Runtime pushdown and scan execution are delegated to
//! `datafusion-table-providers` and `datafusion-federation`. This module keeps
//! only QuiverSQL-owned dialect metadata needed for catalog capabilities,
//! source-native explain labels, and lightweight schema helpers.

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
        sort: true,
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

pub fn sql_type_to_arrow(sql_type: &str) -> Result<DataType, String> {
    let upper = sql_type.trim().to_uppercase();

    // Detect and strip UNSIGNED/SIGNED modifier (may appear after a length spec
    // e.g. "TINYINT(1) UNSIGNED").
    let (without_modifier, unsigned) = if upper.ends_with(" UNSIGNED") {
        (&upper[..upper.len() - 9], true)
    } else if upper.ends_with(" SIGNED") {
        (&upper[..upper.len() - 7], false)
    } else {
        (upper.as_str(), false)
    };

    // Strip length/precision suffix: VARCHAR(255) → VARCHAR, DECIMAL(10,2) → DECIMAL
    let base = match without_modifier.find('(') {
        Some(idx) => without_modifier[..idx].trim(),
        None => without_modifier.trim(),
    };

    match base {
        // ── Boolean ──────────────────────────────────────────────────────────
        "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),

        // ── Integers ─────────────────────────────────────────────────────────
        "TINYINT" | "INT1" => {
            if unsigned {
                Ok(DataType::UInt8)
            } else {
                Ok(DataType::Int8)
            }
        }
        "SMALLINT" | "INT2" | "SMALLSERIAL" | "SERIAL2" => {
            if unsigned {
                Ok(DataType::UInt16)
            } else {
                Ok(DataType::Int16)
            }
        }
        "MEDIUMINT" | "INT" | "INTEGER" | "INT4" | "SERIAL" | "SERIAL4" => {
            if unsigned {
                Ok(DataType::UInt32)
            } else {
                Ok(DataType::Int32)
            }
        }
        "BIGINT" | "INT8" | "BIGSERIAL" | "SERIAL8" => {
            if unsigned {
                Ok(DataType::UInt64)
            } else {
                Ok(DataType::Int64)
            }
        }
        "YEAR" => Ok(DataType::Int16),

        // ── OID / system types ───────────────────────────────────────────────
        "OID" | "XID" | "XID8" | "CID" | "REGCLASS" | "REGTYPE" | "REGPROC" => Ok(DataType::UInt32),

        // ── Floating point ───────────────────────────────────────────────────
        "REAL" | "FLOAT" | "FLOAT4" => Ok(DataType::Float32),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Float64),
        // TODO: use Decimal128(precision, scale) once the parametrised form is parsed
        "NUMERIC" | "DECIMAL" | "DEC" | "MONEY" => Ok(DataType::Float64),

        // ── Date / time ──────────────────────────────────────────────────────
        "DATE" => Ok(DataType::Date32),
        "TIME" | "TIME WITHOUT TIME ZONE" | "TIMETZ" | "TIME WITH TIME ZONE" => {
            Ok(DataType::Time64(TimeUnit::Microsecond))
        }
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME" => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "INTERVAL" => Ok(DataType::Duration(TimeUnit::Microsecond)),

        // ── Binary ───────────────────────────────────────────────────────────
        "BYTEA" | "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB"
        | "BIT" | "VARBIT" | "BIT VARYING" => Ok(DataType::Binary),

        // ── Text / string ────────────────────────────────────────────────────
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "CHAR" | "VARCHAR"
        | "CHARACTER VARYING" | "CHARACTER" | "NCHAR" | "NVARCHAR" | "JSON" | "JSONB"
        | "JSONPATH" | "UUID" | "INET" | "CIDR" | "MACADDR" | "MACADDR8" | "XML" | "TSVECTOR"
        | "TSQUERY" | "ENUM" | "SET" | "PG_LSN" | "PG_SNAPSHOT" => Ok(DataType::Utf8),

        // ── Geometry / spatial ───────────────────────────────────────────────
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "LINE" | "LSEG" | "BOX" | "PATH" | "CIRCLE" => {
            Ok(DataType::Binary)
        }

        // ── SQLite typeless columns return an empty string ───────────────────
        "" => Ok(DataType::Utf8),

        _ => Err(format!(
            "Unrecognised SQL type: '{}'. Cannot determine Arrow DataType.",
            sql_type
        )),
    }
}

pub fn schema_from_fields(fields: Vec<(String, String, bool)>) -> Result<SchemaRef, String> {
    let arrow_fields = fields
        .into_iter()
        .map(|(name, sql_type, nullable)| {
            sql_type_to_arrow(&sql_type).map(|dt| Field::new(name, dt, nullable))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
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
        assert!(caps.sort);
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
    fn integer_types_map_to_correct_widths() {
        assert_eq!(sql_type_to_arrow("TINYINT").unwrap(), DataType::Int8);
        assert_eq!(sql_type_to_arrow("INT1").unwrap(), DataType::Int8);
        assert_eq!(sql_type_to_arrow("SMALLINT").unwrap(), DataType::Int16);
        assert_eq!(sql_type_to_arrow("INT2").unwrap(), DataType::Int16);
        assert_eq!(sql_type_to_arrow("SMALLSERIAL").unwrap(), DataType::Int16);
        assert_eq!(sql_type_to_arrow("INT").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("INT4").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("MEDIUMINT").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("SERIAL").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(sql_type_to_arrow("INT8").unwrap(), DataType::Int64);
        assert_eq!(sql_type_to_arrow("BIGSERIAL").unwrap(), DataType::Int64);
    }

    #[test]
    fn unsigned_integer_types_map_to_uint() {
        assert_eq!(
            sql_type_to_arrow("TINYINT UNSIGNED").unwrap(),
            DataType::UInt8
        );
        assert_eq!(
            sql_type_to_arrow("SMALLINT UNSIGNED").unwrap(),
            DataType::UInt16
        );
        assert_eq!(sql_type_to_arrow("INT UNSIGNED").unwrap(), DataType::UInt32);
        assert_eq!(
            sql_type_to_arrow("INTEGER UNSIGNED").unwrap(),
            DataType::UInt32
        );
        assert_eq!(
            sql_type_to_arrow("BIGINT UNSIGNED").unwrap(),
            DataType::UInt64
        );
        assert_eq!(
            sql_type_to_arrow("TINYINT(1) UNSIGNED").unwrap(),
            DataType::UInt8
        );
        assert_eq!(sql_type_to_arrow("INT SIGNED").unwrap(), DataType::Int32);
    }

    #[test]
    fn float_types_map_to_correct_widths() {
        assert_eq!(sql_type_to_arrow("REAL").unwrap(), DataType::Float32);
        assert_eq!(sql_type_to_arrow("FLOAT").unwrap(), DataType::Float32);
        assert_eq!(sql_type_to_arrow("FLOAT4").unwrap(), DataType::Float32);
        assert_eq!(sql_type_to_arrow("DOUBLE").unwrap(), DataType::Float64);
        assert_eq!(
            sql_type_to_arrow("DOUBLE PRECISION").unwrap(),
            DataType::Float64
        );
        assert_eq!(sql_type_to_arrow("FLOAT8").unwrap(), DataType::Float64);
        assert_eq!(sql_type_to_arrow("NUMERIC").unwrap(), DataType::Float64);
        assert_eq!(sql_type_to_arrow("DECIMAL").unwrap(), DataType::Float64);
        assert_eq!(
            sql_type_to_arrow("DECIMAL(10,2)").unwrap(),
            DataType::Float64
        );
        assert_eq!(sql_type_to_arrow("MONEY").unwrap(), DataType::Float64);
    }

    #[test]
    fn boolean_type_maps_to_boolean() {
        assert_eq!(sql_type_to_arrow("BOOL").unwrap(), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("BOOLEAN").unwrap(), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("boolean").unwrap(), DataType::Boolean);
    }

    #[test]
    fn date_time_types_map_correctly() {
        assert_eq!(sql_type_to_arrow("DATE").unwrap(), DataType::Date32);
        assert_eq!(
            sql_type_to_arrow("TIME").unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            sql_type_to_arrow("TIME WITHOUT TIME ZONE").unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            sql_type_to_arrow("TIMETZ").unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            sql_type_to_arrow("TIMESTAMP").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            sql_type_to_arrow("TIMESTAMP WITHOUT TIME ZONE").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            sql_type_to_arrow("DATETIME").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            sql_type_to_arrow("TIMESTAMP WITH TIME ZONE").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            sql_type_to_arrow("TIMESTAMPTZ").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            sql_type_to_arrow("INTERVAL").unwrap(),
            DataType::Duration(TimeUnit::Microsecond)
        );
        assert_eq!(sql_type_to_arrow("YEAR").unwrap(), DataType::Int16);
    }

    #[test]
    fn binary_types_map_to_binary() {
        for t in &[
            "BYTEA",
            "BINARY",
            "VARBINARY",
            "TINYBLOB",
            "BLOB",
            "MEDIUMBLOB",
            "LONGBLOB",
            "BIT",
            "VARBIT",
            "BIT VARYING",
        ] {
            assert_eq!(
                sql_type_to_arrow(t).unwrap(),
                DataType::Binary,
                "{t} should map to Binary"
            );
        }
    }

    #[test]
    fn text_safe_types_map_to_utf8() {
        for t in &[
            "TEXT",
            "TINYTEXT",
            "MEDIUMTEXT",
            "LONGTEXT",
            "CHAR",
            "VARCHAR",
            "CHARACTER VARYING",
            "CHARACTER",
            "NCHAR",
            "NVARCHAR",
            "JSON",
            "JSONB",
            "UUID",
            "INET",
            "CIDR",
            "MACADDR",
            "XML",
            "TSVECTOR",
            "TSQUERY",
            "ENUM",
            "SET",
        ] {
            assert_eq!(
                sql_type_to_arrow(t).unwrap(),
                DataType::Utf8,
                "{t} should map to Utf8"
            );
        }
    }

    #[test]
    fn geometry_types_map_to_binary() {
        for t in &[
            "GEOMETRY",
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
            "GEOMETRYCOLLECTION",
            "LINE",
            "LSEG",
            "BOX",
            "PATH",
            "CIRCLE",
        ] {
            assert_eq!(
                sql_type_to_arrow(t).unwrap(),
                DataType::Binary,
                "{t} should map to Binary"
            );
        }
    }

    #[test]
    fn oid_types_map_to_uint32() {
        for t in &[
            "OID", "XID", "XID8", "CID", "REGCLASS", "REGTYPE", "REGPROC",
        ] {
            assert_eq!(
                sql_type_to_arrow(t).unwrap(),
                DataType::UInt32,
                "{t} should map to UInt32"
            );
        }
    }

    #[test]
    fn parametrised_types_strip_length() {
        assert_eq!(sql_type_to_arrow("VARCHAR(255)").unwrap(), DataType::Utf8);
        assert_eq!(
            sql_type_to_arrow("TIMESTAMP(6)").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(sql_type_to_arrow("CHAR(10)").unwrap(), DataType::Utf8);
        assert_eq!(sql_type_to_arrow("TINYINT(1)").unwrap(), DataType::Int8);
    }

    #[test]
    fn case_insensitive_matching() {
        assert_eq!(sql_type_to_arrow("int").unwrap(), DataType::Int32);
        assert_eq!(sql_type_to_arrow("Boolean").unwrap(), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("text").unwrap(), DataType::Utf8);
        assert_eq!(
            sql_type_to_arrow("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn empty_type_string_maps_to_utf8() {
        assert_eq!(sql_type_to_arrow("").unwrap(), DataType::Utf8);
        assert_eq!(sql_type_to_arrow("  ").unwrap(), DataType::Utf8);
    }

    #[test]
    fn unknown_type_returns_error() {
        let err = sql_type_to_arrow("XYZFOO").unwrap_err();
        assert!(err.contains("XYZFOO"), "error should include the type name");
        let err2 = sql_type_to_arrow("NOTATYPE").unwrap_err();
        assert!(err2.contains("NOTATYPE"));
    }

    #[test]
    fn schema_from_fields_builds_arrow_schema() {
        let schema = schema_from_fields(vec![
            ("id".to_string(), "INT".to_string(), false),
            ("name".to_string(), "TEXT".to_string(), true),
        ])
        .unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn schema_from_fields_propagates_unknown_type_error() {
        let result = schema_from_fields(vec![
            ("id".to_string(), "INT".to_string(), false),
            ("data".to_string(), "XYZFOO".to_string(), true),
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("XYZFOO"));
    }
}
