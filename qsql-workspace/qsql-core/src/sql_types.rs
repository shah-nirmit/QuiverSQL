//! Shared SQL-string → Arrow `DataType` mapping.
//!
//! Relocated from `qsql-connectors/src/sql.rs` in Phase 8 so the new
//! `fixed_width` module in `qsql-core` can reuse the same type-name
//! vocabulary without creating a `qsql-core → qsql-connectors` cycle.
//! `qsql-connectors` re-exports the symbols below verbatim, so existing
//! callers (`sql_type_to_arrow(...)`, `schema_from_fields(...)`) keep
//! working unchanged.

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Maps an arbitrary SQL type spelling (Postgres / MySQL / SQLite / ANSI)
/// to the most appropriate Arrow `DataType`. Handles `UNSIGNED`/`SIGNED`
/// modifiers and parameterised forms like `VARCHAR(255)` or `DECIMAL(10,2)`.
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

/// Builds an Arrow `SchemaRef` from a list of (name, sql_type, nullable)
/// tuples. Used by both connector schema-introspection and the Phase 8
/// fixed-width layout loader.
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
    fn empty_type_maps_to_utf8() {
        assert_eq!(sql_type_to_arrow("").unwrap(), DataType::Utf8);
    }

    #[test]
    fn unknown_type_returns_error_with_name() {
        let err = sql_type_to_arrow("MARTIANDATA").unwrap_err();
        assert!(err.contains("MARTIANDATA"));
        assert!(err.contains("Unrecognised"));
    }

    #[test]
    fn unsigned_modifier_is_recognised() {
        assert_eq!(
            sql_type_to_arrow("BIGINT UNSIGNED").unwrap(),
            DataType::UInt64
        );
        assert_eq!(
            sql_type_to_arrow("TINYINT(1) UNSIGNED").unwrap(),
            DataType::UInt8
        );
    }

    #[test]
    fn parametrised_types_strip_length_suffix() {
        assert_eq!(sql_type_to_arrow("VARCHAR(255)").unwrap(), DataType::Utf8);
        assert_eq!(
            sql_type_to_arrow("DECIMAL(10, 2)").unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn schema_from_fields_builds_schema_in_order() {
        let schema = schema_from_fields(vec![
            ("id".to_string(), "INTEGER".to_string(), false),
            ("name".to_string(), "VARCHAR".to_string(), true),
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
        let err = schema_from_fields(vec![("x".to_string(), "MARTIANDATA".to_string(), true)])
            .unwrap_err();
        assert!(err.contains("MARTIANDATA"));
    }
}
