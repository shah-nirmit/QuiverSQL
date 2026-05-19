//! SQLite connector for DQL.
//!
//! Implements both the `RemoteConnector` trait (for direct query execution)
//! and a DataFusion `TableProvider` (so SQLite tables can be referenced in
//! federated SQL queries alongside local CSV/Parquet files).

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use rusqlite::{types::ValueRef, Connection};
use std::sync::Arc;

use crate::RemoteConnector;

/// A connector to a local SQLite database file.
#[derive(Debug)]
pub struct SqliteConnector {
    db_path: String,
}

impl SqliteConnector {
    pub fn new(db_path: impl Into<String>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }
}

#[async_trait]
impl RemoteConnector for SqliteConnector {
    fn connector_type(&self) -> &'static str {
        "sqlite"
    }

    /// Execute any SQL query against the SQLite database and return results as
    /// a JSON array of row objects (column_name -> value).
    async fn execute_query(&self, sql: &str) -> Result<Vec<serde_json::Value>, String> {
        let db_path = self.db_path.clone();
        let sql = sql.to_string();

        // rusqlite is sync; run it on a blocking thread so we don't block tokio.
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| format!("Failed to open SQLite DB '{}': {}", db_path, e))?;

            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("Failed to prepare SQL: {}", e))?;

            let column_count = stmt.column_count();
            let column_names: Vec<String> = (0..column_count)
                .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
                .collect();

            let rows = stmt
                .query_map([], |row| {
                    let mut obj = serde_json::Map::new();
                    for (i, name) in column_names.iter().enumerate() {
                        let val = match row.get_ref(i).unwrap_or(ValueRef::Null) {
                            ValueRef::Null => serde_json::Value::Null,
                            ValueRef::Integer(n) => serde_json::json!(n),
                            ValueRef::Real(f) => serde_json::json!(f),
                            ValueRef::Text(t) => {
                                serde_json::Value::String(String::from_utf8_lossy(t).into_owned())
                            }
                            ValueRef::Blob(b) => {
                                serde_json::Value::String(format!("<blob {} bytes>", b.len()))
                            }
                        };
                        obj.insert(name.clone(), val);
                    }
                    Ok(serde_json::Value::Object(obj))
                })
                .map_err(|e| format!("Query failed: {}", e))?;

            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(|e| format!("Row error: {}", e))?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| format!("Thread join error: {}", e))?
    }
}

// ---------------------------------------------------------------------------
// DataFusion TableProvider — lets SQLite tables appear in federated queries
// ---------------------------------------------------------------------------

/// A DataFusion `TableProvider` backed by a single SQLite table.
/// When DataFusion scans this provider it executes `SELECT * FROM <table>`
/// against the SQLite file, converts the result to Arrow, and returns a
/// `MemoryExec` plan. This is a straightforward "full-scan" proxy — predicate
/// pushdown can be layered on top in a future iteration.
#[derive(Debug)]
pub struct SqliteTableProvider {
    connector: Arc<SqliteConnector>,
    table_name: String,
    schema: SchemaRef,
}

impl SqliteTableProvider {
    /// Open `db_path`, introspect `table_name`, and build the Arrow schema
    /// by executing a `LIMIT 0` query and reading column type affinities.
    pub fn try_new(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self, String> {
        let db_path = db_path.into();
        let table_name = table_name.into();

        let conn = Connection::open(&db_path)
            .map_err(|e| format!("SQLite open error: {}", e))?;

        // Use PRAGMA table_info to get column names and declared types.
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info(\"{}\")", table_name))
            .map_err(|e| format!("PRAGMA error: {}", e))?;

        let mut fields: Vec<Field> = Vec::new();
        let rows = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let col_type: String = row.get(2).unwrap_or_default();
                Ok((name, col_type))
            })
            .map_err(|e| format!("PRAGMA query error: {}", e))?;

        for r in rows {
            let (name, col_type) = r.map_err(|e| format!("Row error: {}", e))?;
            let arrow_type = sqlite_type_to_arrow(&col_type);
            fields.push(Field::new(&name, arrow_type, true));
        }

        if fields.is_empty() {
            return Err(format!(
                "Table '{}' not found or has no columns in '{}'",
                table_name, db_path
            ));
        }

        let schema = Arc::new(Schema::new(fields));
        let connector = Arc::new(SqliteConnector::new(db_path));

        Ok(Self {
            connector,
            table_name,
            schema,
        })
    }
}

/// Map SQLite type affinity strings to Arrow DataTypes.
fn sqlite_type_to_arrow(sqlite_type: &str) -> DataType {
    let upper = sqlite_type.to_uppercase();
    if upper.contains("INT") {
        DataType::Int64
    } else if upper.contains("REAL")
        || upper.contains("FLOA")
        || upper.contains("DOUB")
        || upper.contains("NUM")
        || upper.contains("DEC")
    {
        DataType::Float64
    } else if upper.contains("BOOL") {
        DataType::Boolean
    } else {
        // TEXT, BLOB, and everything else → Utf8
        DataType::Utf8
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let sql = format!("SELECT * FROM \"{}\"", self.table_name);

        // Execute via the connector (runs on a blocking thread internally).
        let rows = self
            .connector
            .execute_query(&sql)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        // Convert JSON rows → Arrow RecordBatch using the provider's schema.
        let schema = self.schema.clone();
        let batch = json_rows_to_record_batch(&rows, schema.clone())
            .map_err(|e| DataFusionError::External(e.into()))?;

        // Apply column projection if DataFusion requests a subset of columns.
        let projected_batch = match projection {
            Some(indices) => batch
                .project(indices)
                .map_err(|e| DataFusionError::ArrowError(e, None))?,
            None => batch,
        };

        let projected_schema = projected_batch.schema();
        let partitions = vec![vec![projected_batch]];
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            projected_schema,
            None,
        )?))
    }
}

/// Convert a Vec of JSON row objects into an Arrow RecordBatch using the
/// provided schema. Each column is built independently using typed Arrow builders.
fn json_rows_to_record_batch(
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
                            builder.append_value(n.as_i64().unwrap_or(0));
                        }
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            // Try parsing a string representation
                            builder.append_value(
                                v.as_str()
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(0),
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
                            builder.append_value(n.as_f64().unwrap_or(0.0));
                        }
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
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            builder.append_value(v.as_i64().map(|n| n != 0).unwrap_or(false));
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Default: treat as Utf8 string
                let mut builder = StringBuilder::new();
                for row in rows {
                    match row.get(field.name()) {
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            let s = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            builder.append_value(&s);
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}
