use datafusion::prelude::*;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::options::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

pub struct DqlEngine {
    ctx: SessionContext,
}

impl DqlEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Executes a SQL query and returns the pretty-printed result as a string.
    pub async fn execute_sql_to_string(&self, sql: &str) -> Result<String, String> {
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        
        if batches.is_empty() {
            return Ok("No results or table successfully created.".to_string());
        }

        let formatted = pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string();
            
        Ok(formatted)
    }

    /// Executes a SQL query and returns the result as a JSON string.
    pub async fn execute_sql_to_json(&self, sql: &str) -> Result<serde_json::Value, String> {
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        
        if batches.is_empty() {
            return Ok(serde_json::json!([]));
        }

        let mut buf = Vec::new();
        {
            let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
            for batch in &batches {
                writer.write(batch).map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())?;
        }

        let json_str = String::from_utf8(buf).map_err(|e| e.to_string())?;
        let val: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| e.to_string())?;
        Ok(val)
    }

    /// Registers a local file as a virtual table in the DataFusion context.
    pub async fn register_file(&self, table_name: &str, file_path: &str, format: &str) -> Result<String, String> {
        match format.to_lowercase().as_str() {
            "csv" => {
                self.ctx.register_csv(table_name, file_path, CsvReadOptions::new())
                    .await
                    .map_err(|e| format!("Failed to register CSV: {}", e))?;
            },
            "parquet" => {
                self.ctx.register_parquet(table_name, file_path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| format!("Failed to register Parquet: {}", e))?;
            },
            "json" | "ndjson" => {
                self.ctx.register_json(table_name, file_path, NdJsonReadOptions::default())
                    .await
                    .map_err(|e| format!("Failed to register JSON: {}", e))?;
            },
            _ => return Err(format!("Unsupported format: {}", format)),
        }
        Ok(format!("Successfully registered '{}' as a virtual table.", table_name))
    }

    /// Registers any DataFusion `TableProvider` under a given name.
    /// Used by `dql-connectors` to inject remote sources (SQLite, Postgres, etc.)
    /// into the shared DataFusion session without creating a circular dependency.
    pub fn register_table(
        &self,
        table_name: &str,
        provider: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<String, String> {
        self.ctx
            .register_table(table_name, provider)
            .map_err(|e| format!("Failed to register table '{}': {}", table_name, e))?;
        Ok(format!("Successfully registered '{}' as a federated table.", table_name))
    }
}
