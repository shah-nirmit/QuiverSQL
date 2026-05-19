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

    /// Extracts column-level query lineage from a SQL statement.
    pub async fn get_query_lineage(&self, sql: &str) -> Result<QueryLineage, String> {
        let plan = self.ctx.state().create_logical_plan(sql).await.map_err(|e| e.to_string())?;
        let plan = self.ctx.state().optimize(&plan).map_err(|e| e.to_string())?;
        
        let mut results = std::collections::HashMap::new();
        fn extract_lineage(
            plan: &datafusion::logical_expr::LogicalPlan,
            results: &mut std::collections::HashMap<String, std::collections::HashSet<String>>
        ) {
            use datafusion::logical_expr::LogicalPlan;
            match plan {
                LogicalPlan::TableScan(scan) => {
                    let table_name = scan.table_name.table().to_string();
                    let entry = results.entry(table_name).or_insert_with(std::collections::HashSet::new);
                    let schema = scan.source.schema();
                    if let Some(proj) = &scan.projection {
                        for &idx in proj {
                            if let Some(field) = schema.fields().get(idx) {
                                entry.insert(field.name().clone());
                            }
                        }
                    } else {
                        for field in schema.fields() {
                            entry.insert(field.name().clone());
                        }
                    }
                }
                _ => {
                    for input in plan.inputs() {
                        extract_lineage(input, results);
                    }
                }
            }
        }
        
        extract_lineage(&plan, &mut results);
        
        let mut tables = Vec::new();
        let mut relations = Vec::new();
        for (table_name, cols) in results {
            tables.push(table_name.clone());
            let mut columns: Vec<String> = cols.into_iter().collect();
            columns.sort();
            relations.push(LineageInfo {
                table_name,
                columns,
            });
        }
        tables.sort();
        relations.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        
        Ok(QueryLineage {
            tables,
            relations,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct LineageInfo {
    pub table_name: String,
    pub columns: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct QueryLineage {
    pub tables: Vec<String>,
    pub relations: Vec<LineageInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_temp_csv() -> String {
        let path = std::env::temp_dir().join("test_dql_emp.csv");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "id,name,department,salary").unwrap();
        writeln!(file, "1,Alice,Engineering,100000").unwrap();
        writeln!(file, "2,Bob,Sales,80000").unwrap();
        path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_engine_lifecycle() {
        let engine = DqlEngine::new();
        let res = engine.execute_sql_to_string("SELECT * FROM non_existent").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_query_lineage_simple() {
        let engine = DqlEngine::new();
        let csv_path = create_temp_csv();
        engine.register_file("employees", &csv_path, "csv").await.unwrap();

        let lineage = engine.get_query_lineage("SELECT name, salary FROM employees").await.unwrap();
        assert_eq!(lineage.tables, vec!["employees".to_string()]);
        assert_eq!(lineage.relations.len(), 1);
        assert_eq!(lineage.relations[0].table_name, "employees");
        assert_eq!(lineage.relations[0].columns, vec!["name".to_string(), "salary".to_string()]);

        // Clean up temp file
        let _ = std::fs::remove_file(csv_path);
    }

    #[tokio::test]
    async fn test_query_lineage_errors() {
        let engine = DqlEngine::new();
        let res = engine.get_query_lineage("SELECT name FROM non_existent").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("non_existent"));
    }
}
