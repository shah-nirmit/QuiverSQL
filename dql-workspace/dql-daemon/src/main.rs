use dql_core::{init_core, DqlEngine};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
struct RpcRequest {
    jsonrpc: String,
    method: String,
    params: Option<serde_json::Value>,
    id: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct RpcResponse {
    jsonrpc: String,
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
    id: Option<u64>,
}

#[tokio::main]
async fn main() {
    init_core();
    let engine = Arc::new(DqlEngine::new());
    
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        if let Ok(req) = serde_json::from_str::<RpcRequest>(&line) {
            let response = handle_request(req, engine.clone()).await;
            let response_str = serde_json::to_string(&response).unwrap();
            writeln!(stdout, "{}", response_str).unwrap();
            stdout.flush().unwrap();
        } else {
            // Invalid JSON
            writeln!(stdout, "{{\"jsonrpc\": \"2.0\", \"error\": {{\"code\": -32700, \"message\": \"Parse error\"}}, \"id\": null}}").unwrap();
            stdout.flush().unwrap();
        }
    }
}

async fn handle_request(req: RpcRequest, engine: Arc<DqlEngine>) -> RpcResponse {
    let result = match req.method.as_str() {
        "ping" => Ok(serde_json::json!("pong")),
        "execute" => {
            if let Some(params) = req.params {
                if let Some(sql) = params.as_str() {
                    match engine.execute_sql_to_string(sql).await {
                        Ok(res) => Ok(serde_json::Value::String(res)),
                        Err(e) => Err(e),
                    }
                } else {
                    Err("Invalid params: expected SQL string".to_string())
                }
            } else {
                Err("Missing params for execute".to_string())
            }
        },
        "execute_json" => {
            if let Some(params) = req.params {
                if let Some(sql) = params.as_str() {
                    match engine.execute_sql_to_json(sql).await {
                        Ok(res) => Ok(res),
                        Err(e) => Err(e),
                    }
                } else {
                    Err("Invalid params: expected SQL string".to_string())
                }
            } else {
                Err("Missing params for execute_json".to_string())
            }
        },
        "register_file" => {
            if let Some(params) = req.params {
                let table_name = params.get("table_name").and_then(|v| v.as_str()).unwrap_or("");
                let path = params.get("path").and_then(|v| v.as_str()).unwrap_or("");
                let format = params.get("format").and_then(|v| v.as_str()).unwrap_or("");
                
                if table_name.is_empty() || path.is_empty() || format.is_empty() {
                    Err("Missing table_name, path, or format".to_string())
                } else {
                    match engine.register_file(table_name, path, format).await {
                        Ok(res) => Ok(serde_json::Value::String(res)),
                        Err(e) => Err(e),
                    }
                }
            } else {
                Err("Missing params for register_file".to_string())
            }
        },
        _ => Err("Method not found".to_string()),
    };

    match result {
        Ok(res) => RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(res),
            error: None,
            id: req.id,
        },
        Err(e) => RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(serde_json::json!({"code": -32601, "message": e})),
            id: req.id,
        }
    }
}
