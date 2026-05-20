use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::{init_core, QsqlEngine, QSQL_CORE_VERSION};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

const QSQL_DAEMON_VERSION: &str = env!("CARGO_PKG_VERSION");
const QSQL_RPC_VERSION: &str = "0.1.0";

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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ExecuteRequest {
    sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RegisterFileRequest {
    table_name: String,
    path: String,
    format: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RegisterSqliteRequest {
    db_path: String,
    table_name: String,
    alias: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GetLineageRequest {
    sql: String,
}

#[tokio::main]
async fn main() {
    init_core();
    let engine = Arc::new(QsqlEngine::new());

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

async fn handle_request(req: RpcRequest, engine: Arc<QsqlEngine>) -> RpcResponse {
    let method = req.method.as_str();

    let make_error = |code: i32, message: String| -> RpcResponse {
        RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(serde_json::json!({
                "code": code,
                "message": message
            })),
            id: req.id,
        }
    };

    let make_success = |res: serde_json::Value| -> RpcResponse {
        RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(res),
            error: None,
            id: req.id,
        }
    };

    match method {
        "ping" => make_success(serde_json::json!("pong")),
        "version" => make_success(serde_json::json!({
            "product": "QuiverSQL",
            "version": QSQL_DAEMON_VERSION,
            "daemon": QSQL_DAEMON_VERSION,
            "core": QSQL_CORE_VERSION,
            "connectors": qsql_connectors::QSQL_CONNECTORS_VERSION,
            "rpc": QSQL_RPC_VERSION
        })),
        "execute" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let exec_req: ExecuteRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match engine.execute_sql_to_string(&exec_req.sql).await {
                Ok(res) => make_success(serde_json::Value::String(res)),
                Err(e) => make_error(-32001, e),
            }
        }
        "execute_json" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let exec_req: ExecuteRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match engine.execute_sql_to_json(&exec_req.sql).await {
                Ok(res) => make_success(res),
                Err(e) => make_error(-32001, e),
            }
        }
        "register_file" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let file_req: RegisterFileRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match engine.register_file(&file_req.table_name, &file_req.path, &file_req.format).await {
                Ok(res) => make_success(serde_json::Value::String(res)),
                Err(e) => make_error(-32001, e),
            }
        }
        "register_sqlite" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let sqlite_req: RegisterSqliteRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            let alias = sqlite_req.alias.as_deref().unwrap_or(&sqlite_req.table_name);
            match SqliteTableProvider::try_new(&sqlite_req.db_path, &sqlite_req.table_name) {
                Ok(provider) => match engine.register_table(alias, Arc::new(provider)) {
                    Ok(msg) => make_success(serde_json::Value::String(msg)),
                    Err(e) => make_error(-32001, e),
                },
                Err(e) => make_error(-32001, e),
            }
        }
        "get_lineage" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let lineage_req: GetLineageRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match engine.get_query_lineage(&lineage_req.sql).await {
                Ok(lineage) => make_success(serde_json::to_value(lineage).unwrap()),
                Err(e) => make_error(-32001, e),
            }
        }
        _ => make_error(-32601, "Method not found".to_string()),
    }
}
