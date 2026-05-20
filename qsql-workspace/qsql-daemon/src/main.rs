use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_core::models::{
    build_query_page, normalize_page_size, QueryCancelRequest, QueryCancelResult, QueryError,
    QueryExecutionResult, QueryPageRequest, QueryStartRequest,
};
use qsql_core::{init_core, QsqlEngine, QSQL_CORE_VERSION};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

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

#[derive(Clone)]
struct DaemonState {
    engine: Arc<QsqlEngine>,
    sessions: Arc<Mutex<HashMap<String, QuerySession>>>,
    next_query_id: Arc<AtomicU64>,
}

enum QuerySession {
    Running {
        cancellation_token: CancellationToken,
    },
    Completed {
        result: QueryExecutionResult,
        page_size: usize,
        warning: Option<String>,
    },
}

impl DaemonState {
    fn new(engine: Arc<QsqlEngine>) -> Self {
        Self {
            engine,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_query_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_query_id(&self) -> String {
        let id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        format!("q_{id}")
    }
}

#[tokio::main]
async fn main() {
    init_core();
    let state = DaemonState::new(Arc::new(QsqlEngine::new()));

    let stdin = io::stdin();
    let stdout = Arc::new(Mutex::new(io::stdout()));

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        if let Ok(req) = serde_json::from_str::<RpcRequest>(&line) {
            let state = state.clone();
            let stdout = stdout.clone();
            tokio::spawn(async move {
                let response = handle_request(req, state).await;
                write_response(stdout, response);
            });
        } else {
            // Invalid JSON
            write_response(
                stdout.clone(),
                RpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(serde_json::json!({"code": -32700, "message": "Parse error"})),
                    id: None,
                },
            );
        }
    }
}

fn write_response(stdout: Arc<Mutex<io::Stdout>>, response: RpcResponse) {
    let response_str = serde_json::to_string(&response).unwrap();
    let mut stdout = stdout.lock().unwrap();
    writeln!(stdout, "{response_str}").unwrap();
    stdout.flush().unwrap();
}

async fn handle_request(req: RpcRequest, state: DaemonState) -> RpcResponse {
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

    let make_query_error = |error: QueryError| -> RpcResponse {
        let error_body = match error.details {
            Some(details) => serde_json::json!({
                "code": error.code,
                "message": error.message,
                "data": { "details": details }
            }),
            None => serde_json::json!({
                "code": error.code,
                "message": error.message
            }),
        };

        RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(error_body),
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
            match state.engine.execute_sql_to_string(&exec_req.sql).await {
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
            match state.engine.execute_sql_to_json(&exec_req.sql).await {
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
            match state.engine.register_file(&file_req.table_name, &file_req.path, &file_req.format).await {
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
                Ok(provider) => match state.engine.register_table(alias, Arc::new(provider)) {
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
            match state.engine.get_query_lineage(&lineage_req.sql).await {
                Ok(lineage) => make_success(serde_json::to_value(lineage).unwrap()),
                Err(e) => make_error(-32001, e),
            }
        }
        "query_start" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let start_req: QueryStartRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            let (page_size, warning) = match normalize_page_size(start_req.page_size) {
                Ok(result) => result,
                Err(error) => return make_query_error(error),
            };

            let query_id = state.next_query_id();
            let cancellation_token = CancellationToken::new();
            {
                let mut sessions = state.sessions.lock().unwrap();
                sessions.insert(
                    query_id.clone(),
                    QuerySession::Running {
                        cancellation_token: cancellation_token.clone(),
                    },
                );
            }

            match state
                .engine
                .execute_sql_collect(&start_req.sql, cancellation_token, start_req.timeout_ms)
                .await
            {
                Ok(result) => {
                    let page = build_query_page(
                        query_id.clone(),
                        &result,
                        0,
                        page_size,
                        warning.clone(),
                    );
                    let mut sessions = state.sessions.lock().unwrap();
                    sessions.insert(
                        query_id,
                        QuerySession::Completed {
                            result,
                            page_size,
                            warning,
                        },
                    );
                    make_success(serde_json::to_value(page).unwrap())
                }
                Err(error) => {
                    let mut sessions = state.sessions.lock().unwrap();
                    sessions.remove(&query_id);
                    make_query_error(error)
                }
            }
        }
        "query_page" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let page_req: QueryPageRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };

            let sessions = state.sessions.lock().unwrap();
            let Some(session) = sessions.get(&page_req.query_id) else {
                return make_query_error(QueryError {
                    code: -32004,
                    message: format!("Query '{}' not found", page_req.query_id),
                    details: None,
                });
            };

            match session {
                QuerySession::Running { .. } => make_query_error(QueryError {
                    code: -32005,
                    message: format!("Query '{}' is still running", page_req.query_id),
                    details: None,
                }),
                QuerySession::Completed {
                    result,
                    page_size,
                    warning,
                } => {
                    let (page_size, request_warning) = match page_req.page_size {
                        Some(size) => match normalize_page_size(Some(size)) {
                            Ok(result) => result,
                            Err(error) => return make_query_error(error),
                        },
                        None => (*page_size, None),
                    };
                    let page = build_query_page(
                        page_req.query_id,
                        result,
                        page_req.page_index.unwrap_or(0),
                        page_size,
                        request_warning.or_else(|| warning.clone()),
                    );
                    make_success(serde_json::to_value(page).unwrap())
                }
            }
        }
        "query_cancel" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let cancel_req: QueryCancelRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };

            let mut sessions = state.sessions.lock().unwrap();
            let result = match sessions.remove(&cancel_req.query_id) {
                Some(QuerySession::Running { cancellation_token }) => {
                    cancellation_token.cancel();
                    QueryCancelResult {
                        query_id: cancel_req.query_id,
                        cancelled: true,
                        message: "Query cancellation requested".to_string(),
                    }
                }
                Some(QuerySession::Completed { .. }) => QueryCancelResult {
                    query_id: cancel_req.query_id,
                    cancelled: true,
                    message: "Query results discarded".to_string(),
                },
                None => QueryCancelResult {
                    query_id: cancel_req.query_id,
                    cancelled: false,
                    message: "Query not found".to_string(),
                },
            };
            make_success(serde_json::to_value(result).unwrap())
        }
        _ => make_error(-32601, "Method not found".to_string()),
    }
}
