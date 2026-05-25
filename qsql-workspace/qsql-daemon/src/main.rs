pub mod explain;
use datafusion::arrow::datatypes::SchemaRef;
use qsql_connectors::sql::SqlDialectKind;
use qsql_connectors::RemoteConnector;
use qsql_core::models::{
    normalize_page_size, CatalogSource, ExplainQueryRequest, ExplainQueryResult,
    GetSourceMetadataRequest, ListSourceTablesRequest, ListSourceTablesResult, QueryCancelRequest,
    QueryCancelResult, QueryError, QueryPageRequest, QueryStartRequest, RemoveSourceRequest,
    RemoveSourceResult, SourceKind,
};
use qsql_core::DatabaseTableReference;
use qsql_core::{init_core, QsqlEngine, QueryResultHandle, QSQL_CORE_VERSION};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

const QSQL_DAEMON_VERSION: &str = env!("CARGO_PKG_VERSION");
const QSQL_RPC_VERSION: &str = "0.1.0";
const TABLE_LIST_LIMIT: usize = 5_000;
const SCHEMA_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_CONCURRENT_QUERY_TASKS: usize = 16;
const SQLITE_SOURCE_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_SOURCE_TIMEOUT: Duration = Duration::from_secs(30);
const SCHEMA_INTROSPECTION_TIMEOUT: Duration = Duration::from_secs(5);

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
    alias: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RegisterPostgresRequest {
    connection_string: String,
    alias: String,
    schema: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RegisterMySqlRequest {
    connection_string: String,
    alias: String,
    schema: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GetLineageRequest {
    sql: String,
}

#[derive(Clone)]
struct DaemonState {
    engine: Arc<QsqlEngine>,
    sessions: Arc<Mutex<HashMap<String, QuerySession>>>,
    database_sources: Arc<RwLock<HashMap<String, Arc<DatabaseRegistration>>>>,
    schema_cache: Arc<RwLock<HashMap<SchemaCacheKey, SchemaCacheEntry>>>,
    query_semaphore: Arc<Semaphore>,
    next_query_id: Arc<AtomicU64>,
    next_source_generation: Arc<AtomicU64>,
    /// Lifetime count of broadcast-join rewrites that were actually applied
    /// (one increment per element of `BroadcastRewriteInfo.applied`). Exposed
    /// over the `health_check` RPC; used by integration tests to confirm the
    /// rewrite fired without parsing the full explain response.
    broadcast_rewrites_applied_total: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct DatabaseRegistration {
    kind: SourceKind,
    generation: u64,
    connection_string: Option<String>,
    db_path: Option<String>,
    schema: Option<String>,
    dialect: Option<SqlDialectKind>,
    tables: Vec<String>,
    tables_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaCacheKey {
    alias: String,
    generation: u64,
    table_name: String,
}

#[derive(Debug, Clone)]
struct SchemaCacheEntry {
    schema: SchemaRef,
    cached_at: Instant,
}

enum QuerySession {
    Streaming {
        handle: Arc<AsyncMutex<QueryResultHandle>>,
        cancellation_token: CancellationToken,
        page_size: usize,
        warning: Option<String>,
    },
}

impl DaemonState {
    fn new(engine: Arc<QsqlEngine>) -> Self {
        Self {
            engine,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            database_sources: Arc::new(RwLock::new(HashMap::new())),
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERY_TASKS)),
            next_query_id: Arc::new(AtomicU64::new(1)),
            next_source_generation: Arc::new(AtomicU64::new(1)),
            broadcast_rewrites_applied_total: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_query_id(&self) -> String {
        let id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        format!("q_{id}")
    }

    fn next_source_generation(&self) -> u64 {
        self.next_source_generation.fetch_add(1, Ordering::Relaxed)
    }
}

#[tokio::main]
async fn main() {
    init_core();
    let state = DaemonState::new(Arc::new(QsqlEngine::new()));

    let stdin = io::stdin();
    let stdout = Arc::new(Mutex::new(io::stdout()));
    let mut reader = io::BufReader::new(stdin.lock());

    loop {
        let frame = match read_request_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => break,
        };

        dispatch_frame(frame, state.clone(), stdout.clone());
    }
}

fn read_request_frame<R: BufRead + Read>(reader: &mut R) -> io::Result<Option<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }

    let trimmed = line.trim_end_matches(['\r', '\n']);
    if trimmed.is_empty() {
        return Ok(Some(String::new()));
    }

    if let Some(length) = trimmed
        .strip_prefix("Content-Length:")
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header)? == 0 {
                return Ok(None);
            }
            if header.trim_end_matches(['\r', '\n']).is_empty() {
                break;
            }
        }
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body)?;
        return Ok(Some(String::from_utf8_lossy(&body).into_owned()));
    }

    Ok(Some(trimmed.to_string()))
}

fn dispatch_frame(frame: String, state: DaemonState, stdout: Arc<Mutex<io::Stdout>>) {
    if let Ok(req) = serde_json::from_str::<RpcRequest>(&frame) {
        tokio::spawn(async move {
            let response = handle_request(req, state).await;
            write_response(stdout, response);
        });
    } else {
        write_response(
            stdout,
            RpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(serde_json::json!({"code": -32700, "message": "Parse error"})),
                id: None,
            },
        );
    }
}

fn write_response(stdout: Arc<Mutex<io::Stdout>>, response: RpcResponse) {
    let response = redact_response(response);
    let response_str = serde_json::to_string(&response).unwrap();
    let mut stdout = stdout.lock().unwrap();
    write!(
        stdout,
        "Content-Length: {}\r\n\r\n{}",
        response_str.len(),
        response_str
    )
    .unwrap();
    stdout.flush().unwrap();
}

fn redact_response(mut response: RpcResponse) -> RpcResponse {
    if let Some(result) = response.result.as_mut() {
        redact_json_value(result);
    }
    if let Some(error) = response.error.as_mut() {
        redact_json_value(error);
    }
    response
}

fn redact_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => *text = redact_sensitive_text(text),
        serde_json::Value::Array(values) => {
            for value in values {
                redact_json_value(value);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                redact_json_value(value);
            }
        }
        _ => {}
    }
}

fn redact_sensitive_text(input: &str) -> String {
    let credential_param =
        regex::Regex::new(r"(?i)\b(password|pwd|pass|secret)=([^\s;&]+)").unwrap();
    let dsn_password = regex::Regex::new(
        r"(?i)\b((?:postgres|postgresql|mysql|mariadb)://[^:\s/@]+:)([^@\s]+)(@)",
    )
    .unwrap();
    let redacted = credential_param.replace_all(input, "${1}=***");
    dsn_password
        .replace_all(&redacted, "${1}***${3}")
        .to_string()
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
        "list_sources" => {
            let catalog = state.engine.get_catalog();
            make_success(serde_json::to_value(catalog).unwrap())
        }
        "list_source_tables" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let list_req: ListSourceTablesRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match list_source_tables(&state, list_req).await {
                Ok(result) => make_success(serde_json::to_value(result).unwrap()),
                Err(error) => make_error(error.code, error.message),
            }
        }
        "remove_source" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let remove_req: RemoveSourceRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match state.engine.remove_source(&remove_req.name) {
                Ok(removed) => {
                    let removed_registration = state
                        .database_sources
                        .write()
                        .unwrap()
                        .remove(&remove_req.name)
                        .is_some();
                    clear_schema_cache_for_source(&state, &remove_req.name);
                    let result = RemoveSourceResult {
                        name: remove_req.name,
                        removed: removed || removed_registration,
                    };
                    make_success(serde_json::to_value(result).unwrap())
                }
                Err(e) => make_error(-32001, e),
            }
        }
        "get_source_metadata" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let get_req: GetSourceMetadataRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            match state.engine.get_source_metadata(&get_req.name) {
                Some(source) => make_success(serde_json::to_value(source).unwrap()),
                None => make_error(-32004, format!("Source '{}' not found", get_req.name)),
            }
        }
        "ping" => make_success(serde_json::json!("pong")),
        "version" => make_success(serde_json::json!({
            "product": "QuiverSQL",
            "version": QSQL_DAEMON_VERSION,
            "daemon": QSQL_DAEMON_VERSION,
            "core": QSQL_CORE_VERSION,
            "connectors": qsql_connectors::QSQL_CONNECTORS_VERSION,
            "rpc": QSQL_RPC_VERSION
        })),
        "diagnostics" => make_success(serde_json::json!({
            "counters": {
                "broadcast_rewrites_applied_total":
                    state.broadcast_rewrites_applied_total.load(Ordering::Relaxed),
            }
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
            if let Err(e) = ensure_database_tables_registered(&state, &exec_req.sql).await {
                return make_error(-32001, e);
            }
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
            if let Err(e) = ensure_database_tables_registered(&state, &exec_req.sql).await {
                return make_error(-32001, e);
            }
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
            match state
                .engine
                .register_file(&file_req.table_name, &file_req.path, &file_req.format)
                .await
            {
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
            let alias = sqlite_req.alias.clone();
            let connector = qsql_connectors::sqlite::SqliteConnector::new(&sqlite_req.db_path);

            match list_tables_with_truncation(&connector, None, SQLITE_SOURCE_TIMEOUT).await {
                Ok((tables, tables_truncated)) => {
                    let capabilities = connector.capabilities();
                    let connection_details = serde_json::json!({
                        "db_path": sqlite_req.db_path.clone(),
                        "tables": tables,
                        "tables_truncated": tables_truncated,
                        "table_list_limit": TABLE_LIST_LIMIT,
                    });
                    let catalog_source = CatalogSource {
                        name: alias.clone(),
                        kind: SourceKind::Sqlite,
                        connection_details,
                        schema: None,
                        capabilities: Some(capabilities),
                        status: "ready".to_string(),
                        error: None,
                        tables: Some(tables.clone()),
                    };
                    state.engine.catalog_source(catalog_source);
                    state.database_sources.write().unwrap().insert(
                        alias.clone(),
                        Arc::new(DatabaseRegistration {
                            kind: SourceKind::Sqlite,
                            generation: state.next_source_generation(),
                            connection_string: None,
                            db_path: Some(sqlite_req.db_path.clone()),
                            schema: None,
                            dialect: None,
                            tables: tables.clone(),
                            tables_truncated,
                        }),
                    );
                    make_success(serde_json::Value::String(format!(
                        "Registered database source '{}' with {} table(s)",
                        alias,
                        tables.len()
                    )))
                }
                Err(e) => make_error(-32001, e),
            }
        }
        "register_postgres" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let postgres_req: RegisterPostgresRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            if postgres_req.connection_string.trim().is_empty() {
                return make_error(-32602, "connection_string is required".to_string());
            }

            let alias = postgres_req.alias.clone();
            let connector =
                qsql_connectors::postgres::PostgresConnector::new(&postgres_req.connection_string);

            match list_tables_with_truncation(
                &connector,
                postgres_req.schema.as_deref(),
                REMOTE_SOURCE_TIMEOUT,
            )
            .await
            {
                Ok((tables, tables_truncated)) => {
                    let capabilities = connector.capabilities();
                    let schema = postgres_req
                        .schema
                        .clone()
                        .unwrap_or_else(|| "public".to_string());
                    let connection_details = serde_json::json!({
                        "schema": schema,
                        "connection": "<redacted>",
                        "tables": tables,
                        "tables_truncated": tables_truncated,
                        "table_list_limit": TABLE_LIST_LIMIT,
                    });
                    let catalog_source = CatalogSource {
                        name: alias.clone(),
                        kind: SourceKind::Postgres,
                        connection_details,
                        schema: None,
                        capabilities: Some(capabilities),
                        status: "ready".to_string(),
                        error: None,
                        tables: Some(tables.clone()),
                    };
                    state.engine.catalog_source(catalog_source);
                    state.database_sources.write().unwrap().insert(
                        alias.clone(),
                        Arc::new(DatabaseRegistration {
                            kind: SourceKind::Postgres,
                            generation: state.next_source_generation(),
                            connection_string: Some(postgres_req.connection_string),
                            db_path: None,
                            schema: postgres_req.schema.clone(),
                            dialect: None,
                            tables: tables.clone(),
                            tables_truncated,
                        }),
                    );
                    make_success(serde_json::Value::String(format!(
                        "Registered database source '{}' with {} table(s)",
                        alias,
                        tables.len()
                    )))
                }
                Err(e) => make_error(-32001, e),
            }
        }
        "register_mysql" | "register_mariadb" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let mysql_req: RegisterMySqlRequest = match serde_json::from_value(params) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };
            if mysql_req.connection_string.trim().is_empty() {
                return make_error(-32602, "connection_string is required".to_string());
            }

            let dialect = if method == "register_mariadb" {
                SqlDialectKind::Mariadb
            } else {
                SqlDialectKind::Mysql
            };
            let source_kind = if method == "register_mariadb" {
                SourceKind::Mariadb
            } else {
                SourceKind::Mysql
            };
            let alias = mysql_req.alias.clone();

            {
                let connector = qsql_connectors::mysql::MySqlConnector::new(
                    &mysql_req.connection_string,
                    dialect,
                );
                match list_tables_with_truncation(
                    &connector,
                    mysql_req.schema.as_deref(),
                    REMOTE_SOURCE_TIMEOUT,
                )
                .await
                {
                    Ok((tables, tables_truncated)) => {
                        let capabilities = connector.capabilities();
                        let connection_details = serde_json::json!({
                            "schema": mysql_req.schema.clone(),
                            "connection": "<redacted>",
                            "tables": tables,
                            "tables_truncated": tables_truncated,
                            "table_list_limit": TABLE_LIST_LIMIT,
                        });
                        let catalog_source = CatalogSource {
                            name: alias.clone(),
                            kind: source_kind,
                            connection_details,
                            schema: None,
                            capabilities: Some(capabilities),
                            status: "ready".to_string(),
                            error: None,
                            tables: Some(tables.clone()),
                        };
                        state.engine.catalog_source(catalog_source);
                        state.database_sources.write().unwrap().insert(
                            alias.clone(),
                            Arc::new(DatabaseRegistration {
                                kind: source_kind,
                                generation: state.next_source_generation(),
                                connection_string: Some(mysql_req.connection_string),
                                db_path: None,
                                schema: mysql_req.schema.clone(),
                                dialect: Some(dialect),
                                tables: tables.clone(),
                                tables_truncated,
                            }),
                        );
                        make_success(serde_json::Value::String(format!(
                            "Registered database source '{}' with {} table(s)",
                            alias,
                            tables.len()
                        )))
                    }
                    Err(e) => make_error(-32001, e),
                }
            }
        }
        "explain_query" => {
            let params = match req.params {
                Some(p) => p,
                None => return make_error(-32602, "Missing params".to_string()),
            };
            let req: ExplainQueryRequest = match serde_json::from_value(params.clone()) {
                Ok(r) => r,
                Err(e) => return make_error(-32602, format!("Invalid params: {}", e)),
            };

            let upper_sql = req.sql.trim().to_uppercase();
            if !upper_sql.starts_with("SELECT") && !upper_sql.starts_with("WITH") {
                return make_error(
                    -32602,
                    "Only SELECT and WITH queries are supported for EXPLAIN".to_string(),
                );
            }
            if let Err(e) = ensure_database_tables_registered(&state, &req.sql).await {
                return make_error(-32001, e);
            }

            let (logical_plan, broadcast_info) =
                match state.engine.get_logical_plan_with_broadcast(&req.sql).await {
                    Ok(pair) => pair,
                    Err(e) => {
                        let re = regex::Regex::new(r"(?i)(password|pwd|secret)=[^\s;]+").unwrap();
                        let redacted = re.replace_all(&e, "${1}=***").to_string();
                        return make_error(-32603, format!("Failed to parse query: {}", redacted));
                    }
                };

            // Update the daemon-level counter so simple polling consumers
            // (health/diagnostics, tests) can confirm the rewrite is firing
            // without parsing the full explain response.
            state
                .broadcast_rewrites_applied_total
                .fetch_add(broadcast_info.applied.len() as u64, Ordering::Relaxed);

            let federated_plan =
                explain::build_plan_graph_with_broadcast(&logical_plan, Some(&broadcast_info));
            let database_sources = state.database_sources.read().unwrap().clone();
            let source_plans =
                explain::extract_source_plans(&logical_plan, &database_sources).await;

            let mut source_plans_json = serde_json::Map::new();
            for (k, v) in source_plans {
                source_plans_json.insert(k, v);
            }

            let raw_plan = format!("{}", logical_plan.display_indent());
            let (raw_plan, raw_warning) = explain::truncate_raw_plan(&raw_plan);
            let mut warnings = Vec::new();
            if federated_plan.truncated {
                warnings.push(format!(
                    "Plan graph exceeded {} nodes and was truncated.",
                    explain::MAX_PLAN_NODES
                ));
            }
            if let Some(raw_warning) = raw_warning {
                warnings.push(raw_warning);
            }
            for app in &broadcast_info.applied {
                warnings.push(format!(
                    "Applied broadcast rewrite: {} key{} from {} pushed to {} ({}ms)",
                    app.predicate_value_count,
                    if app.predicate_value_count == 1 { "" } else { "s" },
                    app.local_table,
                    app.remote_table,
                    app.elapsed_ms,
                ));
            }

            let broadcast_rewrites = if broadcast_info.considered == 0 {
                None
            } else {
                Some(broadcast_info)
            };

            let res = ExplainQueryResult {
                sql: req.sql.clone(),
                federated_plan,
                source_plans: serde_json::Value::Object(source_plans_json),
                raw: raw_plan,
                warnings,
                broadcast_rewrites,
            };

            make_success(serde_json::to_value(res).unwrap())
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
            if let Err(e) = ensure_database_tables_registered(&state, &lineage_req.sql).await {
                return make_error(-32001, e);
            }
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
            if let Err(e) = ensure_database_tables_registered(&state, &start_req.sql).await {
                return make_error(-32001, e);
            }

            let Ok(_permit) = state.query_semaphore.clone().try_acquire_owned() else {
                return make_query_error(QueryError {
                    code: -32021,
                    message: format!(
                        "Too many active query tasks; limit is {MAX_CONCURRENT_QUERY_TASKS}"
                    ),
                    details: Some("resource_limit".to_string()),
                });
            };

            let query_id = state.next_query_id();
            let cancellation_token = CancellationToken::new();
            let handle = match state
                .engine
                .start_query_stream(
                    &start_req.sql,
                    cancellation_token.clone(),
                    start_req.timeout_ms,
                )
                .await
            {
                Ok(handle) => Arc::new(AsyncMutex::new(handle)),
                Err(error) => return make_query_error(error),
            };
            {
                let mut sessions = state.sessions.lock().unwrap();
                sessions.insert(
                    query_id.clone(),
                    QuerySession::Streaming {
                        handle: handle.clone(),
                        cancellation_token: cancellation_token.clone(),
                        page_size,
                        warning: warning.clone(),
                    },
                );
            }

            let mut handle_guard = handle.lock().await;
            match handle_guard
                .page(
                    query_id.clone(),
                    0,
                    page_size,
                    warning,
                    cancellation_token,
                    start_req.timeout_ms,
                )
                .await
            {
                Ok(page) => make_success(serde_json::to_value(page).unwrap()),
                Err(error) => {
                    state.sessions.lock().unwrap().remove(&query_id);
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

            let Ok(_permit) = state.query_semaphore.clone().try_acquire_owned() else {
                return make_query_error(QueryError {
                    code: -32021,
                    message: format!(
                        "Too many active query tasks; limit is {MAX_CONCURRENT_QUERY_TASKS}"
                    ),
                    details: Some("resource_limit".to_string()),
                });
            };

            let (handle, cancellation_token, default_page_size, warning) = {
                let sessions = state.sessions.lock().unwrap();
                let Some(session) = sessions.get(&page_req.query_id) else {
                    return make_query_error(QueryError {
                        code: -32004,
                        message: format!("Query '{}' not found", page_req.query_id),
                        details: None,
                    });
                };
                match session {
                    QuerySession::Streaming {
                        handle,
                        cancellation_token,
                        page_size,
                        warning,
                    } => (
                        handle.clone(),
                        cancellation_token.clone(),
                        *page_size,
                        warning.clone(),
                    ),
                }
            };
            let (page_size, request_warning) = match page_req.page_size {
                Some(size) => match normalize_page_size(Some(size)) {
                    Ok(result) => result,
                    Err(error) => return make_query_error(error),
                },
                None => (default_page_size, None),
            };
            let mut handle_guard = handle.lock().await;
            match handle_guard
                .page(
                    page_req.query_id,
                    page_req.page_index.unwrap_or(0),
                    page_size,
                    request_warning.or(warning),
                    cancellation_token,
                    None,
                )
                .await
            {
                Ok(page) => make_success(serde_json::to_value(page).unwrap()),
                Err(error) => make_query_error(error),
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
                Some(QuerySession::Streaming {
                    cancellation_token, ..
                }) => {
                    cancellation_token.cancel();
                    QueryCancelResult {
                        query_id: cancel_req.query_id,
                        cancelled: true,
                        message: "Query cancellation requested".to_string(),
                    }
                }
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

async fn ensure_database_tables_registered(state: &DaemonState, sql: &str) -> Result<(), String> {
    let table_refs = qsql_core::extract_database_table_refs(sql)?;
    for table_ref in table_refs {
        ensure_database_table_registered(state, &table_ref).await?;
    }
    Ok(())
}

async fn list_tables_with_truncation<C: RemoteConnector + ?Sized>(
    connector: &C,
    schema: Option<&str>,
    timeout: Duration,
) -> Result<(Vec<String>, bool), String> {
    let list = connector.list_tables(schema, TABLE_LIST_LIMIT + 1);
    let mut tables = tokio::time::timeout(timeout, list)
        .await
        .map_err(|_| {
            format!(
                "Timed out listing tables for {} after {}s",
                connector.connector_type(),
                timeout.as_secs()
            )
        })?
        .map_err(|e| e.to_string())?;
    let truncated = tables.len() > TABLE_LIST_LIMIT;
    if truncated {
        tables.truncate(TABLE_LIST_LIMIT);
    }
    Ok((tables, truncated))
}

async fn list_source_tables(
    state: &DaemonState,
    request: ListSourceTablesRequest,
) -> Result<ListSourceTablesResult, QueryError> {
    let offset = request.offset.unwrap_or(0);
    let limit = request.limit.unwrap_or(250).clamp(1, TABLE_LIST_LIMIT);

    if let Some((_alias, registration)) = find_database_registration(state, &request.name) {
        let tables = list_database_tables_page(&registration, offset, limit + 1).await?;
        let truncated = tables.len() > limit;
        let tables = tables.into_iter().take(limit).collect::<Vec<_>>();
        return Ok(ListSourceTablesResult {
            name: request.name,
            tables,
            offset,
            limit,
            total_known: if registration.tables_truncated {
                None
            } else {
                Some(registration.tables.len())
            },
            truncated,
        });
    }

    let source = state
        .engine
        .get_source_metadata(&request.name)
        .ok_or_else(|| QueryError {
            code: -32004,
            message: format!("Source '{}' not found", request.name),
            details: None,
        })?;
    let all_tables = source.tables.unwrap_or_default();
    let tables = all_tables
        .iter()
        .skip(offset)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    Ok(ListSourceTablesResult {
        name: request.name,
        tables,
        offset,
        limit,
        total_known: Some(all_tables.len()),
        truncated: offset.saturating_add(limit) < all_tables.len(),
    })
}

async fn list_database_tables_page(
    registration: &DatabaseRegistration,
    offset: usize,
    limit: usize,
) -> Result<Vec<String>, QueryError> {
    let timeout = match registration.kind {
        SourceKind::Sqlite => SQLITE_SOURCE_TIMEOUT,
        SourceKind::Postgres | SourceKind::Mysql | SourceKind::Mariadb => REMOTE_SOURCE_TIMEOUT,
        _ => REMOTE_SOURCE_TIMEOUT,
    };

    let list = async {
        match registration.kind {
            SourceKind::Sqlite => {
                let db_path = registration.db_path.as_ref().ok_or_else(|| QueryError {
                    code: -32001,
                    message: "SQLite source is missing db_path".to_string(),
                    details: None,
                })?;
                let connector = qsql_connectors::sqlite::SqliteConnector::new(db_path);
                connector
                    .list_tables_page(None, offset, limit)
                    .await
                    .map_err(connector_query_error)
            }
            SourceKind::Postgres => {
                let connection_string =
                    registration
                        .connection_string
                        .as_ref()
                        .ok_or_else(|| QueryError {
                            code: -32001,
                            message: "Postgres source is missing connection string".to_string(),
                            details: None,
                        })?;
                let connector =
                    qsql_connectors::postgres::PostgresConnector::new(connection_string);
                connector
                    .list_tables_page(registration.schema.as_deref(), offset, limit)
                    .await
                    .map_err(connector_query_error)
            }
            SourceKind::Mysql | SourceKind::Mariadb => {
                let connection_string =
                    registration
                        .connection_string
                        .as_ref()
                        .ok_or_else(|| QueryError {
                            code: -32001,
                            message: "MySQL/MariaDB source is missing connection string"
                                .to_string(),
                            details: None,
                        })?;
                let dialect = registration.dialect.unwrap_or(match registration.kind {
                    SourceKind::Mariadb => SqlDialectKind::Mariadb,
                    _ => SqlDialectKind::Mysql,
                });
                let connector =
                    qsql_connectors::mysql::MySqlConnector::new(connection_string, dialect);
                connector
                    .list_tables_page(registration.schema.as_deref(), offset, limit)
                    .await
                    .map_err(connector_query_error)
            }
            _ => Ok(Vec::new()),
        }
    };

    tokio::time::timeout(timeout, list)
        .await
        .map_err(|_| QueryError {
            code: -32003,
            message: format!(
                "Timed out listing source tables after {}s",
                timeout.as_secs()
            ),
            details: None,
        })?
}

fn connector_query_error(error: qsql_connectors::ConnectorError) -> QueryError {
    QueryError {
        code: -32001,
        message: error.to_string(),
        details: Some(format!("{:?}", error.kind).to_lowercase()),
    }
}

async fn ensure_database_table_registered(
    state: &DaemonState,
    table_ref: &DatabaseTableReference,
) -> Result<(), String> {
    let Some((alias, registration)) = find_database_registration(state, &table_ref.alias) else {
        return Ok(());
    };

    let table_name = registration
        .tables
        .iter()
        .find(|table| table.as_str() == table_ref.table_name)
        .or_else(|| {
            registration
                .tables
                .iter()
                .find(|table| table.eq_ignore_ascii_case(&table_ref.table_name))
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "Table '{}.{}' is not listed for registered database source '{}'",
                table_ref.alias, table_ref.table_name, alias
            )
        })?;

    if state
        .engine
        .table_registered_in_schema(&alias, &table_ref.table_name)
    {
        return Ok(());
    }

    let cache_key = SchemaCacheKey {
        alias: alias.clone(),
        generation: registration.generation,
        table_name: table_name.clone(),
    };
    let cached_schema = cached_schema(state, &cache_key);

    match registration.kind {
        SourceKind::Sqlite => {
            let db_path = registration
                .db_path
                .as_ref()
                .ok_or_else(|| format!("SQLite source '{}' is missing db_path", alias))?;
            let connector = qsql_connectors::sqlite::SqliteConnector::new(db_path);
            let provider = tokio::time::timeout(
                SCHEMA_INTROSPECTION_TIMEOUT,
                connector.table_provider(None, &table_name, cached_schema),
            )
            .await
            .map_err(|_| {
                format!(
                    "Timed out loading SQLite table schema after {}s",
                    SCHEMA_INTROSPECTION_TIMEOUT.as_secs()
                )
            })?
            .map_err(|e| e.to_string())?;
            cache_schema(state, cache_key, provider.schema());
            state
                .engine
                .register_schema_table(&alias, &table_ref.table_name, provider)?;
        }
        SourceKind::Postgres => {
            let connection_string = registration.connection_string.as_ref().ok_or_else(|| {
                format!("Postgres source '{}' is missing connection string", alias)
            })?;
            let connector = qsql_connectors::postgres::PostgresConnector::new(connection_string);
            let provider = tokio::time::timeout(
                SCHEMA_INTROSPECTION_TIMEOUT,
                connector.table_provider(
                    registration.schema.as_deref(),
                    &table_name,
                    cached_schema,
                ),
            )
            .await
            .map_err(|_| {
                format!(
                    "Timed out loading Postgres table schema after {}s",
                    SCHEMA_INTROSPECTION_TIMEOUT.as_secs()
                )
            })?
            .map_err(|e| e.to_string())?;
            cache_schema(state, cache_key, provider.schema());
            state
                .engine
                .register_schema_table(&alias, &table_ref.table_name, provider)?;
        }
        SourceKind::Mysql | SourceKind::Mariadb => {
            let connection_string = registration.connection_string.as_ref().ok_or_else(|| {
                format!(
                    "MySQL/MariaDB source '{}' is missing connection string",
                    alias
                )
            })?;
            let dialect = registration.dialect.unwrap_or(match registration.kind {
                SourceKind::Mariadb => SqlDialectKind::Mariadb,
                _ => SqlDialectKind::Mysql,
            });
            let connector = qsql_connectors::mysql::MySqlConnector::new(connection_string, dialect);
            let provider = tokio::time::timeout(
                SCHEMA_INTROSPECTION_TIMEOUT,
                connector.table_provider(
                    registration.schema.as_deref(),
                    &table_name,
                    cached_schema,
                ),
            )
            .await
            .map_err(|_| {
                format!(
                    "Timed out loading MySQL/MariaDB table schema after {}s",
                    SCHEMA_INTROSPECTION_TIMEOUT.as_secs()
                )
            })?
            .map_err(|e| e.to_string())?;
            cache_schema(state, cache_key, provider.schema());
            state
                .engine
                .register_schema_table(&alias, &table_ref.table_name, provider)?;
        }
        _ => {}
    }

    Ok(())
}

fn cached_schema(state: &DaemonState, cache_key: &SchemaCacheKey) -> Option<SchemaRef> {
    let now = Instant::now();
    let mut cache = state.schema_cache.write().unwrap();
    match cache.get(cache_key) {
        Some(entry) if now.duration_since(entry.cached_at) <= SCHEMA_CACHE_TTL => {
            Some(entry.schema.clone())
        }
        Some(_) => {
            cache.remove(cache_key);
            None
        }
        None => None,
    }
}

fn cache_schema(state: &DaemonState, cache_key: SchemaCacheKey, schema: SchemaRef) {
    state.schema_cache.write().unwrap().insert(
        cache_key,
        SchemaCacheEntry {
            schema,
            cached_at: Instant::now(),
        },
    );
}

fn clear_schema_cache_for_source(state: &DaemonState, alias: &str) {
    state
        .schema_cache
        .write()
        .unwrap()
        .retain(|key, _| key.alias != alias);
}

fn find_database_registration(
    state: &DaemonState,
    alias: &str,
) -> Option<(String, Arc<DatabaseRegistration>)> {
    let registrations = state.database_sources.read().unwrap();
    if let Some(registration) = registrations.get(alias) {
        return Some((alias.to_string(), Arc::clone(registration)));
    }
    registrations
        .iter()
        .find(|(registered_alias, _)| registered_alias.eq_ignore_ascii_case(alias))
        .map(|(registered_alias, registration)| {
            (registered_alias.clone(), Arc::clone(registration))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_schema_cache_sqlite() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "qsql_schema_cache_{}_{}.db",
            std::process::id(),
            nanos
        ));
        let _ = std::fs::remove_file(&path);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO customers (name) VALUES ('Alice')", [])
            .unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn schema_cache_is_keyed_by_generation_and_expires() {
        let state = DaemonState::new(Arc::new(QsqlEngine::new()));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let current = SchemaCacheKey {
            alias: "pg".to_string(),
            generation: 1,
            table_name: "customers".to_string(),
        };
        let previous = SchemaCacheKey {
            generation: 0,
            ..current.clone()
        };

        cache_schema(&state, current.clone(), schema.clone());

        assert!(cached_schema(&state, &current).is_some());
        assert!(cached_schema(&state, &previous).is_none());

        state.schema_cache.write().unwrap().insert(
            current.clone(),
            SchemaCacheEntry {
                schema,
                cached_at: Instant::now() - SCHEMA_CACHE_TTL - Duration::from_secs(1),
            },
        );

        assert!(cached_schema(&state, &current).is_none());
    }

    #[tokio::test]
    async fn repeated_database_query_uses_one_schema_cache_entry_within_ttl() {
        let db_path = create_schema_cache_sqlite();
        let state = DaemonState::new(Arc::new(QsqlEngine::new()));
        let generation = state.next_source_generation();
        state.database_sources.write().unwrap().insert(
            "sqlite_local".to_string(),
            Arc::new(DatabaseRegistration {
                kind: SourceKind::Sqlite,
                generation,
                connection_string: None,
                db_path: Some(db_path.clone()),
                schema: None,
                dialect: None,
                tables: vec!["customers".to_string()],
                tables_truncated: false,
            }),
        );

        ensure_database_tables_registered(
            &state,
            "SELECT id FROM sqlite_local.customers WHERE id = 1",
        )
        .await
        .unwrap();
        ensure_database_tables_registered(
            &state,
            "SELECT name FROM sqlite_local.customers WHERE id = 1",
        )
        .await
        .unwrap();

        let cache = state.schema_cache.read().unwrap();
        let matching_entries = cache
            .keys()
            .filter(|key| {
                key.alias == "sqlite_local"
                    && key.generation == generation
                    && key.table_name == "customers"
            })
            .count();
        assert_eq!(matching_entries, 1);
        drop(cache);
        assert!(state
            .engine
            .table_registered_in_schema("sqlite_local", "customers"));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn response_boundary_redacts_nested_credentials() {
        let response = RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(serde_json::json!({
                "message": "postgres://user:secret@localhost/db password=hunter2",
                "nested": ["mysql://root:swordfish@localhost/db"]
            })),
            error: Some(serde_json::json!({
                "message": "Failed with pwd=opensesame"
            })),
            id: Some(7),
        };

        let redacted = redact_response(response);
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(!text.contains("secret"));
        assert!(!text.contains("hunter2"));
        assert!(!text.contains("swordfish"));
        assert!(!text.contains("opensesame"));
        assert!(text.contains("***"));
    }

    #[test]
    fn response_boundary_redacts_credential_shapes() {
        let secrets = [
            ("postgres://alice:p4ssw0rd@localhost/db", "p4ssw0rd"),
            ("postgresql://bob:hunter2@example.com:5432/app", "hunter2"),
            ("mysql://root:swordfish@127.0.0.1/qsql", "swordfish"),
            ("mariadb://svc:opensesame@db.internal/qsql", "opensesame"),
            ("password=letmein", "letmein"),
            ("pwd=shhh", "shhh"),
            ("secret=topsecret", "topsecret"),
            ("pass=inline", "inline"),
        ];

        for (message, forbidden) in secrets {
            let response = RpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(serde_json::json!({
                    "code": -32001,
                    "message": format!("connector failed: {message}"),
                    "data": { "details": message }
                })),
                id: Some(9),
            };

            let redacted = redact_response(response);
            let text = serde_json::to_string(&redacted).unwrap();
            assert!(!text.contains(forbidden), "leaked credential text: {text}");
            assert!(text.contains("***"));
        }
    }

    #[test]
    fn content_length_reader_handles_embedded_newlines() {
        let body = "{\n  \"jsonrpc\": \"2.0\",\n  \"method\": \"ping\",\n  \"id\": 1\n}";
        let frame = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(frame.into_bytes());

        let parsed = read_request_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(parsed, body);
    }
}
