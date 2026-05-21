pub mod explain;
use qsql_connectors::sql::SqlDialectKind;
use qsql_connectors::RemoteConnector;
use qsql_core::models::{
    build_query_page, normalize_page_size, CatalogSource, ExplainQueryRequest, ExplainQueryResult,
    GetSourceMetadataRequest, QueryCancelRequest, QueryCancelResult, QueryError,
    QueryExecutionResult, QueryPageRequest, QueryStartRequest, RemoveSourceRequest,
    RemoveSourceResult, SourceKind,
};
use qsql_core::DatabaseTableReference;
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
const TABLE_LIST_LIMIT: usize = 5_000;

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
    database_sources: Arc<Mutex<HashMap<String, DatabaseRegistration>>>,
    next_query_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct DatabaseRegistration {
    kind: SourceKind,
    connection_string: Option<String>,
    db_path: Option<String>,
    schema: Option<String>,
    dialect: Option<SqlDialectKind>,
    tables: Vec<String>,
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
            database_sources: Arc::new(Mutex::new(HashMap::new())),
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
        "list_sources" => {
            let catalog = state.engine.get_catalog();
            make_success(serde_json::to_value(catalog).unwrap())
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
                        .lock()
                        .unwrap()
                        .remove(&remove_req.name)
                        .is_some();
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

            match connector.list_tables(None, TABLE_LIST_LIMIT).await {
                Ok(tables) => {
                    let capabilities = connector.capabilities();
                    let connection_details = serde_json::json!({
                        "db_path": sqlite_req.db_path.clone(),
                        "tables": tables,
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
                    state.database_sources.lock().unwrap().insert(
                        alias.clone(),
                        DatabaseRegistration {
                            kind: SourceKind::Sqlite,
                            connection_string: None,
                            db_path: Some(sqlite_req.db_path.clone()),
                            schema: None,
                            dialect: None,
                            tables: tables.clone(),
                        },
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

            match connector
                .list_tables(postgres_req.schema.as_deref(), TABLE_LIST_LIMIT)
                .await
            {
                Ok(tables) => {
                    let capabilities = connector.capabilities();
                    let schema = postgres_req
                        .schema
                        .clone()
                        .unwrap_or_else(|| "public".to_string());
                    let connection_details = serde_json::json!({
                        "schema": schema,
                        "connection": "<redacted>",
                        "tables": tables,
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
                    state.database_sources.lock().unwrap().insert(
                        alias.clone(),
                        DatabaseRegistration {
                            kind: SourceKind::Postgres,
                            connection_string: Some(postgres_req.connection_string),
                            db_path: None,
                            schema: postgres_req.schema.clone(),
                            dialect: None,
                            tables: tables.clone(),
                        },
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
                match connector
                    .list_tables(mysql_req.schema.as_deref(), TABLE_LIST_LIMIT)
                    .await
                {
                    Ok(tables) => {
                        let capabilities = connector.capabilities();
                        let connection_details = serde_json::json!({
                            "schema": mysql_req.schema.clone(),
                            "connection": "<redacted>",
                            "tables": tables,
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
                        state.database_sources.lock().unwrap().insert(
                            alias.clone(),
                            DatabaseRegistration {
                                kind: source_kind,
                                connection_string: Some(mysql_req.connection_string),
                                db_path: None,
                                schema: mysql_req.schema.clone(),
                                dialect: Some(dialect),
                                tables: tables.clone(),
                            },
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

            let logical_plan = match state.engine.get_logical_plan(&req.sql).await {
                Ok(plan) => plan,
                Err(e) => {
                    let re = regex::Regex::new(r"(?i)(password|pwd|secret)=[^\s;]+").unwrap();
                    let redacted = re.replace_all(&e, "${1}=***").to_string();
                    return make_error(-32603, format!("Failed to parse query: {}", redacted));
                }
            };

            let federated_plan = explain::build_plan_graph(&logical_plan);
            let source_plans = explain::extract_source_plans(&logical_plan).await;

            let mut source_plans_json = serde_json::Map::new();
            for (k, v) in source_plans {
                source_plans_json.insert(k, v);
            }

            let res = ExplainQueryResult {
                sql: req.sql.clone(),
                federated_plan,
                source_plans: serde_json::Value::Object(source_plans_json),
                raw: format!("{}", logical_plan.display_indent()),
                warnings: vec![],
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
                    let page =
                        build_query_page(query_id.clone(), &result, 0, page_size, warning.clone());
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

async fn ensure_database_tables_registered(state: &DaemonState, sql: &str) -> Result<(), String> {
    let table_refs = qsql_core::extract_database_table_refs(sql)?;
    for table_ref in table_refs {
        ensure_database_table_registered(state, &table_ref).await?;
    }
    Ok(())
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

    match registration.kind {
        SourceKind::Sqlite => {
            let db_path = registration
                .db_path
                .ok_or_else(|| format!("SQLite source '{}' is missing db_path", alias))?;
            let provider =
                qsql_connectors::sqlite::SqliteTableProvider::try_new(db_path, table_name)?;
            state.engine.register_schema_table(
                &alias,
                &table_ref.table_name,
                Arc::new(provider),
            )?;
        }
        SourceKind::Postgres => {
            let connection_string = registration.connection_string.ok_or_else(|| {
                format!("Postgres source '{}' is missing connection string", alias)
            })?;
            let provider = qsql_connectors::postgres::PostgresTableProvider::try_new(
                connection_string,
                registration.schema,
                table_name,
            )
            .await?;
            state.engine.register_schema_table(
                &alias,
                &table_ref.table_name,
                Arc::new(provider),
            )?;
        }
        SourceKind::Mysql | SourceKind::Mariadb => {
            let connection_string = registration.connection_string.ok_or_else(|| {
                format!(
                    "MySQL/MariaDB source '{}' is missing connection string",
                    alias
                )
            })?;
            let dialect = registration.dialect.unwrap_or(match registration.kind {
                SourceKind::Mariadb => SqlDialectKind::Mariadb,
                _ => SqlDialectKind::Mysql,
            });
            let provider = qsql_connectors::mysql::MySqlTableProvider::try_new(
                connection_string,
                dialect,
                registration.schema,
                table_name,
            )
            .await?;
            state.engine.register_schema_table(
                &alias,
                &table_ref.table_name,
                Arc::new(provider),
            )?;
        }
        _ => {}
    }

    Ok(())
}

fn find_database_registration(
    state: &DaemonState,
    alias: &str,
) -> Option<(String, DatabaseRegistration)> {
    let registrations = state.database_sources.lock().unwrap();
    if let Some(registration) = registrations.get(alias) {
        return Some((alias.to_string(), registration.clone()));
    }
    registrations
        .iter()
        .find(|(registered_alias, _)| registered_alias.eq_ignore_ascii_case(alias))
        .map(|(registered_alias, registration)| (registered_alias.clone(), registration.clone()))
}
