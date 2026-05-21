use qsql_connectors::RemoteConnector;
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

struct RpcHarness {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl RpcHarness {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_qsql-daemon"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn qsql-daemon");

        let stdin = child.stdin.take().expect("daemon stdin");
        let stdout = BufReader::new(child.stdout.take().expect("daemon stdout"));

        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn request(&mut self, line: &str) -> Value {
        writeln!(self.stdin, "{line}").expect("write request");
        self.stdin.flush().expect("flush request");

        let mut response = String::new();
        self.stdout.read_line(&mut response).expect("read response");

        serde_json::from_str(response.trim()).expect("valid json response")
    }
}

impl Drop for RpcHarness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn ping_returns_pong_with_matching_id() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"ping","id":1}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert_eq!(response["result"], "pong");
    assert!(response.get("error").is_none() || response["error"].is_null());
}

#[test]
fn version_returns_component_versions() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"version","id":2}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 2);
    assert_eq!(response["result"]["product"], "QuiverSQL");
    assert!(response["result"]["daemon"].as_str().is_some());
    assert!(response["result"]["core"].as_str().is_some());
    assert!(response["result"]["connectors"].as_str().is_some());
    assert!(response["result"]["rpc"].as_str().is_some());
    assert!(response.get("error").is_none() || response["error"].is_null());
}

#[test]
fn unknown_method_returns_json_rpc_error() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"does_not_exist","id":3}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 3);
    assert!(response.get("result").is_none() || response["result"].is_null());
    assert_eq!(response["error"]["code"], -32601);
    assert_eq!(response["error"]["message"], "Method not found");
}

#[test]
fn invalid_params_returns_json_rpc_error() {
    let mut rpc = RpcHarness::spawn();

    // Passing wrong param field "query" instead of "sql"
    let response =
        rpc.request(r#"{"jsonrpc":"2.0","method":"execute","params":{"query":"SELECT 1"},"id":4}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 4);
    assert!(response.get("result").is_none() || response["result"].is_null());
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Invalid params"));
}

#[test]
fn execute_query_succeeds_with_structured_params() {
    let mut rpc = RpcHarness::spawn();

    let response =
        rpc.request(r#"{"jsonrpc":"2.0","method":"execute","params":{"sql":"SELECT 1"},"id":5}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 5);
    assert!(response.get("error").is_none() || response["error"].is_null());
    assert!(response["result"].as_str().unwrap().contains("1"));
}

#[test]
fn execute_json_succeeds_with_structured_params() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc
        .request(r#"{"jsonrpc":"2.0","method":"execute_json","params":{"sql":"SELECT 1"},"id":6}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 6);
    assert!(response.get("error").is_none() || response["error"].is_null());
    assert!(response["result"].is_array());
    let arr = response["result"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
}

#[test]
fn query_start_returns_first_page_with_schema_and_metrics() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1 AS value UNION ALL SELECT 2 AS value","page_size":1},"id":7}"#,
    );

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 7);
    assert!(response.get("error").is_none() || response["error"].is_null());
    assert_eq!(response["result"]["query_id"], "q_1");
    assert_eq!(response["result"]["page_index"], 0);
    assert_eq!(response["result"]["page_size"], 1);
    assert_eq!(response["result"]["is_last"], false);
    assert_eq!(response["result"]["data"].as_array().unwrap().len(), 1);
    assert_eq!(response["result"]["schema"]["fields"][0]["name"], "value");
    assert_eq!(response["result"]["metrics"]["rows_produced"], 2);
    assert_eq!(response["result"]["metrics"]["rows_returned"], 1);
}

#[test]
fn query_page_returns_later_page_for_existing_query() {
    let mut rpc = RpcHarness::spawn();

    let start = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1 AS value UNION ALL SELECT 2 AS value","page_size":1},"id":8}"#,
    );
    let query_id = start["result"]["query_id"].as_str().unwrap();
    let page = rpc.request(&format!(
        r#"{{"jsonrpc":"2.0","method":"query_page","params":{{"query_id":"{query_id}","page_index":1}},"id":9}}"#
    ));

    assert_eq!(page["jsonrpc"], "2.0");
    assert_eq!(page["id"], 9);
    assert!(page.get("error").is_none() || page["error"].is_null());
    assert_eq!(page["result"]["query_id"], query_id);
    assert_eq!(page["result"]["page_index"], 1);
    assert_eq!(page["result"]["page_size"], 1);
    assert_eq!(page["result"]["is_last"], true);
    assert_eq!(page["result"]["data"].as_array().unwrap().len(), 1);
}

#[test]
fn query_cancel_discards_existing_query_results() {
    let mut rpc = RpcHarness::spawn();

    let start = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1 AS value","page_size":1},"id":10}"#,
    );
    let query_id = start["result"]["query_id"].as_str().unwrap();
    let cancel = rpc.request(&format!(
        r#"{{"jsonrpc":"2.0","method":"query_cancel","params":{{"query_id":"{query_id}"}},"id":11}}"#
    ));

    assert_eq!(cancel["jsonrpc"], "2.0");
    assert_eq!(cancel["id"], 11);
    assert_eq!(cancel["result"]["query_id"], query_id);
    assert_eq!(cancel["result"]["cancelled"], true);
}

#[test]
fn query_page_unknown_query_returns_structured_error() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_page","params":{"query_id":"q_missing","page_index":1},"id":12}"#,
    );

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 12);
    assert_eq!(response["error"]["code"], -32004);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[test]
fn query_start_invalid_page_size_returns_invalid_params() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1","page_size":0},"id":13}"#,
    );

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 13);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "page_size must be greater than zero"
    );
}

#[test]
fn query_start_large_page_size_is_capped_with_warning() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1 AS value","page_size":10001},"id":14}"#,
    );

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 14);
    assert_eq!(response["result"]["page_size"], 10000);
    assert!(response["result"]["warning"]
        .as_str()
        .unwrap()
        .contains("exceeded the maximum"));
}

#[test]
fn query_start_timeout_returns_structured_error() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT 1","timeout_ms":0},"id":15}"#,
    );

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 15);
    assert_eq!(response["error"]["code"], -32003);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("timed out"));
}

#[test]
fn invalid_json_returns_parse_error_with_null_id() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request("{not json");

    assert_eq!(response["jsonrpc"], "2.0");
    assert!(response.get("result").is_none() || response["result"].is_null());
    assert!(response.get("id").is_none() || response["id"].is_null());
    assert_eq!(response["error"]["code"], -32700);
    assert_eq!(response["error"]["message"], "Parse error");
}

#[test]
fn list_sources_initially_empty() {
    let mut rpc = RpcHarness::spawn();
    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":100}"#);
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 100);
    assert!(response.get("error").is_none() || response["error"].is_null());
    assert_eq!(response["result"].as_array().unwrap().len(), 0);
}

fn create_temp_csv() -> String {
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "test_jsonrpc_emp_{}_{}.csv",
        std::process::id(),
        nanos
    ));
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "id,name\n1,Alice\n2,Bob").unwrap();
    path.to_str().unwrap().to_string()
}

fn create_temp_sqlite() -> String {
    use rusqlite::Connection;
    let path = std::env::temp_dir().join(format!("test_jsonrpc_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", [])
        .unwrap();
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER, quantity INTEGER)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO items (name) VALUES ('Widget')", [])
        .unwrap();
    conn.execute("INSERT INTO orders (item_id, quantity) VALUES (1, 3)", [])
        .unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn catalog_lifecycle_test() {
    let mut rpc = RpcHarness::spawn();

    // 1. Initial list_sources is empty
    let list_res = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":100}"#);
    assert_eq!(list_res["result"].as_array().unwrap().len(), 0);

    // 2. Register file
    let csv_path = create_temp_csv();
    let reg_req = format!(
        r#"{{"jsonrpc":"2.0","method":"register_file","params":{{"table_name":"employees","path":{:?},"format":"csv"}},"id":101}}"#,
        csv_path
    );
    let reg_res = rpc.request(&reg_req);
    assert!(reg_res.get("error").is_none() || reg_res["error"].is_null());

    // 3. Register SQLite
    let db_path = create_temp_sqlite();
    let reg_sql_req = format!(
        r#"{{"jsonrpc":"2.0","method":"register_sqlite","params":{{"db_path":{:?},"alias":"my_sqlite"}},"id":102}}"#,
        db_path
    );
    let reg_sql_res = rpc.request(&reg_sql_req);
    assert!(reg_sql_res.get("error").is_none() || reg_sql_res["error"].is_null());

    // 4. list_sources now has 2 records
    let list_res2 = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":103}"#);
    let sources = list_res2["result"].as_array().unwrap();
    assert_eq!(sources.len(), 2);

    // Verify properties of catalog items
    let emp_source = sources.iter().find(|s| s["name"] == "employees").unwrap();
    assert_eq!(emp_source["kind"], "csv");
    assert_eq!(emp_source["status"], "ready");
    assert!(emp_source["schema"].is_object());

    let items_source = sources.iter().find(|s| s["name"] == "my_sqlite").unwrap();
    assert_eq!(items_source["kind"], "sqlite");
    assert_eq!(items_source["status"], "ready");
    assert!(items_source["capabilities"].is_object());
    assert_eq!(
        items_source["tables"],
        serde_json::json!(["items", "orders"])
    );
    assert_eq!(
        items_source["connection_details"]["tables"],
        serde_json::json!(["items", "orders"])
    );

    let query_res = rpc.request(
        r#"{"jsonrpc":"2.0","method":"query_start","params":{"sql":"SELECT i.name, o.quantity FROM my_sqlite.items i JOIN my_sqlite.orders o ON i.id = o.item_id"},"id":1030}"#,
    );
    assert!(
        query_res.get("error").is_none() || query_res["error"].is_null(),
        "query_start failed: {query_res}"
    );
    assert_eq!(query_res["result"]["data"][0]["name"], "Widget");
    assert_eq!(query_res["result"]["data"][0]["quantity"], 3);

    // 5. get_source_metadata retrieves it properly
    let get_res = rpc.request(r#"{"jsonrpc":"2.0","method":"get_source_metadata","params":{"name":"employees"},"id":104}"#);
    assert_eq!(get_res["result"]["name"], "employees");
    assert_eq!(get_res["result"]["kind"], "csv");

    // Test get_source_metadata not found error (-32004)
    let get_res_err = rpc.request(r#"{"jsonrpc":"2.0","method":"get_source_metadata","params":{"name":"non_existent"},"id":105}"#);
    assert_eq!(get_res_err["error"]["code"], -32004);

    // 6. remove_source successfully deregisters
    let remove_res = rpc.request(
        r#"{"jsonrpc":"2.0","method":"remove_source","params":{"name":"employees"},"id":106}"#,
    );
    assert_eq!(remove_res["result"]["name"], "employees");
    assert_eq!(remove_res["result"]["removed"], true);

    // list_sources now has only 1 record
    let list_res3 = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":107}"#);
    assert_eq!(list_res3["result"].as_array().unwrap().len(), 1);

    // Clean up
    let _ = std::fs::remove_file(csv_path);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn sql_connector_registration_rejects_invalid_params() {
    let mut rpc = RpcHarness::spawn();

    let missing_connection = rpc.request(
        r#"{"jsonrpc":"2.0","method":"register_postgres","params":{"alias":"users"},"id":200}"#,
    );
    assert_eq!(missing_connection["jsonrpc"], "2.0");
    assert_eq!(missing_connection["id"], 200);
    assert_eq!(missing_connection["error"]["code"], -32602);

    let missing_alias = rpc.request(
        r#"{"jsonrpc":"2.0","method":"register_mysql","params":{"connection_string":"mysql://user:secret@localhost/db"},"id":201}"#,
    );
    assert_eq!(missing_alias["jsonrpc"], "2.0");
    assert_eq!(missing_alias["id"], 201);
    assert_eq!(missing_alias["error"]["code"], -32602);
}

#[tokio::test]
#[cfg_attr(
    not(qsql_live_postgres_tests),
    ignore = "requires a live Postgres database and QSQL_POSTGRES_URL"
)]
async fn optional_postgres_registration_redacts_credentials() {
    let url = std::env::var("QSQL_POSTGRES_URL")
        .expect("QSQL_POSTGRES_URL must be set to run Postgres live tests");
    let connector = qsql_connectors::postgres::PostgresConnector::new(url.clone());
    connector
        .execute_query("CREATE TABLE IF NOT EXISTS qsql_phase4_rpc_pg (id INT, name TEXT)")
        .await
        .unwrap();
    connector
        .execute_query("TRUNCATE qsql_phase4_rpc_pg")
        .await
        .unwrap();
    connector
        .execute_query("INSERT INTO qsql_phase4_rpc_pg VALUES (1, 'Alice')")
        .await
        .unwrap();

    let mut rpc = RpcHarness::spawn();

    let setup_req = format!(
        r#"{{"jsonrpc":"2.0","method":"register_postgres","params":{{"connection_string":{:?},"schema":"public","alias":"rpc_pg"}},"id":210}}"#,
        url
    );
    let register = rpc.request(&setup_req);
    if register.get("error").is_some() && !register["error"].is_null() {
        panic!("register_postgres failed: {register}");
    }

    let list = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":211}"#);
    let source = list["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["name"] == "rpc_pg")
        .unwrap();
    assert_eq!(source["kind"], "postgres");
    assert_eq!(source["connection_details"]["connection"], "<redacted>");
    assert!(!source.to_string().contains(&url));
}

#[tokio::test]
#[cfg_attr(
    not(qsql_live_mysql_tests),
    ignore = "requires a live MySQL database and QSQL_MYSQL_URL"
)]
async fn optional_mysql_registration_redacts_credentials() {
    let url = std::env::var("QSQL_MYSQL_URL")
        .expect("QSQL_MYSQL_URL must be set to run MySQL/MariaDB live tests");
    let connector = qsql_connectors::mysql::MySqlConnector::mysql(url.clone());
    connector
        .execute_query("CREATE TABLE IF NOT EXISTS qsql_phase4_rpc_mysql (id INT, name TEXT)")
        .await
        .unwrap();
    connector
        .execute_query("TRUNCATE TABLE qsql_phase4_rpc_mysql")
        .await
        .unwrap();
    connector
        .execute_query("INSERT INTO qsql_phase4_rpc_mysql VALUES (1, 'Alice')")
        .await
        .unwrap();

    let mut rpc = RpcHarness::spawn();

    let setup_req = format!(
        r#"{{"jsonrpc":"2.0","method":"register_mysql","params":{{"connection_string":{:?},"alias":"rpc_mysql"}},"id":220}}"#,
        url
    );
    let register = rpc.request(&setup_req);
    if register.get("error").is_some() && !register["error"].is_null() {
        panic!("register_mysql failed: {register}");
    }

    let list = rpc.request(r#"{"jsonrpc":"2.0","method":"list_sources","id":221}"#);
    let source = list["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["name"] == "rpc_mysql")
        .unwrap();
    assert_eq!(source["kind"], "mysql");
    assert_eq!(source["connection_details"]["connection"], "<redacted>");
    assert!(!source.to_string().contains(&url));
}

#[test]
fn test_explain_query_rpc() {
    let mut harness = RpcHarness::spawn();

    let csv_path = create_temp_csv();
    let req1 = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "register_file",
        "params": {
            "table_name": "test_explain_tbl",
            "path": csv_path,
            "format": "csv"
        }
    });

    let resp1 = harness.request(&req1.to_string());
    assert!(
        resp1["error"].is_null(),
        "Register file failed: {:?}",
        resp1["error"]
    );
    assert_eq!(resp1["id"], 1);

    // Some sleep just in case
    std::thread::sleep(std::time::Duration::from_millis(50));

    let req2 = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "explain_query",
        "params": {
            "sql": "SELECT id, name FROM test_explain_tbl WHERE id = 1",
            "include_native": true
        }
    });

    let resp = harness.request(&req2.to_string());
    assert!(
        resp["error"].is_null(),
        "Explain query failed: {:?}",
        resp["error"]
    );

    assert_eq!(resp["id"], 2);
    let result = &resp["result"];
    assert!(
        !result["federated_plan"].is_null(),
        "federated_plan is null in {:?}",
        result
    );
    assert!(!result["source_plans"].is_null());
    assert!(result["raw"]
        .as_str()
        .unwrap()
        .contains("TableScan: test_explain_tbl"));

    std::fs::remove_file(csv_path).unwrap();
}

#[test]
fn sqlite_explain_uses_qualified_source_plan_keys() {
    let mut harness = RpcHarness::spawn();
    let db_path = create_temp_sqlite();
    let register_req = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"register_sqlite","params":{{"db_path":{:?},"alias":"my_sqlite"}}}}"#,
        db_path
    );

    let register = harness.request(&register_req);
    assert!(
        register["error"].is_null(),
        "Register SQLite failed: {:?}",
        register["error"]
    );

    let explain_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "explain_query",
        "params": {
            "sql": "SELECT name FROM my_sqlite.items WHERE name = 'Widget'",
            "include_native": true
        }
    });
    let explain = harness.request(&explain_req.to_string());
    assert!(
        explain["error"].is_null(),
        "Explain query failed: {:?}",
        explain["error"]
    );

    let result = &explain["result"];
    assert!(result["source_plans"]["my_sqlite.items"].is_string());
    assert!(result["source_plans"]["items"].is_null());

    let nodes = result["federated_plan"]["nodes"].as_object().unwrap();
    let scan = nodes
        .values()
        .find(|node| node["node_type"] == "TableScan")
        .expect("expected a TableScan node");
    assert_eq!(scan["source_ref"], "my_sqlite.items");
    assert_eq!(scan["native_plan_ref"], "my_sqlite.items");
    assert_eq!(scan["attributes"]["table"], "my_sqlite.items");
    assert!(scan["attributes"]["output_columns"]
        .as_str()
        .unwrap()
        .contains("name"));
    assert!(scan["metrics"]["estimated_rows"].is_null());
    assert!(scan["metrics"]["total_cost"].is_null());

    std::fs::remove_file(db_path).unwrap();
}
