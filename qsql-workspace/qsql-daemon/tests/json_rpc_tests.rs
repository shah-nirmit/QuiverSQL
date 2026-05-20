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
        self.stdout
            .read_line(&mut response)
            .expect("read response");

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
    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"execute","params":{"query":"SELECT 1"},"id":4}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 4);
    assert!(response.get("result").is_none() || response["result"].is_null());
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"]["message"].as_str().unwrap().contains("Invalid params"));
}

#[test]
fn execute_query_succeeds_with_structured_params() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"execute","params":{"sql":"SELECT 1"},"id":5}"#);

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 5);
    assert!(response.get("error").is_none() || response["error"].is_null());
    assert!(response["result"].as_str().unwrap().contains("1"));
}

#[test]
fn execute_json_succeeds_with_structured_params() {
    let mut rpc = RpcHarness::spawn();

    let response = rpc.request(r#"{"jsonrpc":"2.0","method":"execute_json","params":{"sql":"SELECT 1"},"id":6}"#);

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
    assert_eq!(response["error"]["message"], "page_size must be greater than zero");
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
