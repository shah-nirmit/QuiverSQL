//! Cancellation-near-baseline RSS test for the daemon subprocess.
//!
//! Closes the "Process RSS near-baseline measurement" item that was struck
//! from Phase 6.2 (current_phase_task.md line 79) because no memory harness
//! existed. Starts 32 streaming queries against a large `generate_series`
//! source, cancels all of them, and asserts the daemon's resident set returns
//! within `DEFAULT_RSS_TOLERANCE_BYTES` of its idle baseline.
//!
//! Only the RSS assertion is gated by `cfg(any(linux, windows, macos))`. On
//! other platforms the cancellation behavior still runs, which preserves the
//! existing cancellation contract from
//! `qsql_core::engine::tests::concurrent_streaming_queries_can_cancel_half_under_load`.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use common::memory::{sample_baseline_rss, wait_until_rss_settles, DEFAULT_RSS_TOLERANCE_BYTES};

const STREAMING_SQL: &str = "SELECT * FROM generate_series(1, 1000000) AS t(value)";
const CONCURRENT_QUERIES: usize = 32;

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
        self.read_response()
    }

    fn read_response(&mut self) -> Value {
        let mut first = String::new();
        self.stdout.read_line(&mut first).expect("read response");
        let trimmed = first.trim_end_matches(['\r', '\n']);
        if let Some(length) = trimmed
            .strip_prefix("Content-Length:")
            .and_then(|value| value.trim().parse::<usize>().ok())
        {
            let mut blank = String::new();
            self.stdout
                .read_line(&mut blank)
                .expect("read response header terminator");
            let mut body = vec![0_u8; length];
            self.stdout
                .read_exact(&mut body)
                .expect("read framed response body");
            return serde_json::from_slice(&body).expect("valid json response");
        }
        serde_json::from_str(trimmed).expect("valid json response")
    }
}

impl Drop for RpcHarness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn cancellation_returns_rss_near_baseline() {
    let mut rpc = RpcHarness::spawn();

    // Warm up the daemon so the baseline reading reflects steady-state rather
    // than the first JSON-RPC handshake. `ping` is cheap and idempotent.
    let ping = rpc.request(r#"{"jsonrpc":"2.0","method":"ping","id":0}"#);
    assert_eq!(ping["result"], "pong");

    let pid = rpc.child.id();

    let baseline = match sample_baseline_rss(pid, Duration::from_secs(2)) {
        Some(rss) => rss,
        None => {
            // OS doesn't expose RSS — skip the assertion but keep exercising
            // the cancellation path. We still want the cancellation behavior
            // to run in CI even when the memory metric is unavailable.
            eprintln!("process_rss_bytes unavailable on this platform; skipping RSS assertion");
            run_cancellation_workload(&mut rpc);
            return;
        }
    };

    run_cancellation_workload(&mut rpc);

    // Allow the daemon a moment to drop streams and release pages before sampling.
    // RSS does not always return to baseline instantly because allocators
    // retain freed pages for reuse.
    let settled = wait_until_rss_settles(
        pid,
        baseline,
        DEFAULT_RSS_TOLERANCE_BYTES,
        Duration::from_secs(10),
    );

    match settled {
        Some(rss) => {
            assert!(
                rss <= baseline.saturating_add(DEFAULT_RSS_TOLERANCE_BYTES),
                "daemon RSS {rss} exceeds baseline {baseline} + {} MiB tolerance after cancellation",
                DEFAULT_RSS_TOLERANCE_BYTES / (1024 * 1024)
            );
        }
        None => {
            // Couldn't read RSS during the wait window — fail loudly rather
            // than silently. If a platform genuinely cannot read RSS the
            // earlier `sample_baseline_rss` returns None and we return above.
            panic!("failed to sample daemon RSS within the settle window");
        }
    }
}

fn run_cancellation_workload(rpc: &mut RpcHarness) {
    let mut query_ids = Vec::with_capacity(CONCURRENT_QUERIES);
    for idx in 0..CONCURRENT_QUERIES {
        let req = format!(
            r#"{{"jsonrpc":"2.0","method":"query_start","params":{{"sql":"{STREAMING_SQL}","page_size":1000}},"id":{}}}"#,
            100 + idx
        );
        let response = rpc.request(&req);
        let query_id = response["result"]["query_id"]
            .as_str()
            .unwrap_or_else(|| panic!("query_start #{idx} returned no query_id: {response}"))
            .to_string();
        query_ids.push(query_id);
    }

    for (idx, query_id) in query_ids.iter().enumerate() {
        let req = format!(
            r#"{{"jsonrpc":"2.0","method":"query_cancel","params":{{"query_id":"{query_id}"}},"id":{}}}"#,
            200 + idx
        );
        let response = rpc.request(&req);
        let cancelled = response["result"]["cancelled"].as_bool().unwrap_or(false);
        assert!(
            cancelled,
            "query_cancel for {query_id} did not report cancelled=true: {response}"
        );
    }

    // Give the daemon a beat to actually drop the cancelled stream handles.
    thread::sleep(Duration::from_millis(200));
}
