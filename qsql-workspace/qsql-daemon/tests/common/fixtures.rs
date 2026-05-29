use rusqlite::Connection;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a unique path in the system temp directory to avoid parallel test collisions.
pub fn unique_temp_path(prefix: &str, ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "{}_{}_{}_{}.{}",
        prefix,
        std::process::id(),
        nanos,
        seq,
        ext
    ))
}

/// Writes a CSV file with schema `(id INTEGER, label TEXT, amount REAL)`.
/// Rows: `id` from 1..=rows, `label = "item_<id>"`, `amount = id * 1.5`.
pub fn generate_medium_csv(path: &Path, rows: usize) {
    let mut file = std::fs::File::create(path)
        .unwrap_or_else(|e| panic!("create CSV {}: {e}", path.display()));
    writeln!(file, "id,label,amount").unwrap();
    for i in 1..=rows {
        writeln!(file, "{},item_{},{:.1}", i, i, i as f64 * 1.5).unwrap();
    }
}

/// Writes a fixed-width file alongside a JSON layout sidecar.
///
/// Data file shape (78-byte rows, newline-terminated):
///   id     (6 bytes, right-justified INTEGER)
///   label  (22 bytes, left-justified VARCHAR, "item_<id>")
///   amount (16 bytes, right-justified DOUBLE, id * 1.5)
///   filler (34 bytes of spaces — exercises the parser on trailing whitespace)
///
/// `path` is the data file; the layout JSON is written to `path` with the
/// `.layout.json` suffix appended (e.g. `foo.fwf` → `foo.fwf.layout.json`).
/// Returns the layout path so the caller can pass it as `options.layout_path`.
pub fn generate_medium_fwf(path: &Path, rows: usize) -> PathBuf {
    let mut file = std::fs::File::create(path)
        .unwrap_or_else(|e| panic!("create FWF {}: {e}", path.display()));
    for i in 1..=rows {
        // 6 + 22 + 16 = 44 bytes of data, then 34 trailing spaces for total 78.
        let line = format!(
            "{:>6}{:<22}{:>16.1}{:<34}",
            i,
            format!("item_{i}"),
            i as f64 * 1.5,
            ""
        );
        debug_assert_eq!(line.len(), 78);
        writeln!(file, "{line}").unwrap();
    }

    let layout_path = path.with_extension(format!(
        "{}.layout.json",
        path.extension().and_then(|e| e.to_str()).unwrap_or("dat")
    ));
    let layout_json = r#"{
        "fields": [
            { "name": "id",     "start": 0,  "length": 6,  "type": "INTEGER", "nullable": false },
            { "name": "label",  "start": 6,  "length": 22, "type": "VARCHAR", "nullable": false },
            { "name": "amount", "start": 28, "length": 16, "type": "DOUBLE",  "nullable": false }
        ]
    }"#;
    std::fs::write(&layout_path, layout_json)
        .unwrap_or_else(|e| panic!("write layout {}: {e}", layout_path.display()));
    layout_path
}

/// Creates a SQLite database at `path` with a single table `table_name` and
/// schema `(id INTEGER, label TEXT, amount REAL)`.
/// Rows: `id` from 1..=rows, `label = "item_<id>"`, `amount = id * 1.5`.
pub fn generate_medium_sqlite(path: &Path, table_name: &str, rows: usize) {
    let _ = std::fs::remove_file(path);
    let conn =
        Connection::open(path).unwrap_or_else(|e| panic!("open SQLite {}: {e}", path.display()));
    conn.execute_batch(&format!(
        "CREATE TABLE {table_name} (id INTEGER, label TEXT, amount REAL)"
    ))
    .unwrap();
    // Batch inserts in chunks of 500 to avoid hitting SQLite's variable limit.
    const CHUNK: usize = 500;
    let mut i = 1usize;
    while i <= rows {
        let end = (i + CHUNK - 1).min(rows);
        let values: Vec<String> = (i..=end)
            .map(|n| format!("({}, 'item_{}', {:.1})", n, n, n as f64 * 1.5))
            .collect();
        conn.execute_batch(&format!(
            "INSERT INTO {table_name} (id, label, amount) VALUES {}",
            values.join(",")
        ))
        .unwrap();
        i = end + 1;
    }
}
