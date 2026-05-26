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
