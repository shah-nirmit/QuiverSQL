pub mod broadcast;
pub mod engine;
pub mod fixed_width;
pub mod models;
pub mod result_ipc;
pub mod sql_types;
pub mod table_refs;

pub use broadcast::{
    apply_broadcast_rewrites, BroadcastApplication, BroadcastRewriteConfig, BroadcastRewriteInfo,
    BroadcastSkip, SkipReason, DEFAULT_MAX_LOCAL_BYTES, DEFAULT_MAX_LOCAL_ROWS,
};
pub use engine::{
    GuardedTableProvider, QsqlEngine, QueryResultHandle, ScanBudget,
    DEFAULT_QUERY_MEMORY_LIMIT_BYTES, DEFAULT_REMOTE_SCAN_MAX_BYTES, DEFAULT_REMOTE_SCAN_MAX_ROWS,
};
pub use table_refs::{extract_database_table_refs, DatabaseTableReference};

pub const QSQL_CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn init_core() {
    eprintln!("QuiverSQL Core initialized (version {QSQL_CORE_VERSION})");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_core_does_not_panic() {
        init_core();
    }

    #[test]
    fn version_constant_is_non_empty() {
        assert!(!QSQL_CORE_VERSION.is_empty());
    }
}
