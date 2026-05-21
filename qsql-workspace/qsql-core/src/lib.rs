pub mod engine;
pub mod models;
pub mod table_refs;

pub use engine::QsqlEngine;
pub use table_refs::{extract_database_table_refs, DatabaseTableReference};

pub const QSQL_CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn init_core() {
    eprintln!("QuiverSQL Core initialized (version {QSQL_CORE_VERSION})");
}
