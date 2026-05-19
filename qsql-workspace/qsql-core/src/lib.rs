pub mod engine;
mod models; // Reserved for future data source and table reference models.

pub use engine::QsqlEngine;

pub const QSQL_CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn init_core() {
    eprintln!("QuiverSQL Core initialized (version {QSQL_CORE_VERSION})");
}
