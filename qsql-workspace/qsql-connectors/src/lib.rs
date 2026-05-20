pub mod sqlite;

use async_trait::async_trait;

pub const QSQL_CONNECTORS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A trait implemented by every remote data-source connector.
/// Each connector knows how to execute a SQL query string against
/// its backing store and return the results as Arrow RecordBatches.
#[async_trait]
pub trait RemoteConnector: Send + Sync {
    /// A human-readable name for this connector (e.g. "sqlite", "postgres").
    fn connector_type(&self) -> &'static str;

    /// Execute an arbitrary SQL statement and return the raw JSON rows.
    /// Connectors may choose to push down full SQL or fall back to
    /// `SELECT *` and let DataFusion handle higher-level planning.
    async fn execute_query(&self, sql: &str) -> Result<Vec<serde_json::Value>, String>;

    /// Returns the capabilities of this connector.
    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities;
}
