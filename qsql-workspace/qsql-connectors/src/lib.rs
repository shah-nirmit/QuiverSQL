pub mod mysql;
pub mod postgres;
pub mod sql;
pub mod sqlite;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

pub const QSQL_CONNECTORS_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorErrorKind {
    Connect,
    Timeout,
    Auth,
    Sql,
    Network,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorError {
    pub kind: ConnectorErrorKind,
    pub message: String,
}

impl ConnectorError {
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn connect(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Connect, message)
    }

    pub fn sql(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Sql, message)
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Other, message)
    }
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConnectorError {}

impl From<String> for ConnectorError {
    fn from(value: String) -> Self {
        Self::other(value)
    }
}

impl From<&str> for ConnectorError {
    fn from(value: &str) -> Self {
        Self::other(value)
    }
}

pub type ConnectorResult<T> = Result<T, ConnectorError>;

/// A trait implemented by every remote data-source connector.
/// Connectors expose upstream DataFusion table providers plus QuiverSQL-owned
/// catalog and source-native explain surfaces.
#[async_trait]
pub trait RemoteConnector: Send + Sync {
    /// A human-readable name for this connector (e.g. "sqlite", "postgres").
    fn connector_type(&self) -> &'static str;

    /// Build a DataFusion table provider backed by the source engine.
    async fn table_provider(
        &self,
        schema: Option<&str>,
        table: &str,
        cached_schema: Option<SchemaRef>,
    ) -> ConnectorResult<Arc<dyn TableProvider>>;

    /// Returns the capabilities of this connector.
    fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities;

    /// Execute a native EXPLAIN query and return the result as a raw string or JSON representation.
    async fn explain_query(&self, sql: &str) -> ConnectorResult<String>;

    async fn list_tables(&self, schema: Option<&str>, limit: usize)
        -> ConnectorResult<Vec<String>>;

    async fn list_tables_page(
        &self,
        schema: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> ConnectorResult<Vec<String>> {
        let fetch = offset.saturating_add(limit.max(1));
        let tables = self.list_tables(schema, fetch).await?;
        Ok(tables.into_iter().skip(offset).take(limit).collect())
    }
}
