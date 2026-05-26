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

// Tests live at the end of the module so clippy's
// `items_after_test_module` lint is happy — the `RemoteConnector` trait
// above is referenced by the tests, and clippy requires every public item
// to come before any `#[cfg(test)] mod tests` block.
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::datasource::TableProvider;
    use std::sync::Arc;

    #[test]
    fn connector_error_constructors_set_kind() {
        let e = ConnectorError::connect("host unreachable");
        assert_eq!(e.kind, ConnectorErrorKind::Connect);
        assert_eq!(e.message, "host unreachable");

        let e = ConnectorError::sql("syntax error");
        assert_eq!(e.kind, ConnectorErrorKind::Sql);

        let e = ConnectorError::other("unknown");
        assert_eq!(e.kind, ConnectorErrorKind::Other);

        let e = ConnectorError::new(ConnectorErrorKind::Auth, "bad creds");
        assert_eq!(e.kind, ConnectorErrorKind::Auth);

        let e = ConnectorError::new(ConnectorErrorKind::Timeout, "timed out");
        assert_eq!(e.kind, ConnectorErrorKind::Timeout);

        let e = ConnectorError::new(ConnectorErrorKind::Network, "reset");
        assert_eq!(e.kind, ConnectorErrorKind::Network);
    }

    #[test]
    fn connector_error_display_matches_message() {
        let e = ConnectorError::sql("bad query");
        assert_eq!(format!("{e}"), "bad query");
    }

    #[test]
    fn connector_error_from_string_and_str_is_other() {
        let e: ConnectorError = "oops".into();
        assert_eq!(e.kind, ConnectorErrorKind::Other);
        assert_eq!(e.message, "oops");

        let e: ConnectorError = String::from("oops2").into();
        assert_eq!(e.kind, ConnectorErrorKind::Other);
        assert_eq!(e.message, "oops2");
    }

    struct StubConnector(Vec<String>);

    #[async_trait]
    impl RemoteConnector for StubConnector {
        fn connector_type(&self) -> &'static str {
            "stub"
        }

        async fn table_provider(
            &self,
            _schema: Option<&str>,
            _table: &str,
            _cached_schema: Option<SchemaRef>,
        ) -> ConnectorResult<Arc<dyn TableProvider>> {
            unimplemented!()
        }

        fn capabilities(&self) -> qsql_core::models::ConnectorCapabilities {
            qsql_core::models::ConnectorCapabilities {
                projection: false,
                filter: false,
                limit: false,
                sort: false,
                aggregate: false,
                joins: false,
                dialect_name: "stub".to_string(),
            }
        }

        async fn explain_query(&self, _sql: &str) -> ConnectorResult<String> {
            unimplemented!()
        }

        async fn list_tables(
            &self,
            _schema: Option<&str>,
            _limit: usize,
        ) -> ConnectorResult<Vec<String>> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn list_tables_page_slices_correctly() {
        let names: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
        let connector = StubConnector(names);

        // page 1: offset=3, limit=4
        let page = connector.list_tables_page(None, 3, 4).await.unwrap();
        assert_eq!(page, vec!["t3", "t4", "t5", "t6"]);

        // offset beyond end returns empty
        let page = connector.list_tables_page(None, 20, 4).await.unwrap();
        assert!(page.is_empty());

        // limit=0 is clamped to 1 by saturating max
        let page = connector.list_tables_page(None, 0, 0).await.unwrap();
        assert!(page.is_empty());
    }
}
