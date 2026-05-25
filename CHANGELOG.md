# Changelog

All notable changes to this project will be documented in this file.

## 0.3.0-alpha.0 - Unreleased

Changes since `0.2.1-alpha.0`.

- Upgraded the workspace to DataFusion 53.1.0 and adopted `datafusion-table-providers` plus `datafusion-federation` for SQLite, Postgres, and MySQL/MariaDB connectors.
- Reworked query execution around streaming `QueryResultHandle` responses, per-request `SessionContext` isolation, explicit memory limits, and cancellation coverage.
- Added federation safeguards, including guarded scan budgets, broadcast-join optimization for small local inputs, generation-counter catalog updates, and strict credential redaction.
- Hardened JSON-RPC transport with LSP-style `Content-Length` framing and byte-accurate VS Code client parsing for multi-byte UTF-8 payloads.
- Hardened database registration, table discovery, and explain/lineage planning around concurrent catalog updates.
- Added plan visualization truncation warnings, JSON-RPC framing documentation, architecture remediation notes, CI updates, and expanded federation/runtime tests.

## 0.2.1-alpha.0 - Unreleased

Changes since `0.2.0-alpha.0`.

- Added typed explain-plan models, JSON-RPC explain payloads, and serde coverage for visual query plans.
- Added a VS Code visual query plan panel with metrics formatting, webview rendering, and client tests.
- Added database-level SQL registration so SQLite/Postgres/MySQL/MariaDB sources register as one alias and query tables as `<alias>.<table_name>`.
- Added bounded table discovery plus lazy JIT table-provider registration before execute, explain, and lineage planning.
- Updated the VS Code source explorer to render database aliases as expandable nodes with table children.
- Added JSON-RPC coverage for multi-table database discovery and querying joined tables through the alias-qualified path.

## 0.2.0-alpha.0 - Unreleased

Changes since `0.1.4-alpha.0`.

- Added a shared SQL pushdown layer for projection, basic filters, and limits using DataFusion SQL unparsing.
- Reworked SQLite scans to generate pushed-down SQL instead of always scanning `SELECT *`.
- Added Postgres and MySQL/MariaDB connectors with schema introspection, table registration, and env-gated live tests.
- Added daemon registration methods for Postgres, MySQL, and MariaDB with credential redaction in catalog responses.
- Extended the VS Code connect wizard and source replay to support SQL database profiles backed by SecretStorage.

## 0.1.4-alpha.0 - 2026-05-20

- Introduced a thread-safe, persistent data source catalog inside the Rust daemon supporting CSV, Parquet, and SQLite.
- Added `list_sources`, `remove_source`, and `get_source_metadata` JSON-RPC endpoints to the daemon control plane.
- Implemented a VS Code `SourceManager` with secure operating system keychain storage integration via `SecretStorage` for database credentials.
- Enabled automatic, concurrent workspace source activation and replay during extension load with graceful, isolated error handling.
- Refactored the tree data explorer to pull directly from the daemon's active catalog state, adding custom file/database icons and rich markdown warning tooltips.

## 0.1.3-alpha.0 - 2026-05-20

- Added `query_start`, `query_page`, and `query_cancel` JSON-RPC endpoints to the daemon to enable paged JSON query delivery and caching.
- Implemented robust, cooperative asynchronous query cancellation using Tokio `CancellationToken` in the execution context.
- Added structured parameter validations, defensive maximum page size limits with user warnings, and zero timeout bounds checking.
- Integrated `startQuery`, `getQueryPage`, and `cancelQuery` endpoints on the VS Code extension TypeScript client.

## 0.1.2-alpha.0 - 2026-05-20

- Upgraded daemon and client integration contracts to typed parameter objects and standard JSON-RPC error codes.
- Added comprehensive serde golden testing to ensure backend-frontend contract alignment and eliminate type drift.
- Extended the `RemoteConnector` trait and the SQLite provider to capture and report metadata capabilities.
- Standardized Promise rejection models in the VS Code client to parse and bubble detailed nested query failure payloads.

## 0.1.1-alpha.0 - 2026-05-20

- Initial alpha prototype.
- Added DataFusion-backed Rust daemon.
- Added CSV, Parquet, JSON, and NDJSON file registration.
- Added SQLite table registration.
- Added VS Code extension commands, source explorer, result grid, explain panel, and basic lineage tree.
- Added quickstart samples and open-source project metadata.
- Added Phase 0 baseline JSON-RPC integration tests, benchmark harness, and scanner/webview escaping coverage.
