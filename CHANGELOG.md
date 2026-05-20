# Changelog

All notable changes to this project will be documented in this file.

## 0.1.4-alpha.0 - Unreleased

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
