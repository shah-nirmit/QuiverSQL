# Changelog

All notable changes to this project will be documented in this file.

## 0.1.2-alpha.0 - Unreleased

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
