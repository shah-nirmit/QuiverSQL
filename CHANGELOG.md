# Changelog

All notable changes to this project will be documented in this file.

## 0.3.1-alpha.0 - Unreleased

Changes since `0.3.0-alpha.0`. Finalises Phase 7 (Sort/Top-K Pushdown + Scan-Guard UX) and revamps the Explain Plan around evidence-driven attribution.

- Formalised sort / top-k pushdown for SQL connectors: `sort: true` capability flag, parity tests across SQLite/Postgres/MySQL, medium-fixture sort smoke tests, and a `sort_pushdown_sqlite_1k_rows` benchmark for regression tracking.
- Added structured scan-guard error code `-32100` with the `[QSQL_SCAN_GUARD]` sentinel and a VS Code suggestion banner that proposes `LIMIT`, additional `WHERE`, or a budget bump when a remote scan exceeds its row/byte budget.
- Captured the *actual* pushed-down SQL per remote `TableScan` by walking the DataFusion physical plan and parsing both `datafusion-federation`'s `VirtualExecutionPlan` and `datafusion-table-providers`' generic `SqlExec` plus DB-specific variants (`MySQLSQLExec`). Replaced the `EXPLAIN SELECT * FROM table` placeholder with the real query — including projection, filter, sort, limit, and broadcast `IN (…)` pushdowns.
- Replaced the Explain panel's "Native Source Plans" wall of text with per-table cards stacked in execution order: Native SQL → Remote EXPLAIN of that SQL → DataFusion logical fragment, with copy-to-clipboard on each section and a clickable cross-link from the Tree-tab `TableScan` to its card.
- Added provider-specific icons for the Data Sources sidebar and plan-graph `TableScan` nodes (Postgres, MySQL, MariaDB, SQLite, CSV, NDJSON, JSON, Parquet, fixed-width). New `providerIcons` module centralises the icon/label registry.
- Reworked the broadcast-rewrite badge to drive off `BroadcastRewriteInfo.applied` rather than structural Filter-node pattern matching, so the badge survives downstream optimiser rearrangements. Stamps three surfaces per rewrite — remote `TableScan` (`Broadcast IN ↓ N keys`), local `TableScan` (`Broadcast keys ↑ N keys`), and the rewritten `Join` (`Broadcast ⇆ N keys`) — with a `broadcast_role` discriminator on each.
- Reworked the sort-pushdown badge to fire only when every `TableScan` in the Sort's subtree has a captured `remote_sql` containing `ORDER BY` — eliminates the false positive on multi-source federated joins where the join (and therefore the sort) must execute locally.
- Added a collapsible `DataFusion Physical Plan` section to the Source tab (collapsed by default) and a legend bar above the plan graph explaining badge colours.
- Added an `Explain Plan` walkthrough (`USER_GUIDE.md` §8) covering provider icons, the per-table card layout, click-through from Tree to Source, and how each pushdown surfaces visually.
- The daemon honours `QSQL_EXPLAIN_TRACE=1` as a developer diagnostic that emits one stderr line per physical-plan node during Explain — useful for future Explain capture issues, not exposed as a VS Code setting.
- **Phase 8 — Fixed-Width File Support.** Custom streaming `TableProvider` + `ExecutionPlan` (`qsql-core/src/fixed_width.rs`) driven by a JSON layout sidecar describing each column's byte-offset, length, SQL type, and nullability. Honours projection + limit pushdown; filter pushdown returns `Unsupported` so DataFusion wraps the scan in `FilterExec`.
- `RegisterFileRequest` gains an additive `options: Option<HashMap>` field (`#[serde(skip_serializing_if = "Option::is_none")]` for wire compatibility). Daemon engine exposes `register_file` (3-arg shim) plus `register_file_with_options` (full 4-arg form) so existing callers stay untouched.
- VS Code connect wizard gains a "Fixed-width File" branch with a two-file picker (data + layout). `PersistentSourceProfile.details` gains an optional `layoutPath`; source replay resends `register_file` with `options.layout_path` so persisted fixed-width sources auto-restore across Extension Host restarts.
- Quickstart sample pair: `samples/quickstart/employees_fwf.txt` (six 79-byte rows mirroring `employees.csv`) + `employees_fwf.layout.json`. New `USER_GUIDE.md` Section 2.C walks through attaching them.
- Relocated `sql_type_to_arrow` and `schema_from_fields` from `qsql-connectors/src/sql.rs` to `qsql-core/src/sql_types.rs` so the fixed-width module can reuse them without a cross-crate cycle. `qsql-connectors` re-exports the symbols verbatim — existing callers stay byte-identical.
- New tests: 13 `fixed_width.rs` unit (layout parse, validation, type mapping) + 5 `qsql-daemon/tests/fixed_width_tests.rs` integration (registration + parity vs CSV + 100K-row medium-fixture smoke + missing/bad layout error paths) + 6 `qsql-core/src/sql_types.rs` unit + 2 TypeScript (profile shape, icon registry).

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
