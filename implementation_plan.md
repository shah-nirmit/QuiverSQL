# QuiverSQL Implementation Plan

This document outlines the finalized multi-phase roadmap for QuiverSQL, incorporating identified risks and their solutions.

## Summary
The goal is to evolve QuiverSQL into a robust, paged, and highly capable local-first query virtualization layer. The control plane remains JSON-RPC over `stdio` initially, with eventual integration of Arrow IPC for the data plane (and potentially gRPC later on). New source integrations (fixed-width, Postgres, MySQL/MariaDB) will be added alongside performance metrics and robust cancellation.

## Key Technical Decisions
- **Arrow IPC Data Plane**: We will initially `base64` encode binary Arrow IPC pages within JSON-RPC responses over `stdio`. If performance is bottlenecked, we will pivot to negotiating an ephemeral local TCP port, named pipe, or consider migrating to **gRPC** for better binary streaming.
- **Query Cancellation**: DataFusion queries will be bound to a `tokio::task::JoinHandle` and a `CancellationToken`. When a `query_cancel` request arrives, the daemon triggers the token and drops the task to immediately free resources.
- **Model Sync**: To prevent drift between Rust models and TypeScript mirrors, we will implement strict "serde golden tests" that verify Rust-generated JSON payloads against TypeScript interfaces.
- **Fixed-Width Files**: Since DataFusion lacks a native fixed-width reader, we will implement a custom `TableProvider` and `ExecutionPlan` in `qsql-core`/`qsql-connectors` that reads files via `std::io::BufReader`, parses rows based on layout metadata, and yields Arrow `RecordBatch` streams.
- **Benchmarking**: We will use the `criterion` crate for Rust benchmarks to ensure stable and robust statistical analysis.

## Phases

### Phase 0: Baseline And Measurement - Complete
- Fix quickstart sample path resolution (handle Windows vs Unix paths correctly).
- Add daemon JSON-RPC integration tests.
- Add `criterion` benchmark harness for file scans, SQLite scans, JSON serialization, first-page latency, and federated joins.
- Tests: quickstart path test, `ping/version/invalid-json` RPC tests, benchmark compile smoke test, scanner and webview escaping tests.

Completed verification:
- `cargo test --locked --workspace`
- `cargo bench --no-run`
- `npm run typecheck`
- `npm run lint`
- `npm run test:scanner`

### Phase 1: Stable Contracts - Complete
- Implement shared Rust models and TypeScript mirrors.
- Replace daemon ad hoc param parsing with typed request structs.
- Return structured JSON-RPC errors.
- Add connector capability metadata (projection, filter, limit, aggregate, joins, dialect name).
- Tests: serde golden tests, invalid-param tests, compatibility tests, typed VS Code client tests.

Completed verification:
- `cargo test --locked --workspace` (golden tests pass)
- `npm run test` (extension scanner and client tests pass)

### Phase 2: Paged JSON Results And Cancellation - Complete
- Add `query_start`, `query_page`, and `query_cancel`.
- Implement `CancellationToken` based query interruption.
- Return first page quickly with schema, metrics, and page metadata.
- Update VS Code grid with loading, cancel, next-page, empty-result, and large-result states.
- Tests: cancellation, timeout, first-page latency, page-size cap, pending-request cleanup, paged grid rendering.

Completed verification:
- `cargo test --locked --workspace` (all 26 integration tests passed successfully)
- `npm run test` (all 18 extension scanner and client integration tests passed successfully)

### Phase 3: Catalog And Source Replay - Complete
- Add runtime source catalog with source metadata, schema, capabilities, registration status, and health.
- Persist source profiles in VS Code storage and replay on activation.
- Store only secret references for database sources; use VS Code SecretStorage for credentials.
- Tests: catalog upsert/list/remove, activation replay, duplicate alias, stale source, metadata-cache invalidation.

### Phase 4: Pushdown And SQL Connectors - Complete
- Extend connector contracts with SQL emission hooks.
- Implement SQLite projection/filter/limit pushdown.
- Add Postgres connector (env-gated live tests, SecretStorage profiles).
- Add MySQL/MariaDB connector (shared implementation with dialect flags).
- Initial SQL scope: table registration, schema introspection, `SELECT` scans, basic pushdowns.
- Tests: SQL-emitter golden tests, pushdown parity tests, fallback tests, optional `QSQL_POSTGRES_URL` and `QSQL_MYSQL_URL` integration tests.

Completed verification:
- `cargo test --locked --workspace`
- `npm run typecheck`
- `npm run lint`
- `npm run test`
- `git diff --check`

#### Pushdown Expansion Gates
- Keep Phase 4 pushdown limited to projection, basic filters, and limits so SQLite/Postgres/MySQL/MariaDB correctness can be proven with parity tests before broader SQL delegation.
- Expand into sort/top-k pushdown immediately after Phase 4 is stable, starting with simple column `ORDER BY`, explicit direction, and optional `LIMIT`.
- Expand into aggregate pushdown after type mapping, metrics, and explain output are reliable, starting with `COUNT`, `MIN`, `MAX`, `SUM`, `AVG`, and simple `GROUP BY` columns.
- Expand into join pushdown last, starting only with same-source inner equi-joins where all tables belong to the same connector profile/database.
- Keep cross-source joins inside DataFusion until QuiverSQL has a federation-aware planner layer.

### Phase 5: Query Plan Visualization - Completed
- Add typed visual explain models for federated plans, source-native subplans, plan nodes, edges, metrics, attributes, warnings, and raw source text.
- Implement safe DataFusion logical-plan visualization without using the current `execute_json EXPLAIN` path for the new UI.
- Add source-native explain support for SQLite, Postgres, MySQL, and MariaDB using planner-only, non-ANALYZE explain commands.
- Add daemon `explain_query` JSON-RPC method with typed params/results and credential-redacted warnings/details.
- Replace the VS Code `qsql.visualizePlan` text-document flow with a webview plan panel.
- Add Tree, Table, and Source views with search, node selection, source-native drilldowns, copy plan/source, empty/error states, and VS Code theme styling.
- Add safeguards for large plans: stable node IDs, node-count truncation, raw-text byte caps, and clear truncation warnings.
- Tests: serde golden tests, DataFusion plan graph tests, native explain parser fixtures, daemon RPC tests, webview render tests, command/client tests, and SQL connector live-test coverage where env vars are set.

Phase 5 defaults and constraints:
- Combined plan scope: show QuiverSQL/DataFusion's federated plan plus source-native subplans where they can be obtained safely.
- UI scope: deliver Tree, Table, and Source views; defer icicle view and richer color-control UI.
- Use planner-only explain paths by default. `EXPLAIN ANALYZE` and runtime-metric visualization remain deferred because they execute the query.
- Show cost/row values only when the underlying planner provides estimates; do not invent DataFusion cost values.

### Phase 6.1: Database-Level Registration And Schema Mapping - Complete
- Deprecate single-table SQL database registration in favor of registering a whole database or schema under one alias.
- Query SQL-backed tables as `<alias>.<table_name>` while file registrations remain top-level table aliases.
- At registration time, list up to 5,000 table names for catalog and UI display without instantiating every `TableProvider`.
- At query/explain/lineage time, parse SQL with `sqlparser`, discover qualified table references, and lazily register only the referenced SQL tables into DataFusion.
- Store SQL catalog entries with table lists and redacted connection details; keep credentials in VS Code SecretStorage-backed source profiles.
- Render database aliases as expandable tree nodes in VS Code with table children.
- Tests: AST table-reference extraction, JSON-RPC SQLite database registration/query through `<alias>.<table>`, catalog table metadata, frontend typecheck and client/scanner tests.

Completed verification:
- `cargo test --locked --workspace`
- `npm run typecheck`
- `npm run test`

### Phase 6.2: Architecture Review Remediation - Complete
Phase 6.2 absorbs the principal architecture review before Phase 7. The order is intentional: shrink the connector/planner code first, then replace the result/session runtime, then harden the surviving surface.

#### 6.2A: DataFusion Ecosystem Adoption
- Adopt latest upstream `datafusion`, `datafusion-table-providers`, and `datafusion-federation` as the mainline alpha path, starting with SQLite, then Postgres and MySQL/MariaDB.
- Narrow `qsql-connectors` into QuiverSQL-specific adapters around upstream `TableProvider` factories; preserve catalog aliases, SecretStorage references, redacted catalog responses, source-native `explain_query`, and bounded table discovery.
- Register `datafusion-federation`'s optimizer in the QuiverSQL session setup so same-source subplans can push down and cross-source plans can push the largest source-local subplans.
- Delete or retire the hand-rolled `SqlTableProvider`, `build_select_sql`, driver-level scan/execute paths, and duplicated connection lifecycle code after parity holds.
- Treat connector bugs that disappear with deletion as closed by migration: numeric parse-to-zero, overly broad SQL-to-Arrow type mapping, unparameterized emitted constants, MySQL pool disconnect-per-query, and SQLite EXPLAIN column-index assumptions.
- Keep a QuiverSQL-owned schema cache around upstream introspection keyed by source generation and table name.

#### 6.2B: Streaming Runtime And Session Isolation
- Replace `collect -> Vec<serde_json::Value> -> cached slice` with a streaming `QueryResultHandle` over DataFusion `SendableRecordBatchStream`.
- Serialize only the requested page from Arrow batches, avoid the JSON string round trip, and keep Arrow IPC page support as a natural extension of the same handle.
- Replace completed result-vector sessions with active stream handles, bounded buffered batches, cancellation, rows/bytes pulled metrics, and terminal state when the stream is exhausted.
- Create per-request `SessionContext`s from a catalog snapshot instead of mutating one process-global context; configure `RuntimeEnv` memory pools and default memory limits.
- Replace catalog and database registration `Mutex<HashMap>` usage with read-friendly locking, and add a daemon task semaphore so runaway or hung requests cannot exhaust the daemon.
- Add query memory and result-size guards that return structured errors instead of risking daemon OOM.

#### 6.2C: Safety, Correctness, And UX Contracts
- Wrap source `TableProvider`s in scan guards with default remote scan ceilings of 1M rows and 1 GiB, configurable per source, with actionable errors when users need `LIMIT`, tighter filters, or higher budgets.
- Bounded broadcast-join rewrite for small local side plus remote-fact inner equi-joins, gated by a configurable row + byte cap. Implemented in `qsql-workspace/qsql-core/src/broadcast.rs` as an async pre-physical-planning pass that materializes the local side's DISTINCT keys (overflow-probed via `LIMIT cap + 1`), wraps the remote side in `LogicalPlan::Filter(InList)`, and re-optimizes so federation pushdown folds the predicate into the source-native SQL. Skips cleanly on non-inner joins, multi-key joins, both-sides-federated, materialization cap overrun, and cancellation — never errors a query.
- Fix catalog/JIT TOCTOU by storing registrations as `Arc<DatabaseRegistration>` with generation counters and refusing stale retries after removal or reconnect.
- Move credential redaction to the JSON-RPC response boundary, covering messages and data for key/value secrets and DSNs.
- Add connector/source timeouts: Postgres/MySQL 30s, SQLite 5s, schema introspection 5s, with query-level override where applicable.
- Implement Phase 5's promised plan node-count truncation with `MAX_PLAN_NODES = 500`, typed warnings, and webview warning rendering.
- Change table discovery to return `(tables, truncated)` and surface 5,000-table truncation in registration responses; add lazy tree pagination for large database schemas.
- Move daemon JSON-RPC framing from newline-delimited JSON to LSP-style `Content-Length` framing while preserving compatibility only if explicitly needed.
- Add a structured `ConnectorError` boundary enum for connect, timeout, auth, SQL, network, and other errors.
- Add quick wins that still survive ecosystem migration: dialect `Unparser` caching if still used, pathological self-join table-ref dedup tests, and cache invalidation tests.

#### 6.2 Verification
- Cache parity: the same federated query should create one schema cache entry per source-table pair within the TTL.
- Streaming first-page latency: a 1M-row query should serve the first 1K-row page without waiting for full materialization; keep a Criterion benchmark and compile-smoke it with `cargo check --benches` until bench-profile compilation is stable locally/CI.
- Memory/resource budget: queries exceeding result-buffer or remote scan row/byte budgets return structured resource errors instead of risking OOM.
- Cancellation under load: concurrent streaming queries can be cancelled. Process RSS near-baseline checks are wired through `qsql-workspace/qsql-daemon/tests/common/memory.rs` (PID-based `sysinfo` reads on Linux/Windows/macOS) and exercised by `qsql-workspace/qsql-daemon/tests/cancellation_rss.rs`, which starts 32 streaming queries, cancels them all, and asserts the daemon subprocess RSS settles within `DEFAULT_RSS_TOLERANCE_BYTES` (50 MiB) of its idle baseline. Future RSS assertions belong in that helper module.
- Credential safety: malformed DSNs and source errors never leak password literals in JSON-RPC responses.
- Federation safety: scan guards block estimated over-budget remote scans. Broadcast-join rewrite parity is enforced by `qsql-workspace/qsql-daemon/tests/broadcast_join_tests.rs`, which asserts byte-for-byte sorted row equality between rewrite-on and rewrite-off runs across CSV ⋈ SQLite, plus cap-overflow fallback, empty-local-side EmptyRelation substitution, and LEFT-JOIN ineligibility cases. Explain visibility flows through `ExplainQueryResult.broadcast_rewrites`, plan-graph node attributes, and the VS Code badge in `planVisualizationPanel.ts`.
- Plan safety: a synthetic 10K-node plan returns `truncated: true` and the webview shows a clear warning.
- Compatibility: `cargo test --locked --workspace`, `npm run typecheck`, `npm run test`, and benchmark compile smoke remain green with no material regression.

### Phase 7: Sort/Top-K Pushdown And Guard UX - Complete

**Key technical fact**: `datafusion-federation`'s SQL unparser converts the full pushable logical plan — including `Sort` nodes — into a complete SQL string (with `ORDER BY` + `LIMIT` embedded) before calling `SQLExecutor::execute()`. Sort pushdown therefore already works automatically via the federation layer for all SQL connectors (confirmed by comparison with spiceai's approach). Phase 7 tracks this capability formally, proves it with parity tests, and improves guard error UX.

#### 7A: Sort Capability Flag
- Add `sort: bool` field to `ConnectorCapabilities` in `qsql-core/src/models.rs`.
- Set `sort: true` in `sql_capabilities()` for all SQL dialects (SQLite, Postgres, MySQL/MariaDB).
- Update the `ConnectorCapabilities` serde golden test to include `"sort": true`.
- Update the `sql_capabilities_reflects_dialect_name` unit test to assert `caps.sort`.

#### 7B: Sort Parity And Verification Tests
- Write `qsql-workspace/qsql-daemon/tests/sort_pushdown_tests.rs` with a shuffled-insert SQLite helper so natural row order ≠ sort order.
- Tests: ASC single-column parity, DESC single-column parity, no-LIMIT full sort, combined filter+sort+limit, SQLite EXPLAIN QUERY PLAN output contains ORDER BY, env-gated Postgres parity, env-gated MySQL parity.
- Medium fixture smoke tests: generate 100K-row CSV and 100K-row SQLite at test time via `tests/common/fixtures.rs`; run `ORDER BY id DESC LIMIT 3` and assert first `id = 100000`.

#### 7C: Sort Explain Visibility
- In `qsql-workspace/qsql-daemon/src/explain.rs`, add `sort_columns` and `sort_pushed_down` attributes to `LogicalPlan::Sort` nodes in `traverse_plan()`.
- In `qsql-vscode/src/planVisualizationPanel.ts`, render a "Sort ↓ pushed" badge on Sort nodes where `sort_pushed_down = "true"`.

#### 7D: Structured Scan-Guard Error Codes
- In `qsql-workspace/qsql-core/src/engine.rs`, prefix `scan_budget_error()` messages with `[QSQL_SCAN_GUARD]`.
- In `qsql-workspace/qsql-daemon/src/lib.rs`, detect the sentinel and map to `QueryError { code: -32100 }` instead of the generic `-32603`.
- Add `SCAN_GUARD_ERROR_CODE: i32 = -32100` constant in `qsql-core/src/models.rs`.
- In `qsql-vscode/src/models.ts`, add `SCAN_GUARD_ERROR_CODE` constant and `isScanGuardError()` helper.
- In `qsql-vscode/src/webviewPanel.ts`, render an actionable suggestion banner (Add LIMIT, Add WHERE filter, Raise budget) when the error code is `-32100`.

#### 7E: Medium/Large Fixture Generation
- Add `qsql-workspace/qsql-daemon/tests/common/fixtures.rs` with `generate_medium_csv()`, `generate_medium_sqlite()`, and `unique_temp_path()` helpers.
- Schema: `(id INTEGER, label TEXT, amount REAL)`, rows `1..=n`, cleaned up via RAII temp guard.
- Used by smoke tests and benchmark setup; no large binary files committed.

#### 7F: Benchmark Additions
- Add `sort_pushdown_sqlite_1k_rows` and `sort_no_pushdown_csv_1k_rows` benchmark groups to `qsql-workspace/qsql-daemon/benches/phase0_benchmarks.rs` using the existing `once_cell` singleton pattern.

#### 7G: TypeScript Guard UX Tests
- Add tests verifying `isScanGuardError()` helper, suggestion banner renders for code `-32100`, and banner is absent for code `-32603`.

#### 7H: Explain Plan Revamp (evidence-driven attribution + provider icons)

Lands the Phase 7 finalisation. Replaces structural pattern-matching of the logical-plan tree with capture-from-the-physical-plan and stamp-from-rewrite-info, so badges and SQL stay correct regardless of what subsequent optimiser passes do.

- Capture the real pushed-down SQL by walking the physical plan: downcast `datafusion-federation::sql::VirtualExecutionPlan` for the federation path, and pattern-match `<Word>Exec sql=…` for the non-federation path (`SqlExec` from `datafusion-table-providers`, `MySQLSQLExec` and similar DB-specific execs). Attribute each captured SQL string back to its logical `TableScan` by searching the SQL's `FROM` clause for the table's quoted identifier across all dialect styles.
- Add `provider_kind` + `remote_sql` to `PlanNode` and a typed `SourcePlanEntry { provider_kind, native_sql, native_explain, dialect }` to `ExplainQueryResult.source_plans` (additive, omitted via `skip_serializing_if = "Option::is_none"` so older clients stay compatible).
- Run remote `EXPLAIN` against the captured pushed-down SQL instead of the placeholder `SELECT * FROM table` so the per-table Remote EXPLAIN reflects the actual query.
- Replace the Source-tab "Native Source Plans" dump with per-table cards (Native SQL → Remote EXPLAIN → Logical fragment), a collapsible `DataFusion Physical Plan` section (collapsed by default), and a legend bar above the plan graph. Tree-tab `TableScan` clicks scroll to the matching card.
- Centralise provider iconography in `qsql-vscode/src/providerIcons.ts` (single source of truth for tree-item `iconPath`, embedded SVG `<symbol>` set for the plan graph, and human-readable labels). Icon files live in `qsql-vscode/media/icons/`.
- Drive the **sort** badge from captured `remote_sql` content (`ORDER BY` substring scan over every `TableScan` in the subtree) instead of the structural `subtree_is_fully_federated` heuristic — eliminates false positives on multi-source joins.
- Drive the **broadcast** badge from `BroadcastRewriteInfo.applied` instead of structural `Filter+InList` matching. Stamps three surfaces with a `broadcast_role` discriminator (`remote_scan` → "Broadcast IN ↓ N keys"; `local_scan` → "Broadcast keys ↑ N keys"; `join` → "Broadcast ⇆ N keys"; legacy `filter` → "Broadcast: N keys") so the badge survives Filter folding by downstream optimisation.
- Add a `QSQL_EXPLAIN_TRACE=1` daemon env-var that emits one stderr line per physical-plan node, kept as a developer diagnostic (no VS Code setting exposure).

#### 7 Verification
- `cargo test --locked --workspace` and `--features postgres,mysql` green.
- `cargo test --locked -p qsql-connectors` and `-p qsql-daemon` green (sort parity + medium fixture smoke + 43 explain unit tests covering remote-SQL capture, provider classification, sort/broadcast attribution).
- `cargo check --locked -p qsql-daemon --benches` benchmark compile smoke.
- `npm run typecheck && npm run lint && npm run test` green (includes provider-icon registry coverage + Explain panel per-table-card rendering tests).
- Acceptance: `sort: true` in golden JSON; parity tests confirm ORDER BY arrives at SQLite layer; scan guard returns code `-32100`; VS Code renders suggestion banner for guard errors.
- Acceptance: Section 8 of `USER_GUIDE.md` walks end-to-end through the federated demo and confirms (a) provider icons in the Data Sources tree and on Tree-tab `TableScan` nodes, (b) per-table cards in the Source tab carrying the *actual* pushed-down SQL, (c) the broadcast badge surfaces on the remote scan + Join + local scan even when the post-rewrite optimiser folds the synthesised Filter, (d) the sort badge fires only when every leaf's captured SQL contains `ORDER BY`.

### Phase 8: Fixed-Width File Support - Complete

Phase 8 turns the existing `SourceKind::FixedWidth` enum variant (already mirrored in TypeScript, the sidebar icon set, and the explain-plan provider-kind table) into a real source. The format dispatch in `engine.rs::register_file` is the only missing arm — Phase 8 adds it, plus the streaming `ExecutionPlan`, layout file format, VS Code wizard branch, sample fixtures, tests, benchmark, and docs.

**Key technical fact**: DataFusion's built-in readers (CSV/Parquet/JSON) infer schemas; fixed-width has no headers, so the layout must be user-supplied. The chosen approach uses a JSON layout file alongside the data file. A streaming `ExecutionPlan` keeps memory bounded the same way `CsvExec` does today.

#### 8A: Layout File + Arrow Schema
- New module `qsql-workspace/qsql-core/src/fixed_width.rs` with `FixedWidthLayout { fields: Vec<FixedWidthField> }` + serde derives.
- `FixedWidthField { name, start, length, type, nullable, trim }`.
- `FixedWidthLayout::from_json_path(path)` / `from_json_str(s)` / `arrow_schema() -> SchemaRef`.
- Validation: overlapping spans, zero/negative `length`, unknown `type`, empty `fields` list.
- Relocate `sql_type_to_arrow` from `qsql-connectors/src/sql.rs:99-200` to a new `qsql-core/src/sql_types.rs` and have `qsql-connectors` re-export to avoid a crate cycle.

#### 8B: Streaming TableProvider + ExecutionPlan
- `FixedWidthTableProvider { layout, path }` impl `TableProvider` — returns `Unsupported` for filter pushdown in v1; honours projection + limit via the exec.
- `FixedWidthExec` impl `ExecutionPlan` — single partition, `Bounded`, `Incremental`, `DisplayAs` prints `FixedWidthExec path=… rows_read=…`.
- `FixedWidthRowStream` impl `RecordBatchStream` — `BufReader<File>`, batch size 8192, byte-offset slicing, projection applied during column-building, limit honoured as row counter, parse errors include row + column + offending slice.

#### 8C: Daemon `register_file` Payload Extension
- Extend `RegisterFileRequest` ([qsql-daemon/src/lib.rs:59](qsql-workspace/qsql-daemon/src/lib.rs)) with `options: Option<HashMap<String, serde_json::Value>>` (skip-if-none for wire compat).
- Extend `QsqlEngine::register_file` ([qsql-core/src/engine.rs:683](qsql-workspace/qsql-core/src/engine.rs)) with the same `options` argument; CSV/JSON/Parquet arms ignore it.
- New `"fixed_width"` arm: reads `options["layout_path"]`, loads `FixedWidthLayout`, constructs `FixedWidthTableProvider`, calls `register_table_entry` (not wrapped in `GuardedTableProvider` — same treatment as other local file providers).
- Mirror `options` in `qsql-vscode/src/models.ts` + `qsql-vscode/src/daemonClient.ts`.

#### 8D: VS Code Wizard Branch (Two-File Picker)
- Add `Fixed-width File` to the `qsql.connectWizard` quickpick at `qsql-vscode/src/extension.ts:478-486`.
- New branch: `showOpenDialog` for data file → `showOpenDialog` for layout file (filter `.json`) → `showInputBox` for alias → `register_file` RPC with `format: 'fixed_width'` and `options: { layout_path }`.
- Bad-file errors → `vscode.window.showErrorMessage` + bail (matches the SQLite picker pattern).

#### 8E: SourceProfile Persistence + Replay
- Extend `PersistentSourceProfile.details` ([sourceManager.ts:4-14](qsql-vscode/src/sourceManager.ts)) with optional `layoutPath?: string`.
- Add a `fixed_width` arm to `replaySources()` (`sourceManager.ts:84-132`) resending the same `register_file` payload.

#### 8F: Sample Fixtures
- `samples/quickstart/employees_fwf.txt` mirroring `employees.csv` row-for-row.
- `samples/quickstart/employees_fwf.layout.json` describing the spans.
- Extend `qsql-workspace/qsql-connectors/examples/generate_quickstart_samples.rs` to emit both from the same source rows.

#### 8G: Tests
- **Layout unit** (`fixed_width.rs`): JSON round-trip; overlapping-spans / zero-length / unknown-type / empty-fields rejection; type-mapping coverage (INTEGER, BIGINT, REAL, DOUBLE, VARCHAR/TEXT, BOOLEAN, DATE, TIMESTAMP).
- **Stream unit** (`fixed_width.rs`): tiny layout + `Cursor` → expected batches; projection trims columns; limit truncates; UTF-8 multi-byte rows; ragged-row → error with row index.
- **Daemon integration** (new `qsql-workspace/qsql-daemon/tests/fixed_width_tests.rs`): JSON-RPC `register_file` with `format: "fixed_width"`, then `execute` confirms row-set parity vs the CSV equivalent.
- **Medium fixture smoke** (Phase 7E pattern): `generate_medium_fwf(path, rows)` added to `tests/common/fixtures.rs`; 100K rows; `SELECT id FROM t ORDER BY id DESC LIMIT 3`; first `id == 100_000`.
- **VS Code** (`detectQueries.test.ts`): wizard payload carries `format: 'fixed_width'` + `options.layout_path`.

#### 8H: Benchmark
- Add `fixed_width_file_scan_1k_rows` and `fixed_width_file_scan_to_json` groups to `qsql-workspace/qsql-daemon/benches/phase0_benchmarks.rs`, mirroring the existing CSV pattern.

#### 8I: Documentation
- `USER_GUIDE.md` — new Section 2.C with layout JSON example + smoke query.
- `README.md` — `Fixed-width files` row Planned → Supported; intro paragraph updated.
- `CHANGELOG.md` — Phase 8 bullet under the running `0.3.1-alpha.0 - Unreleased` block.
- `implementation_plan.md` — this section's status flipped `Current → Complete` on landing.
- `current_phase_task.md` — checkboxes ticked as work lands.

#### 8J: Clippy + Housekeeping
- Roll in the two pre-existing clippy fixes from the previous turn: `qsql-connectors/src/lib.rs` (move `mod tests` to end of file) and `qsql-daemon/src/lib.rs:1563` (drop `.clone()` on the Copy `ConnectorErrorKind`).
- Re-run `cargo clippy --locked --workspace --all-targets -- -D warnings` after each wave; the gate stays green throughout Phase 8.

#### 8 Verification
- `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, `cargo test --locked --workspace` green.
- `cargo check --locked -p qsql-daemon --benches` benchmark compile smoke.
- `npm run typecheck && npm run lint && npm run test` green.
- Acceptance: `register_file` with `format: "fixed_width"` + `options.layout_path` registers a queryable table; rows match the CSV equivalent under `ORDER BY id`.
- Acceptance: Explain on the fixed-width table shows `provider_kind: "fixed_width"` and renders the fixed-width glyph on the `TableScan` node.
- Acceptance: Restarting the Extension Host replays the persisted fixed-width source with no re-prompt.
- Acceptance: An overlapping-spans layout, a zero-length field, and an unknown type each produce a descriptive `register_file` error (not a panic).

#### 8 Parallelization Plan
| Wave | Sub-phases (parallel) | Depends on |
|------|------------------------|------------|
| Wave 1 | 8A · 8C · 8F · 8J | none — all independent |
| Wave 2 | 8B · 8D | Wave 1 (8B needs 8A's schema builder; 8D needs 8C's payload field) |
| Wave 3 | 8E · 8G layout+stream · 8H | Wave 2 |
| Wave 4 | 8G daemon-integration + VS Code tests | Wave 3 |
| Wave 5 | 8I docs + final clippy + amend | All above |

### Phase 9: Arrow IPC Result Pages
- Add Arrow IPC for large/requested result pages (base64 initially over stdio).
- Negotiate result format through `query_start`/`query_page`.
- Keep JSON rows as default for small results.
- Tests: Arrow IPC round-trip, JSON-vs-Arrow parity, schema fidelity, fallback-to-JSON.

### Phase 10: Rich Explain, Lineage, And Performance Visibility
- Upgrade lineage to include source columns, output columns, aliases, joins, aggregates, and CTEs.
- Expand visual explain output with runtime metrics, full-scan warnings, pushdown reasoning, and optional `EXPLAIN ANALYZE` data.
- Surface metrics in VS Code result messages.
- Tests: lineage golden tests, explain snapshots, metrics rendering, full-scan warnings.

### Phase 11: Packaging And Gates
- Package platform daemon binaries with the VSIX.
- Add CI benchmark report artifacts.
- Defer Arrow Flight / gRPC data plane until remote clients/multi-client sessions become concrete.
- Tests: packaged daemon path tests, version-surface tests, artifact build tests, benchmark report generation tests.

## Verification Plan

### Automated Tests
- Run `cargo test` in `qsql-workspace`.
- Run extension unit and scanner tests (`npm run test:scanner`).
- Run `cargo bench` to ensure benchmark compilation.

### Manual Verification
- Test quickstart resolution in the VS Code Extension host.
- Verify sample data is properly accessible across operating systems.
