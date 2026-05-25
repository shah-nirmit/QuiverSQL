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
- `cargo test --locked -p qsql-connectors`
- `cargo test --locked --workspace --features postgres,mysql`
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
- `cargo test --locked --workspace --features postgres,mysql`
- `npm run typecheck`
- `npm run test`

### Phase 6.2: Architecture Review Remediation - Current
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
- Compatibility: `cargo test --locked --workspace`, `cargo test --locked --workspace --features postgres,mysql`, `npm run typecheck`, `npm run test`, and benchmark compile smoke remain green with no material regression.

### Phase 7: Sort/Top-K Pushdown And Guard UX
- Verify whether upstream providers already push simple `ORDER BY` plus optional `LIMIT`; if so, focus on parity tests, explain visibility, and user-facing metrics.
- Surface byte/row limit errors from Phase 6.2 in the VS Code result grid and explain panel.
- Generate medium/large local fixtures during tests instead of committing them.
- Tests: SQL sort/top-k golden/parity tests, result-size guard rendering tests, generated large CSV/Parquet smoke tests, and benchmark regression checks.

### Phase 8: Fixed-Width File Support
- Add fixed-width file registration with required layout metadata.
- Implement custom DataFusion `TableProvider` for fixed-width parsing.
- Add VS Code connect wizard flow for fixed-width data file plus layout file.
- Tests: layout parse tests, malformed layout tests, fixed-width query tests, type coercion tests, malformed-row tests, wizard validation tests.

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
