# QuiverSQL Phase 6.2: Architecture Review Remediation

Phase 6.1 database-level registration is complete. Phase 6.2 is the architecture-review remediation phase that must land before Phase 7 feature work.

All items below — including the items that were previously struck for deferral (broadcast-join rewrite, its explain/metrics visibility, broadcast parity tests, process RSS near-baseline measurement, and the bench-profile compile gate) — have now landed. Phase 6.2 is closed; Phase 7 feature work is unblocked.

## Strict Architecture Review Closure

- [x] Replace `QueryResultHandle`'s cached JSON row vector with bounded `RecordBatch` buffering and page-time JSON serialization.
- [x] Change `execute_sql_to_page` to serve only the requested page from the streaming handle instead of collecting all rows first.
- [x] Build query execution contexts from per-request table-provider snapshots instead of mutating one shared `SessionContext` for JIT database registrations.
- [x] Narrow the connector trait away from arbitrary `execute_query` scan paths; keep only capabilities, table listing, table-provider creation, and source-native explain.
- [x] Retire `SqlTableProvider` / `build_select_sql` from active and test code so pushdown scan execution belongs to `datafusion-table-providers` / `datafusion-federation`.
- [x] Wrap source-native explain calls in per-source timeouts.
- [x] Emit JSON-RPC responses with `Content-Length` framing, retaining request-side compatibility for newline-delimited clients.
- [x] Remove MySQL/MariaDB explicit pool disconnect per operation.
- [x] Add strict acceptance tests for response framing, streaming first-page behavior, per-request snapshot registration, timeout-wrapped explain, credential redaction, and removal of bespoke SQL scan paths.
- [x] Replace the placeholder broadcast-join wrapper with an actual small-local-side-to-remote-fact rewrite and parity tests.
  Completed: the placeholder `QsqlBroadcastJoinOptimizer` is deleted; the real implementation lives in `qsql-workspace/qsql-core/src/broadcast.rs` as a pre-physical-planning async pass invoked from `start_query_stream` and `get_logical_plan_with_broadcast`. Detects inner equi-joins with exactly one federated side, materializes the local side's distinct keys under configurable row+byte caps, and injects an `IN`-list filter that federation pushdown folds into the source-native SQL.

## Execution Order

- [x] **1. Ecosystem Adoption Spike**
  - [x] Pin candidate versions or revisions for `datafusion-table-providers` and `datafusion-federation`.
  - [x] Build a mainline SQLite adapter that registers an upstream `TableProvider` through the existing QuiverSQL catalog/JIT path.
  - [x] Register `datafusion-federation`'s optimizer in the engine session and verify same-source SQLite subplan pushdown.
  - [x] Decide the final migration path from spike results: use latest upstream DataFusion, `datafusion-federation`, and `datafusion-table-providers` as the mainline alpha path.

- [x] **2. SQL Connector Migration**
  - [x] Migrate SQLite, Postgres, and MySQL/MariaDB connectors to thin adapters over upstream table providers.
  - [x] Preserve QuiverSQL-specific behavior: database aliases, SecretStorage-backed source profiles, credential redaction, table discovery, and source-native explain.
  - [x] Add schema cache keyed by source generation and table name with a 5-minute default TTL.
  - [x] Retire hand-rolled SQL scan paths from active providers; keep SQL emission only for source-native explain/golden inspection.
  - [x] Mark review items closed by deletion or upstream ownership: numeric parse-to-zero, broad SQL-to-Arrow type mapping, unparameterized emitted constants in scan execution, MySQL disconnect-per-query, and SQLite EXPLAIN column index assumptions.

- [x] **3. Streaming Query Result Runtime**
  - [x] Add a `QueryResultHandle` over DataFusion `SendableRecordBatchStream`.
  - [x] Replace full `collect()` plus cached `Vec<serde_json::Value>` sessions with active stream handles and bounded batch buffers.
  - [x] Serialize only requested pages from Arrow batches and remove the JSON string round trip.
  - [x] Track rows/bytes pulled, first-page latency, and terminal stream state.
  - [x] Keep JSON pages as the current API default while making Arrow IPC pages a direct follow-on.

- [x] **4. Per-Request Session And Resource Discipline**
  - [x] Create per-request `SessionContext`s from catalog snapshots instead of mutating one process-global context.
  - [x] Configure `RuntimeEnv` memory pools and default query memory limits.
  - [x] Replace catalog/database registration mutex bottlenecks with read-friendly locking.
  - [x] Add a daemon query/task semaphore.
  - [x] Return structured resource-limit errors for query memory, result bytes, page size, and scan-budget violations.

- [x] **5. Federation Safety Surface**
  - [x] Add `GuardedTableProvider` scan budgets with defaults of 1M rows and 1 GiB per remote scan.
  - [x] Add estimator-backed actionable errors that suggest `LIMIT`, tighter filters, or raising a source budget.
  - [x] Add an actual broadcast-join rewrite after federation optimization for small-local-side to remote-fact inner equi-joins.
    Completed in `qsql-core::broadcast::apply_broadcast_rewrites`. Runs after DataFusion logical optimization, materializes the local side's DISTINCT keys with a `LIMIT max_local_rows+1` overflow probe, wraps the remote side in `LogicalPlan::Filter(InList)`, and re-optimizes so federation pushdown folds the filter into the source SQL. Bounded by `BroadcastRewriteConfig` (default 10K rows, 8 MiB).
  - [x] Add explain/metrics visibility for broadcast rewrites.
    Completed: `ExplainQueryResult.broadcast_rewrites: Option<BroadcastRewriteInfo>` surfaces per-application rows/bytes/elapsed; `explain::build_plan_graph_with_broadcast` stamps `broadcast_rewrite=true` + `broadcast_predicate_value_count` attributes on synthesized Filter nodes; one warning per `BroadcastApplication` appears in `warnings`; daemon `diagnostics` RPC exposes `broadcast_rewrites_applied_total`; the VS Code plan webview renders a "Broadcast: N keys" badge on affected nodes.

- [x] **6. Catalog, Security, And Transport Hardening**
  - [x] Fix catalog/JIT TOCTOU with `Arc<DatabaseRegistration>` and per-alias generation counters.
  - [x] Apply credential redaction at the JSON-RPC response boundary for messages and data.
  - [x] Add source operation timeouts: Postgres/MySQL 30s, SQLite 5s, schema introspection 5s, with query override where applicable.
  - [x] Change table discovery to return `(tables, truncated)` and surface 5,000-table truncation warnings.
  - [x] Add lazy table pagination in the VS Code source tree for large schemas.
  - [x] Replace newline-delimited JSON-RPC framing with `Content-Length` framing, with compatibility behavior documented if retained.
  - [x] Add a structured `ConnectorError` enum at the connector boundary.

- [x] **7. Plan Visualization Contract Completion**
  - [x] Implement `MAX_PLAN_NODES = 500` truncation for explain plan graphs.
  - [x] Return typed truncation warnings in `explain_query`.
  - [x] Render plan truncation warnings in the VS Code plan webview.
  - [x] Keep raw plan text byte caps and credential-redacted warning/details behavior.

## Tests And Acceptance

- [x] Cache parity test: repeated federated query issues one schema cache entry per source-table pair within the TTL.
- [x] Streaming first-page benchmark: a 1M-row query returns the first 1K-row page without waiting for full materialization.
  Acceptance: the Criterion first-page benchmark exists and `cargo check --locked -p qsql-daemon --benches` passes; runtime benchmark numbers remain non-gating.
- [x] Memory/resource-budget tests: queries exceeding row, result-buffer, and scan byte/row budgets return structured resource errors.
  Acceptance: fixed the unlimited-scan byte-budget bug and covered it with row and byte guard tests. A direct OOM-style memory-pool exhaustion test is intentionally not part of the default suite.
- [x] Cancellation-under-load test: cancel half of 32 concurrent streaming queries and verify structured cancellation.
  Acceptance: in-process 32-query cancellation coverage in `qsql-core::engine::tests::concurrent_streaming_queries_can_cancel_half_under_load` still passes. Process RSS near-baseline measurement is now wired through `qsql-workspace/qsql-daemon/tests/common/memory.rs` (cross-platform `sysinfo` PID reads) and exercised by `qsql-workspace/qsql-daemon/tests/cancellation_rss.rs::cancellation_returns_rss_near_baseline`, which spawns the daemon subprocess, starts 32 streaming queries, cancels them all, and asserts RSS settles within 50 MiB of the idle baseline.
- [x] Credential-redaction fuzz test: malformed DSNs and connector failures never echo password literals.
- [x] Scan-guard tests: over-budget remote scans fail with actionable errors.
- [x] Broadcast-join tests: small local side plus remote fact join rewrites and returns parity results.
  Completed: 7 unit tests inline in `qsql-core::broadcast::tests` cover detection, predicate synthesis, cap enforcement, empty side, cancellation, and non-inner-join rejection. 4 integration tests in `qsql-workspace/qsql-daemon/tests/broadcast_join_tests.rs` exercise the full CSV ⋈ SQLite path: parity (rewrite-on vs rewrite-off sorted row equality), cap fallback, empty local side via EmptyRelation substitution, and LEFT JOIN ineligibility.
- [x] Plan-truncation test: synthetic 10K-node plan returns `truncated: true` and webview warning text.
- [x] Large-schema test: table-list truncation is surfaced and lazy tree pagination retrieves additional tables.
- [x] Transport tests: JSON-RPC `Content-Length` framing handles large responses and embedded newlines.
- [x] Existing suites: `cargo test --locked --workspace`, `cargo test --locked --workspace --features postgres,mysql`, `npm run typecheck`, `npm run test`, and Criterion benchmark compile smoke.
  Acceptance: all four suites pass. `cargo bench --no-run -p qsql-daemon --bench phase0_benchmarks` now compiles end-to-end (warm rebuild ~1.5 min, cold first build ~49 min) — gated by `[profile.bench]` overrides in `qsql-workspace/Cargo.toml` (opt-level=1, codegen-units=256, lto=false, incremental=true) and added as a CI step in `.github/workflows/ci.yml`. The bench file itself was refactored for engine/Runtime reuse with `Throughput` annotations and now includes two new benches: `broadcast_rewrite_csv_join_sqlite` (rewrite_on vs rewrite_off) and `idle_process_rss_baseline`.

## Defaults

- Remote scan guard defaults: 1,000,000 rows and 1 GiB per source scan.
- Schema cache TTL: 5 minutes.
- Source operation timeouts: SQLite 5s, Postgres/MySQL/MariaDB 30s, schema introspection 5s.
- Plan graph node cap: 500 nodes.
- Table discovery page size/cap remains 5,000 until lazy pagination lands.
