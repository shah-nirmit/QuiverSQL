# QuiverSQL Phase 7: Sort/Top-K Pushdown And Guard UX

Phase 6.2 architecture review remediation is complete. Phase 7 adds formal sort capability tracking (sort pushdown already works via `datafusion-federation`'s SQL unparser), proves it with parity tests, improves scan-guard error UX, generates fixtures at test time, and adds sort benchmarks.

## Execution Order

- [x] **1. Sort Capability Flag (7A)**
  - [x] Add `pub sort: bool` to `ConnectorCapabilities` in `qsql-workspace/qsql-core/src/models.rs` (after `limit`, before `aggregate`).
  - [x] Add `SCAN_GUARD_ERROR_CODE: i32 = -32100` constant to `qsql-workspace/qsql-core/src/models.rs`.
  - [x] Update `sql_capabilities()` in `qsql-workspace/qsql-connectors/src/sql.rs` to set `sort: true` for all SQL dialects.
  - [x] Update the `sql_capabilities_reflects_dialect_name` unit test in `sql.rs` to assert `caps.sort`.
  - [x] Update the `ConnectorCapabilities` golden test in `qsql-workspace/qsql-core/tests/serde_golden_tests.rs` to include `"sort": true`.
  - [x] Fix all stub connectors (in `lib.rs` and `explain.rs` tests) to include `sort: false`.

- [x] **2. Sort Parity And Verification Tests (7B)**
  - [x] Create `qsql-workspace/qsql-daemon/tests/sort_pushdown_tests.rs` with a `make_shuffled_sqlite(n)` helper that inserts rows in non-sequential order.
  - [x] `sort_asc_single_column_sqlite_parity`: `ORDER BY id ASC LIMIT 10`, assert `id=1` first, `id=10` last.
  - [x] `sort_desc_single_column_sqlite_parity`: `ORDER BY id DESC LIMIT 5`, assert `id=<n>` first.
  - [x] `sort_topk_no_limit_sqlite`: full `ORDER BY id ASC`, assert all rows in ascending order.
  - [x] `sort_with_filter_and_limit_sqlite`: `WHERE id > 5 ORDER BY id DESC LIMIT 3`, assert correct subset.
  - [x] `sort_explain_contains_order_by`: call `SqliteConnector::explain_query()` with an ORDER BY query, assert the EXPLAIN QUERY PLAN output mentions sort/order.
  - [x] `postgres_sort_asc_parity`, `postgres_sort_desc_parity`, `postgres_medium_sort_smoke` in `qsql-connectors/src/postgres.rs` (env-gated, uses `generate_series`).
  - [x] `mysql_sort_asc_parity`, `mysql_sort_desc_parity`, `mysql_medium_sort_smoke` in `qsql-connectors/src/mysql.rs` (env-gated, uses shuffled VALUES inserts + recursive CTE for medium fixture).

- [x] **3. Medium Fixture Generation (7E)**
  - [x] Create `qsql-workspace/qsql-daemon/tests/common/fixtures.rs` with `generate_medium_csv(path, rows)`, `generate_medium_sqlite(path, table, rows)`, and `unique_temp_path(prefix, ext)` helpers.
  - [x] Register `fixtures` in `qsql-workspace/qsql-daemon/tests/common/mod.rs`.
  - [x] `medium_csv_sort_smoke`: generate 100K-row CSV, `SELECT id FROM t ORDER BY id DESC LIMIT 3`, assert first `id = 100000`.
  - [x] `medium_sqlite_sort_smoke`: same with 100K-row SQLite.

- [x] **4. Sort Explain Visibility (7C)**
  - [x] In `qsql-workspace/qsql-daemon/src/explain.rs`, add `subtree_is_fully_federated()` helper.
  - [x] Add `sort_columns` and `sort_pushed_down` attributes for `LogicalPlan::Sort` nodes in `plan_attributes()`.
  - [x] Add `sort_node_has_sort_columns_and_pushed_down_attributes` test in `explain.rs`.
  - [x] In `qsql-vscode/src/planVisualizationPanel.ts`, add `.node-sort-pushed` CSS class and render "Sort ↓ pushed" badge on Sort nodes where `sort_pushed_down = "true"`.

- [x] **5. Structured Scan-Guard Error Codes (7D)**
  - [x] In `qsql-workspace/qsql-core/src/engine.rs`, prefix `scan_budget_error()` messages with `[QSQL_SCAN_GUARD] `.
  - [x] In `qsql-workspace/qsql-core/src/engine.rs`, update `query_execution_error()` to detect sentinel and map to `QueryError { code: -32100 }`.
  - [x] In `qsql-vscode/src/models.ts`, add `SCAN_GUARD_ERROR_CODE = -32100` and `isScanGuardError()` helper.
  - [x] In `qsql-vscode/src/webviewPanel.ts`, add `updateQueryError()` method and `renderErrorHtml()` export; render actionable suggestion banner for guard errors (Add LIMIT / Add WHERE / Raise budget).
  - [x] In `qsql-vscode/src/extension.ts`, call `updateQueryError()` when caught error has a numeric code.

- [x] **6. TypeScript Guard UX Tests (7G)**
  - [x] `testIsScanGuardErrorHelper`: `isScanGuardError({ code: -32100 })` → `true`; other codes → `false`.
  - [x] `testScanGuardErrorRendersSuggestionBanner`: mock `-32100` error, assert HTML contains "Scan budget exceeded" and "LIMIT".
  - [x] `testGenericExecutionErrorNoSuggestionBanner`: mock `-32603` error, assert suggestion banner absent.
  - [x] `testScanGuardErrorCodeConstant`: assert constant equals -32100.

- [x] **7. Benchmark Additions (7F)**
  - [x] Add `benchmark_sort_pushdown` function to `qsql-workspace/qsql-daemon/benches/phase0_benchmarks.rs`.
  - [x] `sort_pushdown_sqlite_1k_rows`: ORDER BY DESC LIMIT 100 on 1K-row SQLite.
  - [x] `sort_no_pushdown_csv_1k_rows`: same shape on 1K-row CSV (DataFusion in-memory sort).
  - [x] Register both in `criterion_group!`.

- [x] **8. Explain Plan Revamp (7H)** — added in response to a Phase 7 review pass; replaces the structural pattern-matching attribution with evidence-driven stamping.
  - [x] **Backend — physical-plan SQL capture**:
    - [x] Add `provider_kind: Option<String>` + `remote_sql: Option<String>` to `PlanNode` in `qsql-core/src/models.rs` (additive, skip-if-none).
    - [x] Add typed `SourcePlanEntry { provider_kind, native_sql, native_explain, dialect }` and switch `ExplainQueryResult.source_plans` to `HashMap<String, SourcePlanEntry>`.
    - [x] Add `physical_plan_text: Option<String>` to `ExplainQueryResult`.
    - [x] Add `engine::create_physical_plan_for_explain()` to `qsql-core/src/engine.rs`.
    - [x] In `qsql-daemon/src/explain.rs`, add `collect_remote_sql_for_scans()` that handles both `datafusion-federation::VirtualExecutionPlan` (downcast) and any `<Word>Exec sql=…` from `datafusion-table-providers` (`SqlExec`, `MySQLSQLExec`, future DB variants).
    - [x] Match captured SQL strings to logical `TableScan`s via `sql_references_table()` (covers `"schema"."table"` / `` `schema`.`table` `` / bare).
    - [x] Run remote `EXPLAIN` against the captured SQL instead of the placeholder `SELECT * FROM table`.
    - [x] Add `datafusion-federation = "0.5.3"` to `qsql-daemon/Cargo.toml`.
    - [x] Honour `QSQL_EXPLAIN_TRACE=1` env-var for stderr per-node tracing (developer diagnostic; not exposed as a VS Code setting).
  - [x] **Backend — evidence-driven badges**:
    - [x] Sort badge: drive from `remote_sql` content (`ORDER BY` substring scan over every leaf) instead of `subtree_is_fully_federated`. Fixes false positives on multi-source joins.
    - [x] Broadcast badge: drive from `BroadcastRewriteInfo.applied`. Three surfaces per rewrite — remote `TableScan` (`role=remote_scan`), rewritten `Join` (`role=join`), local `TableScan` (`role=local_scan`), plus legacy `filter` for survival cases. Stamp `broadcast_role`, `broadcast_local_table`, `broadcast_remote_table`, `broadcast_elapsed_ms` for hover tooltips.
  - [x] **Frontend — Source tab restructure**:
    - [x] New `qsql-vscode/src/providerIcons.ts` (single source of truth for tree-item icons + plan-graph SVG `<symbol>` set + labels).
    - [x] 9 provider SVGs in `qsql-vscode/media/icons/` (postgres, mysql, mariadb, sqlite, csv, ndjson, json, parquet, fixed-width).
    - [x] Replace `dataSourcesProvider.ts` generic codicons with `treeIconFor()` calls; thread `extensionUri` through `setContext`.
    - [x] `planVisualizationPanel.ts`: inject `<defs>` symbol library at SVG root, render provider glyph on each `TableScan`, replace Source-tab content with per-table cards (Native SQL → Remote EXPLAIN → Logical fragment), collapsible `Federated Logical Plan` (expanded) + `DataFusion Physical Plan` (collapsed) sections, click-on-TableScan → switch-to-Source-tab-and-scrollIntoView, `<title>` hover tooltips, legend bar.
    - [x] Role-aware broadcast badge rendering (`switch (broadcast_role)`) — "Broadcast IN ↓ N keys", "Broadcast ⇆ N keys", "Broadcast keys ↑ N keys", legacy "Broadcast: N keys".
  - [x] **Tests**:
    - [x] 43 unit tests in `explain.rs` covering capture-side (final-SQL marker extraction, SqlExec parser, `MySQLSQLExec` variant, virtual-execution-plan ignore, non-SQL exec ignore, dialect guessing, table-reference matching, scan-name attribution, sort positive/multi-source-negative/local-csv-negative, broadcast remote/local/join attribution, comma-list lookup, no-applications negative).
    - [x] `qsql-core/src/models.rs` serde round-trip: `SourcePlanEntry` and `PlanNode` with `provider_kind` + `remote_sql` (skips when None, preserves when Set).
    - [x] Update `serde_golden_tests.rs` for new payload shape.
    - [x] Update `json_rpc_tests.rs::sqlite_explain_uses_qualified_source_plan_keys` to assert the new `SourcePlanEntry` shape.
    - [x] TS: `providerIcons` registry coverage, label table, symbol-library inlining, per-table-card rendering, graceful empty-state when no remote sources.
  - [x] **Docs**: USER_GUIDE.md §8 walkthrough (Tree icons + per-table cards + click-through), §6 broadcast-section update to mention all three badge surfaces, §5 sort-section correction (single-source only).

## Tests And Acceptance

- [x] `cargo test --locked --workspace` — all existing + 7 new sort parity tests pass (26 daemon integration + 7 sort tests).
- [x] `cargo check --locked -p qsql-daemon --benches` — sort benchmarks compile.
- [x] `npm run typecheck` — TypeScript compiles clean.
- [x] `npm run lint` — no lint errors.
- [x] `npm run test` (scanner + client tests) — all pass including 4 new guard UX tests.
- [x] Acceptance: `ConnectorCapabilities` golden JSON includes `"sort": true`.
- [x] Acceptance: `sort_asc_single_column_sqlite_parity` and `sort_desc_single_column_sqlite_parity` pass — rows arrive in the correct order.
- [x] Acceptance: `sort_explain_contains_order_by` confirms SQLite's EXPLAIN QUERY PLAN output shows sort activity at the DB layer.
- [x] Acceptance: `medium_csv_sort_smoke` and `medium_sqlite_sort_smoke` pass with no committed binary fixtures.
- [x] Acceptance: `isScanGuardError({ code: -32100 })` returns `true`; scan guard HTML renders suggestion banner.

## Defaults (unchanged from Phase 6.2)

- Remote scan guard defaults: 1,000,000 rows and 1 GiB per source scan.
- Schema cache TTL: 5 minutes.
- Source operation timeouts: SQLite 5s, Postgres/MySQL/MariaDB 30s, schema introspection 5s.
- Plan graph node cap: 500 nodes.
- Table discovery page size/cap: 5,000.
- New in Phase 7: `SCAN_GUARD_ERROR_CODE = -32100`.
