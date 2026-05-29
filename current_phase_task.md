# QuiverSQL Phase 10: Rich Explain, Lineage, And Performance Visibility

Phase 9 (Arrow IPC Result Pages) landed in commit `d9478d1`. Phase 10 upgrades lineage from "which tables and columns were touched" to a richer per-query story — output columns with source attribution, alias maps, join conditions, aggregate inputs — and surfaces real DataFusion runtime metrics + full-scan warnings + pushdown reasoning when the user opts into EXPLAIN ANALYZE.

## Decisions Locked In

- **EXPLAIN ANALYZE gating** = scan-guard enforced + explicit `analyze: Option<bool>` opt-in flag. ANALYZE runs through the same `remoteScanMaxRows` / `remoteScanMaxBytes` budget as the regular execution path; failure surfaces the standard `-32100 Scan Budget Exceeded` error with the existing suggestion banner (Phase 7 UX). No new budget knob.
- **Lineage scope** = output-column resolution + alias map + join conditions + aggregate inputs. Walk `Projection` / `Join` / `Aggregate` / `SubqueryAlias` in addition to `TableScan`. Existing `relations` field stays for back-compat; new fields are additive (skip-if-none).
- **VS Code metrics overlay** = default off; toggle in the legend bar; disabled with tooltip when the page is planner-only.
- **CodeLens visibility** = `qsql.explainAnalyzeEnabled` setting (default `false`); the `🔍 Explain (ANALYZE)` lens only appears when the user has explicitly opted in. Keeps the UX from accidentally running expensive queries for users who don't know what ANALYZE means.
- **No new top-level Rust dep**: every API needed (`ExecutionPlan::metrics()`, `Expr::column_refs`) is already in DataFusion 53.1.0.
- **No new top-level TS dep**: lineage tree + metrics overlay reuse VS Code's `TreeDataProvider` and the existing webview SVG/HTML.

## Execution Order

Sub-phases sequenced into waves to maximise parallel work — see the Parallelization Plan section below.

- [ ] **1. QueryLineage model + serde golden (10A)**
  - [ ] Extend `QueryLineage` in `qsql-core/src/engine.rs` with `output_columns: Vec<OutputColumn>`, `joins: Vec<JoinLineage>`, `aggregates: Vec<AggregateLineage>`, `aliases: HashMap<String, String>` (all `skip_serializing_if = empty / none`).
  - [ ] New nested types: `OutputColumn { name, sources: Vec<ColumnRef>, expression_summary }`, `JoinLineage { kind, left_table, right_table, on: Vec<JoinKey> }`, `AggregateLineage { function, alias, inputs: Vec<ColumnRef> }`, `ColumnRef { table, column }`, `JoinKey { left_col, right_col }`.
  - [ ] Mirror in `qsql-vscode/src/lineageProvider.ts` (or `models.ts` if a generic mirror file is preferred — match existing convention).
  - [ ] Update `qsql-core/tests/serde_golden_tests.rs` golden for `QueryLineage` — existing test stays unchanged (new fields skip-if-empty); new `test_query_lineage_golden_with_outputs_and_joins` asserts the populated shape.

- [ ] **2. Engine-side lineage walk (10B)**
  - [ ] Refactor `extract_lineage` in `qsql-core/src/engine.rs:1073-1101` from "match-TableScan-only-and-recurse" into a typed visitor:
    - `LogicalPlan::Projection(proj)` → for each `proj.expr`, derive `(output_name, source_columns, expression_summary)` and push to `output_columns`.
    - `LogicalPlan::Join(j)` → record `JoinLineage` from `j.join_type`, `j.on`, and the recovered left/right table names; recurse left + right.
    - `LogicalPlan::Aggregate(a)` → for each entry in `a.aggr_expr`, derive `(function, alias, input cols)` and push to `aggregates`; walk `a.group_expr` so grouped columns also appear in `output_columns`.
    - `LogicalPlan::SubqueryAlias(s)` → insert `s.alias.table → fully-qualified-subtree-summary` into the `aliases` map; recurse into `s.input`.
  - [ ] Use `Expr::column_refs` (DataFusion built-in) for column attribution per expression.
  - [ ] New `expression_summary(&Expr) -> String` (~50 LOC) for the common shapes — bare column, literal, `BinaryOp`, `ScalarFunction`, `Aggregate`. Anything exotic falls back to `format!("{expr}")` truncated to 120 chars.

- [ ] **3. Lineage tests (10C)**
  - [ ] Rust unit tests in `engine.rs`:
    - Simple projection: `SELECT name, salary FROM employees` → `output_columns.len() == 2`, each entry has a single `ColumnRef`.
    - Aliased column: `SELECT name AS employee_name FROM employees` → `output_columns[0].name == "employee_name"`, `sources[0]` → `employees.name`.
    - Inner join with on-clause: asserts `joins[0].kind == "Inner"`, both tables present, on-clause keys recorded.
    - Aggregate: `SELECT department_id, SUM(salary) AS total FROM employees GROUP BY department_id` → `aggregates[0].function == "SUM"`, inputs include `employees.salary`.
    - CTE / `SubqueryAlias`: `WITH high AS (SELECT … WHERE salary > 100k) SELECT * FROM high` → `aliases` populated, `tables` still contains `employees`.
  - [ ] Daemon integration test (new `qsql-daemon/tests/lineage_tests.rs`) over JSON-RPC: registers quickstart samples, sends `get_lineage` for a multi-source JOIN query, asserts the new fields propagate end-to-end.

- [ ] **4. PlanMetrics runtime fields + serde golden (10D)**
  - [ ] Add `actual_rows: Option<u64>`, `elapsed_compute_ms: Option<u64>`, `mem_used_bytes: Option<u64>` to `PlanMetrics` (`qsql-core/src/models.rs:175-180`), each skip-if-none.
  - [ ] Existing `serde_golden_tests::test_explain_query_models_golden` stays byte-identical via skip-if-none.
  - [ ] New `test_plan_metrics_golden_with_runtime_fields` asserts the populated shape.

- [ ] **5. Daemon ANALYZE dispatch (10E)**
  - [ ] Add `analyze: Option<bool>` to `ExplainQueryRequest` in `qsql-core/src/models.rs`, skip-if-none.
  - [ ] New `engine.execute_physical_plan_collect_metrics(physical_plan, cancellation_token, timeout)` in `qsql-core/src/engine.rs`. Drains the physical plan through `execute_stream()`, discards `RecordBatch`es, records `ExecutionPlan::metrics()` keyed by a stable node id derived from the physical-plan traversal order. Reuses the existing scan-guard envelope so over-budget queries surface `-32100`.
  - [ ] Daemon `explain_query` handler in `qsql-daemon/src/lib.rs:680-820`: when `req.analyze == Some(true)`, call the new method after `create_physical_plan_for_explain` and before `build_plan_graph_with_broadcast`; pass the resulting `metrics_by_node_id` map into the plan-graph builder so per-node `PlanMetrics` gets stamped with `actual_rows` / `elapsed_compute_ms` / `mem_used_bytes`.

- [ ] **6. Full-scan + pushdown_reason attributes (10F)**
  - [ ] In `qsql-daemon/src/explain.rs::plan_attributes` (`TableScan` branch, line 985-1010): after stamping existing `filters` / `limit` attrs, compute `is_full_scan` from the captured `remote_sql` (when present) or the unfiltered/unlimited shape (when local).
  - [ ] New `classify_pushdown_reason(scan, remote_sqls)` → one of `multi_source_join` / `unsupported_expression` / `local_file_scan`. Stamped on `TableScan` and on the `Filter` directly above it.
  - [ ] Webview rendering (`qsql-vscode/src/planVisualizationPanel.ts::nodeLines`): when `is_full_scan === 'true'`, push a `"Full scan ⚠"` line (`cls: 'node-warn'`); when `pushdown_reason` is set, push a small muted info line.
  - [ ] Update the legend bar to explain the new badges.

- [ ] **7. Lineage tree view rewrite (10G)**
  - [ ] `qsql-vscode/src/lineageProvider.ts::buildTree` rewritten to handle the new shape:
    - Root level: `Output Columns (N)`, `Sources (N)`, `Joins (N)`, `Aggregates (N)` — each collapsible.
    - Each `OutputColumn` expands to a list of `ColumnRef` children with the `symbol-field` icon.
    - Each `JoinLineage` shows the join kind + tables, expandable to its on-clause keys.
    - Each `AggregateLineage` shows `function(inputs) AS alias`.
  - [ ] Forward-compat fall-back: when the daemon response lacks the new fields (older daemon binary), keep the existing flat `tables → columns` layout.
  - [ ] Keep the existing cursor-move / edit-debounce refresh triggers in `extension.ts:230-248`.

- [ ] **8. VS Code metrics overlay (10H)**
  - [ ] `qsql-vscode/src/planVisualizationPanel.ts`: add a `Metrics` toggle button to the legend bar.
  - [ ] When toggled on AND any plan node has non-None `actual_rows`, render an extra muted line per node: `actual: 1.2M rows · 3.4ms`.
  - [ ] Toggle is disabled with a hover tooltip when the page is planner-only ("Re-run with Explain (ANALYZE) to see runtime metrics").

- [ ] **9. CodeLens + setting (10I)**
  - [ ] New `qsql.explainAnalyzeEnabled: boolean` (default `false`) in `qsql-vscode/package.json` `contributes.configuration.properties`. Description explains the cost trade-off and points at the scan guard.
  - [ ] `qsql-vscode/src/extension.ts` CodeLens provider: when the setting is on, add a third lens per query block: `🔍 Explain (ANALYZE)` → new command `qsql.explainAnalyze` that calls `daemonClient.explainQuery(sql, { analyze: true })` and opens the plan panel.
  - [ ] `daemonClient.ts::explainQuery`: extend signature to accept `{ analyze?: boolean }` and forward.

- [ ] **10. Tests (10J)**
  - [ ] Rust unit (`engine.rs`): 3 new tests for `execute_physical_plan_collect_metrics` covering happy path, scan-guard refusal, cancellation.
  - [ ] Rust integration (new `qsql-daemon/tests/explain_analyze_tests.rs`, 3 tests): parity-vs-plain-explain (planner output identical), ANALYZE failure under scan guard (assert `-32100`), full-scan attribute stamped on a `SELECT * FROM employees` shape.
  - [ ] TypeScript (`qsql-vscode/src/test/detectQueries.test.ts`, 4 tests):
    - `testLineageTreeFallbackForLegacyShape` — daemon response without the new fields keeps the old layout.
    - `testLineageTreeRendersOutputColumnsSection` — new shape renders the four root sections.
    - `testPlanMetricsOverlayRendersActualRows` — when `actual_rows.is_some()`, the muted line appears.
    - `testExplainAnalyzeCodeLensVisibilityFollowsSetting` — lens absent by default, present after toggling the setting.

- [ ] **11. Documentation (10L)**
  - [ ] `USER_GUIDE.md` — new "Explain (ANALYZE) & Lineage" section walking through the CodeLens, the metrics-overlay toggle, the lineage tree's new sections, and the full-scan badge.
  - [ ] `README.md` — capability matrix gains `EXPLAIN ANALYZE`, `Column-level lineage`, `Full-scan warnings` rows; roadmap Phase 10 Complete.
  - [ ] `CHANGELOG.md` — Phase 10 bullet under the running `0.3.1-alpha.0 - Unreleased` block.
  - [ ] `docs/JSON_RPC_FRAMING.md` — short "EXPLAIN ANALYZE Result Shape" section noting the additive runtime metrics + the scan-guard semantics.
  - [ ] `implementation_plan.md` — Phase 10 status flipped `Current → Complete` on landing.
  - [ ] `current_phase_task.md` — this file; checkboxes flipped as work lands.

- [ ] **12. Final clippy + verification + commit (10M)**
  - [ ] `cargo fmt --all -- --check` clean.
  - [ ] `cargo clippy --locked --workspace --all-targets -- -D warnings` clean.
  - [ ] `cargo test --locked --workspace` — existing green + ≥12 new tests pass.
  - [ ] `cargo check --locked -p qsql-daemon --benches` clean.
  - [ ] `npm run typecheck && npm run lint && npm run test` — existing green + 4 new TS tests pass.
  - [ ] Stage everything except `.claude/settings.local.json`; commit `feat(phase 10): rich explain, lineage, and performance visibility`.

## Parallelization Plan

| Wave | Sub-phases (parallel) | Reason |
|------|------------------------|--------|
| **Wave 1** | 10K · 10A · 10D | Doc-tracker reset independent; 10A is pure-model; 10D is a single skip-if-none field addition on `PlanMetrics`. |
| **Wave 2** | 10B · 10E · 10F | 10B (lineage walk) needs 10A's model; 10E (ANALYZE dispatch) needs 10D's runtime fields; 10F (attributes) is pure `explain.rs`, independent. |
| **Wave 3** | 10C · 10G · 10H · 10I | 10C lineage tests need 10B; 10G TS lineage tree needs 10A/10B model; 10H metrics overlay needs 10D/10E wire; 10I CodeLens + settings is independent. |
| **Wave 4** | 10J (all remaining tests) | Needs everything else wired. |
| **Wave 5** | 10L docs + 10M final verify + commit | Locks in. |

## Tests And Acceptance

- [ ] `cargo fmt --all -- --check` — clean.
- [ ] `cargo clippy --locked --workspace --all-targets -- -D warnings` — clean.
- [ ] `cargo test --locked --workspace` — all existing green + ≥12 new tests pass (5 lineage unit, 3 daemon lineage integration, 3 daemon ANALYZE integration, 3 ANALYZE engine unit, 2 golden).
- [ ] `cargo check --locked -p qsql-daemon --benches` — benches compile.
- [ ] `npm run typecheck && npm run lint && npm run test` — TypeScript clean; 4 new TS tests pass.
- [ ] Acceptance: `get_lineage` over a multi-source JOIN query returns the new `output_columns` / `joins` / `aggregates` / `aliases` fields populated correctly.
- [ ] Acceptance: `explain_query` with `analyze: true` on a quickstart query returns plan-graph nodes whose `metrics.actual_rows` and `metrics.elapsed_compute_ms` are populated.
- [ ] Acceptance: `qsql.remoteScanMaxRows = 2` + `explain_query` with `analyze: true` on a 6-row query → standard `-32100 Scan Budget Exceeded` error.
- [ ] Acceptance: `SELECT * FROM employees` (no WHERE / LIMIT) → plan-graph `TableScan` carries `is_full_scan = "true"`, webview renders the `Full scan ⚠` badge.
- [ ] Acceptance: VS Code Lineage tree shows the four new root sections (Output Columns, Sources, Joins, Aggregates) for the Phase 7 Section 4 federated query.

## Defaults (carried forward from Phase 7 + 8 + 9)

- Remote scan guard defaults: 1,000,000 rows and 1 GiB per source scan.
- Schema cache TTL: 5 minutes.
- Source operation timeouts: SQLite 5s, Postgres/MySQL/MariaDB 30s, schema introspection 5s.
- Plan graph node cap: 500 nodes.
- Table discovery page size/cap: 5,000.
- `SCAN_GUARD_ERROR_CODE = -32100`.
- `FixedWidthExec` default batch size = 8192 rows.
- `qsql.resultFormat` (default `"json"`).
- New in Phase 10: `qsql.explainAnalyzeEnabled` (default `false`); `analyze: Option<bool>` accepted on `explain_query`.
