# QuiverSQL Phase 8: Fixed-Width File Support

Phase 7 (Sort/Top-K Pushdown + Scan-Guard UX + Explain Plan revamp) is complete and amended into commit `17c7054`. Phase 8 turns the `SourceKind::FixedWidth` enum variant (already in place at every layer — Rust enum, TypeScript mirror, sidebar icon, explain-plan provider mapping) into a working source: layout-file driven schema, streaming `ExecutionPlan`, VS Code two-file wizard branch, parity tests against the CSV equivalent, benchmark, docs.

## Decisions Locked In

- **Layout format**: JSON (no new crate dep, matches existing serde infrastructure).
- **Execution strategy**: Streaming `ExecutionPlan` from day one (not `MemTable`) — matches CSV/Parquet behaviour, bounds memory.
- **Clippy fixes** from previous turn (qsql-connectors `items_after_test_module`, qsql-daemon `clone_on_copy`) rolled into Phase 8's first commit.

## Execution Order

Sub-phases sequenced into waves to maximise parallel work — see the Parallelization Plan section below.

- [x] **1. Layout file + Arrow schema (8A)**
  - [x] New module `qsql-workspace/qsql-core/src/fixed_width.rs` with `FixedWidthLayout { fields: Vec<FixedWidthField> }` + serde derives.
  - [x] `FixedWidthField { name, start, length, type, nullable, trim }` matching the layout JSON shape.
  - [x] `FixedWidthLayout::from_json_path(path)` and `from_json_str(s)` returning `Result<Self, String>`.
  - [x] `FixedWidthLayout::arrow_schema() -> SchemaRef` — builds Arrow `Schema` field-by-field.
  - [x] Validation: overlapping spans rejected, zero/negative `length` rejected, unknown `type` rejected with the offending type name in the error, empty `fields` rejected.
  - [x] Move `sql_type_to_arrow` from `qsql-connectors/src/sql.rs:99-200` into a new `qsql-core/src/sql_types.rs` module (re-exported by `qsql-connectors` to avoid breaking callers) so both crates can reuse the type-string-to-Arrow mapping without a cross-crate cycle.

- [x] **2. Streaming TableProvider + ExecutionPlan (8B)**
  - [x] `FixedWidthTableProvider { layout, path }` impl `TableProvider` (in `fixed_width.rs`).
  - [x] `supports_filters_pushdown` returns `Unsupported` for every filter in v1 (DataFusion wraps the scan in `FilterExec` instead).
  - [x] `scan(projection, _filters, limit)` constructs and returns `Arc<FixedWidthExec>`.
  - [x] `FixedWidthExec` impl `ExecutionPlan`: `Partitioning::UnknownPartitioning(1)`, `Boundedness::Bounded`, `EmissionType::Incremental`.
  - [x] `DisplayAs::fmt_as` prints `FixedWidthExec path=… rows_read=…`.
  - [x] `FixedWidthRowStream` impl `RecordBatchStream + Stream<Item = Result<RecordBatch>>`: `BufReader<File>`, batch size 8192, byte-offset slicing, projection applied during column building (skip non-projected fields), limit honoured as a row counter so we never read past it.
  - [x] Type coercion via Arrow array builders (`Int64Builder`, `StringBuilder`, `Float64Builder`, etc.); parse errors include row index + column name + offending slice in the `DataFusionError::External` message.

- [x] **3. Daemon `register_file` payload extension (8C)**
  - [x] Extend `RegisterFileRequest` in `qsql-workspace/qsql-daemon/src/lib.rs:59` with `options: Option<HashMap<String, serde_json::Value>>` and `#[serde(default, skip_serializing_if = "Option::is_none")]` so existing CSV/JSON/Parquet calls stay byte-identical on the wire.
  - [x] Extend `QsqlEngine::register_file` in `qsql-workspace/qsql-core/src/engine.rs:683` to take an `options` argument; CSV/JSON/Parquet arms ignore it.
  - [x] New `"fixed_width"` arm in the format dispatch: read `options["layout_path"]`, load `FixedWidthLayout`, build `FixedWidthTableProvider`, call `register_table_entry` (not wrapped in `GuardedTableProvider` — consistent with other local file providers; add a one-line comment explaining the choice).
  - [x] Mirror `options` in TypeScript: extend the `RegisterFileRequest` shape in `qsql-vscode/src/models.ts` and the `daemonClient.ts` call.

- [x] **4. VS Code wizard branch (8D)**
  - [x] Add `Fixed-width File` entry to the `qsql.connectWizard` quickpick in `qsql-vscode/src/extension.ts:478-486`.
  - [x] New branch after the existing file-type block at lines 578-625:
    1. `vscode.window.showOpenDialog` for the data file (filters `txt`, `dat`, `fwf`, `*`).
    2. `vscode.window.showOpenDialog` for the layout file (filter `json`).
    3. `vscode.window.showInputBox` for alias (default = data file stem).
    4. `daemonClient.sendRequest('register_file', { table_name, path, format: 'fixed_width', options: { layout_path } })`.
  - [x] Layout-missing, layout-unreadable, and bad-JSON errors → `vscode.window.showErrorMessage` + bail, matching the SQLite picker.

- [x] **5. SourceProfile persistence + replay (8E)**
  - [x] Extend `PersistentSourceProfile.details` in `qsql-vscode/src/sourceManager.ts:4-14` with optional `layoutPath?: string`.
  - [x] New `fixed_width` arm in `replaySources()` (`sourceManager.ts:84-132`) resending the same payload (`options.layout_path` carrying `layoutPath`).
  - [x] Wizard persistence at the end of the new branch writes both `path` and `layoutPath` into the profile.

- [x] **6. Sample fixtures (8F)**
  - [x] `samples/quickstart/employees_fwf.txt` — 50 rows mirroring `employees.csv` so daemon integration tests can assert row-set parity.
  - [x] `samples/quickstart/employees_fwf.layout.json` — field spans matching the data file.
  - [x] Extend `qsql-workspace/qsql-connectors/examples/generate_quickstart_samples.rs` to emit both files from the existing in-memory source rows.

- [x] **7. Tests (8G)**
  - [x] **Layout unit tests** (`fixed_width.rs`): JSON round-trip; overlapping-spans rejection; zero/negative-length rejection; unknown-type rejection; type-mapping coverage (INTEGER → Int64, BIGINT → Int64, REAL → Float32, DOUBLE → Float64, VARCHAR/TEXT → Utf8, BOOLEAN → Boolean, DATE → Date32, TIMESTAMP → Timestamp).
  - [x] **Stream unit tests** (`fixed_width.rs`): tiny layout + `std::io::Cursor` over bytes → expected batches; projection trims columns; limit truncates; UTF-8 multi-byte content (assertion that we slice on bytes not chars and reject mid-codepoint splits); ragged-row → error with row index.
  - [x] **Daemon integration test** (new `qsql-workspace/qsql-daemon/tests/fixed_width_tests.rs`): JSON-RPC `register_file` with `format: "fixed_width"`, then `execute` confirms row-set parity vs the CSV equivalent under the same `ORDER BY id`.
  - [x] **Medium fixture smoke** (Phase 7E pattern): add `generate_medium_fwf(path, rows)` to `qsql-workspace/qsql-daemon/tests/common/fixtures.rs`; generate 100K rows at test time; `SELECT id FROM t ORDER BY id DESC LIMIT 3`; first `id == 100_000`.
  - [x] **VS Code tests** (`qsql-vscode/src/test/detectQueries.test.ts`): new test asserting the wizard's `register_file` payload carries `format: 'fixed_width'` and `options.layout_path`. The provider-icon registry already covers `fixed_width`.

- [x] **8. Benchmark (8H)**
  - [x] Add `fixed_width_file_scan_1k_rows` (full scan) and `fixed_width_file_scan_to_json` groups to `qsql-workspace/qsql-daemon/benches/phase0_benchmarks.rs`, mirroring the CSV pattern at lines 49-100.

- [x] **9. Documentation (8I)**
  - [x] `USER_GUIDE.md` — new Section 2.C "Attaching a Fixed-Width File" with example layout JSON, sample command, and smoke query.
  - [x] `README.md` — capability matrix moves `Fixed-width files` row Planned → Supported; intro paragraph updated.
  - [x] `CHANGELOG.md` — extend the running `0.3.1-alpha.0 - Unreleased` block with a Phase 8 bullet (unless explicit version bump is requested).
  - [x] `implementation_plan.md` — Phase 8 status flipped `Current → Complete` on landing.
  - [x] `current_phase_task.md` — this file; checkboxes flipped as work lands.

- [x] **10. Clippy + housekeeping (8J)**
  - [x] Stage the existing clippy fixes from the previous turn: `qsql-connectors/src/lib.rs` (move `mod tests` to end of file) and `qsql-daemon/src/lib.rs:1563` (drop `.clone()`).
  - [x] Re-run `cargo clippy --locked --workspace --all-targets -- -D warnings` after each wave; the gate stays green throughout Phase 8.

## Parallelization Plan

| Wave | Sub-phases (parallel) | Reason |
|------|------------------------|--------|
| **Wave 1** | 8A · 8C · 8F · 8J | All independent: 8A is pure-core with no callers yet, 8C only touches handler signatures, 8F is data authoring, 8J is two small lint fixes. |
| **Wave 2** | 8B · 8D | 8B needs 8A's `FixedWidthLayout`; 8D needs 8C's new `options` field. |
| **Wave 3** | 8E · 8G unit (layout + stream) · 8H | 8E needs 8D; unit tests need 8A + 8B; benchmark needs 8B. |
| **Wave 4** | 8G daemon-integration + VS Code tests | Needs every other piece wired end-to-end. |
| **Wave 5** | 8I docs + final `cargo clippy` + amend | Locks everything in. |

## Tests And Acceptance

- [x] `cargo fmt --all -- --check` — clean.
- [x] `cargo clippy --locked --workspace --all-targets -- -D warnings` — clean.
- [x] `cargo test --locked --workspace` — all existing + ≥10 new fixed-width tests pass (layout + stream unit, daemon integration, medium-fixture smoke).
- [x] `cargo check --locked -p qsql-daemon --benches` — fixed-width benchmarks compile.
- [x] `npm run typecheck && npm run lint && npm run test` — TypeScript compiles clean, new wizard-payload test passes.
- [x] Acceptance: a `register_file` call with `format: "fixed_width"` + `options.layout_path` registers a queryable table whose rows match the CSV equivalent of the same data under `ORDER BY id`.
- [x] Acceptance: an Explain on the fixed-width table shows `provider_kind: "fixed_width"` and renders the fixed-width glyph on the `TableScan` node.
- [x] Acceptance: Restarting the Extension Host replays the persisted fixed-width source with no re-prompt.
- [x] Acceptance: An overlapping-spans layout, a zero-length field, and an unknown type each produce a descriptive `register_file` error (not a panic).

## Defaults (carried forward from Phase 7)

- Remote scan guard defaults: 1,000,000 rows and 1 GiB per source scan (unchanged; not applied to file providers).
- Schema cache TTL: 5 minutes.
- Source operation timeouts: SQLite 5s, Postgres/MySQL/MariaDB 30s, schema introspection 5s.
- Plan graph node cap: 500 nodes.
- Table discovery page size/cap: 5,000.
- `SCAN_GUARD_ERROR_CODE = -32100`.
- New in Phase 8: `FixedWidthExec` default batch size = 8192 rows (matches DataFusion's default).
