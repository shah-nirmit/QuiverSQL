# QuiverSQL Phase 9: Arrow IPC Result Pages

Phase 8 (Fixed-Width File Support) landed in commit `307ef69`. Phase 9 extends the paged result-delivery path so callers can opt into base64-encoded Arrow IPC streams instead of the verbose `Vec<serde_json::Value>` payload. JSON stays the default; IPC is a transparent transport optimisation that preserves Arrow types end-to-end (no more `i64 → f64` precision loss, no lossy decimal/timestamp coercion, faster encode + smaller payload for big pages).

## Decisions Locked In

- **End-to-end IPC in VS Code, type-aware grid**. Adds `apache-arrow` (~500KB) on the TypeScript side; typed Arrow Vectors flow through to the cell renderer for full fidelity (int64, decimal, timestamp, null preserved through the wire).
- **Single-knob opt-in**: new `qsql.resultFormat: 'json' | 'arrow_ipc'` VS Code setting, default `'json'`. No per-request override surface — keeps the API clean and the VS Code UX consistent.
- **Base64-over-stdio for v1**. Locked in by `implementation_plan.md`'s Key Technical Decisions section. TCP / gRPC alternatives stay deferred to Phase 11.
- **Additive wire shape**. `QueryPage.data` (JSON array) stays unchanged; new optional `data_ipc: Option<String>` carries the base64 IPC blob; new optional `result_format: Option<String>` echoes the chosen format. Both skip-if-none — existing JSON clients stay byte-identical on the wire.
- **No new top-level Rust dep**: `arrow-ipc` 58.3.0 is already a transitive of DataFusion 53.1.0; `base64` v0.22.1 is transitive via postgres-protocol.

## Execution Order

Sub-phases sequenced into waves to maximise parallel work — see the Parallelization Plan section below.

- [x] **1. Daemon IPC serializer + format negotiation (9A)**
  - [x] Add `result_format: Option<String>` to `QueryStartRequest` and `QueryPageRequest` (`qsql-core/src/models.rs`) with `#[serde(default, skip_serializing_if = "Option::is_none")]`. Document accepted values: `"json"` (default) and `"arrow_ipc"`.
  - [x] Add `serialize_batches_to_ipc_base64(batches, start, len, schema) -> Result<String, String>` next to `record_batches_to_json_rows` in `qsql-core/src/engine.rs` (or split into `qsql-core/src/result_ipc.rs` if it grows past ~80 LOC). Uses `arrow::ipc::writer::StreamWriter<Vec<u8>>` (reachable via `datafusion::arrow::ipc::writer`); slices buffered batches by row range; base64-encodes the finalised IPC buffer.
  - [x] Extend `QueryResultHandle::page(...)` signature with `result_format: Option<&str>`. Branch:
      - `None | Some("json")` → existing `record_batches_to_json_rows` path.
      - `Some("arrow_ipc")` → `serialize_batches_to_ipc_base64` path.
      - Anything else → structured `QueryError { code: -32602, message: "Invalid result_format: …" }`.
  - [x] Daemon `query_start` / `query_page` handler arms in `qsql-daemon/src/lib.rs` thread `result_format` from the request through to `handle.page(...)`. Persist on `QuerySession::Streaming` so subsequent `query_page` calls reuse the same format without re-passing it.

- [x] **2. QueryPage model widening + TS mirror (9B)**
  - [x] Add `data_ipc: Option<String>` and `result_format: Option<String>` to `QueryPage` in `qsql-core/src/models.rs`, both skip-if-none.
  - [x] Update the inline page assembly in `QueryResultHandle::page` (`qsql-core/src/engine.rs` lines ~307-322) to populate the right field based on `result_format`. Mutual-exclusion invariant: when IPC mode is on, `data` is empty (`Vec::new()`) and `data_ipc` is `Some(...)`; when JSON mode is on, the inverse.
  - [x] Mirror in `qsql-vscode/src/models.ts` — extend the `QueryPage` interface with `data_ipc?: string` and `result_format?: string`.
  - [x] Update the existing `serde_golden_tests.rs` `QueryPage` case to assert the new optional fields stay skipped when None.

- [x] **3. VS Code type-aware grid (9C)**
  - [x] Add `apache-arrow` to `qsql-vscode/package.json` dependencies (pin to v17.x — compatible with the Node 18 target).
  - [x] New module `qsql-vscode/src/resultPage.ts` with:
      - `RenderablePage` discriminated union: `{ kind: 'json', schema, rows, ... } | { kind: 'arrow', schema, table, ... }` (carries query_id, page_index, page_size, is_last, metrics, warning).
      - `decodeResultPage(raw: QueryPage): RenderablePage` — when `result_format === 'arrow_ipc'`, base64-decode `data_ipc` (`Buffer.from(s, 'base64')`) and `tableFromIPC(bytes)` from apache-arrow; otherwise pass through as `kind: 'json'`.
  - [x] Refactor `formatCellValue` (`qsql-vscode/src/webviewPanel.ts`) to take an optional `dataType?: arrow.DataType | string`:
      - `arrow.DataType.Int64` (or `'int64'` from JSON schema) → render as exact string (avoids JS Number precision loss).
      - `arrow.DataType.Decimal*` → pass through (already string-typed in apache-arrow).
      - `arrow.DataType.Timestamp` / `Date32` / `Date64` → ISO 8601 string.
      - `arrow.DataType.Bool` → `"true"` / `"false"`.
      - Null / undefined → `<span class="cell-null">(null)</span>`.
      - Everything else → existing fallback (`String(value)` + HTML-escape).
    When called without `dataType`, behaviour is unchanged (JSON path stays byte-identical).
  - [x] Refactor `renderQueryPageHtml` to take a `RenderablePage` (back-compat: still accepts a raw `QueryPage` and decodes lazily so existing tests stay valid). Row iteration branches on `page.kind`:
      - `arrow`: column-major access — `for i in 0..table.numRows { fields.forEach((f, ci) => formatCellValue(table.getChildAt(ci).get(i), table.schema.fields[ci].type)) }`.
      - `json`: unchanged row-iteration path.
  - [x] `daemonClient.ts` — read `vscode.workspace.getConfiguration('qsql').get<string>('resultFormat', 'json')` once at `query_start` time and thread through. Subsequent `query_page` calls reuse the daemon-side session state (no need to re-pass).

- [x] **4. Settings registration (9D)**
  - [x] Add `qsql.resultFormat` to `qsql-vscode/package.json` `contributes.configuration.properties` with `enum: ["json", "arrow_ipc"]`, default `"json"`, description explaining it's a transport opt-in for large pages.

- [x] **5. Rust unit tests (9E)**
  - [x] `serialize_batches_to_ipc_base64` round-trip: encode → base64-decode → `arrow::ipc::reader::StreamReader` → assert schema + every cell value matches the input batches.
  - [x] Slice correctness: feed two 100-row batches, request rows 50..150; assert decoded IPC has exactly 100 rows in expected order.
  - [x] Empty range: `start == end` produces a valid empty IPC stream (schema-only, zero batches).
  - [x] Result-format validation: `QueryResultHandle::page(..., Some("totally_unknown"))` returns a structured invalid-params error.
  - [x] Mutual-exclusion invariant: `result_format="arrow_ipc"` → `QueryPage.data` empty + `data_ipc.is_some()`; `result_format="json"` → inverse.

- [x] **6. Daemon integration tests (9F)**
  - [x] New `qsql-workspace/qsql-daemon/tests/arrow_ipc_tests.rs`:
      - Register `employees.csv`, run `query_start` with `result_format: "arrow_ipc"`, decode the page's IPC payload via `StreamReader`, assert row count + first/last row values match a parallel `result_format: "json"` run.
      - Multi-page test: 250 synthesised rows / 100-row pages over arrow_ipc; assert pages 0 and 1 carry IPC bytes and page 2 has `is_last == true`.
      - Confirm `quickstart_samples_tests` keeps passing (JSON path unchanged — no `result_format` field on the request).

- [x] **7. TypeScript tests (9G)**
  - [x] `qsql-vscode/src/test/detectQueries.test.ts` adds:
      - `testResultPageDecodeJsonPassThrough` — `QueryPage` without `data_ipc` passes through to `{ kind: 'json', rows }`.
      - `testResultPageDecodeArrowIpcRoundTrip` — synthesise an Arrow table in-memory via `apache-arrow`, write to IPC, base64-encode, feed through `decodeResultPage`, assert column count + schema.
      - `testFormatCellValueInt64PreservesPrecision` — `9_007_199_254_740_993n` renders as the exact string.
      - `testFormatCellValueTimestampRendersIso` — `Timestamp(UTC)` → ISO 8601.
      - `testFormatCellValueNullRendersMuted` — null cell carries `class="cell-null"`.
      - `testRenderQueryPageHtmlArrowMode` — Arrow mode produces same column headers + type-aware cells.

- [x] **8. Benchmark (9H)**
  - [x] New groups in `qsql-workspace/qsql-daemon/benches/phase0_benchmarks.rs`:
      - `query_page_serialize_to_json_100k_rows` — baseline.
      - `query_page_serialize_to_ipc_base64_100k_rows` — measure encode time + payload byte size.
  - [x] No CI gate; just a regression watch.

- [x] **9. Documentation (9I)**
  - [x] `docs/JSON_RPC_FRAMING.md` — new "Binary Result Format" section describing `result_format`, the `data_ipc` base64-encoded Arrow IPC stream, and the mutual-exclusion invariant.
  - [x] `USER_GUIDE.md` — short Settings note pointing users at `qsql.resultFormat` for big-result perf; transparent to query authoring.
  - [x] `README.md` — capability matrix new "Arrow IPC result pages" row Supported; roadmap Phase 9 Complete; intro paragraph updated.
  - [x] `CHANGELOG.md` — Phase 9 bullet under the running `0.3.1-alpha.0 - Unreleased` block.
  - [x] `implementation_plan.md` — Phase 9 section status flipped `Current → Complete` on landing.
  - [x] `current_phase_task.md` — this file; checkboxes flipped as work lands.

- [x] **10. Final clippy + verification + commit (9J)**
  - [x] `cargo fmt --all -- --check` clean.
  - [x] `cargo clippy --locked --workspace --all-targets -- -D warnings` clean.
  - [x] `cargo test --locked --workspace` — all existing green + ≥10 new IPC tests pass.
  - [x] `cargo check --locked -p qsql-daemon --benches` clean.
  - [x] `npm run typecheck && npm run lint && npm run test` — existing tests green + 6 new Arrow tests pass.
  - [x] Stage everything except `.claude/settings.local.json`; commit with a Phase-9 message in the same style as the Phase 8 commit (`feat(phase 9): arrow IPC result pages with type-aware VS Code grid`).

## Parallelization Plan

| Wave | Sub-phases (parallel) | Reason |
|------|------------------------|--------|
| **Wave 1** | 9A · 9B · 9D | All independent: 9A is pure-core + daemon-handler signature, 9B only widens the wire model, 9D is a single package.json key. |
| **Wave 2** | 9C (TS decoder + type-aware grid) | Needs 9B's wire shape + 9D's setting key. |
| **Wave 3** | 9E · 9F · 9H | Need 9A's serializer; daemon-integration test (9F) also needs the model widening from 9B. |
| **Wave 4** | 9G (TS tests) | Needs 9C in place. |
| **Wave 5** | 9I docs + final clippy + commit (9J) | Locks everything in. |

## Tests And Acceptance

- [x] `cargo fmt --all -- --check` — clean.
- [x] `cargo clippy --locked --workspace --all-targets -- -D warnings` — clean.
- [x] `cargo test --locked --workspace` — all existing green + ≥10 new IPC tests pass (5 unit + 3 integration + 2 golden).
- [x] `cargo check --locked -p qsql-daemon --benches` — IPC benches compile.
- [x] `npm run typecheck && npm run lint && npm run test` — TypeScript clean; 6 new Arrow tests pass.
- [x] Acceptance: `query_start` with `result_format: "arrow_ipc"` on a 100-row table returns a `QueryPage` where `data == []`, `data_ipc.is_some()`, and decoding the IPC yields the same rows the JSON path returns.
- [x] Acceptance: `qsql.resultFormat = "arrow_ipc"` in VS Code settings makes the result grid render `SELECT 9007199254740993 AS big_id` as the exact string (no JS Number precision loss).
- [x] Acceptance: Default-setting smoke run shows existing JSON grid behaviour byte-identical.
- [x] Acceptance: An unknown `result_format` (e.g. `"avro"`) returns a structured `-32602 Invalid params` error.

## Defaults (carried forward from Phase 7 + 8)

- Remote scan guard defaults: 1,000,000 rows and 1 GiB per source scan (unchanged; not applied to file providers).
- Schema cache TTL: 5 minutes.
- Source operation timeouts: SQLite 5s, Postgres/MySQL/MariaDB 30s, schema introspection 5s.
- Plan graph node cap: 500 nodes.
- Table discovery page size/cap: 5,000.
- `SCAN_GUARD_ERROR_CODE = -32100`.
- `FixedWidthExec` default batch size = 8192 rows (matches DataFusion's default).
- New in Phase 9: `qsql.resultFormat` (default `"json"`), `result_format` accepted values `"json"` | `"arrow_ipc"`.
