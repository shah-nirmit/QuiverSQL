# QuiverSQL — Principal Database Engineer Review

**Reviewer perspective:** 20 years across query engines, federation, OLTP, and analytics.
**Scope reviewed:** `implementation_plan.md` (Phases 0–11), `qsql-core`, `qsql-connectors`, `qsql-daemon`, `qsql-vscode`. Phases 0–4 and 6 marked complete; Phase 5 in flight.
**Verdict in one line:** The product vision is sound and the phased plan is unusually sober, but (a) the *core runtime data path* is built on a model (`collect → JSON → cache → slice`) that will not survive Phase 7, and (b) the hand-rolled connector + federation layer duplicates work the DataFusion ecosystem now does better (`datafusion-table-providers`, `datafusion-federation`). Pivot on both before piling more features on top.

---

## 1. Context

QuiverSQL targets a real and underserved niche: an Arrow-native, *local-first* SQL virtualization layer that sits between DuckDB (single-binary file analytics) and Trino/Dremio (clustered federation), delivered as a VS Code experience. The plan picks the right building blocks — DataFusion, `sqlparser`, Arrow, `tokio` — and the phasing is honest about constraints (e.g., "cross-source joins remain in DataFusion until federation-aware planner is ready", `implementation_plan.md:80`).

The implementation faithfully delivers what each completed phase advertised. The concern is not "did you finish Phase 6" — you did. Two concerns:

1. **Runtime foundation.** The execution path under Phases 0–6 (`collect → JSON → cached vector → slice`) makes Phases 7, 9, and 10 substantially harder than the plan acknowledges. Some "Complete" phases ship behaviors the plan didn't promise (silent truncations, partial credential redaction).

2. **Ecosystem position.** Since this plan was drafted, the DataFusion community has shipped `datafusion-table-providers` (9 source TableProviders with upstream pool management, v0.11, more mature than QuiverSQL's hand-rolled 3) and `datafusion-federation` (alpha framework providing the `FederationProvider` / `FederationOptimizerRule` abstractions that implement subplan pushdown). Adopting these collapses the open-ended "federation-aware planner" deferral in `implementation_plan.md:80` from a 6-month project into a 3-week migration. The closest competitor — Spice.ai, also DataFusion-based, Apache 2.0 — has already built a server-side federation engine on top of these primitives. QuiverSQL's moat is not the planner; it's the *VS Code-native, local-first* product experience.

This document gives priority-ranked recommendations and, for each architectural pivot, a migration blueprint sketched concretely enough to start work. The original review concluded "stop pretending cross-source joins scale" — that framing was too dismissive and has been revised: with the ecosystem in hand, cross-source joins are credibly in scope; the work is integration and guards, not greenfield planner.

---

## 2. What is well-designed

Credit where due — these are not the easy parts and they are right:

- **Phased plan with explicit expansion gates** (`implementation_plan.md:75-80`). Refusing to do aggregate/join pushdown before type mapping and parity tests is the discipline most query-engine startups skip and regret.
- **Serde golden tests as a contract** between Rust and TypeScript mirrors. This is the right cross-language drift defense.
- **`sqlparser` for AST extraction** in Phase 6's JIT registration ([qsql-core/src/table_refs.rs](qsql-workspace/qsql-core/src/table_refs.rs)). Walking JOINs, subqueries, and CTEs with `HashSet<(alias, table)>` deduplication is correct.
- **DataFusion `Unparser` for predicate emission** with per-dialect `SqliteDialect`/`PostgreSqlDialect`/`MySqlDialect` ([sql.rs:57-63](qsql-workspace/qsql-connectors/src/sql.rs:57)). This is the right place to delegate dialect concerns.
- **Identifier quoting via `quote_identifier`** with doubled quote char ([sql.rs:300-304](qsql-workspace/qsql-connectors/src/sql.rs:300)). Correct escaping discipline.
- **`CancellationToken` + `tokio::select!`** for query cancellation ([engine.rs:173-191](qsql-workspace/qsql-core/src/engine.rs:173)). Right pattern even if execution can't always be killed mid-batch.
- **VS Code SecretStorage** for credentials, with `<redacted>` in `connection_details` ([daemon/main.rs:395](qsql-workspace/qsql-daemon/src/main.rs:395)). Credentials never persist in the catalog file.
- **Criterion benchmark harness in Phase 0.** Rare and good — it gives you a regression floor.

---

## 3. Critical Design Pivots (P0)

These must change before piling on Phases 7–11. Each one will get harder the longer it waits.

### P0-1. Replace the JSON materialization model with a streaming RecordBatch result handle

**Current state.** [engine.rs:154](qsql-workspace/qsql-core/src/engine.rs:154) does `df.collect().await` (full materialization). [engine.rs:157](qsql-workspace/qsql-core/src/engine.rs:157) immediately converts the full `Vec<RecordBatch>` to `Vec<serde_json::Value>` via an Arrow `JsonWriter` that writes into a buffer, then [engine.rs:104-105](qsql-workspace/qsql-core/src/engine.rs:104) reads the bytes back as `String` and re-parses with `serde_json::from_str`. That `Vec<Value>` then lives in `QuerySession::Completed { result, ... }` ([daemon/main.rs:596-612](qsql-workspace/qsql-daemon/src/main.rs:596)) and gets sliced per page via `result.data[start..end].to_vec()` ([models.rs:209](qsql-workspace/qsql-core/src/models.rs:209)).

**Why it fails.**
1. Memory footprint = `O(rows × cols × avg_value_size)` in `serde_json::Value` form, which is 3–10× the Arrow representation due to box+tag overhead and string-keyed maps per row.
2. JSON-string round-trip (write → bytes → `from_str`) wastes ~2× the time the conversion itself takes and produces garbage to be collected.
3. The "pager" is a slice operation on a frozen vector — paging gives no latency benefit because page 1 of N waits for *all* N to be computed and serialized.
4. Phase 7 ("Stream Arrow batches") and Phase 9 ("Arrow IPC pages") both promise things this representation precludes. You cannot retrofit streaming onto a cached `Vec<Value>` — you have to delete it.
5. `QueryError::query_cancelled` after `df.collect()` started can't actually free the rows already streamed into the collected vec until the entire collect finishes.

**Recommendation.** Hold query state as an open Arrow stream + bounded buffer of materialized-but-unfetched batches, and serialize per-page on demand. This is also a precondition for Arrow IPC pages without further rework.

**Migration blueprint.**

```rust
// qsql-core/src/result_stream.rs (new)
use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::Mutex;

pub struct QueryResultHandle {
    schema: SchemaRef,
    /// Live DataFusion stream. None once exhausted.
    stream: Mutex<Option<SendableRecordBatchStream>>,
    /// Already-pulled-but-not-yet-fetched batches, bounded.
    buffered: Mutex<VecDeque<RecordBatch>>,
    /// Cumulative rows pulled — for backpressure decisions.
    rows_pulled: AtomicU64,
    /// Cumulative bytes pulled (Arrow allocated_size) — for byte guards.
    bytes_pulled: AtomicU64,
    metrics: PerformanceMetrics,
    cancel: CancellationToken,
}

impl QueryResultHandle {
    /// Pull batches until at least `target_rows` are buffered or stream ends.
    async fn fill_to(&self, target_rows: usize) -> Result<(), QueryError> { ... }

    /// Drain up to `page_size` rows from buffered batches, slicing the head batch
    /// rather than copying whole batches around. Returns Arrow + JSON renderings.
    pub async fn next_page(&self, page_size: usize, format: PageFormat)
        -> Result<QueryPage, QueryError> { ... }
}

pub enum PageFormat { Json, ArrowIpcBase64 /* Phase 9 */ }
```

**Migration steps.**
1. Introduce `QueryResultHandle` behind a feature flag. Wire `execute_sql_to_page` to use it for the *first page only*, falling back to the current path. Validate first-page latency improves and equality holds.
2. Replace `QuerySession::Completed { result }` with `QuerySession::Active { handle: Arc<QueryResultHandle> }`. The "completed" state goes away — the stream's exhaustion + last buffered batch drained is the new terminal condition.
3. Delete `record_batches_to_json_rows` (the double-serialize one). Replace with a per-page Arrow→JSON converter that uses `JsonWriter` directly into a `serde_json::Value::Array` constructor — no intermediate `String`.
4. Add `bytes_pulled` budget per query (config: default 256 MiB). On exceed: return structured `QueryError::ResultTooLarge { bytes, limit }` and let the user re-issue with a tighter `LIMIT`. This is the Phase 7 "byte guard" the plan promises.
5. Re-run all paging tests; add a "cancel mid-stream" integration test that asserts memory residency.

**Effort:** ~2 weeks. **Blocks:** Phases 7, 9, 10.

---

### P0-2. Adopt `datafusion-table-providers` and `datafusion-federation` — stop maintaining a parallel connector ecosystem

**Note.** This pivot supersedes my earlier (rejected) "build connection pools yourself" framing and merges with the original P0-4 recommendation. The DataFusion-contrib ecosystem has matured to the point where building bespoke connectors *and* a federation planner is no longer the rational choice; integrating with it is.

**Current state.**
- `qsql-connectors` is ~1,200 lines of hand-rolled `SqliteConnector` / `PostgresConnector` / `MySqlConnector` plus a `SqlTableProvider` (~730 LOC) that re-implements projection/filter/limit pushdown.
- Connection lifecycle is broken: Postgres opens a TCP connection per call ([postgres.rs:31-39](qsql-workspace/qsql-connectors/src/postgres.rs:31)), MySQL constructs *and disconnects* an entire pool per query ([mysql.rs:121-134](qsql-workspace/qsql-connectors/src/mysql.rs:121)), SQLite opens the file per query ([sqlite.rs:97-139](qsql-workspace/qsql-connectors/src/sqlite.rs:97)).
- Schema introspection re-runs per `TableProvider::try_new` with no cache — under Phase 6's JIT registration that's a fresh schema query per first-use of every table, per query.
- Cross-source joins fall back to in-memory hash join in DataFusion ([impl_plan:80](implementation_plan.md:80)), with no subplan pushdown.

**Why it fails.** Two compounding problems. (1) The connector layer is a maintenance burden you didn't have to take on: every type-mapping bug, every dialect quirk, every connection-pool tuning issue is now your problem. (2) The "federation-aware planner" deferral in the plan is open-ended — without one, cross-source joins remain a footgun, but writing one from scratch is genuinely a 6-month rabbit hole.

**The ecosystem now solves both:**

- **`datafusion-table-providers`** (v0.11, [github.com/datafusion-contrib/datafusion-table-providers](https://github.com/datafusion-contrib/datafusion-table-providers)) — pre-built `TableProvider` implementations for PostgreSQL, MySQL, SQLite, DuckDB, ClickHouse, Flight SQL, MongoDB, ADBC, and ODBC. Connection pooling and dialect handling are upstream's problem. Integrates with `datafusion-federation` for subplan pushdown.
- **`datafusion-federation`** ([github.com/datafusion-contrib/datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation)) — *alpha* framework providing `FederationProvider`, `FederatedTableSource`, and `FederationOptimizerRule`. Identifies the largest pushable subplan per source, rewrites the DataFusion plan to delegate it, supports multiple federated sources per query. This is the planner layer Phase 4/5/6 has been deferring.

**Risk.** `datafusion-federation` is alpha. You take a dependency on a moving target. Mitigation: pin to a known-good rev, fork if necessary, contribute upstream rather than diverge. The risk of *not* taking the dependency — open-ended maintenance of a parallel federation planner — is materially worse.

**Recommendation.**

Migrate `qsql-connectors` to be thin adapters that wrap `datafusion-table-providers`' `TableProvider`s, retaining only the QuiverSQL-specific concerns: VS Code SecretStorage integration, redacted catalog responses, the `RemoteConnector` trait facade (for capabilities discovery and the `explain_query` UX). Register `datafusion-federation`'s optimizer on the `SessionContext`. Delete `SqlTableProvider`, `build_select_sql`, and the bespoke `RemoteConnector::execute_query` SQL emission path — they exist to do what the upstream ecosystem now does better.

**Migration blueprint.**

```rust
// qsql-connectors/Cargo.toml
[dependencies]
datafusion-table-providers = { version = "0.11", features = [
    "sqlite", "postgres", "mysql"
    // Phase-gate "duckdb", "clickhouse", "flight-sql", "odbc" behind future phases
] }
datafusion-federation = "<pinned-rev>"

// qsql-connectors/src/lib.rs — keep the trait facade, narrow its responsibilities
pub trait RemoteConnector: Send + Sync {
    fn connector_type(&self) -> &'static str;
    fn capabilities(&self) -> ConnectorCapabilities;  // unchanged
    /// Build a TableProvider backed by upstream `datafusion-table-providers`.
    async fn table_provider(&self, schema: Option<&str>, table: &str)
        -> Result<Arc<dyn TableProvider>, ConnectorError>;
    /// Surfaces source-native explain (used by Phase 5 webview, no replacement upstream).
    async fn explain_query(&self, sql: &str) -> Result<String, ConnectorError>;
    /// Surfaces source table list (5K cap + truncation flag from P1-7).
    async fn list_tables(&self, schema: Option<&str>, limit: usize)
        -> Result<(Vec<String>, bool /* truncated */), ConnectorError>;
}

// qsql-connectors/src/postgres.rs — example post-migration
use datafusion_table_providers::postgres::PostgresTableFactory;

pub struct PostgresConnector {
    factory: Arc<PostgresTableFactory>,  // owns the upstream pool
    secret_ref: String,                  // SecretStorage key, not the DSN itself
}
impl PostgresConnector {
    pub async fn try_new(dsn: SecretString) -> Result<Self, ConnectorError> {
        let factory = PostgresTableFactory::new(dsn.expose_secret()).await?;
        Ok(Self { factory: Arc::new(factory), secret_ref: ... })
    }
}
#[async_trait]
impl RemoteConnector for PostgresConnector {
    async fn table_provider(&self, schema: Option<&str>, table: &str)
        -> Result<Arc<dyn TableProvider>, ConnectorError>
    {
        let table_ref = TableReference::partial(
            schema.unwrap_or("public"), table);
        self.factory.table_provider(table_ref).await
            .map(|p| p as Arc<dyn TableProvider>)
            .map_err(ConnectorError::from)
    }
    // explain_query stays in-house (no upstream equivalent)
    // list_tables stays in-house (catalog discovery is QuiverSQL-specific)
}

// qsql-core/src/engine.rs — register federation optimizer once per session
use datafusion_federation::FederationOptimizerRule;

impl QsqlEngine {
    fn build_session_ctx(&self) -> SessionContext {
        let cfg = self.base_config.clone();
        let state = SessionStateBuilder::new()
            .with_config(cfg)
            .with_runtime_env(self.runtime.clone())  // from P0-3
            .with_optimizer_rule(Arc::new(FederationOptimizerRule::new()))
            .build();
        SessionContext::new_with_state(state)
    }
}
```

**Migration steps.**
1. **Spike (3 days).** Stand up a feature-flagged path: `qsql-connectors-v2` crate with one source (SQLite, since it's simplest). Verify parity against existing tests. Confirm `datafusion-federation`'s optimizer slots in. If the spike fails (e.g., upstream API in flux), the existing connector code keeps shipping.
2. **Migrate SQLite (3 days).** Same-source join pushdown should now work natively (e.g., joining two SQLite tables generates one combined SQL query instead of two scans + DataFusion join). Add a parity bench.
3. **Migrate Postgres + MySQL (1 week).** Reuse the same pattern. Remove `qsql-connectors/src/{sqlite,postgres,mysql}.rs` once parity holds.
4. **Delete dead code (1 day).** `SqlTableProvider`, `build_select_sql`, the `RemoteConnector::execute_query`/`scan` paths. ~700 LOC deleted.
5. **Register federation optimizer in `build_session_ctx`** ([engine.rs](qsql-workspace/qsql-core/src/engine.rs)) — see P0-3. Cross-source joins now get *automatic* subplan pushdown for each side, with the cross-source join happening in DataFusion. Same-source joins push down fully.
6. **Schema cache** still lives in QuiverSQL (upstream may or may not provide caching — verify in the spike). The `(SourceKey, TableName) → SchemaRef` cache from the original P1-8 is still the right pattern, but it now caches around upstream's introspection rather than yours.
7. **Credential flow unchanged.** VS Code SecretStorage → `SecretString` → factory constructor. Never log, never serialize. The connector adapter is the right boundary for redaction.

**What you keep building in-house:**
- The VS Code extension and webview (no equivalent upstream).
- The catalog ([engine.rs:18](qsql-workspace/qsql-core/src/engine.rs:18)) and `<alias>.<table>` namespacing — this is your product surface.
- The Phase 5 source-native explain UX — `datafusion-table-providers` does not expose `EXPLAIN (FORMAT JSON)` results in a structured form; you're the one who needs them for the webview.
- The JIT registration logic in [main.rs:707-805](qsql-workspace/qsql-daemon/src/main.rs:707) — but the registered objects are now upstream `TableProvider`s.
- The credential model (SecretStorage references, never DSNs in catalog).
- Result streaming and paging (P0-1) — this is your product, not a planner concern.

**What you stop maintaining:**
- All driver-level connection management.
- The `SqlTableProvider` pushdown engine.
- The `build_select_sql` builder, including identifier quoting and `Unparser` integration.
- Dialect-specific type mapping for the common types (P1-2 mostly goes away — upstream owns it).
- The fix list P1-1 (NULL coercion), P1-10 (parameterization), P2-2 (`Pool::disconnect()`), P2-4 (SQLite EXPLAIN column hardcoding) all become "no longer applicable" because the code is deleted.

**Effort:** ~3 weeks end-to-end vs ~4–6 weeks to build the equivalent in-house plus indefinite maintenance. **Blocks:** any real-user pilot.

**The judgment call.** If you have strategic reason to own the connector layer — e.g., a closed-source connector for a proprietary source, or a fundamentally different connection model (Arrow Flight-only, say) — keep `qsql-connectors` and apply the original P0-2 pool refactor. For QuiverSQL as positioned (developer tool, local-first, common SQL DBs), there's no such reason. Adopt the ecosystem.

---

### P0-3. Replace process-global mutable `SessionContext` with a per-request session derived from a catalog snapshot

**Current state.** A single `SessionContext` is created once in `QsqlEngine::new()` ([engine.rs:65](qsql-workspace/qsql-core/src/engine.rs:65)) with `SessionContext::new()` (no `RuntimeEnv` config, no memory pool). It is shared across every query in the daemon process and across every concurrent request task. Phase 6's JIT registration mutates this shared context. The catalog HashMap uses `Mutex<HashMap>` ([engine.rs:18](qsql-workspace/qsql-core/src/engine.rs:18)), so readers serialize on writers.

**Why it fails.**
1. **No memory budget.** A wide query can OOM the entire daemon (and every other in-flight query with it).
2. **JIT registration is a shared mutation** — concurrent queries register conflicting `schema.table` providers against the same context. DataFusion tolerates re-registration but the *cleanup* (eviction policy when 5,000+ tables accumulate over a long-running daemon) is undefined.
3. **No isolation.** A bad query touches global planner state; no per-tenant or per-window resource control.
4. **Read-write contention.** `Mutex` instead of `RwLock` for the catalog is a free win you're not taking.

**Recommendation.** Keep one *base* `SessionConfig` + `RuntimeEnv` with a configured memory pool. Build per-request `SessionContext`es from that base, registering only the tables JIT-extracted for *this* query. The catalog HashMap becomes `RwLock<HashMap>`, and registration is pure read on it.

**Migration blueprint.**

```rust
pub struct QsqlEngine {
    base_config: SessionConfig,
    runtime: Arc<RuntimeEnv>,                      // shared memory pool
    catalog:   Arc<RwLock<HashMap<String, CatalogSource>>>,  // RwLock not Mutex
    pools:     Arc<ConnectorPools>,                // from P0-2
    schema_cache: Arc<SchemaCache>,                // from P0-2
}

impl QsqlEngine {
    pub fn build_session_for(&self, sql: &str) -> Result<SessionContext, QueryError> {
        let ctx = SessionContext::new_with_config_rt(self.base_config.clone(), self.runtime.clone());
        // Register only files referenced by `sql` (cheap — files are pure metadata)
        // Register only DB tables referenced by `sql` (P0-2's schema cache hits)
        // Returns a fresh, sql-scoped context.
        Ok(ctx)
    }
}

// RuntimeEnv with a FairSpillPool budget — Phase 7's "memory discipline" lives here.
fn build_runtime(cfg: &EngineConfig) -> Arc<RuntimeEnv> {
    let pool = Arc::new(FairSpillPool::new(cfg.memory_limit_bytes));
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager(DiskManagerConfig::NewSpecified(vec![cfg.spill_dir.clone()]))
        .build_arc().unwrap();
    env
}
```

**Migration steps.**
1. Introduce `EngineConfig { memory_limit_bytes: usize, spill_dir: PathBuf, max_concurrent_queries: usize }`. Default mem to `min(8 GiB, sys_mem / 4)`, max concurrency = 16.
2. Build `RuntimeEnv` once with a `FairSpillPool`. Keep it on `QsqlEngine`.
3. Convert catalog `Mutex` to `RwLock`. Reads (`get_catalog`, `get_source_metadata`) take `.read()`; mutations (`catalog_source`) take `.write()`.
4. Migrate `register_file` and `register_schema_table` to operate against a *passed-in* `SessionContext`, not `self.ctx`.
5. `execute_sql_to_page` builds a fresh context per call, registers the JIT-extracted references against *that* context.
6. Bound concurrent requests via a `tokio::sync::Semaphore` at the daemon layer ([daemon/main.rs:139](qsql-workspace/qsql-daemon/src/main.rs:139)): `let _permit = semaphore.acquire().await;` before spawning. This gives you the backpressure the current `tokio::spawn`-without-limits design lacks.
7. Add a `RuntimeMetrics` exposing `pool.allocated()` over JSON-RPC. The webview can show "memory: 412 MiB / 2 GiB" — and Phase 10's metrics work gets cheaper because you already have the plumbing.

**Effort:** ~2 weeks. **Blocks:** Phase 7, multi-user support, any operational deployment.

---

### P0-4. Defend the federation runway with scan guards and a broadcast-join hint, even after P0-2

**Note.** This is the *belt-and-suspenders* companion to P0-2. `datafusion-federation` will push the largest pushable subplan down to each source, but it cannot prevent a user from writing a query that *forces* a full remote scan ("`SELECT * FROM pg.orders JOIN csv.regions`" — the join column has no useful index on the remote side, the planner correctly extracts a full scan of `orders`, and you OOM). The planner is honest, but honesty is not a defense.

**Current state.** No scan guards anywhere. No row/byte ceilings on remote scans. The combination of "no memory budget" (P0-3) + "no scan ceiling" (here) is what produces the "QuiverSQL just hung my laptop" failure mode.

**Why it still matters after P0-2.** `datafusion-federation` rewrites plans; it does not refuse them. A subplan that reads 50M rows from Postgres into an in-memory join with 200 CSV rows is *correctly federated* — the entire upper join cannot push down, so DataFusion takes both sides. The user experience is identical to the un-federated version.

**Recommendation.** Two complementary guards.

1. **Per-source row/byte budget at scan time.** Default 1M rows and 1 GiB per remote scan, configurable per source. Implemented as a wrapping `TableProvider` adapter around the upstream `datafusion-table-providers` provider, so the guard remains yours even after P0-2.

   ```rust
   // qsql-core/src/scan_guard.rs (new)
   pub struct GuardedTableProvider {
       inner: Arc<dyn TableProvider>,
       limits: ScanLimits,        // { max_rows, max_bytes }
       source_label: String,      // for the error message
       estimator: Arc<dyn RowCountEstimator>,  // via EXPLAIN or pg_class.reltuples
   }
   #[async_trait]
   impl TableProvider for GuardedTableProvider {
       async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>,
           filters: &[Expr], limit: Option<usize>)
           -> Result<Arc<dyn ExecutionPlan>>
       {
           let est = self.estimator.estimate(filters, &self.limits).await?;
           if est.rows > self.limits.max_rows && limit.is_none() {
               return Err(DataFusionError::Plan(format!(
                   "{}: estimated {} rows exceeds budget of {}. Add a LIMIT \
                    or tighten WHERE clause; raise the budget with \
                    `qsql.set-scan-budget {} <N>`.",
                   self.source_label, est.rows, self.limits.max_rows, self.source_label)));
           }
           self.inner.scan(state, projection, filters, limit).await
       }
   }
   ```

2. **Explicit broadcast-join hint.** For the common "join my CSV/Parquet to a filtered remote table" pattern, expose a hint or auto-detect when one side is < N rows AND the other side has a remote source. Materialize the small side, rewrite the join predicate as `WHERE remote_col IN (small_side_values)` and push the rewritten predicate to the remote. This is *complementary* to `datafusion-federation`: federation handles same-source pushdown; broadcast handles cross-source dimensional joins. It's the 80% case for "join local lookup to remote fact" and ~200 LOC.

   ```rust
   // qsql-core/src/broadcast_join.rs (new)
   /// LogicalPlan rewrite: if one side of an Inner equi-join is a finite local
   /// source whose row count <= threshold, replace the join with:
   ///   1) Collect the small side.
   ///   2) Push down `WHERE join_col IN (small_side_values)` to the large side.
   ///   3) Join the collected small side with the filtered large side.
   pub struct BroadcastJoinRule { pub threshold_rows: usize }
   impl OptimizerRule for BroadcastJoinRule { ... }
   ```

   Register *after* `FederationOptimizerRule` so federation has first crack at full pushdown.

**Strategic framing — revised.** My earlier "stop pretending cross-source joins scale" was wrong-headed. The correct framing is: *cross-source joins scale to the extent that one side is small or both sides push down*. `datafusion-federation` handles the second case; broadcast joins handle the first; scan guards handle the failure case. The product can credibly support cross-source joins with these three pieces in place.

**Effort:** ~3 days for `GuardedTableProvider` + estimator (PG: `EXPLAIN`, MySQL: `EXPLAIN FORMAT=JSON`, SQLite: `EXPLAIN QUERY PLAN` + `COUNT(*)` fallback). ~1 week for the broadcast-join rule with golden tests.

**Impact.** Turns the most damaging failure mode (silent OOM on cross-source joins) into a clear actionable error, *and* makes the common cross-source pattern fast — without needing to write a federation planner from scratch.

---

## 4. Required Fixes (P1)

Serious bugs/anti-patterns. Each one is a small change but each is a real correctness or security issue.

### P1-1. Numeric type coercion silently substitutes `0` for NULL

[sql.rs:370](qsql-workspace/qsql-connectors/src/sql.rs:370): `v.as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0)` and the analogous `unwrap_or(0.0)` for Float64. A row with an unparseable integer becomes a *zero* in the Arrow output. This is silent data corruption.

**Fix.** Replace with `match parse { Ok(v) => append_value(v), Err(_) => append_null() }`. This is one line per type, ~5 lines total. Add a golden test: input `{"col": "not-a-number"}` produces `NULL`, not `0`.

### P1-2. SQL→Arrow type mapping collapses every interesting type to `Utf8`

[sql.rs:310-337](qsql-workspace/qsql-connectors/src/sql.rs:310): `sql_type_to_arrow` uses `to_uppercase().contains("…")` and maps everything outside `Bool/Int64/Float64` to `Utf8`. Timestamps, dates, decimals, UUIDs, intervals, JSONB, arrays — all become strings. This breaks the moment a user does `WHERE created_at > NOW() - INTERVAL '7 days'` and the local DataFusion side has `created_at: Utf8` so the comparison either errors or uses lexicographic string ordering.

**Fix.** Per-dialect type maps, not substring matching.

```rust
// qsql-connectors/src/typemap.rs (new)
pub fn pg_type_to_arrow(pg_type: &str) -> DataType { /* exact match table */ }
pub fn mysql_type_to_arrow(mysql_type: &str) -> DataType { ... }
pub fn sqlite_type_to_arrow(sqlite_type: &str) -> DataType { ... }
```

Cover at minimum: `BOOLEAN`, `SMALLINT/INT2`, `INTEGER/INT4`, `BIGINT/INT8`, `REAL/FLOAT4`, `DOUBLE PRECISION/FLOAT8`, `NUMERIC/DECIMAL(p,s) → Decimal128`, `DATE → Date32`, `TIME → Time64(Micro)`, `TIMESTAMP/TIMESTAMPTZ → Timestamp(Micro, tz)`, `UUID → FixedSizeBinary(16)` or `Utf8`, `JSON/JSONB → Utf8`, `BYTEA/BLOB → Binary`. Add a golden parity test per type per dialect.

### P1-3. TOCTOU race in catalog ↔ JIT registration

[daemon/main.rs:719-739](qsql-workspace/qsql-daemon/src/main.rs:719): `ensure_database_table_registered` locks `state.database_sources`, finds the registration, *releases the lock*, then proceeds to construct a connector with the now-detached `registration.connection_string`. A concurrent `remove_source` ([daemon/main.rs:224](qsql-workspace/qsql-daemon/src/main.rs:224)) can complete in between — the in-flight query continues using credentials the user just revoked.

**Fix.** Wrap `DatabaseRegistration` in `Arc`, so the lookup returns `Arc<DatabaseRegistration>` and the in-flight reference outlives the catalog removal. Add a generation counter in the catalog so the engine can refuse stale registrations on retry:

```rust
struct DatabaseRegistration {
    generation: u64,           // bumped on every catalog mutation for this alias
    // ...
}
```

After this lands, P0-2's `pools.evict()` should also be tied to generation — a query holding generation `N` can still finish, but the next attempt with generation `N+1` gets a fresh pool.

### P1-4. Credential redaction only fires on parse-phase errors

[daemon/main.rs:526-529](qsql-workspace/qsql-daemon/src/main.rs:526): the regex `(?i)(password|pwd|secret)=[^\s;]+` is applied to the parse-error path only. Execution errors (`connection refused: postgres://user:Pa$$w0rd@host`) bypass it.

**Fix.** A `RedactError` helper applied at the JSON-RPC response boundary, *not* per error site:

```rust
fn redact(s: &str) -> String {
    // 1) password/pwd/secret=...
    // 2) full DSN: postgres://user:pwd@host  →  postgres://user:***@host
    // 3) mysql://user:pwd@host  →  mysql://user:***@host
}
```

Wire it into the `JsonRpcError::data` and `JsonRpcError::message` writers so it cannot be skipped.

### P1-5. No timeouts on any source-DB call

No `tokio::time::timeout` wraps `execute_query`, `explain_query`, `list_tables`, or `introspect_*_schema`. A hung Postgres replica blocks the request task forever; with P0-3's task semaphore that's one slot permanently consumed.

**Fix.** Per-source-kind default timeout (Postgres/MySQL = 30s, SQLite = 5s, schema introspection = 5s), overridable via the `query_start` request. Wrap every connector call:

```rust
match tokio::time::timeout(self.timeout, conn.query(sql, &[])).await {
    Ok(Ok(rows)) => Ok(rows),
    Ok(Err(e))   => Err(ConnectorError::Source(e)),
    Err(_)       => Err(ConnectorError::Timeout { source: kind, after: self.timeout }),
}
```

### P1-6. Phase 5 "node-count truncation" is documented but unimplemented

[implementation_plan.md:89](implementation_plan.md:89) promises "node-count truncation, raw-text byte caps, and clear truncation warnings". Only the 50KB raw-text cap exists. A plan with 10K nodes will lock the webview's SVG rendering. Add a `MAX_PLAN_NODES = 500` constant; truncate the tree at that depth and emit a typed warning matching the plan's contract.

### P1-7. Silent truncation at 5,000 tables

`TABLE_LIST_LIMIT = 5_000` ([daemon/main.rs:22](qsql-workspace/qsql-daemon/src/main.rs:22)) silently drops table 5,001+. Enterprise Postgres schemas commonly have more. The fix is two parts:

1. Have `.list_tables()` return `(Vec<String>, bool /* truncated */)` and surface a warning in the registration response.
2. Make the tree-view lazy (request next page of tables on expand) so the 5K cap becomes a UX choice, not a hard limit. This is also the right shape for the *eventual* "schema search" feature in Phase 10.

### P1-8. Per-query schema-introspection re-runs

Even without P0-2's full pool refactor, the cheapest immediate win: cache `(SourceKey, TableName) → SchemaRef` in `daemon::DaemonState` with TTL 5 min. Phase 6 turns every cold reference into an introspection query *per query*; caching cuts this to once per 5 min per table. ~50 lines.

### P1-9. Daemon JSON-RPC framing is line-delimited and unbuffered

[daemon/main.rs:130-163](qsql-workspace/qsql-daemon/src/main.rs:130) reads via `stdin.lock().lines()` and writes via `writeln!` + `flush`. Two problems:

1. A `\n` in a stringified payload (unlikely in serde-emitted JSON, but possible in user-emitted error messages) breaks framing.
2. Large pages are one giant `writeln!` line — no streaming, no incremental flush.

**Fix.** Adopt LSP-style framing: `Content-Length: <n>\r\n\r\n<bytes>`. Same wire format LSP uses; trivial to implement, robust, and *required* before Phase 9's Arrow IPC base64 (long lines kill pipes on some platforms — Windows in particular).

### P1-10. Filter rendering escapes via Unparser only; constants still embedded into SQL

[postgres.rs:85-93](qsql-workspace/qsql-connectors/src/postgres.rs:85): `client.query(sql, &[])` — no parameter binding. The entire defense rests on `Unparser::expr_to_sql` correctly escaping literals. It does, today, but you're betting your security model on a transitive library's correctness with no defense-in-depth.

**Fix.** Parameterize. Rewrite filter rendering to emit `$1, $2, …` (Postgres) / `?, ?, …` (MySQL/SQLite) placeholders and a parallel `Vec<ScalarValue>` to bind via the driver's typed parameter API. Defer to Phase 7 if scope-tight, but track it explicitly.

---

## 5. Quick Wins (P2)

Each is < 1 day, no architecture change.

- **P2-1.** Cache the `Unparser` per dialect ([sql.rs:484](qsql-workspace/qsql-connectors/src/sql.rs:484)) — `OnceLock<Unparser<'static>>` per dialect kind.
- **P2-2.** `Pool::disconnect()`-per-query in [mysql.rs:131-134](qsql-workspace/qsql-connectors/src/mysql.rs:131): delete those lines. Pool drop is idempotent; you don't need to actively disconnect. This alone will 10× MySQL throughput.
- **P2-3.** Replace `Mutex<HashMap>` with `RwLock<HashMap>` for the catalog ([engine.rs:18](qsql-workspace/qsql-core/src/engine.rs:18)) and `database_sources` ([daemon/main.rs:81](qsql-workspace/qsql-daemon/src/main.rs:81)). Net change ~10 lines.
- **P2-4.** SQLite EXPLAIN column index hardcoding ([sqlite.rs:58-64](qsql-workspace/qsql-connectors/src/sqlite.rs:58)): use column-name lookup. PRAGMA output shape is documented but not contractual.
- **P2-5.** Add structured error enum `ConnectorError` (alongside the existing `String`) at least at the trait boundary — `Result<T, ConnectorError>` where `ConnectorError = { Connect, Timeout, Auth, Sql, Network, Other(String) }`. The error message stringification stays the same for callers, but downstream code can branch on the kind.
- **P2-6.** Dedup table refs in `extract_database_table_refs` *before* iterating — already done in the function (`HashSet`), but the outer caller in [daemon/main.rs:707-712](qsql-workspace/qsql-daemon/src/main.rs:707) loops over the returned `Vec`; if any path bypasses the HashSet, you get O(n²) lookups. Verify with a test on a pathological self-joining query.

---

## 6. Where the implementation_plan.md and code diverge

Cross-checking what's marked "Complete" against what was actually delivered:

| Plan promise | Status | Note |
|---|---|---|
| Phase 5: "node-count truncation" ([impl_plan:89](implementation_plan.md:89)) | ❌ Missing | Only raw-text byte cap exists. See P1-6. |
| Phase 5: "credential-redacted warnings/details" ([impl_plan:86](implementation_plan.md:86)) | ⚠ Partial | Parse path only. See P1-4. |
| Phase 6: "list up to 5,000 table names" ([impl_plan:101](implementation_plan.md:101)) | ⚠ Silent | Truncation not surfaced to user. See P1-7. |
| Phase 3: "metadata-cache invalidation tests" ([impl_plan:56](implementation_plan.md:56)) | ❌ No cache | There's no metadata cache to invalidate. Phase 3 marked complete but the test name implies a cache exists. |
| Phase 7: "Stream Arrow batches through result conversion" | ❌ Blocked | Current model (P0-1) precludes this without rewrite. |
| Phase 9: "Arrow IPC for large/requested result pages" | ❌ Blocked | Same — current result lifecycle is `Vec<serde_json::Value>`, not `RecordBatch`. |
| Phase 1: "structured JSON-RPC errors" | ⚠ Partial | Errors are typed at the boundary but flattened to `String` inside the connector layer (P2-5). |
| Phase 4: "SQL emission hooks" with capabilities | ✅ Delivered | Genuinely well done. |
| Phase 6: "AST table-reference extraction" | ✅ Delivered | Correctly handles CTEs, JOINs, subqueries. |

The pattern: **completion is generous on plumbing, sparse on the safety/discipline items** (truncation warnings, redaction completeness, metadata caches). These were never adversarial code — they're the items you skip when shipping and the items that bite you in production.

---

## 7. Recommended phase re-prioritization

The plan's Phase 7 is "Large Local Data, Memory Discipline, And Sort Pushdown" — but memory discipline cannot be added to the current execution model without the P0-1/P0-3 pivots first. P0-2 (ecosystem adoption) is the biggest single accelerant of all subsequent phases.

**Suggested re-shuffle:**

- **Phase 6.5 (new) — Ecosystem adoption, ~3 weeks.** P0-2 (`datafusion-table-providers` + `datafusion-federation`). Net effect: ~700 LOC deleted from `qsql-connectors`, 6 new sources unlocked for free, same-source join pushdown gained automatically, federation planner unblocked. **Do this first** — every subsequent phase becomes cheaper because there's less hand-rolled code in the way.
- **Phase 6.6 (new) — Runtime discipline, ~2 weeks.** P0-1 (streaming result handle) + P0-3 (per-request session + `RuntimeEnv` memory pool). This *is* "memory discipline"; it cannot be tacked on later. Note that P0-3's `with_optimizer_rule(FederationOptimizerRule)` integration depends on 6.5 landing first.
- **Phase 6.7 (new) — Safety surface, ~1 week.** P0-4 scan guards + broadcast-join rule; P1-3 (catalog TOCTOU), P1-4 (full credential redaction), P1-5 (timeouts), P1-6 (plan node-count truncation), P1-7 (table-list truncation surfaced), P1-9 (Content-Length JSON-RPC framing). Most of P1-1, P1-2, P1-10, P2-2, P2-4 become *deleted* (the code they applied to is gone in 6.5).
- **Phase 7 (revised):** Sort/top-k pushdown — but check whether upstream already covers it; if so, this phase is mostly tests + UI surfacing. Byte/row limit error surfaces (already wired in 6.6 + 6.7).
- **Phase 8:** Fixed-width support (unchanged). Broadcast-join hint moves earlier to 6.7.
- **Phase 9:** Arrow IPC pages — trivial because P0-1 gave you a `SendableRecordBatchStream`-shaped result handle.
- **Phase 10:** Rich explain + lineage with runtime metrics — runtime metrics come for free from P0-3's `RuntimeEnv`. Source-native explain stays in-house (no upstream equivalent for the structured webview UX).
- **Phase 11:** Packaging unchanged.

**Why this ordering.** Phase 6.5 (ecosystem adoption) maximally reduces the surface area you have to fix in 6.6 and 6.7. If you do 6.6/6.7 *before* 6.5, you fix bugs in code you're about to delete. The order is: shrink the codebase first, then harden what remains.

The phases that *look* slower (6.5–6.7) are the only phases where you pay down debt instead of adding to it. Every phase after 6.7 gets cheaper *and* delivers a more honest product.

---

## 8. Strategic / product framing

Four pieces of non-code feedback, because as a Principal you take them.

1. **Know the competitive landscape — Spice.ai is the closest neighbor.** [github.com/spiceai/spiceai](https://github.com/spiceai/spiceai) is also DataFusion-based, Apache 2.0, ships 30+ connectors with advanced pushdown, has materialization/acceleration engines (Cayenne/Vortex/Arrow/DuckDB/SQLite/Postgres), and uses a cluster-sidecar deployment model with Arrow Flight as the data plane. It is *not* a VS Code extension and not local-first in the QuiverSQL sense — it's a server. **That gap is QuiverSQL's moat.** Don't try to out-server Spice; out-IDE them. Tight VS Code integration, single-binary daemon, no cluster, no sidecar, instant feedback loop — those are the things Spice cannot match without abandoning its architecture, and they're the things QuiverSQL is uniquely positioned to ship.

2. **The competitive moat is "files + SQL DBs from VS Code, fast, local."** Federation is now a *table-stakes* feature you get largely for free by adopting `datafusion-federation` + `datafusion-table-providers` (see P0-2). The differentiator is the developer experience — query authoring, paged results, lineage, visual explain, source replay, secret storage, all in the IDE — not the planner. Treat the planner as a dependency, not a product.

3. **The honest demo is single-source pushdown + same-source join pushdown + small-side broadcast joins to remote facts.** With P0-2 (federation adoption) + P0-4 (scan guards + broadcast rule), all three are within scope for an honest demo. Build the world-class version of *that* in VS Code, and you have a tool a working data engineer will keep in their `code` shortcut bar.

4. **Phase 10's `EXPLAIN ANALYZE` deferral is correct; double-check the temptation to backport it.** Once you have `RuntimeEnv` metrics from P0-3, you'll be tempted to expose them as if they're `EXPLAIN ANALYZE`. They're not — they're per-context runtime stats, which is *more* informative for the local-first use case anyway. Name them honestly ("memory pressure", "spill bytes", "rows pulled from `<source>`") rather than aping a Postgres-shaped concept.

5. **If you do want to own a federation planner long-term, contribute upstream rather than fork.** `datafusion-federation` is alpha and accepts contributions with commit access on merged PRs. The path to "owning" federation is to *become a maintainer* of the upstream crate — you get to shape the API to your needs without the maintenance cost of a parallel implementation. This is also the position that lets QuiverSQL diverge cleanly later (e.g., a QuiverSQL-specific planner extension that lives in `qsql-core` but builds on the upstream traits).

---

## 9. Critical files

Files modified, created, and *deleted* by the P0/P1 work above. Listing so future-you knows the blast radius:

**Created:**
- [qsql-workspace/qsql-core/src/result_stream.rs](qsql-workspace/qsql-core/src/result_stream.rs) — P0-1, streaming `QueryResultHandle`.
- [qsql-workspace/qsql-core/src/scan_guard.rs](qsql-workspace/qsql-core/src/scan_guard.rs) — P0-4, `GuardedTableProvider` wrapper.
- [qsql-workspace/qsql-core/src/broadcast_join.rs](qsql-workspace/qsql-core/src/broadcast_join.rs) — P0-4, broadcast-join `OptimizerRule`.

**Modified:**
- [qsql-workspace/qsql-core/src/engine.rs](qsql-workspace/qsql-core/src/engine.rs) — P0-1 (streaming handle), P0-3 (per-request session, `RuntimeEnv` memory pool, `RwLock` catalog, register `FederationOptimizerRule`).
- [qsql-workspace/qsql-connectors/src/lib.rs](qsql-workspace/qsql-connectors/src/lib.rs) — narrow `RemoteConnector` to `table_provider` / `explain_query` / `list_tables`.
- [qsql-workspace/qsql-connectors/src/{sqlite,postgres,mysql}.rs](qsql-workspace/qsql-connectors/src/) — rewrite as thin adapters over upstream `*TableFactory`s. Preserve `explain_query` and `list_tables` (no upstream equivalent for the Phase 5 UX).
- [qsql-workspace/qsql-connectors/Cargo.toml](qsql-workspace/qsql-connectors/Cargo.toml) — add `datafusion-table-providers`, `datafusion-federation` (pinned rev).
- [qsql-workspace/qsql-daemon/src/main.rs](qsql-workspace/qsql-daemon/src/main.rs) — P0-3 (semaphore), P1-3 (Arc<DatabaseRegistration> + generation), P1-4 (boundary-level redaction), P1-7 (truncation flag), P1-9 (Content-Length framing).
- [qsql-workspace/qsql-daemon/src/explain.rs](qsql-workspace/qsql-daemon/src/explain.rs) — P1-5 (timeouts), P1-6 (node-count truncation).
- [qsql-vscode/src/planVisualizationPanel.ts](qsql-vscode/src/planVisualizationPanel.ts) — P1-6 (consume node-count truncation warning).
- [implementation_plan.md](implementation_plan.md) — phase re-shuffle per §7.

**Deleted (after P0-2 lands):**
- `qsql-workspace/qsql-connectors/src/sql.rs` — `SqlTableProvider` and `build_select_sql`. ~730 LOC. Pushdown is now upstream's responsibility.
- The bulk of `qsql-workspace/qsql-connectors/src/{sqlite,postgres,mysql}.rs` `execute_query` / `scan` paths. ~600 LOC across the three files.

Net: roughly **+800 LOC of pivots, −1,300 LOC of replaced code** = ~500 LOC smaller, with 6 additional source types unlocked.

---

## 10. Verification

End-to-end gates the new work should pass, in order:

1. **Cache parity.** After P0-2's schema cache: running the same federated query twice should issue exactly *one* introspection query per source-table pair, not two. Verify with a test connector that counts calls.

2. **Streaming first-page latency.** After P0-1: a query returning 1M rows should serve page 1 (1K rows) in < 200 ms over warm cache. Currently it waits for full materialization. Add a Criterion bench.

3. **Memory budget honored.** After P0-3: set `memory_limit_bytes = 256 MiB`, run a query that would naturally need 2 GiB, expect `QueryError::ResourceExhausted` rather than OOM kill. Use the existing Criterion harness — add a "memory pressure" suite.

4. **Cancellation under load.** Spawn 32 concurrent queries (after P0-3's semaphore lands), cancel half mid-stream, verify daemon RSS returns to baseline within 5s.

5. **No credential leaks.** Fuzz the daemon's error responses with malformed DSNs containing real-looking passwords; grep response payloads for the password literal. Should never appear (P1-4).

6. **MySQL throughput.** Microbench: 1000 sequential `execute_query` calls. Currently ~1 connection setup each = limited by handshake. After P2-2 + P0-2: should be 10–50× faster.

7. **Phase 5 truncation contract.** Generate a synthetic 10K-node plan, verify the explain response includes `truncated: true` and the webview displays the warning (P1-6).

8. **Existing test suites stay green.** `cargo test --locked --workspace`, `cargo test --locked --workspace --features postgres,mysql`, `npm run test`, `npm run typecheck`.

9. **Benchmarks regress no more than 5%** on the existing Phase 0 suite. Track via the CI artifact already planned in Phase 11.

---

## TL;DR

- **Strengths:** sober phased plan, correct foundations (DataFusion + sqlparser + Arrow), good identifier-quoting and cancellation primitives, real benchmark discipline, correct AST extraction in Phase 6.
- **Critical pivots (do in this order, before Phase 7):**
  1. **Adopt the ecosystem.** Migrate `qsql-connectors` to `datafusion-table-providers`; register `datafusion-federation`'s optimizer on `SessionContext`. Deletes ~700 LOC, unlocks 6 sources, gains same-source join pushdown for free.
  2. **Kill the JSON materialization model.** Replace `df.collect() → Vec<serde_json::Value> → slice` with a streaming `QueryResultHandle` over `SendableRecordBatchStream`. Precondition for Phases 7 and 9.
  3. **Per-request `SessionContext` derived from a base with `RuntimeEnv` memory pool.** `RwLock` the catalog. Bounded task semaphore at the daemon.
  4. **Belt-and-suspenders:** scan guards + broadcast-join rewrite for the cross-source-join footgun. Federation is now in the box; honesty about *what scales* still has to be built.
- **Required fixes (mostly absorbed by the pivots):** silent zero-on-parse-failure (deleted by P0-2), Utf8-collapse type mapping (mostly deleted by P0-2), catalog TOCTOU (P1-3), partial credential redaction (P1-4), no timeouts anywhere (P1-5), missing node-count truncation (P1-6), silent 5K-table truncation (P1-7), line-delimited JSON-RPC (P1-9), unparameterized constants in pushdown SQL (deleted by P0-2).
- **Quick wins (most also absorbed):** `RwLock` swap (P2-3), Phase 6.5/6.7 fixes around the surface that survives.
- **Plan ↔ code:** several "Complete" phases skip the safety/discipline items they promised. Phase 7's "memory discipline" cannot be added to the current execution model without the P0 pivots first.
- **Strategic:** the moat is "files + SQL DBs, local, in VS Code, fast" — the IDE integration, *not* the planner. Spice.ai already won the "DataFusion-based federation server" race; don't try to play their game. Take the federation crate as a dependency; out-build them in the IDE.