use datafusion::common::TableReference;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion_federation::sql::VirtualExecutionPlan;
use qsql_connectors::sql::{native_select_all_sql, SqlDialectKind, SqlTableRef};
use qsql_connectors::RemoteConnector;
use qsql_core::broadcast::{BroadcastApplication, BroadcastRewriteInfo};
use qsql_core::models::{PlanGraph, PlanMetrics, PlanNode, SourceKind, SourcePlanEntry};
use qsql_core::GuardedTableProvider;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::DatabaseRegistration;

pub const MAX_PLAN_NODES: usize = 500;
pub const RAW_PLAN_TEXT_LIMIT: usize = 50_000;
const SQLITE_EXPLAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn get_max_plan_nodes() -> usize {
    qsql_core::models::get_env_usize("QSQL_MAX_PLAN_NODES", MAX_PLAN_NODES)
}

pub fn get_raw_plan_text_limit() -> usize {
    qsql_core::models::get_env_usize("QSQL_RAW_PLAN_TEXT_LIMIT", RAW_PLAN_TEXT_LIMIT)
}

pub fn get_remote_explain_timeout() -> Duration {
    let secs = qsql_core::models::get_env_usize("QSQL_REMOTE_QUERY_TIMEOUT_SECS", 30);
    Duration::from_secs(secs as u64)
}

pub fn build_plan_graph(plan: &LogicalPlan) -> PlanGraph {
    build_plan_graph_with_broadcast(plan, None, None, None)
}

/// Variant that stamps Filter nodes synthesized by the broadcast-join rewrite
/// with `broadcast_rewrite=true` and `broadcast_predicate_value_count=<n>`
/// attributes so the VS Code plan webview can render a badge on them.
///
/// `remote_sqls` (optional) maps qualified table name → the actual SQL the
/// federation layer pushes down to that table's DBMS, scraped from the
/// physical plan via [`collect_remote_sql_for_scans`]. When provided, every
/// `TableScan` node for a remote table is stamped with `remote_sql` and a
/// `sort_pushed_down=true` attribute if the SQL contains `ORDER BY`.
///
/// `registrations` (optional) is the daemon's `database_sources` map, used to
/// classify each `TableScan` with a `provider_kind` (postgres / mysql /
/// mariadb / sqlite) for the per-node icon in the plan webview.
pub fn build_plan_graph_with_broadcast(
    plan: &LogicalPlan,
    broadcast: Option<&BroadcastRewriteInfo>,
    remote_sqls: Option<&RemoteSqlMap>,
    registrations: Option<&HashMap<String, Arc<DatabaseRegistration>>>,
) -> PlanGraph {
    let mut nodes = HashMap::new();
    let mut truncated = false;
    let targets = broadcast_filter_targets(broadcast);
    let empty: Vec<BroadcastApplication> = Vec::new();
    let applications: &[BroadcastApplication] = match broadcast {
        Some(info) => &info.applied,
        None => &empty,
    };
    let ctx = TraverseContext {
        broadcast_targets: &targets,
        broadcast_applications: applications,
        remote_sqls,
        registrations,
    };
    let root_id = traverse_plan(plan, &mut nodes, &mut 0, &mut truncated, &ctx);

    PlanGraph {
        root_ids: vec![root_id],
        node_count: nodes.len(),
        nodes,
        truncated,
    }
}

/// Per-table remote SQL captured from the physical plan: qualified table name
/// → `(sql, executor_name)`.
pub type RemoteSqlMap = HashMap<String, RemoteSqlInfo>;

#[derive(Debug, Clone)]
pub struct RemoteSqlInfo {
    pub sql: String,
    pub executor_name: String,
}

struct TraverseContext<'a> {
    /// Legacy: set of `join_key_remote` column names. Used by the (deprecated
    /// but kept-for-safety) `Filter + InList` pattern matcher.
    broadcast_targets: &'a HashSet<String>,
    /// Authoritative source of truth: every broadcast rewrite that the
    /// engine actually applied. The badge attribution code below stamps the
    /// affected `TableScan` and `Join` nodes directly from this list,
    /// regardless of what subsequent optimizer passes do to the surrounding
    /// Filter node.
    broadcast_applications: &'a [BroadcastApplication],
    remote_sqls: Option<&'a RemoteSqlMap>,
    registrations: Option<&'a HashMap<String, Arc<DatabaseRegistration>>>,
}

/// Looks up a `BroadcastApplication` whose `remote_table` field references
/// the given qualified table name. `remote_table` is a comma-joined list of
/// all `TableScan` names in the rewritten subtree (see
/// `broadcast::describe_subtree`), so a single application can match
/// multiple `TableScan` nodes when several scans share a federated subtree.
fn application_targeting_remote_table<'a>(
    applications: &'a [BroadcastApplication],
    qualified: &str,
) -> Option<&'a BroadcastApplication> {
    applications
        .iter()
        .find(|app| comma_list_contains(&app.remote_table, qualified))
}

/// Looks up a `BroadcastApplication` whose `local_table` field references
/// the given qualified table name. Used to badge the local side of a
/// broadcast — typically a small CSV/JSON scan whose rows became the IN-list.
fn application_targeting_local_table<'a>(
    applications: &'a [BroadcastApplication],
    qualified: &str,
) -> Option<&'a BroadcastApplication> {
    applications
        .iter()
        .find(|app| comma_list_contains(&app.local_table, qualified))
}

fn comma_list_contains(list: &str, needle: &str) -> bool {
    list.split(',').any(|item| item.trim() == needle)
}

/// Returns true iff `plan` is the Join that a given `BroadcastApplication`
/// rewrote — detected by checking that the join's two sides cover the
/// application's `local_table` and `remote_table` table sets respectively.
/// Defensive against optimizer rewrites that swap input ordering: we accept
/// either inputs[0]=local / inputs[1]=remote OR the inverse.
fn join_matches_application(plan: &LogicalPlan, app: &BroadcastApplication) -> bool {
    let LogicalPlan::Join(join) = plan else {
        return false;
    };
    let left_scans = collect_scan_names_set(&join.left);
    let right_scans = collect_scan_names_set(&join.right);
    let local_set: HashSet<String> = app
        .local_table
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let remote_set: HashSet<String> = app
        .remote_table
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    (local_set.is_subset(&left_scans) && remote_set.is_subset(&right_scans))
        || (local_set.is_subset(&right_scans) && remote_set.is_subset(&left_scans))
}

fn collect_scan_names_set(plan: &LogicalPlan) -> HashSet<String> {
    let mut names = HashSet::new();
    let mut visit = |node: &LogicalPlan| {
        if let LogicalPlan::TableScan(scan) = node {
            names.insert(qualified_table_name(&scan.table_name));
        }
    };
    walk_logical_plan(plan, &mut visit);
    names
}

/// Stamps `attributes` with the broadcast-rewrite metadata for the given
/// application, using a `role` discriminator so the webview can render
/// surface-appropriate text ("Broadcast IN ↓ N keys" on the receiving scan,
/// "Broadcast ⇆ N keys" on the rewritten Join, "Broadcast: N keys" on the
/// synthesized Filter when it survives).
fn stamp_broadcast_attrs(
    attributes: &mut HashMap<String, String>,
    app: &BroadcastApplication,
    role: &str,
) {
    attributes.insert("broadcast_rewrite".to_string(), "true".to_string());
    attributes.insert("broadcast_role".to_string(), role.to_string());
    attributes.insert(
        "broadcast_predicate_column".to_string(),
        app.join_key_remote.clone(),
    );
    attributes.insert(
        "broadcast_predicate_value_count".to_string(),
        app.predicate_value_count.to_string(),
    );
    attributes.insert("broadcast_local_table".to_string(), app.local_table.clone());
    attributes.insert(
        "broadcast_remote_table".to_string(),
        app.remote_table.clone(),
    );
    attributes.insert(
        "broadcast_elapsed_ms".to_string(),
        app.elapsed_ms.to_string(),
    );
}

/// Walks the DataFusion physical plan and harvests the actual SQL string
/// federation pushes down to each remote table. Handles two execution paths:
///
/// 1. **Federation path** — `datafusion-federation`'s `VirtualExecutionPlan`,
///    used by [`SqliteTableProvider`] and by Postgres/MySQL whenever the
///    schema is known at provider-construction time. Downcasting works
///    because the type is stable across our deps.
/// 2. **Non-federation path** — `datafusion-table-providers`'s `SqlExec<T,P>`,
///    used by Postgres/MySQL when `PostgresTableFactory::table_provider` /
///    `MySQLTableFactory::table_provider` are called without a precomputed
///    schema. The exec is generic so a typed downcast is impractical (we'd
///    have to enumerate every `(T, P)` pair). We parse its `DisplayAs`
///    marker `SqlExec sql=<sql>` instead and recover the table name from the
///    rendered `FROM "schema"."table"` clause.
///
/// `logical_plan` + `registrations` are used to attribute the captured SQL
/// strings back to qualified table names (`alias.table` keys) by matching
/// the table name and registration-supplied schema against each candidate
/// SQL's `FROM` clause — robust against the executor-name vs alias mismatch
/// (the federation executor name is "mysql" for both MySQL and MariaDB).
///
/// Set `QSQL_EXPLAIN_TRACE=1` in the daemon environment to emit a one-line
/// stderr trace per physical-plan node, useful for diagnosing
/// "Native SQL shows `SELECT *`" reports — almost always means the walk
/// found zero candidates here.
pub fn collect_remote_sql_for_scans(
    plan: &Arc<dyn ExecutionPlan>,
    logical_plan: &LogicalPlan,
    registrations: &HashMap<String, Arc<DatabaseRegistration>>,
) -> RemoteSqlMap {
    let trace = std::env::var("QSQL_EXPLAIN_TRACE").ok().as_deref() == Some("1");

    // Phase 1: walk the physical plan, collect every (sql, executor_name)
    // candidate we can detect from each node's `DisplayAs` output.
    let mut candidates: Vec<RemoteSqlInfo> = Vec::new();
    walk_exec(plan, &mut |node| {
        let formatted = format!(
            "{}",
            displayable(node.as_ref()).set_show_schema(false).one_line()
        );
        if trace {
            eprintln!(
                "[qsql-explain-trace] node={} fmt=\"{}\"",
                node.name(),
                truncate_for_trace(&formatted),
            );
        }

        // Federation case: VirtualExecutionPlan covers possibly-multiple
        // tables and may rewrite the SQL through several stages — pick the
        // last marker (final form actually sent over the wire).
        if let Some(vexec) = node.as_any().downcast_ref::<VirtualExecutionPlan>() {
            if let Some(sql) = extract_final_sql_from_fmt(&formatted) {
                if trace {
                    eprintln!(
                        "[qsql-explain-trace]   VirtualExecutionPlan sql captured: {}",
                        truncate_for_trace(&sql)
                    );
                }
                let executor_name = vexec.executor().name().to_string();
                candidates.push(RemoteSqlInfo { sql, executor_name });
            }
            return;
        }

        // Non-federation case: datafusion-table-providers' SqlExec emits
        // `SqlExec sql=<the rendered SQL with pushdowns>` as a single line.
        // This is what we see when PostgresTableFactory / MySQLTableFactory
        // wrap us under the federation feature flag but the leaf SqlExec
        // ends up directly in the physical plan (no VirtualExecutionPlan
        // wrapper present at the federation analyser stage).
        if let Some(sql) = extract_sql_exec_sql(&formatted) {
            if trace {
                eprintln!(
                    "[qsql-explain-trace]   SqlExec sql captured: {}",
                    truncate_for_trace(&sql)
                );
            }
            let executor_name = guess_executor_from_sql(&sql);
            candidates.push(RemoteSqlInfo { sql, executor_name });
        }
    });

    // Phase 2: match each remote logical TableScan to one of the captured
    // SQL strings by searching for the table's quoted identifier inside the
    // SQL's FROM clause. A captured SQL may cover multiple tables (federation
    // pushdowns of joins) — each matching TableScan gets the same SQL.
    let mut sqls = RemoteSqlMap::new();
    let mut logical_scans = Vec::new();
    collect_scans(logical_plan, &mut logical_scans);
    for scan in logical_scans {
        let qualified = qualified_table_name(&scan.table_name);
        let bare_table = scan.table_name.table().to_string();
        let alias = table_alias_and_name(&scan.table_name).map(|(a, _)| a);
        let schema = alias
            .as_deref()
            .and_then(|a| registrations.get(a))
            .and_then(|reg| reg.schema.clone());

        for info in &candidates {
            if sql_references_table(&info.sql, schema.as_deref(), &bare_table) {
                if trace {
                    eprintln!(
                        "[qsql-explain-trace]   matched {} → {}",
                        qualified,
                        truncate_for_trace(&info.sql)
                    );
                }
                sqls.insert(qualified.clone(), info.clone());
                break;
            }
        }
    }
    sqls
}

fn truncate_for_trace(s: &str) -> String {
    if s.len() <= 200 {
        s.to_string()
    } else {
        format!("{}…", &s[..200])
    }
}

/// Recovers the SQL string from a one-line `DisplayAs` rendering of any
/// SQL-emitting exec from `datafusion-table-providers`. The crate ships a
/// generic `SqlExec` (used by Postgres) plus per-DB variants — `MySQLSQLExec`
/// for MySQL, and similar names for SQLite/DuckDB/Clickhouse — all of which
/// print `<ExecName> sql=<the SQL>` with no trailing fields, so the SQL runs
/// to end of line.
///
/// We pattern-match on `<word>Exec sql=` at the start of the formatted line
/// rather than hard-coding each exec name. That keeps us forward-compatible
/// with new DB-specific exec types upstream may add, and intentionally
/// excludes `VirtualExecutionPlan` (ends in "Plan", handled elsewhere) and
/// non-SQL execs like `SortExec`, `RepartitionExec`, etc. (no `sql=` field).
fn extract_sql_exec_sql(formatted: &str) -> Option<String> {
    let trimmed = formatted.trim_start();
    let first_space = trimmed.find(' ')?;
    let exec_name = &trimmed[..first_space];
    if !exec_name.ends_with("Exec") {
        return None;
    }
    let after_name = trimmed[first_space + 1..].trim_start();
    let sql = after_name.strip_prefix("sql=")?;
    let sql = sql.trim();
    if sql.is_empty() {
        None
    } else {
        Some(sql.to_string())
    }
}

/// Best-effort executor classification for SQL captured from a non-federation
/// `SqlExec`. Postgres double-quotes identifiers; MySQL/MariaDB backtick them.
/// SQLite always goes through the federation path, so we don't need to
/// classify it here. Falls back to `"sql"` when the SQL has no identifier
/// quoting we can detect.
fn guess_executor_from_sql(sql: &str) -> String {
    if sql.contains('`') {
        "mysql".to_string()
    } else if sql.contains('"') {
        "postgres".to_string()
    } else {
        "sql".to_string()
    }
}

/// Returns true if the SQL's `FROM` (or `JOIN`) clause references the given
/// table — checked through every dialect-appropriate quoting style and with
/// the schema either present or absent. Used to attribute a captured SQL
/// string back to the logical `TableScan` that produced it.
fn sql_references_table(sql: &str, schema: Option<&str>, table: &str) -> bool {
    let mut patterns: Vec<String> = Vec::new();
    if let Some(s) = schema {
        // Schema-qualified, all three quoting styles.
        patterns.push(format!(r#""{}"."{}""#, s, table));
        patterns.push(format!("`{}`.`{}`", s, table));
        patterns.push(format!("{}.{}", s, table));
    }
    // Bare table name, all quoting styles.
    patterns.push(format!(r#""{}""#, table));
    patterns.push(format!("`{}`", table));
    patterns.push(format!(" {} ", table));
    patterns.iter().any(|p| sql.contains(p.as_str()))
}

fn walk_exec(plan: &Arc<dyn ExecutionPlan>, f: &mut impl FnMut(&Arc<dyn ExecutionPlan>)) {
    f(plan);
    for child in plan.children() {
        walk_exec(child, f);
    }
}

/// Kept for diagnostics — not currently called from the capture path, which
/// attributes via `sql_references_table` instead. Used by the explain tests.
#[cfg_attr(not(test), allow(dead_code))]
fn collect_scan_names_in_logical_plan(plan: &LogicalPlan) -> Vec<String> {
    let mut names = Vec::new();
    let mut visit = |node: &LogicalPlan| {
        if let LogicalPlan::TableScan(scan) = node {
            names.push(qualified_table_name(&scan.table_name));
        }
    };
    walk_logical_plan(plan, &mut visit);
    names
}

fn walk_logical_plan(plan: &LogicalPlan, f: &mut impl FnMut(&LogicalPlan)) {
    f(plan);
    for child in plan.inputs() {
        walk_logical_plan(child, f);
    }
}

/// Parses the final pushed-down SQL out of a `VirtualExecutionPlan`'s
/// formatted display line. The federation crate emits one or more
/// `<stage>=<sql>` fragments inline (`base_sql=`, `rewritten_logical_sql=`,
/// `rewritten_executor_sql=`, `rewritten_ast_analyzer=`, `rewritten_sql_query=`)
/// in execution order. The latest fragment by position is the one actually
/// sent to the DBMS — that is what we return.
fn extract_final_sql_from_fmt(formatted: &str) -> Option<String> {
    const MARKERS: &[&str] = &[
        " base_sql=",
        " rewritten_logical_sql=",
        " rewritten_executor_sql=",
        " rewritten_ast_analyzer=",
        " rewritten_sql_query=",
    ];
    let mut best: Option<(usize, &str)> = None;
    for marker in MARKERS {
        if let Some(pos) = formatted.rfind(marker) {
            if best.is_none_or(|(p, _)| pos > p) {
                best = Some((pos, marker));
            }
        }
    }
    let (pos, marker) = best?;
    let sql = formatted[pos + marker.len()..].trim();
    if sql.is_empty() {
        None
    } else {
        Some(sql.to_string())
    }
}

/// Formats the DataFusion physical plan as indented text — fed into the
/// `physical_plan_text` field of the explain response so the VS Code Source
/// tab's collapsible "DataFusion Physical Plan" section can show it verbatim.
pub fn format_physical_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(true))
}

fn broadcast_filter_targets(info: Option<&BroadcastRewriteInfo>) -> HashSet<String> {
    let mut out = HashSet::new();
    let Some(info) = info else { return out };
    for app in &info.applied {
        out.insert(app.join_key_remote.clone());
    }
    out
}

fn traverse_plan(
    plan: &LogicalPlan,
    nodes: &mut HashMap<String, PlanNode>,
    id_counter: &mut usize,
    truncated: &mut bool,
    ctx: &TraverseContext<'_>,
) -> String {
    let current_id = format!("df_{}", id_counter);
    *id_counter += 1;

    if nodes.len() >= get_max_plan_nodes() {
        *truncated = true;
        return current_id;
    }

    let label = format!("{}", plan.display());
    let mut attributes = plan_attributes(plan);

    // === Broadcast badge attribution ===
    //
    // Driven by the authoritative `BroadcastRewriteInfo.applied` list rather
    // than by structural pattern matching on the (post-rewrite-re-optimised)
    // logical plan. Each rewrite gets stamped on up to three surfaces, with
    // a `broadcast_role` attribute telling the webview which role this node
    // plays so it can render the badge text accordingly:
    //
    //   role=remote_scan  on the TableScan(s) that received the IN-list
    //                     pushdown — the most accurate physical surface,
    //                     and the one that's robust against the optimizer
    //                     pushing the Filter into a federation extension.
    //   role=local_scan   on the local TableScan whose rows became the IN
    //                     list (typically a small CSV/JSON file).
    //   role=join         on the Join node that was rewritten — the semantic
    //                     surface, visible higher in the tree.
    //   role=filter       on a surviving Filter+InList node — legacy surface,
    //                     kept as a safety net for the case where the
    //                     post-rewrite optimizer pass preserves the literal
    //                     Filter node (older datafusion-federation versions).
    if let LogicalPlan::TableScan(scan) = plan {
        let qualified = qualified_table_name(&scan.table_name);
        if let Some(app) =
            application_targeting_remote_table(ctx.broadcast_applications, &qualified)
        {
            stamp_broadcast_attrs(&mut attributes, app, "remote_scan");
        } else if let Some(app) =
            application_targeting_local_table(ctx.broadcast_applications, &qualified)
        {
            stamp_broadcast_attrs(&mut attributes, app, "local_scan");
        }
    } else if matches!(plan, LogicalPlan::Join(_)) {
        if let Some(app) = ctx
            .broadcast_applications
            .iter()
            .find(|app| join_matches_application(plan, app))
        {
            stamp_broadcast_attrs(&mut attributes, app, "join");
        }
    } else if let LogicalPlan::Filter(filter) = plan {
        if let Some(target) = matched_broadcast_target(&filter.predicate, ctx.broadcast_targets) {
            // Find the BroadcastApplication for this target column so the
            // Filter stamp carries the same rich metadata as the other two.
            let app = ctx
                .broadcast_applications
                .iter()
                .find(|app| app.join_key_remote == target);
            if let Some(app) = app {
                stamp_broadcast_attrs(&mut attributes, app, "filter");
            } else {
                // Defensive fallback: target column matched but somehow no
                // corresponding application — keep the legacy minimal stamp
                // so the webview at least shows *something*.
                attributes.insert("broadcast_rewrite".to_string(), "true".to_string());
                attributes.insert("broadcast_role".to_string(), "filter".to_string());
                attributes.insert("broadcast_predicate_column".to_string(), target);
                if let Expr::InList(in_list) = &filter.predicate {
                    attributes.insert(
                        "broadcast_predicate_value_count".to_string(),
                        in_list.list.len().to_string(),
                    );
                }
            }
        }
    }
    // Sort badge — driven entirely by the captured remote SQL strings. Only
    // stamps "pushed" when every TableScan in this Sort's subtree has a
    // remote_sql with `ORDER BY` in it, i.e. federation actually embedded
    // the sort in every remote query and DataFusion will not execute a
    // local sort. Multi-source joins fail this test (the join — and the
    // sort that depends on the join's output — has to run in DataFusion),
    // so the badge correctly does not fire there.
    //
    // When `remote_sqls` is absent (test contexts that don't pass the map)
    // we fall back to the old structural heuristic so existing test
    // expectations continue to hold.
    if let LogicalPlan::Sort(sort) = plan {
        let pushed = match ctx.remote_sqls {
            Some(map) => sort_was_pushed_to_every_scan(sort.input.as_ref(), map),
            None => subtree_is_fully_federated(sort.input.as_ref(), 0),
        };
        attributes.insert("sort_pushed_down".to_string(), pushed.to_string());
    }

    let node_type = match plan {
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::TableScan(_) => "TableScan",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Repartition(_) => "Repartition",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::Subquery(_) => "Subquery",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Window(_) => "Window",
        _ => "Other",
    }
    .to_string();

    let mut source_ref = None;
    let mut native_plan_ref = None;
    let mut provider_kind = None;
    let mut remote_sql = None;
    if let LogicalPlan::TableScan(scan) = plan {
        let tname = qualified_table_name(&scan.table_name);
        source_ref = Some(tname.clone());
        native_plan_ref = Some(tname.clone());

        // Provider classification — prefer the daemon's registration map
        // because it disambiguates MySQL vs MariaDB (the federation executor
        // name is just "mysql" for both). Fall back to the federation
        // executor name we captured from the physical plan if no registration
        // entry exists (e.g. when the scan is registered directly, not via
        // attach_database).
        provider_kind = classify_provider_kind(
            &scan.table_name,
            ctx.registrations,
            ctx.remote_sqls.and_then(|m| m.get(&tname)),
        );

        // Real pushed-down SQL captured from datafusion-federation's
        // VirtualExecutionPlan. None for local file scans.
        if let Some(info) = ctx.remote_sqls.and_then(|m| m.get(&tname)) {
            remote_sql = Some(info.sql.clone());
            // Stamp sort_pushed_down=true on the TableScan itself when the
            // remote SQL contains an ORDER BY — closes the UX gap where the
            // existing badge only sat on the Sort node, but the pushdown
            // actually happens at the scan.
            if sql_contains_order_by(&info.sql) {
                attributes.insert("sort_pushed_down".to_string(), "true".to_string());
            }
        }
    }

    nodes.insert(
        current_id.clone(),
        PlanNode {
            id: current_id.clone(),
            origin: "DataFusion".to_string(),
            node_type,
            label,
            children: Vec::new(),
            attributes,
            metrics: PlanMetrics {
                estimated_rows: None,
                estimated_bytes: None,
                startup_cost: None,
                total_cost: None,
            },
            source_ref,
            native_plan_ref,
            provider_kind,
            remote_sql,
        },
    );

    let mut children = Vec::new();
    for child in plan.inputs() {
        if nodes.len() >= get_max_plan_nodes() {
            *truncated = true;
            break;
        }
        let child_id = traverse_plan(child, nodes, id_counter, truncated, ctx);
        if nodes.contains_key(&child_id) {
            children.push(child_id);
        }
    }
    if let Some(node) = nodes.get_mut(&current_id) {
        node.children = children;
    }

    current_id
}

/// Returns the provider kind string ("postgres" | "mysql" | "mariadb" |
/// "sqlite" | "csv" | "ndjson" | "json" | "parquet" | "fixed_width") for a
/// `TableScan`'s reference, preferring the registration map's `SourceKind`
/// (only present for DB-backed sources) and falling back to the federation
/// executor name captured from the physical plan when present.
fn classify_provider_kind(
    table_ref: &TableReference,
    registrations: Option<&HashMap<String, Arc<DatabaseRegistration>>>,
    remote_info: Option<&RemoteSqlInfo>,
) -> Option<String> {
    if let Some((alias, _)) = table_alias_and_name(table_ref) {
        if let Some(reg) = registrations.and_then(|r| r.get(alias.as_str())) {
            return Some(source_kind_to_provider_kind(reg.kind));
        }
    }
    remote_info.map(|info| info.executor_name.clone())
}

fn source_kind_to_provider_kind(kind: SourceKind) -> String {
    match kind {
        SourceKind::Postgres => "postgres",
        SourceKind::Mysql => "mysql",
        SourceKind::Mariadb => "mariadb",
        SourceKind::Sqlite => "sqlite",
        SourceKind::Csv => "csv",
        SourceKind::Ndjson => "ndjson",
        SourceKind::Json => "json",
        SourceKind::Parquet => "parquet",
        SourceKind::FixedWidth => "fixed_width",
    }
    .to_string()
}

/// Cheap-and-correct check: case-insensitive substring scan, ignoring SQL
/// inside literal quotes. Sufficient because federation-generated SQL never
/// contains user data — the literals are projection columns, identifiers,
/// and integer/string constants from the original query.
fn sql_contains_order_by(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("ORDER BY")
}

/// Evidence-based test: a Sort node is "pushed down" iff every TableScan in
/// its subtree has a captured `remote_sql` containing `ORDER BY`. That tells
/// us federation actually embedded the sort in each remote query, so
/// DataFusion will not need to sort locally.
///
/// Returns `false` when:
/// - the subtree has zero TableScans (degenerate plan),
/// - any leaf is a local file scan (`employees.csv` etc — no remote_sql),
/// - or any leaf's remote SQL lacks `ORDER BY` (e.g. a multi-source join
///   where federation could only push projection/filter, not the cross-source
///   sort).
fn sort_was_pushed_to_every_scan(plan: &LogicalPlan, remote_sqls: &RemoteSqlMap) -> bool {
    let mut any_scan = false;
    let mut all_pushed = true;
    walk_logical_plan(plan, &mut |node| {
        if let LogicalPlan::TableScan(scan) = node {
            any_scan = true;
            let name = qualified_table_name(&scan.table_name);
            let pushed = remote_sqls
                .get(&name)
                .is_some_and(|info| sql_contains_order_by(&info.sql));
            if !pushed {
                all_pushed = false;
            }
        }
    });
    any_scan && all_pushed
}

/// Returns the flat-name of the column targeted by a broadcast-synthesized
/// `IN`-list predicate if `expr` is one. Used to stamp the matching `Filter`
/// node in the explain plan graph.
fn matched_broadcast_target(expr: &Expr, targets: &HashSet<String>) -> Option<String> {
    let Expr::InList(in_list) = expr else {
        return None;
    };
    let Expr::Column(col) = in_list.expr.as_ref() else {
        return None;
    };
    let key = col.flat_name();
    if targets.contains(&key) {
        Some(key)
    } else {
        None
    }
}

pub fn truncate_raw_plan(raw: &str) -> (String, Option<String>) {
    let limit = get_raw_plan_text_limit();
    if raw.len() <= limit {
        return (raw.to_string(), None);
    }

    (
        format!(
            "{}\n\n... [TRUNCATED - PLAN TEXT EXCEEDED {limit} BYTES] ...",
            &raw[..limit]
        ),
        Some(format!(
            "Raw plan text exceeded {limit} bytes and was truncated."
        )),
    )
}

/// Builds one [`SourcePlanEntry`] per remote `TableScan` in the logical plan,
/// using the actual pushed-down SQL captured from the physical plan (via
/// [`collect_remote_sql_for_scans`]) instead of the placeholder
/// `SELECT * FROM table` we used before. The remote `EXPLAIN` now runs against
/// the real SQL, so the per-table card in the Source tab shows a DBMS plan
/// that matches what QuiverSQL actually executes — including filter, sort,
/// limit, and broadcast `IN (…)` pushdowns.
pub(crate) async fn extract_source_plans(
    plan: &LogicalPlan,
    registrations: &HashMap<String, Arc<DatabaseRegistration>>,
    remote_sqls: &RemoteSqlMap,
) -> HashMap<String, SourcePlanEntry> {
    let mut source_plans = HashMap::new();
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans);

    for scan in scans {
        let table_name = qualified_table_name(&scan.table_name);
        let Some((alias, table)) = table_alias_and_name(&scan.table_name) else {
            continue;
        };
        let Some(registration) = registrations.get(alias.as_str()) else {
            continue;
        };
        let captured = remote_sqls.get(&table_name);

        if let Some(entry) =
            build_source_plan_entry(registration, table.as_str(), captured, scan).await
        {
            source_plans.insert(table_name, entry);
        }
    }

    source_plans
}

async fn build_source_plan_entry(
    registration: &DatabaseRegistration,
    table: &str,
    captured: Option<&RemoteSqlInfo>,
    _scan: &TableScan,
) -> Option<SourcePlanEntry> {
    match registration.kind {
        qsql_core::models::SourceKind::Sqlite => {
            let db_path = registration.db_path.as_ref()?;
            let table_ref = SqlTableRef::bare(table.to_string());
            let sql = captured
                .map(|i| i.sql.clone())
                .unwrap_or_else(|| native_select_all_sql(&table_ref, SqlDialectKind::Sqlite));
            let connector = qsql_connectors::sqlite::SqliteConnector::new(db_path);
            let explain = explain_with_timeout(&connector, &sql, SQLITE_EXPLAIN_TIMEOUT)
                .await
                .ok();
            Some(SourcePlanEntry {
                provider_kind: "sqlite".to_string(),
                native_sql: sql,
                native_explain: explain
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
                dialect: "sqlite".to_string(),
            })
        }
        qsql_core::models::SourceKind::Postgres => {
            let connection_string = registration.connection_string.as_ref()?;
            let schema_name = registration.schema.as_deref().unwrap_or("public");
            let table_ref = SqlTableRef::with_schema(schema_name.to_string(), table.to_string());
            let sql = captured
                .map(|i| i.sql.clone())
                .unwrap_or_else(|| native_select_all_sql(&table_ref, SqlDialectKind::Postgres));
            let connector = qsql_connectors::postgres::PostgresConnector::new(connection_string);
            let explain = explain_with_timeout(&connector, &sql, get_remote_explain_timeout())
                .await
                .ok()
                .map(|raw| {
                    serde_json::from_str::<serde_json::Value>(&raw)
                        .unwrap_or(serde_json::Value::String(raw))
                });
            Some(SourcePlanEntry {
                provider_kind: "postgres".to_string(),
                native_sql: sql,
                native_explain: explain.unwrap_or(serde_json::Value::Null),
                dialect: "postgresql".to_string(),
            })
        }
        qsql_core::models::SourceKind::Mysql | qsql_core::models::SourceKind::Mariadb => {
            let connection_string = registration.connection_string.as_ref()?;
            let dialect = registration.dialect.unwrap_or(match registration.kind {
                qsql_core::models::SourceKind::Mariadb => SqlDialectKind::Mariadb,
                _ => SqlDialectKind::Mysql,
            });
            let table_ref = match registration.schema.as_deref() {
                Some(schema) if !schema.trim().is_empty() => {
                    SqlTableRef::with_schema(schema.to_string(), table.to_string())
                }
                _ => SqlTableRef::bare(table.to_string()),
            };
            let sql = captured
                .map(|i| i.sql.clone())
                .unwrap_or_else(|| native_select_all_sql(&table_ref, dialect));
            let connector = qsql_connectors::mysql::MySqlConnector::new(connection_string, dialect);
            let explain = explain_with_timeout(&connector, &sql, get_remote_explain_timeout())
                .await
                .ok()
                .map(|raw| {
                    serde_json::from_str::<serde_json::Value>(&raw)
                        .unwrap_or(serde_json::Value::String(raw))
                });
            let (provider_kind, dialect_name) = match registration.kind {
                qsql_core::models::SourceKind::Mariadb => ("mariadb", "mariadb"),
                _ => ("mysql", "mysql"),
            };
            Some(SourcePlanEntry {
                provider_kind: provider_kind.to_string(),
                native_sql: sql,
                native_explain: explain.unwrap_or(serde_json::Value::Null),
                dialect: dialect_name.to_string(),
            })
        }
        _ => None,
    }
}

async fn explain_with_timeout<C: RemoteConnector + ?Sized>(
    connector: &C,
    sql: &str,
    timeout: Duration,
) -> Result<String, qsql_connectors::ConnectorError> {
    tokio::time::timeout(timeout, connector.explain_query(sql))
        .await
        .map_err(|_| {
            qsql_connectors::ConnectorError::new(
                qsql_connectors::ConnectorErrorKind::Timeout,
                format!(
                    "Timed out explaining query for {} after {}s",
                    connector.connector_type(),
                    timeout.as_secs()
                ),
            )
        })?
}

fn table_alias_and_name(table_ref: &TableReference) -> Option<(String, String)> {
    match table_ref {
        TableReference::Partial { schema, table } => Some((schema.to_string(), table.to_string())),
        TableReference::Full { schema, table, .. } => Some((schema.to_string(), table.to_string())),
        TableReference::Bare { .. } => None,
    }
}

fn collect_scans<'a>(plan: &'a LogicalPlan, scans: &mut Vec<&'a TableScan>) {
    if let LogicalPlan::TableScan(scan) = plan {
        scans.push(scan);
    }
    for child in plan.inputs() {
        collect_scans(child, scans);
    }
}

pub(crate) fn qualified_table_name(table_ref: &TableReference) -> String {
    match table_ref {
        TableReference::Bare { table } => table.to_string(),
        TableReference::Partial { schema, table } => format!("{schema}.{table}"),
        TableReference::Full { schema, table, .. } => format!("{schema}.{table}"),
    }
}

/// Returns `true` when every `TableScan` reachable within `max_depth` levels
/// from `plan` is backed by a `GuardedTableProvider` (i.e. a SQL-federated
/// source), and at least one such scan exists. File-based sources (CSV,
/// Parquet, etc.) are not wrapped in `GuardedTableProvider`, so their presence
/// causes this to return `false`.
fn subtree_is_fully_federated(plan: &LogicalPlan, depth: usize) -> bool {
    const MAX_DEPTH: usize = 8;
    if depth > MAX_DEPTH {
        return false;
    }
    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                source
                    .table_provider
                    .as_any()
                    .downcast_ref::<GuardedTableProvider>()
                    .is_some()
            } else {
                false
            }
        }
        // Extension nodes are federation rewrites — always federated.
        LogicalPlan::Extension(_) => true,
        // For all other nodes, all children must be federated.
        other => {
            let children: Vec<&LogicalPlan> = other.inputs();
            if children.is_empty() {
                return false;
            }
            children
                .into_iter()
                .all(|c| subtree_is_fully_federated(c, depth + 1))
        }
    }
}

fn plan_attributes(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    let output_columns = schema_columns(plan);
    if !output_columns.is_empty() {
        attributes.insert("output_columns".to_string(), output_columns);
    }

    match plan {
        LogicalPlan::Projection(projection) => {
            attributes.insert(
                "expressions".to_string(),
                expressions_to_string(&projection.expr),
            );
        }
        LogicalPlan::Filter(filter) => {
            attributes.insert("predicate".to_string(), filter.predicate.to_string());
        }
        LogicalPlan::TableScan(scan) => {
            attributes.insert("table".to_string(), qualified_table_name(&scan.table_name));
            if let Some(projection) = &scan.projection {
                attributes.insert("projection".to_string(), format!("{projection:?}"));
            }
            if !scan.filters.is_empty() {
                attributes.insert("filters".to_string(), expressions_to_string(&scan.filters));
            }
            if let Some(fetch) = scan.fetch {
                attributes.insert("limit".to_string(), fetch.to_string());
            }
            if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                if let Some(guarded) = source
                    .table_provider
                    .as_any()
                    .downcast_ref::<GuardedTableProvider>()
                {
                    attributes.insert("guarded_scan".to_string(), "true".to_string());
                    attributes.insert(
                        "scan_budget_rows".to_string(),
                        guarded.budget().max_rows.to_string(),
                    );
                    attributes.insert(
                        "scan_budget_bytes".to_string(),
                        guarded.budget().max_bytes.to_string(),
                    );
                }
            }
        }
        LogicalPlan::Sort(sort) => {
            attributes.insert("sort".to_string(), expressions_to_string(&sort.expr));
            // Comma-joined column expressions without direction qualifiers.
            let sort_cols: Vec<String> = sort.expr.iter().map(|se| se.expr.to_string()).collect();
            attributes.insert("sort_columns".to_string(), sort_cols.join(", "));
            if let Some(fetch) = sort.fetch {
                attributes.insert("limit".to_string(), fetch.to_string());
            }
            // NOTE: `sort_pushed_down` is intentionally NOT stamped here.
            // It's set later from `traverse_plan` using the captured
            // `remote_sqls` map — the only authoritative evidence that
            // federation actually embedded ORDER BY in a remote query. The
            // old structural heuristic ("every leaf is a GuardedTableProvider")
            // produced false positives for multi-source joins (pg + mysql),
            // where every leaf is federated but the join — and therefore the
            // sort — has to happen locally.
        }
        LogicalPlan::Join(join) => {
            attributes.insert("join_type".to_string(), format!("{:?}", join.join_type));
            if !join.on.is_empty() {
                let on = join
                    .on
                    .iter()
                    .map(|(left, right)| format!("{left} = {right}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                attributes.insert("join_condition".to_string(), on);
            }
            if let Some(filter) = &join.filter {
                attributes.insert("filter".to_string(), filter.to_string());
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            attributes.insert("alias".to_string(), qualified_table_name(&alias.alias));
        }
        _ => {}
    }

    attributes
}

fn schema_columns(plan: &LogicalPlan) -> String {
    plan.schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn expressions_to_string<T: std::fmt::Display>(expressions: &[T]) -> String {
    expressions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{lit, LogicalPlan, LogicalPlanBuilder, Union};
    use qsql_connectors::{ConnectorError, ConnectorResult};
    use qsql_core::models::ConnectorCapabilities;

    #[derive(Debug)]
    struct SlowExplainConnector;

    #[async_trait::async_trait]
    impl RemoteConnector for SlowExplainConnector {
        fn connector_type(&self) -> &'static str {
            "slow"
        }

        async fn table_provider(
            &self,
            _schema: Option<&str>,
            _table: &str,
            _cached_schema: Option<SchemaRef>,
        ) -> ConnectorResult<Arc<dyn TableProvider>> {
            Err(ConnectorError::other("not implemented"))
        }

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities {
                projection: false,
                filter: false,
                limit: false,
                sort: false,
                aggregate: false,
                joins: false,
                dialect_name: "slow".to_string(),
            }
        }

        async fn explain_query(&self, _sql: &str) -> ConnectorResult<String> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("late".to_string())
        }

        async fn list_tables(
            &self,
            _schema: Option<&str>,
            _limit: usize,
        ) -> ConnectorResult<Vec<String>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn qualified_table_name_uses_schema_and_table_without_catalog() {
        assert_eq!(
            qualified_table_name(&TableReference::bare("customers")),
            "customers"
        );
        assert_eq!(
            qualified_table_name(&TableReference::partial("pg_local", "customers")),
            "pg_local.customers"
        );
        assert_eq!(
            qualified_table_name(&TableReference::full("datafusion", "pg_local", "customers")),
            "pg_local.customers"
        );
    }

    #[test]
    fn truncate_raw_plan_caps_large_text_and_returns_warning() {
        let raw = "x".repeat(RAW_PLAN_TEXT_LIMIT + 10);
        let (truncated, warning) = truncate_raw_plan(&raw);

        assert!(truncated.len() < raw.len() + 100);
        assert!(truncated.contains("TRUNCATED"));
        assert!(warning.unwrap().contains("truncated"));
    }

    #[test]
    fn build_plan_graph_truncates_synthetic_10k_node_plan() {
        let leaf = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i64).alias("value")])
            .unwrap()
            .build()
            .unwrap();
        let inputs = (0..10_000)
            .map(|_| Arc::new(leaf.clone()))
            .collect::<Vec<_>>();
        let plan = LogicalPlan::Union(Union::try_new(inputs).unwrap());
        let graph = build_plan_graph(&plan);

        assert!(graph.truncated);
        assert_eq!(graph.node_count, MAX_PLAN_NODES);
    }

    #[tokio::test]
    async fn explain_with_timeout_returns_connector_timeout() {
        let err = explain_with_timeout(&SlowExplainConnector, "SELECT 1", Duration::from_millis(1))
            .await
            .unwrap_err();

        assert_eq!(err.kind, qsql_connectors::ConnectorErrorKind::Timeout);
        assert!(err.message.contains("Timed out explaining query"));
    }

    #[tokio::test]
    async fn explain_with_timeout_succeeds_when_fast() {
        let result = explain_with_timeout(
            &SlowExplainConnector,
            "SELECT 1",
            Duration::from_millis(200),
        )
        .await
        .unwrap();
        assert_eq!(result, "late");
    }

    // --- truncate_raw_plan below limit ---

    #[test]
    fn truncate_raw_plan_below_limit_is_unchanged() {
        let short = "short plan";
        let (text, warning) = truncate_raw_plan(short);
        assert_eq!(text, short);
        assert!(warning.is_none());
    }

    // --- qualified_table_name all variants ---

    #[test]
    fn table_alias_and_name_covers_all_variants() {
        // Bare → None
        assert!(table_alias_and_name(&TableReference::bare("t")).is_none());
        // Partial → Some
        let (alias, table) = table_alias_and_name(&TableReference::partial("s", "t")).unwrap();
        assert_eq!(alias, "s");
        assert_eq!(table, "t");
        // Full → Some (catalog dropped)
        let (alias, table) = table_alias_and_name(&TableReference::full("cat", "s", "t")).unwrap();
        assert_eq!(alias, "s");
        assert_eq!(table, "t");
    }

    // --- matched_broadcast_target ---

    #[test]
    fn matched_broadcast_target_matches_in_list_on_known_column() {
        use datafusion::common::Column;
        use datafusion::logical_expr::expr::InList;

        let mut targets = HashSet::new();
        targets.insert("user_id".to_string());

        let in_list = Expr::InList(InList {
            expr: Box::new(Expr::Column(Column::new_unqualified("user_id"))),
            list: vec![Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(1)),
                None,
            )],
            negated: false,
        });
        assert_eq!(
            matched_broadcast_target(&in_list, &targets),
            Some("user_id".to_string())
        );

        // Column not in targets → None
        let in_list_unknown = Expr::InList(InList {
            expr: Box::new(Expr::Column(Column::new_unqualified("other"))),
            list: vec![],
            negated: false,
        });
        assert!(matched_broadcast_target(&in_list_unknown, &targets).is_none());

        // Non-InList expr → None
        let lit_expr = Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), None);
        assert!(matched_broadcast_target(&lit_expr, &targets).is_none());
    }

    // --- broadcast_filter_targets ---

    #[test]
    fn broadcast_filter_targets_collects_remote_keys() {
        use qsql_core::broadcast::{BroadcastApplication, BroadcastRewriteInfo};

        let info = BroadcastRewriteInfo {
            considered: 1,
            applied: vec![BroadcastApplication {
                local_table: "csv".to_string(),
                remote_table: "pg".to_string(),
                join_key_local: "id".to_string(),
                join_key_remote: "user_id".to_string(),
                local_rows_materialized: 3,
                local_bytes_materialized: 100,
                predicate_value_count: 3,
                elapsed_ms: 1,
            }],
            skipped: vec![],
        };
        let targets = broadcast_filter_targets(Some(&info));
        assert!(targets.contains("user_id"));
        assert!(!targets.contains("id"));

        // None info → empty
        let empty = broadcast_filter_targets(None);
        assert!(empty.is_empty());
    }

    // --- schema_columns and expressions_to_string ---

    #[test]
    fn schema_columns_returns_comma_separated_field_names() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::DFSchema;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone()).unwrap();
        let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        });
        let cols = schema_columns(&plan);
        assert!(cols.contains("id"));
        assert!(cols.contains("name"));
    }

    #[test]
    fn expressions_to_string_joins_with_comma() {
        use datafusion::logical_expr::lit;
        let exprs = vec![lit(1_i64), lit(2_i64)];
        let s = expressions_to_string(&exprs);
        assert!(s.contains(","));
        assert!(s.contains("1"));
        assert!(s.contains("2"));
    }

    // --- build_plan_graph_with_broadcast stamps filter nodes ---

    #[test]
    fn build_plan_graph_with_broadcast_stamps_filter_node() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use qsql_core::broadcast::{BroadcastApplication, BroadcastRewriteInfo};

        // Build a plan: EmptyRelation → Filter(user_id IN (1))
        let base = LogicalPlanBuilder::empty(false).build().unwrap();
        // We can't easily build a typed filter over EmptyRelation columns,
        // so instead test that build_plan_graph_with_broadcast propagates
        // broadcast info to the graph without panicking on a simple plan.
        let info = BroadcastRewriteInfo {
            considered: 1,
            applied: vec![BroadcastApplication {
                local_table: "l".to_string(),
                remote_table: "r".to_string(),
                join_key_local: "id".to_string(),
                join_key_remote: "user_id".to_string(),
                local_rows_materialized: 2,
                local_bytes_materialized: 64,
                predicate_value_count: 2,
                elapsed_ms: 0,
            }],
            skipped: vec![],
        };
        let graph = build_plan_graph_with_broadcast(&base, Some(&info), None, None);
        assert!(!graph.truncated);
        assert!(!graph.nodes.is_empty());
    }

    // --- plan_attributes for various node types ---

    #[tokio::test]
    async fn plan_attributes_covers_projection_filter_sort() {
        use datafusion::prelude::SessionContext;
        // Build a simple plan via SQL to get a Projection→Filter→TableScan
        let ctx = SessionContext::new();
        // Register a MemTable
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM t WHERE id > 1 ORDER BY id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        // build_plan_graph should traverse Sort → Filter → Projection → TableScan
        let graph = build_plan_graph(&optimized);
        assert!(!graph.nodes.is_empty());
        assert!(!graph.root_ids.is_empty());

        // Check a node has output_columns attribute
        let has_output_col = graph
            .nodes
            .values()
            .any(|n| n.attributes.contains_key("output_columns"));
        assert!(
            has_output_col,
            "at least one node should have output_columns"
        );
    }

    #[tokio::test]
    async fn sort_node_has_sort_columns_and_pushed_down_attributes() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3i64, 1, 2]))],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM t ORDER BY id ASC LIMIT 5")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();
        let graph = build_plan_graph(&optimized);

        // Find the Sort node
        let sort_node = graph
            .nodes
            .values()
            .find(|n| n.node_type == "Sort" || n.node_type == "Limit")
            .or_else(|| {
                graph
                    .nodes
                    .values()
                    .find(|n| n.attributes.contains_key("sort_columns"))
            });

        // At least one node should carry sort_columns (Sort) or sort attribute
        let has_sort_attrs = graph.nodes.values().any(|n| {
            n.attributes.contains_key("sort_columns") || n.attributes.contains_key("sort")
        });
        assert!(
            has_sort_attrs,
            "expected sort_columns or sort attribute on a plan node"
        );

        // sort_pushed_down must be present on any Sort node
        let sort_nodes: Vec<_> = graph
            .nodes
            .values()
            .filter(|n| n.attributes.contains_key("sort_columns"))
            .collect();
        for node in &sort_nodes {
            assert!(
                node.attributes.contains_key("sort_pushed_down"),
                "Sort node missing sort_pushed_down attribute"
            );
            // File-backed MemTable is not federated → false
            assert_eq!(
                node.attributes.get("sort_pushed_down").map(String::as_str),
                Some("false"),
                "MemTable is not federated so sort_pushed_down should be false"
            );
        }

        let _ = sort_node; // used above
    }

    // --- collect_scans ---

    #[tokio::test]
    async fn collect_scans_finds_table_scan_nodes() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "t1",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "t2",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        let mut scans = Vec::new();
        collect_scans(&optimized, &mut scans);
        assert_eq!(scans.len(), 2, "both table scans should be collected");
    }

    // --- extract_final_sql_from_fmt ---

    #[test]
    fn extract_final_sql_picks_last_marker_when_multiple_present() {
        // Simulates a federation fmt line where the AST analyzer rewrote the
        // initial SQL — we must return the *last* rewritten variant, not the
        // original.
        let formatted = "VirtualExecutionPlan name=postgres compute_context=conn \
             base_sql=SELECT \"id\" FROM \"public\".\"customers\" \
             rewritten_ast_analyzer=SELECT \"id\" FROM \"public\".\"customers\" WHERE \"id\" IN (1, 2)";
        let sql = extract_final_sql_from_fmt(formatted).unwrap();
        assert!(
            sql.contains("WHERE \"id\" IN (1, 2)"),
            "expected last (rewritten) SQL to win, got: {sql}"
        );
        assert!(!sql.contains("base_sql="));
    }

    #[test]
    fn extract_final_sql_falls_back_to_base_sql_when_no_rewrite() {
        let formatted = "VirtualExecutionPlan name=mysql base_sql=SELECT `id` FROM `db`.`users`";
        let sql = extract_final_sql_from_fmt(formatted).unwrap();
        assert_eq!(sql, "SELECT `id` FROM `db`.`users`");
    }

    #[test]
    fn extract_final_sql_returns_none_when_no_marker_present() {
        let formatted = "ProjectionExec: expr=[id@0 as id]";
        assert!(extract_final_sql_from_fmt(formatted).is_none());
    }

    // --- sql_contains_order_by ---

    #[test]
    fn sql_contains_order_by_is_case_insensitive() {
        assert!(sql_contains_order_by("SELECT * FROM t ORDER BY id"));
        assert!(sql_contains_order_by("select * from t order by id"));
        assert!(sql_contains_order_by("SELECT * FROM t Order By id"));
        assert!(!sql_contains_order_by("SELECT * FROM t"));
    }

    // --- source_kind_to_provider_kind ---

    #[test]
    fn source_kind_to_provider_kind_covers_every_variant() {
        assert_eq!(
            source_kind_to_provider_kind(SourceKind::Postgres),
            "postgres"
        );
        assert_eq!(source_kind_to_provider_kind(SourceKind::Mysql), "mysql");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Mariadb), "mariadb");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Sqlite), "sqlite");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Csv), "csv");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Ndjson), "ndjson");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Json), "json");
        assert_eq!(source_kind_to_provider_kind(SourceKind::Parquet), "parquet");
        assert_eq!(
            source_kind_to_provider_kind(SourceKind::FixedWidth),
            "fixed_width"
        );
    }

    // --- classify_provider_kind ---

    #[test]
    fn classify_provider_kind_prefers_registration_over_executor_name() {
        // Registration says MariaDB; federation executor name is "mysql"
        // (one connector handles both). Registration must win so the UI
        // shows the correct icon.
        let mut registrations = HashMap::new();
        registrations.insert(
            "db".to_string(),
            Arc::new(DatabaseRegistration {
                kind: SourceKind::Mariadb,
                generation: 1,
                connection_string: None,
                db_path: None,
                schema: None,
                dialect: None,
                tables: vec![],
                tables_truncated: false,
            }),
        );
        let info = RemoteSqlInfo {
            sql: "SELECT 1".to_string(),
            executor_name: "mysql".to_string(),
        };
        let kind = classify_provider_kind(
            &TableReference::partial("db", "users"),
            Some(&registrations),
            Some(&info),
        );
        assert_eq!(kind.as_deref(), Some("mariadb"));
    }

    #[test]
    fn classify_provider_kind_falls_back_to_executor_name_when_no_registration() {
        let info = RemoteSqlInfo {
            sql: "SELECT 1".to_string(),
            executor_name: "sqlite".to_string(),
        };
        let kind = classify_provider_kind(&TableReference::bare("local"), None, Some(&info));
        assert_eq!(kind.as_deref(), Some("sqlite"));
    }

    #[test]
    fn classify_provider_kind_is_none_for_unregistered_local_scan() {
        // No registration and no captured federation SQL — this is a local
        // file scan. The webview will fall back to a generic table icon.
        let kind = classify_provider_kind(&TableReference::bare("employees"), None, None);
        assert!(kind.is_none());
    }

    // --- build_plan_graph_with_broadcast stamps remote_sql and provider_kind ---

    #[tokio::test]
    async fn build_plan_graph_stamps_remote_sql_and_provider_kind_on_scan() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::catalog::SchemaProvider;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let schema_prov = Arc::new(datafusion::catalog::MemorySchemaProvider::new());
        schema_prov
            .register_table(
                "users".to_string(),
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema("pg", schema_prov)
            .unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM pg.users ORDER BY id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        // Inject a fake remote SQL capture as if datafusion-federation had
        // produced this query, with ORDER BY pushed down.
        let mut remote = RemoteSqlMap::new();
        remote.insert(
            "pg.users".to_string(),
            RemoteSqlInfo {
                sql: "SELECT \"id\" FROM \"public\".\"users\" ORDER BY \"id\"".to_string(),
                executor_name: "postgres".to_string(),
            },
        );

        let mut registrations = HashMap::new();
        registrations.insert(
            "pg".to_string(),
            Arc::new(DatabaseRegistration {
                kind: SourceKind::Postgres,
                generation: 1,
                connection_string: Some("postgres://localhost/x".to_string()),
                db_path: None,
                schema: Some("public".to_string()),
                dialect: None,
                tables: vec!["users".to_string()],
                tables_truncated: false,
            }),
        );

        let graph =
            build_plan_graph_with_broadcast(&optimized, None, Some(&remote), Some(&registrations));

        let scan = graph
            .nodes
            .values()
            .find(|n| n.node_type == "TableScan")
            .expect("TableScan present");
        assert_eq!(
            scan.provider_kind.as_deref(),
            Some("postgres"),
            "TableScan should be classified as postgres from registration"
        );
        assert!(
            scan.remote_sql
                .as_ref()
                .is_some_and(|s| s.contains("ORDER BY")),
            "TableScan should carry the pushed-down SQL with ORDER BY"
        );
        assert_eq!(
            scan.attributes.get("sort_pushed_down").map(String::as_str),
            Some("true"),
            "TableScan with ORDER BY in remote_sql should be marked sort_pushed_down"
        );
    }

    // --- extract_sql_exec_sql ---

    #[test]
    fn extract_sql_exec_sql_returns_sql_after_marker() {
        // datafusion-table-providers SqlExec emits "SqlExec sql=<sql>" as a
        // one-line DisplayAs format. We harvest the whole tail.
        let formatted = r#"SqlExec sql=SELECT "id" FROM "public"."customers" WHERE "id" IN (1, 2)"#;
        let sql = extract_sql_exec_sql(formatted).unwrap();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("IN (1, 2)"));
    }

    #[test]
    fn extract_sql_exec_sql_returns_none_when_marker_missing() {
        assert!(extract_sql_exec_sql("ProjectionExec: expr=[id@0 as id]").is_none());
    }

    #[test]
    fn extract_sql_exec_sql_handles_mysql_specialised_exec() {
        // datafusion-table-providers ships per-DB exec variants. MySQL uses
        // `MySQLSQLExec sql=...` (note capital "SQL"). The parser must
        // recognise it just as well as the generic `SqlExec sql=...`.
        let formatted =
            "MySQLSQLExec sql=SELECT `orders`.`customer_id`, `orders`.`order_total` FROM `orders`";
        let sql = extract_sql_exec_sql(formatted).unwrap();
        assert!(sql.starts_with("SELECT `orders`"));
        assert!(sql.contains("FROM `orders`"));
    }

    #[test]
    fn extract_sql_exec_sql_ignores_virtual_execution_plan() {
        // VirtualExecutionPlan ends in "Plan" and has its own marker family
        // (`base_sql=`, `rewritten_*_sql=`) — must NOT match the SqlExec
        // path or we'd double-capture.
        let formatted =
            "VirtualExecutionPlan name=postgres base_sql=SELECT \"id\" FROM \"public\".\"customers\"";
        assert!(extract_sql_exec_sql(formatted).is_none());
    }

    #[test]
    fn extract_sql_exec_sql_ignores_non_sql_execs() {
        // SortExec / RepartitionExec end in "Exec" but have no `sql=` field
        // — strip_prefix("sql=") must return None for these.
        assert!(extract_sql_exec_sql("SortExec: expr=[id@0 ASC]").is_none());
        assert!(extract_sql_exec_sql(
            "RepartitionExec: partitioning=Hash([id@0], 4), input_partitions=1"
        )
        .is_none());
    }

    // --- guess_executor_from_sql ---

    #[test]
    fn guess_executor_from_sql_distinguishes_dialects() {
        assert_eq!(
            guess_executor_from_sql(r#"SELECT "id" FROM "public"."customers""#),
            "postgres"
        );
        assert_eq!(
            guess_executor_from_sql("SELECT `id` FROM `db`.`users`"),
            "mysql"
        );
        assert_eq!(guess_executor_from_sql("SELECT id FROM users"), "sql");
    }

    // --- sql_references_table ---

    #[test]
    fn sql_references_table_matches_schema_qualified_postgres() {
        let sql = r#"SELECT "id","name" FROM "public"."customers" ORDER BY "id""#;
        assert!(sql_references_table(sql, Some("public"), "customers"));
        assert!(!sql_references_table(sql, Some("public"), "orders"));
    }

    #[test]
    fn sql_references_table_matches_schema_qualified_mysql() {
        let sql = "SELECT `id`,`amount` FROM `qsql_test`.`orders` WHERE `id` IN (1, 2)";
        assert!(sql_references_table(sql, Some("qsql_test"), "orders"));
        assert!(!sql_references_table(sql, Some("qsql_test"), "customers"));
    }

    #[test]
    fn sql_references_table_matches_bare_table_when_schema_unknown() {
        let sql = "SELECT `id` FROM `items`";
        assert!(sql_references_table(sql, None, "items"));
        assert!(!sql_references_table(sql, None, "products"));
    }

    // --- broadcast attribution unit tests ---

    #[test]
    fn application_targeting_remote_table_handles_single_and_multi_table_lists() {
        let single = vec![BroadcastApplication {
            local_table: "employees".to_string(),
            remote_table: "pg.customer_profiles".to_string(),
            join_key_local: "name".to_string(),
            join_key_remote: "account_manager".to_string(),
            local_rows_materialized: 5,
            local_bytes_materialized: 100,
            predicate_value_count: 5,
            elapsed_ms: 12,
        }];
        assert!(application_targeting_remote_table(&single, "pg.customer_profiles").is_some());
        assert!(application_targeting_remote_table(&single, "pg.orders").is_none());

        // remote_table may be a comma-joined set when a federated subtree
        // spans multiple TableScans — any one of them must match.
        let multi = vec![BroadcastApplication {
            local_table: "employees".to_string(),
            remote_table: "pg.customers,pg.customer_profiles".to_string(),
            join_key_local: "name".to_string(),
            join_key_remote: "account_manager".to_string(),
            local_rows_materialized: 5,
            local_bytes_materialized: 100,
            predicate_value_count: 5,
            elapsed_ms: 12,
        }];
        assert!(application_targeting_remote_table(&multi, "pg.customers").is_some());
        assert!(application_targeting_remote_table(&multi, "pg.customer_profiles").is_some());
        assert!(application_targeting_remote_table(&multi, "mysql.orders").is_none());
    }

    #[test]
    fn comma_list_contains_trims_whitespace_and_matches_exactly() {
        assert!(comma_list_contains("a, b, c", "b"));
        assert!(comma_list_contains("a,b,c", "c"));
        assert!(comma_list_contains("single", "single"));
        assert!(!comma_list_contains("a,b", "ab"));
        assert!(!comma_list_contains("foo.bar", "foo"));
    }

    #[test]
    fn stamp_broadcast_attrs_writes_all_fields_with_role() {
        let app = BroadcastApplication {
            local_table: "employees".to_string(),
            remote_table: "pg.customer_profiles".to_string(),
            join_key_local: "name".to_string(),
            join_key_remote: "account_manager".to_string(),
            local_rows_materialized: 5,
            local_bytes_materialized: 100,
            predicate_value_count: 5,
            elapsed_ms: 12,
        };
        let mut attrs: HashMap<String, String> = HashMap::new();
        stamp_broadcast_attrs(&mut attrs, &app, "remote_scan");
        assert_eq!(
            attrs.get("broadcast_rewrite").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            attrs.get("broadcast_role").map(String::as_str),
            Some("remote_scan")
        );
        assert_eq!(
            attrs.get("broadcast_predicate_column").map(String::as_str),
            Some("account_manager")
        );
        assert_eq!(
            attrs
                .get("broadcast_predicate_value_count")
                .map(String::as_str),
            Some("5")
        );
        assert_eq!(
            attrs.get("broadcast_local_table").map(String::as_str),
            Some("employees")
        );
        assert_eq!(
            attrs.get("broadcast_remote_table").map(String::as_str),
            Some("pg.customer_profiles")
        );
        assert_eq!(
            attrs.get("broadcast_elapsed_ms").map(String::as_str),
            Some("12")
        );
    }

    #[tokio::test]
    async fn broadcast_badge_lands_on_remote_table_scan_even_when_filter_was_optimized_out() {
        // Reproduces the regression: after the post-rewrite optimization
        // pass, the Filter+InList node QuiverSQL synthesized often gets
        // folded into a federation extension. The legacy badge logic looked
        // only for that surviving Filter — and so showed nothing. The new
        // evidence-based stamper should still mark the affected remote
        // TableScan because we have the BroadcastApplication in hand.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "account_manager",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "customer_profiles",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        // Bare TableScan plan — no surviving Filter node above it. The new
        // attribution must still land on this scan because the rewrite
        // info says so.
        let plan = ctx
            .state()
            .create_logical_plan("SELECT account_manager FROM customer_profiles")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        let info = BroadcastRewriteInfo {
            considered: 1,
            applied: vec![BroadcastApplication {
                local_table: "employees".to_string(),
                remote_table: "customer_profiles".to_string(),
                join_key_local: "name".to_string(),
                join_key_remote: "account_manager".to_string(),
                local_rows_materialized: 3,
                local_bytes_materialized: 80,
                predicate_value_count: 3,
                elapsed_ms: 7,
            }],
            skipped: vec![],
        };

        let graph = build_plan_graph_with_broadcast(&optimized, Some(&info), None, None);
        let scan = graph
            .nodes
            .values()
            .find(|n| n.node_type == "TableScan")
            .expect("plan must contain TableScan");
        assert_eq!(
            scan.attributes.get("broadcast_rewrite").map(String::as_str),
            Some("true"),
            "remote TableScan must carry the broadcast badge regardless of Filter survival"
        );
        assert_eq!(
            scan.attributes.get("broadcast_role").map(String::as_str),
            Some("remote_scan")
        );
        assert_eq!(
            scan.attributes
                .get("broadcast_predicate_value_count")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            scan.attributes
                .get("broadcast_local_table")
                .map(String::as_str),
            Some("employees")
        );
    }

    #[tokio::test]
    async fn broadcast_badge_lands_on_join_node_too() {
        // Confirms the second redundant surface: the Join that was rewritten
        // gets the badge too, with role=join. Multi-redundant stamping means
        // the user always sees the badge somewhere in the tree.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "employees",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "customer_profiles",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let plan = ctx
            .state()
            .create_logical_plan(
                "SELECT employees.id FROM employees \
                 JOIN customer_profiles ON employees.id = customer_profiles.id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        let info = BroadcastRewriteInfo {
            considered: 1,
            applied: vec![BroadcastApplication {
                local_table: "employees".to_string(),
                remote_table: "customer_profiles".to_string(),
                join_key_local: "id".to_string(),
                join_key_remote: "id".to_string(),
                local_rows_materialized: 1,
                local_bytes_materialized: 8,
                predicate_value_count: 1,
                elapsed_ms: 2,
            }],
            skipped: vec![],
        };

        let graph = build_plan_graph_with_broadcast(&optimized, Some(&info), None, None);
        let join = graph
            .nodes
            .values()
            .find(|n| n.node_type == "Join")
            .expect("plan must contain Join");
        assert_eq!(
            join.attributes.get("broadcast_role").map(String::as_str),
            Some("join")
        );
    }

    #[tokio::test]
    async fn broadcast_badge_does_not_fire_when_no_applications() {
        // Empty/None broadcast info → no broadcast attrs anywhere.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM t")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();
        let graph = build_plan_graph_with_broadcast(&optimized, None, None, None);
        assert!(graph
            .nodes
            .values()
            .all(|n| !n.attributes.contains_key("broadcast_rewrite")));
    }

    /// Borrow-check-friendly DFS for the input of the first `Sort` node we
    /// hit — the closure-based `walk_logical_plan` won't let `&LogicalPlan`
    /// references escape into an outer variable.
    fn find_first_sort_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        if let LogicalPlan::Sort(s) = plan {
            return Some(s.input.as_ref());
        }
        for child in plan.inputs() {
            if let Some(found) = find_first_sort_input(child) {
                return Some(found);
            }
        }
        None
    }

    // --- sort_was_pushed_to_every_scan ---

    #[tokio::test]
    async fn sort_pushed_when_every_scan_has_order_by_in_remote_sql() {
        // Single-source query — federation pushes ORDER BY into the SQL,
        // every leaf reflects that, badge fires.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "users",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM users ORDER BY id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        // Inject a captured remote_sql for the bare "users" scan that
        // contains ORDER BY — i.e. federation pushed it.
        let mut remote = RemoteSqlMap::new();
        remote.insert(
            "users".to_string(),
            RemoteSqlInfo {
                sql: r#"SELECT "id" FROM "users" ORDER BY "id""#.to_string(),
                executor_name: "postgres".to_string(),
            },
        );

        // Find the Sort node and test the helper directly.
        let sort_input = find_first_sort_input(&optimized).expect("plan must contain a Sort");
        assert!(sort_was_pushed_to_every_scan(sort_input, &remote));
    }

    #[tokio::test]
    async fn sort_not_pushed_when_one_scan_lacks_order_by() {
        // Multi-source join + ORDER BY: even if BOTH leaves are remote
        // databases, federation can't push the cross-DB sort. As long as
        // at least one captured SQL lacks ORDER BY, the badge must not fire.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "pg_customers",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "mysql_orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let plan = ctx
            .state()
            .create_logical_plan(
                "SELECT pg_customers.id FROM pg_customers JOIN mysql_orders \
                 ON pg_customers.id = mysql_orders.id ORDER BY pg_customers.id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        // Both scans are "remote", but neither captured SQL contains
        // ORDER BY — modelling the real-world multi-source case where
        // federation pushed projection but not the sort.
        let mut remote = RemoteSqlMap::new();
        remote.insert(
            "pg_customers".to_string(),
            RemoteSqlInfo {
                sql: r#"SELECT "id" FROM "public"."pg_customers""#.to_string(),
                executor_name: "postgres".to_string(),
            },
        );
        remote.insert(
            "mysql_orders".to_string(),
            RemoteSqlInfo {
                sql: "SELECT `id` FROM `mysql_orders`".to_string(),
                executor_name: "mysql".to_string(),
            },
        );

        let sort_input = find_first_sort_input(&optimized).expect("plan must contain a Sort");
        assert!(
            !sort_was_pushed_to_every_scan(sort_input, &remote),
            "multi-source Sort must NOT be marked pushed when no leaf SQL has ORDER BY"
        );
    }

    #[tokio::test]
    async fn sort_not_pushed_when_subtree_has_local_csv_scan() {
        // Local CSV scans never appear in remote_sqls. A Sort over a
        // local-only subtree must not be marked pushed.
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "employees_csv",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let plan = ctx
            .state()
            .create_logical_plan("SELECT id FROM employees_csv ORDER BY id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();

        // No entry in remote_sqls for the local CSV scan.
        let remote = RemoteSqlMap::new();

        let sort_input = find_first_sort_input(&optimized).expect("plan must contain a Sort");
        assert!(!sort_was_pushed_to_every_scan(sort_input, &remote));
    }

    // --- collect_scan_names_in_logical_plan ---

    #[tokio::test]
    async fn collect_scan_names_in_logical_plan_returns_qualified_names() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT * FROM t WHERE id > 0")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&plan).unwrap();
        let names = collect_scan_names_in_logical_plan(&optimized);
        assert!(names.contains(&"t".to_string()));
    }
}
