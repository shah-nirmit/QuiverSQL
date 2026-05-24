use datafusion::common::TableReference;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan};
use qsql_connectors::mysql::MySqlTableProvider;
use qsql_connectors::postgres::PostgresTableProvider;
use qsql_connectors::sql::{native_select_all_sql, SqlDialectKind, SqlTableRef};
use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_connectors::RemoteConnector;
use qsql_core::broadcast::BroadcastRewriteInfo;
use qsql_core::models::{PlanGraph, PlanMetrics, PlanNode};
use qsql_core::GuardedTableProvider;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::DatabaseRegistration;

pub const MAX_PLAN_NODES: usize = 500;
pub const RAW_PLAN_TEXT_LIMIT: usize = 50_000;
const SQLITE_EXPLAIN_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_EXPLAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub fn build_plan_graph(plan: &LogicalPlan) -> PlanGraph {
    build_plan_graph_with_broadcast(plan, None)
}

/// Variant that stamps Filter nodes synthesized by the broadcast-join rewrite
/// with `broadcast_rewrite=true` and `broadcast_predicate_value_count=<n>`
/// attributes so the VS Code plan webview can render a badge on them.
pub fn build_plan_graph_with_broadcast(
    plan: &LogicalPlan,
    broadcast: Option<&BroadcastRewriteInfo>,
) -> PlanGraph {
    let mut nodes = HashMap::new();
    let mut truncated = false;
    let targets = broadcast_filter_targets(broadcast);
    let root_id = traverse_plan(plan, &mut nodes, &mut 0, &mut truncated, &targets);

    PlanGraph {
        root_ids: vec![root_id],
        node_count: nodes.len(),
        nodes,
        truncated,
    }
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
    broadcast_targets: &HashSet<String>,
) -> String {
    let current_id = format!("df_{}", id_counter);
    *id_counter += 1;

    if nodes.len() >= MAX_PLAN_NODES {
        *truncated = true;
        return current_id;
    }

    let label = format!("{}", plan.display());
    let mut attributes = plan_attributes(plan);
    if let LogicalPlan::Filter(filter) = plan {
        if let Some(target) = matched_broadcast_target(&filter.predicate, broadcast_targets) {
            attributes.insert("broadcast_rewrite".to_string(), "true".to_string());
            attributes.insert("broadcast_predicate_column".to_string(), target);
            if let Expr::InList(in_list) = &filter.predicate {
                attributes.insert(
                    "broadcast_predicate_value_count".to_string(),
                    in_list.list.len().to_string(),
                );
            }
        }
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
    if let LogicalPlan::TableScan(scan) = plan {
        let tname = qualified_table_name(&scan.table_name);
        source_ref = Some(tname.clone());
        native_plan_ref = Some(tname);
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
        },
    );

    let mut children = Vec::new();
    for child in plan.inputs() {
        if nodes.len() >= MAX_PLAN_NODES {
            *truncated = true;
            break;
        }
        let child_id = traverse_plan(child, nodes, id_counter, truncated, broadcast_targets);
        if nodes.contains_key(&child_id) {
            children.push(child_id);
        }
    }
    if let Some(node) = nodes.get_mut(&current_id) {
        node.children = children;
    }

    current_id
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
    if raw.len() <= RAW_PLAN_TEXT_LIMIT {
        return (raw.to_string(), None);
    }

    (
        format!(
            "{}\n\n... [TRUNCATED - PLAN TEXT EXCEEDED {RAW_PLAN_TEXT_LIMIT} BYTES] ...",
            &raw[..RAW_PLAN_TEXT_LIMIT]
        ),
        Some(format!(
            "Raw plan text exceeded {RAW_PLAN_TEXT_LIMIT} bytes and was truncated."
        )),
    )
}

pub(crate) async fn extract_source_plans(
    plan: &LogicalPlan,
    registrations: &HashMap<String, Arc<DatabaseRegistration>>,
) -> HashMap<String, serde_json::Value> {
    let mut source_plans = HashMap::new();
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans);

    for scan in scans {
        let table_name = qualified_table_name(&scan.table_name);
        if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
            let provider_arc = unguarded_table_provider(source);
            let provider = provider_arc.as_any();

            if let Some(sqlite) = provider.downcast_ref::<SqliteTableProvider>() {
                let sql = sqlite.native_select_sql();
                if let Ok(explain) =
                    explain_with_timeout(sqlite.connector().as_ref(), &sql, SQLITE_EXPLAIN_TIMEOUT)
                        .await
                {
                    source_plans.insert(table_name, serde_json::Value::String(explain));
                }
            } else if let Some(pg) = provider.downcast_ref::<PostgresTableProvider>() {
                let sql = pg.native_select_sql();
                if let Ok(explain) =
                    explain_with_timeout(pg.connector().as_ref(), &sql, REMOTE_EXPLAIN_TIMEOUT)
                        .await
                {
                    let parsed: serde_json::Value = serde_json::from_str(&explain)
                        .unwrap_or_else(|_| serde_json::Value::String(explain.clone()));
                    source_plans.insert(table_name, parsed);
                }
            } else if let Some(my) = provider.downcast_ref::<MySqlTableProvider>() {
                let sql = my.native_select_sql();
                if let Ok(explain) =
                    explain_with_timeout(my.connector().as_ref(), &sql, REMOTE_EXPLAIN_TIMEOUT)
                        .await
                {
                    let parsed: serde_json::Value = serde_json::from_str(&explain)
                        .unwrap_or_else(|_| serde_json::Value::String(explain.clone()));
                    source_plans.insert(table_name, parsed);
                }
            } else if let Some((alias, table)) = table_alias_and_name(&scan.table_name) {
                if let Some(registration) = registrations.get(alias.as_str()) {
                    if let Some(plan) =
                        explain_registered_database_scan(registration, table.as_str(), scan).await
                    {
                        source_plans.insert(table_name, plan);
                    }
                }
            }
        }
    }

    source_plans
}

async fn explain_registered_database_scan(
    registration: &DatabaseRegistration,
    table: &str,
    _scan: &TableScan,
) -> Option<serde_json::Value> {
    match registration.kind {
        qsql_core::models::SourceKind::Sqlite => {
            let db_path = registration.db_path.as_ref()?;
            let table_ref = SqlTableRef::bare(table.to_string());
            let sql = native_select_all_sql(&table_ref, SqlDialectKind::Sqlite);
            let connector = qsql_connectors::sqlite::SqliteConnector::new(db_path);
            explain_with_timeout(&connector, &sql, SQLITE_EXPLAIN_TIMEOUT)
                .await
                .ok()
                .map(serde_json::Value::String)
        }
        qsql_core::models::SourceKind::Postgres => {
            let connection_string = registration.connection_string.as_ref()?;
            let schema_name = registration.schema.as_deref().unwrap_or("public");
            let table_ref = SqlTableRef::with_schema(schema_name.to_string(), table.to_string());
            let sql = native_select_all_sql(&table_ref, SqlDialectKind::Postgres);
            let connector = qsql_connectors::postgres::PostgresConnector::new(connection_string);
            explain_with_timeout(&connector, &sql, REMOTE_EXPLAIN_TIMEOUT)
                .await
                .ok()
                .map(|explain| {
                    serde_json::from_str::<serde_json::Value>(&explain)
                        .unwrap_or(serde_json::Value::String(explain))
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
            let sql = native_select_all_sql(&table_ref, dialect);
            let connector = qsql_connectors::mysql::MySqlConnector::new(connection_string, dialect);
            explain_with_timeout(&connector, &sql, REMOTE_EXPLAIN_TIMEOUT)
                .await
                .ok()
                .map(|explain| {
                    serde_json::from_str::<serde_json::Value>(&explain)
                        .unwrap_or(serde_json::Value::String(explain))
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

fn unguarded_table_provider(source: &DefaultTableSource) -> Arc<dyn TableProvider> {
    if let Some(guarded) = source
        .table_provider
        .as_any()
        .downcast_ref::<GuardedTableProvider>()
    {
        return guarded.inner();
    }

    Arc::clone(&source.table_provider)
}

pub(crate) fn qualified_table_name(table_ref: &TableReference) -> String {
    match table_ref {
        TableReference::Bare { table } => table.to_string(),
        TableReference::Partial { schema, table } => format!("{schema}.{table}"),
        TableReference::Full { schema, table, .. } => format!("{schema}.{table}"),
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
            if let Some(fetch) = sort.fetch {
                attributes.insert("limit".to_string(), fetch.to_string());
            }
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
}
