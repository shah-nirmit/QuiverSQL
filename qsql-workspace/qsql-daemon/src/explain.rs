use datafusion::common::TableReference;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use qsql_connectors::mysql::MySqlTableProvider;
use qsql_connectors::postgres::PostgresTableProvider;
use qsql_connectors::sqlite::SqliteTableProvider;
use qsql_connectors::RemoteConnector;
use qsql_core::models::{PlanGraph, PlanMetrics, PlanNode};
use std::collections::HashMap;

pub fn build_plan_graph(plan: &LogicalPlan) -> PlanGraph {
    let mut nodes = HashMap::new();
    let root_id = traverse_plan(plan, &mut nodes, &mut 0);

    PlanGraph {
        root_ids: vec![root_id],
        node_count: nodes.len(),
        nodes,
        truncated: false,
    }
}

fn traverse_plan(
    plan: &LogicalPlan,
    nodes: &mut HashMap<String, PlanNode>,
    id_counter: &mut usize,
) -> String {
    let current_id = format!("df_{}", id_counter);
    *id_counter += 1;

    let mut children = Vec::new();
    for child in plan.inputs() {
        let child_id = traverse_plan(child, nodes, id_counter);
        children.push(child_id);
    }

    let label = format!("{}", plan.display());
    let attributes = plan_attributes(plan);

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
            children,
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

    current_id
}

pub async fn extract_source_plans(plan: &LogicalPlan) -> HashMap<String, serde_json::Value> {
    let mut source_plans = HashMap::new();
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans);

    for scan in scans {
        let table_name = qualified_table_name(&scan.table_name);
        if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
            let provider = source.table_provider.as_any();

            if let Some(sqlite) = provider.downcast_ref::<SqliteTableProvider>() {
                let sql = sqlite
                    .build_select_sql(scan.projection.as_ref(), &scan.filters, None)
                    .map(|r| r.sql)
                    .unwrap_or_else(|_| "SELECT *".to_string());
                if let Ok(explain) = sqlite.connector().explain_query(&sql).await {
                    source_plans.insert(table_name, serde_json::Value::String(explain));
                }
            } else if let Some(pg) = provider.downcast_ref::<PostgresTableProvider>() {
                let sql = pg
                    .build_select_sql(scan.projection.as_ref(), &scan.filters, None)
                    .map(|r| r.sql)
                    .unwrap_or_else(|_| "SELECT *".to_string());
                if let Ok(explain) = pg.connector().explain_query(&sql).await {
                    let parsed: serde_json::Value = serde_json::from_str(&explain)
                        .unwrap_or_else(|_| serde_json::Value::String(explain.clone()));
                    source_plans.insert(table_name, parsed);
                }
            } else if let Some(my) = provider.downcast_ref::<MySqlTableProvider>() {
                let sql = my
                    .build_select_sql(scan.projection.as_ref(), &scan.filters, None)
                    .map(|r| r.sql)
                    .unwrap_or_else(|_| "SELECT *".to_string());
                if let Ok(explain) = my.connector().explain_query(&sql).await {
                    let parsed: serde_json::Value = serde_json::from_str(&explain)
                        .unwrap_or_else(|_| serde_json::Value::String(explain.clone()));
                    source_plans.insert(table_name, parsed);
                }
            }
        }
    }

    source_plans
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
}
