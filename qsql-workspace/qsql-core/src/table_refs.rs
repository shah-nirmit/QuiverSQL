use std::collections::HashSet;

use sqlparser::ast::{
    Expr, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseTableReference {
    pub alias: String,
    pub table_name: String,
}

pub fn extract_database_table_refs(sql: &str) -> Result<Vec<DatabaseTableReference>, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let mut refs = Vec::new();
    let mut seen = HashSet::new();

    for statement in statements {
        collect_statement(&statement, &HashSet::new(), &mut refs, &mut seen);
    }

    Ok(refs)
}

fn collect_statement(
    statement: &Statement,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    if let Statement::Query(query) = statement {
        collect_query(query, ctes, refs, seen);
    }
}

fn collect_query(
    query: &Query,
    inherited_ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    let mut ctes = inherited_ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            ctes.insert(cte.alias.name.value.clone());
        }
        for cte in &with.cte_tables {
            collect_query(&cte.query, &ctes, refs, seen);
        }
    }

    collect_set_expr(&query.body, &ctes, refs, seen);
}

fn collect_set_expr(
    set_expr: &SetExpr,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    match set_expr {
        SetExpr::Select(select) => collect_select(select, ctes, refs, seen),
        SetExpr::Query(query) => collect_query(query, ctes, refs, seen),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr(left, ctes, refs, seen);
            collect_set_expr(right, ctes, refs, seen);
        }
        SetExpr::Table(table) => {
            if let (Some(schema_name), Some(table_name)) = (&table.schema_name, &table.table_name) {
                add_schema_table(schema_name, table_name, ctes, refs, seen);
            }
        }
        _ => {}
    }
}

fn collect_select(
    select: &Select,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    for table in &select.from {
        collect_table_with_joins(table, ctes, refs, seen);
    }
    for item in &select.projection {
        collect_select_item(item, ctes, refs, seen);
    }
    if let Some(selection) = &select.selection {
        collect_expr(selection, ctes, refs, seen);
    }
    if let Some(having) = &select.having {
        collect_expr(having, ctes, refs, seen);
    }
}

fn collect_select_item(
    item: &SelectItem,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    match item {
        SelectItem::UnnamedExpr(expr) => collect_expr(expr, ctes, refs, seen),
        SelectItem::ExprWithAlias { expr, .. } => collect_expr(expr, ctes, refs, seen),
        _ => {}
    }
}

fn collect_expr(
    expr: &Expr,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    match expr {
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expr(expr, ctes, refs, seen);
            collect_query(subquery, ctes, refs, seen);
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            collect_query(subquery, ctes, refs, seen);
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_expr(left, ctes, refs, seen);
            collect_expr(right, ctes, refs, seen);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_expr(expr, ctes, refs, seen),
        Expr::InList { expr, list, .. } => {
            collect_expr(expr, ctes, refs, seen);
            for item in list {
                collect_expr(item, ctes, refs, seen);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr(expr, ctes, refs, seen);
            collect_expr(low, ctes, refs, seen);
            collect_expr(high, ctes, refs, seen);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_expr(expr, ctes, refs, seen);
            collect_expr(pattern, ctes, refs, seen);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr(operand, ctes, refs, seen);
            }
            for condition in conditions {
                collect_expr(&condition.condition, ctes, refs, seen);
                collect_expr(&condition.result, ctes, refs, seen);
            }
            if let Some(else_result) = else_result {
                collect_expr(else_result, ctes, refs, seen);
            }
        }
        Expr::Tuple(exprs) => {
            for expr in exprs {
                collect_expr(expr, ctes, refs, seen);
            }
        }
        _ => {}
    }
}

fn collect_table_with_joins(
    table: &TableWithJoins,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    collect_table_factor(&table.relation, ctes, refs, seen);
    for join in &table.joins {
        collect_table_factor(&join.relation, ctes, refs, seen);
    }
}

fn collect_table_factor(
    factor: &TableFactor,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    match factor {
        TableFactor::Table { name, .. } => add_object_name(name, ctes, refs, seen),
        TableFactor::Derived { subquery, .. } => collect_query(subquery, ctes, refs, seen),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_table_with_joins(table_with_joins, ctes, refs, seen),
        _ => {}
    }
}

fn add_object_name(
    name: &ObjectName,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    let parts = name
        .0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>();

    if parts.len() != 2 || ctes.contains(&parts[0]) {
        return;
    }

    add_schema_table(&parts[0], &parts[1], ctes, refs, seen);
}

fn add_schema_table(
    schema_name: &str,
    table_name: &str,
    ctes: &HashSet<String>,
    refs: &mut Vec<DatabaseTableReference>,
    seen: &mut HashSet<(String, String)>,
) {
    if ctes.contains(schema_name) {
        return;
    }

    let key = (schema_name.to_string(), table_name.to_string());
    if seen.insert(key.clone()) {
        refs.push(DatabaseTableReference {
            alias: key.0,
            table_name: key.1,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_qualified_tables_from_joins_and_subqueries() {
        let refs = extract_database_table_refs(
            "SELECT * FROM pg_local.customers c \
             JOIN mysql_local.orders o ON c.id = o.customer_id \
             WHERE EXISTS (SELECT 1 FROM pg_local.regions r)",
        )
        .unwrap();

        assert_eq!(
            refs,
            vec![
                DatabaseTableReference {
                    alias: "pg_local".to_string(),
                    table_name: "customers".to_string()
                },
                DatabaseTableReference {
                    alias: "mysql_local".to_string(),
                    table_name: "orders".to_string()
                },
                DatabaseTableReference {
                    alias: "pg_local".to_string(),
                    table_name: "regions".to_string()
                }
            ]
        );
    }

    #[test]
    fn ignores_cte_references_but_extracts_cte_sources() {
        let refs = extract_database_table_refs(
            "WITH recent AS (SELECT * FROM pg_local.orders) \
             SELECT * FROM recent JOIN mysql_local.customers c ON recent.customer_id = c.id",
        )
        .unwrap();

        assert_eq!(
            refs,
            vec![
                DatabaseTableReference {
                    alias: "pg_local".to_string(),
                    table_name: "orders".to_string()
                },
                DatabaseTableReference {
                    alias: "mysql_local".to_string(),
                    table_name: "customers".to_string()
                }
            ]
        );
    }

    #[test]
    fn union_walks_both_sides() {
        let refs = extract_database_table_refs(
            "SELECT * FROM pg_local.customers \
             UNION ALL \
             SELECT * FROM mysql_local.customers",
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs.iter().any(|r| r.alias == "pg_local"));
        assert!(refs.iter().any(|r| r.alias == "mysql_local"));
    }

    #[test]
    fn intersect_and_except_walk_both_sides() {
        let refs_intersect = extract_database_table_refs(
            "SELECT id FROM pg_local.a INTERSECT SELECT id FROM pg_local.b",
        )
        .unwrap();
        assert_eq!(refs_intersect.len(), 2);

        let refs_except = extract_database_table_refs(
            "SELECT id FROM pg_local.a EXCEPT SELECT id FROM pg_local.b",
        )
        .unwrap();
        assert_eq!(refs_except.len(), 2);
    }

    #[test]
    fn subquery_in_select_list_is_extracted() {
        let refs = extract_database_table_refs(
            "SELECT (SELECT MAX(rev) FROM db.revenue) AS max_rev FROM db.customers",
        )
        .unwrap();
        let names: Vec<&str> = refs.iter().map(|r| r.table_name.as_str()).collect();
        assert!(names.contains(&"customers"));
        assert!(names.contains(&"revenue"));
    }

    #[test]
    fn in_subquery_is_extracted() {
        let refs = extract_database_table_refs(
            "SELECT * FROM pg.orders WHERE customer_id IN (SELECT id FROM pg.customers)",
        )
        .unwrap();
        let names: Vec<&str> = refs.iter().map(|r| r.table_name.as_str()).collect();
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"customers"));
    }

    #[test]
    fn same_table_referenced_twice_is_deduplicated() {
        let refs = extract_database_table_refs(
            "SELECT a.id, b.id FROM pg.orders a JOIN pg.orders b ON a.id = b.parent_id",
        )
        .unwrap();
        assert_eq!(refs.len(), 1, "same schema.table should appear only once");
    }

    #[test]
    fn bare_table_name_produces_no_refs() {
        let refs = extract_database_table_refs("SELECT * FROM customers").unwrap();
        assert!(refs.is_empty(), "bare table names are not tracked");
    }

    #[test]
    fn three_part_name_produces_no_refs() {
        let refs = extract_database_table_refs("SELECT * FROM catalog.schema.table").unwrap();
        assert!(refs.is_empty(), "3-part names are not tracked");
    }

    #[test]
    fn invalid_sql_returns_error() {
        assert!(extract_database_table_refs("NOT VALID SQL !!!").is_err());
    }

    #[test]
    fn derived_subquery_in_from_is_extracted() {
        let refs =
            extract_database_table_refs("SELECT * FROM (SELECT * FROM pg.orders) sub").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].table_name, "orders");
    }

    #[test]
    fn exists_subquery_is_extracted() {
        let refs = extract_database_table_refs(
            "SELECT * FROM db.a WHERE EXISTS (SELECT 1 FROM db.b WHERE db.b.id = db.a.id)",
        )
        .unwrap();
        let names: Vec<&str> = refs.iter().map(|r| r.table_name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
