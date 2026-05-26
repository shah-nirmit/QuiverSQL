//! Bounded broadcast-join rewrite for small-local-side to federated-fact joins.
//!
//! Detects inner equi-joins where exactly one side is local (CSV, Parquet,
//! in-memory, etc.) and the other side reads through a [`GuardedTableProvider`]
//! (federated/remote source). When the local side's distinct join keys fit
//! within the configured caps, materializes those keys and injects an
//! `<remote_key> IN (...)` filter above the remote scan. DataFusion's existing
//! predicate pushdown plus the federation rewrite let the source-native SQL
//! pick up the narrowed predicate.
//!
//! Not a physical broadcast (no `BroadcastExchange`), not cost-based, and not
//! multi-key/outer/anti/semi. Single inner equi-join only. Multi-key support
//! would multiply the candidate-value space and deserves a separate design.
//!
//! Runs as an async pass after DataFusion's logical optimization and before
//! physical planning. The engine re-runs `optimize` when [`BroadcastRewriteInfo::applied`]
//! is non-empty so the injected filter participates in another pushdown round.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    Expr, Extension, Join, JoinType, LogicalPlan, LogicalPlanBuilder, TableScan,
};
use datafusion::prelude::SessionContext;
use tokio_util::sync::CancellationToken;

use crate::engine::GuardedTableProvider;

/// Default upper bound on the number of distinct local-side join keys we will
/// materialize before falling back to the un-rewritten plan.
pub const DEFAULT_MAX_LOCAL_ROWS: usize = 10_000;
pub const DEFAULT_MAX_LOCAL_BYTES: usize = 8 * 1024 * 1024;

pub fn get_max_local_rows() -> usize {
    crate::models::get_env_usize("QSQL_MAX_LOCAL_ROWS", DEFAULT_MAX_LOCAL_ROWS)
}

pub fn get_max_local_bytes() -> usize {
    crate::models::get_env_usize("QSQL_MAX_LOCAL_BYTES", DEFAULT_MAX_LOCAL_BYTES)
}

/// Knobs controlling the rewrite. Default settings are tuned for "join my CSV
/// to a filtered Postgres dimension"-shaped queries; tests/benches pass
/// [`Self::disabled`] to compare against the un-rewritten baseline.
#[derive(Debug, Clone)]
pub struct BroadcastRewriteConfig {
    pub enabled: bool,
    pub max_local_rows: usize,
    pub max_local_bytes: usize,
}

impl Default for BroadcastRewriteConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_local_rows: get_max_local_rows(),
            max_local_bytes: get_max_local_bytes(),
        }
    }
}

impl BroadcastRewriteConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }
}

/// Summary of what the rewrite pass did. Always returned; an empty
/// `applied`/`skipped` pair means no joins were inspected (or the pass was
/// disabled).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastRewriteInfo {
    pub considered: usize,
    pub applied: Vec<BroadcastApplication>,
    pub skipped: Vec<BroadcastSkip>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastApplication {
    pub local_table: String,
    pub remote_table: String,
    pub join_key_local: String,
    pub join_key_remote: String,
    pub local_rows_materialized: usize,
    pub local_bytes_materialized: usize,
    pub predicate_value_count: usize,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSkip {
    pub reason: SkipReason,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SkipReason {
    NotInnerEquiJoin,
    NoFederatedSide,
    BothSidesFederated,
    NonColumnJoinKey,
    UnsupportedKeyType,
    LocalSideMaterializationExceededCap,
    LocalSideMaterializationError,
    CancellationRequested,
}

/// Side classification used during plan walking. A subtree is `Federated` if
/// any reachable `TableScan` is backed by a `GuardedTableProvider`; otherwise
/// it is `Local`. Mixed subtrees (e.g., the upper side of another join) are
/// classified by the same rule and treated as `Federated` if they touch any
/// remote source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SideKind {
    Local,
    Federated,
}

/// Entry point: walk the optimized logical plan and rewrite eligible joins in
/// place. Returns the rewritten plan plus a structured account of what
/// happened. On cancellation, returns the partially-rewritten plan and a
/// `CancellationRequested` skip entry — never errors the whole query because
/// the rewrite stalled.
pub async fn apply_broadcast_rewrites(
    ctx: &SessionContext,
    plan: LogicalPlan,
    config: &BroadcastRewriteConfig,
    cancellation: CancellationToken,
) -> Result<(LogicalPlan, BroadcastRewriteInfo), DataFusionError> {
    let mut info = BroadcastRewriteInfo::default();
    if !config.enabled {
        return Ok((plan, info));
    }
    let rewritten = rewrite_node(ctx, plan, config, &cancellation, &mut info).await?;
    Ok((rewritten, info))
}

/// Async recursion needs explicit boxing — the futures aren't Send-by-default
/// inside an async fn that calls itself. Box::pin gives us a uniform shape.
fn rewrite_node<'a>(
    ctx: &'a SessionContext,
    plan: LogicalPlan,
    config: &'a BroadcastRewriteConfig,
    cancellation: &'a CancellationToken,
    info: &'a mut BroadcastRewriteInfo,
) -> Pin<Box<dyn std::future::Future<Output = Result<LogicalPlan, DataFusionError>> + Send + 'a>> {
    Box::pin(async move {
        // Recurse into children first so nested joins get a chance to rewrite
        // independently. The natural bottom-up order also means the parent
        // join sees already-rewritten subplans when it inspects sides.
        let plan = recurse_into_children(ctx, plan, config, cancellation, info).await?;

        match &plan {
            LogicalPlan::Join(join) => try_rewrite_join(ctx, join, config, cancellation, info)
                .await
                .map(|maybe| maybe.unwrap_or(plan)),
            _ => Ok(plan),
        }
    })
}

async fn recurse_into_children(
    ctx: &SessionContext,
    plan: LogicalPlan,
    config: &BroadcastRewriteConfig,
    cancellation: &CancellationToken,
    info: &mut BroadcastRewriteInfo,
) -> Result<LogicalPlan, DataFusionError> {
    let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
    if inputs.is_empty() {
        return Ok(plan);
    }

    let mut new_inputs = Vec::with_capacity(inputs.len());
    for child in inputs {
        let rewritten = rewrite_node(ctx, child, config, cancellation, info).await?;
        new_inputs.push(rewritten);
    }
    plan.with_new_exprs(plan.expressions(), new_inputs)
}

async fn try_rewrite_join(
    ctx: &SessionContext,
    join: &Join,
    config: &BroadcastRewriteConfig,
    cancellation: &CancellationToken,
    info: &mut BroadcastRewriteInfo,
) -> Result<Option<LogicalPlan>, DataFusionError> {
    info.considered += 1;

    if join.join_type != JoinType::Inner {
        info.skipped.push(BroadcastSkip {
            reason: SkipReason::NotInnerEquiJoin,
            detail: format!("join_type = {:?}", join.join_type),
        });
        return Ok(None);
    }

    // MVP: exactly one equi-key. Multi-key would Cartesian-expand the IN list.
    if join.on.len() != 1 {
        info.skipped.push(BroadcastSkip {
            reason: SkipReason::NotInnerEquiJoin,
            detail: format!("expected 1 equi-key, found {}", join.on.len()),
        });
        return Ok(None);
    }
    // Extra non-equi predicates (the `filter` field on `Join`) are fine — we
    // keep them in place; they just don't drive the rewrite.

    let (left_key_expr, right_key_expr) = &join.on[0];
    let left_col = match column_of(left_key_expr) {
        Some(c) => c,
        None => {
            info.skipped.push(BroadcastSkip {
                reason: SkipReason::NonColumnJoinKey,
                detail: format!("left key is not a column: {left_key_expr}"),
            });
            return Ok(None);
        }
    };
    let right_col = match column_of(right_key_expr) {
        Some(c) => c,
        None => {
            info.skipped.push(BroadcastSkip {
                reason: SkipReason::NonColumnJoinKey,
                detail: format!("right key is not a column: {right_key_expr}"),
            });
            return Ok(None);
        }
    };

    let left_kind = classify_side(&join.left);
    let right_kind = classify_side(&join.right);

    let (local_input, remote_input, local_col, remote_col, local_is_left) =
        match (left_kind, right_kind) {
            (SideKind::Local, SideKind::Federated) => {
                (&join.left, &join.right, &left_col, &right_col, true)
            }
            (SideKind::Federated, SideKind::Local) => {
                (&join.right, &join.left, &right_col, &left_col, false)
            }
            (SideKind::Local, SideKind::Local) => {
                info.skipped.push(BroadcastSkip {
                    reason: SkipReason::NoFederatedSide,
                    detail: "both sides are local; let DataFusion handle the join".to_string(),
                });
                return Ok(None);
            }
            (SideKind::Federated, SideKind::Federated) => {
                info.skipped.push(BroadcastSkip {
                    reason: SkipReason::BothSidesFederated,
                    detail: "both sides touch federated sources; broadcast not yet supported"
                        .to_string(),
                });
                return Ok(None);
            }
        };

    // Materialize DISTINCT local_col with LIMIT max_local_rows + 1. Overflow
    // detection is single-row past the cap, not a full second pass.
    let started = Instant::now();
    let materialized =
        match materialize_distinct_keys(ctx, local_input.as_ref(), local_col, config, cancellation)
            .await
        {
            Materialization::Values {
                batches,
                rows,
                bytes,
            } => (batches, rows, bytes),
            Materialization::Cancelled => {
                info.skipped.push(BroadcastSkip {
                    reason: SkipReason::CancellationRequested,
                    detail: "rewrite cancelled before local side materialized".to_string(),
                });
                return Ok(None);
            }
            Materialization::ExceededCap { detail } => {
                info.skipped.push(BroadcastSkip {
                    reason: SkipReason::LocalSideMaterializationExceededCap,
                    detail,
                });
                return Ok(None);
            }
            Materialization::Error { detail } => {
                info.skipped.push(BroadcastSkip {
                    reason: SkipReason::LocalSideMaterializationError,
                    detail,
                });
                return Ok(None);
            }
        };
    let (batches, local_rows, local_bytes) = materialized;

    // Empty local side: the inner join must produce zero rows. Short-circuit
    // before the key-type check — an empty CSV with no data has schema fields
    // typed as Null, which we'd otherwise reject as unsupported even though
    // we don't actually need the type for an empty IN-list.
    if local_rows == 0 {
        let empty = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: join.schema.clone(),
        });
        info.applied.push(BroadcastApplication {
            local_table: describe_subtree(local_input),
            remote_table: describe_subtree(remote_input),
            join_key_local: local_col.flat_name(),
            join_key_remote: remote_col.flat_name(),
            local_rows_materialized: 0,
            local_bytes_materialized: 0,
            predicate_value_count: 0,
            elapsed_ms: started.elapsed().as_millis() as u64,
        });
        return Ok(Some(empty));
    }

    // Derive the key type from the local subplan's schema rather than the
    // materialized batches — different batches in a partitioned local source
    // could in principle disagree, and the schema is authoritative.
    let key_data_type = match local_input.schema().qualified_field_from_column(local_col) {
        Ok((_, field)) => field.data_type().clone(),
        Err(err) => {
            info.skipped.push(BroadcastSkip {
                reason: SkipReason::LocalSideMaterializationError,
                detail: format!("local subplan schema lookup failed: {err}"),
            });
            return Ok(None);
        }
    };

    if !is_supported_key_type(&key_data_type) {
        info.skipped.push(BroadcastSkip {
            reason: SkipReason::UnsupportedKeyType,
            detail: format!("join key type {key_data_type} not in broadcast support set"),
        });
        return Ok(None);
    }

    let distinct_values = match collect_distinct_scalars(&batches, &key_data_type) {
        Ok(v) => v,
        Err(err) => {
            info.skipped.push(BroadcastSkip {
                reason: SkipReason::LocalSideMaterializationError,
                detail: format!("failed to extract scalar values: {err}"),
            });
            return Ok(None);
        }
    };

    // All materialized values were NULL — equivalent to empty for an inner
    // equi-join (NULLs don't match anything). Substitute EmptyRelation.
    if distinct_values.is_empty() {
        let empty = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: join.schema.clone(),
        });
        info.applied.push(BroadcastApplication {
            local_table: describe_subtree(local_input),
            remote_table: describe_subtree(remote_input),
            join_key_local: local_col.flat_name(),
            join_key_remote: remote_col.flat_name(),
            local_rows_materialized: local_rows,
            local_bytes_materialized: local_bytes,
            predicate_value_count: 0,
            elapsed_ms: started.elapsed().as_millis() as u64,
        });
        return Ok(Some(empty));
    }

    let in_list = Expr::InList(InList {
        expr: Box::new(Expr::Column(remote_col.clone())),
        list: distinct_values
            .iter()
            .map(|v| Expr::Literal(v.clone(), None))
            .collect(),
        negated: false,
    });

    let predicate_count = distinct_values.len();

    let filtered_remote = LogicalPlanBuilder::from(remote_input.as_ref().clone())
        .filter(in_list)?
        .build()?;

    // Rebuild the join with the filtered remote side in the original position.
    let new_join = if local_is_left {
        Join {
            left: join.left.clone(),
            right: Arc::new(filtered_remote),
            on: join.on.clone(),
            filter: join.filter.clone(),
            join_type: join.join_type,
            join_constraint: join.join_constraint,
            schema: join.schema.clone(),
            null_equality: join.null_equality,
            null_aware: join.null_aware,
        }
    } else {
        Join {
            left: Arc::new(filtered_remote),
            right: join.right.clone(),
            on: join.on.clone(),
            filter: join.filter.clone(),
            join_type: join.join_type,
            join_constraint: join.join_constraint,
            schema: join.schema.clone(),
            null_equality: join.null_equality,
            null_aware: join.null_aware,
        }
    };

    info.applied.push(BroadcastApplication {
        local_table: describe_subtree(local_input),
        remote_table: describe_subtree(remote_input),
        join_key_local: local_col.flat_name(),
        join_key_remote: remote_col.flat_name(),
        local_rows_materialized: local_rows,
        local_bytes_materialized: local_bytes,
        predicate_value_count: predicate_count,
        elapsed_ms: started.elapsed().as_millis() as u64,
    });

    Ok(Some(LogicalPlan::Join(new_join)))
}

enum Materialization {
    Values {
        batches: Vec<RecordBatch>,
        rows: usize,
        bytes: usize,
    },
    Cancelled,
    ExceededCap {
        detail: String,
    },
    Error {
        detail: String,
    },
}

async fn materialize_distinct_keys(
    ctx: &SessionContext,
    local_subplan: &LogicalPlan,
    local_col: &Column,
    config: &BroadcastRewriteConfig,
    cancellation: &CancellationToken,
) -> Materialization {
    let key_only = match LogicalPlanBuilder::from(local_subplan.clone())
        .project(vec![Expr::Column(local_col.clone())])
        .and_then(|b| b.distinct())
        .and_then(|b| b.limit(0, Some(config.max_local_rows + 1)))
        .and_then(|b| b.build())
    {
        Ok(plan) => plan,
        Err(err) => {
            return Materialization::Error {
                detail: format!("failed to build key-extraction plan: {err}"),
            };
        }
    };

    let df = match ctx.execute_logical_plan(key_only).await {
        Ok(df) => df,
        Err(err) => {
            return Materialization::Error {
                detail: format!("failed to plan key extraction: {err}"),
            };
        }
    };

    let collect_fut = df.collect();
    let batches = tokio::select! {
        _ = cancellation.cancelled() => return Materialization::Cancelled,
        res = collect_fut => match res {
            Ok(b) => b,
            Err(err) => return Materialization::Error {
                detail: format!("collect failed: {err}"),
            },
        },
    };

    let mut rows = 0usize;
    let mut bytes = 0usize;
    for b in &batches {
        rows = rows.saturating_add(b.num_rows());
        bytes = bytes.saturating_add(b.get_array_memory_size());
    }
    if rows > config.max_local_rows {
        return Materialization::ExceededCap {
            detail: format!(
                "local side produced {} rows, exceeds max_local_rows={}",
                rows, config.max_local_rows
            ),
        };
    }
    if bytes > config.max_local_bytes {
        return Materialization::ExceededCap {
            detail: format!(
                "local side produced {} bytes, exceeds max_local_bytes={}",
                bytes, config.max_local_bytes
            ),
        };
    }

    Materialization::Values {
        batches,
        rows,
        bytes,
    }
}

fn collect_distinct_scalars(
    batches: &[RecordBatch],
    expected_ty: &DataType,
) -> Result<Vec<ScalarValue>, DataFusionError> {
    let mut out: Vec<ScalarValue> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for batch in batches {
        let array = batch.column(0);
        for row in 0..array.len() {
            let scalar = ScalarValue::try_from_array(array, row)?;
            // DISTINCT in the plan already deduplicated, but we run a second
            // pass to be defensive (the distinct projection could be elided
            // by future optimizer changes — cheap insurance).
            let key = format!("{scalar:?}");
            if seen.insert(key) {
                if scalar.data_type() != *expected_ty {
                    return Err(DataFusionError::Internal(format!(
                        "materialized key type {} did not match expected {}",
                        scalar.data_type(),
                        expected_ty
                    )));
                }
                if !scalar.is_null() {
                    out.push(scalar);
                }
            }
        }
    }
    Ok(out)
}

fn is_supported_key_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Date64
    )
}

fn column_of(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(c) => Some(c.clone()),
        Expr::Cast(cast) => column_of(&cast.expr),
        Expr::Alias(alias) => column_of(&alias.expr),
        _ => None,
    }
}

fn classify_side(plan: &LogicalPlan) -> SideKind {
    if subtree_touches_guarded_provider(plan) {
        SideKind::Federated
    } else {
        SideKind::Local
    }
}

fn subtree_touches_guarded_provider(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::TableScan(scan) = plan {
        if scan_uses_guarded_provider(scan) {
            return true;
        }
    }
    if let LogicalPlan::Extension(Extension { node }) = plan {
        for input in node.inputs() {
            if subtree_touches_guarded_provider(input) {
                return true;
            }
        }
    }
    plan.inputs()
        .iter()
        .any(|child| subtree_touches_guarded_provider(child))
}

fn scan_uses_guarded_provider(scan: &TableScan) -> bool {
    if let Some(default) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
        return provider_is_guarded(default.table_provider.as_ref());
    }
    false
}

fn provider_is_guarded(provider: &dyn TableProvider) -> bool {
    provider
        .as_any()
        .downcast_ref::<GuardedTableProvider>()
        .is_some()
}

fn describe_subtree(plan: &LogicalPlan) -> String {
    let mut tables = Vec::new();
    collect_table_names(plan, &mut tables);
    if tables.is_empty() {
        format!("{}", plan.display())
    } else {
        tables.join(",")
    }
}

fn collect_table_names(plan: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan(scan) = plan {
        out.push(scan.table_name.to_string());
    }
    for child in plan.inputs() {
        collect_table_names(child, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    use crate::engine::{GuardedTableProvider, ScanBudget};

    fn small_local_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    fn small_remote_table() -> Arc<GuardedTableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("bonus", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
            as Arc<dyn TableProvider>;
        Arc::new(GuardedTableProvider::with_budget(
            "test_remote".to_string(),
            mem,
            ScanBudget::default(),
        ))
    }

    async fn build_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_table("locals", small_local_table()).unwrap();
        ctx.register_table("remotes", small_remote_table()).unwrap();
        ctx
    }

    fn cancel_token() -> CancellationToken {
        CancellationToken::new()
    }

    #[tokio::test]
    async fn disabled_config_skips_walk() {
        let ctx = build_ctx().await;
        let logical = ctx.state().create_logical_plan("SELECT 1").await.unwrap();
        let (out, info) = apply_broadcast_rewrites(
            &ctx,
            logical.clone(),
            &BroadcastRewriteConfig::disabled(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert_eq!(info.considered, 0);
        assert!(info.applied.is_empty());
        assert!(info.skipped.is_empty());
        assert_eq!(
            format!("{}", out.display()),
            format!("{}", logical.display())
        );
    }

    #[tokio::test]
    async fn local_join_local_is_skipped_with_no_federated_side() {
        let ctx = build_ctx().await;
        ctx.register_table("locals2", small_local_table()).unwrap();
        let logical = ctx
            .state()
            .create_logical_plan("SELECT a.id FROM locals a JOIN locals2 b ON a.id = b.id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (_out, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert_eq!(info.considered, 1);
        assert_eq!(info.applied.len(), 0);
        assert_eq!(info.skipped.len(), 1);
        assert_eq!(info.skipped[0].reason, SkipReason::NoFederatedSide);
    }

    #[tokio::test]
    async fn local_to_remote_inner_equi_join_is_rewritten() {
        let ctx = build_ctx().await;
        let logical = ctx
            .state()
            .create_logical_plan(
                "SELECT l.id, r.bonus FROM locals l JOIN remotes r ON l.id = r.user_id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (rewritten, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert_eq!(info.considered, 1);
        assert_eq!(info.applied.len(), 1);
        let app = &info.applied[0];
        // local side had 3 distinct ids
        assert_eq!(app.local_rows_materialized, 3);
        assert_eq!(app.predicate_value_count, 3);

        // The rewritten plan should contain a Filter with InList over user_id.
        let rendered = format!("{}", rewritten.display_indent());
        assert!(
            rendered.contains("user_id IN")
                || rendered.contains("Filter") && rendered.contains("user_id"),
            "expected an IN-list filter on user_id, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn cap_exceeded_skips_rewrite() {
        let ctx = SessionContext::new();
        // Local side with 50 distinct keys, cap to 10 rows.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..50).collect::<Vec<_>>()))],
        )
        .unwrap();
        let local = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        ctx.register_table("big_local", local).unwrap();
        ctx.register_table("remotes", small_remote_table()).unwrap();

        let logical = ctx
            .state()
            .create_logical_plan("SELECT b.id FROM big_local b JOIN remotes r ON b.id = r.user_id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let config = BroadcastRewriteConfig {
            enabled: true,
            max_local_rows: 10,
            max_local_bytes: DEFAULT_MAX_LOCAL_BYTES,
        };
        let (_out, info) = apply_broadcast_rewrites(&ctx, optimized, &config, cancel_token())
            .await
            .unwrap();
        assert_eq!(info.considered, 1);
        assert!(info.applied.is_empty());
        assert_eq!(
            info.skipped[0].reason,
            SkipReason::LocalSideMaterializationExceededCap
        );
    }

    #[tokio::test]
    async fn empty_local_side_produces_empty_relation_marked_as_applied() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        ctx.register_table("empty_local", local).unwrap();
        ctx.register_table("remotes", small_remote_table()).unwrap();

        let logical = ctx
            .state()
            .create_logical_plan(
                "SELECT e.id FROM empty_local e JOIN remotes r ON e.id = r.user_id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (rewritten, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert_eq!(info.applied.len(), 1);
        assert_eq!(info.applied[0].predicate_value_count, 0);
        let rendered = format!("{}", rewritten.display_indent());
        assert!(
            rendered.contains("EmptyRelation"),
            "expected EmptyRelation, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn cancellation_before_materialization_skips_with_reason() {
        let ctx = build_ctx().await;
        let logical = ctx
            .state()
            .create_logical_plan("SELECT l.id FROM locals l JOIN remotes r ON l.id = r.user_id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let (_out, info) =
            apply_broadcast_rewrites(&ctx, optimized, &BroadcastRewriteConfig::default(), token)
                .await
                .unwrap();
        assert!(info
            .skipped
            .iter()
            .any(|s| s.reason == SkipReason::CancellationRequested));
    }

    #[tokio::test]
    async fn non_inner_join_is_skipped() {
        let ctx = build_ctx().await;
        let logical = ctx
            .state()
            .create_logical_plan(
                "SELECT l.id, r.bonus FROM locals l LEFT JOIN remotes r ON l.id = r.user_id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (_out, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert!(info
            .skipped
            .iter()
            .any(|s| s.reason == SkipReason::NotInnerEquiJoin));
    }

    #[tokio::test]
    async fn both_sides_federated_is_skipped() {
        let ctx = SessionContext::new();
        ctx.register_table("remote_a", small_remote_table())
            .unwrap();
        ctx.register_table("remote_b", small_remote_table())
            .unwrap();
        let logical = ctx
            .state()
            .create_logical_plan(
                "SELECT a.user_id FROM remote_a a JOIN remote_b b ON a.user_id = b.user_id",
            )
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (_out, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert!(info
            .skipped
            .iter()
            .any(|s| s.reason == SkipReason::BothSidesFederated));
    }

    #[tokio::test]
    async fn federated_local_swap_also_rewrites() {
        // Remote on left, local on right — should still rewrite.
        let ctx = build_ctx().await;
        let logical = ctx
            .state()
            .create_logical_plan("SELECT r.bonus FROM remotes r JOIN locals l ON r.user_id = l.id")
            .await
            .unwrap();
        let optimized = ctx.state().optimize(&logical).unwrap();
        let (_, info) = apply_broadcast_rewrites(
            &ctx,
            optimized,
            &BroadcastRewriteConfig::default(),
            cancel_token(),
        )
        .await
        .unwrap();
        assert_eq!(info.applied.len(), 1);
    }

    #[test]
    fn is_supported_key_type_accepts_common_types() {
        for ty in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Date32,
            DataType::Date64,
        ] {
            assert!(is_supported_key_type(&ty), "{ty:?} should be supported");
        }
        assert!(!is_supported_key_type(&DataType::Binary));
        assert!(!is_supported_key_type(&DataType::Null));
    }

    #[test]
    fn broadcast_rewrite_config_default_and_disabled() {
        let cfg = BroadcastRewriteConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.max_local_rows, DEFAULT_MAX_LOCAL_ROWS);
        assert_eq!(cfg.max_local_bytes, DEFAULT_MAX_LOCAL_BYTES);

        let dis = BroadcastRewriteConfig::disabled();
        assert!(!dis.enabled);
    }

    #[test]
    fn broadcast_rewrite_info_default_is_empty() {
        let info = BroadcastRewriteInfo::default();
        assert_eq!(info.considered, 0);
        assert!(info.applied.is_empty());
        assert!(info.skipped.is_empty());
    }

    #[test]
    fn column_of_extracts_from_cast_and_alias() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::common::Column;
        use datafusion::logical_expr::{Cast, Expr};

        let col_expr = Expr::Column(Column::new_unqualified("id"));
        // Direct column
        assert!(column_of(&col_expr).is_some());

        // Cast wrapping column
        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(col_expr.clone()),
            data_type: DataType::Int64,
        });
        assert!(column_of(&cast_expr).is_some());

        // Non-column literal
        let lit = Expr::Literal(datafusion::common::ScalarValue::Int64(Some(1)), None);
        assert!(column_of(&lit).is_none());
    }
}
