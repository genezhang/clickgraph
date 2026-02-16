use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{LogicalPlan, ProjectionItem};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};
use crate::utils::cte_naming::generate_cte_base_name;
use std::collections::HashMap;
use std::sync::Arc;

use super::errors::RenderBuildError;

use super::filter_builder::FilterBuilder;
use super::from_builder::FromBuilder;
use super::group_by_builder::GroupByBuilder;
use super::join_builder::JoinBuilder;
use super::properties_builder::PropertiesBuilder;
use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderExpr, TableAlias,
};
use super::select_builder::SelectBuilder;
use super::{
    ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
    JoinItems, JoinType, LimitItem, OrderByItem, OrderByItems, RenderPlan, SelectItem, SelectItems,
    SkipItem, Union, UnionItems, ViewTableRef,
};
use crate::render_plan::cte_extraction::extract_ctes_with_context;

// Import ALL helper functions from the dedicated helpers module using glob import
// This allows existing code to call helpers without changes (e.g., extract_table_name())
// The compiler will use the module functions when available
#[allow(unused_imports)]
use super::plan_builder_helpers::*;
use super::plan_builder_utils::rewrite_vlp_union_branch_aliases;
use super::utils::alias_utils::*;
use super::CteGenerationContext;

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Post-process SELECT items to wrap non-ID, non-aggregated columns with anyLast()
/// when there's a GROUP BY clause.
///
/// This fixes Bug #11: When `RETURN a` is expanded to all properties and there's an aggregation
/// causing GROUP BY, the GROUP BY only includes ID columns, but SELECT includes all columns.
/// ClickHouse requires non-aggregated, non-grouped columns to use aggregate functions.
///
/// # Arguments
/// * `select_items` - The SELECT items to process
/// * `group_by_exprs` - The GROUP BY expressions (if empty, no wrapping is done)
/// * `plan` - The logical plan (used to find ID columns)
///
/// # Returns
/// Modified SELECT items with anyLast() wrapping where needed
fn apply_anylast_wrapping_for_group_by(
    select_items: Vec<SelectItem>,
    group_by_exprs: &[RenderExpr],
    _plan: &LogicalPlan,
) -> RenderPlanBuilderResult<Vec<SelectItem>> {
    // If no GROUP BY, return items as-is
    if group_by_exprs.is_empty() {
        return Ok(select_items);
    }

    log::info!(
        "🔧 apply_anylast_wrapping: Processing {} SELECT items with {} GROUP BY expressions",
        select_items.len(),
        group_by_exprs.len()
    );

    let wrapped_items = select_items
        .into_iter()
        .map(|item| {
            // Check if this item needs wrapping:
            // 1. Skip if already an aggregate function
            // 2. Skip if it's in the GROUP BY
            // 3. Wrap if it's a PropertyAccess that's not an ID column

            // Skip if already an aggregate
            if matches!(&item.expression, RenderExpr::AggregateFnCall(_)) {
                return Ok(item);
            }

            // Check if this expression is in GROUP BY
            let in_group_by = group_by_exprs
                .iter()
                .any(|group_expr| expressions_match(group_expr, &item.expression));

            if in_group_by {
                return Ok(item);
            }

            // Only wrap PropertyAccess expressions
            if let RenderExpr::PropertyAccessExp(ref _prop_access) = item.expression {
                // Check if this is an ID column (ID columns are in GROUP BY, shouldn't be wrapped)
                // ID columns typically end with "_id" or ".id" in the alias
                let is_id_column = if let Some(ref col_alias) = item.col_alias {
                    let alias_str = &col_alias.0;
                    alias_str.ends_with("_id") || alias_str.ends_with(".id") || alias_str == "id"
                } else {
                    false
                };

                if is_id_column {
                    log::debug!(
                        "🔧 apply_anylast_wrapping: Skipping ID column {:?}",
                        item.col_alias
                    );
                    return Ok(item);
                }

                // Wrap with anyLast()
                log::debug!(
                    "🔧 apply_anylast_wrapping: Wrapping {:?} with anyLast()",
                    item.col_alias
                );
                Ok(SelectItem {
                    expression: RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: "anyLast".to_string(),
                        args: vec![item.expression.clone()],
                    }),
                    col_alias: item.col_alias,
                })
            } else {
                // Not a PropertyAccess, keep as-is
                Ok(item)
            }
        })
        .collect::<RenderPlanBuilderResult<Vec<SelectItem>>>()?;

    log::info!(
        "✅ apply_anylast_wrapping: Processed {} items",
        wrapped_items.len()
    );
    Ok(wrapped_items)
}

/// Helper to check if two RenderExpr are functionally equivalent
/// Used to determine if a SELECT item is in the GROUP BY
fn expressions_match(expr1: &RenderExpr, expr2: &RenderExpr) -> bool {
    match (expr1, expr2) {
        (RenderExpr::PropertyAccessExp(p1), RenderExpr::PropertyAccessExp(p2)) => {
            p1.table_alias.0 == p2.table_alias.0 && p1.column == p2.column
        }
        (RenderExpr::Column(c1), RenderExpr::Column(c2)) => c1.0 == c2.0,
        (RenderExpr::TableAlias(t1), RenderExpr::TableAlias(t2)) => t1.0 == t2.0,
        _ => false,
    }
}

// Methods are used internally via recursive calls within the impl block.
// The dead_code warning is a false positive since Rust can't see the recursive usage.
#[allow(dead_code)]
pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(
        &self,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
    ) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_ctes_with_context(
        &self,
        last_node_alias: &str,
        context: &mut CteGenerationContext,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Vec<Cte>>;

    /// Find the ID column for a given table alias by traversing the logical plan
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String>;

    /// Find ID column for an alias with CTE context (checks CTE schemas first)
    fn find_id_column_with_cte_context(
        &self,
        alias: &str,
        cte_schemas: &super::CteSchemas,
        cte_references: &HashMap<String, String>,
    ) -> RenderPlanBuilderResult<String>;

    /// Get all properties for an alias along with the actual table alias to use for SQL generation.
    /// For denormalized nodes, this returns the relationship alias instead of the node alias.
    /// Returns: (properties, actual_table_alias) where actual_table_alias is None to use the original alias
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> RenderPlanBuilderResult<(Vec<(String, String)>, Option<String>)>;

    /// Normalize aggregate function arguments: convert TableAlias(a) to PropertyAccess(a.id_column)
    /// This is needed for queries like COUNT(b) where b is a node alias
    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr>;

    fn extract_select_items(
        &self,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_distinct(&self) -> bool;

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_joins(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Vec<Join>>;

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>>;

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn extract_union(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Option<Union>>;

    /// Extract union with plan_ctx for path variable support.
    /// This is a bridge method that allows Union branches to be rendered with plan_ctx.
    fn extract_union_with_ctx(
        &self,
        schema: &GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Option<Union>>;

    /// Extract UNWIND clause as ARRAY JOIN items
    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<super::ArrayJoin>>;

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan>;

    /// Convert to render plan with access to analysis-phase context (PlanCtx).
    /// This method should be preferred over `to_render_plan` when `plan_ctx` is available,
    /// as it provides access to VLP endpoint information and other analysis metadata.
    fn to_render_plan_with_ctx(
        &self,
        schema: &GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<RenderPlan>;
}

// ============================================================================
// VLP Union Branch Alias Rewriting
// ============================================================================

// ============================================================================
// Rewrite SELECT aliases in Union branches that reference VLP CTEs.
//
// Problem: Undirected shortestPath creates Union with 2 branches (forward/backward).
// Each branch uses Cypher aliases (a, b) but JOINs to VLP tables (start_node, end_node).
// SELECT items reference non-existent aliases causing "Unknown expression identifier".
//
// Solution: For each Union branch:
// 1. Find VLP CTEs it references (look for vlp_cte joins)
// 2. Get VLP metadata (cypher_start_alias → start_node mapping)
// 3. Rewrite SELECT items: a.property → start_node.property
//
// Extract VLP alias mappings from CTEs: Cypher alias → VLP table alias.
// Also extracts relationship aliases for denormalized patterns.
// ============================================================================
// ARCHITECTURAL NOTE: Multi-Type VLP Alias Mapping Evolution (Dec 27, 2025)
// ============================================================================
//
// PROBLEM: Multi-type VLP patterns like (u)-[:FOLLOWS|AUTHORED*1..2]->(x) create
// 3 layers of aliases:
//   1. Cypher aliases (u, x, r) - what users write
//   2. VLP internal aliases (start_node, end_node) - metadata for recursion
//   3. CTE names (vlp_multi_type_u_x) - actual table references
//
// FAILED APPROACH (removed Dec 27, 2025):
// Attempted complex multi-pass rewriting with `rewrite_cte_column_refs()`:
//   - Selectively rewrite CTE columns (end_type, end_id) to use CTE alias
//   - Leave regular properties unchanged for JSON extraction
//   - Result: Combinatorial complexity, error-prone, hard to maintain
//
// SUCCESSFUL APPROACH (implemented Dec 27, 2025):
// Set correct alias at FROM clause (see lines 11780-11810):
//   ```rust
//   final_from = Some(FromTable::new(Some(ViewTableRef {
//       name: cte.cte_name.clone(),           // vlp_multi_type_u_x
//       alias: Some(cypher_end_alias.clone()), // x (Cypher alias!)
//       use_final: false,
//   })));
//   ```
// Generated SQL: `FROM vlp_multi_type_u_x AS x`
// Then naturally:
//   - x.end_type → CTE column (direct access)
//   - x.name → property (SQL generator extracts from JSON)
//   - No rewriting needed - aliases match naturally!
//
// LESSON: Set it right at the source, not through multi-pass rewriting.
// Git history preserves the complex rewriting implementation for reference.
// ============================================================================

// Recursively rewrite RenderExpr to use VLP table aliases

// ============================================================================
// WITH Clause Helper Functions (Code Deduplication)
// ============================================================================

/// Helper: Expand a TableAlias to ALL column SelectItems.
///
/// Used by WITH clause handlers when they need to convert LogicalExpr::TableAlias
/// to multiple RenderExpr SelectItems (one per property).
///
/// Expand a table alias to SELECT items using pre-resolved CTE references.
///
/// The analyzer phase has already determined which variables come from which CTEs.
/// This function simply looks up the CTE name and fetches the columns.
///
/// Strategy (SIMPLE - no searching!):
/// 1. Check cte_references map: does this alias reference a CTE?
/// 2. If yes, get columns from cte_schemas[cte_name] with this alias prefix
/// 3. If no, it's a fresh variable - query the plan for base table properties
///
/// When a Union is rendered to a RenderPlan, the first branch's data lives in
/// the base plan fields (select/from/joins/filters) while remaining branches
/// are in union.input. For the GraphJoins flat-extraction path, we need ALL
/// branches in union.input because the outer GraphJoins will overwrite
/// plan.select with the outer projection. This helper moves the first branch.
fn move_first_branch_into_union(plan: RenderPlan) -> Option<Union> {
    if let Some(mut union) = plan.union.0 {
        if plan.from.0.is_some() {
            let first_branch = RenderPlan {
                ctes: CteItems(vec![]),
                select: plan.select,
                from: plan.from,
                joins: plan.joins,
                array_join: plan.array_join,
                filters: plan.filters,
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
                fixed_path_info: None,
                is_multi_label_scan: false,
            };
            union.input.insert(0, first_branch);
        }
        Some(union)
    } else {
        None
    }
}

/// # Arguments
/// * `has_aggregation` - If true, wraps non-ID columns with anyLast() for efficient aggregation
/// * `plan_ctx` - Optional PlanCtx for accessing PropertyRequirements (property pruning optimization)
impl RenderPlanBuilder for LogicalPlan {
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String> {
        // Traverse the plan tree to find a GraphNode or ViewScan with matching alias
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // Found the matching node - extract ID column from its ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    return Ok(scan.id_column.clone());
                } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                    // For denormalized polymorphic nodes, the input is a UNION of ViewScans
                    // All ViewScans should have the same id_column, so use the first one
                    if let Some(first_input) = union_plan.inputs.first() {
                        if let LogicalPlan::ViewScan(scan) = first_input.as_ref() {
                            return Ok(scan.id_column.clone());
                        }
                    }
                }
            }
            LogicalPlan::GraphRel(rel) => {
                // First try to find the actual GraphNode's ID column in left/right branches.
                // This takes priority because the node's real ID column (e.g., "user_id")
                // is the correct answer for WITH CTE schemas.
                if let Ok(id) = rel.left.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
                if let Ok(id) = rel.right.find_id_column_for_alias(alias) {
                    return Ok(id);
                }

                // VLP ENDPOINT FALLBACK: For variable-length paths, if the alias wasn't
                // found as a GraphNode (e.g., denormalized schemas without separate node
                // tables), use start_id/end_id from the VLP CTE.
                if rel.variable_length.is_some() {
                    let start_alias = &rel.left_connection;
                    let end_alias = &rel.right_connection;

                    if alias == start_alias {
                        log::info!(
                            "🎯 VLP: Alias '{}' is VLP start endpoint (fallback) -> using '{}' as ID column",
                            alias,
                            VLP_START_ID_COLUMN
                        );
                        return Ok(VLP_START_ID_COLUMN.to_string());
                    }
                    if alias == end_alias {
                        log::info!(
                            "🎯 VLP: Alias '{}' is VLP end endpoint (fallback) -> using '{}' as ID column",
                            alias,
                            VLP_END_ID_COLUMN
                        );
                        return Ok(VLP_END_ID_COLUMN.to_string());
                    }
                }
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Union(union) => {
                // For UNION, check all branches and return the first match
                // All branches should have the same schema, so any match is valid
                for input in &union.inputs {
                    if let Ok(id) = input.find_id_column_for_alias(alias) {
                        return Ok(id);
                    }
                }
            }
            LogicalPlan::WithClause(wc) => {
                // For WITH clause, check if the alias is exported and get its ID column from input
                if wc.exported_aliases.contains(&alias.to_string()) {
                    return wc.input.find_id_column_for_alias(alias);
                }
            }
            _ => {}
        }
        Err(RenderBuildError::InvalidRenderPlan(format!(
            "Cannot find ID column for alias '{}'",
            alias
        )))
    }

    // REMOVED: get_all_properties_for_alias function (Phase 3D)
    // This function was marked as dead_code and never called externally.
    // It traversed the plan tree to extract all properties for an alias.
    // Removed as part of renderer simplification - ~180 lines.

    /// Find ID column for an alias by checking CTE schemas first, then plan tree
    /// This handles both regular nodes and aliases from CTEs (like VLP results)
    fn find_id_column_with_cte_context(
        &self,
        alias: &str,
        cte_schemas: &super::CteSchemas,
        cte_references: &HashMap<String, String>,
    ) -> RenderPlanBuilderResult<String> {
        // First, check if this alias comes from a CTE
        if let Some(cte_name) = cte_references.get(alias) {
            if let Some((_select_items, _property_names, alias_to_id_column, _prop_map)) =
                cte_schemas.get(cte_name)
            {
                // Look up the ID column for this specific alias
                if let Some(id_col) = alias_to_id_column.get(alias) {
                    log::info!(
                        "✅ Found ID column '{}' for alias '{}' in CTE '{}'",
                        id_col,
                        alias,
                        cte_name
                    );
                    return Ok(id_col.clone());
                } else {
                    log::warn!(
                        "⚠️ CTE '{}' found for alias '{}' but no ID column mapping exists",
                        cte_name,
                        alias
                    );
                    log::warn!("⚠️ Available alias mappings: {:?}", alias_to_id_column);
                }
            }
        }

        // Fall back to plan tree traversal
        self.find_id_column_for_alias(alias)
    }

    /// Get all properties for an alias, returning both properties and the actual table alias to use.
    /// For denormalized nodes, the table alias is the relationship alias (not the node alias).
    /// Returns: (properties, actual_table_alias) where actual_table_alias is None to use the original alias
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> RenderPlanBuilderResult<(Vec<(String, String)>, Option<String>)> {
        // Delegate to the PropertiesBuilder trait implementation
        <LogicalPlan as PropertiesBuilder>::get_properties_with_table_alias(self, alias)
    }
    // REMOVED: find_denormalized_properties function (Phase 3D)
    // This function was marked as dead_code and never called externally.
    // It traversed the plan tree to find denormalized node properties.
    // Removed as part of renderer simplification - ~54 lines.

    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr> {
        match expr {
            RenderExpr::AggregateFnCall(mut agg) => {
                // Recursively normalize all arguments
                agg.args = agg
                    .args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::AggregateFnCall(agg))
            }
            RenderExpr::TableAlias(alias) => {
                // Convert COUNT(b) to COUNT(b.user_id)
                let id_col = self.find_id_column_for_alias(&alias.0)?;
                Ok(RenderExpr::PropertyAccessExp(
                    super::render_expr::PropertyAccess {
                        table_alias: alias,
                        column: PropertyValue::Column(id_col),
                    },
                ))
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                // Recursively normalize operands
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|operand| self.normalize_aggregate_args(operand))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::OperatorApplicationExp(op))
            }
            RenderExpr::ScalarFnCall(mut func) => {
                // Recursively normalize function arguments
                func.args = func
                    .args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::ScalarFnCall(func))
            }
            // Other expressions pass through unchanged
            _ => Ok(expr),
        }
    }

    fn extract_last_node_cte(
        &self,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
    ) -> RenderPlanBuilderResult<Option<Cte>> {
        let last_node_cte = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_last_node_cte(schema)?,
            LogicalPlan::GraphRel(graph_rel) => {
                // Last node is at the top of the tree.
                // process left node first.
                let left_node_cte_opt = graph_rel.left.extract_last_node_cte(schema)?;

                // If last node is still not found then check at the right tree
                if left_node_cte_opt.is_none() {
                    graph_rel.right.extract_last_node_cte(schema)?
                } else {
                    left_node_cte_opt
                }
            }
            LogicalPlan::Filter(filter) => filter.input.extract_last_node_cte(schema)?,
            LogicalPlan::Projection(projection) => {
                projection.input.extract_last_node_cte(schema)?
            }
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_last_node_cte(schema)?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_last_node_cte(schema)?,
            LogicalPlan::Skip(skip) => skip.input.extract_last_node_cte(schema)?,
            LogicalPlan::Limit(limit) => limit.input.extract_last_node_cte(schema)?,
            LogicalPlan::GraphJoins(graph_joins) => {
                graph_joins.input.extract_last_node_cte(schema)?
            }
            LogicalPlan::Cte(logical_cte) => {
                // 🔧 FIX: Use the schema parameter instead of creating an empty schema
                let render_cte = Cte::new(
                    strip_database_prefix(&logical_cte.name),
                    super::CteContent::Structured(Box::new(
                        logical_cte.input.to_render_plan(schema)?,
                    )),
                    false, // is_recursive
                );
                Some(render_cte)
            }
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    if let Some(cte) = input_plan.extract_last_node_cte(schema)? {
                        return Ok(Some(cte));
                    }
                }
                None
            }
            LogicalPlan::PageRank(_) => None,
            LogicalPlan::Unwind(u) => u.input.extract_last_node_cte(schema)?,
            LogicalPlan::CartesianProduct(cp) => {
                // Try left first, then right
                cp.left
                    .extract_last_node_cte(schema)?
                    .or(cp.right.extract_last_node_cte(schema)?)
            }
            LogicalPlan::WithClause(wc) => wc.input.extract_last_node_cte(schema)?,
        };
        Ok(last_node_cte)
    }

    fn extract_ctes_with_context(
        &self,
        last_node_alias: &str,
        context: &mut CteGenerationContext,
        schema: &GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Vec<Cte>> {
        extract_ctes_with_context(self, last_node_alias, context, schema, plan_ctx)
    }

    fn extract_select_items(
        &self,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        // Delegate to the SelectBuilder trait implementation
        <LogicalPlan as SelectBuilder>::extract_select_items(self, plan_ctx)
    }

    fn extract_distinct(&self) -> bool {
        // Delegate to the FilterBuilder trait implementation
        <LogicalPlan as FilterBuilder>::extract_distinct(self)
    }

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        // Delegate to the FilterBuilder trait implementation
        <LogicalPlan as FilterBuilder>::extract_filters(self)
    }

    fn extract_joins(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Vec<Join>> {
        // Delegate to the JoinBuilder trait implementation
        <LogicalPlan as JoinBuilder>::extract_joins(self, schema)
    }

    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<super::ArrayJoin>> {
        // Delegate to the JoinBuilder trait implementation
        <LogicalPlan as JoinBuilder>::extract_array_join(self)
    }

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
        // Delegate to the GroupByBuilder trait implementation
        <LogicalPlan as GroupByBuilder>::extract_group_by(self)
    }

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        // Note: HAVING clauses are handled by LogicalPlan::GroupBy nodes in to_render_plan().
        // This method returns None for GraphJoins/GraphRel which don't directly have HAVING.
        Ok(None)
    }

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        // Note: ORDER BY is handled by LogicalPlan::OrderBy nodes in to_render_plan().
        // This method returns empty for GraphJoins/GraphRel which don't directly have ORDER BY.
        Ok(vec![])
    }

    fn extract_limit(&self) -> Option<i64> {
        // Note: LIMIT is handled by LogicalPlan::Limit nodes in to_render_plan().
        // This method returns None for GraphJoins/GraphRel which don't directly have LIMIT.
        None
    }

    fn extract_skip(&self) -> Option<i64> {
        // Note: SKIP is handled by LogicalPlan::Skip nodes in to_render_plan().
        // This method returns None for GraphJoins/GraphRel which don't directly have SKIP.
        None
    }

    fn extract_union(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Option<Union>> {
        // For GraphJoins, check if Union is nested inside (possibly wrapped in GraphNode, Projection, GroupBy, etc.)
        if let LogicalPlan::GraphJoins(gj) = self {
            let mut current = gj.input.as_ref();
            loop {
                match current {
                    LogicalPlan::GraphNode(gn) => current = gn.input.as_ref(),
                    LogicalPlan::Projection(proj) => current = proj.input.as_ref(),
                    LogicalPlan::GroupBy(gb) => current = gb.input.as_ref(),
                    LogicalPlan::Union(_union) => {
                        // Found Union nested deep, convert it to render plan
                        log::debug!("extract_union: found nested Union, converting to render");
                        let union_render_plan = current.to_render_plan(schema)?;

                        // The first branch is stored in the base render plan (select/from/etc.)
                        // and remaining branches are in union.0. We need ALL branches in
                        // union.input so the SQL generator uses them correctly (the outer
                        // GraphJoins will overwrite plan.select with the outer projection).
                        return Ok(move_first_branch_into_union(union_render_plan));
                    }
                    _ => break,
                }
            }
        }

        // Note: UNION is handled by LogicalPlan::Union nodes in to_render_plan().
        // This method returns None for other node types.
        Ok(None)
    }

    /// Extract union with plan_ctx for path variable support.
    /// This is a bridge method that allows Union branches to be rendered with plan_ctx.
    fn extract_union_with_ctx(
        &self,
        schema: &GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<Option<Union>> {
        // Unwrap Limit/Skip/OrderBy wrappers to find GraphJoins
        let graph_joins_node = match self {
            LogicalPlan::GraphJoins(_) => self,
            LogicalPlan::Limit(l) => l.input.as_ref(),
            LogicalPlan::Skip(s) => s.input.as_ref(),
            LogicalPlan::OrderBy(o) => o.input.as_ref(),
            _ => {
                log::warn!("🔀 extract_union_with_ctx: Not GraphJoins or wrapper, returning None");
                return Ok(None);
            }
        };

        // For GraphJoins, check if Union is nested inside (possibly wrapped in GraphNode, Projection, GroupBy, etc.)
        if let LogicalPlan::GraphJoins(gj) = graph_joins_node {
            log::warn!(
                "🔀 extract_union_with_ctx: GraphJoins.input type: {:?}",
                std::mem::discriminant(gj.input.as_ref())
            );
            let mut current = gj.input.as_ref();
            loop {
                match current {
                    LogicalPlan::GraphNode(gn) => {
                        log::warn!("🔀 extract_union_with_ctx: Found GraphNode, recursing...");
                        current = gn.input.as_ref();
                    }
                    LogicalPlan::Projection(proj) => {
                        log::warn!("🔀 extract_union_with_ctx: Found Projection, recursing...");
                        current = proj.input.as_ref();
                    }
                    LogicalPlan::GroupBy(gb) => {
                        log::warn!("🔀 extract_union_with_ctx: Found GroupBy, recursing...");
                        current = gb.input.as_ref();
                    }
                    LogicalPlan::Union(_union) => {
                        // Found Union nested deep, convert it to render plan WITH plan_ctx
                        log::warn!("🔀 extract_union_with_ctx: found nested Union, calling to_render_plan_with_ctx");
                        let union_render_plan =
                            current.to_render_plan_with_ctx(schema, plan_ctx)?;
                        log::warn!(
                            "🔀 extract_union_with_ctx: Union rendered, has_union={:?}",
                            union_render_plan.union.0.is_some()
                        );

                        // Move first branch into union.input (same logic as extract_union)
                        return Ok(move_first_branch_into_union(union_render_plan));
                    }
                    other => {
                        log::warn!(
                            "🔀 extract_union_with_ctx: Found {:?}, breaking loop",
                            std::mem::discriminant(other)
                        );
                        break;
                    }
                }
            }
        }

        // Note: UNION is handled by LogicalPlan::Union nodes in to_render_plan().
        // This method returns None for other node types.
        log::warn!("🔀 extract_union_with_ctx: No Union found, returning None");
        Ok(None)
    }

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
        // CRITICAL: If the plan contains WITH clauses, use the specialized handler
        // build_chained_with_match_cte_plan handles chained/nested WITH correctly
        use super::plan_builder_utils::{
            build_chained_with_match_cte_plan, has_with_clause_in_graph_rel,
        };

        let plan_name = match self {
            LogicalPlan::GraphJoins(_) => "GraphJoins",
            LogicalPlan::GraphRel(_) => "GraphRel",
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::OrderBy(_) => "OrderBy",
            LogicalPlan::Skip(_) => "Skip",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::GroupBy(_) => "GroupBy",
            LogicalPlan::GraphNode(_) => "GraphNode",
            LogicalPlan::ViewScan(_) => "ViewScan",
            LogicalPlan::Union(_) => "Union",
            LogicalPlan::WithClause(_) => "WithClause",
            _ => "Other",
        };
        log::debug!("to_render_plan called with: {}", plan_name);

        if has_with_clause_in_graph_rel(self) {
            return build_chained_with_match_cte_plan(self, schema, None);
        }

        match self {
            LogicalPlan::GraphJoins(gj) => {
                let mut select_items = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?,
                    distinct: FilterBuilder::extract_distinct(self),
                };

                // Extract CTEs FIRST - this registers CTE names in task-local QueryContext
                let mut context = super::cte_generation::CteGenerationContext::new();
                let ctes = CteItems(extract_ctes_with_context(
                    &gj.input,
                    "",
                    &mut context,
                    schema,
                    None,
                )?);

                // NOW extract FROM with context so multi-type VLP can look up registered CTE names
                use crate::render_plan::from_builder::FromBuilder;
                let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));

                // Now extract joins WITH the context so multi-type relationships can look up their CTE names
                let raw_joins = <LogicalPlan as JoinBuilder>::extract_joins_with_context(
                    self, schema, &context,
                )?;

                // Deduplicate aliases
                let joins = JoinItems::new(raw_joins);

                let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
                let filters = FilterItems(FilterBuilder::extract_filters(self)?);
                let group_by =
                    GroupByExpressions(<LogicalPlan as GroupByBuilder>::extract_group_by(self)?);
                let having_clause = self.extract_having()?;

                // 🔧 BUG #11 FIX: Wrap non-ID, non-aggregated columns with anyLast() when GROUP BY present
                // This fixes queries like: RETURN a, count(b) where a expands to all properties
                // but GROUP BY only has a.user_id
                select_items.items =
                    apply_anylast_wrapping_for_group_by(select_items.items, &group_by.0, self)?;

                let order_by = OrderByItems(self.extract_order_by()?);
                let skip = SkipItem(self.extract_skip());
                let limit = LimitItem(self.extract_limit());
                let union = UnionItems(self.extract_union(schema)?);

                let mut render_plan = RenderPlan {
                    ctes,
                    select: select_items,
                    from,
                    joins,
                    array_join,
                    filters,
                    group_by,
                    having_clause,
                    order_by,
                    skip,
                    limit,
                    union,
                    fixed_path_info: None,
                    is_multi_label_scan: false,
                    // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                };

                // Populate the CTE column registry from CTE metadata
                // populate_cte_column_registry(&mut render_plan);

                // 🔧 CRITICAL: Rewrite JOIN conditions for UNION branches with VLP
                // If this render plan has UNIONs with VLP CTEs, we need to rewrite
                // JOIN conditions that reference Cypher aliases to use VLP CTE columns
                rewrite_vlp_union_branch_aliases(&mut render_plan)?;

                // Handle RETURN-context pattern comprehensions nested inside GroupBy->Projection
                let pattern_comps = find_pattern_comprehensions_in_plan(&gj.input);
                if !pattern_comps.is_empty() {
                    log::info!(
                        "🔧 GraphJoins: Found {} pattern comprehension(s) in nested Projection",
                        pattern_comps.len()
                    );
                    for (pc_idx, pc_meta) in pattern_comps.iter().enumerate() {
                        let pc_cte_name =
                            format!("pattern_comp_{}_{}", pc_meta.correlation_var, pc_idx);

                        if let Some(pc_sql) =
                            super::plan_builder_utils::build_pattern_comprehension_sql(
                                &pc_meta.correlation_label,
                                &pc_meta.direction,
                                &pc_meta.rel_types,
                                &pc_meta.agg_type,
                                schema,
                                pc_meta.target_label.as_deref(),
                                pc_meta.target_property.as_deref(),
                            )
                        {
                            let pc_cte = super::Cte::new(
                                pc_cte_name.clone(),
                                super::CteContent::RawSql(pc_sql),
                                false,
                            );
                            render_plan.ctes.0.push(pc_cte);

                            let from_alias = render_plan
                                .from
                                .0
                                .as_ref()
                                .map(|f| f.alias.clone().unwrap_or_else(|| f.name.clone()))
                                .unwrap_or_else(|| pc_meta.correlation_var.clone());

                            let pc_alias = format!("__pc_{}", pc_idx);
                            let lhs_expr = super::plan_builder_utils::build_node_id_expr_for_join(
                                &from_alias,
                                &pc_meta.correlation_label,
                                schema,
                            );
                            let on_clause = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    lhs_expr,
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(pc_alias.clone()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("node_id".to_string()),
                                    }),
                                ],
                            };

                            render_plan.joins.0.push(super::Join {
                                table_name: pc_cte_name.clone(),
                                table_alias: pc_alias.clone(),
                                joining_on: vec![on_clause],
                                join_type: super::JoinType::Left,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });

                            // Replace with coalesce(__pc_N.result, default)
                            let result_alias = pc_meta.result_alias.clone();
                            let default_val = if matches!(
                                pc_meta.agg_type,
                                crate::query_planner::logical_plan::AggregationType::GroupArray
                            ) {
                                RenderExpr::List(vec![])
                            } else {
                                RenderExpr::Literal(Literal::Integer(0))
                            };
                            let coalesce_expr = RenderExpr::ScalarFnCall(
                                super::render_expr::ScalarFnCall {
                                    name: "coalesce".to_string(),
                                    args: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(pc_alias),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column("result".to_string()),
                                        }),
                                        default_val,
                                    ],
                                }
                            );

                            for item in &mut render_plan.select.items {
                                if let Some(ref alias) = item.col_alias {
                                    if alias.0 == result_alias {
                                        item.expression = coalesce_expr.clone();
                                        break;
                                    }
                                }
                            }

                            // Remove GROUP BY — CTE+JOIN doesn't need it
                            render_plan.group_by.0.clear();

                            log::info!("✅ Added pattern comp CTE '{}' with LEFT JOIN (GraphJoins context)", pc_cte_name);
                        }
                    }
                }

                Ok(render_plan)
            }
            LogicalPlan::GraphRel(gr) => {
                // Check if this is an optional variable-length path
                let is_optional_vlp =
                    gr.variable_length.is_some() && gr.is_optional.unwrap_or(false);

                if is_optional_vlp {
                    // Handle optional VLP: use base table as FROM, CTE as LEFT JOIN
                    log::info!("🎯 OPTIONAL VLP detected: restructuring query to use LEFT JOIN");

                    // Extract CTEs first for variable-length paths
                    let mut context = super::cte_generation::CteGenerationContext::new();
                    let ctes = CteItems(extract_ctes_with_context(
                        self,
                        &gr.right_connection,
                        &mut context,
                        schema,
                        None,
                    )?);

                    // For optional VLP, FROM should be the start node table
                    // Extract FROM from the start node (left side of GraphRel)
                    let from = FromTableItem(gr.left.extract_from()?.and_then(|ft| ft.table));

                    // Generate unique names to avoid conflicts
                    let vlp_alias = format!("__vlp_{}_{}", gr.left_connection, gr.right_connection);
                    let cte_name = format!("vlp_{}_{}", gr.left_connection, gr.right_connection);
                    let start_id_column = extract_id_column(&gr.left).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "start node ID column for optional VLP".to_string(),
                        )
                    })?;

                    // Create join condition using the dynamic VLP alias and start node ID column
                    let join_condition = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(gr.left_connection.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        start_id_column.clone(),
                                    ),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(vlp_alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        "start_id".to_string(),
                                    ),
                            }),
                        ],
                    };

                    let vlp_join = Join {
                        join_type: JoinType::Left,
                        table_name: cte_name,
                        table_alias: vlp_alias.clone(),
                        joining_on: vec![join_condition],
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    };

                    let joins = JoinItems::new(vec![vlp_join]);

                    // Extract select items WITHOUT CTE registry (properties come from base table)
                    let select_items = SelectItems {
                        items: <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?,
                        distinct: FilterBuilder::extract_distinct(self),
                    };

                    let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
                    let filters = FilterItems(FilterBuilder::extract_filters(self)?);
                    let group_by = GroupByExpressions(
                        <LogicalPlan as GroupByBuilder>::extract_group_by(self)?,
                    );
                    let having_clause = self.extract_having()?;

                    let order_by = OrderByItems(self.extract_order_by()?);
                    let skip = SkipItem(self.extract_skip());
                    let limit = LimitItem(self.extract_limit());
                    let union = UnionItems(self.extract_union(schema)?);

                    let render_plan = RenderPlan {
                        ctes,
                        select: select_items,
                        from,
                        joins,
                        array_join,
                        filters,
                        group_by,
                        having_clause,
                        order_by,
                        skip,
                        limit,
                        union,
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                        // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                    };

                    Ok(render_plan)
                } else {
                    // Regular VLP or non-VLP GraphRel
                    // Extract CTEs first for variable-length paths
                    let mut context = super::cte_generation::CteGenerationContext::new();
                    let ctes = CteItems(extract_ctes_with_context(
                        self,
                        &gr.right_connection,
                        &mut context,
                        schema,
                        None,
                    )?);

                    // Create temporary render plan to populate CTE registry
                    let _temp_render_plan = RenderPlan {
                        ctes: ctes.clone(),
                        select: SelectItems {
                            items: vec![],
                            distinct: false,
                        },
                        from: FromTableItem(None),
                        joins: JoinItems(vec![]),
                        array_join: ArrayJoinItem(vec![]),
                        filters: FilterItems(None),
                        group_by: GroupByExpressions(vec![]),
                        having_clause: None,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                        // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                    };

                    // Populate the CTE column registry from CTE metadata
                    // populate_cte_column_registry(&mut temp_render_plan);

                    // Set the CTE column registry in task-local storage for property resolution
                    // use crate::render_plan::set_cte_column_registry;
                    // set_cte_column_registry(temp_render_plan.cte_column_registry.clone());

                    // Now extract select items with CTE registry available
                    let mut select_items = SelectItems {
                        items: <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?,
                        distinct: FilterBuilder::extract_distinct(self),
                    };
                    let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));

                    // 🔧 FIX for VLP: Don't extract joins when this is a Variable-Length Path
                    // VLP patterns use the recursive CTE as FROM, and the joins are only needed
                    // for CTE generation (in extract_ctes_with_context), not for the final SELECT
                    let joins = if gr.variable_length.is_some() {
                        log::info!("🔧 VLP detected: Skipping JOIN extraction (using CTE as FROM)");
                        JoinItems(vec![])
                    } else {
                        JoinItems::new(RenderPlanBuilder::extract_joins(self, schema)?)
                    };

                    let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
                    let filters = FilterItems(FilterBuilder::extract_filters(self)?);
                    let group_by = GroupByExpressions(
                        <LogicalPlan as GroupByBuilder>::extract_group_by(self)?,
                    );
                    let having_clause = self.extract_having()?;

                    // 🔧 BUG #11 FIX: Wrap non-ID, non-aggregated columns with anyLast() when GROUP BY present
                    select_items.items =
                        apply_anylast_wrapping_for_group_by(select_items.items, &group_by.0, self)?;

                    let order_by = OrderByItems(self.extract_order_by()?);

                    let skip = SkipItem(self.extract_skip());
                    let limit = LimitItem(self.extract_limit());
                    let union = UnionItems(self.extract_union(schema)?);

                    let render_plan = RenderPlan {
                        ctes,
                        select: select_items,
                        from,
                        joins,
                        array_join,
                        filters,
                        group_by,
                        having_clause,
                        order_by,
                        skip,
                        limit,
                        union,
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                        // cte_column_registry: temp_render_plan.cte_column_registry, // REMOVED: No longer used
                    };

                    Ok(render_plan)
                }
            }
            LogicalPlan::Projection(p) => {
                // For Projection, convert the input plan and override the select items
                log::debug!(
                    "Projection::to_render_plan: input type={}",
                    match p.input.as_ref() {
                        LogicalPlan::GraphNode(_) => "GraphNode",
                        LogicalPlan::Union(_) => "Union",
                        _ => "Other",
                    }
                );

                let mut render_plan = p.input.to_render_plan(schema)?;

                log::debug!(
                    "Projection::to_render_plan: after input conversion, has_union={}, is_multi_label_scan={}",
                    render_plan.union.0.is_some(),
                    render_plan.is_multi_label_scan
                );

                // 🔧 Multi-label scan: Skip SELECT overwriting to preserve special columns
                // When a multi-label scan creates _label, _id, _properties columns,
                // we must NOT overwrite them with the normal extract_select_items result
                if render_plan.is_multi_label_scan {
                    log::info!(
                        "🎯 Projection over multi-label scan: preserving special SELECT columns"
                    );
                    // Just apply distinct if needed, but keep the SELECT items
                    render_plan.select.distinct = p.distinct;
                    return Ok(render_plan);
                }

                // CRITICAL FIX: If the input is a WithClause, the render_plan will have the CTE registry
                // Set it as thread-local so that extract_select_items can use it for property resolution
                if matches!(p.input.as_ref(), LogicalPlan::WithClause(_)) {
                    log::info!("🔧 Projection over WithClause: Using TypedVariable for property resolution");

                    // Extract select items with PlanCtx available for TypedVariable resolution
                    let select_items =
                        <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?;
                    render_plan.select = SelectItems {
                        items: select_items,
                        distinct: p.distinct,
                    };
                } else {
                    let select_items =
                        <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?;

                    // Check if this Projection is over an optional VLP GraphRel
                    if let LogicalPlan::GraphRel(gr) = p.input.as_ref() {
                        if gr.variable_length.is_some() && gr.is_optional.unwrap_or(false) {
                            log::info!("🎯 Projection over optional VLP: aggregations handled by LEFT JOIN with COUNT(*)");
                        }
                    }

                    render_plan.select = SelectItems {
                        items: select_items,
                        distinct: p.distinct,
                    };
                }

                // RETURN-context pattern comprehensions: generate CTE+JOIN (same as WITH context)
                if !p.pattern_comprehensions.is_empty() {
                    log::info!(
                        "🔧 RETURN pattern comprehension: generating {} CTE+JOIN(s)",
                        p.pattern_comprehensions.len()
                    );
                    for (pc_idx, pc_meta) in p.pattern_comprehensions.iter().enumerate() {
                        let pc_cte_name =
                            format!("pattern_comp_{}_{}", pc_meta.correlation_var, pc_idx);

                        if let Some(pc_sql) =
                            super::plan_builder_utils::build_pattern_comprehension_sql(
                                &pc_meta.correlation_label,
                                &pc_meta.direction,
                                &pc_meta.rel_types,
                                &pc_meta.agg_type,
                                schema,
                                pc_meta.target_label.as_deref(),
                                pc_meta.target_property.as_deref(),
                            )
                        {
                            // Add the pattern comp CTE
                            let pc_cte = super::Cte::new(
                                pc_cte_name.clone(),
                                super::CteContent::RawSql(pc_sql),
                                false,
                            );
                            render_plan.ctes.0.push(pc_cte);

                            // Determine the FROM table alias for the correlation variable
                            let from_alias = render_plan
                                .from
                                .0
                                .as_ref()
                                .map(|f| f.alias.clone().unwrap_or_else(|| f.name.clone()))
                                .unwrap_or_else(|| pc_meta.correlation_var.clone());

                            // Add LEFT JOIN to the render plan
                            let pc_alias = format!("__pc_{}", pc_idx);
                            let lhs_expr = super::plan_builder_utils::build_node_id_expr_for_join(
                                &from_alias,
                                &pc_meta.correlation_label,
                                schema,
                            );
                            let on_clause = super::render_expr::OperatorApplication {
                                operator: super::render_expr::Operator::Equal,
                                operands: vec![
                                    lhs_expr,
                                    RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                        table_alias: super::render_expr::TableAlias(pc_alias.clone()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("node_id".to_string()),
                                    }),
                                ],
                            };

                            render_plan.joins.0.push(super::Join {
                                table_name: pc_cte_name.clone(),
                                table_alias: pc_alias.clone(),
                                joining_on: vec![on_clause],
                                join_type: super::JoinType::Left,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });

                            // Replace the select item with coalesce(__pc_N.result, default)
                            let result_alias = pc_meta.result_alias.clone();
                            let default_val = if matches!(
                                pc_meta.agg_type,
                                crate::query_planner::logical_plan::AggregationType::GroupArray
                            ) {
                                RenderExpr::List(vec![])
                            } else {
                                RenderExpr::Literal(Literal::Integer(0))
                            };
                            let coalesce_expr = RenderExpr::ScalarFnCall(
                                super::render_expr::ScalarFnCall {
                                    name: "coalesce".to_string(),
                                    args: vec![
                                        RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                            table_alias: super::render_expr::TableAlias(pc_alias),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column("result".to_string()),
                                        }),
                                        default_val,
                                    ],
                                }
                            );

                            // Find and replace the count(*) item that has the matching alias
                            let mut replaced = false;
                            for item in &mut render_plan.select.items {
                                if let Some(ref alias) = item.col_alias {
                                    if alias.0 == result_alias {
                                        item.expression = coalesce_expr.clone();
                                        replaced = true;
                                        break;
                                    }
                                }
                            }
                            if !replaced {
                                // Add as new select item
                                render_plan.select.items.push(SelectItem {
                                    expression: coalesce_expr,
                                    col_alias: Some(ColumnAlias(result_alias)),
                                });
                            }

                            // Remove GROUP BY if it was only for the count(*) aggregation
                            // With CTE+JOIN, we don't need GROUP BY anymore
                            if !render_plan.group_by.0.is_empty() {
                                log::info!("🔧 Removing GROUP BY (pattern comp uses CTE+JOIN, not aggregation)");
                                render_plan.group_by.0.clear();
                            }

                            log::info!(
                                "✅ Added pattern comp CTE '{}' with LEFT JOIN for RETURN context",
                                pc_cte_name
                            );
                        }
                    }
                }

                Ok(render_plan)
            }
            LogicalPlan::Filter(f) => {
                // 🚀 OPTIMIZER: Detect Filter(Empty) early and short-circuit
                // This handles cases like `id(b) IN []` which produces Empty plan
                // Instead of generating complex SQL that returns 0 rows, return immediately
                // Why Neo4j Browser sends this: It's looking for edges between existing nodes
                // and new nodes. When there are no new nodes, newNodeIds=[], so IN [] is correct.
                if matches!(f.input.as_ref(), LogicalPlan::Empty) {
                    log::info!("🚀 OPTIMIZER: Filter(Empty) detected - short-circuiting to empty result (Neo4j Browser: no new nodes to connect)");
                    return Ok(RenderPlan {
                        ctes: CteItems(vec![]),
                        select: SelectItems {
                            items: vec![SelectItem {
                                expression: RenderExpr::Literal(Literal::Integer(1)),
                                col_alias: Some(ColumnAlias("_empty".to_string())),
                            }],
                            distinct: false,
                        },
                        from: FromTableItem(None),
                        joins: JoinItems(vec![]),
                        array_join: ArrayJoinItem(vec![]),
                        filters: FilterItems(Some(RenderExpr::Literal(Literal::Boolean(false)))),
                        group_by: GroupByExpressions(vec![]),
                        having_clause: None,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                    });
                }

                // For Filter, convert the input plan and combine filters
                let mut render_plan = f.input.to_render_plan(schema)?;

                // 🔧 BUG #10 FIX: For VLP/shortest path queries, filters on start/end nodes
                // are already pushed into the CTE during extraction. Don't duplicate them
                // in the outer SELECT WHERE clause.
                use super::plan_builder_helpers::has_variable_length_or_shortest_path;
                let has_vlp_or_shortest_path = has_variable_length_or_shortest_path(&f.input);

                eprintln!(
                    "DEBUG Filter::to_render_plan: has_vlp={}",
                    has_vlp_or_shortest_path
                );
                eprintln!("DEBUG Filter::to_render_plan: predicate={:?}", f.predicate);

                if has_vlp_or_shortest_path {
                    log::info!(
                        "🔧 BUG #10: Skipping Filter for VLP/shortest path - already in CTE"
                    );
                    eprintln!("🔧 BUG #10: Skipping Filter for VLP/shortest path - already in CTE");
                    // Don't add this filter - it's already in the CTE
                    // Just return the render plan from the input
                    Ok(render_plan)
                } else {
                    eprintln!("DEBUG Filter::to_render_plan: Normal filter handling");
                    // Normal filter handling

                    // 🔧 FIX: Rewrite property names to DB column names BEFORE converting to RenderExpr
                    // This uses the same function as WITH clause processing for consistency
                    use crate::query_planner::logical_expr::expression_rewriter::{
                        rewrite_expression_with_property_mapping, ExpressionRewriteContext,
                    };
                    let rewrite_ctx = ExpressionRewriteContext::new(&f.input);
                    let rewritten_predicate =
                        rewrite_expression_with_property_mapping(&f.predicate, &rewrite_ctx);

                    log::debug!(
                        "Filter rewrite: {:?} → {:?}",
                        f.predicate,
                        rewritten_predicate
                    );

                    // Convert the rewritten predicate to RenderExpr
                    let filter_expr: RenderExpr = rewritten_predicate.try_into()?;

                    // Combine with existing filters if any
                    render_plan.filters = match render_plan.filters.0 {
                        Some(existing) => FilterItems(Some(RenderExpr::OperatorApplicationExp(
                            crate::render_plan::render_expr::OperatorApplication {
                                operator: crate::render_plan::render_expr::Operator::And,
                                operands: vec![existing, filter_expr],
                            },
                        ))),
                        None => FilterItems(Some(filter_expr)),
                    };

                    Ok(render_plan)
                }
            }
            LogicalPlan::OrderBy(ob) => {
                // For OrderBy, convert the input plan and set order_by
                let mut render_plan = ob.input.to_render_plan(schema)?;

                // Convert logical OrderByItems to render OrderByItems
                let order_by_items: Result<Vec<OrderByItem>, _> = ob
                    .items
                    .iter()
                    .map(|item| item.clone().try_into())
                    .collect();
                render_plan.order_by = OrderByItems(order_by_items?);

                Ok(render_plan)
            }
            LogicalPlan::Skip(s) => {
                // For Skip, convert the input plan and set skip
                let mut render_plan = s.input.to_render_plan(schema)?;
                render_plan.skip = SkipItem(Some(s.count));
                Ok(render_plan)
            }
            LogicalPlan::Limit(l) => {
                // For Limit, convert the input plan and set limit
                let mut render_plan = l.input.to_render_plan(schema)?;
                render_plan.limit = LimitItem(Some(l.count));
                Ok(render_plan)
            }
            LogicalPlan::GroupBy(gb) => {
                // For GroupBy, convert the input plan and set group_by and having_clause
                let mut render_plan = gb.input.to_render_plan(schema)?;

                log::debug!(
                    "GroupBy::to_render_plan: has_union={}",
                    render_plan.union.0.is_some()
                );

                // Convert group by expressions
                let group_by_exprs: Result<Vec<RenderExpr>, _> = gb
                    .expressions
                    .iter()
                    .map(|expr| expr.clone().try_into())
                    .collect();
                render_plan.group_by = GroupByExpressions(group_by_exprs?);

                // Convert having clause if present
                if let Some(ref having) = gb.having_clause {
                    let mut having_expr: RenderExpr = having.clone().try_into()?;
                    apply_property_mapping_to_expr(&mut having_expr, &gb.input);
                    render_plan.having_clause = Some(having_expr);
                }

                Ok(render_plan)
            }
            LogicalPlan::GraphNode(gn) => {
                // GraphNode is a wrapper around a ViewScan that provides the Cypher alias
                // We need to preserve this alias in the FROM clause
                log::debug!(
                    "GraphNode::to_render_plan: input type={}",
                    match gn.input.as_ref() {
                        LogicalPlan::Union(_) => "Union",
                        LogicalPlan::ViewScan(_) => "ViewScan",
                        _ => "Other",
                    }
                );

                let mut render_plan = gn.input.to_render_plan(schema)?;

                log::debug!(
                    "GraphNode::to_render_plan: after input conversion, has_union={}",
                    render_plan.union.0.is_some()
                );

                // Apply GraphNode's alias to the FROM clause
                if let FromTableItem(Some(ref mut view_ref)) = render_plan.from {
                    view_ref.alias = Some(gn.alias.clone());
                    log::debug!(
                        "GraphNode.to_render_plan: Applied alias '{}' to FROM clause",
                        gn.alias
                    );
                }

                // Qualify unqualified Column expressions in SELECT items with the alias.
                // ViewScan generates bare Column("dest_city") without a table qualifier;
                // the SQL generator would then guess a table alias via heuristics (often "t").
                // By converting them to PropertyAccessExp here, the correct alias is used.
                for item in &mut render_plan.select.items {
                    if let RenderExpr::Column(Column(ref prop_val)) = item.expression {
                        let col_name = prop_val.raw().to_string();
                        item.expression = RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(gn.alias.clone()),
                            column: PropertyValue::Column(col_name),
                        });
                    }
                }

                Ok(render_plan)
            }
            LogicalPlan::ViewScan(vs) => {
                // ViewScan is a simple table scan - convert to basic RenderPlan
                // This is used for standalone node queries without relationships
                let select_items = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?,
                    distinct: false,
                };

                let from = FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: vs.source_table.clone(),
                    alias: None, // ViewScan doesn't have an alias at this level
                    use_final: vs.use_final,
                }));

                Ok(RenderPlan {
                    ctes: CteItems(vec![]),
                    select: select_items,
                    from,
                    joins: JoinItems(vec![]),
                    array_join: ArrayJoinItem(vec![]),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(vec![]),
                    having_clause: None,
                    order_by: OrderByItems(vec![]),
                    skip: SkipItem(None),
                    limit: LimitItem(None),
                    union: UnionItems(None),
                    fixed_path_info: None,
                    is_multi_label_scan: false,
                    // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                })
            }
            LogicalPlan::WithClause(with) => {
                log::warn!("🔍 Rendering WithClause");
                log::warn!(
                    "🔍 WithClause input type: {:?}",
                    std::mem::discriminant(with.input.as_ref())
                );

                // Check if the input contains a denormalized Union.
                // Denormalized Unions need special handling: each branch has different
                // property mappings (from_properties vs to_properties), so the flat
                // extractor approach (extract_from, extract_select_items, extract_filters)
                // cannot handle them — it collapses to one branch with wrong mappings.
                // Instead, use to_render_plan() which correctly renders both branches.
                fn input_has_denormalized_union(plan: &LogicalPlan) -> bool {
                    match plan {
                        LogicalPlan::Union(u) => u.inputs.iter().any(|input| {
                            fn has_denorm_vs(p: &LogicalPlan) -> bool {
                                match p {
                                    LogicalPlan::ViewScan(vs) => vs.is_denormalized,
                                    LogicalPlan::GraphNode(gn) => has_denorm_vs(gn.input.as_ref()),
                                    LogicalPlan::Filter(f) => has_denorm_vs(f.input.as_ref()),
                                    LogicalPlan::Projection(p) => has_denorm_vs(p.input.as_ref()),
                                    _ => false,
                                }
                            }
                            has_denorm_vs(input.as_ref())
                        }),
                        LogicalPlan::Filter(f) => input_has_denormalized_union(f.input.as_ref()),
                        LogicalPlan::GraphNode(gn) => {
                            input_has_denormalized_union(gn.input.as_ref())
                        }
                        LogicalPlan::Projection(p) => {
                            input_has_denormalized_union(p.input.as_ref())
                        }
                        _ => false,
                    }
                }

                let is_denormalized_input = input_has_denormalized_union(with.input.as_ref());

                // Handle WithClause by building a CTE from the input and creating a render plan with the CTE
                let has_aggregation = with
                    .items
                    .iter()
                    .any(|item| matches!(item.expression, LogicalExpr::AggregateFnCall(_)));

                let cte_content = if is_denormalized_input {
                    // Denormalized Union path: use to_render_plan() which correctly
                    // renders both UNION branches with per-branch property resolution
                    log::info!("🔧 WithClause: Using to_render_plan for denormalized Union input");
                    let mut input_plan = with.input.to_render_plan(schema)?;

                    // Apply WHERE clause from WITH if present
                    if let Some(where_clause) = &with.where_clause {
                        let render_where: RenderExpr =
                            where_clause.clone().try_into().map_err(|_| {
                                RenderBuildError::InvalidRenderPlan(
                                    "Failed to convert where clause".to_string(),
                                )
                            })?;
                        if has_aggregation {
                            input_plan.having_clause = Some(render_where);
                        } else {
                            input_plan.filters = match input_plan.filters.0 {
                                Some(existing) => FilterItems(Some(
                                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                                        operator: Operator::And,
                                        operands: vec![existing, render_where],
                                    }),
                                )),
                                None => FilterItems(Some(render_where)),
                            };
                        }
                    }

                    // Apply ORDER BY/SKIP/LIMIT from WITH clause
                    if let Some(order_by) = &with.order_by {
                        let items: Result<Vec<OrderByItem>, _> = order_by
                            .iter()
                            .map(|ob| OrderByItem::try_from(ob.clone()))
                            .collect();
                        input_plan.order_by = OrderByItems(items?);
                    }
                    if let Some(skip) = with.skip {
                        input_plan.skip = SkipItem(Some(skip as i64));
                    }
                    if let Some(limit) = with.limit {
                        input_plan.limit = LimitItem(Some(limit as i64));
                    }

                    CteContent::Structured(Box::new(input_plan))
                } else {
                    // Standard path: use flat extractors for non-Union inputs

                    log::warn!("🔍 Calling extract_filters on WithClause input...");
                    let mut cte_filters = FilterBuilder::extract_filters(with.input.as_ref())?;
                    log::warn!("🔍 extract_filters returned: {:?}", cte_filters);

                    let mut cte_having = with.input.extract_having()?;

                    if let Some(where_clause) = &with.where_clause {
                        let render_where: RenderExpr =
                            where_clause.clone().try_into().map_err(|_| {
                                RenderBuildError::InvalidRenderPlan(
                                    "Failed to convert where clause".to_string(),
                                )
                            })?;
                        if has_aggregation {
                            if cte_having.is_some() {
                                return Err(RenderBuildError::InvalidRenderPlan(
                                    "Multiple having clauses".to_string(),
                                ));
                            }
                            cte_having = Some(render_where);
                        } else if let Some(existing) = cte_filters {
                            cte_filters =
                                Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![existing, render_where],
                                }));
                        } else {
                            cte_filters = Some(render_where);
                        }
                    }

                    log::warn!("🔍🔍🔍 BEFORE extract_select_items for WITH.input");
                    let mut cte_select_items =
                        <LogicalPlan as SelectBuilder>::extract_select_items(
                            with.input.as_ref(),
                            None,
                        )?;
                    log::warn!(
                        "🔍🔍🔍 AFTER extract_select_items: got {} items",
                        cte_select_items.len()
                    );

                    // 🔧 FIX: Process scalar expressions from WITH items
                    // For expressions like `u.name AS userName`, we need to:
                    // 1. Rewrite property access (u.name → u.full_name based on schema)
                    // 2. Convert to SelectItem and add to cte_select_items
                    use crate::query_planner::logical_expr::expression_rewriter::{
                        rewrite_expression_with_property_mapping, ExpressionRewriteContext,
                    };
                    let rewrite_ctx = ExpressionRewriteContext::new(&with.input);

                    for item in &with.items {
                        // Skip TableAlias items (node pass-through) - they're already expanded
                        if matches!(&item.expression, LogicalExpr::TableAlias(_)) {
                            continue;
                        }

                        // Rewrite the expression to map properties to DB columns
                        let rewritten_expr = rewrite_expression_with_property_mapping(
                            &item.expression,
                            &rewrite_ctx,
                        );

                        // Convert to RenderExpr
                        let render_expr: RenderExpr = rewritten_expr.try_into().map_err(|e| {
                            RenderBuildError::InvalidRenderPlan(format!(
                                "Failed to convert WITH expression: {:?}",
                                e
                            ))
                        })?;

                        // Use the explicit alias from the WITH item
                        let col_alias = item.col_alias.as_ref().map(|ca| ColumnAlias(ca.0.clone()));

                        log::info!(
                            "🔧 Added WITH scalar expression: {:?} AS {:?}",
                            render_expr,
                            col_alias
                        );

                        cte_select_items.push(SelectItem {
                            expression: render_expr,
                            col_alias,
                        });
                    }

                    // ✅ FIX (Phase 6): Remap column aliases to match exported aliases
                    // When we have `WITH u AS person`, the select items will have aliases like `u.name`
                    // but they need to be remapped to `person.name` for the CTE output
                    let alias_mapping =
                        build_with_alias_mapping(&with.items, &with.exported_aliases);
                    if !alias_mapping.is_empty() {
                        cte_select_items =
                            remap_select_item_aliases(cte_select_items, &alias_mapping);
                    }

                    let mut temp_context = super::cte_generation::CteGenerationContext::new();
                    // Extract CTEs from WITH.input to populate context with CTE names
                    let _temp_ctes = extract_ctes_with_context(
                        &with.input,
                        "",
                        &mut temp_context,
                        schema,
                        None,
                    )?;

                    // Now extract FROM with context so VLP CTEs can be looked up
                    use crate::render_plan::from_builder::FromBuilder;
                    let cte_from =
                        FromTableItem(with.input.extract_from()?.and_then(|ft| ft.table));
                    let cte_joins = JoinItems::new(RenderPlanBuilder::extract_joins(
                        with.input.as_ref(),
                        schema,
                    )?);
                    let cte_group_by = GroupByExpressions(
                        <LogicalPlan as GroupByBuilder>::extract_group_by(with.input.as_ref())?,
                    );
                    let cte_order_by = OrderByItems(with.input.extract_order_by()?);
                    let cte_skip = SkipItem(with.input.extract_skip());
                    let cte_limit = LimitItem(with.input.extract_limit());

                    CteContent::Structured(Box::new(RenderPlan {
                        ctes: CteItems(vec![]),
                        select: SelectItems {
                            items: cte_select_items,
                            distinct: false,
                        },
                        from: cte_from,
                        joins: cte_joins,
                        array_join: ArrayJoinItem(vec![]),
                        filters: FilterItems(cte_filters),
                        group_by: cte_group_by,
                        having_clause: cte_having,
                        order_by: cte_order_by,
                        skip: cte_skip,
                        limit: cte_limit,
                        union: UnionItems(None),
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                    }))
                }; // end of if/else is_denormalized_input

                // Use CTE name from analyzer (includes counter for uniqueness)
                // The analyzer set this name using CteSchemaResolver with proper counter tracking
                // Format: "with_{sorted_aliases}_cte_{counter}" (e.g., "with_o_cte_0")
                let cte_name = with.cte_name.clone().unwrap_or_else(|| {
                    // FALLBACK: If analyzer didn't set cte_name (shouldn't happen after CteSchemaResolver)
                    log::warn!(
                        "⚠️ WithClause.cte_name is None, generating base name without counter"
                    );
                    generate_cte_base_name(&with.exported_aliases)
                });
                let mut cte = Cte::new(cte_name.clone(), cte_content, false);
                cte.with_exported_aliases = with.exported_aliases.clone();
                let ctes = CteItems(vec![cte]);

                let from = FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: cte_name.clone(),
                    alias: None,
                    use_final: false,
                }));

                let select = SelectItems {
                    items: vec![],
                    distinct: false,
                };
                let joins = JoinItems(vec![]);
                let array_join = ArrayJoinItem(vec![]);
                let filters = FilterItems(None);
                let group_by = GroupByExpressions(vec![]);
                let having_clause = None;
                let order_by = OrderByItems(vec![]);
                let skip = SkipItem(None);
                let limit = LimitItem(None);
                let union = UnionItems(None);

                Ok(RenderPlan {
                    ctes,
                    select,
                    from,
                    joins,
                    array_join,
                    filters,
                    group_by,
                    having_clause,
                    order_by,
                    skip,
                    limit,
                    union,
                    fixed_path_info: None,
                    is_multi_label_scan: false,
                    // cte_column_registry: registry, // REMOVED: No longer used
                })
            }
            LogicalPlan::CartesianProduct(cp) => {
                // CartesianProduct represents disconnected patterns (WITH...MATCH or OPTIONAL MATCH without overlap)
                // Strategy: Render left side as base, render right side and add as JOIN
                // - Non-optional: CROSS JOIN (or comma-join)
                // - Optional (is_optional=true): LEFT JOIN
                //
                // SPECIAL CASE: When OPTIONAL MATCH comes first (left is optional, right is required),
                // we need to SWAP the rendering order so that:
                // 1. Required pattern becomes FROM (base)
                // 2. Optional pattern becomes LEFT JOIN
                // This prevents generating invalid SQL where optional joins reference undefined aliases.

                let left_is_optional = cp.left.is_optional_pattern();
                let right_is_optional = cp.right.is_optional_pattern();

                log::info!(
                    "🔧 CartesianProduct.to_render_plan: is_optional={}, left_is_optional={}, right_is_optional={}, has_join_condition={}",
                    cp.is_optional,
                    left_is_optional,
                    right_is_optional,
                    cp.join_condition.is_some()
                );

                // Determine if we need to swap: left is optional BUT right is required
                let swap_order = left_is_optional && !right_is_optional;

                if swap_order {
                    log::info!("🔄 CartesianProduct: SWAPPING order - right (required) becomes FROM, left (optional) becomes LEFT JOIN");
                }

                // Render both sides
                let left_render = cp.left.to_render_plan(schema)?;

                // CRITICAL FIX: Pass CTE registry from left side to right side
                // When left is a WITH clause, it creates CTEs with column aliases
                // The right side needs to know about these to resolve property access expressions
                // REMOVED: CTE registry no longer used

                let right_render = cp.right.to_render_plan(schema)?;

                // Clear the CTE registry after rendering the right side
                // REMOVED: CTE registry no longer used

                // Decide which is base and which is joined based on swap_order
                let (mut base_render, joined_render, joined_is_optional) = if swap_order {
                    // Right becomes base, left (optional) becomes joined
                    (right_render, left_render, true)
                } else {
                    // Normal order: left is base, right is joined
                    (left_render, right_render, cp.is_optional)
                };

                // Merge CTEs from both sides
                let mut all_ctes = base_render.ctes.0;
                all_ctes.extend(joined_render.ctes.0);

                // Get the joined side's FROM as a JOIN target
                if let FromTableItem(Some(joined_from)) = &joined_render.from {
                    // Determine join type
                    let join_type = if joined_is_optional {
                        super::JoinType::Left
                    } else {
                        super::JoinType::Join // CROSS JOIN
                    };

                    // Build join condition if present
                    let joining_on: Vec<OperatorApplication> =
                        if let Some(ref join_cond) = cp.join_condition {
                            extract_join_condition_ops(join_cond).unwrap_or_default()
                        } else {
                            vec![]
                        };

                    // Create the JOIN from the joined side's FROM clause
                    let join = super::Join {
                        join_type: join_type.clone(),
                        table_name: joined_from.name.clone(),
                        table_alias: joined_from
                            .alias
                            .clone()
                            .unwrap_or_else(|| joined_from.name.clone()),
                        joining_on,
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    };

                    // Add to existing joins
                    base_render.joins.0.push(join);

                    // Add any joins from the joined side
                    // IMPORTANT: If we swapped and left was optional, these joins need to be LEFT JOINs
                    if swap_order {
                        // Convert all joined side's joins to LEFT JOIN since the whole pattern is optional
                        for mut j in joined_render.joins.0 {
                            j.join_type = super::JoinType::Left;
                            base_render.joins.0.push(j);
                        }
                    } else {
                        base_render.joins.0.extend(joined_render.joins.0);
                    }
                }

                // If base has no FROM but joined does, use joined's FROM as base
                if matches!(base_render.from, FromTableItem(None))
                    && !matches!(joined_render.from, FromTableItem(None))
                {
                    base_render.from = joined_render.from.clone();
                }

                // Merge select items if base has none
                if base_render.select.items.is_empty() {
                    base_render.select = joined_render.select;
                } else if swap_order && !joined_render.select.items.is_empty() {
                    // When swapping, we need to include both sides' select items
                    // The original left (now joined) had the main select items
                    base_render.select.items.extend(joined_render.select.items);
                }

                // Merge filters
                if let (FilterItems(Some(base_filter)), FilterItems(Some(joined_filter))) =
                    (&base_render.filters, &joined_render.filters)
                {
                    base_render.filters = FilterItems(Some(RenderExpr::OperatorApplicationExp(
                        OperatorApplication {
                            operator: Operator::And,
                            operands: vec![base_filter.clone(), joined_filter.clone()],
                        },
                    )));
                } else if matches!(base_render.filters, FilterItems(None)) {
                    base_render.filters = joined_render.filters;
                }

                base_render.ctes = CteItems(all_ctes);

                Ok(base_render)
            }
            LogicalPlan::Union(union) => {
                log::debug!("Union rendering: {} inputs", union.inputs.len());

                // Union - convert each branch to RenderPlan and combine with UNION ALL
                if union.inputs.is_empty() {
                    return Err(RenderBuildError::InvalidRenderPlan(
                        "Union has no inputs".to_string(),
                    ));
                }

                // SPECIAL CASE: Multi-label node scan (labelless queries like `MATCH (n) RETURN n`)
                // Detect if all Union inputs are ViewScans (possibly wrapped in GraphNode, GraphJoins, etc.)
                // In this case, use json_builder::generate_multi_type_union_sql() for uniform columns
                fn is_node_scan_input(plan: &LogicalPlan, depth: usize) -> bool {
                    let indent = "  ".repeat(depth);
                    match plan {
                        LogicalPlan::ViewScan(_) => {
                            log::debug!("{}ViewScan -> true", indent);
                            true
                        }
                        LogicalPlan::GraphNode(gn) => {
                            log::debug!("{}GraphNode -> checking input", indent);
                            is_node_scan_input(gn.input.as_ref(), depth + 1)
                        }
                        LogicalPlan::GraphJoins(gj) => {
                            log::debug!("{}GraphJoins -> checking input", indent);
                            is_node_scan_input(gj.input.as_ref(), depth + 1)
                        }
                        LogicalPlan::Union(u) => {
                            log::debug!(
                                "{}Union({} inputs) -> checking all",
                                indent,
                                u.inputs.len()
                            );
                            u.inputs
                                .iter()
                                .all(|i| is_node_scan_input(i.as_ref(), depth + 1))
                        }
                        LogicalPlan::Projection(p) => {
                            log::debug!("{}Projection -> checking input", indent);
                            is_node_scan_input(p.input.as_ref(), depth + 1)
                        }
                        LogicalPlan::Filter(f) => {
                            log::debug!("{}Filter -> checking input", indent);
                            is_node_scan_input(f.input.as_ref(), depth + 1)
                        }
                        LogicalPlan::Limit(l) => {
                            log::debug!("{}Limit -> checking input", indent);
                            is_node_scan_input(l.input.as_ref(), depth + 1)
                        }
                        _ => {
                            log::debug!("{}Other plan type -> false", indent);
                            false
                        }
                    }
                }

                log::debug!(
                    "Checking {} Union inputs for multi-label scan",
                    union.inputs.len()
                );
                let is_multi_label_scan = union
                    .inputs
                    .iter()
                    .all(|input| is_node_scan_input(input.as_ref(), 1));

                // Check if this is a denormalized single-label UNION (from/to positions)
                // Denormalized unions have the same label in all branches and should NOT
                // go through the multi-label json_builder path
                let is_denormalized_union = union.inputs.iter().any(|input| {
                    fn has_denormalized_view_scan(plan: &LogicalPlan) -> bool {
                        match plan {
                            LogicalPlan::ViewScan(vs) => vs.is_denormalized,
                            LogicalPlan::GraphNode(gn) => {
                                has_denormalized_view_scan(gn.input.as_ref())
                            }
                            LogicalPlan::Projection(p) => {
                                has_denormalized_view_scan(p.input.as_ref())
                            }
                            LogicalPlan::Filter(f) => has_denormalized_view_scan(f.input.as_ref()),
                            _ => false,
                        }
                    }
                    has_denormalized_view_scan(input.as_ref())
                });

                log::debug!(
                    "is_multi_label_scan={}, is_denormalized_union={}",
                    is_multi_label_scan,
                    is_denormalized_union
                );

                if is_multi_label_scan && union.inputs.len() > 1 && !is_denormalized_union {
                    log::info!(
                        "Multi-label node scan detected: {} ViewScans - using json_builder for uniform UNION",
                        union.inputs.len()
                    );

                    // Use json_builder to generate UNION with _label, _id, _properties columns
                    let union_sql = crate::clickhouse_query_generator::json_builder::generate_multi_type_union_sql(
                        schema.all_node_schemas(),
                        None, // LIMIT will be applied at outer query level
                    );

                    // Create a CTE with the UNION SQL
                    let cte_name = "__multi_label_union".to_string();
                    let cte = super::Cte::new(
                        cte_name.clone(),
                        CteContent::RawSql(union_sql),
                        false, // not recursive
                    );

                    // Create RenderPlan that selects from this CTE
                    // The alias is determined from the first ViewScan
                    let node_alias =
                        if let LogicalPlan::ViewScan(_first_scan) = union.inputs[0].as_ref() {
                            // Extract alias from the source table or use a default
                            // ViewScan doesn't store the alias directly, so we need to infer it
                            // For now, use "n" as default for labelless queries
                            "n".to_string()
                        } else {
                            "n".to_string()
                        };

                    let render_plan = RenderPlan {
                        ctes: CteItems(vec![cte]),
                        select: SelectItems {
                            items: vec![
                                SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(node_alias.clone()),
                                        column: PropertyValue::Column("_label".to_string()),
                                    }),
                                    col_alias: Some(ColumnAlias(format!("{}_label", node_alias))),
                                },
                                SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(node_alias.clone()),
                                        column: PropertyValue::Column("_id".to_string()),
                                    }),
                                    col_alias: Some(ColumnAlias(format!("{}_id", node_alias))),
                                },
                                SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(node_alias.clone()),
                                        column: PropertyValue::Column("_properties".to_string()),
                                    }),
                                    col_alias: Some(ColumnAlias(format!(
                                        "{}_properties",
                                        node_alias
                                    ))),
                                },
                            ],
                            distinct: false,
                        },
                        from: FromTableItem(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::Empty), // Placeholder - we're using CTE instead
                            name: cte_name,
                            alias: Some(node_alias.clone()),
                            use_final: false,
                        })),
                        joins: JoinItems(vec![]),
                        array_join: ArrayJoinItem(vec![]),
                        filters: FilterItems(None),
                        group_by: GroupByExpressions(vec![]),
                        having_clause: None,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                        fixed_path_info: None,
                        is_multi_label_scan: true, // Prevent Projection from overwriting SELECT
                    };

                    return Ok(render_plan);
                }

                // Regular UNION handling
                // Check if this is a path UNION query (GraphRel branches with path_variable)
                // Note: UNION branches can be either:
                // 1. GraphRel directly (from planner's UNION expansion)
                // 2. GraphJoins wrapping GraphRel (from other code paths)
                let has_path_variable = union.inputs.iter().any(|branch| {
                    // Helper to recursively find GraphRel and check path_variable
                    fn find_path_variable(plan: &LogicalPlan) -> bool {
                        match plan {
                            LogicalPlan::GraphRel(graph_rel) => {
                                log::warn!(
                                    "  Found GraphRel: alias={}, path_variable={:?}",
                                    graph_rel.alias,
                                    graph_rel.path_variable
                                );
                                graph_rel.path_variable.is_some()
                            }
                            LogicalPlan::GraphJoins(gj) => find_path_variable(&gj.input),
                            LogicalPlan::Projection(p) => find_path_variable(&p.input),
                            LogicalPlan::Filter(f) => find_path_variable(&f.input),
                            _ => false,
                        }
                    }
                    find_path_variable(branch.as_ref())
                });

                // Check if all branches are graph patterns (GraphRel or GraphJoins)
                let has_graph_patterns = union.inputs.iter().all(|input| {
                    matches!(
                        input.as_ref(),
                        LogicalPlan::GraphRel(_)
                            | LogicalPlan::GraphJoins(_)
                            | LogicalPlan::Projection(_)
                            | LogicalPlan::Filter(_)
                    )
                });

                let is_path_union = has_graph_patterns && has_path_variable;

                log::warn!("🔍 Path UNION detection: has_graph_patterns={}, has_path_variable={}, is_path_union={}",
                          has_graph_patterns, has_path_variable, is_path_union);

                // Convert first branch to get the base plan
                let first_input = &union.inputs[0];
                log::warn!(
                    "🔀 Union branch 0 plan type: {:?}",
                    std::mem::discriminant(first_input.as_ref())
                );
                let mut base_plan = first_input.to_render_plan(schema)?;

                // If there's only one branch, just return it
                if union.inputs.len() == 1 {
                    return Ok(base_plan);
                }

                // Convert remaining branches
                let mut union_branches = Vec::new();
                for (idx, input) in union.inputs.iter().enumerate().skip(1) {
                    log::warn!(
                        "🔀 Union branch {} plan type: {:?}",
                        idx,
                        std::mem::discriminant(input.as_ref())
                    );
                    let branch_plan = input.to_render_plan(schema)?;
                    union_branches.push(branch_plan);
                }

                // If this is a path UNION, convert to JSON format for uniform schema
                if is_path_union && union.inputs.len() > 1 {
                    log::info!(
                        "🎯 Path UNION query detected: {} branches - converting to JSON format for uniform schema",
                        union.inputs.len()
                    );

                    // Collect all branches (base + union branches)
                    let mut all_branches = vec![base_plan];
                    all_branches.extend(union_branches);

                    // Convert to JSON format with logical plans for explicit relationship type
                    let json_branches = super::plan_builder_helpers::convert_path_branches_to_json(
                        all_branches,
                        Some(&union.inputs),
                    );

                    // Split back into base + union branches
                    let mut iter = json_branches.into_iter();
                    base_plan = iter.next().expect("Should have at least one branch");
                    union_branches = iter.collect();
                }

                // Store union branches in the base plan
                let render_union_type = super::UnionType::try_from(union.union_type.clone())
                    .unwrap_or(super::UnionType::All);
                base_plan.union = UnionItems(Some(super::Union {
                    input: union_branches,
                    union_type: render_union_type,
                }));

                // 🔧 CRITICAL: After combining UNION branches, rewrite VLP endpoint aliases
                // This is the RIGHT place to do it - now the plan has the full UNION structure
                // with both base_plan.joins and union_branches defined
                log::warn!(
                    "❌❌❌ Union handler: About to call rewrite_vlp_union_branch_aliases ❌❌❌"
                );
                rewrite_vlp_union_branch_aliases(&mut base_plan)?;
                log::warn!(
                    "❌❌❌ Union handler: Finished rewrite_vlp_union_branch_aliases ❌❌❌"
                );

                Ok(base_plan)
            }
            LogicalPlan::Empty => {
                // Empty plan represents a pruned branch (e.g., no types matched a property filter)
                // Return an empty RenderPlan that will generate no rows
                // Must include at least one select item for valid SQL: SELECT 1 WHERE false
                log::info!(
                    "🔧 to_render_plan: Empty plan (pruned branch) - generating empty result"
                );
                Ok(RenderPlan {
                    ctes: CteItems(vec![]),
                    select: SelectItems {
                        items: vec![SelectItem {
                            expression: RenderExpr::Literal(super::render_expr::Literal::Integer(
                                1,
                            )),
                            col_alias: Some(ColumnAlias("_empty".to_string())),
                        }],
                        distinct: false,
                    },
                    from: FromTableItem(None),
                    joins: JoinItems(vec![]),
                    array_join: ArrayJoinItem(vec![]),
                    filters: FilterItems(Some(RenderExpr::Literal(
                        super::render_expr::Literal::Boolean(false),
                    ))), // WHERE false
                    group_by: GroupByExpressions(vec![]),
                    having_clause: None,
                    order_by: OrderByItems(vec![]),
                    skip: SkipItem(None),
                    limit: LimitItem(None),
                    union: UnionItems(None),
                    fixed_path_info: None,
                    is_multi_label_scan: false,
                })
            }
            _ => todo!(
                "Render plan conversion not implemented for LogicalPlan variant: {:?}",
                std::mem::discriminant(self)
            ),
        }
    }

    fn to_render_plan_with_ctx(
        &self,
        schema: &GraphSchema,
        plan_ctx: Option<&PlanCtx>,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        log::warn!(
            "🔀🔀🔀 to_render_plan_with_ctx ENTRY - plan type: {:?}",
            std::mem::discriminant(self)
        );

        // CRITICAL: If the plan contains WITH clauses, use the specialized handler
        // build_chained_with_match_cte_plan handles chained/nested WITH correctly
        // AND needs plan_ctx for VLP endpoint information
        use super::plan_builder_utils::{
            build_chained_with_match_cte_plan, has_with_clause_in_graph_rel,
        };

        let has_with_clause = has_with_clause_in_graph_rel(self);
        log::debug!(
            "to_render_plan_with_ctx: has_with_clause={}, plan_ctx available: {}, plan discriminant: {:?}",
            has_with_clause,
            plan_ctx.is_some(),
            std::mem::discriminant(self)
        );

        if has_with_clause {
            log::debug!("to_render_plan_with_ctx: Calling build_chained_with_match_cte_plan with plan_ctx={}", plan_ctx.is_some());
            return build_chained_with_match_cte_plan(self, schema, plan_ctx);
        }

        // For non-WITH clause queries, we need to pass plan_ctx through to extract_select_items
        // to enable TypedVariable lookup (particularly for path variables)
        // The plan may be wrapped in Limit/Skip/OrderBy, so we need to check the structure

        // Helper to check if plan contains GraphJoins (handles Limit, Skip, OrderBy wrappers)
        fn contains_graph_joins(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::GraphJoins(_) => true,
                LogicalPlan::Limit(l) => contains_graph_joins(&l.input),
                LogicalPlan::Skip(s) => contains_graph_joins(&s.input),
                LogicalPlan::OrderBy(o) => contains_graph_joins(&o.input),
                LogicalPlan::Filter(f) => contains_graph_joins(&f.input),
                _ => false,
            }
        }

        // Helper to check if plan's core is Empty (all node types filtered out by Track C)
        fn core_is_empty(plan: &LogicalPlan) -> bool {
            let result = match plan {
                LogicalPlan::Empty => true,
                LogicalPlan::Limit(l) => core_is_empty(&l.input),
                LogicalPlan::Skip(s) => core_is_empty(&s.input),
                LogicalPlan::OrderBy(o) => core_is_empty(&o.input),
                LogicalPlan::Filter(f) => core_is_empty(&f.input),
                LogicalPlan::Projection(p) => core_is_empty(&p.input),
                LogicalPlan::GraphJoins(gj) => core_is_empty(&gj.input),
                LogicalPlan::GraphNode(gn) => {
                    log::debug!("core_is_empty: GraphNode, checking input");
                    core_is_empty(&gn.input)
                }
                _ => {
                    log::debug!(
                        "core_is_empty: unhandled variant {:?}",
                        std::mem::discriminant(plan)
                    );
                    false
                }
            };
            log::debug!(
                "core_is_empty: {:?} -> {}",
                std::mem::discriminant(plan),
                result
            );
            result
        }

        // EARLY EXIT: If the plan's core is Empty (Track C filtered all types),
        // return empty result immediately to avoid generating SQL without FROM clause
        if core_is_empty(self) {
            log::debug!(
                "to_render_plan_with_ctx: Plan core is Empty (all types filtered) - generating empty result"
            );
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items: vec![SelectItem {
                        expression: RenderExpr::Literal(super::render_expr::Literal::Integer(1)),
                        col_alias: Some(ColumnAlias("_empty".to_string())),
                    }],
                    distinct: false,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(vec![]),
                filters: FilterItems(Some(RenderExpr::Literal(
                    super::render_expr::Literal::Boolean(false),
                ))), // WHERE false
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
                fixed_path_info: None,
                is_multi_label_scan: false,
            });
        }

        // Helper to check if plan contains Union (handles Limit, Skip, OrderBy wrappers)
        fn contains_union(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Union(_) => true,
                LogicalPlan::Limit(l) => contains_union(&l.input),
                LogicalPlan::Skip(s) => contains_union(&s.input),
                LogicalPlan::OrderBy(o) => contains_union(&o.input),
                LogicalPlan::Filter(f) => contains_union(&f.input),
                LogicalPlan::Projection(p) => contains_union(&p.input),
                _ => false,
            }
        }

        // If this plan contains a Union, handle it with plan_ctx for path variables
        // Check BEFORE GraphJoins because Union branches may contain GraphJoins
        if contains_union(self) {
            log::warn!("🔀 to_render_plan_with_ctx: Plan contains Union, checking if also contains GraphJoins...");

            // If we have Union but NO top-level GraphJoins, handle Union directly
            // This happens when structure is: Limit → Union → [GraphJoins → GraphRel, ...]
            let has_graph_joins = contains_graph_joins(self);
            log::warn!(
                "🔀 to_render_plan_with_ctx: contains_graph_joins={}",
                has_graph_joins
            );

            if !has_graph_joins {
                // Unwrap Limit/Skip/OrderBy to find Union
                let union_node = match self {
                    LogicalPlan::Union(_) => self,
                    LogicalPlan::Limit(l) => l.input.as_ref(),
                    LogicalPlan::Skip(s) => s.input.as_ref(),
                    LogicalPlan::OrderBy(o) => o.input.as_ref(),
                    _ => self,
                };

                if let LogicalPlan::Union(union) = union_node {
                    log::warn!("🔀 to_render_plan_with_ctx: Direct Union (no top-level GraphJoins), rendering branches with plan_ctx");

                    if union.inputs.is_empty() {
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Union has no inputs".to_string(),
                        ));
                    }

                    // Render each branch with plan_ctx
                    let mut branch_renders = Vec::new();
                    for (idx, branch) in union.inputs.iter().enumerate() {
                        log::debug!(
                            "🔀 Rendering Union branch {} type: {:?}, with plan_ctx",
                            idx,
                            std::mem::discriminant(branch.as_ref())
                        );
                        let branch_render = branch.to_render_plan_with_ctx(schema, plan_ctx)?;
                        branch_renders.push(branch_render);
                    }

                    // Check if this is a path UNION query by examining the logical plan
                    // Note: UNION branches can be either:
                    // 1. GraphRel directly (from planner's UNION expansion)
                    // 2. GraphJoins wrapping GraphRel (from other code paths)
                    let has_path_variable = union.inputs.iter().any(|branch| {
                        // Helper to recursively find GraphRel and check path_variable
                        fn find_path_variable(plan: &LogicalPlan) -> bool {
                            match plan {
                                LogicalPlan::GraphRel(graph_rel) => {
                                    log::warn!(
                                        "  Found GraphRel: alias={}, path_variable={:?}",
                                        graph_rel.alias,
                                        graph_rel.path_variable
                                    );
                                    graph_rel.path_variable.is_some()
                                }
                                LogicalPlan::GraphJoins(gj) => find_path_variable(&gj.input),
                                LogicalPlan::Projection(p) => find_path_variable(&p.input),
                                LogicalPlan::Filter(f) => find_path_variable(&f.input),
                                _ => false,
                            }
                        }
                        find_path_variable(branch.as_ref())
                    });

                    // Check if all branches are graph patterns (GraphRel or GraphJoins)
                    let has_graph_patterns = union.inputs.iter().all(|input| {
                        matches!(
                            input.as_ref(),
                            LogicalPlan::GraphRel(_)
                                | LogicalPlan::GraphJoins(_)
                                | LogicalPlan::Projection(_)
                                | LogicalPlan::Filter(_)
                        )
                    });

                    let is_path_union = has_graph_patterns && has_path_variable;

                    log::warn!("🔍 Path UNION detection (ctx): has_graph_patterns={}, has_path_variable={}, is_path_union={}",
                              has_graph_patterns, has_path_variable, is_path_union);

                    if is_path_union && union.inputs.len() > 1 {
                        log::warn!(
                            "🎯 Path UNION query detected: {} branches - converting to JSON format for uniform schema",
                            union.inputs.len()
                        );

                        // Convert branches to JSON format: p, _start_properties, _end_properties, _rel_properties, __rel_type__
                        branch_renders = super::plan_builder_helpers::convert_path_branches_to_json(
                            branch_renders,
                            Some(&union.inputs),
                        );
                    } else {
                        // Always normalize UNION branches for consistent column schema.
                        // NULL padding + toString wrapping ensures ClickHouse UNION ALL works.
                        log::warn!("🔀 Normalizing {} UNION branches for consistent column schema (is_path_union={}, inputs.len={})",
                                  branch_renders.len(), is_path_union, union.inputs.len());
                        branch_renders =
                            super::plan_builder_helpers::normalize_union_branches(branch_renders);

                        // Classify each branch as node-only or has-relationship
                        fn contains_graph_rel(plan: &LogicalPlan) -> bool {
                            match plan {
                                LogicalPlan::GraphRel(_) => true,
                                LogicalPlan::GraphNode(gn) => contains_graph_rel(&gn.input),
                                LogicalPlan::Projection(p) => contains_graph_rel(&p.input),
                                LogicalPlan::Filter(f) => contains_graph_rel(&f.input),
                                LogicalPlan::GraphJoins(gj) => contains_graph_rel(&gj.input),
                                LogicalPlan::Limit(l) => contains_graph_rel(&l.input),
                                LogicalPlan::Skip(s) => contains_graph_rel(&s.input),
                                LogicalPlan::OrderBy(o) => contains_graph_rel(&o.input),
                                _ => false,
                            }
                        }

                        // Check if the RETURN clause returns whole nodes/relationships
                        // (bare variable like `RETURN n`) vs specific properties
                        // (`RETURN n.name`). __label__ is only needed for whole-node returns.
                        fn returns_whole_entity(plan: &LogicalPlan) -> bool {
                            match plan {
                                LogicalPlan::Projection(p) => {
                                    p.items.iter().any(|item| {
                                        matches!(&item.expression,
                                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(_))
                                    })
                                }
                                LogicalPlan::Limit(l) => returns_whole_entity(&l.input),
                                LogicalPlan::Skip(s) => returns_whole_entity(&s.input),
                                LogicalPlan::OrderBy(o) => returns_whole_entity(&o.input),
                                LogicalPlan::Filter(f) => returns_whole_entity(&f.input),
                                LogicalPlan::GraphJoins(gj) => returns_whole_entity(&gj.input),
                                _ => false,
                            }
                        }

                        let rel_count = union.inputs.iter()
                            .filter(|input| contains_graph_rel(input.as_ref()))
                            .count();
                        let all_have_rels = rel_count == union.inputs.len();
                        let none_have_rels = rel_count == 0;
                        let has_whole_entity_return = union.inputs.iter()
                            .any(|input| returns_whole_entity(input.as_ref()));
                        log::debug!(
                            "🔀 Label decision: none_have_rels={}, all_have_rels={}, has_whole_entity_return={}",
                            none_have_rels, all_have_rels, has_whole_entity_return
                        );

                        // Add label columns only when:
                        // 1. ALL branches are the same kind (all nodes or all rels)
                        // 2. The RETURN clause returns whole entities (RETURN n, not RETURN n.name)
                        if none_have_rels && has_whole_entity_return {
                            log::info!("🏷️ Adding __label__ column for whole-node UNION return");
                            branch_renders =
                                super::plan_builder_helpers::add_label_column_to_union_branches(
                                    branch_renders,
                                    &union.inputs,
                                    schema,
                                );
                        } else if all_have_rels && has_whole_entity_return {
                            log::info!("🏷️ Adding __start_label__/__end_label__ columns for whole-relationship UNION return");
                            branch_renders =
                                super::plan_builder_helpers::add_path_label_columns_to_union_branches(
                                    branch_renders,
                                    &union.inputs,
                                    schema,
                                );
                        } else {
                            log::info!("🔀 UNION with specific property returns: no label columns needed");
                        }
                    }

                    // Use first branch as base and put rest in union.input
                    let mut all_renders: Vec<RenderPlan> = branch_renders.into_iter().collect();

                    // Collect ALL CTEs from all branches first
                    // When multiple branches produce CTEs with the same name (e.g., both
                    // User→Post and User→User branches create vlp_multi_type_a_b), rename
                    // the duplicate and update the branch's FROM reference to match.
                    let mut all_ctes: Vec<super::Cte> = Vec::new();
                    for render in all_renders.iter_mut() {
                        let mut renamed = Vec::new(); // (old_name, new_name)
                        for cte in &render.ctes.0 {
                            if let Some(existing_idx) =
                                all_ctes.iter().position(|e| e.cte_name == cte.cte_name)
                            {
                                // If existing CTE is empty and this one isn't, replace it
                                let existing_empty = matches!(&all_ctes[existing_idx].content,
                                    super::CteContent::RawSql(s) if s.contains("WHERE 0 = 1"));
                                let new_empty = matches!(&cte.content,
                                    super::CteContent::RawSql(s) if s.contains("WHERE 0 = 1"));
                                if existing_empty && !new_empty {
                                    log::info!(
                                        "🔀 UNION: Replacing empty CTE '{}' with non-empty version",
                                        cte.cte_name
                                    );
                                    all_ctes[existing_idx] = cte.clone();
                                } else if !new_empty {
                                    // Both non-empty with same name: check if content is identical
                                    let same_content =
                                        match (&all_ctes[existing_idx].content, &cte.content) {
                                            (
                                                super::CteContent::RawSql(a),
                                                super::CteContent::RawSql(b),
                                            ) => a == b,
                                            _ => false,
                                        };
                                    if same_content {
                                        // Exact duplicate — skip it
                                        log::debug!(
                                            "UNION: Skipping identical duplicate CTE '{}'",
                                            cte.cte_name
                                        );
                                    } else {
                                        // Different content, same name: rename the new one
                                        let mut suffix = 2;
                                        let base_name = cte.cte_name.clone();
                                        let mut new_name = format!("{}_{}", base_name, suffix);
                                        while all_ctes.iter().any(|e| e.cte_name == new_name) {
                                            suffix += 1;
                                            new_name = format!("{}_{}", base_name, suffix);
                                        }
                                        log::debug!(
                                            "UNION: Renaming duplicate CTE '{}' → '{}'",
                                            base_name,
                                            new_name
                                        );
                                        let mut renamed_cte = cte.clone();
                                        renamed_cte.cte_name = new_name.clone();
                                        renamed.push((base_name, new_name));
                                        all_ctes.push(renamed_cte);
                                    }
                                }
                            } else {
                                log::debug!("UNION: Collecting CTE '{}'", cte.cte_name);
                                all_ctes.push(cte.clone());
                            }
                        }
                        // Update FROM reference for renamed CTEs
                        for (old_name, new_name) in renamed {
                            if let Some(ref mut from_ref) = render.from.0 {
                                if from_ref.name == old_name {
                                    log::debug!(
                                        "UNION: Updating FROM '{}' → '{}'",
                                        old_name,
                                        new_name
                                    );
                                    from_ref.name = new_name;
                                }
                            }
                        }
                    }

                    // Filter out branches reading from empty VLP CTEs (WHERE 0 = 1)
                    // and dedup identical VLP branches
                    if all_renders.len() > 1 {
                        let mut kept_renders: Vec<RenderPlan> = Vec::new();
                        for render in all_renders {
                            let from_name = render.from.0.as_ref().map(|f| f.name.as_str());

                            // Skip branches reading from empty VLP CTEs
                            if let Some(name) = from_name {
                                if name.starts_with("vlp_multi_type_") {
                                    let is_empty = all_ctes.iter().any(|cte| {
                                        cte.cte_name == name
                                            && matches!(&cte.content, super::CteContent::RawSql(s) if s.contains("WHERE 0 = 1"))
                                    });
                                    if is_empty {
                                        log::info!(
                                            "🔀 UNION: Skipping branch reading from empty CTE '{}'",
                                            name
                                        );
                                        continue;
                                    }
                                }
                            }

                            // Skip branches reading from reverse VLP CTE when forward CTE
                            // already includes incoming edges (undirected).
                            // Only skip if the forward branch is ALREADY in kept_renders.
                            if let Some(name) = from_name {
                                if let Some(suffix) = name.strip_prefix("vlp_multi_type_") {
                                    if let Some(sep_pos) = suffix.find('_') {
                                        let start = &suffix[..sep_pos];
                                        let end = &suffix[sep_pos + 1..];
                                        let forward_name =
                                            format!("vlp_multi_type_{}_{}", end, start);
                                        // Only skip if the forward CTE exists AND is already kept
                                        let forward_kept = kept_renders.iter().any(|kept| {
                                            kept.from
                                                .0
                                                .as_ref()
                                                .is_some_and(|f| f.name == forward_name)
                                        });
                                        if forward_kept {
                                            log::info!("🔀 UNION: Skipping redundant reverse VLP branch '{}' (forward '{}' already kept)", name, forward_name);
                                            continue;
                                        }
                                    }
                                }
                            }

                            // Dedup: skip if identical to an already-kept branch
                            let is_dup = kept_renders.iter().any(|kept| {
                                let kept_from = kept.from.0.as_ref().map(|f| f.name.as_str());
                                if from_name == kept_from
                                    && from_name.is_some_and(|n| {
                                        n.starts_with("vlp_multi_type_")
                                            || n.starts_with("pattern_union_")
                                    })
                                    && render.select.items.len() == kept.select.items.len()
                                    && render.filters == kept.filters
                                {
                                    render
                                        .select
                                        .items
                                        .iter()
                                        .zip(kept.select.items.iter())
                                        .all(|(a, b)| a.col_alias == b.col_alias)
                                } else {
                                    false
                                }
                            });
                            if is_dup {
                                log::info!(
                                    "🔀 UNION: Deduplicating identical CTE branch from '{}'",
                                    from_name.unwrap_or("?")
                                );
                                continue;
                            }

                            kept_renders.push(render);
                        }
                        all_renders = kept_renders;
                    }

                    // Each rendered branch may have its own inner union (from
                    // TypeInference expanding unlabeled nodes) and per-arm LIMIT.
                    // When any branch is complex, put ALL branches in union.input
                    // so render_union_branch_sql wraps each in a subquery.
                    let any_complex = all_renders.iter().any(|r| {
                        r.union.0.is_some() || r.limit.0.is_some()
                    });

                    let mut base_render = all_renders.remove(0);
                    base_render.ctes.0 = all_ctes;

                    if any_complex && base_render.union.0.is_some() {
                        // Base has inner union — can't use it as first branch directly.
                        // Put ALL branches (including base) into union.input.
                        let mut all_branches = vec![base_render];
                        all_branches.extend(all_renders);

                        // Create shell base that holds CTEs + union list
                        let all_ctes_collected: Vec<super::Cte> = all_branches.iter()
                            .flat_map(|b| b.ctes.0.iter().cloned())
                            .collect();
                        // Dedup CTEs by name (keep first occurrence)
                        let mut seen_cte_names = std::collections::HashSet::new();
                        let deduped_ctes: Vec<super::Cte> = all_ctes_collected.into_iter()
                            .filter(|cte| seen_cte_names.insert(cte.cte_name.clone()))
                            .collect();
                        // Clear CTEs from branches (they live on the shell base)
                        for branch in &mut all_branches {
                            branch.ctes.0.clear();
                        }

                        let render_union_type =
                            super::UnionType::try_from(union.union_type.clone())
                                .unwrap_or(super::UnionType::All);

                        base_render = RenderPlan {
                            ctes: CteItems(deduped_ctes),
                            select: SelectItems { items: vec![], distinct: false },
                            from: FromTableItem(None),
                            joins: JoinItems::new(vec![]),
                            array_join: ArrayJoinItem(vec![]),
                            filters: FilterItems(None),
                            group_by: GroupByExpressions(vec![]),
                            having_clause: None,
                            order_by: OrderByItems(vec![]),
                            skip: SkipItem(None),
                            limit: LimitItem(None),
                            union: UnionItems(Some(super::Union {
                                input: all_branches,
                                union_type: render_union_type,
                            })),
                            fixed_path_info: None,
                            is_multi_label_scan: false,
                        };
                    } else if !all_renders.is_empty() {
                        let render_union_type =
                            super::UnionType::try_from(union.union_type.clone())
                                .unwrap_or(super::UnionType::All);
                        base_render.union = UnionItems(Some(super::Union {
                            input: all_renders,
                            union_type: render_union_type,
                        }));
                    }

                    // Apply Limit/OrderBy/Skip from wrapper nodes
                    fn apply_wrappers(
                        plan: &LogicalPlan,
                        render: &mut RenderPlan,
                    ) -> Result<(), RenderBuildError> {
                        match plan {
                            LogicalPlan::Limit(l) => {
                                render.limit = LimitItem(Some(l.count));
                                apply_wrappers(&l.input, render)?;
                            }
                            LogicalPlan::OrderBy(ob) => {
                                let order_by_items: Result<Vec<OrderByItem>, _> = ob
                                    .items
                                    .iter()
                                    .map(|item| item.clone().try_into())
                                    .collect();
                                render.order_by = OrderByItems(order_by_items?);
                                apply_wrappers(&ob.input, render)?;
                            }
                            LogicalPlan::Skip(s) => {
                                render.skip = SkipItem(Some(s.count));
                                apply_wrappers(&s.input, render)?;
                            }
                            _ => {}
                        }
                        Ok(())
                    }
                    apply_wrappers(self, &mut base_render)?;

                    return Ok(base_render);
                }
            }

            // Has top-level GraphJoins, fall through to GraphJoins handling below
        }

        // If this plan contains GraphJoins, we handle it ourselves with plan_ctx
        if contains_graph_joins(self) {
            log::info!("to_render_plan_with_ctx: plan contains GraphJoins, using plan_ctx for SELECT extraction");

            let mut select_items = SelectItems {
                items: <LogicalPlan as SelectBuilder>::extract_select_items(self, plan_ctx)?,
                distinct: FilterBuilder::extract_distinct(self),
            };
            let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));
            let joins = JoinItems::new(RenderPlanBuilder::extract_joins(self, schema)?);
            let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
            let filters = FilterItems(FilterBuilder::extract_filters(self)?);
            let group_by =
                GroupByExpressions(<LogicalPlan as GroupByBuilder>::extract_group_by(self)?);
            let having_clause = self.extract_having()?;

            select_items.items =
                apply_anylast_wrapping_for_group_by(select_items.items, &group_by.0, self)?;

            let order_by = OrderByItems(self.extract_order_by()?);
            // Use utility functions that properly handle Limit/Skip wrappers
            let skip = SkipItem(super::plan_builder_utils::extract_skip(self));
            let limit = LimitItem(super::plan_builder_utils::extract_limit(self));
            // Use extract_union_with_ctx to pass plan_ctx through to Union branches
            let union = UnionItems(self.extract_union_with_ctx(schema, plan_ctx)?);

            // Extract CTEs from the inner plan
            let cte_input = match self {
                LogicalPlan::GraphJoins(gj) => &gj.input,
                LogicalPlan::Limit(l) => match l.input.as_ref() {
                    LogicalPlan::GraphJoins(gj) => &gj.input,
                    other => other,
                },
                LogicalPlan::Skip(s) => match s.input.as_ref() {
                    LogicalPlan::GraphJoins(gj) => &gj.input,
                    other => other,
                },
                LogicalPlan::OrderBy(o) => match o.input.as_ref() {
                    LogicalPlan::GraphJoins(gj) => &gj.input,
                    other => other,
                },
                other => other,
            };

            let mut context = super::cte_generation::CteGenerationContext::new();
            let ctes = CteItems(extract_ctes_with_context(
                cte_input,
                "",
                &mut context,
                schema,
                plan_ctx,
            )?);

            let mut render_plan = RenderPlan {
                ctes,
                select: select_items,
                from,
                joins,
                array_join,
                filters,
                group_by,
                having_clause,
                order_by,
                skip,
                limit,
                union,
                fixed_path_info: None,
                is_multi_label_scan: false,
            };

            rewrite_vlp_union_branch_aliases(&mut render_plan)?;

            // Handle RETURN-context pattern comprehensions (same logic as GraphJoins match arm)
            let gj_input = match self {
                LogicalPlan::GraphJoins(gj) => Some(&gj.input),
                LogicalPlan::Limit(l) => {
                    if let LogicalPlan::GraphJoins(gj) = l.input.as_ref() {
                        Some(&gj.input)
                    } else {
                        None
                    }
                }
                LogicalPlan::Skip(s) => {
                    if let LogicalPlan::GraphJoins(gj) = s.input.as_ref() {
                        Some(&gj.input)
                    } else {
                        None
                    }
                }
                LogicalPlan::OrderBy(o) => {
                    if let LogicalPlan::GraphJoins(gj) = o.input.as_ref() {
                        Some(&gj.input)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(gj_input) = gj_input {
                let pattern_comps = find_pattern_comprehensions_in_plan(gj_input);
                if !pattern_comps.is_empty() {
                    log::info!(
                        "🔧 GraphJoins(with_ctx): Found {} pattern comprehension(s) in nested plan",
                        pattern_comps.len()
                    );
                    for (pc_idx, pc_meta) in pattern_comps.iter().enumerate() {
                        let pc_cte_name =
                            format!("pattern_comp_{}_{}", pc_meta.correlation_var, pc_idx);

                        if let Some(pc_sql) =
                            super::plan_builder_utils::build_pattern_comprehension_sql(
                                &pc_meta.correlation_label,
                                &pc_meta.direction,
                                &pc_meta.rel_types,
                                &pc_meta.agg_type,
                                schema,
                                pc_meta.target_label.as_deref(),
                                pc_meta.target_property.as_deref(),
                            )
                        {
                            let pc_cte = super::Cte::new(
                                pc_cte_name.clone(),
                                super::CteContent::RawSql(pc_sql),
                                false,
                            );
                            render_plan.ctes.0.push(pc_cte);

                            let from_alias = render_plan
                                .from
                                .0
                                .as_ref()
                                .map(|f| f.alias.clone().unwrap_or_else(|| f.name.clone()))
                                .unwrap_or_else(|| pc_meta.correlation_var.clone());

                            let pc_alias = format!("__pc_{}", pc_idx);
                            let lhs_expr = super::plan_builder_utils::build_node_id_expr_for_join(
                                &from_alias,
                                &pc_meta.correlation_label,
                                schema,
                            );
                            let on_clause = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    lhs_expr,
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(pc_alias.clone()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("node_id".to_string()),
                                    }),
                                ],
                            };

                            render_plan.joins.0.push(super::Join {
                                table_name: pc_cte_name.clone(),
                                table_alias: pc_alias.clone(),
                                joining_on: vec![on_clause],
                                join_type: super::JoinType::Left,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });

                            // Replace with coalesce(__pc_N.result, default)
                            let result_alias = pc_meta.result_alias.clone();
                            let default_val = if matches!(
                                pc_meta.agg_type,
                                crate::query_planner::logical_plan::AggregationType::GroupArray
                            ) {
                                RenderExpr::List(vec![])
                            } else {
                                RenderExpr::Literal(Literal::Integer(0))
                            };
                            let coalesce_expr = RenderExpr::ScalarFnCall(
                                super::render_expr::ScalarFnCall {
                                    name: "coalesce".to_string(),
                                    args: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(pc_alias),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column("result".to_string()),
                                        }),
                                        default_val,
                                    ],
                                }
                            );

                            for item in &mut render_plan.select.items {
                                if let Some(ref alias) = item.col_alias {
                                    if alias.0 == result_alias {
                                        item.expression = coalesce_expr.clone();
                                        break;
                                    }
                                }
                            }

                            // Remove GROUP BY — CTE+JOIN doesn't need it
                            render_plan.group_by.0.clear();

                            log::info!(
                                "✅ Added pattern comp CTE '{}' with LEFT JOIN (with_ctx path)",
                                pc_cte_name
                            );
                        }
                    }
                }
            }

            return Ok(render_plan);
        }

        // If this is a Union without GraphJoins, handle it specially to pass plan_ctx to branches
        if let LogicalPlan::Union(union) = self {
            log::warn!("🔀 to_render_plan_with_ctx: Union without GraphJoins, rendering branches with plan_ctx");

            if union.inputs.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Union has no inputs".to_string(),
                ));
            }

            // Render each branch with plan_ctx
            let mut branch_renders = Vec::new();
            for (idx, branch) in union.inputs.iter().enumerate() {
                log::warn!(
                    "🔀 Rendering Union branch {} type: {:?}, with plan_ctx",
                    idx,
                    std::mem::discriminant(branch.as_ref())
                );
                let branch_render = branch.to_render_plan_with_ctx(schema, plan_ctx)?;
                branch_renders.push(branch_render);
            }

            // Use first branch as base and put rest in union.input
            let mut base_render = branch_renders.into_iter().next().unwrap();

            // Set union field if there are multiple branches
            if union.inputs.len() > 1 {
                let remaining_renders: Vec<RenderPlan> = union.inputs[1..]
                    .iter()
                    .enumerate()
                    .map(|(idx, branch)| {
                        log::debug!("Converting Union branch {} to RenderPlan", idx + 1);
                        branch.to_render_plan_with_ctx(schema, plan_ctx)
                    })
                    .collect::<RenderPlanBuilderResult<Vec<_>>>()?;

                let render_union_type = super::UnionType::try_from(union.union_type.clone())
                    .unwrap_or(super::UnionType::All);
                base_render.union = UnionItems(Some(super::Union {
                    input: remaining_renders,
                    union_type: render_union_type,
                }));
            }

            return Ok(base_render);
        }

        // Handle Projection with plan_ctx to enable path variable property expansion
        if let LogicalPlan::Projection(p) = self {
            log::warn!(
                "🔀 to_render_plan_with_ctx: Projection detected, rendering input with plan_ctx"
            );

            // Render input with plan_ctx
            let mut render_plan = p.input.to_render_plan_with_ctx(schema, plan_ctx)?;

            // Multi-label scan: Skip SELECT overwriting to preserve special columns
            if render_plan.is_multi_label_scan {
                log::info!(
                    "🎯 Projection over multi-label scan: preserving special SELECT columns"
                );
                render_plan.select.distinct = p.distinct;
                return Ok(render_plan);
            }

            // Extract select items WITH plan_ctx for path variable expansion
            log::warn!(
                "🔀 Projection: extracting select items with plan_ctx for property expansion"
            );
            let select_items =
                <LogicalPlan as SelectBuilder>::extract_select_items(self, plan_ctx)?;
            render_plan.select = SelectItems {
                items: select_items,
                distinct: p.distinct,
            };

            return Ok(render_plan);
        }

        // Filter WITH plan_ctx - need to extract CTEs with plan_ctx before delegating
        if let LogicalPlan::Filter(filter) = self {
            // Check if the filter contains GraphRel (VLP) which needs plan_ctx for CTE generation
            if matches!(filter.input.as_ref(), LogicalPlan::GraphRel(_)) {
                log::debug!(
                    "to_render_plan_with_ctx: Filter(GraphRel), extracting CTEs with plan_ctx"
                );

                // Extract CTEs WITH plan_ctx so VLP generator gets property requirements
                let mut context = super::cte_generation::CteGenerationContext::new();
                let ctes = CteItems(extract_ctes_with_context(
                    &filter.input,
                    "",
                    &mut context,
                    schema,
                    plan_ctx,
                )?);

                // Now render the rest using standard path
                let mut render_plan = self.to_render_plan(schema)?;

                // Replace CTEs with the ones we extracted with plan_ctx
                render_plan.ctes = ctes;

                return Ok(render_plan);
            }
        }

        // GraphRel WITH plan_ctx (for UNION branches with path variables)
        if let LogicalPlan::GraphRel(graph_rel) = self {
            log::debug!(
                "to_render_plan_with_ctx: GraphRel MATCHED, path_variable='{:?}'",
                graph_rel.path_variable
            );

            // Extract CTEs WITH plan_ctx so VLP generator gets property requirements
            let mut context = super::cte_generation::CteGenerationContext::new();
            let ctes = CteItems(extract_ctes_with_context(
                self,
                "",
                &mut context,
                schema,
                plan_ctx,
            )?);

            // Now render the rest using standard path
            let mut render_plan = self.to_render_plan(schema)?;

            // Replace CTEs with the ones we extracted with plan_ctx
            render_plan.ctes = ctes;

            // Then extract select items WITH plan_ctx for path variable expansion
            if graph_rel.path_variable.is_some() {
                log::debug!(
                    "GraphRel: extracting select items with plan_ctx for path variable expansion"
                );
                let select_items =
                    <LogicalPlan as SelectBuilder>::extract_select_items(self, plan_ctx)?;
                render_plan.select = SelectItems {
                    items: select_items,
                    distinct: false,
                };
            }

            return Ok(render_plan);
        }

        // GraphJoins WITH plan_ctx (for UNION branches that wrap GraphRel)
        if let LogicalPlan::GraphJoins(_gj) = self {
            log::debug!(
                "to_render_plan_with_ctx: GraphJoins, passing plan_ctx to extract_select_items"
            );

            // Use standard GraphJoins rendering but with plan_ctx for select items
            let mut select_items = SelectItems {
                items: <LogicalPlan as SelectBuilder>::extract_select_items(self, plan_ctx)?,
                distinct: FilterBuilder::extract_distinct(self),
            };
            let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));
            let joins = JoinItems::new(RenderPlanBuilder::extract_joins(self, schema)?);
            let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
            let filters = FilterItems(FilterBuilder::extract_filters(self)?);
            let group_by =
                GroupByExpressions(<LogicalPlan as GroupByBuilder>::extract_group_by(self)?);
            let having_clause = self.extract_having()?;

            select_items.items =
                apply_anylast_wrapping_for_group_by(select_items.items, &group_by.0, self)?;

            let order_by = OrderByItems(self.extract_order_by()?);
            let skip = SkipItem(self.extract_skip());
            let limit = LimitItem(self.extract_limit());
            let union = UnionItems(self.extract_union(schema)?);

            let mut context = super::cte_generation::CteGenerationContext::new();
            let ctes = CteItems(extract_ctes_with_context(
                &_gj.input,
                "",
                &mut context,
                schema,
                None,
            )?);

            return Ok(RenderPlan {
                ctes,
                select: select_items,
                from,
                joins,
                array_join,
                filters,
                group_by,
                having_clause,
                order_by,
                skip,
                limit,
                union,
                fixed_path_info: None,
                is_multi_label_scan: false,
            });
        }

        // WithClause WITH plan_ctx
        if let LogicalPlan::WithClause(with) = self {
            log::debug!("to_render_plan_with_ctx: WithClause, passing plan_ctx to extract_ctes");

            // Extract CTEs from WITH.input WITH plan_ctx so VLP CTEs get property info
            let mut temp_context = super::cte_generation::CteGenerationContext::new();
            let _temp_ctes = extract_ctes_with_context(
                &with.input,
                "",
                &mut temp_context,
                schema,
                plan_ctx, // ✅ Pass plan_ctx here!
            )?;

            // Now delegate to old to_render_plan for the rest of WITH processing
            // (which will reuse the CTEs we just generated)
            return self.to_render_plan(schema);
        }

        // Unwrap Limit/OrderBy/Skip wrappers and recurse with plan_ctx preserved
        if let LogicalPlan::Limit(l) = self {
            let mut render_plan = l.input.to_render_plan_with_ctx(schema, plan_ctx)?;
            render_plan.limit = LimitItem(Some(l.count));
            return Ok(render_plan);
        }
        if let LogicalPlan::OrderBy(ob) = self {
            let mut render_plan = ob.input.to_render_plan_with_ctx(schema, plan_ctx)?;
            let order_by_items: Result<Vec<OrderByItem>, _> = ob
                .items
                .iter()
                .map(|item| item.clone().try_into())
                .collect();
            render_plan.order_by = OrderByItems(order_by_items?);
            return Ok(render_plan);
        }
        if let LogicalPlan::Skip(s) = self {
            let mut render_plan = s.input.to_render_plan_with_ctx(schema, plan_ctx)?;
            render_plan.skip = SkipItem(Some(s.count));
            return Ok(render_plan);
        }

        // For all other cases, log what type we're delegating
        log::debug!(
            "to_render_plan_with_ctx: delegating {:?} to old to_render_plan (no special handler, plan_ctx LOST)",
            std::mem::discriminant(self)
        );

        // For all other cases, delegate to the standard to_render_plan
        // TODO: This loses plan_ctx - each case should have a proper handler
        self.to_render_plan(schema)
    }
}

/// Extract join condition as OperatorApplication format for JOIN ON clauses
/// Converts LogicalExpr equality/and conditions to RenderExpr OperatorApplications
fn extract_join_condition_ops(expr: &LogicalExpr) -> Option<Vec<OperatorApplication>> {
    use crate::query_planner::logical_expr::Operator as LogicalOp;

    match expr {
        LogicalExpr::OperatorApplicationExp(op_app) => {
            match &op_app.operator {
                LogicalOp::Equal => {
                    // Handle simple equality: a.col = b.col
                    if op_app.operands.len() == 2 {
                        let left = logical_to_render_expr(&op_app.operands[0])?;
                        let right = logical_to_render_expr(&op_app.operands[1])?;
                        Some(vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![left, right],
                        }])
                    } else {
                        None
                    }
                }
                LogicalOp::And => {
                    // Handle AND of multiple conditions
                    let mut ops = vec![];
                    for operand in &op_app.operands {
                        if let Some(sub_ops) = extract_join_condition_ops(operand) {
                            ops.extend(sub_ops);
                        }
                    }
                    if ops.is_empty() {
                        None
                    } else {
                        Some(ops)
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Convert a LogicalExpr to RenderExpr for use in join conditions
fn logical_to_render_expr(expr: &LogicalExpr) -> Option<RenderExpr> {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            // PropertyAccess has table_alias and column (PropertyValue)
            // The PropertyValue types are the same between logical and render
            Some(RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(pa.table_alias.0.clone()),
                column: pa.column.clone(),
            }))
        }
        LogicalExpr::Column(col) => {
            // Logical Column is String, Render Column is PropertyValue
            Some(RenderExpr::Column(Column(PropertyValue::Column(
                col.0.clone(),
            ))))
        }
        LogicalExpr::TableAlias(ta) => Some(RenderExpr::TableAlias(TableAlias(ta.0.clone()))),
        _ => None,
    }
}
/// Build a mapping from source alias to exported alias for WITH clause renaming
/// Returns a HashMap mapping source_alias -> output_alias (e.g., "u" -> "person")
fn build_with_alias_mapping(
    items: &[ProjectionItem],
    _exported_aliases: &[String],
) -> std::collections::HashMap<String, String> {
    let mut mapping = std::collections::HashMap::new();

    for item in items {
        if let Some(col_alias) = &item.col_alias {
            let output_alias = &col_alias.0;
            // Extract source alias from the expression
            if let LogicalExpr::TableAlias(ta) = &item.expression {
                mapping.insert(ta.0.clone(), output_alias.clone());
            }
        }
    }

    mapping
}

/// Remap select item aliases to match WITH clause exported aliases
/// Changes column aliases like "u_name" to "person_name" when WITH u AS person
fn remap_select_item_aliases(
    items: Vec<SelectItem>,
    alias_mapping: &std::collections::HashMap<String, String>,
) -> Vec<SelectItem> {
    let mut remapped = Vec::new();
    for item in items {
        if let Some(col_alias) = &item.col_alias {
            log::info!(
                "Remapping check: col_alias='{}', mapping={:?}",
                col_alias.0,
                alias_mapping
            );
            // Check if the column alias starts with a source alias
            for (source_alias, output_alias) in alias_mapping.iter() {
                // Handle dot format: "u.name"
                let prefix_dot = format!("{}.", source_alias);

                if col_alias.0.starts_with(&prefix_dot) {
                    // Format: "u.name" -> "person.name"
                    let property_part = &col_alias.0[prefix_dot.len()..];
                    let new_alias = format!("{}.{}", output_alias, property_part);
                    log::info!("  -> Remapped (dot) {} to {}", col_alias.0, new_alias);
                    remapped.push(SelectItem {
                        expression: item.expression.clone(),
                        col_alias: Some(ColumnAlias(new_alias)),
                    });
                    return remapped; // Early return per item
                } else if let Some((parsed_alias, property_part)) = parse_cte_column(&col_alias.0) {
                    // Handle new p{N} format: "p1_u_name" -> "p6_person_name"
                    if &parsed_alias == source_alias {
                        let new_alias = cte_column_name(output_alias, &property_part);
                        log::info!(
                            "  -> Remapped (p{{N}} format) {} to {}",
                            col_alias.0,
                            new_alias
                        );
                        remapped.push(SelectItem {
                            expression: item.expression.clone(),
                            col_alias: Some(ColumnAlias(new_alias)),
                        });
                        return remapped; // Early return per item
                    }
                } else {
                    // Fallback: old underscore format "u_name"
                    let prefix_underscore = format!("{}_", source_alias);
                    if col_alias.0.starts_with(&prefix_underscore) {
                        let property_part = &col_alias.0[prefix_underscore.len()..];
                        let new_alias = cte_column_name(output_alias, property_part);
                        log::info!(
                            "  -> Remapped (underscore) {} to {}",
                            col_alias.0,
                            new_alias
                        );
                        remapped.push(SelectItem {
                            expression: item.expression.clone(),
                            col_alias: Some(ColumnAlias(new_alias)),
                        });
                        return remapped; // Early return per item
                    }
                }
            }
            log::info!("  -> Not remapped (no prefix match)");
        }
        remapped.push(item);
    }
    remapped
}

/// Find PatternComprehensionMeta in a nested plan tree (e.g., inside GroupBy -> Projection).
fn find_pattern_comprehensions_in_plan(
    plan: &LogicalPlan,
) -> Vec<crate::query_planner::logical_plan::PatternComprehensionMeta> {
    match plan {
        LogicalPlan::Projection(p) => p.pattern_comprehensions.clone(),
        LogicalPlan::GroupBy(gb) => find_pattern_comprehensions_in_plan(&gb.input),
        LogicalPlan::Filter(f) => find_pattern_comprehensions_in_plan(&f.input),
        LogicalPlan::OrderBy(o) => find_pattern_comprehensions_in_plan(&o.input),
        LogicalPlan::Skip(s) => find_pattern_comprehensions_in_plan(&s.input),
        LogicalPlan::Limit(l) => find_pattern_comprehensions_in_plan(&l.input),
        LogicalPlan::GraphNode(gn) => find_pattern_comprehensions_in_plan(&gn.input),
        _ => vec![],
    }
}
