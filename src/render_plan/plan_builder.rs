use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_expr::{Direction, LogicalExpr};
use crate::query_planner::logical_plan::{
    GraphRel, GroupBy, LogicalPlan, Projection, ProjectionItem, ViewScan,
};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::utils::cte_naming::{generate_cte_base_name, generate_cte_name};
use log::debug;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::cte_generation::{analyze_property_requirements, extract_var_len_properties};
use super::errors::RenderBuildError;
use super::expression_utils::{references_alias as expr_references_alias, rewrite_aliases};
use super::filter_builder::FilterBuilder;
use super::filter_pipeline::{
    categorize_filters, clean_last_node_filters, rewrite_expr_for_mixed_denormalized_cte,
    rewrite_expr_for_var_len_cte, rewrite_labels_subscript_for_multi_type_vlp,
    rewrite_vlp_internal_to_cypher_alias,
};
use super::from_builder::FromBuilder;
use super::group_by_builder::GroupByBuilder;
use super::join_builder::JoinBuilder;
use super::properties_builder::PropertiesBuilder;
use super::render_expr::RenderCase;
use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderExpr, ScalarFnCall, TableAlias,
};
use super::select_builder::SelectBuilder;
use super::{
    view_table_ref::{from_table_to_view_ref, view_ref_to_from_table},
    ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTable, FromTableItem,
    GroupByExpressions, Join, JoinItems, JoinType, LimitItem, OrderByItem, OrderByItems,
    OrderByOrder, RenderPlan, SelectItem, SelectItems, SkipItem, Union, UnionItems, ViewTableRef,
};
use crate::render_plan::cte_extraction::extract_ctes_with_context;
use crate::render_plan::cte_extraction::{
    build_vlp_context, expand_fixed_length_joins_with_context, extract_node_label_from_viewscan,
    extract_relationship_columns, get_fixed_path_info, get_path_variable, get_shortest_path_mode,
    get_variable_length_aliases, get_variable_length_denorm_info, get_variable_length_rel_info,
    get_variable_length_spec, has_variable_length_rel, is_variable_length_denormalized,
    is_variable_length_optional, label_to_table_name, rel_type_to_table_name,
    rel_types_to_table_names, table_to_id_column, RelationshipColumns, VlpSchemaType,
};

// Import ALL helper functions from the dedicated helpers module using glob import
// This allows existing code to call helpers without changes (e.g., extract_table_name())
// The compiler will use the module functions when available
#[allow(unused_imports)]
use super::plan_builder_helpers::*;
use super::plan_builder_utils::{
    build_chained_with_match_cte_plan,
    build_with_aggregation_match_cte_plan,
    collapse_passthrough_with,
    collect_aliases_from_render_expr,
    // Import all extracted utility functions to avoid duplicates
    convert_correlation_predicates_to_joins,
    // New extracted functions
    count_with_cte_refs,
    expand_table_alias_to_group_by_id_only,
    expand_table_alias_to_select_items,
    extract_correlation_predicates,
    extract_cte_conditions_recursive,
    extract_cte_join_conditions,
    extract_cte_references,
    extract_join_from_equality,
    extract_join_from_logical_equality,
    extract_sorted_properties,
    extract_vlp_alias_mappings,
    find_all_with_clauses_grouped,
    find_group_by_subplan,
    generate_swapped_joins_for_optional_match,
    has_multi_type_vlp,
    has_with_clause_in_tree,
    hoist_nested_ctes,
    is_join_for_inner_scope,
    plan_contains_with_clause,
    prune_joins_covered_by_cte,
    remap_cte_names_in_expr,
    remap_cte_names_in_render_plan,
    replace_group_by_with_cte_reference,
    replace_wildcards_with_group_by_columns,
    replace_with_clause_with_cte_reference_v2,
    rewrite_cte_column_references,
    rewrite_cte_expression,
    rewrite_expression_with_cte_alias,
    rewrite_operator_application,
    rewrite_operator_application_with_cte_alias,
    rewrite_render_expr_for_vlp,
    rewrite_render_plan_expressions,
    rewrite_vlp_union_branch_aliases,
    update_graph_joins_cte_refs,
};
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
    plan: &LogicalPlan,
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
            if let RenderExpr::PropertyAccessExp(ref prop_access) = item.expression {
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

pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(
        &self,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
    ) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes_with_context(
        &self,
        last_node_alias: &str,
        context: &mut CteGenerationContext,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
    ) -> RenderPlanBuilderResult<Vec<Cte>>;

    /// Find the ID column for a given table alias by traversing the logical plan
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String>;

    /// Find ID column for an alias with CTE context (checks CTE schemas first)
    fn find_id_column_with_cte_context(
        &self,
        alias: &str,
        cte_schemas: &HashMap<
            String,
            (
                Vec<SelectItem>,
                Vec<String>,
                HashMap<String, String>,
                HashMap<(String, String), String>,
            ),
        >,
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

    /// Extract UNWIND clause as ARRAY JOIN items
    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<super::ArrayJoin>>;

    fn try_build_join_based_plan(
        &self,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan>;

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

/// Rewrite SELECT aliases in Union branches that reference VLP CTEs.
///
/// Problem: Undirected shortestPath creates Union with 2 branches (forward/backward).
/// Each branch uses Cypher aliases (a, b) but JOINs to VLP tables (start_node, end_node).
/// SELECT items reference non-existent aliases causing "Unknown expression identifier".
///
/// Solution: For each Union branch:
/// 1. Find VLP CTEs it references (look for vlp_cte joins)
/// 2. Get VLP metadata (cypher_start_alias → start_node mapping)
/// 3. Rewrite SELECT items: a.property → start_node.property

/// Extract VLP alias mappings from CTEs: Cypher alias → VLP table alias
/// Also extracts relationship aliases for denormalized patterns

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

/// Recursively rewrite RenderExpr to use VLP table aliases

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
                // 🔧 VLP ENDPOINT FIX: For variable-length paths, check if alias is a VLP endpoint
                // In denormalized schemas, VLP endpoints don't have separate node tables
                // Their "ID column" is start_id or end_id in the VLP CTE
                if rel.variable_length.is_some() {
                    // Extract endpoint aliases from GraphRel connections
                    // left_connection = start node, right_connection = end node
                    let start_alias = &rel.left_connection;
                    let end_alias = &rel.right_connection;

                    if alias == start_alias {
                        log::info!(
                            "🎯 VLP: Alias '{}' is VLP start endpoint -> using '{}' as ID column",
                            alias,
                            VLP_START_ID_COLUMN
                        );
                        return Ok(VLP_START_ID_COLUMN.to_string());
                    }
                    if alias == end_alias {
                        log::info!(
                            "🎯 VLP: Alias '{}' is VLP end endpoint -> using '{}' as ID column",
                            alias,
                            VLP_END_ID_COLUMN
                        );
                        return Ok(VLP_END_ID_COLUMN.to_string());
                    }
                }

                // Check both left and right branches
                if let Ok(id) = rel.left.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
                if let Ok(id) = rel.right.find_id_column_for_alias(alias) {
                    return Ok(id);
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
        cte_schemas: &HashMap<
            String,
            (
                Vec<SelectItem>,
                Vec<String>,
                HashMap<String, String>,
                HashMap<(String, String), String>,
            ),
        >,
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
                    super::CteContent::Structured(logical_cte.input.to_render_plan(schema)?),
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
    ) -> RenderPlanBuilderResult<Vec<Cte>> {
        extract_ctes_with_context(self, last_node_alias, context, schema)
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

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        // Delegate to the FilterBuilder trait implementation
        <LogicalPlan as FilterBuilder>::extract_final_filters(self)
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
                    LogicalPlan::Union(union) => {
                        // Found Union nested deep, convert it to render plan
                        log::debug!("extract_union: found nested Union, converting to render");
                        let union_render_plan = current.to_render_plan(schema)?;
                        return Ok(union_render_plan.union.0);
                    }
                    _ => break,
                }
            }
        }

        // Note: UNION is handled by LogicalPlan::Union nodes in to_render_plan().
        // This method returns None for other node types.
        Ok(None)
    }

    fn try_build_join_based_plan(
        &self,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        // Delegate to JoinBuilder
        <LogicalPlan as JoinBuilder>::try_build_join_based_plan(self, schema)
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
                let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));
                let joins = JoinItems(RenderPlanBuilder::extract_joins(self, schema)?);
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

                // Extract CTEs from the input plan
                let mut context = super::cte_generation::CteGenerationContext::new();
                let ctes = CteItems(extract_ctes_with_context(
                    &gj.input,
                    &"".to_string(),
                    &mut context,
                    schema,
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
                    // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                };

                // Populate the CTE column registry from CTE metadata
                // populate_cte_column_registry(&mut render_plan);

                // 🔧 CRITICAL: Rewrite JOIN conditions for UNION branches with VLP
                // If this render plan has UNIONs with VLP CTEs, we need to rewrite
                // JOIN conditions that reference Cypher aliases to use VLP CTE columns
                rewrite_vlp_union_branch_aliases(&mut render_plan)?;

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
                        table_name: format!(
                            "(SELECT start_id, COUNT(*) as __vlp_count FROM {} GROUP BY start_id)",
                            cte_name
                        ),
                        table_alias: vlp_alias.clone(),
                        joining_on: vec![join_condition],
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    };

                    let joins = JoinItems(vec![vlp_join]);

                    // Extract select items WITHOUT CTE registry (properties come from base table)
                    let mut select_items = SelectItems {
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
                    )?);

                    // Create temporary render plan to populate CTE registry
                    let mut temp_render_plan = RenderPlan {
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
                        JoinItems(RenderPlanBuilder::extract_joins(self, schema)?)
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
                    "Projection::to_render_plan: after input conversion, has_union={}",
                    render_plan.union.0.is_some()
                );

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
                    let mut select_items =
                        <LogicalPlan as SelectBuilder>::extract_select_items(self, None)?;

                    // Check if this Projection is over an optional VLP GraphRel
                    if let LogicalPlan::GraphRel(gr) = p.input.as_ref() {
                        if gr.variable_length.is_some() && gr.is_optional.unwrap_or(false) {
                            log::info!("🎯 Projection over optional VLP: aggregations handled by LEFT JOIN with COUNT(*)");

                            // ClickHouse returns 0 (not NULL) for COUNT(*) with empty groups in LEFT JOIN + GROUP BY
                            // So no COALESCE wrapper needed - aggregations work correctly as-is
                        }
                    }

                    render_plan.select = SelectItems {
                        items: select_items,
                        distinct: p.distinct,
                    };
                }
                Ok(render_plan)
            }
            LogicalPlan::Filter(f) => {
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
                    // Convert the filter predicate to RenderExpr
                    let mut filter_expr: RenderExpr = f.predicate.clone().try_into()?;
                    apply_property_mapping_to_expr(&mut filter_expr, &f.input);

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
                    // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                })
            }
            LogicalPlan::WithClause(with) => {
                // Handle WithClause by building a CTE from the input and creating a render plan with the CTE
                let has_aggregation = with
                    .items
                    .iter()
                    .any(|item| matches!(item.expression, LogicalExpr::AggregateFnCall(_)));

                let mut cte_filters = FilterBuilder::extract_filters(with.input.as_ref())?;
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
                    } else {
                        if let Some(existing) = cte_filters {
                            cte_filters =
                                Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![existing, render_where],
                                }));
                        } else {
                            cte_filters = Some(render_where);
                        }
                    }
                }

                log::warn!("🔍🔍🔍 BEFORE extract_select_items for WITH.input");
                let mut cte_select_items = <LogicalPlan as SelectBuilder>::extract_select_items(
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
                    let rewritten_expr =
                        rewrite_expression_with_property_mapping(&item.expression, &rewrite_ctx);

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
                let alias_mapping = build_with_alias_mapping(&with.items, &with.exported_aliases);
                if !alias_mapping.is_empty() {
                    cte_select_items = remap_select_item_aliases(cte_select_items, &alias_mapping);
                }

                let cte_from = FromTableItem(with.input.extract_from()?.and_then(|ft| ft.table));
                let cte_joins = JoinItems(RenderPlanBuilder::extract_joins(
                    with.input.as_ref(),
                    schema,
                )?);
                let cte_group_by = GroupByExpressions(
                    <LogicalPlan as GroupByBuilder>::extract_group_by(with.input.as_ref())?,
                );
                let cte_order_by = OrderByItems(with.input.extract_order_by()?);
                let cte_skip = SkipItem(with.input.extract_skip());
                let cte_limit = LimitItem(with.input.extract_limit());

                let cte_content = CteContent::Structured(RenderPlan {
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
                    // cte_column_registry: CteColumnRegistry::new(), // REMOVED: No longer used
                });

                // Generate CTE base name using centralized utility
                // Note: to_render_plan doesn't have access to counter, so we use base name.
                // This creates names like "with_p_cte" (without counter suffix).
                // The counter is only added during query planning when available.
                // This is safe because base names are still recognized by is_generated_cte_name().
                let cte_name = generate_cte_base_name(&with.exported_aliases);
                let cte = Cte::new(cte_name.clone(), cte_content, false);
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
                // Union - convert each branch to RenderPlan and combine with UNION ALL
                if union.inputs.is_empty() {
                    return Err(RenderBuildError::InvalidRenderPlan(
                        "Union has no inputs".to_string(),
                    ));
                }

                // Convert first branch to get the base plan
                let first_input = &union.inputs[0];
                let mut base_plan = first_input.to_render_plan(schema)?;

                // If there's only one branch, just return it
                if union.inputs.len() == 1 {
                    return Ok(base_plan);
                }

                // Convert remaining branches
                let mut union_branches = Vec::new();
                for input in union.inputs.iter().skip(1) {
                    let branch_plan = input.to_render_plan(schema)?;
                    union_branches.push(branch_plan);
                }

                // Store union branches in the base plan
                // UnionType::All corresponds to UNION ALL
                base_plan.union = UnionItems(Some(super::Union {
                    input: union_branches,
                    union_type: super::UnionType::All,
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
        // CRITICAL: If the plan contains WITH clauses, use the specialized handler
        // build_chained_with_match_cte_plan handles chained/nested WITH correctly
        // AND needs plan_ctx for VLP endpoint information
        use super::plan_builder_utils::{
            build_chained_with_match_cte_plan, has_with_clause_in_graph_rel,
        };

        let has_with_clause = has_with_clause_in_graph_rel(self);
        log::debug!(
            "to_render_plan_with_ctx: has_with_clause={}",
            has_with_clause
        );

        if has_with_clause {
            return build_chained_with_match_cte_plan(self, schema, plan_ctx);
        }

        // For all other cases, delegate to the standard to_render_plan
        // (which doesn't need plan_ctx for non-WITH clause handling)
        self.to_render_plan(schema)
    }
}

// Helper function to check if an expression contains aggregation functions
fn contains_aggregation_function(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::AggregateFnCall(_) => true,
        RenderExpr::OperatorApplicationExp(op) => {
            op.operands.iter().any(contains_aggregation_function)
        }
        RenderExpr::ScalarFnCall(fn_call) => fn_call.args.iter().any(contains_aggregation_function),
        RenderExpr::Case(case) => {
            case.when_then.iter().any(|(cond, val)| {
                contains_aggregation_function(cond) || contains_aggregation_function(val)
            }) || case
                .else_expr
                .as_ref()
                .is_some_and(|e| contains_aggregation_function(e))
        }
        _ => false,
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
    exported_aliases: &[String],
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
                // Handle both formats: "u.name" and "u_name"
                let prefix_dot = format!("{}.", source_alias);
                let prefix_underscore = format!("{}_", source_alias);

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
                } else if col_alias.0.starts_with(&prefix_underscore) {
                    // Format: "u_name" -> "person_name"
                    let property_part = &col_alias.0[prefix_underscore.len()..];
                    let new_alias = format!("{}_{}", output_alias, property_part);
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
            log::info!("  -> Not remapped (no prefix match)");
        }
        remapped.push(item);
    }
    remapped
}
