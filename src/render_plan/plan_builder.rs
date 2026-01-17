use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::{
    GraphRel, GroupBy, LogicalPlan, Projection, ProjectionItem, ViewScan,
};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::utils::cte_naming::generate_cte_name;
use log::debug;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::cte_generation::{analyze_property_requirements, extract_var_len_properties};
use super::errors::RenderBuildError;
use super::expression_utils::{references_alias as expr_references_alias, rewrite_aliases};
use super::filter_pipeline::{
    categorize_filters, clean_last_node_filters, rewrite_expr_for_mixed_denormalized_cte,
    rewrite_expr_for_var_len_cte, rewrite_labels_subscript_for_multi_type_vlp,
    rewrite_vlp_internal_to_cypher_alias,
};
use super::render_expr::RenderCase;
use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderExpr, ScalarFnCall, TableAlias,
};
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

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_distinct(&self) -> bool;

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>>;

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

    fn build_simple_relationship_render_plan(
        &self,
        distinct_override: Option<bool>,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan>;

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan>;
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
                        log::info!("🎯 VLP: Alias '{}' is VLP start endpoint -> using 'start_id' as ID column", alias);
                        return Ok("start_id".to_string());
                    }
                    if alias == end_alias {
                        log::info!(
                            "🎯 VLP: Alias '{}' is VLP end endpoint -> using 'end_id' as ID column",
                            alias
                        );
                        return Ok("end_id".to_string());
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
        crate::debug_println!(
            "DEBUG get_properties_with_table_alias: alias='{}', plan type={:?}",
            alias,
            std::mem::discriminant(self)
        );
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // FAST PATH: Use pre-computed projected_columns if available
                // (populated by ProjectedColumnsResolver analyzer pass)
                if let Some(projected_cols) = &node.projected_columns {
                    // projected_columns format: Vec<(property_name, qualified_column)>
                    // e.g., [("firstName", "p.first_name"), ("age", "p.age")]
                    // We need to return unqualified column names: ("firstName", "first_name")
                    let properties: Vec<(String, String)> = projected_cols
                        .iter()
                        .map(|(prop_name, qualified_col)| {
                            // Extract unqualified column: "p.first_name" -> "first_name"
                            // 🔧 FIX: Handle column names with multiple dots like "n.id.orig_h" -> "id.orig_h"
                            // Use splitn(2) to split only on the FIRST dot, keeping the rest intact
                            let unqualified = qualified_col
                                .splitn(2, '.')
                                .nth(1)
                                .unwrap_or(qualified_col)
                                .to_string();
                            (prop_name.clone(), unqualified)
                        })
                        .collect();
                    return Ok((properties, None));
                }

                // FALLBACK: Compute from ViewScan (for nodes without projected_columns)
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    log::debug!("get_properties_with_table_alias: GraphNode '{}' has ViewScan, is_denormalized={}, from_node_properties={:?}, to_node_properties={:?}",
                        alias, scan.is_denormalized,
                        scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()));
                    // For denormalized nodes with properties on the ViewScan (from standalone node query)
                    if scan.is_denormalized {
                        if let Some(from_props) = &scan.from_node_properties {
                            let properties = extract_sorted_properties(from_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} from_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None)); // Use original alias
                            }
                        }
                        if let Some(to_props) = &scan.to_node_properties {
                            let properties = extract_sorted_properties(to_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} to_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                    // Standard nodes - try property_mapping first
                    let mut properties = extract_sorted_properties(&scan.property_mapping);

                    // ZEEK FIX: If property_mapping is empty, try from_node_properties (for coupled edge schemas)
                    if properties.is_empty() {
                        if let Some(from_props) = &scan.from_node_properties {
                            properties = extract_sorted_properties(from_props);
                        }
                        if properties.is_empty() {
                            if let Some(to_props) = &scan.to_node_properties {
                                properties = extract_sorted_properties(to_props);
                            }
                        }
                    }
                    return Ok((properties, None));
                } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                    // For denormalized polymorphic nodes, the input is a UNION of ViewScans
                    // Each ViewScan has either from_node_properties or to_node_properties
                    // Use the first available ViewScan to get the property list
                    log::debug!(
                        "get_properties_with_table_alias: GraphNode '{}' has Union with {} inputs",
                        alias,
                        union_plan.inputs.len()
                    );
                    if let Some(first_input) = union_plan.inputs.first() {
                        if let LogicalPlan::ViewScan(scan) = first_input.as_ref() {
                            log::debug!("get_properties_with_table_alias: First UNION input is ViewScan, is_denormalized={}, from_node_properties={:?}, to_node_properties={:?}",
                                scan.is_denormalized,
                                scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
                                scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()));

                            // Try from_node_properties first
                            if let Some(from_props) = &scan.from_node_properties {
                                let properties = extract_sorted_properties(from_props);
                                if !properties.is_empty() {
                                    log::debug!("get_properties_with_table_alias: Returning {} from_node_properties from UNION for '{}'", properties.len(), alias);
                                    return Ok((properties, None));
                                }
                            }
                            // Then try to_node_properties
                            if let Some(to_props) = &scan.to_node_properties {
                                let properties = extract_sorted_properties(to_props);
                                if !properties.is_empty() {
                                    log::debug!("get_properties_with_table_alias: Returning {} to_node_properties from UNION for '{}'", properties.len(), alias);
                                    return Ok((properties, None));
                                }
                            }
                            // Fallback to property_mapping
                            let properties = extract_sorted_properties(&scan.property_mapping);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} property_mapping from UNION for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                }
            }
            LogicalPlan::GraphRel(rel) => {
                // Check if this relationship's alias matches
                if rel.alias == alias {
                    if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                        let mut properties = extract_sorted_properties(&scan.property_mapping);

                        // Add from_id and to_id columns for relationships
                        // These are required for RETURN r to expand correctly
                        if let Some(ref from_id) = scan.from_id {
                            properties.insert(0, ("from_id".to_string(), from_id.clone()));
                        }
                        if let Some(ref to_id) = scan.to_id {
                            properties.insert(1, ("to_id".to_string(), to_id.clone()));
                        }

                        return Ok((properties, None));
                    }
                }

                // For denormalized nodes, properties are in the relationship center's ViewScan
                // IMPORTANT: Direction affects which properties to use!
                // - Outgoing: left_connection → from_node_properties, right_connection → to_node_properties
                // - Incoming: left_connection → to_node_properties, right_connection → from_node_properties
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    let is_incoming = rel.direction == Direction::Incoming;

                    crate::debug_println!("DEBUG GraphRel: alias='{}' checking left='{}', right='{}', rel_alias='{}', direction={:?}",
                        alias, rel.left_connection, rel.right_connection, rel.alias, rel.direction);
                    crate::debug_println!(
                        "DEBUG GraphRel: from_node_properties={:?}, to_node_properties={:?}",
                        scan.from_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>())
                    );

                    // Check if BOTH nodes are denormalized on this edge
                    // If so, right_connection should use left_connection's alias (the FROM table)
                    // because the edge is fully denormalized - no separate JOIN for the edge
                    let left_props_exist = if is_incoming {
                        scan.to_node_properties.is_some()
                    } else {
                        scan.from_node_properties.is_some()
                    };
                    let right_props_exist = if is_incoming {
                        scan.from_node_properties.is_some()
                    } else {
                        scan.to_node_properties.is_some()
                    };
                    let both_nodes_denormalized = left_props_exist && right_props_exist;

                    // Check if alias matches left_connection
                    if alias == rel.left_connection {
                        // For Incoming direction, left node is on the TO side of the edge
                        let props = if is_incoming {
                            &scan.to_node_properties
                        } else {
                            &scan.from_node_properties
                        };
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                // Left connection uses its own alias as the FROM table
                                // Return None to use the original alias (which IS the FROM)
                                return Ok((properties, None));
                            }
                        }
                    }
                    // Check if alias matches right_connection
                    if alias == rel.right_connection {
                        // For Incoming direction, right node is on the FROM side of the edge
                        let props = if is_incoming {
                            &scan.from_node_properties
                        } else {
                            &scan.to_node_properties
                        };
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                // For fully denormalized edges (both nodes on edge), use left_connection
                                // alias because it's the FROM table and right node shares the same row
                                // For partially denormalized, use relationship alias as before
                                if both_nodes_denormalized {
                                    // Use left_connection alias (the FROM table)
                                    return Ok((properties, Some(rel.left_connection.clone())));
                                } else {
                                    // Use relationship alias for denormalized nodes
                                    return Ok((properties, Some(rel.alias.clone())));
                                }
                            }
                        }
                    }
                }

                // Check left and right branches
                if let Ok(result) = rel.left.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
                if let Ok(result) = rel.right.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
                if let Ok(result) = rel.center.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Union(union) => {
                if let Some(first_input) = union.inputs.first() {
                    if let Ok(result) = first_input.get_properties_with_table_alias(alias) {
                        return Ok(result);
                    }
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                // Search both branches for the alias
                if let Ok(result) = cp.left.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
                if let Ok(result) = cp.right.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
            }
            LogicalPlan::Unwind(u) => {
                // Check if the alias matches the unwound variable
                if u.alias == alias {
                    // If we have tuple_properties metadata, return it as property mappings
                    // Convert tuple position to "1", "2", "3" for tuple index access
                    if let Some(tuple_props) = &u.tuple_properties {
                        let properties: Vec<(String, String)> = tuple_props
                            .iter()
                            .map(|(prop_name, idx)| (prop_name.clone(), idx.to_string()))
                            .collect();
                        return Ok((properties, None));
                    }
                    // Fallback: Try to get properties from the label (if set)
                    if let Some(_label) = &u.label {
                        // TODO: Could look up schema properties by label here
                        // For now, return empty to avoid errors
                        return Ok((vec![], None));
                    }
                }
                // Not this unwind, recurse to input
                return u.input.get_properties_with_table_alias(alias);
            }
            _ => {}
        }
        Err(RenderBuildError::InvalidRenderPlan(format!(
            "Cannot find properties with table alias for '{}'",
            alias
        )))
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

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        crate::debug_println!("DEBUG: extract_select_items called on: {:?}", self);
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::ViewScan(view_scan) => {
                // Build select items from ViewScan's property mappings and projections
                // This is needed for multiple relationship types where ViewScan nodes are created
                // for start/end nodes but don't have explicit projections

                if !view_scan.projections.is_empty() {
                    // Use explicit projections if available
                    view_scan
                        .projections
                        .iter()
                        .map(|proj| {
                            let expr: RenderExpr = proj.clone().try_into()?;
                            Ok(SelectItem {
                                expression: expr,
                                col_alias: None,
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else if !view_scan.property_mapping.is_empty() {
                    // Fall back to property mappings - build select items for each property
                    view_scan
                        .property_mapping
                        .iter()
                        .map(|(prop_name, col_name)| {
                            Ok(SelectItem {
                                expression: RenderExpr::Column(Column(col_name.clone())),
                                col_alias: Some(ColumnAlias(prop_name.clone())),
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else {
                    // No projections or property mappings - this might be a relationship scan
                    // Return empty for now (relationship CTEs are handled differently)
                    vec![]
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // FIX: GraphNode must generate PropertyAccessExp with its own alias,
                // not delegate to ViewScan which doesn't know the alias.
                // This fixes the bug where "a.name" becomes "u.name" in OPTIONAL MATCH queries.

                match graph_node.input.as_ref() {
                    LogicalPlan::ViewScan(view_scan) => {
                        if !view_scan.projections.is_empty() {
                            // Use explicit projections if available
                            view_scan
                                .projections
                                .iter()
                                .map(|proj| {
                                    let expr: RenderExpr = proj.clone().try_into()?;
                                    Ok(SelectItem {
                                        expression: expr,
                                        col_alias: None,
                                    })
                                })
                                .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else if !view_scan.property_mapping.is_empty() {
                            // Build PropertyAccessExp using GraphNode's alias (e.g., "a")
                            // instead of bare Column which defaults to heuristic "u"
                            view_scan
                                .property_mapping
                                .iter()
                                .map(|(prop_name, col_name)| {
                                    Ok(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(graph_node.alias.clone()),
                                            column: col_name.clone(),
                                        }),
                                        // Use qualified alias like "a.name" to avoid duplicates
                                        // when multiple nodes have the same property names
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            graph_node.alias, prop_name
                                        ))),
                                    })
                                })
                                .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else if view_scan.is_denormalized
                            && (view_scan.from_node_properties.is_some()
                                || view_scan.to_node_properties.is_some())
                        {
                            // DENORMALIZED NODE-ONLY QUERY
                            // For denormalized nodes, we need to translate logical property names
                            // to actual column names from the edge table.
                            //
                            // For BOTH positions (from + to), we'll generate UNION ALL later.
                            // For now, use from_node_properties if available, else to_node_properties.

                            let props_to_use = view_scan
                                .from_node_properties
                                .as_ref()
                                .or(view_scan.to_node_properties.as_ref());

                            if let Some(props) = props_to_use {
                                props
                                    .iter()
                                    .map(|(prop_name, prop_value)| {
                                        // Extract the actual column name from PropertyValue
                                        let actual_column = match prop_value {
                                            PropertyValue::Column(col) => col.clone(),
                                            PropertyValue::Expression(expr) => expr.clone(),
                                        };

                                        Ok(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: TableAlias(
                                                        graph_node.alias.clone(),
                                                    ),
                                                    column: PropertyValue::Column(actual_column),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.{}",
                                                graph_node.alias, prop_name
                                            ))),
                                        })
                                    })
                                    .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                            } else {
                                vec![]
                            }
                        } else {
                            vec![]
                        }
                    }
                    _ => graph_node.input.extract_select_items()?,
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];

                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items()?);

                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items()?);

                items
            }
            LogicalPlan::Filter(filter) => filter.input.extract_select_items()?,
            LogicalPlan::Projection(projection) => {
                // Phase 3 cleanup: Removed with_aliases HashMap system
                // The VariableResolver analyzer pass now handles variable resolution,
                // so we don't need to build a with_aliases HashMap here anymore.

                let path_var = get_path_variable(&projection.input);

                // CRITICAL: Check if projection contains aggregation
                // If yes, we need to wrap non-ID columns with anyLast() when expanding TableAlias
                // This unifies WITH and RETURN aggregation logic to prevent GROUP BY errors
                let has_aggregation = projection.items.iter().any(|item| {
                    matches!(
                        &item.expression,
                        crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)
                    )
                });

                log::info!(
                    "🔧 extract_select_items (Projection): has_aggregation={}",
                    has_aggregation
                );

                // EXPANDED NODE FIX: Check if we need to expand node variables to all properties
                // This happens when users write `RETURN u` (returning whole node)
                // The ProjectionTagging analyzer may convert this to `u.*`, OR it may leave it as TableAlias
                let mut expanded_items = Vec::new();
                crate::debug_println!(
                    "DEBUG: Processing {} projection items",
                    projection.items.len()
                );
                for (_idx, item) in projection.items.iter().enumerate() {
                    crate::debug_println!(
                        "DEBUG: Projection item {}: expr={:?}, alias={:?}",
                        _idx,
                        item.expression,
                        item.col_alias
                    );

                    // FIRST: Check for collect(node) expansion
                    // Must happen BEFORE TableAlias expansion to catch collect(u) patterns
                    let item_to_process =
                        if let crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(
                            ref agg,
                        ) = item.expression
                        {
                            if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                                if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(
                                    ref alias,
                                ) = agg.args[0]
                                {
                                    log::info!("🔧 extract_select_items: Expanding collect({}) to groupArray(tuple(...))", alias.0);

                                    // Get all properties for this alias
                                    match self.get_properties_with_table_alias(&alias.0) {
                                        Ok((props, _actual_alias)) if !props.is_empty() => {
                                            log::info!(
                                                "🔧 Found {} properties for alias '{}', expanding",
                                                props.len(),
                                                alias.0
                                            );

                                            // Use centralized expansion utility
                                            // Note: property_requirements not available in extract_select_items context
                                            // Pruning optimization only available via WITH clause or explicit projection
                                            use crate::render_plan::property_expansion::expand_collect_to_group_array;
                                            let expanded_expr = expand_collect_to_group_array(
                                                &alias.0, props, None,
                                            );

                                            // Create new item with expanded expression
                                            ProjectionItem {
                                                expression: expanded_expr,
                                                col_alias: item.col_alias.clone(),
                                            }
                                        }
                                        _ => {
                                            log::warn!("⚠️  Could not expand collect({}) - no properties found, keeping as-is", alias.0);
                                            item.clone()
                                        }
                                    }
                                } else {
                                    item.clone()
                                }
                            } else {
                                item.clone()
                            }
                        } else {
                            item.clone()
                        };

                    // Check for TableAlias (u) - expand to all properties
                    if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) =
                        &item_to_process.expression
                    {
                        crate::debug_print!(
                            "DEBUG: Found TableAlias {} - checking if should expand to properties",
                            alias.0
                        );

                        // Get all properties AND the actual table alias to use
                        // For denormalized nodes, actual_table_alias will be the relationship alias
                        if let Ok((properties, actual_table_alias)) =
                            self.get_properties_with_table_alias(&alias.0)
                        {
                            if !properties.is_empty() {
                                println!(
                                    "DEBUG: Expanding TableAlias {} to {} properties (has_aggregation={})",
                                    alias.0,
                                    properties.len(),
                                    has_aggregation
                                );

                                // Get ID column for this alias (needed for anyLast() wrapping determination)
                                // For relationships, this will fail because they have from_id/to_id, not a single ID
                                // That's OK - we can still expand properties, just use the first property as the "ID"
                                let id_col_result = self.find_id_column_for_alias(&alias.0);

                                if let Err(_) = id_col_result {
                                    // No single ID column found
                                    // This should only happen for relationships (which have from_id/to_id instead)
                                    // Scalars from WITH are handled as PropertyAccessExp (e.g., total.total), not TableAlias

                                    if properties
                                        .iter()
                                        .any(|(name, _)| name == "from_id" || name == "to_id")
                                    {
                                        // This is a relationship alias - expand properties without anyLast()
                                        // Relationships don't need aggregation wrapping
                                        log::info!("🔧 Relationship alias '{}' detected (has from_id/to_id) - expanding {} properties without anyLast()",
                                                   alias.0, properties.len());

                                        use crate::render_plan::property_expansion::{
                                            expand_alias_to_projection_items_unified,
                                            PropertyAliasFormat,
                                        };

                                        // Use first property column as "ID" for expansion
                                        let pseudo_id = properties[0].1.clone();
                                        let property_items =
                                            expand_alias_to_projection_items_unified(
                                                &alias.0,
                                                properties,
                                                &pseudo_id,
                                                actual_table_alias,
                                                false, // Never wrap relationship properties with anyLast()
                                                PropertyAliasFormat::Underscore,
                                            );

                                        expanded_items.extend(property_items);
                                        continue; // Skip adding the TableAlias item itself
                                    } else {
                                        // UNEXPECTED: Properties exist but no ID and no from_id/to_id
                                        // This should not happen in normal operation:
                                        // - Nodes have ID column
                                        // - Relationships have from_id/to_id
                                        // - Scalars from WITH use PropertyAccessExp, not TableAlias
                                        log::warn!("⚠️ Alias '{}' has {} properties but no ID column and no from_id/to_id - this is unexpected. Properties: {:?}",
                                                   alias.0, properties.len(), properties.iter().map(|(n, _)| n).collect::<Vec<_>>());
                                        // Skip expansion to avoid SQL conflicts (duplicate alias errors)
                                        // Continue to avoid adding raw TableAlias which would conflict with JOIN alias
                                        continue;
                                    }
                                } else {
                                    // Node alias with proper ID column
                                    let id_col = id_col_result.unwrap();

                                    if has_aggregation {
                                        log::info!("🔧 Aggregation detected: wrapping non-ID columns with anyLast() for alias '{}', ID column='{}'",
                                                   alias.0, id_col);
                                    }

                                    // Use unified expansion helper (consolidates RETURN/WITH logic)
                                    use crate::render_plan::property_expansion::{
                                        expand_alias_to_projection_items_unified,
                                        PropertyAliasFormat,
                                    };

                                    let property_items = expand_alias_to_projection_items_unified(
                                        &alias.0,
                                        properties,
                                        &id_col,
                                        actual_table_alias,
                                        has_aggregation, // Enables anyLast() wrapping for non-ID columns
                                        PropertyAliasFormat::Underscore,
                                    );

                                    expanded_items.extend(property_items);
                                    continue; // Skip adding the TableAlias item itself
                                }
                            }
                        }
                    }

                    // Check for PropertyAccessExp with wildcard (u.*) - expand to all properties
                    if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                        prop,
                    ) = &item_to_process.expression
                    {
                        if prop.column.raw() == "*" {
                            // This is u.* - need to expand to all properties from schema
                            // IMPORTANT: For denormalized nodes, the table_alias may have been converted
                            // to the edge alias, but we can use col_alias to recover the original node name
                            let original_alias = item_to_process
                                .col_alias
                                .as_ref()
                                .and_then(|ca| ca.0.strip_suffix(".*"))
                                .unwrap_or(&prop.table_alias.0);

                            crate::debug_print!(
                                "DEBUG: Found wildcard property access {}.* - original alias: '{}', looking up properties",
                                prop.table_alias.0, original_alias
                            );

                            // Get all properties AND the actual table alias to use
                            // This works for both nodes and relationships (from_id/to_id included via get_properties_with_table_alias)
                            // Try original alias first (for recovering denormalized node properties)
                            let lookup_result = self
                                .get_properties_with_table_alias(original_alias)
                                .or_else(|_| {
                                    self.get_properties_with_table_alias(&prop.table_alias.0)
                                });

                            if let Ok((properties, actual_table_alias)) = lookup_result {
                                // Only expand if we actually have properties
                                // CTE references return Ok but with empty properties - fall through to keep wildcard
                                if !properties.is_empty() {
                                    let table_alias_to_use = actual_table_alias
                                        .as_ref()
                                        .map(|s| {
                                            crate::query_planner::logical_expr::TableAlias(
                                                s.clone(),
                                            )
                                        })
                                        .unwrap_or_else(|| prop.table_alias.clone());

                                    crate::debug_print!(
                                        "DEBUG: Expanding {}.* to {} properties",
                                        original_alias,
                                        properties.len()
                                    );

                                    // Use centralized expansion utility
                                    use crate::render_plan::property_expansion::{
                                        expand_alias_to_properties, PropertyAliasFormat,
                                    };
                                    let property_items = expand_alias_to_properties(
                                        original_alias,
                                        properties,
                                        actual_table_alias,
                                        PropertyAliasFormat::Underscore,
                                    );
                                    expanded_items.extend(property_items);
                                    continue; // Skip adding the wildcard item itself
                                } else {
                                    crate::debug_print!(
                                        "DEBUG: Empty properties for {}.* - keeping as wildcard (likely CTE reference)",
                                        original_alias
                                    );
                                    // Fall through to keep the wildcard, but without alias
                                    // (can't have AS "friend.*" with friend.*)
                                    expanded_items.push(ProjectionItem {
                                        expression: item.expression.clone(),
                                        col_alias: None, // Strip alias for wildcard
                                    });
                                    continue;
                                }
                            } else {
                                crate::debug_print!(
                                    "DEBUG: Could not expand {}.* - falling back to wildcard",
                                    original_alias
                                );
                                // Fall through - wildcard without alias will be added below
                            }
                        }
                    }

                    // Not a node variable or wildcard expansion failed - keep the item as-is
                    // For wildcards, strip the alias (can't alias a wildcard in ClickHouse)
                    let should_strip_alias = matches!(
                        &item_to_process.expression,
                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa)
                        if pa.column.raw() == "*"
                    );

                    if should_strip_alias {
                        expanded_items.push(ProjectionItem {
                            expression: item_to_process.expression.clone(),
                            col_alias: None,
                        });
                    } else {
                        expanded_items.push(item_to_process.clone());
                    }
                }

                let items = expanded_items.iter().map(|item| {
                    // Phase 3 cleanup: Removed with_aliases lookup
                    // The VariableResolver analyzer pass already transformed TableAlias → PropertyAccessExp
                    // No need to resolve variables here anymore

                    // COLLECT() EXPANSION for UNWIND support:
                    // collect(node_variable) where node_variable is a TableAlias needs to be expanded
                    // to groupArray(tuple(node.prop1, node.prop2, ...)) so UNWIND/ARRAY JOIN can work.
                    // This expansion must happen BEFORE converting to RenderExpr.
                    let logical_expr = if let crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(ref agg) = item.expression {
                        if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                            if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                                log::info!("🔧 Expanding collect({}) to groupArray(tuple(...)) in SELECT items", alias.0);

                                // Get all properties for this alias
                                let props_result = self.get_properties_with_table_alias(&alias.0);

                                match props_result {
                                    Ok((props, _actual_alias)) if !props.is_empty() => {
                                        log::info!("🔧 Found {} properties for alias '{}'", props.len(), alias.0);

                                        // Use centralized expansion utility
                                        // Note: property_requirements not available in extract_select_items context
                                        use crate::render_plan::property_expansion::expand_collect_to_group_array;
                                        expand_collect_to_group_array(&alias.0, props, None)
                                    }
                                    _ => {
                                        log::warn!("⚠️  Could not expand collect({}) - no properties found, keeping as-is", alias.0);
                                        item.expression.clone()
                                    }
                                }
                            } else {
                                item.expression.clone()
                            }
                        } else {
                            item.expression.clone()
                        }
                    } else {
                        item.expression.clone()
                    };

                    // Convert logical expression to render expression
                    let expr: RenderExpr = logical_expr.try_into()?;

                    // DENORMALIZED TABLE ALIAS RESOLUTION:
                    // For denormalized nodes on fully denormalized edges (like (ip1)-[]->(d) where both
                    // ip1 and d are from the same row), the table alias `d` doesn't exist in SQL.
                    // We need to resolve `d` to the actual table alias (e.g., `ip1`).
                    // Note: By this point, property names have already been converted to column names
                    // by the analyzer, so we just need to fix the table alias.
                    let translated_expr = if let RenderExpr::PropertyAccessExp(ref prop_access) = expr {
                        crate::debug_println!("DEBUG: Checking denormalized alias for {}.{}", prop_access.table_alias.0, prop_access.column.raw());
                        // Check if this alias is denormalized and needs to point to a different table
                        match self.get_properties_with_table_alias(&prop_access.table_alias.0) {
                            Ok((_props, actual_table_alias)) => {
                                crate::debug_println!("DEBUG: get_properties_with_table_alias for '{}' returned Ok: {} properties, actual_alias={:?}",
                                    prop_access.table_alias.0, _props.len(), actual_table_alias);
                                if let Some(actual_alias) = actual_table_alias {
                                    // This is a denormalized alias - use the actual table alias
                                    println!(
                                        "DEBUG: Translated denormalized alias {}.{} -> {}.{}",
                                        prop_access.table_alias.0, prop_access.column.raw(),
                                        actual_alias, prop_access.column.raw()
                                    );
                                    Some(RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(actual_alias),
                                        column: prop_access.column.clone(),
                                    }))
                                } else {
                                    crate::debug_println!("DEBUG: No actual_table_alias for '{}'", prop_access.table_alias.0);
                                    None // Use original alias
                                }
                            }
                            Err(_e) => {
                                crate::debug_println!("DEBUG: get_properties_with_table_alias for '{}' returned Err: {:?}",
                                    prop_access.table_alias.0, _e);
                                None
                            }
                        }
                    } else {
                        None
                    };

                    let mut expr = translated_expr.unwrap_or(expr);

                    // Check if this is a path variable that needs to be converted to tuple construction
                    if let (Some(path_var_name), RenderExpr::TableAlias(TableAlias(alias))) =
                        (&path_var, &expr)
                    {
                        if alias == path_var_name {
                            // Convert path variable to named tuple construction
                            // Use tuple(nodes, length, relationships) instead of map() to avoid type conflicts
                            expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "tuple".to_string(),
                                args: vec![
                                    RenderExpr::Column(Column(PropertyValue::Column("path_nodes".to_string()))),
                                    RenderExpr::Column(Column(PropertyValue::Column("hop_count".to_string()))),
                                    RenderExpr::Column(Column(PropertyValue::Column("path_relationships".to_string()))),
                                ],
                            });
                        }
                    }

                    // Rewrite path function calls: length(p), nodes(p), relationships(p)
                    // Determine table alias based on pattern:
                    // - Multi-type VLP (vlp_multi_type_X_Y): Use Y as the table alias (Cypher end alias)
                    // - Single-type VLP: Use "t" for backward compatibility
                    if let Some(path_var_name) = &path_var {
                        // Default table alias for VLP CTEs
                        let mut table_alias_for_path = "t";

                        // Try to extract FROM table information to detect multi-type VLP
                        // Multi-type VLP CTEs are named like "vlp_multi_type_u_x" where "x" is the end alias
                        // We can infer the alias from the CTE name pattern
                        if let LogicalPlan::Projection(proj) = &self {
                            // Look for FROM information in the input plan
                            // This is a simplified heuristic - check if path exists
                            // For multi-type VLP, the path variable exists and pattern contains multiple types
                            if let Some(ref graph_rel) = get_graph_rel_from_plan(&proj.input) {
                                if let Some(ref labels) = graph_rel.labels {
                                    // If multiple relationship types, this is multi-type
                                    if labels.len() > 1 {
                                        // Multi-type pattern: use the path variable name as alias
                                        // In multi-type VLP, the FROM is "vlp_multi_type_X_Y AS Y"
                                        // So the table alias matches the right side of the GraphRel
                                        if let LogicalPlan::GraphNode(ref right_node) = graph_rel.right.as_ref() {
                                            table_alias_for_path = &right_node.alias;
                                            log::debug!("🎯 Multi-type VLP detected: using end alias '{}' for path functions", table_alias_for_path);
                                        }
                                    }
                                }
                            }
                        }

                        expr = rewrite_path_functions_with_table(&expr, path_var_name, table_alias_for_path);
                    }

                    // For fixed multi-hop patterns (no variable length), rewrite path functions
                    // This handles queries like: MATCH p = (a)-[r1]->(b)-[r2]->(c) RETURN length(p), nodes(p)
                    if path_var.is_none() {
                        if let Some(path_info) = get_fixed_path_info(&projection.input)? {
                            expr = rewrite_fixed_path_functions_with_info(&expr, &path_info);
                        }
                    }

                    // IMPORTANT: Property mapping is already done in the analyzer phase by FilterTagging.apply_property_mapping
                    // for schema-based queries (which use ViewScan). Re-mapping here causes errors because the analyzer
                    // has already converted Cypher property names (e.g., "name") to database column names (e.g., "full_name").
                    // Trying to map "full_name" again fails because it's not in the property_mappings.
                    //
                    // DO NOT apply property mapping here for Projection nodes - it's already been done correctly.

                    let alias = item
                        .col_alias
                        .clone()
                        .map(ColumnAlias::try_from)
                        .transpose()?;
                    Ok(SelectItem {
                        expression: expr,
                        col_alias: alias,
                    })
                });

                items.collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
            }
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items()?,
            LogicalPlan::GroupBy(group_by) => {
                // CRITICAL: When extracting SELECT items from a GroupBy, we need to wrap
                // non-ID columns of TableAlias items with anyLast() for efficient aggregation
                // We CANNOT use group_by.input.extract_select_items() because that will expand
                // TableAlias to properties WITHOUT anyLast wrapping. We need to handle it ourselves.

                // Get the Projection items BEFORE they're expanded
                // (we need to expand them ourselves WITH anyLast wrapping)
                use crate::query_planner::logical_plan::LogicalPlan;
                let projection_items = match group_by.input.as_ref() {
                    LogicalPlan::Projection(proj) => &proj.items,
                    _ => {
                        // GroupBy input is not a Projection, delegate to standard extract_select_items
                        return group_by.input.extract_select_items();
                    }
                };

                // Now process each item and expand TableAlias with anyLast() wrapping
                let wrapped_items: Vec<SelectItem> = projection_items.iter().flat_map(|item| {
                    use crate::query_planner::logical_expr::LogicalExpr;

                    // Check if this is a TableAlias that needs expansion
                    if let LogicalExpr::TableAlias(ref alias) = item.expression {
                        // Find ID column for this alias
                        match group_by.input.find_id_column_for_alias(&alias.0) {
                            Ok(id_col) => {
                                // Get properties for this alias
                                match group_by.input.get_properties_with_table_alias(&alias.0) {
                                    Ok((properties, actual_alias)) if !properties.is_empty() => {
                                        let table_alias_to_use = actual_alias.clone().unwrap_or_else(|| alias.0.clone());

                                        // Expand to multiple PropertyAccess SelectItems, wrapping non-ID with anyLast()
                                        properties.into_iter().map(|(prop_name, col_name)| {
                                            use crate::graph_catalog::expression_parser::PropertyValue;

                                            let base_expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(table_alias_to_use.clone()),
                                                column: PropertyValue::Column(col_name.clone()),
                                            });

                                            // Wrap non-ID columns with anyLast()
                                            let wrapped_expr = if col_name == id_col {
                                                base_expr
                                            } else {
                                                RenderExpr::AggregateFnCall(AggregateFnCall {
                                                    name: "anyLast".to_string(),
                                                    args: vec![base_expr],
                                                })
                                            };

                                            SelectItem {
                                                expression: wrapped_expr,
                                                col_alias: if let Some(ref alias_str) = item.col_alias {
                                                    Some(ColumnAlias(format!("{}_{}", alias_str.0, prop_name)))
                                                } else {
                                                    Some(ColumnAlias(format!("{}_{}", alias.0, prop_name)))
                                                },
                                            }
                                        }).collect()
                                    }
                                    _ => {
                                        // Could not get properties - convert to RenderExpr and keep as-is
                                        let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                                        match render_expr {
                                            Ok(expr) => vec![SelectItem {
                                                expression: expr,
                                                col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                            }],
                                            Err(_) => vec![]
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                // Could not find ID column - convert to RenderExpr and keep as-is
                                let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                                match render_expr {
                                    Ok(expr) => vec![SelectItem {
                                        expression: expr,
                                        col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                    }],
                                    Err(_) => vec![]
                                }
                            }
                        }
                    } else if let LogicalExpr::AggregateFnCall(ref agg) = item.expression {
                        // Aggregate function - check if it's collect(node) that needs expansion
                        if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                            if let LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                                // Get properties for this alias and expand to groupArray(tuple(...))
                                match group_by.input.get_properties_with_table_alias(&alias.0) {
                                    Ok((properties, _actual_alias)) if !properties.is_empty() => {
                                        use crate::render_plan::property_expansion::expand_collect_to_group_array;
                                        // Note: property_requirements not available in extract_select_items context
                                        let expanded_logical = expand_collect_to_group_array(&alias.0, properties.clone(), None);

                                        // Convert to RenderExpr
                                        let render_expr: Result<RenderExpr, _> = expanded_logical.try_into();
                                        match render_expr {
                                            Ok(expr) => vec![SelectItem {
                                                expression: expr,
                                                col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                            }],
                                            Err(_) => vec![]
                                        }
                                    }
                                    _ => {
                                        // Could not get properties - convert to RenderExpr and keep as-is
                                        let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                                        match render_expr {
                                            Ok(expr) => vec![SelectItem {
                                                expression: expr,
                                                col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                            }],
                                            Err(_) => vec![]
                                        }
                                    }
                                }
                            } else {
                                // collect() of something other than TableAlias - convert and keep as-is
                                let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                                match render_expr {
                                    Ok(expr) => vec![SelectItem {
                                        expression: expr,
                                        col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                    }],
                                    Err(_) => vec![]
                                }
                            }
                        } else {
                            // Other aggregate function - convert and keep as-is
                            let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                            match render_expr {
                                Ok(expr) => vec![SelectItem {
                                    expression: expr,
                                    col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                                }],
                                Err(_) => vec![]
                            }
                        }
                    } else {
                        // Other expression - convert and keep as-is
                        let render_expr: Result<RenderExpr, _> = item.expression.clone().try_into();
                        match render_expr {
                            Ok(expr) => vec![SelectItem {
                                expression: expr,
                                col_alias: item.col_alias.as_ref().map(|s| ColumnAlias(s.0.clone())),
                            }],
                            Err(_) => vec![]
                        }
                    }
                }).collect();

                wrapped_items
            }
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items()?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items()?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items()?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items()?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
            LogicalPlan::Unwind(u) => u.input.extract_select_items()?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine select items from both sides
                let mut items = cp.left.extract_select_items()?;
                items.extend(cp.right.extract_select_items()?);
                items
            }
            LogicalPlan::WithClause(wc) => wc.input.extract_select_items()?,
        };

        Ok(select_items)
    }

    fn extract_distinct(&self) -> bool {
        // Extract distinct flag from Projection nodes
        let result = match &self {
            LogicalPlan::Projection(projection) => {
                crate::debug_println!(
                    "DEBUG extract_distinct: Found Projection, distinct={}",
                    projection.distinct
                );
                projection.distinct
            }
            LogicalPlan::OrderBy(order_by) => {
                crate::debug_println!("DEBUG extract_distinct: OrderBy, recursing");
                order_by.input.extract_distinct()
            }
            LogicalPlan::Skip(skip) => {
                crate::debug_println!("DEBUG extract_distinct: Skip, recursing");
                skip.input.extract_distinct()
            }
            LogicalPlan::Limit(limit) => {
                crate::debug_println!("DEBUG extract_distinct: Limit, recursing");
                limit.input.extract_distinct()
            }
            LogicalPlan::GroupBy(group_by) => {
                crate::debug_println!("DEBUG extract_distinct: GroupBy, recursing");
                group_by.input.extract_distinct()
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                crate::debug_println!("DEBUG extract_distinct: GraphJoins, recursing");
                graph_joins.input.extract_distinct()
            }
            LogicalPlan::Filter(filter) => {
                crate::debug_println!("DEBUG extract_distinct: Filter, recursing");
                filter.input.extract_distinct()
            }
            _ => {
                crate::debug_println!("DEBUG extract_distinct: Other variant, returning false");
                false
            }
        };
        crate::debug_println!("DEBUG extract_distinct: Returning {}", result);
        result
    }

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>> {
        log::debug!(
            "🔍 extract_from START: plan type={:?}",
            std::mem::discriminant(self)
        );

        let from_ref = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::ViewScan(scan) => {
                // Check if this is a relationship ViewScan (has from_id/to_id)
                if scan.from_id.is_some() && scan.to_id.is_some() {
                    // For denormalized edges, use the actual table name directly
                    // CTE references (rel_*) are only needed for standard edges with separate node tables
                    // Denormalized ViewScans have from_node_properties/to_node_properties indicating
                    // node data is stored on the edge table itself
                    let use_actual_table =
                        scan.from_node_properties.is_some() && scan.to_node_properties.is_some();

                    debug!("📊 extract_from ViewScan: source_table={}, from_props={:?}, to_props={:?}, use_actual_table={}",
                        scan.source_table,
                        scan.from_node_properties.as_ref().map(|p| p.len()),
                        scan.to_node_properties.as_ref().map(|p| p.len()),
                        use_actual_table);

                    if use_actual_table {
                        // Denormalized: use actual table name
                        debug!("✅ Using actual table name: {}", scan.source_table);
                        Some(ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            scan.source_table.clone(),
                        ))
                    } else {
                        // Standard edge: use CTE reference
                        let cte_name =
                            format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""));
                        debug!("🔄 Using CTE reference: {}", cte_name);
                        Some(ViewTableRef::new_table(scan.as_ref().clone(), cte_name))
                    }
                } else {
                    // For node ViewScans, use the table name
                    Some(ViewTableRef::new_table(
                        scan.as_ref().clone(),
                        scan.source_table.clone(),
                    ))
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // For GraphNode, extract FROM from the input but use this GraphNode's alias
                // CROSS JOINs for multiple standalone nodes are handled in extract_joins
                println!(
                    "DEBUG: GraphNode.extract_from() - alias: {}, input: {:?}",
                    graph_node.alias, graph_node.input
                );
                match &*graph_node.input {
                    LogicalPlan::ViewScan(scan) => {
                        println!(
                            "DEBUG: GraphNode.extract_from() - matched ViewScan, table: {}",
                            scan.source_table
                        );
                        // Check if this is a relationship ViewScan (has from_id/to_id)
                        let table_or_cte_name = if scan.from_id.is_some() && scan.to_id.is_some() {
                            // For denormalized edges, use actual table; for standard edges, use CTE
                            let use_actual_table = scan.from_node_properties.is_some()
                                && scan.to_node_properties.is_some();
                            if use_actual_table {
                                scan.source_table.clone()
                            } else {
                                format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""))
                            }
                        } else {
                            // For node ViewScans, use the table name
                            scan.source_table.clone()
                        };
                        // ViewScan already returns ViewTableRef, just update the alias
                        let mut view_ref =
                            ViewTableRef::new_table(scan.as_ref().clone(), table_or_cte_name);
                        view_ref.alias = Some(graph_node.alias.clone());
                        println!(
                            "DEBUG: GraphNode.extract_from() - created ViewTableRef: {:?}",
                            view_ref
                        );
                        Some(view_ref)
                    }
                    _ => {
                        println!(
                            "DEBUG: GraphNode.extract_from() - not a ViewScan, input type: {:?}",
                            graph_node.input
                        );
                        // For other input types, extract FROM and convert
                        let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                        // Use this GraphNode's alias
                        if let Some(ref mut view_ref) = from_ref {
                            view_ref.alias = Some(graph_node.alias.clone());
                        }
                        from_ref
                    }
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // DENORMALIZED EDGE TABLE CHECK
                // For denormalized patterns, both nodes are virtual - use relationship table as FROM
                let left_is_denormalized = is_node_denormalized(&graph_rel.left);
                let right_is_denormalized = is_node_denormalized(&graph_rel.right);

                log::debug!(
                    "🔍 extract_from GraphRel: alias='{}', left_is_denorm={}, right_is_denorm={}",
                    graph_rel.alias,
                    left_is_denormalized,
                    right_is_denormalized
                );

                if left_is_denormalized && right_is_denormalized {
                    log::debug!("✓ DENORMALIZED pattern: both nodes on edge table, using edge table as FROM");

                    // For multi-hop denormalized, find the first (leftmost) relationship
                    fn find_first_graph_rel(
                        graph_rel: &crate::query_planner::logical_plan::GraphRel,
                    ) -> &crate::query_planner::logical_plan::GraphRel {
                        match graph_rel.left.as_ref() {
                            LogicalPlan::GraphRel(left_rel) => find_first_graph_rel(left_rel),
                            _ => graph_rel,
                        }
                    }

                    let first_graph_rel = find_first_graph_rel(graph_rel);

                    // Try ViewScan first (normal case)
                    if let LogicalPlan::ViewScan(scan) = first_graph_rel.center.as_ref() {
                        log::debug!(
                            "✓ Using ViewScan edge table '{}' AS '{}'",
                            scan.source_table,
                            first_graph_rel.alias
                        );
                        return Ok(Some(FromTable::new(Some(ViewTableRef {
                            source: first_graph_rel.center.clone(),
                            name: scan.source_table.clone(),
                            alias: Some(first_graph_rel.alias.clone()),
                            use_final: scan.use_final,
                        }))));
                    }

                    log::debug!(
                        "⚠️  Could not extract edge table from center (type: {:?})",
                        std::mem::discriminant(first_graph_rel.center.as_ref())
                    );
                }

                // Check if both nodes are anonymous (edge-driven query)
                let left_table_name = extract_table_name(&graph_rel.left);
                let right_table_name = extract_table_name(&graph_rel.right);

                // If both nodes are anonymous, use the relationship table as FROM
                if left_table_name.is_none() && right_table_name.is_none() {
                    // Edge-driven query: use relationship table directly (not as CTE)
                    // Extract table name from the relationship ViewScan
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        // Use actual table name, not CTE name
                        return Ok(Some(FromTable::new(Some(ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            scan.source_table.clone(),
                        )))));
                    }
                    // Fallback to normal extraction if not a ViewScan
                    return Ok(None);
                }

                // For GraphRel with labeled nodes, we need to include the start node in the FROM clause
                // This handles simple relationship queries where the start node should be FROM

                // ALWAYS use left node as FROM for relationship patterns.
                // The is_optional flag determines JOIN type (INNER vs LEFT), not FROM table selection.
                //
                // For `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`:
                //   - a is the left connection (required, already defined)
                //   - b is the right connection (optional, newly introduced)
                //   - FROM should be `a`, with LEFT JOIN to relationship and `b`
                //
                // For `MATCH (a) OPTIONAL MATCH (b)-[:R]->(a)`:
                //   - b is the left connection (optional, newly introduced)
                //   - a is the right connection (required, already defined)
                //   - FROM should be `a` (the required one), but the pattern structure has `b` on left
                //   - This case needs special handling: find which connection is NOT optional

                println!("DEBUG: graph_rel.is_optional = {:?}", graph_rel.is_optional);

                // Use left as primary, right as fallback
                let (primary_from, fallback_from) = (
                    graph_rel.left.extract_from(),
                    graph_rel.right.extract_from(),
                );

                crate::debug_println!("DEBUG: primary_from = {:?}", primary_from);
                crate::debug_println!("DEBUG: fallback_from = {:?}", fallback_from);

                if let Ok(Some(from_table)) = primary_from {
                    from_table_to_view_ref(Some(from_table))
                } else {
                    // If primary node doesn't have FROM, try fallback
                    let right_from = fallback_from;
                    crate::debug_println!("DEBUG: Using fallback FROM");
                    crate::debug_println!("DEBUG: right_from = {:?}", right_from);

                    if let Ok(Some(from_table)) = right_from {
                        from_table_to_view_ref(Some(from_table))
                    } else {
                        // If right also doesn't have FROM, check if right contains a nested GraphRel
                        if let LogicalPlan::GraphRel(nested_graph_rel) = graph_rel.right.as_ref() {
                            // Extract FROM from the nested GraphRel's left node
                            let nested_left_from = nested_graph_rel.left.extract_from();
                            crate::debug_println!(
                                "DEBUG: nested_graph_rel.left = {:?}",
                                nested_graph_rel.left
                            );
                            crate::debug_println!(
                                "DEBUG: nested_left_from = {:?}",
                                nested_left_from
                            );

                            if let Ok(Some(nested_from_table)) = nested_left_from {
                                from_table_to_view_ref(Some(nested_from_table))
                            } else {
                                // If nested left also doesn't have FROM, create one from the nested left_connection alias
                                let table_name = extract_table_name(&nested_graph_rel.left)
                                    .ok_or_else(|| {
                                        super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        nested_graph_rel.left_connection, nested_graph_rel.left
                                    ))
                                    })?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(nested_graph_rel.left_connection.clone()),
                                    use_final: false,
                                })
                            }
                        } else {
                            // If right doesn't have FROM, we need to determine which node should be the anchor
                            // Use find_anchor_node logic to choose the correct anchor
                            let all_connections = get_all_relationship_connections(&self);
                            let optional_aliases = std::collections::HashSet::new();
                            let denormalized_aliases = std::collections::HashSet::new();

                            if let Some(anchor_alias) = find_anchor_node(
                                &all_connections,
                                &optional_aliases,
                                &denormalized_aliases,
                            ) {
                                // Determine which node (left or right) the anchor corresponds to
                                let (table_plan, connection_alias) =
                                    if anchor_alias == graph_rel.left_connection {
                                        (&graph_rel.left, &graph_rel.left_connection)
                                    } else {
                                        (&graph_rel.right, &graph_rel.right_connection)
                                    };

                                let table_name = extract_table_name(table_plan)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for anchor alias '{}', plan: {:?}",
                                        connection_alias, table_plan
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(connection_alias.clone()),
                                    use_final: false,
                                })
                            } else {
                                // Fallback: use left_connection as anchor (traditional behavior)
                                let table_name =
                                    extract_table_name(&graph_rel.left).ok_or_else(|| {
                                        super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        graph_rel.left_connection, graph_rel.left
                                    ))
                                    })?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(graph_rel.left_connection.clone()),
                                    use_final: false,
                                })
                            }
                        }
                    }
                }
            }
            LogicalPlan::Filter(filter) => {
                log::debug!(
                    "  → Filter, recursing to input type={:?}",
                    std::mem::discriminant(filter.input.as_ref())
                );
                from_table_to_view_ref(filter.input.extract_from()?)
            }
            LogicalPlan::Projection(projection) => {
                log::debug!(
                    "  → Projection, recursing to input type={:?}",
                    std::mem::discriminant(projection.input.as_ref())
                );
                from_table_to_view_ref(projection.input.extract_from()?)
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // ============================================================================
                // CLEAN DESIGN: FROM table determination for GraphJoins
                // ============================================================================
                //
                // The logical model is simple:
                // 1. Every table in a graph query is represented as a Join in graph_joins.joins
                // 2. A Join with EMPTY joining_on is a FROM marker (no join conditions = base table)
                // 3. A Join with NON-EMPTY joining_on is a real JOIN
                // 4. There should be exactly ONE FROM marker per GraphJoins
                //
                // This function finds that FROM marker and returns it.
                // NO FALLBACKS. If there's no FROM marker, something is wrong upstream.
                // ============================================================================

                log::debug!(
                    "🔍 GraphJoins.extract_from: {} joins, anchor_table={:?}",
                    graph_joins.joins.len(),
                    graph_joins.anchor_table
                );

                // 🔧 PARAMETERIZED VIEW FIX: Get parameterized table references from input plan
                let parameterized_tables = extract_rel_and_node_tables(&graph_joins.input);

                // STEP 1: Find FROM marker (Join with empty joining_on)
                // This is the authoritative source - it was set by graph_join_inference
                for join in &graph_joins.joins {
                    if join.joining_on.is_empty() {
                        // 🔧 PARAMETERIZED VIEW FIX: Use parameterized table reference if available
                        let table_name = parameterized_tables
                            .get(&join.table_alias)
                            .cloned()
                            .unwrap_or_else(|| join.table_name.clone());

                        log::info!(
                            "✅ Found FROM marker: table='{}' (original='{}') alias='{}'",
                            table_name,
                            join.table_name,
                            join.table_alias
                        );
                        return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                            source: std::sync::Arc::new(LogicalPlan::Empty),
                            name: table_name,
                            alias: Some(join.table_alias.clone()),
                            use_final: false,
                        }))));
                    }
                }

                // STEP 2: No FROM marker found - check special cases that don't use joins

                // Helper to find GraphRel through wrappers
                fn find_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
                    match plan {
                        LogicalPlan::GraphRel(gr) => Some(gr),
                        LogicalPlan::Projection(proj) => find_graph_rel(&proj.input),
                        LogicalPlan::Filter(filter) => find_graph_rel(&filter.input),
                        LogicalPlan::Unwind(u) => find_graph_rel(&u.input),
                        LogicalPlan::GraphJoins(gj) => find_graph_rel(&gj.input),
                        _ => None,
                    }
                }

                // Helper to find GraphNode for node-only queries
                fn find_graph_node(
                    plan: &LogicalPlan,
                ) -> Option<&crate::query_planner::logical_plan::GraphNode> {
                    match plan {
                        LogicalPlan::GraphNode(gn) => Some(gn),
                        LogicalPlan::Projection(proj) => find_graph_node(&proj.input),
                        LogicalPlan::Filter(filter) => find_graph_node(&filter.input),
                        LogicalPlan::Unwind(u) => find_graph_node(&u.input),
                        LogicalPlan::GraphJoins(gj) => find_graph_node(&gj.input),
                        _ => None,
                    }
                }

                // Helper to find CartesianProduct
                fn find_cartesian_product(
                    plan: &LogicalPlan,
                ) -> Option<&crate::query_planner::logical_plan::CartesianProduct> {
                    match plan {
                        LogicalPlan::CartesianProduct(cp) => Some(cp),
                        LogicalPlan::Filter(f) => find_cartesian_product(&f.input),
                        LogicalPlan::Projection(p) => find_cartesian_product(&p.input),
                        _ => None,
                    }
                }

                fn is_cte_reference(plan: &LogicalPlan) -> bool {
                    match plan {
                        LogicalPlan::WithClause(_) => true,
                        LogicalPlan::ViewScan(vs) => vs.source_table.starts_with("with_"),
                        LogicalPlan::GraphNode(gn) => is_cte_reference(&gn.input),
                        LogicalPlan::Projection(p) => is_cte_reference(&p.input),
                        LogicalPlan::Filter(f) => is_cte_reference(&f.input),
                        _ => false,
                    }
                }

                // CASE A: Empty joins - check for denormalized edge or node-only patterns
                if graph_joins.joins.is_empty() {
                    log::debug!("📋 No joins - checking for special patterns");

                    // A.1: Denormalized edge pattern - use edge table directly
                    if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                        if let LogicalPlan::ViewScan(rel_scan) = graph_rel.center.as_ref() {
                            if rel_scan.from_node_properties.is_some()
                                || rel_scan.to_node_properties.is_some()
                            {
                                log::info!(
                                    "🎯 DENORMALIZED: Using edge table '{}' as FROM",
                                    rel_scan.source_table
                                );
                                return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                    source: graph_rel.center.clone(),
                                    name: rel_scan.source_table.clone(),
                                    alias: Some(graph_rel.alias.clone()),
                                    use_final: rel_scan.use_final,
                                }))));
                            }
                        }

                        // A.2: Polymorphic edge - use the labeled node
                        if let LogicalPlan::GraphNode(left_node) = graph_rel.left.as_ref() {
                            if let LogicalPlan::ViewScan(scan) = left_node.input.as_ref() {
                                log::info!(
                                    "🎯 POLYMORPHIC: Using left node '{}' as FROM",
                                    left_node.alias
                                );
                                return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::GraphNode(
                                        left_node.clone(),
                                    )),
                                    name: scan.source_table.clone(),
                                    alias: Some(left_node.alias.clone()),
                                    use_final: scan.use_final,
                                }))));
                            }
                        }
                        if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                            if let LogicalPlan::ViewScan(scan) = right_node.input.as_ref() {
                                log::info!(
                                    "🎯 POLYMORPHIC: Using right node '{}' as FROM",
                                    right_node.alias
                                );
                                return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::GraphNode(
                                        right_node.clone(),
                                    )),
                                    name: scan.source_table.clone(),
                                    alias: Some(right_node.alias.clone()),
                                    use_final: scan.use_final,
                                }))));
                            }
                        }
                    }

                    // A.3: Node-only query (MATCH (n:Label) RETURN n)
                    if let Some(graph_node) = find_graph_node(&graph_joins.input) {
                        if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                            log::info!("🎯 NODE-ONLY: Using node '{}' as FROM", graph_node.alias);
                            let view_ref = super::ViewTableRef::new_table_with_alias(
                                scan.as_ref().clone(),
                                scan.source_table.clone(),
                                graph_node.alias.clone(),
                            );
                            return Ok(Some(FromTable::new(Some(view_ref))));
                        }
                    }

                    // A.4: CartesianProduct (WITH...MATCH or comma patterns)
                    if let Some(cp) = find_cartesian_product(&graph_joins.input) {
                        if is_cte_reference(&cp.left) {
                            log::info!("🎯 WITH...MATCH: FROM comes from right side");
                            return cp.right.extract_from();
                        } else {
                            log::info!("🎯 COMMA PATTERN: FROM comes from left side");
                            return cp.left.extract_from();
                        }
                    }

                    // No valid FROM found for empty joins - this is unexpected
                    log::warn!("⚠️ GraphJoins has empty joins and no recognizable pattern - returning None");
                    return Ok(None);
                }

                // CASE B: Has joins but no FROM marker
                // This happens for OPTIONAL MATCH where the anchor comes from a prior MATCH
                // The anchor_table is set but the anchor table info is in the input plan, not in joins
                //
                // ALSO: After WITH scope barriers, anchor_table may be None if the original anchor
                // was not exported by the WITH. In this case, pick the first join as anchor.
                if let Some(anchor_alias) = &graph_joins.anchor_table {
                    log::info!(
                        "🔍 No FROM marker in joins, looking for anchor '{}' in input plan",
                        anchor_alias
                    );

                    // Try to find the anchor table in the input plan tree
                    // For OPTIONAL MATCH, the anchor is from the first MATCH (which is in input)
                    let rel_tables = extract_rel_and_node_tables(&graph_joins.input);
                    if let Some(table_name) = rel_tables.get(anchor_alias) {
                        log::info!(
                            "✅ Found anchor '{}' table '{}' in input plan",
                            anchor_alias,
                            table_name
                        );
                        return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                            source: std::sync::Arc::new(LogicalPlan::Empty),
                            name: table_name.clone(),
                            alias: Some(anchor_alias.clone()),
                            use_final: false,
                        }))));
                    }

                    // Also check CTE references
                    if let Some(cte_name) = graph_joins.cte_references.get(anchor_alias) {
                        log::info!(
                            "✅ Anchor '{}' has CTE reference: '{}'",
                            anchor_alias,
                            cte_name
                        );
                        return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                            source: std::sync::Arc::new(LogicalPlan::Empty),
                            name: cte_name.clone(),
                            alias: Some(anchor_alias.clone()),
                            use_final: false,
                        }))));
                    }

                    // Try find_table_name_for_alias as last resort
                    if let Some(table_name) =
                        find_table_name_for_alias(&graph_joins.input, anchor_alias)
                    {
                        log::info!(
                            "✅ Found anchor '{}' via find_table_name_for_alias: '{}'",
                            anchor_alias,
                            table_name
                        );
                        return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                            source: std::sync::Arc::new(LogicalPlan::Empty),
                            name: table_name,
                            alias: Some(anchor_alias.clone()),
                            use_final: false,
                        }))));
                    }
                } else {
                    // No anchor_table - likely cleared due to scope barrier
                    // PRIORITY: If we have CTE references, use the LATEST CTE as FROM
                    // The CTE references represent variables that are in scope after WITH clauses
                    // We want the LAST CTE (highest sequence number) as it represents the final scope

                    if !graph_joins.cte_references.is_empty() {
                        log::warn!(
                            "🔍 anchor_table is None, but have {} CTE references - finding latest CTE as FROM",
                            graph_joins.cte_references.len()
                        );

                        // Find the CTE with the highest sequence number (format: with_*_cte_N)
                        // This is the most recent WITH clause's output
                        let mut best_cte: Option<(&String, &String, usize)> = None;
                        for (alias, cte_name) in &graph_joins.cte_references {
                            // Extract sequence number from CTE name
                            // Format: "with_tag_cte_1" or "with_inValidPostCount_postCount_tag_cte_1"
                            let seq_num = if let Some(pos) = cte_name.rfind("_cte_") {
                                cte_name[pos + 5..].parse::<usize>().unwrap_or(0)
                            } else {
                                0
                            };

                            // Keep the CTE with highest sequence number (latest in the chain)
                            // Tie-breaker: prefer longer CTE names (more aliases = more complete)
                            match &best_cte {
                                None => best_cte = Some((alias, cte_name, seq_num)),
                                Some((_, current_name, current_seq)) => {
                                    if seq_num > *current_seq
                                        || (seq_num == *current_seq
                                            && cte_name.len() > current_name.len())
                                    {
                                        best_cte = Some((alias, cte_name, seq_num));
                                    }
                                }
                            }
                        }

                        if let Some((alias, cte_name, _)) = best_cte {
                            log::info!(
                                "✅ Using latest CTE '{}' AS '{}' as FROM (from cte_references)",
                                cte_name,
                                alias
                            );
                            return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: cte_name.clone(),
                                alias: Some(alias.clone()),
                                use_final: false,
                            }))));
                        }
                    }

                    // SECONDARY FALLBACK: Pick first join as FROM table
                    log::warn!(
                        "🔍 anchor_table is None and no CTE references, using first join as FROM"
                    );
                    if let Some(first_join) = graph_joins.joins.first() {
                        // Check if this join has a CTE reference
                        if let Some(cte_name) =
                            graph_joins.cte_references.get(&first_join.table_alias)
                        {
                            log::info!(
                                "✅ Using first join '{}' → CTE '{}' as FROM",
                                first_join.table_alias,
                                cte_name
                            );
                            return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: cte_name.clone(),
                                alias: Some(first_join.table_alias.clone()),
                                use_final: false,
                            }))));
                        } else {
                            log::info!(
                                "✅ Using first join '{}' (table '{}') as FROM",
                                first_join.table_alias,
                                first_join.table_name
                            );
                            return Ok(Some(FromTable::new(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: first_join.table_name.clone(),
                                alias: Some(first_join.table_alias.clone()),
                                use_final: false,
                            }))));
                        }
                    }
                }

                // If we still can't find FROM, this is a real bug
                log::error!("❌ BUG: GraphJoins has {} joins but NO FROM marker and couldn't resolve anchor! anchor_table={:?}",
                    graph_joins.joins.len(), graph_joins.anchor_table);
                for (i, join) in graph_joins.joins.iter().enumerate() {
                    log::error!(
                        "  join[{}]: table='{}' alias='{}' conditions={}",
                        i,
                        join.table_name,
                        join.table_alias,
                        join.joining_on.len()
                    );
                }

                // Return None to surface the bug
                None
            }
            LogicalPlan::GroupBy(group_by) => {
                from_table_to_view_ref(group_by.input.extract_from()?)
            }
            LogicalPlan::OrderBy(order_by) => {
                from_table_to_view_ref(order_by.input.extract_from()?)
            }
            LogicalPlan::Skip(skip) => from_table_to_view_ref(skip.input.extract_from()?),
            LogicalPlan::Limit(limit) => from_table_to_view_ref(limit.input.extract_from()?),
            LogicalPlan::Cte(cte) => from_table_to_view_ref(cte.input.extract_from()?),
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
            LogicalPlan::Unwind(u) => from_table_to_view_ref(u.input.extract_from()?),
            LogicalPlan::CartesianProduct(cp) => {
                // Try left side first (for most queries)
                let left_from = cp.left.extract_from()?;
                if left_from.is_some() {
                    // Left has a table, use it (normal case)
                    from_table_to_view_ref(left_from)
                } else {
                    // Left has no FROM (e.g., WITH clause creating a CTE)
                    // Use right side as FROM source (e.g., new MATCH after WITH)
                    log::info!(
                        "CartesianProduct: Left side has no FROM (likely CTE), using right side"
                    );
                    from_table_to_view_ref(cp.right.extract_from()?)
                }
            }
            LogicalPlan::WithClause(wc) => from_table_to_view_ref(wc.input.extract_from()?),
        };
        Ok(view_ref_to_from_table(from_ref))
    }

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::ViewScan(scan) => {
                // ViewScan.view_filter should be None after CleanupViewScanFilters optimizer.
                // All filters are consolidated in GraphRel.where_predicate.
                // This case handles standalone ViewScans outside of GraphRel contexts.
                let mut filters = Vec::new();

                // Add view_filter if present
                if let Some(ref filter) = scan.view_filter {
                    let mut expr: RenderExpr = filter.clone().try_into()?;
                    apply_property_mapping_to_expr(&mut expr, &LogicalPlan::ViewScan(scan.clone()));
                    filters.push(expr);
                }

                // Add schema_filter if present (defined in YAML schema)
                if let Some(ref schema_filter) = scan.schema_filter {
                    // Use a default alias for standalone ViewScans
                    // In practice, these will be wrapped in GraphNode which provides the alias
                    if let Ok(sql) = schema_filter.to_sql("t") {
                        log::debug!("ViewScan: Adding schema filter: {}", sql);
                        filters.push(RenderExpr::Raw(sql));
                    }
                }

                if filters.is_empty() {
                    None
                } else if filters.len() == 1 {
                    Some(filters.into_iter().next().unwrap())
                } else {
                    // Combine with AND
                    let combined = filters
                        .into_iter()
                        .reduce(|acc, pred| {
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![acc, pred],
                            })
                        })
                        .unwrap();
                    Some(combined)
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // For node-only queries, extract both view_filter and schema_filter from the input ViewScan
                if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                    log::info!(
                        "🔍 GraphNode '{}' extract_filters: ViewScan table={}",
                        graph_node.alias,
                        scan.source_table
                    );

                    let mut filters = Vec::new();

                    // Extract view_filter (user's WHERE clause, injected by optimizer)
                    if let Some(ref view_filter) = scan.view_filter {
                        log::debug!(
                            "extract_filters: view_filter BEFORE conversion: {:?}",
                            view_filter
                        );
                        let mut expr: RenderExpr = view_filter.clone().try_into()?;
                        log::debug!("extract_filters: view_filter AFTER conversion: {:?}", expr);
                        apply_property_mapping_to_expr(&mut expr, &graph_node.input);
                        log::debug!(
                            "extract_filters: view_filter AFTER property mapping: {:?}",
                            expr
                        );
                        log::info!(
                            "GraphNode '{}': Adding view_filter: {:?}",
                            graph_node.alias,
                            expr
                        );
                        filters.push(expr);
                    }

                    // Extract schema_filter (from YAML schema)
                    // Wrap in parentheses to ensure correct operator precedence when combined with user filters
                    if let Some(ref schema_filter) = scan.schema_filter {
                        if let Ok(sql) = schema_filter.to_sql(&graph_node.alias) {
                            log::info!(
                                "GraphNode '{}': Adding schema filter: {}",
                                graph_node.alias,
                                sql
                            );
                            // Always wrap schema filter in parentheses for safe combination
                            filters.push(RenderExpr::Raw(format!("({})", sql)));
                        }
                    }

                    // Combine filters with AND if multiple
                    // Use explicit AND combination - each operand will be wrapped appropriately
                    if filters.is_empty() {
                        return Ok(None);
                    } else if filters.len() == 1 {
                        return Ok(Some(filters.into_iter().next().unwrap()));
                    } else {
                        // When combining filters, wrap non-Raw expressions in parentheses
                        // to handle AND/OR precedence correctly
                        let combined = filters
                            .into_iter()
                            .reduce(|acc, pred| {
                                // The OperatorApplicationExp will render as "(left) AND (right)"
                                // due to the render_expr_to_sql_string logic
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, pred],
                                })
                            })
                            .unwrap();
                        return Ok(Some(combined));
                    }
                }
                None
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!(
                    "GraphRel node detected, collecting filters from ALL nested where_predicates"
                );

                // Collect all where_predicates from this GraphRel and nested GraphRel nodes
                // Using helper functions from plan_builder_helpers module
                let all_predicates =
                    collect_graphrel_predicates(&LogicalPlan::GraphRel(graph_rel.clone()));

                let mut all_predicates = all_predicates;

                // 🔒 Add schema-level filters from ViewScans
                let schema_filters =
                    collect_schema_filters(&LogicalPlan::GraphRel(graph_rel.clone()), None);
                if !schema_filters.is_empty() {
                    log::info!(
                        "Adding {} schema filter(s) to WHERE clause",
                        schema_filters.len()
                    );
                    all_predicates.extend(schema_filters);
                }

                // TODO: Add relationship uniqueness filters for undirected multi-hop patterns
                // This requires fixing Issue #1 (Undirected Multi-Hop Patterns Generate Broken SQL) first.
                // See KNOWN_ISSUES.md for details.
                // Currently, undirected multi-hop patterns generate broken SQL with wrong aliases,
                // so adding uniqueness filters here would not work correctly.

                // 🚀 ADD CYCLE PREVENTION for fixed-length paths (only for 2+ hops)
                // Single hop (*1) can't have cycles - no need for cycle prevention
                if let Some(spec) = &graph_rel.variable_length {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        // Skip cycle prevention for *1 - single hop can't cycle
                        if exact_hops >= 2 && graph_rel.shortest_path_mode.is_none() {
                            crate::debug_println!("DEBUG: extract_filters - Adding cycle prevention for fixed-length *{}", exact_hops);

                            // Check if this is a denormalized pattern
                            let is_denormalized = is_node_denormalized(&graph_rel.left)
                                && is_node_denormalized(&graph_rel.right);

                            // Extract table/column info for cycle prevention
                            // Use extract_table_name directly to avoid wrong fallbacks
                            let start_table =
                                extract_table_name(&graph_rel.left).ok_or_else(|| {
                                    RenderBuildError::MissingTableInfo(
                                        "start node in cycle prevention".to_string(),
                                    )
                                })?;
                            let end_table =
                                extract_table_name(&graph_rel.right).ok_or_else(|| {
                                    RenderBuildError::MissingTableInfo(
                                        "end node in cycle prevention".to_string(),
                                    )
                                })?;

                            let rel_cols = extract_relationship_columns(&graph_rel.center)
                                .unwrap_or(RelationshipColumns {
                                    from_id: "from_node_id".to_string(),
                                    to_id: "to_node_id".to_string(),
                                });

                            // For denormalized, use relationship columns directly
                            // For normal, use node ID columns
                            let (start_id_col, end_id_col) = if is_denormalized {
                                (rel_cols.from_id.clone(), rel_cols.to_id.clone())
                            } else {
                                let start = extract_id_column(&graph_rel.left)
                                    .unwrap_or_else(|| table_to_id_column(&start_table));
                                let end = extract_id_column(&graph_rel.right)
                                    .unwrap_or_else(|| table_to_id_column(&end_table));
                                (start, end)
                            };

                            // Generate cycle prevention filters
                            if let Some(cycle_filter) = crate::render_plan::cte_extraction::generate_cycle_prevention_filters(
                                exact_hops,
                                &start_id_col,
                                &rel_cols.to_id,
                                &rel_cols.from_id,
                                &end_id_col,
                                &graph_rel.left_connection,
                                &graph_rel.right_connection,
                            ) {
                                crate::debug_println!("DEBUG: extract_filters - Generated cycle prevention filter");
                                all_predicates.push(cycle_filter);
                            }
                        }
                    }
                }

                if all_predicates.is_empty() {
                    None
                } else if all_predicates.len() == 1 {
                    log::trace!("Found 1 GraphRel predicate");
                    Some(all_predicates.into_iter().next().unwrap())
                } else {
                    // Combine with AND
                    log::trace!(
                        "Found {} GraphRel predicates, combining with AND",
                        all_predicates.len()
                    );
                    let combined = all_predicates
                        .into_iter()
                        .reduce(|acc, pred| {
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![acc, pred],
                            })
                        })
                        .unwrap();
                    Some(combined)
                }
            }
            LogicalPlan::Filter(filter) => {
                println!(
                    "DEBUG: extract_filters - Found Filter node with predicate: {:?}",
                    filter.predicate
                );
                println!(
                    "DEBUG: extract_filters - Filter input type: {:?}",
                    std::mem::discriminant(&*filter.input)
                );
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);

                // Also check for schema filters from the input (e.g., GraphNode → ViewScan)
                if let Some(input_filter) = filter.input.extract_filters()? {
                    crate::debug_println!("DEBUG: extract_filters - Combining Filter predicate with input schema filter");
                    // Combine the Filter predicate with input's schema filter using AND
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![input_filter, expr],
                    }))
                } else {
                    crate::debug_println!("DEBUG: extract_filters - Returning Filter predicate only (no input filter)");
                    Some(expr)
                }
            }
            LogicalPlan::Projection(projection) => {
                crate::debug_println!(
                    "DEBUG: extract_filters - Projection, recursing to input type: {:?}",
                    std::mem::discriminant(&*projection.input)
                );
                projection.input.extract_filters()?
            }
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters()?,
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
            LogicalPlan::Unwind(u) => u.input.extract_filters()?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine filters from both sides with AND
                let left_filters = cp.left.extract_filters()?;
                let right_filters = cp.right.extract_filters()?;

                // DEBUG: Log what we're extracting
                log::info!("🔍 CartesianProduct extract_filters:");
                log::info!("  Left filters: {:?}", left_filters);
                log::info!("  Right filters: {:?}", right_filters);

                match (left_filters, right_filters) {
                    (None, None) => None,
                    (Some(l), None) => {
                        log::info!("  ✅ Returning left filters only");
                        Some(l)
                    }
                    (None, Some(r)) => {
                        log::info!("  ✅ Returning right filters only");
                        Some(r)
                    }
                    (Some(l), Some(r)) => {
                        log::warn!("  ⚠️ BOTH sides have filters - combining with AND!");
                        log::warn!("  ⚠️ This may cause duplicates if filters are the same!");
                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![l, r],
                        }))
                    }
                }
            }
            LogicalPlan::WithClause(wc) => wc.input.extract_filters()?,
        };
        Ok(filters)
    }

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let final_filters = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_final_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_final_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_final_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_final_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_final_filters()?,
            LogicalPlan::Projection(projection) => projection.input.extract_final_filters()?,
            LogicalPlan::Filter(filter) => {
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);
                Some(expr)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // For GraphRel, extract path function filters that should be applied to the final query
                if let Some(logical_expr) = &graph_rel.where_predicate {
                    let mut filter_expr: RenderExpr = logical_expr.clone().try_into()?;
                    // Apply property mapping to the where predicate
                    apply_property_mapping_to_expr(
                        &mut filter_expr,
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                    );
                    let start_alias = graph_rel.left_connection.clone();
                    let end_alias = graph_rel.right_connection.clone();

                    // For extract_final_filters, we only need to categorize path function filters
                    // Schema-aware categorization is not needed here since this is just for
                    // separating path functions from other filters. Use a dummy categorization.
                    let rel_labels = graph_rel.labels.clone().unwrap_or_default();

                    // Try to get schema for proper categorization
                    use crate::server::GLOBAL_SCHEMAS;
                    let schemas_lock = GLOBAL_SCHEMAS.get().expect("Schemas not initialized");
                    let schemas = schemas_lock
                        .try_read()
                        .expect("Failed to acquire schema lock");

                    // Try to find a schema that has this relationship type
                    let schema_for_categorization = if !rel_labels.is_empty() {
                        schemas.values().find(|s| {
                            rel_labels
                                .iter()
                                .any(|label| s.get_rel_schema(label).is_ok())
                        })
                    } else {
                        None
                    };

                    let schema_ref = schema_for_categorization.unwrap_or_else(|| {
                        schemas
                            .values()
                            .next()
                            .expect("At least one schema must be loaded")
                    });

                    let categorized = categorize_filters(
                        Some(&filter_expr),
                        &start_alias,
                        &end_alias,
                        &graph_rel.alias,
                        schema_ref,
                        &rel_labels,
                    );

                    categorized.path_function_filters
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(final_filters)
    }

    fn extract_joins(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Vec<Join>> {
        // Helper functions for edge constraint compilation

        // Extract relationship type and node labels from GraphRel in the plan
        fn extract_relationship_context(
            plan: &LogicalPlan,
            rel_alias: &str,
        ) -> Option<(String, String, String)> {
            match plan {
                LogicalPlan::Projection(proj) => {
                    extract_relationship_context(&proj.input, rel_alias)
                }
                LogicalPlan::Filter(filter) => {
                    extract_relationship_context(&filter.input, rel_alias)
                }
                LogicalPlan::GraphRel(gr) if gr.alias == rel_alias => {
                    let rel_type = gr.labels.as_ref()?.first()?.clone();
                    let from_label = extract_node_label(&gr.left)?;
                    let to_label = extract_node_label(&gr.right)?;
                    Some((rel_type, from_label, to_label))
                }
                LogicalPlan::GraphRel(gr) => extract_relationship_context(&gr.left, rel_alias)
                    .or_else(|| extract_relationship_context(&gr.center, rel_alias))
                    .or_else(|| extract_relationship_context(&gr.right, rel_alias)),
                LogicalPlan::GraphNode(gn) => extract_relationship_context(&gn.input, rel_alias),
                _ => None,
            }
        }

        // Extract node label from GraphNode
        fn extract_node_label(plan: &LogicalPlan) -> Option<String> {
            match plan {
                LogicalPlan::GraphNode(gn) => gn.label.clone(),
                _ => None,
            }
        }

        // Extract relationship context for FK-edge patterns
        // For FK-edge, the JOIN uses the to_node alias, so we search for GraphRel nodes
        // that connect to this alias
        fn extract_fk_edge_relationship_context(
            plan: &LogicalPlan,
            node_alias: &str,
        ) -> Option<(String, String, String)> {
            match plan {
                LogicalPlan::Projection(proj) => {
                    extract_fk_edge_relationship_context(&proj.input, node_alias)
                }
                LogicalPlan::Filter(filter) => {
                    extract_fk_edge_relationship_context(&filter.input, node_alias)
                }
                LogicalPlan::GraphRel(gr) => {
                    // Check if this GraphRel's right node (to_node) matches the alias
                    if let LogicalPlan::GraphNode(to_node) = &*gr.right {
                        if to_node.alias == node_alias {
                            let rel_type = gr.labels.as_ref()?.first()?.clone();
                            let from_label = extract_node_label(&gr.left)?;
                            let to_label = to_node.label.clone()?;
                            return Some((rel_type, from_label, to_label));
                        }
                    }

                    // Recurse into nested patterns
                    extract_fk_edge_relationship_context(&gr.left, node_alias)
                        .or_else(|| extract_fk_edge_relationship_context(&gr.center, node_alias))
                        .or_else(|| extract_fk_edge_relationship_context(&gr.right, node_alias))
                }
                LogicalPlan::GraphNode(gn) => {
                    extract_fk_edge_relationship_context(&gn.input, node_alias)
                }
                _ => None,
            }
        }

        // Extract from/to node aliases for a relationship alias from JOIN list and anchor
        fn extract_node_aliases_from_joins(
            joins: &[crate::query_planner::logical_plan::Join],
            rel_alias: &str,
        ) -> Option<(String, String)> {
            // From alias: look for FROM marker (JOIN with no conditions = anchor node)
            let from_alias = joins
                .iter()
                .find(|j| j.joining_on.is_empty())
                .map(|j| j.table_alias.clone());

            // To alias: find the OTHER node (not the relationship, not the anchor, has conditions)
            let to_alias = joins
                .iter()
                .find(|j| {
                    j.table_alias != rel_alias
                        && !j.joining_on.is_empty()
                        && j.table_alias != from_alias.as_ref().map(|s| s.as_str()).unwrap_or("")
                })
                .map(|j| j.table_alias.clone());

            match (from_alias, to_alias) {
                (Some(from), Some(to)) => {
                    log::info!("🔍 Extracted node aliases: from={}, to={}", from, to);
                    Some((from, to))
                }
                _ => {
                    log::warn!(
                        "⚠️  Could not extract node aliases for relationship {}",
                        rel_alias
                    );
                    None
                }
            }
        }

        // Main extract_joins implementation

        // Use helper functions from plan_builder_helpers module
        // get_schema_filter_for_node() - extracts schema filter from LogicalPlan
        // get_polymorphic_edge_filter_for_join() - generates polymorphic edge type filter
        // extract_predicates_for_alias_logical() - extracts predicates for specific alias
        // combine_render_exprs_with_and() - combines filters with AND

        let joins = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_joins(schema)?,
            LogicalPlan::Skip(skip) => skip.input.extract_joins(schema)?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_joins(schema)?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_joins(schema)?,
            LogicalPlan::Filter(filter) => filter.input.extract_joins(schema)?,
            LogicalPlan::Projection(projection) => projection.input.extract_joins(schema)?,
            LogicalPlan::GraphNode(graph_node) => {
                // For nested GraphNodes (multiple standalone nodes), create CROSS JOINs
                let mut joins = vec![];

                // If this GraphNode has another GraphNode as input, create a CROSS JOIN for the inner node
                if let LogicalPlan::GraphNode(inner_node) = graph_node.input.as_ref() {
                    if let Some(table_name) = extract_table_name(&graph_node.input) {
                        joins.push(Join {
                            table_name,
                            table_alias: inner_node.alias.clone(), // Use the inner GraphNode's alias
                            joining_on: vec![],                    // Empty for CROSS JOIN
                            join_type: JoinType::Join,             // CROSS JOIN
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                        });
                    }
                }

                // Recursively get joins from the input
                let mut inner_joins = graph_node.input.extract_joins(schema)?;
                joins.append(&mut inner_joins);

                joins
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // 🔧 FIX: For GraphJoins with CTE references, delegate to input.extract_joins()
                // The analyzer creates joins that reference CTEs (like "with_friend_cte_1"),
                // but they're in the input plan, not in the deprecated graph_joins.joins field.
                // We need to delegate to the input to get the actual joins.
                if !graph_joins.cte_references.is_empty() {
                    log::warn!(
                        "🔧 GraphJoins has {} CTE references - delegating to input.extract_joins()",
                        graph_joins.cte_references.len()
                    );
                    for (alias, cte_name) in &graph_joins.cte_references {
                        log::warn!("  CTE ref: {} → {}", alias, cte_name);
                    }
                    // Delegate to input to get the joins with CTE references
                    return graph_joins.input.extract_joins(schema);
                }

                // Check if input has a fixed-length variable-length pattern with >1 hops
                // For those, we need to use the expanded JOINs from extract_joins on the input
                // (which will call GraphRel.extract_joins -> expand_fixed_length_joins)
                if let Some(spec) = get_variable_length_spec(&graph_joins.input) {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        if exact_hops > 1 {
                            println!(
                                "DEBUG: GraphJoins has fixed-length *{} input - delegating to input.extract_joins()",
                                exact_hops
                            );
                            // Delegate to input to get the expanded multi-hop JOINs
                            return graph_joins.input.extract_joins(schema);
                        }
                    }
                }

                // FIX: Use ViewScan source_table instead of deprecated joins field table_name
                // The deprecated joins field has incorrect table names for polymorphic relationships
                // Extract alias → parameterized table reference mapping from GraphRel/GraphNode nodes
                // This uses the centralized helper that handles parameterized views correctly

                // Use the centralized helper from plan_builder_helpers.rs
                let rel_tables = extract_rel_and_node_tables(graph_joins.input.as_ref());

                // Collect edge constraints to apply to the final node JOIN
                // Edge constraints reference both from/to nodes, so must be applied after both are joined
                let mut edge_constraints: Vec<(String, RenderExpr)> = Vec::new();

                // Convert joins, SKIPPING FROM markers (joins with empty joining_on)
                // FROM markers are used by extract_from(), not extract_joins()
                let mut joins: Vec<Join> = Vec::new();
                for logical_join in &graph_joins.joins {
                    // SKIP FROM markers - they have empty joining_on
                    if logical_join.joining_on.is_empty() {
                        continue;
                    }

                    let mut render_join: Join = logical_join.clone().try_into()?;

                    // Compile edge constraints for relationship JOINs (if constraints defined in schema)
                    // Store them to be applied to the final node JOIN (where both from/to tables are available)
                    if render_join.from_id_column.is_some() && render_join.to_id_column.is_some() {
                        log::debug!(
                            "🔍 JOIN {} has from_id/to_id columns - checking for constraints",
                            render_join.table_alias
                        );

                        // This is a relationship JOIN (has from/to ID columns)
                        // Try two patterns:
                        // 1. Standard edge: table_alias matches GraphRel rel_alias (e.g., "c" for COPIED_BY)
                        // 2. FK-edge: table_alias matches GraphRel to_node alias (e.g., "folder" for IN_FOLDER)
                        let rel_context = extract_relationship_context(
                            &graph_joins.input,
                            &render_join.table_alias,
                        )
                        .or_else(|| {
                            log::debug!(
                                "Standard rel context not found, trying FK-edge pattern..."
                            );
                            extract_fk_edge_relationship_context(
                                &graph_joins.input,
                                &render_join.table_alias,
                            )
                        });

                        if let Some((rel_type, from_label, to_label)) = rel_context {
                            log::debug!(
                                "✓ Found relationship context: type={}, from={}, to={}",
                                rel_type,
                                from_label,
                                to_label
                            );
                            if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type)
                            {
                                log::debug!("✓ Found relationship schema for {}", rel_type);
                                if let Some(ref constraint_expr) = rel_schema.constraints {
                                    log::debug!(
                                        "✓ Found constraint expression: {}",
                                        constraint_expr
                                    );
                                    if let (Some(from_schema), Some(to_schema)) = (
                                        schema.get_node_schema_opt(&from_label),
                                        schema.get_node_schema_opt(&to_label),
                                    ) {
                                        // Find from/to aliases from JOIN conditions
                                        // For standard edge, use extract_node_aliases_from_joins
                                        // For FK-edge, extract directly from JOIN (from and to are the joined nodes)
                                        let node_aliases = extract_node_aliases_from_joins(
                                            &graph_joins.joins,
                                            &render_join.table_alias,
                                        )
                                        .or_else(|| {
                                            // FK-edge fallback: infer from the JOIN itself
                                            // The JOIN connects from_node to to_node, alias is to_node
                                            // For FK-edge with 1 JOIN, the anchor is the from_node
                                            let from_alias_opt = graph_joins
                                                .joins
                                                .iter()
                                                .find(|j| {
                                                    !j.joining_on.is_empty()
                                                        && j.table_alias != render_join.table_alias
                                                })
                                                .map(|j| j.table_alias.clone())
                                                .or_else(|| {
                                                    // If no other JOIN, from_node is the anchor (FROM table)
                                                    // Use graph_joins.anchor_table
                                                    graph_joins.anchor_table.clone()
                                                });

                                            from_alias_opt.map(|from_alias| {
                                                log::debug!(
                                                    "🔍 FK-edge alias extraction: from={}, to={}",
                                                    from_alias,
                                                    render_join.table_alias
                                                );
                                                (from_alias, render_join.table_alias.clone())
                                            })
                                        });

                                        if let Some((from_alias, to_alias)) = node_aliases {
                                            match crate::graph_catalog::constraint_compiler::compile_constraint(
                                                constraint_expr,
                                                from_schema,
                                                to_schema,
                                                &from_alias,
                                                &to_alias,
                                            ) {
                                                Ok(compiled_sql) => {
                                                    log::info!("✓ Compiled edge constraint for {} ({}): {} → {}",
                                                        render_join.table_alias, rel_type, constraint_expr, compiled_sql);

                                                    // Store constraint to apply to this JOIN directly
                                                    // (for FK-edge, this IS the to_node JOIN)
                                                    let constraint_expr = RenderExpr::Raw(compiled_sql);
                                                    edge_constraints.push((to_alias, constraint_expr));
                                                }
                                                Err(e) => {
                                                    log::warn!("Failed to compile edge constraint for {}: {}", render_join.table_alias, e);
                                                }
                                            }
                                        } else {
                                            log::debug!("Could not extract node aliases for FK-edge JOIN: {}", render_join.table_alias);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Fix table_name if we have a resolved table for this alias
                    if let Some(resolved_table) = rel_tables.get(&render_join.table_alias) {
                        render_join.table_name = resolved_table.clone();
                    }

                    joins.push(render_join);
                }

                // Apply collected edge constraints to the appropriate node JOINs
                for (to_alias, constraint) in edge_constraints {
                    if let Some(join) = joins.iter_mut().find(|j| j.table_alias == to_alias) {
                        join.pre_filter = if let Some(existing) = join.pre_filter.clone() {
                            Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing, constraint],
                            }))
                        } else {
                            Some(constraint)
                        };
                        log::debug!("Applied edge constraint to node JOIN: {}", to_alias);
                    }
                }

                joins
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate JOINs for the relationship traversal
                // This fixes OPTIONAL MATCH queries by creating proper JOIN clauses

                // 🚀 FIXED-LENGTH VLP: Use consolidated VlpContext for all schema types
                if let Some(vlp_ctx) = build_vlp_context(graph_rel) {
                    let exact_hops = vlp_ctx.exact_hops.unwrap_or(1);

                    // Special case: *0 pattern (zero hops = same node)
                    // Return empty joins - both a and b reference the same node
                    if vlp_ctx.is_fixed_length && exact_hops == 0 {
                        crate::debug_println!(
                            "DEBUG: extract_joins - Zero-hop pattern (*0) - returning empty joins"
                        );
                        return Ok(Vec::new());
                    }

                    if vlp_ctx.is_fixed_length && exact_hops > 0 {
                        println!(
                            "DEBUG: extract_joins - Fixed-length VLP (*{}) with {:?} schema",
                            exact_hops, vlp_ctx.schema_type
                        );

                        // Use the consolidated function that handles all schema types
                        let (_from_table, _from_alias, joins) =
                            expand_fixed_length_joins_with_context(&vlp_ctx);

                        // Store the VLP context for later use by FROM clause and property resolution
                        // (This is done via the existing pattern of passing info through the plan)

                        return Ok(joins);
                    }

                    // VARIABLE-LENGTH VLP (recursive CTE): Return empty joins
                    // The recursive CTE handles the relationship traversal, so we don't need
                    // to generate the relationship table join here. The endpoint JOINs
                    // (to Person tables) will be added by the VLP rendering logic.
                    if !vlp_ctx.is_fixed_length {
                        crate::debug_println!("DEBUG: extract_joins - Variable-length VLP (recursive CTE) - returning empty joins");
                        return Ok(Vec::new());
                    }
                }

                // MULTI-HOP FIX: If left side is another GraphRel, recursively extract its joins first
                // This handles patterns like (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
                let mut joins = vec![];

                // DENORMALIZED EDGE TABLE CHECK
                // For denormalized patterns, nodes are virtual (stored on edge table)
                // We need to JOIN edge tables directly, not node tables
                let left_is_denormalized = is_node_denormalized(&graph_rel.left);
                let right_is_denormalized = is_node_denormalized(&graph_rel.right);

                println!(
                    "DEBUG: extract_joins - left_is_denormalized={}, right_is_denormalized={}",
                    left_is_denormalized, right_is_denormalized
                );

                // For denormalized patterns, handle specially
                if left_is_denormalized && right_is_denormalized {
                    crate::debug_println!("DEBUG: DENORMALIZED multi-hop pattern detected");

                    // Get the relationship table with parameterized view syntax if applicable
                    let rel_table = extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone());

                    // Get relationship columns (from_id and to_id)
                    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                        RelationshipColumns {
                            from_id: "from_node_id".to_string(),
                            to_id: "to_node_id".to_string(),
                        },
                    );

                    // Check if this is a chained hop (left side is another GraphRel)
                    if let LogicalPlan::GraphRel(left_rel) = graph_rel.left.as_ref() {
                        println!(
                            "DEBUG: DENORMALIZED multi-hop - chaining {} -> {}",
                            left_rel.alias, graph_rel.alias
                        );

                        // First, recursively get joins from the left GraphRel
                        let mut left_joins = graph_rel.left.extract_joins(schema)?;
                        joins.append(&mut left_joins);

                        // Get the left relationship's to_id column for joining
                        let left_rel_cols = extract_relationship_columns(&left_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // =========================================================
                        // COUPLED EDGE DETECTION
                        // =========================================================
                        // Check if the left and current edges are coupled (same table, coupling node)
                        // If so, they exist in the same row - NO JOIN needed!
                        let current_rel_type =
                            graph_rel.labels.as_ref().and_then(|l| l.first().cloned());
                        let left_rel_type =
                            left_rel.labels.as_ref().and_then(|l| l.first().cloned());

                        if let (Some(curr_type), Some(left_type)) =
                            (current_rel_type, left_rel_type)
                        {
                            // Try to get coupling info from schema
                            if let Some(schema_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                if let Ok(schemas) = schema_lock.try_read() {
                                    // Try different schema names
                                    for schema_name in ["default", ""] {
                                        if let Some(schema) = schemas.get(schema_name) {
                                            if let Some(coupling_info) =
                                                schema.get_coupled_edge_info(&left_type, &curr_type)
                                            {
                                                println!(
                                                    "DEBUG: COUPLED EDGES DETECTED! {} and {} share coupling node {} in table {}",
                                                    left_type, curr_type, coupling_info.coupling_node, coupling_info.table_name
                                                );

                                                // Skip the JOIN - edges are in the same row!
                                                // If arrays need expansion, user should use UNWIND clause
                                                return Ok(joins);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Not coupled - add the JOIN as usual
                        // JOIN this relationship table to the previous one
                        // e.g., INNER JOIN flights AS f2 ON f2.Origin = f1.Dest
                        joins.push(Join {
                            table_name: rel_table.clone(),
                            table_alias: graph_rel.alias.clone(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_rel.alias.clone()),
                                        column: PropertyValue::Column(left_rel_cols.to_id.clone()),
                                    }),
                                ],
                            }],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(rel_cols.from_id.clone()), // Preserve for NULL checks
                            to_id_column: Some(rel_cols.to_id.clone()), // Preserve for NULL checks
                        });
                    }
                    // For single-hop denormalized, no JOINs needed - relationship table IS the data
                    // Just return empty joins, the FROM clause will use the relationship table

                    return Ok(joins);
                }

                // STANDARD (non-denormalized) multi-hop handling
                // MULTI-HOP FIX: Check BOTH left and right sides for nested GraphRel patterns
                if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    println!(
                        "🔍 DEBUG: Multi-hop pattern detected on LEFT side - recursively extracting left GraphRel joins (alias={})",
                        graph_rel.alias
                    );
                    let mut left_joins = graph_rel.left.extract_joins(schema)?;
                    println!("  ↳ Got {} joins from left GraphRel", left_joins.len());
                    joins.append(&mut left_joins);
                }

                // Also check right side for nested GraphRel (e.g., (a)-[r1]->(b)-[r2]->(c))
                // In this case, right side contains (b)-[r2]->(c) which needs its own joins
                if let LogicalPlan::GraphRel(inner_rel) = graph_rel.right.as_ref() {
                    println!(
                        "🔍 DEBUG: Multi-hop pattern detected on RIGHT side - recursively extracting right GraphRel joins (alias={})",
                        graph_rel.alias
                    );

                    // NESTED PATTERN JOIN ORDERING FIX
                    // ================================
                    // For pattern like (post)<-[:HAS_CREATOR]-(f)-[:KNOWS]-(p):
                    // - Outer: left=post, right=inner, right_connection="f" (shared node)
                    // - Inner: left=p, right=f, left_connection="p", right_connection="f"
                    //
                    // The inner extract_joins assumes left_connection (p) is the anchor/FROM,
                    // generating: t1 ON t1.from_id = p.id (WRONG - p not available yet!)
                    //
                    // In nested context, the SHARED node (f) is the anchor, so we need:
                    // - t1 should connect to f (shared): t1.to_id = f.id
                    // - p should connect to t1: p.id = t1.from_id
                    //
                    // SOLUTION: Don't use inner extract_joins which has wrong anchor assumption.
                    // Instead, manually build the correct JOINs for nested patterns.

                    let shared_node_alias = &graph_rel.right_connection;
                    let inner_left_alias = &inner_rel.left_connection;
                    let inner_right_alias = &inner_rel.right_connection;

                    // Determine which inner node is the shared node
                    let shared_is_inner_left = inner_left_alias == shared_node_alias;
                    let shared_is_inner_right = inner_right_alias == shared_node_alias;

                    println!("🔍 DEBUG: Nested pattern - shared='{}', inner_left='{}', inner_right='{}', shared_is_left={}, shared_is_right={}",
                             shared_node_alias, inner_left_alias, inner_right_alias, shared_is_inner_left, shared_is_inner_right);

                    if shared_is_inner_right {
                        // Shared node is inner's right_connection (e.g., f)
                        // Non-shared node is inner's left_connection (e.g., p)
                        // We need:
                        // 1. t1 (relationship) connecting to shared node (f): t1.to_id = f.id
                        // 2. p (non-shared) connecting to t1: p.id = t1.from_id

                        let non_shared_alias = inner_left_alias;

                        let inner_rel_cols = extract_relationship_columns(&inner_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // Get shared node's ID column
                        let shared_id_col = extract_end_node_id_column(&inner_rel.right)
                            .unwrap_or_else(|| "id".to_string());

                        // JOIN 1: Relationship table connecting to shared node
                        // t1.to_id = f.id (since f = right_connection → to_id per GraphRel convention)
                        let rel_table = extract_parameterized_table_ref(&inner_rel.center)
                            .unwrap_or_else(|| inner_rel.alias.clone());

                        let rel_join_condition = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(inner_rel.alias.clone()),
                                    column: PropertyValue::Column(inner_rel_cols.to_id.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(shared_node_alias.clone()),
                                    column: PropertyValue::Column(shared_id_col),
                                }),
                            ],
                        };

                        joins.push(Join {
                            table_name: rel_table,
                            table_alias: inner_rel.alias.clone(),
                            joining_on: vec![rel_join_condition],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(inner_rel_cols.from_id.clone()),
                            to_id_column: Some(inner_rel_cols.to_id.clone()),
                        });

                        // JOIN 2: Non-shared node connecting to relationship
                        // p.id = t1.from_id (since p = left_connection → from_id)
                        if let Some(non_shared_table) = extract_table_name(&inner_rel.left) {
                            let non_shared_id_col = extract_id_column(&inner_rel.left)
                                .unwrap_or_else(|| "id".to_string());

                            let non_shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(non_shared_alias.clone()),
                                        column: PropertyValue::Column(non_shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(inner_rel.alias.clone()),
                                        column: PropertyValue::Column(
                                            inner_rel_cols.from_id.clone(),
                                        ),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name: non_shared_table,
                                table_alias: non_shared_alias.clone(),
                                joining_on: vec![non_shared_join_condition],
                                join_type: JoinType::Inner,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        }

                        println!(
                            "  ✅ Built nested pattern JOINs: {} → {}",
                            inner_rel.alias, non_shared_alias
                        );
                    } else if shared_is_inner_left {
                        // Shared node is inner's left_connection
                        // This case should work with normal extract_joins since left is anchor
                        // But let's still use the manual approach for consistency

                        let non_shared_alias = inner_right_alias;

                        let inner_rel_cols = extract_relationship_columns(&inner_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // Get shared node's ID column
                        let shared_id_col =
                            extract_id_column(&inner_rel.left).unwrap_or_else(|| "id".to_string());

                        // JOIN 1: Relationship connecting to shared node (left)
                        // t1.from_id = f.id (since f = left_connection → from_id)
                        let rel_table = extract_parameterized_table_ref(&inner_rel.center)
                            .unwrap_or_else(|| inner_rel.alias.clone());

                        let rel_join_condition = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(inner_rel.alias.clone()),
                                    column: PropertyValue::Column(inner_rel_cols.from_id.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(shared_node_alias.clone()),
                                    column: PropertyValue::Column(shared_id_col),
                                }),
                            ],
                        };

                        joins.push(Join {
                            table_name: rel_table,
                            table_alias: inner_rel.alias.clone(),
                            joining_on: vec![rel_join_condition],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(inner_rel_cols.from_id.clone()),
                            to_id_column: Some(inner_rel_cols.to_id.clone()),
                        });

                        // JOIN 2: Non-shared node (right) connecting to relationship
                        // p.id = t1.to_id (since p = right_connection → to_id)
                        if let Some(non_shared_table) =
                            extract_end_node_table_name(&inner_rel.right)
                        {
                            let non_shared_id_col = extract_end_node_id_column(&inner_rel.right)
                                .unwrap_or_else(|| "id".to_string());

                            let non_shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(non_shared_alias.clone()),
                                        column: PropertyValue::Column(non_shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(inner_rel.alias.clone()),
                                        column: PropertyValue::Column(inner_rel_cols.to_id.clone()),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name: non_shared_table,
                                table_alias: non_shared_alias.clone(),
                                joining_on: vec![non_shared_join_condition],
                                join_type: JoinType::Inner,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        }

                        println!(
                            "  ✅ Built nested pattern JOINs (left shared): {} → {}",
                            inner_rel.alias, non_shared_alias
                        );
                    } else {
                        // Shared node doesn't match either inner connection - fallback to old behavior
                        println!("⚠️ DEBUG: Shared node '{}' doesn't match inner connections - using fallback", shared_node_alias);
                        let mut right_joins = graph_rel.right.extract_joins(schema)?;
                        joins.append(&mut right_joins);
                    }
                }

                // CTE REFERENCE CHECK: If right side is GraphJoins with pre-computed joins,
                // use those instead of generating new joins. This handles chained WITH clauses
                // where the right node is a CTE reference.
                if let LogicalPlan::GraphJoins(right_joins) = graph_rel.right.as_ref() {
                    println!(
                        "DEBUG: GraphRel.right is GraphJoins with {} pre-computed joins - using them",
                        right_joins.joins.len()
                    );
                    // The GraphJoins contains pre-computed joins that reference the CTE correctly.
                    // However, some joins may have stale conditions referencing tables from
                    // previous WITH clause scopes. Filter those out.

                    // First, add the relationship table join (center -> left node)
                    // Use extract_parameterized_table_ref to handle parameterized views correctly
                    let rel_table = extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| {
                            println!("WARNING: extract_parameterized_table_ref returned None for relationship alias '{}', falling back to alias", graph_rel.alias);
                            graph_rel.alias.clone()
                        });

                    println!("DEBUG extract_joins GraphRel: alias='{}', rel_table from extract_parameterized_table_ref='{}'", graph_rel.alias, rel_table);

                    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                        RelationshipColumns {
                            from_id: "from_node_id".to_string(),
                            to_id: "to_node_id".to_string(),
                        },
                    );

                    // Get left side ID column from the FROM table
                    let left_id_col = extract_id_column(&graph_rel.left).ok_or_else(|| {
                        RenderBuildError::InvalidRenderPlan(format!(
                            "Cannot determine ID column for left node '{}' in relationship '{}'. \
                             Node schema must define id_column in YAML, or node might have invalid plan structure.",
                            graph_rel.left_connection, graph_rel.alias
                        ))
                    })?;

                    // Determine join condition based on direction
                    let is_optional = graph_rel.is_optional.unwrap_or(false);
                    let join_type = if is_optional {
                        JoinType::Left
                    } else {
                        JoinType::Inner
                    };

                    // For relationship joins, the columns are determined by the edge definition:
                    // - from_id connects to the SOURCE node (where edge originates)
                    // - to_id connects to the TARGET node (where edge points)
                    //
                    // Due to how left_connection/right_connection are computed in match_clause.rs:
                    // - Outgoing (a)-[r]->(b): left_conn=a, right_conn=b -> a is source, b is target
                    // - Incoming (a)<-[r]-(b): left_conn=b, right_conn=a -> b is source, a is target
                    //
                    // In both cases: left_connection is the SOURCE, right_connection is the TARGET
                    // So we always use: left_conn.id = rel.from_id, right_conn.id = rel.to_id
                    let rel_col_start = &rel_cols.from_id; // for left_connection (SOURCE)
                    let rel_col_end = &rel_cols.to_id; // for right_connection (TARGET)

                    // JOIN 1: Relationship table -> FROM (left) node
                    joins.push(Join {
                        table_name: rel_table,
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: PropertyValue::Column(rel_col_start.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: PropertyValue::Column(left_id_col),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                        pre_filter: None,
                        from_id_column: Some(rel_col_start.clone()), // Preserve for NULL checks
                        to_id_column: Some(rel_col_end.clone()),     // Preserve for NULL checks
                    });

                    // JOIN 2: CTE (right node) -> Relationship table
                    // Get the CTE table name from the GraphJoins input
                    if let LogicalPlan::GraphNode(gn) = right_joins.input.as_ref() {
                        if let Some(cte_table) = extract_table_name(&gn.input) {
                            // Get the right node's ID column
                            let right_id_col = extract_id_column(&right_joins.input).ok_or_else(|| {
                                RenderBuildError::InvalidRenderPlan(format!(
                                    "Cannot determine ID column for right node '{}' in relationship '{}'. \
                                     Node schema must define id_column in YAML, or node might have invalid plan structure.",
                                    graph_rel.right_connection, graph_rel.alias
                                ))
                            })?;

                            joins.push(Join {
                                table_name: cte_table,
                                table_alias: graph_rel.right_connection.clone(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(
                                                graph_rel.right_connection.clone(),
                                            ),
                                            column: PropertyValue::Column(right_id_col),
                                        }),
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(graph_rel.alias.clone()),
                                            column: PropertyValue::Column(rel_col_end.clone()),
                                        }),
                                    ],
                                }],
                                join_type,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        }
                    }

                    // Skip the pre-computed joins from GraphJoins - they have stale conditions
                    // We've generated fresh joins above with correct conditions
                    return Ok(joins);
                }

                // First, check if the plan_ctx marks this relationship as optional
                // This is set by OPTIONAL MATCH clause processing
                let is_optional = graph_rel.is_optional.unwrap_or(false);
                let join_type = if is_optional {
                    JoinType::Left
                } else {
                    JoinType::Inner
                };

                // Extract table names and columns
                // IMPORTANT: For CTE references, use the source_table directly from ViewScan
                // because CTEs don't have labels in the schema

                /// Get table name for START node (left side of GraphRel)
                /// Uses standard extract_table_name which returns relationship table for GraphRel
                fn get_start_table_name_or_cte(
                    plan: &LogicalPlan,
                ) -> Result<String, RenderBuildError> {
                    // First, try to get source_table directly from ViewScan (handles CTE references)
                    if let Some(table_name) = extract_table_name(plan) {
                        // Check if this looks like a CTE (starts with "with_")
                        if table_name.starts_with("with_") {
                            return Ok(table_name);
                        }
                    }
                    // Extract table name from ViewScan - no fallback
                    extract_table_name(plan).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "start node table in extract_joins".to_string(),
                        )
                    })
                }

                /// Get table name for END node (right side of GraphRel)
                /// CRITICAL: For nested GraphRel patterns (multi-hop), uses extract_end_node_table_name
                /// which correctly traverses to the rightmost node instead of returning the relationship table
                fn get_end_table_name_or_cte(
                    plan: &LogicalPlan,
                ) -> Result<String, RenderBuildError> {
                    // First, try to get source_table directly from ViewScan (handles CTE references)
                    if let Some(table_name) = extract_end_node_table_name(plan) {
                        // Check if this looks like a CTE (starts with "with_")
                        if table_name.starts_with("with_") {
                            return Ok(table_name);
                        }
                    }
                    // Extract END NODE table name - handles nested GraphRel correctly
                    extract_end_node_table_name(plan).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "end node table in extract_joins".to_string(),
                        )
                    })
                }

                // Helper function to get table name from relationship schema
                // Used when target node doesn't have a specific label (e.g., in multi-relationship queries)
                fn get_table_from_rel_schema(
                    labels: &Option<Vec<String>>,
                    is_from_node: bool,
                ) -> Option<String> {
                    if let Some(label_list) = labels {
                        if !label_list.is_empty() {
                            // Use the first relationship type to get the table name
                            // For multi-relationship queries, all relationships should connect to the same table
                            // (or the query should use denormalized edges)
                            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                if let Ok(schemas) = schemas_lock.try_read() {
                                    // Try "default" schema first, then empty string
                                    for schema_name in ["default", ""] {
                                        if let Some(schema) = schemas.get(schema_name) {
                                            if let Ok(rel_schema) =
                                                schema.get_rel_schema(&label_list[0])
                                            {
                                                let table_name = if is_from_node {
                                                    &rel_schema.from_node_table
                                                } else {
                                                    &rel_schema.to_node_table
                                                };
                                                return Some(format!(
                                                    "{}.{}",
                                                    rel_schema.database, table_name
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None
                }

                let start_table = get_start_table_name_or_cte(&graph_rel.left).or_else(|_| {
                    // Fallback: try to get from relationship schema
                    get_table_from_rel_schema(&graph_rel.labels, true).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "start node table in extract_joins".to_string(),
                        )
                    })
                })?;

                // CRITICAL FIX: Use get_end_table_name_or_cte for the right side
                // This correctly handles nested GraphRel patterns (multi-hop traversals)
                // where graph_rel.right is itself a GraphRel, not a simple GraphNode
                let end_table = get_end_table_name_or_cte(&graph_rel.right).or_else(|_| {
                    // Fallback: try to get from relationship schema
                    get_table_from_rel_schema(&graph_rel.labels, false).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "end node table in extract_joins".to_string(),
                        )
                    })
                })?;

                // Also extract labels for schema filter generation (optional for CTEs)
                let start_label = extract_node_label_from_viewscan(&graph_rel.left);
                let end_label = extract_node_label_from_viewscan(&graph_rel.right);

                // Get relationship table with parameterized view syntax if applicable
                // POLYMORPHIC FIX: Always use extract from ViewScan source_table
                // which contains the correctly resolved polymorphic table name (e.g., "ldbc.Person_likes_Message")
                // NOT rel_type_to_table_name which doesn't know about node labels
                let rel_table = if matches!(&*graph_rel.center, LogicalPlan::Cte(_)) {
                    // CTEs don't have parameterized views
                    extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
                } else {
                    // Use extract_parameterized_table_ref for ViewScan (handles parameterized views)
                    extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone())
                };

                println!(
                    "DEBUG: GraphRel extract_joins - rel_table='{}' for alias='{}'",
                    rel_table, graph_rel.alias
                );

                // MULTI-HOP FIX: For ID columns, use table lookup based on connection aliases
                // instead of extract_id_column which fails for nested GraphRel
                // The left_connection tells us which node alias we're connecting from
                let start_id_col = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    // Multi-hop: left side is another GraphRel, so left_connection points to intermediate node
                    // Look up the node's table and get its ID column
                    println!(
                        "DEBUG: Multi-hop - left_connection={}, using table lookup for ID column",
                        graph_rel.left_connection
                    );
                    table_to_id_column(&start_table)
                } else {
                    // Single hop: extract ID column from the node ViewScan
                    extract_id_column(&graph_rel.left)
                        .unwrap_or_else(|| table_to_id_column(&start_table))
                };
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));

                // Get relationship columns
                let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                    RelationshipColumns {
                        from_id: "from_node_id".to_string(),
                        to_id: "to_node_id".to_string(),
                    },
                );

                // JOIN ORDER: For standard patterns like (a)-[:R]->(b), we join:
                // 1. Relationship table (can reference anchor `a` from FROM clause)
                // 2. End node `b` (can reference relationship)
                //
                // The `is_optional` flag determines JOIN TYPE (LEFT vs INNER), not order.
                // The FROM clause is always the left/anchor node, so normal order works.

                // For LEFT JOINs, we need to extract:
                // 1. Schema filters from YAML config (ViewScan.schema_filter)
                // 2. User WHERE predicates that reference ONLY optional aliases
                // Both go into pre_filter (subquery form) for correct LEFT JOIN semantics
                //
                // IMPORTANT: In OPTIONAL MATCH (a)-[r]->(b):
                // - left_connection (a) is the REQUIRED anchor - do NOT extract its predicates!
                // - alias (r) is optional - extract its predicates
                // - right_connection (b) is optional - extract its predicates

                // Extract user predicates ONLY for optional aliases (rel and right)
                // DO NOT extract for left_connection - it's the required anchor!
                let (rel_user_pred, remaining_after_rel) = if is_optional {
                    extract_predicates_for_alias_logical(
                        &graph_rel.where_predicate,
                        &graph_rel.alias,
                    )
                } else {
                    (None, graph_rel.where_predicate.clone())
                };

                let (right_user_pred, _remaining) = if is_optional {
                    extract_predicates_for_alias_logical(
                        &remaining_after_rel,
                        &graph_rel.right_connection,
                    )
                } else {
                    (None, remaining_after_rel)
                };

                // Get schema filters from YAML config
                // Note: left_connection is the anchor node, but it might still have a schema filter
                let left_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.left, &graph_rel.left_connection)
                } else {
                    None
                };
                let rel_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.center, &graph_rel.alias)
                } else {
                    None
                };
                let right_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.right, &graph_rel.right_connection)
                } else {
                    None
                };

                // Generate polymorphic edge filter (type_column IN ('TYPE1', 'TYPE2') AND from_label = 'X' AND to_label = 'Y')
                // This applies regardless of whether the JOIN is optional or required
                let rel_types_for_filter: Vec<String> = graph_rel
                    .labels
                    .as_ref()
                    .map(|labels| labels.clone())
                    .unwrap_or_default();
                let polymorphic_filter = get_polymorphic_edge_filter_for_join(
                    &graph_rel.center,
                    &graph_rel.alias,
                    &rel_types_for_filter,
                    &start_label,
                    &end_label,
                );

                // Combine schema filter + user predicates for each alias's pre_filter
                // Note: left_connection is anchor, so we only use schema filter (no user predicate extraction)
                // Using combine_optional_filters_with_and from plan_builder_helpers

                // left_node uses ONLY schema filter (no user predicates - anchor node predicates stay in WHERE)
                let _left_node_pre_filter = left_schema_filter;
                // Relationship pre_filter combines: schema filter + polymorphic filter + user predicates
                let rel_pre_filter = combine_optional_filters_with_and(vec![
                    rel_schema_filter,
                    polymorphic_filter,
                    rel_user_pred,
                ]);
                let right_node_pre_filter =
                    combine_optional_filters_with_and(vec![right_schema_filter, right_user_pred]);

                // Standard join order: relationship first, then end node
                // The FROM clause is always the left/anchor node.

                // DEBUG: Log CTE references at start of extract_joins
                log::info!("🔍 extract_joins: left_connection='{}', right_connection='{}', cte_references={:?}",
                           graph_rel.left_connection, graph_rel.right_connection, graph_rel.cte_references);

                // Helper: Resolve table alias and column for CTE references
                // When a node connection (e.g., "b") references a CTE, we need to use
                // the CTE alias (e.g., "a_b") instead of the node alias ("b")
                let resolve_cte_reference = |node_alias: &str, column: &str| -> (String, String) {
                    if let Some(cte_name) = graph_rel.cte_references.get(node_alias) {
                        // Calculate CTE alias: "with_a_b_cte_1" -> "a_b"
                        // Strategy: strip "with_" prefix, then strip "_cte" or "_cte_N" suffix
                        let after_prefix = cte_name.strip_prefix("with_").unwrap_or(cte_name);
                        let cte_alias = after_prefix
                            .strip_suffix("_cte")
                            .or_else(|| after_prefix.strip_suffix("_cte_1"))
                            .or_else(|| after_prefix.strip_suffix("_cte_2"))
                            .or_else(|| after_prefix.strip_suffix("_cte_3"))
                            .unwrap_or(after_prefix);

                        // Column name in CTE: node_alias_column (e.g., "b_user_id")
                        let cte_column = format!("{}_{}", node_alias, column);

                        log::info!(
                            "🔧 Resolved CTE reference: {} -> CTE '{}' (alias '{}'), column '{}'",
                            node_alias,
                            cte_name,
                            cte_alias,
                            cte_column
                        );

                        (cte_alias.to_string(), cte_column)
                    } else {
                        // Not a CTE reference, use as-is
                        (node_alias.to_string(), column.to_string())
                    }
                };

                // Import Direction for bidirectional pattern support
                use crate::query_planner::logical_expr::Direction;

                // Determine if this is an undirected pattern (Direction::Either)
                let is_bidirectional = graph_rel.direction == Direction::Either;

                // JOIN 1: Start node -> Relationship table
                //   For outgoing: r.from_id = a.user_id
                //   For incoming: r.to_id = a.user_id
                //   For either: (r.from_id = a.user_id) OR (r.to_id = a.user_id)
                let rel_join_condition = if is_bidirectional {
                    // Bidirectional: create OR condition for both directions
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let outgoing_cond = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_cols.from_id.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias.clone()),
                                column: PropertyValue::Column(left_column.clone()),
                            }),
                        ],
                    };
                    let incoming_cond = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_cols.to_id.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias.clone()),
                                column: PropertyValue::Column(left_column.clone()),
                            }),
                        ],
                    };
                    OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(outgoing_cond),
                            RenderExpr::OperatorApplicationExp(incoming_cond),
                        ],
                    }
                } else {
                    // Directional: left is always source (from), right is always target (to)
                    // The GraphRel representation normalizes this - direction only affects
                    // how nodes are assigned to left/right during parsing.
                    // JOIN 1: relationship.from_id = left_node.id
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let rel_col = &rel_cols.from_id;
                    OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias),
                                column: PropertyValue::Column(left_column),
                            }),
                        ],
                    }
                };

                println!(
                    "🔧 DEBUG: About to push JOIN 1 (relationship): {} AS {}",
                    rel_table, graph_rel.alias
                );

                // Compile edge constraints if present
                // Look up relationship schema and check for constraints field
                let mut combined_pre_filter = rel_pre_filter.clone();

                log::info!(
                    "🔍 Edge constraint check: is_bidirectional={}",
                    is_bidirectional
                );

                if !is_bidirectional {
                    // Only compile constraints for directional edges (bidirectional is complex OR condition)
                    if let Some(schema_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                        log::info!("🔍 GLOBAL_SCHEMAS lock acquired");
                        if let Ok(schemas) = schema_lock.try_read() {
                            log::info!(
                                "🔍 Schemas read lock acquired, available schemas: {:?}",
                                schemas.keys().collect::<Vec<_>>()
                            );
                            // Try to find schema - check all available schemas
                            // Priority: "default", then empty string, then any other schema
                            let mut schema_to_use = None;
                            if let Some(schema) = schemas.get("default") {
                                schema_to_use = Some(("default", schema));
                            } else if let Some(schema) = schemas.get("") {
                                schema_to_use = Some(("", schema));
                            } else if let Some((name, schema)) = schemas.iter().next() {
                                // Use first available schema if no default
                                schema_to_use = Some((name.as_str(), schema));
                            }

                            if let Some((schema_name, schema)) = schema_to_use {
                                log::info!("🔍 Using schema: {}", schema_name);
                                // Get the first relationship type (for multi-type like [:TYPE1|TYPE2], constraints not supported)
                                if let Some(labels_vec) = &graph_rel.labels {
                                    log::info!("🔍 Relationship labels: {:?}", labels_vec);
                                    if let Some(rel_type) = labels_vec.first() {
                                        log::info!("🔍 Looking up relationship type: {}", rel_type);
                                        // Look up relationship schema by type
                                        if let Some(rel_schema) =
                                            schema.get_relationships_schema_opt(rel_type)
                                        {
                                            log::info!("🔍 Found relationship schema for {}, constraints={:?}", rel_type, rel_schema.constraints);
                                            // Check if constraints are defined
                                            if let Some(ref constraint_expr) =
                                                rel_schema.constraints
                                            {
                                                log::info!(
                                                    "🔍 Found constraint expression: {}",
                                                    constraint_expr
                                                );
                                                // Get node schemas for from/to nodes
                                                log::info!(
                                                    "🔍 Node labels: start={:?}, end={:?}",
                                                    start_label,
                                                    end_label
                                                );
                                                if let (Some(start_label), Some(end_label)) =
                                                    (&start_label, &end_label)
                                                {
                                                    log::info!("🔍 Looking up node schemas: start={}, end={}", start_label, end_label);
                                                    if let (
                                                        Some(from_node_schema),
                                                        Some(to_node_schema),
                                                    ) = (
                                                        schema.get_node_schema_opt(start_label),
                                                        schema.get_node_schema_opt(end_label),
                                                    ) {
                                                        log::info!("🔍 Found both node schemas, compiling constraint...");
                                                        // Compile the constraint expression
                                                        match crate::graph_catalog::constraint_compiler::compile_constraint(
                                                                constraint_expr,
                                                                from_node_schema,
                                                                to_node_schema,
                                                                &graph_rel.left_connection,
                                                                &graph_rel.right_connection,
                                                            ) {
                                                                Ok(compiled_sql) => {
                                                                    log::info!(
                                                                        "✅ Compiled edge constraint for {} (schema={}): {} → {}",
                                                                        graph_rel.alias, schema_name, constraint_expr, compiled_sql
                                                                    );
                                                                    // Add compiled constraint to pre_filter (will be added to ON clause)
                                                                    let constraint_render_expr = RenderExpr::Raw(compiled_sql);
                                                                    combined_pre_filter = if let Some(existing) = combined_pre_filter {
                                                                        // Combine with existing pre_filter using AND
                                                                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                                                            operator: Operator::And,
                                                                            operands: vec![existing, constraint_render_expr],
                                                                        }))
                                                                    } else {
                                                                        Some(constraint_render_expr)
                                                                    };
                                                                }
                                                                Err(e) => {
                                                                    log::warn!(
                                                                        "⚠️  Failed to compile edge constraint for {} (schema={}): {}",
                                                                        graph_rel.alias, schema_name, e
                                                                    );
                                                                }
                                                            }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                joins.push(Join {
                    table_name: rel_table.clone(),
                    table_alias: graph_rel.alias.clone(),
                    joining_on: vec![rel_join_condition],
                    join_type: join_type.clone(),
                    pre_filter: combined_pre_filter,
                    from_id_column: Some(rel_cols.from_id.clone()),
                    to_id_column: Some(rel_cols.to_id.clone()),
                });

                // CRITICAL FIX: Handle nested GraphRel patterns differently
                // In nested multi-hop patterns like (post)<-[:HAS_CREATOR]-(f)-[:KNOWS]-(p):
                // - The outer GraphRel (HAS_CREATOR) has right = inner GraphRel (KNOWS)
                // - We need to add a JOIN for the SHARED node (f) connecting to outer rel (t2)
                // - The inner pattern JOINs were already added earlier in this function
                // - Then we skip the normal "JOIN 2" code which would try to add a duplicate
                let right_is_nested_graph_rel =
                    matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_));

                if right_is_nested_graph_rel {
                    println!(
                        "🔍 DEBUG: Nested GraphRel detected for {} - adding shared node JOIN",
                        graph_rel.alias
                    );

                    // The shared node (right_connection, e.g., 'f') needs to be JOINed to OUTER rel (t2)
                    // According to GraphRel convention: right_connection connects to to_id
                    // So: f.id = t2.PersonId (to_id)

                    // Get table info for the shared node
                    // The shared node is embedded inside the inner GraphRel
                    // For inner pattern (p)-[:KNOWS]-(f), if f is right_connection of inner,
                    // then f's table info is in inner_rel.right
                    if let LogicalPlan::GraphRel(inner_rel) = graph_rel.right.as_ref() {
                        let shared_alias = &graph_rel.right_connection;

                        // Determine which side of inner pattern has the shared node
                        let shared_is_inner_right = &inner_rel.right_connection == shared_alias;

                        let (shared_table, shared_id_col) = if shared_is_inner_right {
                            (
                                extract_end_node_table_name(&inner_rel.right),
                                extract_end_node_id_column(&inner_rel.right)
                                    .unwrap_or_else(|| "id".to_string()),
                            )
                        } else {
                            (
                                extract_table_name(&inner_rel.left),
                                extract_id_column(&inner_rel.left)
                                    .unwrap_or_else(|| "id".to_string()),
                            )
                        };

                        if let Some(table_name) = shared_table {
                            // Create JOIN for shared node: f.id = t2.to_id
                            let shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(shared_alias.clone()),
                                        column: PropertyValue::Column(shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name,
                                table_alias: shared_alias.clone(),
                                joining_on: vec![shared_join_condition],
                                join_type: join_type.clone(),
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });

                            println!(
                                "  ✅ Added JOIN for shared node '{}' connecting to outer rel '{}'",
                                shared_alias, graph_rel.alias
                            );
                        }
                    }

                    println!(
                        "📤 DEBUG: GraphRel (alias={}) returning {} total joins (nested pattern)",
                        graph_rel.alias,
                        joins.len()
                    );
                    return Ok(joins);
                }

                // JOIN 2: Relationship table -> End node
                //   For outgoing: b.user_id = r.to_id
                //   For incoming: b.user_id = r.from_id
                //   For either: (b.user_id = r.to_id AND r.from_id = a.user_id) OR (b.user_id = r.from_id AND r.to_id = a.user_id)
                //   Simplified for bidirectional: b.user_id = CASE WHEN r.from_id = a.user_id THEN r.to_id ELSE r.from_id END
                //   Actually simpler: just check b connects to whichever end of r that's NOT a
                let end_join_condition = if is_bidirectional {
                    // For bidirectional, the end node connects to whichever side of r that ISN'T the start node
                    // This is expressed as: (b.id = r.to_id AND r.from_id = a.id) OR (b.id = r.from_id AND r.to_id = a.id)
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let (right_table_alias, right_column) =
                        resolve_cte_reference(&graph_rel.right_connection, &end_id_col);
                    let outgoing_side = OperatorApplication {
                        operator: Operator::And,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_table_alias.clone()),
                                        column: PropertyValue::Column(right_column.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                ],
                            }),
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_table_alias.clone()),
                                        column: PropertyValue::Column(left_column.clone()),
                                    }),
                                ],
                            }),
                        ],
                    };
                    let incoming_side = OperatorApplication {
                        operator: Operator::And,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_table_alias.clone()),
                                        column: PropertyValue::Column(right_column.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                ],
                            }),
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_table_alias.clone()),
                                        column: PropertyValue::Column(left_column.clone()),
                                    }),
                                ],
                            }),
                        ],
                    };
                    OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(outgoing_side),
                            RenderExpr::OperatorApplicationExp(incoming_side),
                        ],
                    }
                } else {
                    // Directional: right is always target (to)
                    // JOIN 2: right_node.id = relationship.to_id
                    let (right_table_alias, right_column) =
                        resolve_cte_reference(&graph_rel.right_connection, &end_id_col);
                    let rel_col = &rel_cols.to_id;
                    OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_table_alias),
                                column: PropertyValue::Column(right_column),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_col.clone()),
                            }),
                        ],
                    }
                };

                println!(
                    "🔧 DEBUG: About to push JOIN 2 (end node): {} AS {}",
                    end_table, graph_rel.right_connection
                );
                joins.push(Join {
                    table_name: end_table,
                    table_alias: graph_rel.right_connection.clone(),
                    joining_on: vec![end_join_condition],
                    join_type,
                    pre_filter: right_node_pre_filter.clone(),
                    from_id_column: None,
                    to_id_column: None,
                });

                println!(
                    "📤 DEBUG: GraphRel (alias={}) returning {} total joins",
                    graph_rel.alias,
                    joins.len()
                );
                joins
            }
            LogicalPlan::CartesianProduct(cp) => {
                // For CartesianProduct, generate JOIN with ON clause if join_condition exists
                // or CROSS JOIN semantics if no join_condition
                let mut joins = cp.left.extract_joins(schema)?;

                // Check if right side is a GraphRel - OPTIONAL MATCH case needs special handling
                if let LogicalPlan::GraphRel(graph_rel) = cp.right.as_ref() {
                    // OPTIONAL MATCH with GraphRel pattern
                    // Need to determine which connection is the anchor (already defined in cp.left)
                    // and generate joins in the correct order

                    // Get the anchor alias from cp.left (the base pattern)
                    let anchor_alias = get_anchor_alias_from_plan(&cp.left);
                    crate::debug_print!(
                        "CartesianProduct with GraphRel: anchor_alias={:?}",
                        anchor_alias
                    );
                    crate::debug_print!(
                        "  left_connection={}, right_connection={}",
                        graph_rel.left_connection,
                        graph_rel.right_connection
                    );

                    // Determine if anchor is on left or right
                    let anchor_is_right = anchor_alias
                        .as_ref()
                        .map(|a| a == &graph_rel.right_connection)
                        .unwrap_or(false);

                    if cp.is_optional && anchor_is_right {
                        // OPTIONAL MATCH where anchor is on right side
                        // e.g., MATCH (post:Post) OPTIONAL MATCH (liker:Person)-[:LIKES]->(post)
                        // Anchor is 'post' (right_connection), new node is 'liker' (left_connection)
                        crate::debug_print!("  -> Anchor is on RIGHT, generating swapped joins");

                        let swapped_joins = generate_swapped_joins_for_optional_match(graph_rel)?;
                        joins.extend(swapped_joins);
                    } else {
                        // Normal case: anchor is on left, or non-optional
                        // Use standard extract_joins
                        joins.extend(cp.right.extract_joins(schema)?);
                    }
                } else {
                    // Non-GraphRel right side (e.g., simple node patterns)
                    // Get the right side's FROM table to create a JOIN
                    if let Some(right_from) = cp.right.extract_from()? {
                        let join_type = if cp.is_optional {
                            JoinType::Left
                        } else {
                            JoinType::Inner
                        };

                        if let Some(right_table) = right_from.table {
                            // Convert join_condition to OperatorApplication for the ON clause
                            let joining_on = if let Some(ref join_cond) = cp.join_condition {
                                // Convert LogicalExpr to RenderExpr, then extract OperatorApplication
                                let render_expr: Result<RenderExpr, _> =
                                    join_cond.clone().try_into();
                                match render_expr {
                                    Ok(RenderExpr::OperatorApplicationExp(op)) => vec![op],
                                    Ok(_other) => {
                                        // Wrap non-operator expressions in equality check
                                        crate::debug_print!("CartesianProduct: join_condition is not OperatorApplication: {:?}", _other);
                                        vec![]
                                    }
                                    Err(_e) => {
                                        crate::debug_print!("CartesianProduct: Failed to convert join_condition: {:?}", _e);
                                        vec![]
                                    }
                                }
                            } else {
                                vec![] // No join condition - pure CROSS JOIN semantics
                            };

                            crate::debug_print!("CartesianProduct extract_joins: table={}, alias={}, joining_on={:?}",
                                right_table.name, right_table.alias.as_deref().unwrap_or(""), joining_on);

                            joins.push(super::Join {
                                table_name: right_table.name.clone(),
                                table_alias: right_table.alias.clone().unwrap_or_default(),
                                joining_on,
                                join_type,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        }
                    }

                    // Include any joins from the right side
                    joins.extend(cp.right.extract_joins(schema)?);
                }

                joins
            }
            _ => vec![],
        };
        Ok(joins)
    }

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
        use crate::graph_catalog::expression_parser::PropertyValue;

        log::info!(
            "🔧 GROUP BY: extract_group_by() called for plan type {:?}",
            std::mem::discriminant(self)
        );

        /// Helper to find node properties when the alias is a relationship alias with "*" column.
        /// For denormalized schemas, the node alias gets remapped to the relationship alias,
        /// so we need to look up which node this represents and get its properties.
        fn find_node_properties_for_rel_alias(
            plan: &LogicalPlan,
            rel_alias: &str,
        ) -> Option<(Vec<(String, String)>, String)> {
            match plan {
                LogicalPlan::GraphRel(rel) if rel.alias == rel_alias => {
                    // This relationship matches - get the left node's properties (most common case)
                    // Left node is typically the one being grouped in WITH clause
                    if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                        // Check direction to determine which properties to use
                        let is_incoming = rel.direction == Direction::Incoming;
                        let props = if is_incoming {
                            &scan.to_node_properties
                        } else {
                            &scan.from_node_properties
                        };

                        if let Some(node_props) = props {
                            let properties: Vec<(String, String)> = node_props
                                .iter()
                                .map(|(prop_name, prop_value)| {
                                    (prop_name.clone(), prop_value.raw().to_string())
                                })
                                .collect();
                            if !properties.is_empty() {
                                // Return properties and the actual table alias to use
                                return Some((properties, rel.alias.clone()));
                            }
                        }
                    }
                    None
                }
                LogicalPlan::GraphRel(rel) => {
                    // Not this relationship - search children
                    if let Some(result) = find_node_properties_for_rel_alias(&rel.left, rel_alias) {
                        return Some(result);
                    }
                    if let Some(result) = find_node_properties_for_rel_alias(&rel.center, rel_alias)
                    {
                        return Some(result);
                    }
                    find_node_properties_for_rel_alias(&rel.right, rel_alias)
                }
                LogicalPlan::Projection(proj) => {
                    find_node_properties_for_rel_alias(&proj.input, rel_alias)
                }
                LogicalPlan::Filter(filter) => {
                    find_node_properties_for_rel_alias(&filter.input, rel_alias)
                }
                LogicalPlan::GroupBy(gb) => {
                    find_node_properties_for_rel_alias(&gb.input, rel_alias)
                }
                LogicalPlan::GraphJoins(joins) => {
                    find_node_properties_for_rel_alias(&joins.input, rel_alias)
                }
                LogicalPlan::OrderBy(order) => {
                    find_node_properties_for_rel_alias(&order.input, rel_alias)
                }
                LogicalPlan::Skip(skip) => {
                    find_node_properties_for_rel_alias(&skip.input, rel_alias)
                }
                LogicalPlan::Limit(limit) => {
                    find_node_properties_for_rel_alias(&limit.input, rel_alias)
                }
                _ => None,
            }
        }

        let group_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_group_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_group_by()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_group_by()?,
            LogicalPlan::Projection(projection) => projection.input.extract_group_by()?,
            LogicalPlan::Filter(filter) => filter.input.extract_group_by()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_group_by()?,
            LogicalPlan::GraphNode(node) => node.input.extract_group_by()?,
            LogicalPlan::GraphRel(rel) => {
                // For relationships, try left first, then center, then right
                rel.left
                    .extract_group_by()
                    .or_else(|_| rel.center.extract_group_by())
                    .or_else(|_| rel.right.extract_group_by())?
            }
            LogicalPlan::GroupBy(group_by) => {
                log::info!(
                    "🔧 GROUP BY: Found GroupBy plan, processing {} expressions",
                    group_by.expressions.len()
                );
                let mut result: Vec<RenderExpr> = vec![];

                // Track which aliases we've already added to GROUP BY
                // This is used for the optimization: GROUP BY only the ID column
                let mut seen_group_by_aliases: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                for expr in &group_by.expressions {
                    // Check if this is a TableAlias that needs expansion
                    if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) = expr
                    {
                        // OPTIMIZATION: For node aliases in GROUP BY, we only need the ID column.
                        // All other columns are functionally dependent on the ID.
                        // This reduces GROUP BY from 8+ columns to just 1, improving performance.
                        if let Ok((properties, actual_table_alias)) =
                            group_by.input.get_properties_with_table_alias(&alias.0)
                        {
                            if !properties.is_empty() {
                                let table_alias_to_use =
                                    actual_table_alias.unwrap_or_else(|| alias.0.clone());

                                // Skip if we've already added this alias (avoid duplicates)
                                if seen_group_by_aliases.contains(&table_alias_to_use) {
                                    continue;
                                }
                                seen_group_by_aliases.insert(table_alias_to_use.clone());

                                // Get the ID column from the schema (via ViewScan.id_column)
                                // This is the proper way - use schema definition, not pattern matching
                                let id_col = group_by.input.find_id_column_for_alias(&alias.0)
                                    .unwrap_or_else(|_| {
                                        log::warn!("⚠️ Could not find ID column for alias '{}', using fallback", alias.0);
                                        "id".to_string()
                                    });

                                log::debug!("🔧 GROUP BY optimization: Using ID column '{}' from schema instead of {} properties for alias '{}'",
                                    id_col, properties.len(), table_alias_to_use);

                                result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(table_alias_to_use.clone()),
                                    column: PropertyValue::Column(id_col),
                                }));
                                continue;
                            }
                        }
                    }

                    // Check if this is a PropertyAccessExp with wildcard column "*"
                    // This happens when ProjectionTagging converts TableAlias to PropertyAccessExp(alias.*)
                    if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                        prop_access,
                    ) = expr
                    {
                        if prop_access.column.raw() == "*" {
                            // OPTIMIZATION: For node alias wildcards in GROUP BY, we only need the ID column.
                            // All other columns are functionally dependent on the ID.
                            if let Ok((properties, actual_table_alias)) = group_by
                                .input
                                .get_properties_with_table_alias(&prop_access.table_alias.0)
                            {
                                let table_alias_to_use = actual_table_alias
                                    .unwrap_or_else(|| prop_access.table_alias.0.clone());

                                // Skip if we've already added this alias (avoid duplicates)
                                if seen_group_by_aliases.contains(&table_alias_to_use) {
                                    continue;
                                }
                                seen_group_by_aliases.insert(table_alias_to_use.clone());

                                // Better approach: try to find node properties for this rel alias
                                if let Some((node_props, table_alias)) =
                                    find_node_properties_for_rel_alias(
                                        &group_by.input,
                                        &prop_access.table_alias.0,
                                    )
                                {
                                    // Found denormalized node properties - get ID from schema (MUST succeed)
                                    let id_col = group_by.input.find_id_column_for_alias(&prop_access.table_alias.0)
                                        .map_err(|e| RenderBuildError::InvalidRenderPlan(
                                            format!("Cannot find ID column for denormalized alias '{}': {}", prop_access.table_alias.0, e)
                                        ))?;

                                    log::debug!("🔧 GROUP BY optimization: Using ID column '{}' from schema for denormalized alias '{}'",
                                        id_col, table_alias);

                                    result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(table_alias.clone()),
                                        column: PropertyValue::Column(id_col),
                                    }));
                                    continue;
                                }

                                // Fallback: use ID column from schema
                                if !properties.is_empty() {
                                    let id_col = group_by
                                        .input
                                        .find_id_column_for_alias(&prop_access.table_alias.0)
                                        .map_err(|e| {
                                            RenderBuildError::InvalidRenderPlan(format!(
                                                "Cannot find ID column for alias '{}': {}",
                                                prop_access.table_alias.0, e
                                            ))
                                        })?;

                                    log::debug!("🔧 GROUP BY optimization: Using ID column '{}' instead of {} properties for alias '{}'",
                                        id_col, properties.len(), table_alias_to_use);

                                    result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(table_alias_to_use.clone()),
                                        column: PropertyValue::Column(id_col),
                                    }));
                                    continue;
                                }
                            }
                        }
                    }

                    // Not a TableAlias/wildcard or couldn't expand - convert normally
                    let mut render_expr: RenderExpr = expr.clone().try_into()?;
                    apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                    result.push(render_expr);
                }

                result
            }
            _ => vec![],
        };
        Ok(group_by)
    }

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let having_clause = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_having()?,
            LogicalPlan::Skip(skip) => skip.input.extract_having()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_having()?,
            LogicalPlan::Projection(projection) => projection.input.extract_having()?,
            LogicalPlan::GroupBy(group_by) => {
                if let Some(having) = &group_by.having_clause {
                    let mut render_expr: RenderExpr = having.clone().try_into()?;
                    // Apply property mapping to the HAVING expression
                    apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                    Some(render_expr)
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(having_clause)
    }

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        let order_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_order_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_order_by()?,
            LogicalPlan::OrderBy(order_by) => order_by
                .items
                .iter()
                .cloned()
                .map(|item| {
                    let mut order_item: OrderByItem = item.try_into()?;
                    // Apply property mapping to the order by expression
                    apply_property_mapping_to_expr(&mut order_item.expression, &order_by.input);
                    Ok(order_item)
                })
                .collect::<Result<Vec<OrderByItem>, RenderBuildError>>()?,
            _ => vec![],
        };
        Ok(order_by)
    }

    fn extract_skip(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_skip(),
            LogicalPlan::Skip(skip) => Some(skip.count),
            _ => None,
        }
    }

    fn extract_limit(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => Some(limit.count),
            _ => None,
        }
    }

    fn extract_union(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Option<Union>> {
        let union_opt = match &self {
            LogicalPlan::Union(union) => {
                log::info!(
                    "🔍 extract_union: Processing Union with {} branches",
                    union.inputs.len()
                );

                let mut render_plans: Vec<RenderPlan> = union
                    .inputs
                    .iter()
                    .map(|input| input.to_render_plan(schema))
                    .collect::<Result<Vec<RenderPlan>, RenderBuildError>>()?;

                // CRITICAL FIX: Rewrite SELECT aliases for VLP Union branches
                // Each branch may reference VLP CTEs with internal aliases (start_node, end_node)
                // but SELECT items use Cypher aliases (a, b). We need to map them.
                log::info!(
                    "🔍 extract_union: Calling rewrite_vlp_union_branch_aliases for {} branches",
                    render_plans.len()
                );
                for (idx, plan) in render_plans.iter_mut().enumerate() {
                    log::info!("🔍 extract_union: Processing branch {}", idx);
                    rewrite_vlp_union_branch_aliases(plan)?;
                }

                Some(Union {
                    input: render_plans,
                    union_type: union.union_type.clone().try_into()?,
                })
            }
            _ => None,
        };
        Ok(union_opt)
    }

    /// Extract UNWIND clauses as ARRAY JOIN items
    /// Traverses the logical plan tree to find ALL Unwind nodes for cartesian product
    /// Multiple UNWIND clauses generate multiple ARRAY JOIN clauses in sequence
    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<super::ArrayJoin>> {
        let mut array_joins = Vec::new();

        match self {
            LogicalPlan::Unwind(u) => {
                // Convert LogicalExpr to RenderExpr for this UNWIND
                let render_expr = RenderExpr::try_from(u.expression.clone())?;
                array_joins.push(super::ArrayJoin {
                    expression: render_expr,
                    alias: u.alias.clone(),
                });
                // Recursively collect UNWIND nodes from input
                let mut inner_joins = u.input.extract_array_join()?;
                array_joins.append(&mut inner_joins);
                Ok(array_joins)
            }
            // Recursively check children for more UNWIND nodes
            LogicalPlan::Projection(p) => p.input.extract_array_join(),
            LogicalPlan::Filter(f) => f.input.extract_array_join(),
            LogicalPlan::GroupBy(g) => g.input.extract_array_join(),
            LogicalPlan::OrderBy(o) => o.input.extract_array_join(),
            LogicalPlan::Limit(l) => l.input.extract_array_join(),
            LogicalPlan::Skip(s) => s.input.extract_array_join(),
            LogicalPlan::GraphJoins(gj) => gj.input.extract_array_join(),
            LogicalPlan::GraphNode(gn) => gn.input.extract_array_join(),
            LogicalPlan::GraphRel(gr) => {
                // Check all branches for UNWIND nodes
                let mut joins = gr.center.extract_array_join()?;
                joins.append(&mut gr.left.extract_array_join()?);
                joins.append(&mut gr.right.extract_array_join()?);
                Ok(joins)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Try to build a JOIN-based render plan for simple queries
    /// Returns Ok(plan) if successful, Err(_) if this query needs CTE-based processing
    fn try_build_join_based_plan(
        &self,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        crate::debug_println!("DEBUG: try_build_join_based_plan called");
        crate::debug_println!("DEBUG: self plan type = {:?}", std::mem::discriminant(self));

        // Extract DISTINCT flag BEFORE unwrapping OrderBy/Limit/Skip
        let distinct = self.extract_distinct();
        crate::debug_println!(
            "DEBUG: try_build_join_based_plan - extracted distinct: {}",
            distinct
        );

        // First, extract ORDER BY/LIMIT/SKIP if present
        let (core_plan, order_by_items, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                crate::debug_println!("DEBUG: Found Limit node, checking input...");
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        crate::debug_println!(
                            "DEBUG: Limit input is OrderBy with {} items",
                            order_node.items.len()
                        );
                        (
                            order_node.input.as_ref(),
                            Some(&order_node.items),
                            Some(limit_node.count),
                            None,
                        )
                    }
                    other => {
                        crate::debug_println!(
                            "DEBUG: Limit input is NOT OrderBy: {:?}",
                            std::mem::discriminant(other)
                        );
                        (other, None, Some(limit_node.count), None)
                    }
                }
            }
            LogicalPlan::OrderBy(order_node) => (
                order_node.input.as_ref(),
                Some(&order_node.items),
                None,
                None,
            ),
            LogicalPlan::Skip(skip_node) => {
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => {
                crate::debug_println!(
                    "DEBUG: self is NOT Limit/OrderBy/Skip: {:?}",
                    std::mem::discriminant(other)
                );
                (other, None, None, None)
            }
        };

        crate::debug_println!(
            "DEBUG: order_by_items present = {}",
            order_by_items.is_some()
        );

        // Check core_plan for WITH+aggregation pattern
        // This catches cases where GroupBy is inside the core plan after unwrapping Limit/OrderBy
        if has_with_aggregation_pattern(core_plan) {
            println!("DEBUG: core_plan contains WITH aggregation + MATCH pattern - need CTE-based processing");
            return Err(RenderBuildError::InvalidRenderPlan(
                "WITH aggregation followed by MATCH requires CTE-based processing".to_string(),
            ));
        }

        // Check if the core plan contains a Union (denormalized node-only queries)
        // For Union, we need to build each branch separately and combine them
        // If branches have aggregation, we'll handle it specially (subquery + outer GROUP BY)
        if let Some(union) = find_nested_union(core_plan) {
            crate::debug_println!(
                "DEBUG: Found nested Union with {} inputs, building UNION ALL plan",
                union.inputs.len()
            );

            // ⚠️ CRITICAL FIX: Check if Union branches contain WITH clauses
            // If so, we need to bail out and let the top-level WITH handling deal with it
            // This prevents each branch from being processed independently and creating duplicate CTEs
            let branches_have_with = union
                .inputs
                .iter()
                .any(|input| has_with_clause_in_graph_rel(input));
            if branches_have_with {
                crate::debug_println!("DEBUG: Union branches contain WITH clauses - delegating to top-level WITH handling");
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Union branches contain WITH clauses - need top-level processing".to_string(),
                ));
            }

            // 🔧 FIX: Use the schema parameter instead of creating an empty schema
            // Creating an empty schema caused node lookups to fail in VLP queries

            // Build render plan for each Union branch
            // NOTE: Don't add LIMIT to branches - LIMIT applies to the combined UNION result
            let union_plans: Result<Vec<RenderPlan>, RenderBuildError> = union
                .inputs
                .iter()
                .map(|branch| branch.to_render_plan(schema))
                .collect();

            let mut union_plans = union_plans?;

            // 🔧 CRITICAL FIX: Rewrite SELECT aliases for VLP Union branches
            // Each branch may reference VLP CTEs with internal aliases (start_node, end_node)
            // but SELECT items use Cypher aliases (a, b). We need to map them.
            log::info!("🔍 try_build_join_based_plan: Calling rewrite_vlp_union_branch_aliases for {} branches", union_plans.len());
            for (idx, plan) in union_plans.iter_mut().enumerate() {
                log::info!("🔍 try_build_join_based_plan: Processing branch {}", idx);
                rewrite_vlp_union_branch_aliases(plan)?;
            }

            // Normalize UNION branches so all have the same columns
            // This handles denormalized nodes where from_node_properties and to_node_properties
            // might have different property sets - missing properties get NULL values
            let union_plans = normalize_union_branches(union_plans);

            // 🔧 FIX: Collect all CTEs from all branches and hoist to outer plan
            // This is critical for VLP with aggregation - each branch has its own recursive CTE
            // that needs to be available at the outer query level
            let all_branch_ctes: Vec<Cte> = union_plans
                .iter()
                .flat_map(|plan| plan.ctes.0.clone())
                .collect();

            crate::debug_println!(
                "DEBUG: Collected {} CTEs from union branches",
                all_branch_ctes.len()
            );

            // Check if the OUTER plan has GROUP BY or aggregation
            // This happens when return_clause.rs keeps aggregation at the outer level
            // We need to extract this info from core_plan (which wraps the Union)
            let outer_aggregation_info = extract_outer_aggregation_info(core_plan);

            crate::debug_println!(
                "DEBUG: outer_aggregation_info = {:?}",
                outer_aggregation_info.is_some()
            );

            if let Some((mut outer_select, mut outer_group_by)) = outer_aggregation_info {
                crate::debug_println!("DEBUG: Creating aggregation-aware UNION plan with {} outer SELECT items, {} GROUP BY",
                    outer_select.len(), outer_group_by.len());

                // 🔧 CRITICAL FIX: Rewrite outer SELECT and GROUP BY expressions to use CTE column names
                // The CTE exports columns like "friend_id", "friend_firstName" (underscore format)
                // but logical expressions reference "friend.id", "friend.firstName" (dot format)
                // We need to rewrite "friend.id" → "friend.friend_id" to match CTE columns
                log::info!("🔧 try_build_join_based_plan: Rewriting {} outer SELECT items for CTE column names", outer_select.len());
                for select_item in &mut outer_select {
                    log::debug!("🔧 Before rewrite: {:?}", select_item.expression);
                    rewrite_cte_column_references(&mut select_item.expression);
                    log::debug!("🔧 After rewrite: {:?}", select_item.expression);
                }

                log::info!("🔧 try_build_join_based_plan: Rewriting {} GROUP BY expressions for CTE column names", outer_group_by.len());
                for group_by_expr in &mut outer_group_by {
                    log::debug!("🔧 Before rewrite: {:?}", group_by_expr);
                    rewrite_cte_column_references(group_by_expr);
                    log::debug!("🔧 After rewrite: {:?}", group_by_expr);
                }

                // The union branches already have the correct base columns (no aggregation)
                // We just need to apply outer SELECT and GROUP BY on top

                // Convert ORDER BY for outer query
                let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items
                {
                    items
                        .iter()
                        .filter_map(|item| {
                            use crate::query_planner::logical_expr::LogicalExpr;
                            match &item.expression {
                                LogicalExpr::PropertyAccessExp(prop) => Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!(
                                        "\"{}.{}\"",
                                        prop.table_alias.0,
                                        prop.column.raw()
                                    )),
                                    order: item
                                        .order
                                        .clone()
                                        .try_into()
                                        .unwrap_or(OrderByOrder::Asc),
                                }),
                                LogicalExpr::ColumnAlias(alias) => Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    order: item
                                        .order
                                        .clone()
                                        .try_into()
                                        .unwrap_or(OrderByOrder::Asc),
                                }),
                                _ => None,
                            }
                        })
                        .collect()
                } else {
                    vec![]
                };

                return Ok(RenderPlan {
                    ctes: CteItems(all_branch_ctes.clone()),
                    select: SelectItems {
                        items: outer_select,
                        distinct: distinct,
                    },
                    from: FromTableItem(None),
                    joins: JoinItems(vec![]),
                    array_join: ArrayJoinItem(Vec::new()),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(outer_group_by),
                    having_clause: None,
                    order_by: OrderByItems(order_by_items_converted),
                    skip: SkipItem(skip_val),
                    limit: LimitItem(limit_val),
                    union: UnionItems(Some(Union {
                        input: union_plans,
                        union_type: union.union_type.clone().try_into()?,
                    })),
                });
            }

            // Also check if branches have GROUP BY with aggregation (legacy case where analyzers pushed it down)
            let branches_have_aggregation = union_plans.iter().any(|plan| {
                !plan.group_by.0.is_empty()
                    || plan
                        .select
                        .items
                        .iter()
                        .any(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
            });

            crate::debug_println!(
                "DEBUG: branches_have_aggregation = {}",
                branches_have_aggregation
            );

            if branches_have_aggregation {
                // Extract GROUP BY and aggregation from first branch (all branches should be similar)
                let first_plan = union_plans.first().ok_or_else(|| {
                    RenderBuildError::InvalidRenderPlan("Union has no inputs".to_string())
                })?;

                // Collect non-aggregate SELECT items (these become GROUP BY columns)
                let mut base_select_items: Vec<SelectItem> = first_plan
                    .select
                    .items
                    .iter()
                    .filter(|item| !matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                    .cloned()
                    .collect();

                // 🔧 CRITICAL FIX: Also collect aliases used in aggregate expressions
                // Example: COUNT(DISTINCT m) needs m.id in the SELECT list
                log::info!("🔧 VLP CTE Scoping Fix: Collecting aliases from aggregate expressions");
                let mut aggregate_aliases: Vec<String> = Vec::new();
                for item in &first_plan.select.items {
                    if let RenderExpr::AggregateFnCall(agg) = &item.expression {
                        collect_aliases_from_render_expr(&agg.args, &mut aggregate_aliases);
                    }
                }
                log::info!(
                    "🔧 Found {} aliases in aggregates: {:?}",
                    aggregate_aliases.len(),
                    aggregate_aliases
                );

                // For each alias in aggregates, add its ID column to base_select_items
                for alias in &aggregate_aliases {
                    // Skip if already included in base_select_items
                    let already_included = base_select_items.iter().any(|item| {
                        if let Some(col_alias) = &item.col_alias {
                            col_alias.0.starts_with(&format!("{}.", alias))
                        } else {
                            false
                        }
                    });

                    if !already_included {
                        // Find ID column for this alias from the plan
                        if let Ok(id_col) = core_plan.find_id_column_for_alias(alias) {
                            log::info!(
                                "🔧 Adding {}.{} to UNION SELECT for COUNT(DISTINCT {})",
                                alias,
                                id_col,
                                alias
                            );
                            base_select_items.push(SelectItem {
                                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(alias.clone()),
                                    column: PropertyValue::Column(id_col.clone()),
                                }),
                                col_alias: Some(ColumnAlias(format!("{}.{}", alias, id_col))),
                            });
                        }
                    }
                }

                // If there are no base columns but there are aggregates, use constant 1
                let _branch_select = if base_select_items.is_empty() {
                    SelectItems {
                        items: vec![SelectItem {
                            expression: RenderExpr::Literal(Literal::Integer(1)),
                            col_alias: Some(ColumnAlias("__dummy".to_string())),
                        }],
                        distinct: false,
                    }
                } else {
                    SelectItems {
                        items: base_select_items.clone(),
                        distinct: first_plan.select.distinct,
                    }
                };

                // Create stripped branch plans (no GROUP BY, no aggregation)
                let stripped_union_plans: Vec<RenderPlan> = union_plans
                    .iter()
                    .map(|plan| {
                        // Use base_select_items which now includes aggregate-referenced columns
                        let branch_items: Vec<SelectItem> = if base_select_items.is_empty() {
                            vec![SelectItem {
                                expression: RenderExpr::Literal(Literal::Integer(1)),
                                col_alias: Some(ColumnAlias("__dummy".to_string())),
                            }]
                        } else {
                            base_select_items.clone() // ✅ Use enhanced base_select_items
                        };

                        RenderPlan {
                            ctes: CteItems(vec![]),
                            select: SelectItems {
                                items: branch_items,
                                distinct: plan.select.distinct,
                            },
                            from: plan.from.clone(),
                            joins: plan.joins.clone(),
                            array_join: ArrayJoinItem(Vec::new()),
                            filters: plan.filters.clone(),
                            group_by: GroupByExpressions(vec![]), // No GROUP BY in branches
                            having_clause: None,
                            order_by: OrderByItems(vec![]),
                            skip: SkipItem(None),
                            limit: LimitItem(None),
                            union: UnionItems(None),
                        }
                    })
                    .collect();

                // Build outer GROUP BY expressions (use column aliases from SELECT)
                let outer_group_by: Vec<RenderExpr> = base_select_items
                    .iter()
                    .filter_map(|item| {
                        item.col_alias
                            .as_ref()
                            .map(|alias| RenderExpr::Raw(format!("\"{}\"", alias.0)))
                    })
                    .collect();

                // Build outer SELECT with aggregations referencing column aliases
                let outer_select_items: Vec<SelectItem> = first_plan
                    .select
                    .items
                    .iter()
                    .map(|item| {
                        // For non-aggregates, reference the column alias
                        // For aggregates, keep as-is (they'll reference subquery columns)
                        if matches!(&item.expression, RenderExpr::AggregateFnCall(_)) {
                            item.clone()
                        } else {
                            // Use the column alias as the expression
                            if let Some(alias) = &item.col_alias {
                                SelectItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    col_alias: item.col_alias.clone(),
                                }
                            } else {
                                item.clone()
                            }
                        }
                    })
                    .collect();

                // Convert ORDER BY for outer query
                let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items
                {
                    items
                        .iter()
                        .filter_map(|item| {
                            use crate::query_planner::logical_expr::LogicalExpr;
                            match &item.expression {
                                LogicalExpr::PropertyAccessExp(prop) => Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!(
                                        "\"{}.{}\"",
                                        prop.table_alias.0,
                                        prop.column.raw()
                                    )),
                                    order: item
                                        .order
                                        .clone()
                                        .try_into()
                                        .unwrap_or(OrderByOrder::Asc),
                                }),
                                LogicalExpr::ColumnAlias(alias) => Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    order: item
                                        .order
                                        .clone()
                                        .try_into()
                                        .unwrap_or(OrderByOrder::Asc),
                                }),
                                _ => None,
                            }
                        })
                        .collect()
                } else {
                    vec![]
                };

                crate::debug_println!("DEBUG: Creating aggregation-aware UNION plan with {} outer SELECT items, {} GROUP BY",
                    outer_select_items.len(), outer_group_by.len());

                return Ok(RenderPlan {
                    ctes: CteItems(all_branch_ctes.clone()),
                    select: SelectItems {
                        items: outer_select_items,
                        distinct: first_plan.select.distinct,
                    },
                    from: FromTableItem(None),
                    joins: JoinItems(vec![]),
                    array_join: ArrayJoinItem(Vec::new()),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(outer_group_by),
                    having_clause: first_plan.having_clause.clone(),
                    order_by: OrderByItems(order_by_items_converted),
                    skip: SkipItem(skip_val),
                    limit: LimitItem(limit_val),
                    union: UnionItems(Some(Union {
                        input: stripped_union_plans,
                        union_type: union.union_type.clone().try_into()?,
                    })),
                });
            }

            // Non-aggregation case: use original logic
            // Create a render plan with the union field populated
            // The first branch provides the SELECT structure
            let first_plan = union_plans.first().ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan("Union has no inputs".to_string())
            })?;

            // Convert ORDER BY items for UNION - use quoted alias names when possible
            // For UNION, ORDER BY must reference result column aliases.
            // If ORDER BY column matches a SELECT alias, use "alias"
            // If not, apply property mapping (for columns not in SELECT list)
            let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items {
                items.iter().filter_map(|item| {
                    use crate::query_planner::logical_expr::LogicalExpr;

                    let expr = match &item.expression {
                        LogicalExpr::PropertyAccessExp(prop) => {
                            // Try to find matching SELECT item by table alias
                            let matching_select = first_plan.select.items.iter()
                                .find(|s| matches!(&s.expression, RenderExpr::PropertyAccessExp(p) if p.table_alias.0 == prop.table_alias.0));

                            if let Some(select_item) = matching_select {
                                // Found matching SELECT item - use its alias
                                select_item.col_alias.as_ref()
                                    .map(|a| RenderExpr::Raw(format!("\"{}\"", a.0)))
                            } else {
                                // Not in SELECT - apply property mapping
                                let mut order_item: OrderByItem = item.clone().try_into().ok()?;
                                apply_property_mapping_to_expr(&mut order_item.expression, core_plan);
                                Some(order_item.expression)
                            }
                        }
                        LogicalExpr::ColumnAlias(alias) => Some(RenderExpr::Raw(format!("\"{}\"", alias.0))),
                        _ => None,
                    };

                    expr.map(|e| OrderByItem {
                        expression: e,
                        order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                    })
                }).collect()
            } else {
                vec![]
            };

            // Strip CTEs from union branches - they've been hoisted to outer level
            let stripped_union_plans: Vec<RenderPlan> = union_plans
                .into_iter()
                .map(|plan| {
                    RenderPlan {
                        ctes: CteItems(vec![]), // CTEs hoisted to outer level
                        ..plan
                    }
                })
                .collect();

            return Ok(RenderPlan {
                ctes: CteItems(all_branch_ctes), // Use hoisted CTEs from all branches
                select: SelectItems {
                    items: vec![],
                    distinct: false,
                }, // Empty - let to_sql use SELECT *
                from: FromTableItem(None),       // Union doesn't need FROM at top level
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(Vec::new()),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(order_by_items_converted),
                skip: SkipItem(skip_val),
                limit: LimitItem(limit_val), // LIMIT applies to entire UNION result
                union: UnionItems(Some(Union {
                    input: stripped_union_plans,
                    union_type: union.union_type.clone().try_into()?,
                })),
            });
        }

        // Check for GraphJoins wrapping Projection(Return) -> GroupBy pattern
        if let LogicalPlan::GraphJoins(graph_joins) = core_plan {
            crate::debug_println!("DEBUG: core_plan is GraphJoins");
            // Check if there's a variable-length or shortest path pattern in the tree
            // These require recursive CTEs and cannot use inline JOINs
            if has_variable_length_or_shortest_path(&graph_joins.input) {
                println!(
                    "DEBUG: Variable-length or shortest path detected in GraphJoins tree, returning Err to use CTE path"
                );
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Variable-length or shortest path patterns require CTE-based processing"
                        .to_string(),
                ));
            }

            // Check if there's a multiple-relationship OR polymorphic edge GraphRel anywhere in the tree
            if has_polymorphic_or_multi_rel(&graph_joins.input) {
                println!(
                    "DEBUG: Multiple relationship types or polymorphic edge detected in GraphJoins tree, returning Err to use CTE path"
                );
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Multiple relationship types require CTE-based processing with UNION"
                        .to_string(),
                ));
            }

            if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                if let LogicalPlan::GroupBy(group_by) = proj.input.as_ref() {
                    if group_by.having_clause.is_some() || !group_by.expressions.is_empty() {
                        println!(
                                "DEBUG: GraphJoins wrapping Projection(Return)->GroupBy detected, delegating to child"
                            );
                        // Delegate to the inner Projection -> GroupBy for CTE-based processing
                        let mut plan = graph_joins.input.to_render_plan(schema)?;

                        // Add ORDER BY/LIMIT/SKIP if they were present in the original query
                        if let Some(items) = order_by_items {
                            // Rewrite ORDER BY expressions for CTE context
                            let mut order_by_items_vec = vec![];
                            for item in items {
                                let rewritten_expr = match &item.expression {
                                        crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(col_alias) => {
                                            // ORDER BY column_alias -> ORDER BY grouped_data.column_alias
                                            RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: TableAlias("grouped_data".to_string()),
                                                    column: PropertyValue::Column(col_alias.0.clone()),
                                                }
                                            )
                                        }
                                        other_expr => {
                                            // Try to convert the expression
                                            other_expr.clone().try_into()?
                                        }
                                    };
                                order_by_items_vec.push(OrderByItem {
                                    expression: rewritten_expr,
                                    order: item.order.clone().try_into()?,
                                });
                            }
                            plan.order_by = OrderByItems(order_by_items_vec);
                        }

                        if let Some(limit) = limit_val {
                            plan.limit = LimitItem(Some(limit));
                        }

                        if let Some(skip) = skip_val {
                            plan.skip = SkipItem(Some(skip));
                        }

                        return Ok(plan);
                    }
                }
            }
        }

        // Check if this query needs CTE-based processing
        // First, check if there's any variable-length path anywhere in the plan
        // that isn't fixed-length (which can use inline JOINs)
        let has_vlp = self.contains_variable_length_path();
        crate::debug_println!("DEBUG: contains_variable_length_path() = {}", has_vlp);
        if has_vlp {
            // Check if it's truly variable (needs CTE) vs fixed-length (can use JOINs)
            let spec_opt = get_variable_length_spec(self);
            crate::debug_println!("DEBUG: get_variable_length_spec() = {:?}", spec_opt);
            if let Some(spec) = spec_opt {
                let is_fixed_length = spec.exact_hop_count().is_some();
                crate::debug_println!("DEBUG: is_fixed_length = {}", is_fixed_length);
                if !is_fixed_length {
                    crate::debug_println!(
                        "DEBUG: Plan contains variable-length path (range pattern) - need CTE"
                    );
                    return Err(RenderBuildError::InvalidRenderPlan(
                        "Variable-length paths require CTE-based processing".to_string(),
                    ));
                }
            }
        }

        // Check for WITH clause in GraphRel patterns
        // "MATCH (...) WITH x MATCH (x)-[...]->(y)" requires CTE-based processing
        // because the WITH clause creates a derived table that subsequent MATCH must join against
        if has_with_clause_in_graph_rel(self) {
            println!(
                "DEBUG: Plan contains WITH clause in GraphRel pattern - need CTE-based processing"
            );
            return Err(RenderBuildError::InvalidRenderPlan(
                "WITH clause followed by MATCH requires CTE-based processing".to_string(),
            ));
        }

        // Check for GraphJoins with CTE references
        // After WITH clauses are converted to CTEs, GraphJoins may have joins that reference CTEs
        // These need special handling that try_build_join_based_plan doesn't provide
        fn has_cte_joins(plan: &LogicalPlan, schema: &GraphSchema) -> bool {
            match plan {
                LogicalPlan::GraphJoins(gj) => {
                    // Extract joins properly using extract_joins()
                    match gj.input.extract_joins(schema) {
                        Ok(joins) => {
                            log::warn!(
                                "🐛 DEBUG has_cte_joins: Extracted {} joins from GraphJoins",
                                joins.len()
                            );
                            for (i, j) in joins.iter().enumerate() {
                                log::warn!(
                                    "🐛 DEBUG:   JOIN {}: table='{}', starts_with_with={}",
                                    i,
                                    j.table_name,
                                    j.table_name.starts_with("with_")
                                );
                            }
                            // Check if any join references a CTE (table name starts with "with_")
                            let has_cte = joins.iter().any(|j| j.table_name.starts_with("with_"));
                            if has_cte {
                                log::warn!("🐛 DEBUG: Found CTE reference in extracted joins!");
                                return true;
                            }
                            has_cte_joins(&gj.input, schema)
                        }
                        Err(e) => {
                            log::warn!("🐛 DEBUG has_cte_joins: Failed to extract joins: {}", e);
                            false
                        }
                    }
                }
                LogicalPlan::Projection(p) => has_cte_joins(&p.input, schema),
                LogicalPlan::Filter(f) => has_cte_joins(&f.input, schema),
                LogicalPlan::Limit(l) => has_cte_joins(&l.input, schema),
                LogicalPlan::OrderBy(o) => has_cte_joins(&o.input, schema),
                LogicalPlan::Skip(s) => has_cte_joins(&s.input, schema),
                LogicalPlan::GroupBy(g) => has_cte_joins(&g.input, schema),
                _ => false,
            }
        }

        log::warn!("🐛 DEBUG: Checking for CTE joins in plan...");
        if has_cte_joins(self, schema) {
            println!("DEBUG: Plan contains CTE reference joins - need CTE-aware rendering");
            log::warn!("❌ BLOCKING try_build_join_based_plan: Found CTE reference joins");
            return Err(RenderBuildError::InvalidRenderPlan(
                "CTE reference joins require CTE-aware rendering".to_string(),
            ));
        }
        log::warn!("🐛 DEBUG: No CTE joins found, continuing with try_build_join_based_plan");

        // Check for WITH+aggregation followed by MATCH pattern
        // "MATCH (...) WITH x, count(*) AS cnt MATCH (x)-[...]->(y)" requires CTE
        // because aggregation must be computed before the second MATCH
        if has_with_aggregation_pattern(self) {
            println!(
                "DEBUG: Plan contains WITH aggregation + MATCH pattern - need CTE-based processing"
            );
            return Err(RenderBuildError::InvalidRenderPlan(
                "WITH aggregation followed by MATCH requires CTE-based processing".to_string(),
            ));
        }

        if let LogicalPlan::Projection(proj) = self {
            if let LogicalPlan::GraphRel(graph_rel) = proj.input.as_ref() {
                // Variable-length paths: check if truly variable or just fixed-length
                if let Some(spec) = &graph_rel.variable_length {
                    let is_fixed_length =
                        spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

                    if is_fixed_length {
                        // 🚀 Fixed-length pattern (*2, *3) - can use inline JOINs!
                        println!(
                            "DEBUG: Fixed-length pattern (*{}) detected - will use inline JOINs",
                            spec.exact_hop_count().unwrap()
                        );
                        // Continue to extract_joins() path
                    } else {
                        // Truly variable-length (*1.., *0..5) or shortest path - needs CTE
                        crate::debug_println!("DEBUG: Variable-length pattern detected, returning Err to use CTE path");
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Variable-length paths require CTE-based processing".to_string(),
                        ));
                    }
                }

                // Multiple relationship types need UNION CTEs
                if let Some(labels) = &graph_rel.labels {
                    if labels.len() > 1 {
                        println!(
                            "DEBUG: Multiple relationship types detected ({}), returning Err to use CTE path",
                            labels.len()
                        );
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Multiple relationship types require CTE-based processing with UNION"
                                .to_string(),
                        ));
                    }
                }
            }
        }

        // Try to build with JOINs - this will work for:
        // - Simple MATCH queries with relationships
        // - OPTIONAL MATCH queries (via GraphRel.extract_joins)
        // - Multiple MATCH clauses (via GraphRel.extract_joins)
        // It will fail (return Err) for:
        // - Variable-length paths (need recursive CTEs)
        // - Multiple relationship types (need UNION CTEs)
        // - Complex nested queries
        // - Queries that don't have extractable JOINs

        crate::debug_println!(
            "DEBUG: Calling build_simple_relationship_render_plan with distinct: {}",
            distinct
        );
        self.build_simple_relationship_render_plan(Some(distinct), schema)
    }

    /// Build render plan for simple relationship queries using direct JOINs
    fn build_simple_relationship_render_plan(
        &self,
        distinct_override: Option<bool>,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        println!(
            "DEBUG: build_simple_relationship_render_plan START - plan type: {:?}",
            std::mem::discriminant(self)
        );

        // Extract distinct flag from the outermost Projection BEFORE unwrapping
        // This must be done first because unwrapping will replace self with core_plan
        // However, if distinct_override is provided, use that instead
        let distinct = distinct_override.unwrap_or_else(|| self.extract_distinct());
        println!(
            "DEBUG: build_simple_relationship_render_plan - extracted distinct (early): {}",
            distinct
        );

        // Special case: Detect Projection over GroupBy
        // This can be wrapped in OrderBy/Limit/Skip nodes
        // CTE is needed when RETURN items require data not available from WITH output

        // Unwrap OrderBy, Limit, Skip to find the core Projection
        let (core_plan, order_by, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                crate::debug_println!("DEBUG: Unwrapping Limit node, count={}", limit_node.count);
                let limit_val = limit_node.count;
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        crate::debug_println!("DEBUG: Found OrderBy inside Limit");
                        (
                            order_node.input.as_ref(),
                            Some(&order_node.items),
                            Some(limit_val),
                            None,
                        )
                    }
                    LogicalPlan::Skip(skip_node) => {
                        crate::debug_println!("DEBUG: Found Skip inside Limit");
                        (
                            skip_node.input.as_ref(),
                            None,
                            Some(limit_val),
                            Some(skip_node.count),
                        )
                    }
                    other => {
                        println!(
                            "DEBUG: Limit contains other type: {:?}",
                            std::mem::discriminant(other)
                        );
                        (other, None, Some(limit_val), None)
                    }
                }
            }
            LogicalPlan::OrderBy(order_node) => {
                crate::debug_println!("DEBUG: Unwrapping OrderBy node");
                (
                    order_node.input.as_ref(),
                    Some(&order_node.items),
                    None,
                    None,
                )
            }
            LogicalPlan::Skip(skip_node) => {
                crate::debug_println!("DEBUG: Unwrapping Skip node");
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => {
                println!(
                    "DEBUG: No unwrapping needed, plan type: {:?}",
                    std::mem::discriminant(other)
                );
                (other, None, None, None)
            }
        };

        println!(
            "DEBUG: After unwrapping - core_plan type: {:?}, has_order_by: {}, has_limit: {}, has_skip: {}",
            std::mem::discriminant(core_plan),
            order_by.is_some(),
            limit_val.is_some(),
            skip_val.is_some()
        );

        // Check for nested GroupBy pattern: GroupBy(GraphJoins(Projection(GroupBy(...))))
        // This happens with two-level aggregation: WITH has aggregation, RETURN has aggregation
        // Both need their own GROUP BY, requiring a subquery structure
        if let LogicalPlan::GroupBy(outer_group_by) = core_plan {
            // Check if there's an inner GroupBy (indicating two-level aggregation)
            fn find_inner_group_by(plan: &LogicalPlan) -> Option<&GroupBy> {
                match plan {
                    LogicalPlan::GroupBy(gb) => Some(gb),
                    LogicalPlan::GraphJoins(gj) => find_inner_group_by(&gj.input),
                    LogicalPlan::Projection(p) => find_inner_group_by(&p.input),
                    LogicalPlan::Filter(f) => find_inner_group_by(&f.input),
                    _ => None,
                }
            }

            // Also find the Projection that contains the RETURN items (between outer GroupBy and inner GroupBy)
            fn find_return_projection(plan: &LogicalPlan) -> Option<&Projection> {
                match plan {
                    LogicalPlan::Projection(p) => Some(p),
                    LogicalPlan::GraphJoins(gj) => find_return_projection(&gj.input),
                    LogicalPlan::Filter(f) => find_return_projection(&f.input),
                    _ => None,
                }
            }

            if let Some(inner_group_by) = find_inner_group_by(&outer_group_by.input) {
                println!("DEBUG: Detected nested GroupBy pattern (two-level aggregation)");

                // Find the RETURN projection items
                let return_projection = find_return_projection(&outer_group_by.input);

                // Extract WITH aliases from the inner GroupBy's input Projection
                // Also collect table aliases that refer to nodes passed through WITH
                fn extract_inner_with_aliases(
                    plan: &LogicalPlan,
                ) -> (
                    std::collections::HashSet<String>,
                    std::collections::HashSet<String>,
                ) {
                    match plan {
                        LogicalPlan::Projection(proj) => {
                            let mut aliases = std::collections::HashSet::new();
                            let mut table_aliases = std::collections::HashSet::new();
                            for item in &proj.items {
                                if let Some(a) = item.col_alias.as_ref() {
                                    aliases.insert(a.0.clone());
                                }
                                // Also track table aliases used in WITH (like "person" in "WITH person, count(...)")
                                match &item.expression {
                                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(ta) => {
                                        table_aliases.insert(ta.0.clone());
                                    }
                                    crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) => {
                                        table_aliases.insert(pa.table_alias.0.clone());
                                    }
                                    _ => {}
                                }
                            }
                            (aliases, table_aliases)
                        }
                        LogicalPlan::GraphJoins(gj) => extract_inner_with_aliases(&gj.input),
                        LogicalPlan::Filter(f) => extract_inner_with_aliases(&f.input),
                        _ => (
                            std::collections::HashSet::new(),
                            std::collections::HashSet::new(),
                        ),
                    }
                }
                let (with_aliases, with_table_aliases) =
                    extract_inner_with_aliases(&inner_group_by.input);
                println!("DEBUG: Found WITH aliases: {:?}", with_aliases);
                println!("DEBUG: Found WITH table aliases: {:?}", with_table_aliases);

                // Build the inner query (WITH clause result) as a CTE
                // Structure: SELECT <with_items> FROM <tables> GROUP BY <non-aggregates>
                // 🔧 FIX: Use the schema parameter instead of creating an empty schema

                // Build render plan for the inner GroupBy's input (the WITH clause query)
                let inner_render_plan = inner_group_by.input.to_render_plan(schema)?;

                // Extract GROUP BY expressions from SELECT items (non-aggregates)
                // For node variables (WITH a where a is a table alias), only GROUP BY ID columns
                // For other expressions, include them in GROUP BY as normal
                log::info!(
                    "🔧 Extracting GROUP BY from {} SELECT items, with_table_aliases: {:?}",
                    inner_render_plan.select.items.len(),
                    with_table_aliases
                );
                let inner_group_by_exprs: Vec<RenderExpr> = inner_render_plan
                    .select
                    .items
                    .iter()
                    .filter(|item| !matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                    .filter_map(|item| {
                        // Check if this is a property of a node variable (table alias)
                        if let RenderExpr::PropertyAccessExp(pa) = &item.expression {
                            let table_alias = &pa.table_alias.0;
                            log::info!(
                                "🔧   Checking item: table_alias={}, col_alias={:?}",
                                table_alias,
                                item.col_alias
                            );
                            // If this is a WITH table alias (node variable), only include ID columns
                            if with_table_aliases.contains(table_alias) {
                                log::info!("🔧     Is WITH table alias");
                                // Only include if it's an ID column
                                // ID columns have aliases like "a_user_id", "a_id", etc.
                                if let Some(col_alias) = &item.col_alias {
                                    let alias = &col_alias.0;
                                    // Pattern: <table>_*_id or <table>_id
                                    if alias.ends_with("_id") || alias.ends_with("_user_id") {
                                        log::info!("🔧       ID column, including: {}", alias);
                                        return Some(item.expression.clone());
                                    } else {
                                        log::info!("🔧       Non-ID column, skipping: {}", alias);
                                    }
                                }
                                // Not an ID column, skip it for GROUP BY
                                return None;
                            }
                        }
                        // For non-node variables, include in GROUP BY
                        log::info!("🔧   Including non-node expression");
                        Some(item.expression.clone())
                    })
                    .collect();
                log::info!(
                    "🔧 GROUP BY will have {} expressions",
                    inner_group_by_exprs.len()
                );

                // Create CTE for the inner (WITH) query
                // Generate CTE name from all exported aliases (both table aliases and aggregates)
                // Format: with_<alias1>_<alias2>_..._cte
                // This matches the format used in analyzer (graph_join_inference.rs)
                let mut all_with_aliases: Vec<String> = with_table_aliases
                    .iter()
                    .chain(with_aliases.iter())
                    .cloned()
                    .collect();
                all_with_aliases.sort(); // Ensure consistent ordering
                                         // Use base name (without counter) - counter added later if needed
                let cte_name = crate::utils::cte_naming::generate_cte_base_name(&all_with_aliases);
                let inner_cte = Cte::new(
                    cte_name.clone(),
                    super::CteContent::Structured(RenderPlan {
                        ctes: CteItems(vec![]),
                        select: inner_render_plan.select.clone(),
                        from: inner_render_plan.from.clone(),
                        joins: inner_render_plan.joins.clone(),
                        array_join: ArrayJoinItem(Vec::new()),
                        filters: inner_render_plan.filters.clone(),
                        group_by: GroupByExpressions(inner_group_by_exprs),
                        having_clause: inner_group_by
                            .having_clause
                            .as_ref()
                            .map(|h| h.clone().try_into())
                            .transpose()?,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                    }),
                    false, // is_recursive
                );

                // Build outer SELECT items from RETURN projection, rewriting WITH alias references
                let outer_select_items: Vec<SelectItem> = if let Some(proj) = return_projection {
                    proj.items
                        .iter()
                        .map(|item| {
                            let mut render_expr: RenderExpr = item.expression.clone().try_into()?;

                            // Rewrite WITH alias references (like postCount) to CTE references
                            let (rewritten, _) =
                                super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                                    render_expr.clone(),
                                    &with_aliases,
                                    &cte_name,
                                );
                            render_expr = rewritten;

                            // Also rewrite table alias references (like person.id) to CTE references
                            render_expr = super::plan_builder_helpers::rewrite_table_aliases_to_cte(
                                render_expr,
                                &with_table_aliases,
                                &cte_name,
                            );

                            Ok(SelectItem {
                                expression: render_expr,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|a| super::render_expr::ColumnAlias(a.0.clone())),
                            })
                        })
                        .collect::<Result<Vec<_>, RenderBuildError>>()?
                } else {
                    vec![]
                };

                // Build outer GROUP BY from outer_group_by.expressions, rewriting aliases
                let mut outer_group_by_exprs: Vec<RenderExpr> = Vec::new();
                for expr in &outer_group_by.expressions {
                    let render_expr: RenderExpr = expr.clone().try_into()?;
                    let (rewritten, _) = super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                        render_expr,
                        &with_aliases,
                        &cte_name,
                    );
                    outer_group_by_exprs.push(rewritten);
                }

                // Build ORDER BY items, rewriting WITH alias references
                let order_by_items = if let Some(order_items) = order_by {
                    order_items
                        .iter()
                        .map(|item| {
                            let expr: RenderExpr = item.expression.clone().try_into()?;
                            let (rewritten, _) =
                                super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                                    expr,
                                    &with_aliases,
                                    &cte_name,
                                );
                            Ok(super::OrderByItem {
                                expression: rewritten,
                                order: match item.order {
                                    crate::query_planner::logical_plan::OrderByOrder::Asc => {
                                        super::OrderByOrder::Asc
                                    }
                                    crate::query_planner::logical_plan::OrderByOrder::Desc => {
                                        super::OrderByOrder::Desc
                                    }
                                },
                            })
                        })
                        .collect::<Result<Vec<_>, RenderBuildError>>()?
                } else {
                    vec![]
                };

                // Return the nested query structure
                return Ok(RenderPlan {
                    ctes: CteItems(vec![inner_cte]),
                    select: SelectItems {
                        items: outer_select_items,
                        distinct: false,
                    },
                    from: FromTableItem(Some(ViewTableRef {
                        source: Arc::new(LogicalPlan::Empty),
                        name: cte_name.clone(),
                        alias: Some(cte_name.clone()),
                        use_final: false,
                    })),
                    joins: JoinItems(vec![]),
                    array_join: ArrayJoinItem(Vec::new()),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(outer_group_by_exprs),
                    having_clause: outer_group_by
                        .having_clause
                        .as_ref()
                        .map(|h| h.clone().try_into())
                        .transpose()?,
                    order_by: OrderByItems(order_by_items),
                    skip: SkipItem(skip_val),
                    limit: LimitItem(limit_val),
                    union: UnionItems(None),
                });
            }
        }

        // Now check if core_plan is Projection(Return) over GroupBy
        if let LogicalPlan::Projection(outer_proj) = core_plan {
            if let LogicalPlan::GroupBy(group_by) = outer_proj.input.as_ref() {
                // Check for variable-length paths in GroupBy's input
                // VLP with aggregation requires CTE-based processing
                if group_by.input.contains_variable_length_path() {
                    crate::debug_println!(
                        "DEBUG: GroupBy contains variable-length path - need CTE"
                    );
                    return Err(RenderBuildError::InvalidRenderPlan(
                        "Variable-length paths with aggregation require CTE-based processing"
                            .to_string(),
                    ));
                }

                // Check if RETURN items need data beyond what WITH provides
                // CTE is needed if RETURN contains:
                // 1. Node references (TableAlias that refers to a node, not a WITH alias)
                // 2. Wildcards (like `a.*`)
                // 3. References to WITH projection aliases that aren't in the inner projection

                // Collect all WITH projection aliases AND table aliases from the inner Projection
                // Handle GraphJoins wrapper by looking inside it
                let (with_aliases, with_table_aliases): (
                    std::collections::HashSet<String>,
                    std::collections::HashSet<String>,
                ) = {
                    // Helper to extract WITH aliases and table aliases from Projection(With)
                    fn extract_with_aliases_and_tables(
                        plan: &LogicalPlan,
                    ) -> (
                        std::collections::HashSet<String>,
                        std::collections::HashSet<String>,
                    ) {
                        match plan {
                            LogicalPlan::Projection(proj) => {
                                let mut aliases = std::collections::HashSet::new();
                                let mut table_aliases = std::collections::HashSet::new();

                                for item in &proj.items {
                                    // Collect explicit aliases (like `count(post) AS messageCount`)
                                    if let Some(alias) = &item.col_alias {
                                        aliases.insert(alias.0.clone());
                                    }
                                    // Collect table aliases from pass-through expressions (like `WITH person, ...`)
                                    match &item.expression {
                                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(ta) => {
                                                table_aliases.insert(ta.0.clone());
                                            }
                                            crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) => {
                                                table_aliases.insert(pa.table_alias.0.clone());
                                            }
                                            _ => {}
                                        }
                                }
                                (aliases, table_aliases)
                            }
                            LogicalPlan::GraphJoins(graph_joins) => {
                                // Look inside GraphJoins for the Projection
                                extract_with_aliases_and_tables(&graph_joins.input)
                            }
                            _ => (
                                std::collections::HashSet::new(),
                                std::collections::HashSet::new(),
                            ),
                        }
                    }
                    extract_with_aliases_and_tables(group_by.input.as_ref())
                };

                crate::debug_println!("DEBUG: WITH aliases found: {:?}", with_aliases);
                crate::debug_println!("DEBUG: WITH table aliases found: {:?}", with_table_aliases);

                // CTE is always needed when there are WITH aliases (aggregates)
                // because the outer query needs to reference them from the CTE
                let needs_cte = !with_aliases.is_empty()
                    || outer_proj.items.iter().any(|item| match &item.expression {
                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                            prop,
                        ) if prop.column.raw() == "*" => true,
                        _ => false,
                    });

                if needs_cte {
                    println!(
                            "DEBUG: Detected Projection(Return) over GroupBy where RETURN needs data beyond WITH output - using CTE pattern"

                        );

                    // Build the GROUP BY subquery as a CTE
                    // Step 1: Build inner query (GROUP BY + HAVING) as a RenderPlan
                    use crate::graph_catalog::graph_schema::GraphSchema;
                    use std::collections::HashMap;
                    let empty_schema = GraphSchema::build(
                        1,
                        "default".to_string(),
                        HashMap::new(),
                        HashMap::new(),
                    );
                    let inner_render_plan = group_by.input.to_render_plan(&empty_schema)?;

                    // Step 2: Extract GROUP BY expressions and HAVING clause
                    // For wildcards, we need to either:
                    // 1. GROUP BY all properties (to match SELECT), or
                    // 2. Only SELECT the ID column (to match GROUP BY)
                    // We'll do option 1: expand wildcards to all properties in GROUP BY
                    let mut group_by_exprs: Vec<RenderExpr> = Vec::new();
                    for expr in group_by.expressions.iter() {
                        match expr {
                            crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                prop,
                            ) if prop.column.raw() == "*" => {
                                // Expand a.* to all properties: a.age, a.name, a.user_id
                                if let Ok((properties, actual_table_alias)) =
                                    self.get_properties_with_table_alias(&prop.table_alias.0)
                                {
                                    let table_alias_to_use = actual_table_alias
                                        .as_ref()
                                        .map(|s| {
                                            crate::query_planner::logical_expr::TableAlias(
                                                s.clone(),
                                            )
                                        })
                                        .unwrap_or_else(|| prop.table_alias.clone());

                                    for (_prop_name, col_name) in properties {
                                        let expr = crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                                crate::query_planner::logical_expr::PropertyAccess {
                                                    table_alias: table_alias_to_use.clone(),
                                                    column: PropertyValue::Column(col_name),
                                                }
                                            );
                                        group_by_exprs.push(expr.try_into()?);
                                    }
                                } else {
                                    // Fallback to just ID column
                                    let id_column =
                                        self.find_id_column_for_alias(&prop.table_alias.0)?;
                                    let expr = crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: PropertyValue::Column(id_column),
                                            }
                                        );
                                    group_by_exprs.push(expr.try_into()?);
                                }
                            }
                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                // Expand table alias to all properties
                                if let Ok((properties, actual_table_alias)) =
                                    self.get_properties_with_table_alias(&alias.0)
                                {
                                    let table_alias_to_use = actual_table_alias
                                        .as_ref()
                                        .map(|s| {
                                            crate::query_planner::logical_expr::TableAlias(
                                                s.clone(),
                                            )
                                        })
                                        .unwrap_or_else(|| alias.clone());

                                    for (_prop_name, col_name) in properties {
                                        let expr = crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                                crate::query_planner::logical_expr::PropertyAccess {
                                                    table_alias: table_alias_to_use.clone(),
                                                    column: PropertyValue::Column(col_name),
                                                }
                                            );
                                        group_by_exprs.push(expr.try_into()?);
                                    }
                                } else {
                                    // Fallback to just ID column
                                    let id_column = self.find_id_column_for_alias(&alias.0)?;
                                    let expr = crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: alias.clone(),
                                                column: PropertyValue::Column(id_column),
                                            }
                                        );
                                    group_by_exprs.push(expr.try_into()?);
                                }
                            }
                            _ => {
                                group_by_exprs.push(expr.clone().try_into()?);
                            }
                        }
                    }

                    let having_expr: Option<RenderExpr> =
                        if let Some(having) = &group_by.having_clause {
                            Some(having.clone().try_into()?)
                        } else {
                            None
                        };

                    // Step 2.5: Build SELECT list for CTE (only grouping keys + aggregates, not wildcards)
                    // Extract from the inner Projection (child of GroupBy)
                    let cte_select_items = if let LogicalPlan::Projection(inner_proj) =
                        group_by.input.as_ref()
                    {
                        inner_proj
                            .items
                            .iter()
                            .map(|item| {
                                // For each projection item, check if it's an aggregate or grouping key
                                let render_expr: RenderExpr = item.expression.clone().try_into()?;

                                // Normalize aggregate arguments: COUNT(b) -> COUNT(b.user_id)
                                let normalized_expr = self.normalize_aggregate_args(render_expr)?;

                                // Replace wildcard expressions with the specific ID column
                                let (fixed_expr, auto_alias) = match &normalized_expr {
                                    RenderExpr::PropertyAccessExp(prop)
                                        if prop.column.raw() == "*" =>
                                    {
                                        // Find the ID column for this alias
                                        let id_col =
                                            self.find_id_column_for_alias(&prop.table_alias.0)?;
                                        let expr = RenderExpr::PropertyAccessExp(
                                            super::render_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: PropertyValue::Column(id_col.clone()),
                                            },
                                        );
                                        // Add alias so it can be referenced as grouped_data.user_id
                                        (expr, Some(super::render_expr::ColumnAlias(id_col)))
                                    }
                                    _ => (normalized_expr, None),
                                };

                                // Use existing alias if present, otherwise use auto-generated alias for grouping keys
                                let col_alias = item
                                    .col_alias
                                    .as_ref()
                                    .map(|a| super::render_expr::ColumnAlias(a.0.clone()))
                                    .or(auto_alias);

                                Ok(super::SelectItem {
                                    expression: fixed_expr,
                                    col_alias,
                                })
                            })
                            .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?
                    } else {
                        // Fallback to original select items
                        inner_render_plan.select.items.clone()
                    };

                    // Step 3: Create CTE with GROUP BY + HAVING
                    let cte_name = "grouped_data".to_string();
                    let cte = Cte::new(
                        cte_name.clone(),
                        super::CteContent::Structured(RenderPlan {
                            ctes: CteItems(vec![]),
                            select: SelectItems {
                                items: cte_select_items,
                                distinct: false,
                            },
                            from: inner_render_plan.from.clone(),
                            joins: inner_render_plan.joins.clone(),
                            array_join: ArrayJoinItem(Vec::new()),
                            filters: inner_render_plan.filters.clone(),
                            group_by: GroupByExpressions(group_by_exprs.clone()), // Clone to preserve for later use
                            having_clause: having_expr,
                            order_by: OrderByItems(vec![]),
                            skip: SkipItem(None),
                            limit: LimitItem(None),
                            union: UnionItems(None),
                        }),
                        false, // is_recursive
                    );

                    // Step 4: Build outer query that joins to CTE
                    // Extract the grouping key to use for join (use the FIXED expression with ID column)
                    let grouping_key_render = if let Some(first_expr) = group_by_exprs.first() {
                        first_expr.clone()
                    } else {
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "GroupBy has no grouping expressions after fixing wildcards"
                                .to_string(),
                        ));
                    };

                    // Extract table alias and column name from the fixed grouping key
                    let (table_alias, key_column) = match &grouping_key_render {
                        RenderExpr::PropertyAccessExp(prop_access) => (
                            prop_access.table_alias.0.clone(),
                            prop_access.column.clone(),
                        ),
                        _ => {
                            return Err(RenderBuildError::InvalidRenderPlan(
                                "Grouping expression is not a property access after fixing"
                                    .to_string(),
                            ));
                        }
                    };

                    // Build outer SELECT items from outer_proj
                    // Need to rewrite references to WITH aliases AND table aliases to pull from the CTE
                    // Also track if ALL RETURN items reference WITH aliases or table aliases
                    let mut all_items_from_with = true;
                    let outer_select_items = outer_proj
                            .items
                            .iter()
                            .map(|item| {
                                let expr: RenderExpr = item.expression.clone().try_into()?;

                                // Step 1: Rewrite TableAlias/ColumnAlias references that are WITH aliases
                                // This handles cases like AVG(follows) -> AVG(grouped_data.follows)
                                let (rewritten_expr, from_with_alias) =
                                    super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                                        expr,
                                        &with_aliases,
                                        &cte_name
                                    );

                                // Step 2: Also rewrite table alias references (like person.id) to CTE references
                                // This handles cases like `WITH person, ...` -> person.id becomes grouped_data."person.id"
                                let final_expr = super::plan_builder_helpers::rewrite_table_aliases_to_cte(
                                    rewritten_expr,
                                    &with_table_aliases,
                                    &cte_name,
                                );

                                // Check if the original expression referenced a table alias from WITH
                                let from_table_alias = matches!(&item.expression,
                                    crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa)
                                        if with_table_aliases.contains(&pa.table_alias.0));

                                if !from_with_alias && !from_table_alias {
                                    all_items_from_with = false;
                                }

                                Ok(super::SelectItem {
                                    expression: final_expr,
                                    col_alias: item.col_alias.as_ref().map(|alias| {
                                        super::render_expr::ColumnAlias(alias.0.clone())
                                    }),
                                })
                            })
                            .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?;

                    println!(
                        "DEBUG: all_items_from_with={}, with_aliases={:?}",
                        all_items_from_with, with_aliases
                    );

                    // If ALL RETURN items come from WITH aliases, we can SELECT directly from the CTE
                    // without needing to join back to the original table
                    if all_items_from_with {
                        println!(
                            "DEBUG: All RETURN items come from WITH - selecting directly from CTE"
                        );

                        // Build ORDER BY items for the direct-from-CTE case
                        let order_by_items = if let Some(order_items) = order_by {
                            order_items.iter()
                                    .map(|item| {
                                        let expr: RenderExpr = item.expression.clone().try_into()?;
                                        // Recursively rewrite WITH aliases to CTE references
                                        let (rewritten_expr, _) =
                                            super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                                                expr,
                                                &with_aliases,
                                                &cte_name
                                            );
                                        // Also rewrite table alias references
                                        let final_expr = super::plan_builder_helpers::rewrite_table_aliases_to_cte(
                                            rewritten_expr,
                                            &with_table_aliases,
                                            &cte_name,
                                        );
                                        Ok(super::OrderByItem {
                                            expression: final_expr,
                                            order: match item.order {
                                                crate::query_planner::logical_plan::OrderByOrder::Asc => super::OrderByOrder::Asc,
                                                crate::query_planner::logical_plan::OrderByOrder::Desc => super::OrderByOrder::Desc,
                                            },
                                        })
                                    })
                                    .collect::<Result<Vec<_>, RenderBuildError>>()?
                        } else {
                            vec![]
                        };

                        // Return CTE-based plan that SELECT directly from CTE (no join)
                        return Ok(RenderPlan {
                            ctes: CteItems(vec![cte]),
                            select: SelectItems {
                                items: outer_select_items,
                                distinct: false,
                            },
                            from: FromTableItem(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: cte_name.clone(),
                                alias: Some(cte_name.clone()),
                                use_final: false,
                            })),
                            joins: JoinItems(vec![]), // No joins needed
                            array_join: ArrayJoinItem(Vec::new()),
                            filters: FilterItems(None),
                            group_by: GroupByExpressions(vec![]),
                            having_clause: None,
                            order_by: OrderByItems(order_by_items),
                            skip: SkipItem(skip_val),
                            limit: LimitItem(limit_val),
                            union: UnionItems(None),
                        });
                    }

                    // Extract FROM table for the outer query
                    // IMPORTANT: The outer query needs to use the table for the grouping key alias,
                    // not the inner query's FROM table. For example, if we're grouping by g.group_id
                    // where g is a Group, the outer query should FROM sec_groups AS g, not sec_users.
                    let outer_from = {
                        // Find the table name for the grouping key's alias
                        if let Some(table_name) = find_table_name_for_alias(self, &table_alias) {
                            FromTableItem(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: table_name,
                                alias: Some(table_alias.clone()),
                                use_final: false,
                            }))
                        } else {
                            // Fallback to inner query's FROM if we can't find the table
                            inner_render_plan.from.clone()
                        }
                    };

                    // Create JOIN condition: a.user_id = grouped_data.user_id
                    let cte_key_expr =
                        RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                            table_alias: super::render_expr::TableAlias(cte_name.clone()),
                            column: key_column.clone(),
                        });

                    let join_condition = super::render_expr::OperatorApplication {
                        operator: super::render_expr::Operator::Equal,
                        operands: vec![grouping_key_render, cte_key_expr],
                    };

                    // Create a join to the CTE
                    let cte_join = super::Join {
                        table_name: cte_name.clone(),
                        table_alias: cte_name.clone(),
                        joining_on: vec![join_condition],
                        join_type: super::JoinType::Inner,
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                    };

                    println!(
                        "DEBUG: Created GroupBy CTE pattern with table_alias={}, key_column={}",
                        table_alias,
                        key_column.raw()
                    );

                    // Build ORDER BY items, rewriting WITH alias references to CTE references
                    let order_by_items = if let Some(order_items) = order_by {
                        order_items
                            .iter()
                            .map(|item| {
                                let expr: RenderExpr = item.expression.clone().try_into()?;
                                // Recursively rewrite WITH aliases to CTE references
                                let (rewritten_expr, _) =
                                    super::plan_builder_helpers::rewrite_with_aliases_to_cte(
                                        expr,
                                        &with_aliases,
                                        &cte_name,
                                    );
                                // Also rewrite table alias references
                                let final_expr =
                                    super::plan_builder_helpers::rewrite_table_aliases_to_cte(
                                        rewritten_expr,
                                        &with_table_aliases,
                                        &cte_name,
                                    );

                                Ok(super::OrderByItem {
                                    expression: final_expr,
                                    order: match item.order {
                                        crate::query_planner::logical_plan::OrderByOrder::Asc => {
                                            super::OrderByOrder::Asc
                                        }
                                        crate::query_planner::logical_plan::OrderByOrder::Desc => {
                                            super::OrderByOrder::Desc
                                        }
                                    },
                                })
                            })
                            .collect::<Result<Vec<_>, RenderBuildError>>()?
                    } else {
                        vec![]
                    };

                    // Return the CTE-based plan with proper JOIN, ORDER BY, and LIMIT
                    return Ok(RenderPlan {
                        ctes: CteItems(vec![cte]),
                        select: SelectItems {
                            items: outer_select_items,
                            distinct: false,
                        },
                        from: outer_from,
                        joins: JoinItems(vec![cte_join]),
                        array_join: ArrayJoinItem(Vec::new()),
                        filters: FilterItems(None),
                        group_by: GroupByExpressions(vec![]),
                        having_clause: None,
                        order_by: OrderByItems(order_by_items),
                        skip: SkipItem(skip_val),
                        limit: LimitItem(limit_val),
                        union: UnionItems(None),
                    });
                }
            } else {
                println!(
                    "DEBUG: Projection(Return) input is NOT GroupBy, discriminant: {:?}",
                    std::mem::discriminant(outer_proj.input.as_ref())
                );
            }
        } else {
            println!(
                "DEBUG: core_plan is NOT Projection, discriminant: {:?}",
                std::mem::discriminant(core_plan)
            );
        }

        let mut final_select_items = self.extract_select_items()?;
        log::debug!(
            "build_simple_relationship_render_plan - final_select_items BEFORE alias remap: {:?}",
            final_select_items
        );

        // For denormalized patterns (zeek unified, etc.), remap node aliases to edge aliases
        // This ensures SELECT src."id.orig_h" becomes SELECT a06963149f."id.orig_h" when src is denormalized on edge a06963149f
        for item in &mut final_select_items {
            apply_property_mapping_to_expr(&mut item.expression, self);
        }
        log::debug!(
            "build_simple_relationship_render_plan - final_select_items AFTER alias remap: {:?}",
            final_select_items
        );

        // Validate that we have proper select items
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found for relationship query. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }

        // Validate that select items are not just literals (which would indicate failed expression conversion)
        for item in &final_select_items {
            if let RenderExpr::Literal(_) = &item.expression {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Select item is a literal value, indicating failed expression conversion. Check schema mappings and query structure.".to_string()
                ));
            }
        }

        // 🔧 CRITICAL FIX FOR MULTI-HOP PATTERNS:
        // Recursively search for FROM marker in GraphJoins, handling all plan structure variations.
        // FROM markers are joins with empty joining_on, created by infer_graph_join for anchor nodes.
        // They can be buried under Filter (WHERE), Limit, Skip, OrderBy, or other wrapper nodes.
        //
        // Examples that need FROM markers:
        // - MATCH (u)-[:FOLLOWS]->(f1)-[:FOLLOWS]->(f2) WHERE u.user_id = 1
        // - MATCH (a)-[]->(b)-[]->(c) WHERE a.id = 1 LIMIT 10
        // - MATCH (x)-[]->(y)-[]->(z) ORDER BY x.name WHERE x.active = true
        //
        // Generic recursive search finds FROM marker at any depth, not just specific patterns.
        fn find_from_marker_recursive(plan: &LogicalPlan) -> Option<FromTable> {
            log::info!(
                "🔍 find_from_marker_recursive: examining plan type: {:?}",
                std::mem::discriminant(plan)
            );
            match plan {
                // Recurse through wrapper nodes (WHERE, LIMIT, ORDER BY, etc.)
                LogicalPlan::Projection(proj) => {
                    log::info!("  ↳ Recursing through Projection");
                    find_from_marker_recursive(&proj.input)
                }
                LogicalPlan::Filter(filter) => {
                    log::info!("  ↳ Recursing through Filter");
                    find_from_marker_recursive(&filter.input)
                }
                LogicalPlan::Limit(limit) => {
                    log::info!("  ↳ Recursing through Limit");
                    find_from_marker_recursive(&limit.input)
                }
                LogicalPlan::Skip(skip) => {
                    log::info!("  ↳ Recursing through Skip");
                    find_from_marker_recursive(&skip.input)
                }
                LogicalPlan::OrderBy(order) => {
                    log::info!("  ↳ Recursing through OrderBy");
                    find_from_marker_recursive(&order.input)
                }
                LogicalPlan::GroupBy(group) => {
                    log::info!("  ↳ Recursing through GroupBy");
                    find_from_marker_recursive(&group.input)
                }

                // Found GraphJoins - search for FROM marker
                LogicalPlan::GraphJoins(graph_joins) => {
                    log::info!(
                        "  ↳ Found GraphJoins with {} joins",
                        graph_joins.joins.len()
                    );
                    for (i, j) in graph_joins.joins.iter().enumerate() {
                        log::info!(
                            "      Join[{}]: table='{}' alias='{}' joining_on.len={}",
                            i,
                            j.table_name,
                            j.table_alias,
                            j.joining_on.len()
                        );
                    }
                    graph_joins
                        .joins
                        .iter()
                        .find(|j| j.joining_on.is_empty())
                        .map(|from_marker| {
                            log::info!(
                                "🏠 Found FROM marker: '{}' AS '{}' (recursive search)",
                                from_marker.table_name,
                                from_marker.table_alias
                            );
                            FromTable::new(Some(ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: from_marker.table_name.clone(),
                                alias: Some(from_marker.table_alias.clone()),
                                use_final: false,
                            }))
                        })
                }

                // Stop recursion at other node types
                _ => {
                    log::info!(
                        "  ↳ Stopping recursion at node type: {:?}",
                        std::mem::discriminant(plan)
                    );
                    None
                }
            }
        }

        let from_marker_from = find_from_marker_recursive(core_plan);
        let from_marker_present = from_marker_from.is_some();
        let mut final_from = from_marker_from.or_else(|| core_plan.extract_from().ok().flatten());

        log::debug!(
            "🔍 build_simple_relationship_render_plan - extracted final_from from core_plan type: {:?}, is_some: {}, from_marker_used: {}",
            std::mem::discriminant(core_plan),
            final_from.is_some(),
            from_marker_present
        );

        // 🚀 CONSOLIDATED VLP FROM CLAUSE AND ALIAS REWRITING
        // For fixed-length VLP patterns, we need to:
        // 1. Set the correct FROM table based on schema type
        // 2. For Denormalized schemas, build alias mappings and rewrite expressions
        //
        // CRITICAL: Must search recursively because self could be Limit(GraphJoins(...))
        fn find_vlp_graph_rel_recursive(
            plan: &LogicalPlan,
        ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
            match plan {
                LogicalPlan::GraphRel(gr) if gr.variable_length.is_some() => Some(gr),
                LogicalPlan::GraphJoins(gj) => find_vlp_graph_rel_recursive(&gj.input),
                LogicalPlan::Projection(p) => find_vlp_graph_rel_recursive(&p.input),
                LogicalPlan::Filter(f) => find_vlp_graph_rel_recursive(&f.input),
                LogicalPlan::Limit(l) => find_vlp_graph_rel_recursive(&l.input),
                LogicalPlan::Skip(s) => find_vlp_graph_rel_recursive(&s.input),
                LogicalPlan::OrderBy(o) => find_vlp_graph_rel_recursive(&o.input),
                _ => None,
            }
        }

        // Store VLP alias mapping for denormalized schemas
        // Format: (simple_alias_map, rel_column_to_hop_map, rel_alias)
        let mut vlp_alias_map: Option<(
            std::collections::HashMap<String, String>, // simple: a -> r1, b -> rN
            std::collections::HashMap<String, String>, // column -> hop: Origin -> r1, DestCityName -> rN
            String,                                    // rel_alias (f)
        )> = None;

        if let Some(graph_rel) = find_vlp_graph_rel_recursive(self) {
            if let Some(vlp_ctx) = build_vlp_context(graph_rel) {
                if vlp_ctx.is_fixed_length {
                    let exact_hops = vlp_ctx.exact_hops.unwrap_or(1);

                    // Get FROM info from the consolidated context
                    let (from_table, from_alias, _) =
                        expand_fixed_length_joins_with_context(&vlp_ctx);

                    println!(
                        "DEBUG: Fixed-length VLP (*{}) {:?} - setting FROM {} AS {}",
                        exact_hops, vlp_ctx.schema_type, from_table, from_alias
                    );

                    final_from = Some(FromTable::new(Some(ViewTableRef {
                        source: std::sync::Arc::new(LogicalPlan::Empty),
                        name: from_table,
                        alias: Some(from_alias),
                        use_final: false,
                    })));

                    // For denormalized schemas, build alias mapping:
                    // - start_alias (a) -> r1
                    // - end_alias (b) -> rN
                    // - rel_alias (f) -> DEPENDS on column (from_node_properties -> r1, to_node_properties -> rN)
                    if vlp_ctx.schema_type == VlpSchemaType::Denormalized {
                        // Simple alias map for node aliases
                        let mut simple_map = std::collections::HashMap::new();
                        simple_map.insert(vlp_ctx.start_alias.clone(), "r1".to_string());
                        simple_map.insert(vlp_ctx.end_alias.clone(), format!("r{}", exact_hops));

                        // Build column -> hop alias mapping for relationship alias
                        // from_node_properties -> r1
                        // to_node_properties -> rN
                        let mut rel_column_map: std::collections::HashMap<String, String> =
                            std::collections::HashMap::new();

                        // Try to get node properties from the schema
                        // The node label should be the same for both (Airport in ontime_denormalized)
                        if let Some(schema_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                            if let Ok(schemas) = schema_lock.try_read() {
                                // Try different schema names
                                for schema_name in ["default", ""] {
                                    if let Some(schema) = schemas.get(schema_name) {
                                        // Get the node label from the graph_rel
                                        if let Some(_node_label) =
                                            graph_rel.labels.as_ref().and_then(|l| l.first())
                                        {
                                            // Actually, we need the node label, not rel label
                                            // Get it from the left/right GraphNodes
                                            fn get_node_label(
                                                plan: &LogicalPlan,
                                            ) -> Option<String>
                                            {
                                                match plan {
                                                    LogicalPlan::GraphNode(n) => n.label.clone(),
                                                    _ => None,
                                                }
                                            }

                                            if let Some(label) = get_node_label(&graph_rel.left) {
                                                if let Some(node_schema) =
                                                    schema.get_nodes_schemas().get(&label)
                                                {
                                                    // Add from_properties columns -> r1
                                                    if let Some(ref from_props) =
                                                        node_schema.from_properties
                                                    {
                                                        for (_, col_value) in from_props {
                                                            let col_name = col_value.clone();
                                                            rel_column_map
                                                                .insert(col_name, "r1".to_string());
                                                        }
                                                    }
                                                    // Add to_properties columns -> rN
                                                    if let Some(ref to_props) =
                                                        node_schema.to_properties
                                                    {
                                                        for (_, col_value) in to_props {
                                                            let col_name = col_value.clone();
                                                            rel_column_map.insert(
                                                                col_name,
                                                                format!("r{}", exact_hops),
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        // Fallback: use VlpContext properties if available
                        if let Some(ref from_props) = vlp_ctx.from_node_properties {
                            for (_, col_value) in from_props {
                                let col_name = match col_value {
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(c) => c.clone(),
                                    crate::graph_catalog::expression_parser::PropertyValue::Expression(e) => e.clone(),
                                };
                                rel_column_map.insert(col_name, "r1".to_string());
                            }
                        }

                        if let Some(ref to_props) = vlp_ctx.to_node_properties {
                            for (_, col_value) in to_props {
                                let col_name = match col_value {
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(c) => c.clone(),
                                    crate::graph_catalog::expression_parser::PropertyValue::Expression(e) => e.clone(),
                                };
                                rel_column_map.insert(col_name, format!("r{}", exact_hops));
                            }
                        }

                        // Also add from_id and to_id columns
                        rel_column_map.insert(vlp_ctx.rel_from_col.clone(), "r1".to_string());
                        rel_column_map
                            .insert(vlp_ctx.rel_to_col.clone(), format!("r{}", exact_hops));

                        println!(
                            "DEBUG: Denormalized VLP alias mapping - simple: {:?}, rel_column: {:?}",
                            simple_map, rel_column_map
                        );

                        vlp_alias_map =
                            Some((simple_map, rel_column_map, vlp_ctx.rel_alias.clone()));
                    }
                }
            }
        }

        // Check if we have UNWIND clauses - if so and no FROM, resolve the source table
        let array_joins = self.extract_array_join()?;
        if final_from.is_none() && !array_joins.is_empty() {
            log::debug!("UNWIND clause detected without FROM table - checking for CTE references");

            // Try to find CTE references from GraphJoins in the plan
            fn extract_cte_references_from_plan(
                plan: &LogicalPlan,
            ) -> std::collections::HashMap<String, String> {
                match plan {
                    LogicalPlan::GraphJoins(gj) => gj.cte_references.clone(),
                    LogicalPlan::Projection(p) => extract_cte_references_from_plan(&p.input),
                    LogicalPlan::Filter(f) => extract_cte_references_from_plan(&f.input),
                    LogicalPlan::Unwind(u) => extract_cte_references_from_plan(&u.input),
                    LogicalPlan::Limit(l) => extract_cte_references_from_plan(&l.input),
                    LogicalPlan::Skip(s) => extract_cte_references_from_plan(&s.input),
                    LogicalPlan::OrderBy(o) => extract_cte_references_from_plan(&o.input),
                    LogicalPlan::GroupBy(g) => extract_cte_references_from_plan(&g.input),
                    _ => std::collections::HashMap::new(),
                }
            }

            let cte_refs = extract_cte_references_from_plan(self);

            if let Some(cte_name) = cte_refs.values().next() {
                // Use the CTE as FROM table
                log::info!("✅ UNWIND: Found CTE reference '{}', using as FROM table instead of system.one", cte_name);
                final_from = Some(FromTable::new(Some(ViewTableRef {
                    source: std::sync::Arc::new(
                        crate::query_planner::logical_plan::LogicalPlan::Empty,
                    ),
                    name: cte_name.clone(),
                    alias: Some(cte_name.clone()),
                    use_final: false,
                })));
            } else {
                // Check if the first UNWIND expression is standalone (no schema references)
                // Only use system.one if unwinding a literal or standalone expression
                let first_array_join = &array_joins[0];
                let is_standalone = super::plan_builder_helpers::is_standalone_expression(
                    &first_array_join.expression,
                );

                if is_standalone {
                    // Standalone UNWIND with literals: UNWIND [1,2,3] AS n
                    log::debug!(
                        "✅ UNWIND: Expression is standalone (no schema refs), using system.one"
                    );
                    final_from = Some(FromTable::new(Some(ViewTableRef {
                        source: std::sync::Arc::new(
                            crate::query_planner::logical_plan::LogicalPlan::Empty,
                        ),
                        name: "system.one".to_string(),
                        alias: Some("_dummy".to_string()),
                        use_final: false,
                    })));
                } else {
                    // UNWIND references schema elements but no CTE found - this is an error!
                    log::error!("❌ UNWIND: Expression references schema elements but no CTE or FROM table found!");
                    log::error!("   Expression: {:?}", first_array_join.expression);
                    return Err(RenderBuildError::InvalidRenderPlan(
                        format!("UNWIND expression references schema elements (columns/properties) but no FROM table or CTE was found. \
                                This indicates a query planning bug. Expression: {:?}", first_array_join.expression)
                    ));
                }
            }
        }

        // Validate that we have a FROM clause
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM table found for relationship query. Schema inference may have failed."
                    .to_string(),
            ));
        }

        // Helper function to rewrite table aliases in RenderExpr for denormalized VLP
        // Takes: (simple_alias_map, column_to_hop_map, rel_alias)
        fn rewrite_aliases_in_expr_vlp(
            expr: RenderExpr,
            simple_map: &std::collections::HashMap<String, String>,
            column_map: &std::collections::HashMap<String, String>,
            rel_alias: &str,
        ) -> RenderExpr {
            use super::render_expr::{
                AggregateFnCall, OperatorApplication, PropertyAccess, ScalarFnCall, TableAlias,
            };
            use crate::graph_catalog::expression_parser::PropertyValue;

            match expr {
                RenderExpr::PropertyAccessExp(prop) => {
                    // Get the column name from the PropertyValue
                    let col_name = match &prop.column {
                        PropertyValue::Column(c) => c.clone(),
                        PropertyValue::Expression(e) => e.clone(),
                    };

                    let new_alias = if prop.table_alias.0 == rel_alias {
                        // This is a relationship alias - look up by column name
                        column_map
                            .get(&col_name)
                            .cloned()
                            .unwrap_or_else(|| "r1".to_string())
                    } else {
                        // Check simple map for node aliases
                        simple_map
                            .get(&prop.table_alias.0)
                            .cloned()
                            .unwrap_or_else(|| prop.table_alias.0.clone())
                    };

                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(new_alias),
                        column: prop.column,
                    })
                }
                RenderExpr::TableAlias(alias) => {
                    if let Some(new_alias) = simple_map.get(&alias.0) {
                        RenderExpr::TableAlias(TableAlias(new_alias.clone()))
                    } else {
                        RenderExpr::TableAlias(alias)
                    }
                }
                RenderExpr::OperatorApplicationExp(op) => {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator,
                        operands: op
                            .operands
                            .into_iter()
                            .map(|o| {
                                rewrite_aliases_in_expr_vlp(o, simple_map, column_map, rel_alias)
                            })
                            .collect(),
                    })
                }
                RenderExpr::ScalarFnCall(func) => RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: func.name,
                    args: func
                        .args
                        .into_iter()
                        .map(|a| rewrite_aliases_in_expr_vlp(a, simple_map, column_map, rel_alias))
                        .collect(),
                }),
                RenderExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name,
                    args: agg
                        .args
                        .into_iter()
                        .map(|a| rewrite_aliases_in_expr_vlp(a, simple_map, column_map, rel_alias))
                        .collect(),
                }),
                RenderExpr::List(items) => RenderExpr::List(
                    items
                        .into_iter()
                        .map(|i| rewrite_aliases_in_expr_vlp(i, simple_map, column_map, rel_alias))
                        .collect(),
                ),
                RenderExpr::Case(case) => RenderExpr::Case(super::render_expr::RenderCase {
                    expr: case.expr.map(|e| {
                        Box::new(rewrite_aliases_in_expr_vlp(
                            *e, simple_map, column_map, rel_alias,
                        ))
                    }),
                    when_then: case
                        .when_then
                        .into_iter()
                        .map(|(w, t)| {
                            (
                                rewrite_aliases_in_expr_vlp(w, simple_map, column_map, rel_alias),
                                rewrite_aliases_in_expr_vlp(t, simple_map, column_map, rel_alias),
                            )
                        })
                        .collect(),
                    else_expr: case.else_expr.map(|e| {
                        Box::new(rewrite_aliases_in_expr_vlp(
                            *e, simple_map, column_map, rel_alias,
                        ))
                    }),
                }),
                // Pass through expressions that don't contain table aliases
                other => other,
            }
        }

        // Apply alias rewriting for denormalized VLP if we have a mapping
        if let Some((ref simple_map, ref column_map, ref rel_alias)) = vlp_alias_map {
            crate::debug_println!("DEBUG: Rewriting select items with VLP alias map: simple={:?}, column={:?}, rel={}",
                     simple_map, column_map, rel_alias);
            final_select_items = final_select_items
                .into_iter()
                .map(|item| SelectItem {
                    expression: rewrite_aliases_in_expr_vlp(
                        item.expression,
                        simple_map,
                        column_map,
                        rel_alias,
                    ),
                    col_alias: item.col_alias,
                })
                .collect();
        }

        let mut final_filters = self.extract_filters()?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - final_filters: {:?}",
            final_filters
        );

        // Apply alias rewriting to filters for denormalized VLP
        if let Some((ref simple_map, ref column_map, ref rel_alias)) = vlp_alias_map {
            if let Some(filter) = final_filters {
                crate::debug_println!(
                    "DEBUG: Rewriting filters with VLP alias map: simple={:?}, column={:?}, rel={}",
                    simple_map,
                    column_map,
                    rel_alias
                );
                final_filters = Some(rewrite_aliases_in_expr_vlp(
                    filter, simple_map, column_map, rel_alias,
                ));
            }
        }

        // Apply property mapping to filters to translate denormalized node aliases to their SQL table aliases.
        // For denormalized nodes (like `d:Domain` stored on edge table), the Cypher alias `d`
        // doesn't exist in SQL. We must rewrite `d.answers` to `edge_alias.answers`.
        // Uses the same apply_property_mapping_to_expr that works for SELECT items.
        if let Some(ref mut filter) = final_filters {
            apply_property_mapping_to_expr(filter, self);
        }

        // Validate that filters don't contain obviously invalid expressions
        if let Some(ref filter_expr) = final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter expression appears invalid (e.g., '1 = 0'). This usually indicates schema mapping issues.".to_string()
                ));
            }
        }

        let mut extracted_joins = self.extract_joins(schema)?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - extracted {} joins",
            extracted_joins.len()
        );

        // Filter out JOINs that duplicate the FROM table
        // If we're starting FROM node 'a', we shouldn't also have it in the JOINs list
        // BUT: If the filtered-out JOIN has a pre_filter (e.g., polymorphic edge filter),
        // we need to preserve it as a WHERE filter
        let from_alias = final_from
            .as_ref()
            .and_then(|ft| ft.table.as_ref())
            .and_then(|vt| vt.alias.clone());
        let mut anchor_pre_filter: Option<RenderExpr> = None;
        let filtered_joins: Vec<Join> = if let Some(ref anchor_alias) = from_alias {
            extracted_joins.into_iter()
                .filter(|join| {
                    if &join.table_alias == anchor_alias {
                        crate::debug_println!("DEBUG: Filtering out JOIN for '{}' because it's already in FROM clause", anchor_alias);
                        // Preserve the pre_filter from the anchor JOIN
                        if join.pre_filter.is_some() {
                            anchor_pre_filter = join.pre_filter.clone();
                            crate::debug_println!("DEBUG: Preserving pre_filter from anchor JOIN: {:?}", anchor_pre_filter);
                        }
                        false
                    } else {
                        true
                    }
                })
                .collect()
        } else {
            extracted_joins
        };

        // Add anchor pre_filter to final_filters if present
        let final_filters = if let Some(filter) = anchor_pre_filter {
            crate::debug_println!("DEBUG: Adding anchor pre_filter to final_filters");
            match final_filters {
                Some(existing) => {
                    // Combine existing filter with anchor pre_filter using AND
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![existing, filter],
                    }))
                }
                None => Some(filter),
            }
        } else {
            final_filters
        };

        println!(
            "DEBUG: build_simple_relationship_render_plan - filtered_joins: {:?}",
            filtered_joins
        );

        // Sort JOINs by dependency order to ensure referenced tables are defined before use
        // CRITICAL FIX: This was missing in CTE generation path, causing multi-hop WITH clauses
        // to generate JOINs in wrong order (e.g., JOIN t1 ON t1.MessageId = message.id before message is defined)
        let sorted_joins = sort_joins_by_dependency(filtered_joins, final_from.as_ref());
        println!(
            "DEBUG: build_simple_relationship_render_plan - sorted_joins: {:?}",
            sorted_joins
        );

        // distinct was already extracted at the beginning of this function
        println!(
            "DEBUG: build_simple_relationship_render_plan - using pre-extracted distinct: {}",
            distinct
        );

        //  🔧 WITH CLAUSE CTE EXTRACTION
        // Extract CTEs from WITH clauses in the logical plan
        // This is needed for WITH...MATCH patterns where first MATCH is in CTE
        println!(
            "DEBUG CTE EXTRACTION SHORT: About to extract CTEs from plan type {:?}",
            std::mem::discriminant(self)
        );
        let mut context = analyze_property_requirements(self, schema);
        let extracted_ctes = match self.extract_ctes_with_context("_", &mut context, schema) {
            Ok(ctes) => {
                println!(
                    "DEBUG CTE EXTRACTION SHORT: Successfully extracted {} CTEs",
                    ctes.len()
                );
                for (i, cte) in ctes.iter().enumerate() {
                    println!("DEBUG CTE EXTRACTION SHORT: CTE {}: {}", i, cte.cte_name);
                }
                if !ctes.is_empty() {
                    log::info!(
                        "🔧 Extracted {} CTEs in simple relationship render",
                        ctes.len()
                    );
                }
                ctes
            }
            Err(e) => {
                println!("DEBUG CTE EXTRACTION SHORT: Failed with error: {:?}", e);
                log::debug!("CTE extraction returned error (may be expected): {:?}", e);
                vec![]
            }
        };

        // 🔧 CTE JOIN GENERATION
        // If we extracted CTEs, check if they need to be joined to the main query
        // This handles WITH...MATCH patterns where WITH exports variables used in subsequent MATCH
        let mut cte_joins = vec![];
        if !extracted_ctes.is_empty() {
            // Extract CTE references to see which aliases map to which CTEs
            let cte_references = extract_cte_references(self);
            log::info!(
                "🔧 CTE JOIN: Found {} CTE references: {:?}",
                cte_references.len(),
                cte_references
            );
            log::info!("🔧 CTE JOIN: final_filters = {:?}", final_filters);

            // Extract correlation predicates from logical plan (from CartesianJoinExtraction optimizer)
            let correlation_predicates = extract_correlation_predicates(self);
            log::info!(
                "🔧 CTE JOIN: Found {} correlation predicates from optimizer",
                correlation_predicates.len()
            );

            // Convert correlation predicates to join conditions
            let mut join_conditions =
                convert_correlation_predicates_to_joins(&correlation_predicates, &cte_references);

            // Also extract join conditions from WHERE clause filters as fallback
            // This finds equality comparisons like: WHERE src2.ip = source_ip
            let filter_conditions = extract_cte_join_conditions(&final_filters, &cte_references);
            join_conditions.extend(filter_conditions);

            // Build map of CTE name → join conditions
            let mut cte_join_map: std::collections::HashMap<String, Vec<(String, String, String)>> =
                std::collections::HashMap::new();

            for (cte_name, cte_column, main_table_alias, main_column) in join_conditions {
                cte_join_map.entry(cte_name).or_insert_with(Vec::new).push((
                    cte_column,
                    main_table_alias,
                    main_column,
                ));
            }

            // Only use heuristic inference if no join conditions found at all
            if cte_join_map.is_empty() && !extracted_ctes.is_empty() {
                log::warn!("⚠️ CTE JOIN: No join conditions from optimizer or filters - falling back to heuristic (should not happen in production)");

                // Collect CTE column names from the CTE's SELECT items
                for cte in &extracted_ctes {
                    if let CteContent::Structured(ref cte_plan) = cte.content {
                        let cte_columns: Vec<String> = cte_plan
                            .select
                            .items
                            .iter()
                            .filter_map(|item| item.col_alias.clone().map(|a| a.0))
                            .collect();

                        log::info!(
                            "🔧 CTE JOIN: CTE '{}' exports columns: {:?}",
                            cte.cte_name,
                            cte_columns
                        );
                        log::info!(
                            "🔧 CTE JOIN: Main SELECT items: {:?}",
                            final_select_items
                                .iter()
                                .map(|item| format!("{:?}: {:?}", item.col_alias, item.expression))
                                .collect::<Vec<_>>()
                        );

                        // Check which CTE columns are referenced in main SELECT items
                        // CTE columns can appear as either ColumnAlias or TableAlias in SELECT
                        let mut used_cte_cols = vec![];
                        for select_item in &final_select_items {
                            match &select_item.expression {
                                RenderExpr::ColumnAlias(col) if cte_columns.contains(&col.0) => {
                                    used_cte_cols.push(col.0.clone());
                                    log::info!("🔧 CTE JOIN: SELECT references CTE column (ColumnAlias): {}", col.0);
                                }
                                RenderExpr::TableAlias(tbl_alias)
                                    if cte_columns.contains(&tbl_alias.0) =>
                                {
                                    used_cte_cols.push(tbl_alias.0.clone());
                                    log::info!("🔧 CTE JOIN: SELECT references CTE column (TableAlias): {}", tbl_alias.0);
                                }
                                _ => {}
                            }
                        }

                        if !used_cte_cols.is_empty() {
                            log::info!(
                                "🔧 CTE JOIN: Found {} CTE columns used in SELECT: {:?}",
                                used_cte_cols.len(),
                                used_cte_cols
                            );

                            // Try to infer join column from CTE's internal query
                            // Look at the CTE's FROM table and its first ID-like column
                            if let Some(ref cte_from_table) = cte_plan.from.0 {
                                // Get the main query's FROM table
                                if let Some(ref main_from) = final_from {
                                    if let Some(ref main_table) = main_from.table {
                                        let main_alias = main_table
                                            .alias
                                            .clone()
                                            .unwrap_or_else(|| main_table.name.clone());

                                        // Infer: Both queries reference IP nodes, likely joining on IP column
                                        // Use the first used CTE column as the join column
                                        if let Some(cte_col) = used_cte_cols.first() {
                                            // Heuristic: If column name contains "ip" or "id", use it for join
                                            // Otherwise, assume it correlates with a matching column in main table
                                            let main_col = if cte_col.contains("ip") {
                                                // Try to get node schema and extract ID column
                                                let _table_name = cte_from_table
                                                    .name
                                                    .split('.')
                                                    .last()
                                                    .unwrap_or("unknown");

                                                // For Zeek IP nodes, the ID column is the IP address column itself
                                                // In this schema, dns_log uses orig_h, conn_log also uses orig_h
                                                "orig_h".to_string()
                                            } else {
                                                cte_col.clone()
                                            };

                                            log::info!("🔧 CTE JOIN: Inferred join condition: {}.{} = {}.{}",
                                                       main_alias, main_col, cte.cte_name, cte_col);

                                            cte_join_map
                                                .entry(cte.cte_name.clone())
                                                .or_insert_with(Vec::new)
                                                .push((
                                                    cte_col.clone(),
                                                    main_alias.clone(),
                                                    main_col,
                                                ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // For each CTE, generate a JOIN with the extracted or inferred conditions
            for cte in &extracted_ctes {
                let cte_alias = cte.cte_name.clone();

                // Get join conditions for this CTE
                let join_condition = if let Some(conditions) = cte_join_map.get(&cte.cte_name) {
                    // Generate ON clause from extracted conditions
                    let mut join_ops = vec![];
                    for (cte_col, main_alias, main_col) in conditions {
                        // CRITICAL FIX: Check if main_alias is from a CTE
                        // If so, we need to use the CTE's table alias and qualified column name
                        let (resolved_table_alias, resolved_column) =
                            if let Some(ref_cte_name) = cte_references.get(main_alias) {
                                // main_alias (e.g., "b") is from a CTE - resolve to CTE format
                                // Calculate CTE alias: "with_a_b_cte_1" -> "a_b"
                                // Strategy: strip "with_" prefix, then strip "_cte" or "_cte_N" suffix
                                let after_prefix: &str = ref_cte_name
                                    .strip_prefix("with_")
                                    .unwrap_or(ref_cte_name.as_str());
                                let ref_cte_alias = after_prefix
                                    .strip_suffix("_cte")
                                    .or_else(|| after_prefix.strip_suffix("_cte_1"))
                                    .or_else(|| after_prefix.strip_suffix("_cte_2"))
                                    .or_else(|| after_prefix.strip_suffix("_cte_3"))
                                    .unwrap_or(after_prefix);

                                // Column name in CTE: alias_column (e.g., "b_user_id")
                                let cte_column = format!("{}_{}", main_alias, main_col);

                                log::info!(
                                "🔧 CTE JOIN: Resolved '{}' -> CTE '{}' (alias '{}'), column '{}'",
                                main_alias,
                                ref_cte_name,
                                ref_cte_alias,
                                cte_column
                            );

                                (ref_cte_alias.to_string(), cte_column)
                            } else {
                                // Not from CTE, use as-is
                                (main_alias.clone(), main_col.clone())
                            };

                        join_ops.push(OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(resolved_table_alias),
                                    column: PropertyValue::Column(resolved_column),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.clone()),
                                    column: PropertyValue::Column(cte_col.clone()),
                                }),
                            ],
                        });
                    }

                    log::info!(
                        "🔧 CTE JOIN: Generated {} join conditions for CTE '{}'",
                        join_ops.len(),
                        cte.cte_name
                    );
                    join_ops
                } else {
                    log::warn!("⚠️ CTE JOIN: No join conditions found for CTE '{}' - CTE may be unreferenced",
                               cte.cte_name);
                    vec![]
                };

                if !join_condition.is_empty() {
                    cte_joins.push(Join {
                        table_name: cte.cte_name.clone(),
                        table_alias: cte_alias,
                        joining_on: join_condition,
                        join_type: JoinType::Inner,
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                    });
                }
            }
        }

        Ok(RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems {
                items: final_select_items,
                distinct,
            },
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems({
                let mut all_joins = sorted_joins; // CRITICAL FIX: Use sorted_joins instead of filtered_joins
                all_joins.extend(cte_joins);
                all_joins
            }),
            array_join: ArrayJoinItem(self.extract_array_join()?),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(self.extract_group_by()?),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(self.extract_order_by()?),
            skip: SkipItem(self.extract_skip()),
            limit: LimitItem(self.extract_limit()),
            union: UnionItems(None),
        })
    }

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
        // Log what plan we receive
        let cte_refs = count_with_cte_refs(self);
        eprintln!(
            "🚨🚨🚨 to_render_plan ENTRY: WITH clauses cte_references: {:?} 🚨🚨🚨",
            cte_refs
        );

        // CRITICAL: Apply alias transformation BEFORE rendering
        // This rewrites denormalized node aliases to use relationship table aliases
        let transformed_plan = {
            use crate::render_plan::alias_resolver::AliasResolverContext;
            let alias_context = AliasResolverContext::from_logical_plan(self);
            alias_context.transform_plan(self.clone())
        };

        let cte_refs_after = count_with_cte_refs(&transformed_plan);
        eprintln!(
            "🚨🚨🚨 to_render_plan AFTER TRANSFORM: WITH clauses cte_references: {:?} 🚨🚨🚨",
            cte_refs_after
        );

        // Special case for PageRank - it generates complete SQL directly
        if let LogicalPlan::PageRank(_pagerank) = &transformed_plan {
            // For PageRank, we create a minimal RenderPlan that will be handled specially
            // The actual SQL generation happens in the server handler
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items: vec![],
                    distinct: false,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(Vec::new()),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
            });
        }

        // NEW ARCHITECTURE: Prioritize JOINs over CTEs
        // Only use CTEs for variable-length paths and complex cases
        // Try to build a simple JOIN-based plan first

        // ⚠️ EXCEPTION: Skip JOIN-based plan for multi-type VLP
        // Multi-type VLP requires CTE with UNION ALL, cannot use simple JOINs
        if has_multi_type_vlp(&transformed_plan, schema) {
            log::info!(
                "🎯 Detected multi-type VLP - skipping try_build_join_based_plan, using CTE logic"
            );
        } else {
            crate::debug_println!("DEBUG: Trying try_build_join_based_plan");
            log::info!("🚀🚀🚀 Trying try_build_join_based_plan for query");
            match transformed_plan.try_build_join_based_plan(schema) {
                Ok(plan) => {
                    crate::debug_println!("DEBUG: try_build_join_based_plan succeeded");
                    log::info!("✅ try_build_join_based_plan SUCCEEDED - VLP endpoint JOIN code below will NOT run!");
                    return Ok(plan);
                }
                Err(e) => {
                    crate::debug_println!(
                        "DEBUG: try_build_join_based_plan failed: {:?}, falling back to CTE logic",
                        e
                    );
                    log::info!(
                        "❌ try_build_join_based_plan FAILED: {:?}, falling back to CTE logic",
                        e
                    );
                }
            }
        }

        // === NEW: Handle WITH+MATCH patterns ===
        // These patterns have nested Union/GraphJoins inside GraphRel.right that represent
        // the WITH clause output. We need to render this as a CTE and join to it.
        // For CHAINED WITH patterns (WITH...MATCH...WITH...MATCH), we need to process
        // each WITH clause iteratively until none remain.
        let has_with = has_with_clause_in_graph_rel(&transformed_plan);
        println!(
            "DEBUG: has_with_clause_in_graph_rel(&transformed_plan) = {}, plan type = {:?}",
            has_with,
            std::mem::discriminant(&transformed_plan)
        );
        if has_with {
            log::info!("🔧 Handling WITH+MATCH pattern with CTE generation");
            println!("DEBUG: CALLING build_chained_with_match_cte_plan from to_render_plan");
            return build_chained_with_match_cte_plan(&transformed_plan, schema, None);
        }

        // === Handle WITH+aggregation+MATCH patterns ===
        // These patterns have GroupBy inside GraphRel.right which contains aggregation from WITH clause
        // The aggregation must be materialized as a subquery before joining
        if has_with_aggregation_pattern(&transformed_plan) {
            println!("DEBUG: Building WITH+aggregation+MATCH CTE plan");
            return build_with_aggregation_match_cte_plan(&transformed_plan, schema);
        }

        // Variable-length paths are now supported via recursive CTE generation
        // Two-pass architecture:
        // 1. Analyze property requirements across the entire plan
        // 2. Generate CTEs with full context including required properties

        log::trace!(
            "Starting render plan generation for plan type: {}",
            match &transformed_plan {
                LogicalPlan::Empty => "Empty",
                LogicalPlan::ViewScan(_) => "ViewScan",
                LogicalPlan::GraphNode(_) => "GraphNode",
                LogicalPlan::GraphRel(_) => "GraphRel",
                LogicalPlan::Filter(_) => "Filter",
                LogicalPlan::Projection(_) => "Projection",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                LogicalPlan::GroupBy(_) => "GroupBy",
                LogicalPlan::OrderBy(_) => "OrderBy",
                LogicalPlan::Skip(_) => "Skip",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::Cte(_) => "Cte",
                LogicalPlan::Union(_) => "Union",
                LogicalPlan::PageRank(_) => "PageRank",
                LogicalPlan::Unwind(_) => "Unwind",
                LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                LogicalPlan::WithClause(_) => "WithClause",
            }
        );

        // First pass: analyze what properties are needed
        let mut context = analyze_property_requirements(&transformed_plan, schema);

        let mut extracted_ctes: Vec<Cte> = Vec::new();
        let mut final_from: Option<FromTable> = None;
        let mut final_filters: Option<RenderExpr>;

        let last_node_cte_opt = transformed_plan.extract_last_node_cte(schema)?;
        println!(
            "DEBUG: last_node_cte_opt = {:?}",
            last_node_cte_opt.as_ref().map(|c| &c.cte_name)
        );

        if let Some(last_node_cte) = last_node_cte_opt {
            println!("DEBUG: Has last_node_cte: {}", last_node_cte.cte_name);
            // Extract the last part after splitting by '_'
            // This handles both "prefix_alias" and "rel_left_right" formats
            let parts: Vec<&str> = last_node_cte.cte_name.split('_').collect();
            let last_node_alias = parts.last().ok_or(RenderBuildError::MalformedCTEName)?;

            // Second pass: generate CTEs with full context
            println!(
                "DEBUG: About to call extract_ctes_with_context with last_node_alias={}",
                last_node_alias
            );
            extracted_ctes.extend(transformed_plan.extract_ctes_with_context(
                last_node_alias,
                &mut context,
                schema,
            )?);
            println!("DEBUG: Extracted {} CTEs", extracted_ctes.len());
            for (i, cte) in extracted_ctes.iter().enumerate() {
                println!("  DEBUG CTE[{}]: {}", i, cte.cte_name);
            }

            // Check if we have a variable-length CTE (it will be a recursive RawSql CTE)
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                let is_recursive = cte.is_recursive;
                let is_raw_sql = matches!(&cte.content, super::CteContent::RawSql(_));
                is_recursive && is_raw_sql
            });

            if has_variable_length_cte {
                // For variable-length paths, we need to handle OPTIONAL MATCH specially:
                // - Required VLP: FROM cte AS t JOIN users AS a ...
                // - Optional VLP: FROM users AS a LEFT JOIN cte AS t ... (preserves anchor when no paths)
                let var_len_cte = extracted_ctes
                    .iter()
                    .find(|cte| cte.is_recursive)
                    .expect("Variable-length CTE should exist");

                let vlp_is_optional = is_variable_length_optional(&transformed_plan);

                if vlp_is_optional {
                    // OPTIONAL MATCH with VLP: Use the ANCHOR NODE (from required MATCH) as FROM,
                    // then LEFT JOIN the VLP CTE.
                    // For: MATCH (person:Person) OPTIONAL MATCH (person)<-...-(message)-[:REL*0..]->(post)
                    // SQL should be: FROM Person LEFT JOIN vlp_cte ... (NOT FROM vlp_cte)

                    // Extract the base FROM (anchor node from required MATCH)
                    let base_from = transformed_plan.extract_from()?;
                    if base_from.is_some() {
                        final_from = base_from;
                        log::info!(
                            "🎯 OPTIONAL VLP: Using anchor node as FROM (from required MATCH), VLP CTE {} will be LEFT JOINed",
                            var_len_cte.cte_name
                        );
                    } else {
                        // No base FROM found (shouldn't happen), use VLP start node as fallback
                        if let Some((start_alias, _)) = has_variable_length_rel(&transformed_plan) {
                            if let Some(ref info) =
                                get_variable_length_denorm_info(&transformed_plan)
                            {
                                if let Some(start_table) = &info.start_table {
                                    final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(
                                            crate::query_planner::logical_plan::LogicalPlan::Empty,
                                        ),
                                        name: start_table.clone(),
                                        alias: Some(start_alias.clone()),
                                        use_final: false,
                                    })));
                                    log::warn!(
                                        "🎯 OPTIONAL VLP: No base FROM found, using VLP start node {} AS {} as fallback",
                                        start_table, start_alias
                                    );
                                }
                            }
                        }
                        if final_from.is_none() {
                            log::warn!("OPTIONAL VLP: Could not determine anchor node, falling back to CTE as FROM");
                            let vlp_alias = var_len_cte
                                .cte_name
                                .replace("vlp_cte", "vlp")
                                .replace("chained_path_", "vlp");
                            final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(
                                    crate::query_planner::logical_plan::LogicalPlan::Empty,
                                ),
                                name: var_len_cte.cte_name.clone(),
                                alias: Some(vlp_alias),
                                use_final: false,
                            })));
                        }
                    }
                    // VLP alias computed locally where needed
                } else {
                    // Required VLP: Use CTE as FROM (original behavior)
                    let vlp_alias = var_len_cte
                        .cte_name
                        .replace("vlp_cte", "vlp")
                        .replace("chained_path_", "vlp");
                    final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                        source: std::sync::Arc::new(
                            crate::query_planner::logical_plan::LogicalPlan::Empty,
                        ),
                        name: var_len_cte.cte_name.clone(),
                        alias: Some(vlp_alias),
                        use_final: false,
                    })));
                }

                // Note: End node filters are now applied inside the CTE, not in outer query.
                // Chained pattern filters (nodes outside VLP) are handled later via references_only_vlp_aliases.
                final_filters = None; // Will be populated by chained pattern logic below if needed
            } else {
                // Extract from the CTE content (normal path)
                let (cte_from, cte_filters) = match &last_node_cte.content {
                    super::CteContent::Structured(plan) => {
                        (plan.from.0.clone(), plan.filters.0.clone())
                    }
                    super::CteContent::RawSql(_) => (None, None), // Raw SQL CTEs don't have structured access
                };

                final_from = view_ref_to_from_table(cte_from);

                let last_node_filters_opt = clean_last_node_filters(cte_filters);

                let final_filters_opt = transformed_plan.extract_final_filters()?;

                let final_combined_filters = if let (Some(final_filters), Some(last_node_filters)) =
                    (&final_filters_opt, &last_node_filters_opt)
                {
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![final_filters.clone(), last_node_filters.clone()],
                    }))
                } else if final_filters_opt.is_some() {
                    final_filters_opt
                } else if last_node_filters_opt.is_some() {
                    last_node_filters_opt
                } else {
                    None
                };

                final_filters = final_combined_filters;
            }
        } else {
            println!("DEBUG: No last_node_cte, taking else branch");
            println!(
                "DEBUG: transformed_plan type = {:?}",
                std::mem::discriminant(&transformed_plan)
            );
            // No CTE wrapper, but check for variable-length paths which generate CTEs directly
            // Extract CTEs with a dummy alias and context (variable-length doesn't use the alias)
            println!("DEBUG: About to call extract_ctes_with_context in else branch");
            extracted_ctes.extend(transformed_plan.extract_ctes_with_context(
                "_",
                &mut context,
                schema,
            )?);
            println!("DEBUG: else branch extracted {} CTEs", extracted_ctes.len());

            // Check if we have a variable-length CTE (recursive or chained join)
            // Both types use RawSql content and need special FROM clause handling
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                matches!(&cte.content, super::CteContent::RawSql(_))
                    && (cte.cte_name.starts_with("vlp_")
                        || cte.cte_name.starts_with("chained_path_"))
            });

            if has_variable_length_cte {
                // For variable-length paths, handle OPTIONAL MATCH specially
                let var_len_cte = extracted_ctes
                    .iter()
                    .find(|cte| {
                        cte.cte_name.starts_with("vlp_")
                            || cte.cte_name.starts_with("chained_path_")
                    })
                    .expect("Variable-length CTE should exist");

                let vlp_is_optional = is_variable_length_optional(&transformed_plan);

                if vlp_is_optional {
                    // OPTIONAL MATCH with VLP: Use the ANCHOR NODE (from required MATCH) as FROM
                    // Extract the base FROM first
                    let base_from = transformed_plan.extract_from()?;
                    if base_from.is_some() {
                        final_from = base_from;
                        log::info!(
                            "🎯 OPTIONAL VLP (no wrapper): Using anchor node as FROM (from required MATCH), VLP CTE {} will be LEFT JOINed",
                            var_len_cte.cte_name
                        );
                    } else {
                        // No base FROM, try VLP start node as fallback
                        if let Some((start_alias, _)) = has_variable_length_rel(&transformed_plan) {
                            if let Some(ref info) =
                                get_variable_length_denorm_info(&transformed_plan)
                            {
                                if let Some(start_table) = &info.start_table {
                                    final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(
                                            crate::query_planner::logical_plan::LogicalPlan::Empty,
                                        ),
                                        name: start_table.clone(),
                                        alias: Some(start_alias.clone()),
                                        use_final: false,
                                    })));
                                    log::warn!(
                                        "🎯 OPTIONAL VLP (no wrapper): No base FROM, using VLP start node {} AS {} as fallback",
                                        start_table, start_alias
                                    );
                                }
                            }
                        }
                        if final_from.is_none() {
                            log::warn!("OPTIONAL VLP (no wrapper): Could not determine anchor node, falling back to CTE as FROM");
                            let vlp_alias = var_len_cte
                                .cte_name
                                .replace("vlp_cte", "vlp")
                                .replace("chained_path_", "vlp");
                            final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                                source: std::sync::Arc::new(
                                    crate::query_planner::logical_plan::LogicalPlan::Empty,
                                ),
                                name: var_len_cte.cte_name.clone(),
                                alias: Some(vlp_alias),
                                use_final: false,
                            })));
                        }
                    }
                } else {
                    // Required VLP: Use CTE as FROM
                    let vlp_alias = var_len_cte
                        .cte_name
                        .replace("vlp_cte", "vlp")
                        .replace("chained_path_", "vlp");
                    final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                        source: std::sync::Arc::new(
                            crate::query_planner::logical_plan::LogicalPlan::Empty,
                        ),
                        name: var_len_cte.cte_name.clone(),
                        alias: Some(vlp_alias),
                        use_final: false,
                    })));
                }

                // For variable-length paths, apply schema filters in the outer query
                // The outer query JOINs to base tables (users_bench AS u, users_bench AS v)
                // so we need schema filters on those base table JOINs
                if let Some((start_alias, end_alias)) = has_variable_length_rel(self) {
                    let mut filter_parts: Vec<RenderExpr> = Vec::new();

                    // For OPTIONAL MATCH VLP, we also need the start node filter in the outer query
                    // The filter is pushed into the CTE for performance, but we also need it
                    // in the outer query to filter the anchor node (since FROM is the anchor node)
                    if vlp_is_optional {
                        // Extract the where_predicate from the GraphRel (start node filter)
                        fn extract_start_filter_for_outer_query(
                            plan: &LogicalPlan,
                        ) -> Option<RenderExpr> {
                            match plan {
                                LogicalPlan::GraphRel(gr) => {
                                    // Use the where_predicate as the start filter
                                    if let Some(ref predicate) = gr.where_predicate {
                                        RenderExpr::try_from(predicate.clone()).ok()
                                    } else {
                                        None
                                    }
                                }
                                LogicalPlan::Projection(p) => {
                                    extract_start_filter_for_outer_query(&p.input)
                                }
                                LogicalPlan::Filter(f) => {
                                    // Also check Filter for where clause
                                    if let Ok(expr) = RenderExpr::try_from(f.predicate.clone()) {
                                        Some(expr)
                                    } else {
                                        extract_start_filter_for_outer_query(&f.input)
                                    }
                                }
                                LogicalPlan::GraphJoins(gj) => {
                                    extract_start_filter_for_outer_query(&gj.input)
                                }
                                LogicalPlan::GroupBy(gb) => {
                                    extract_start_filter_for_outer_query(&gb.input)
                                }
                                LogicalPlan::Limit(l) => {
                                    extract_start_filter_for_outer_query(&l.input)
                                }
                                LogicalPlan::OrderBy(o) => {
                                    extract_start_filter_for_outer_query(&o.input)
                                }
                                _ => None,
                            }
                        }

                        if let Some(start_filter) =
                            extract_start_filter_for_outer_query(&transformed_plan)
                        {
                            log::debug!("OPTIONAL VLP: Adding start node filter to outer query");
                            filter_parts.push(start_filter);
                        }
                    }

                    // Note: End node filters are applied inside the CTE, not rewritten to outer query.
                    // The var_len_cte_alias is still needed for schema filters below.
                    let var_len_cte_alias = var_len_cte
                        .cte_name
                        .replace("vlp_cte", "vlp")
                        .replace("chained_path_", "vlp");
                    let _ = var_len_cte_alias; // Silence unused warning - used in collect_schema_filter_for_alias

                    // Helper to extract schema filter from ViewScan for a given alias
                    fn collect_schema_filter_for_alias(
                        plan: &LogicalPlan,
                        target_alias: &str,
                    ) -> Option<String> {
                        match plan {
                            LogicalPlan::GraphRel(gr) => {
                                // Check right side for end node
                                if gr.right_connection == target_alias {
                                    if let LogicalPlan::GraphNode(gn) = gr.right.as_ref() {
                                        if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                                            if let Some(ref sf) = vs.schema_filter {
                                                return sf.to_sql(target_alias).ok();
                                            }
                                        }
                                    }
                                }
                                // Also check left side
                                if gr.left_connection == target_alias {
                                    if let LogicalPlan::GraphNode(gn) = gr.left.as_ref() {
                                        if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                                            if let Some(ref sf) = vs.schema_filter {
                                                return sf.to_sql(target_alias).ok();
                                            }
                                        }
                                    }
                                }
                                // Recurse into children
                                collect_schema_filter_for_alias(&gr.left, target_alias).or_else(
                                    || collect_schema_filter_for_alias(&gr.right, target_alias),
                                )
                            }
                            LogicalPlan::GraphNode(gn) => {
                                collect_schema_filter_for_alias(&gn.input, target_alias)
                            }
                            LogicalPlan::Filter(f) => {
                                collect_schema_filter_for_alias(&f.input, target_alias)
                            }
                            LogicalPlan::Projection(p) => {
                                collect_schema_filter_for_alias(&p.input, target_alias)
                            }
                            LogicalPlan::GraphJoins(gj) => {
                                collect_schema_filter_for_alias(&gj.input, target_alias)
                            }
                            LogicalPlan::Limit(l) => {
                                collect_schema_filter_for_alias(&l.input, target_alias)
                            }
                            _ => None,
                        }
                    }

                    // Get start node schema filter (for JOIN to start node base table)
                    if let Some(schema_sql) = collect_schema_filter_for_alias(self, &start_alias) {
                        log::info!(
                            "VLP outer query: Adding schema filter for start node '{}': {}",
                            start_alias,
                            schema_sql
                        );
                        filter_parts.push(RenderExpr::Raw(format!("({})", schema_sql)));
                    }

                    // Get end node schema filter (for JOIN to end node base table)
                    if let Some(schema_sql) = collect_schema_filter_for_alias(self, &end_alias) {
                        log::info!(
                            "VLP outer query: Adding schema filter for end node '{}': {}",
                            end_alias,
                            schema_sql
                        );
                        filter_parts.push(RenderExpr::Raw(format!("({})", schema_sql)));
                    }

                    // 🎯 FIX Issue #5: Add user-defined filters on CHAINED PATTERN nodes
                    // For queries like (u)-[*]->(g)-[:REL]->(f) WHERE f.sensitive_data = 1
                    // The filter on 'f' should go into the final WHERE clause, not the CTE.
                    // Extract all user filters, then exclude VLP start/end/relationship filters (already in CTE).
                    if let Ok(Some(all_user_filters)) = transformed_plan.extract_filters() {
                        // 🔧 HOLISTIC FIX: Get all VLP aliases including relationship alias
                        let vlp_rel_alias =
                            get_variable_length_aliases(self).map(|(_, _, rel_alias)| rel_alias);

                        // Helper to check if expression references ONLY VLP aliases (start, end, or relationship)
                        fn references_only_vlp_aliases(
                            expr: &RenderExpr,
                            start_alias: &str,
                            end_alias: &str,
                            rel_alias: Option<&str>,
                        ) -> bool {
                            fn collect_aliases(
                                expr: &RenderExpr,
                                aliases: &mut std::collections::HashSet<String>,
                            ) {
                                match expr {
                                    RenderExpr::PropertyAccessExp(prop) => {
                                        aliases.insert(prop.table_alias.0.clone());
                                    }
                                    RenderExpr::OperatorApplicationExp(op) => {
                                        for operand in &op.operands {
                                            collect_aliases(operand, aliases);
                                        }
                                    }
                                    RenderExpr::ScalarFnCall(fn_call) => {
                                        for arg in &fn_call.args {
                                            collect_aliases(arg, aliases);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            let mut aliases = std::collections::HashSet::new();
                            collect_aliases(expr, &mut aliases);
                            // Returns true if ALL referenced aliases are VLP-related (start, end, or relationship)
                            !aliases.is_empty()
                                && aliases.iter().all(|a| {
                                    a == start_alias
                                        || a == end_alias
                                        || rel_alias.map(|r| a == r).unwrap_or(false)
                                })
                        }

                        // Split AND-connected filters
                        fn split_and_filters(expr: RenderExpr) -> Vec<RenderExpr> {
                            match expr {
                                RenderExpr::OperatorApplicationExp(op)
                                    if matches!(op.operator, Operator::And) =>
                                {
                                    let mut result = Vec::new();
                                    for operand in op.operands {
                                        result.extend(split_and_filters(operand));
                                    }
                                    result
                                }
                                _ => vec![expr],
                            }
                        }

                        let all_filters = split_and_filters(all_user_filters);
                        for filter in all_filters {
                            // Include filter if it references nodes OUTSIDE the VLP (chained pattern nodes)
                            // 🔧 HOLISTIC FIX: Now also checks relationship alias to avoid duplicating VLP filters
                            if !references_only_vlp_aliases(
                                &filter,
                                &start_alias,
                                &end_alias,
                                vlp_rel_alias.as_deref(),
                            ) {
                                log::info!(
                                    "VLP outer query: Adding chained-pattern filter: {:?}",
                                    filter
                                );
                                filter_parts.push(filter);
                            } else {
                                log::debug!("VLP outer query: Skipping VLP-only filter (already in CTE): {:?}", filter);
                            }
                        }
                    }

                    // Combine all filters with AND
                    final_filters = if filter_parts.is_empty() {
                        None
                    } else if filter_parts.len() == 1 {
                        Some(filter_parts.into_iter().next().unwrap())
                    } else {
                        Some(
                            filter_parts
                                .into_iter()
                                .reduce(|acc, f| {
                                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                                        operator: Operator::And,
                                        operands: vec![acc, f],
                                    })
                                })
                                .unwrap(),
                        )
                    };
                } else {
                    final_filters = None;
                }
            } else {
                // Check if we have a polymorphic/multi-relationship CTE (starts with "rel_")
                let has_polymorphic_cte = extracted_ctes
                    .iter()
                    .any(|cte| cte.cte_name.starts_with("rel_"));

                if has_polymorphic_cte {
                    // For polymorphic edge CTEs, find a labeled node to use as FROM
                    // This handles MATCH (u:User)-[r]->(target) where target is $any
                    log::info!("🎯 POLYMORPHIC CTE: Looking for labeled node as FROM");

                    // For polymorphic edges, ALWAYS find the leftmost ViewScan node first
                    // because extract_from() may return a CTE placeholder instead
                    fn find_leftmost_viewscan_node(
                        plan: &LogicalPlan,
                    ) -> Option<&super::super::query_planner::logical_plan::GraphNode>
                    {
                        match plan {
                            LogicalPlan::GraphNode(gn) => {
                                if matches!(gn.input.as_ref(), LogicalPlan::ViewScan(_)) {
                                    return Some(gn);
                                }
                                None
                            }
                            LogicalPlan::GraphRel(gr) => {
                                // Prefer left (from) node first - recurse into left branch
                                if let Some(node) = find_leftmost_viewscan_node(&gr.left) {
                                    return Some(node);
                                }
                                // Check if left is a GraphNode with ViewScan
                                if let LogicalPlan::GraphNode(left_node) = gr.left.as_ref() {
                                    if matches!(left_node.input.as_ref(), LogicalPlan::ViewScan(_))
                                        && !left_node.is_denormalized
                                    {
                                        return Some(left_node);
                                    }
                                }
                                // Then try right node
                                if let LogicalPlan::GraphNode(right_node) = gr.right.as_ref() {
                                    if matches!(right_node.input.as_ref(), LogicalPlan::ViewScan(_))
                                        && !right_node.is_denormalized
                                    {
                                        return Some(right_node);
                                    }
                                }
                                // Recurse into right
                                find_leftmost_viewscan_node(&gr.right)
                            }
                            LogicalPlan::Filter(f) => find_leftmost_viewscan_node(&f.input),
                            LogicalPlan::Projection(p) => find_leftmost_viewscan_node(&p.input),
                            LogicalPlan::GraphJoins(gj) => find_leftmost_viewscan_node(&gj.input),
                            LogicalPlan::Limit(l) => find_leftmost_viewscan_node(&l.input),
                            LogicalPlan::OrderBy(o) => find_leftmost_viewscan_node(&o.input),
                            LogicalPlan::Skip(s) => find_leftmost_viewscan_node(&s.input),
                            _ => None,
                        }
                    }

                    // Find the leftmost ViewScan node for FROM
                    if let Some(graph_node) = find_leftmost_viewscan_node(&transformed_plan) {
                        if let LogicalPlan::ViewScan(vs) = graph_node.input.as_ref() {
                            log::info!(
                                "🎯 POLYMORPHIC: Using leftmost node '{}' with table '{}' as FROM",
                                graph_node.alias,
                                vs.source_table
                            );
                            final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                                source: graph_node.input.clone(),
                                name: vs.source_table.clone(),
                                alias: Some(graph_node.alias.clone()),
                                use_final: vs.use_final,
                            })));
                        }
                    }

                    // Fallback to extract_from if find_leftmost failed
                    if final_from.is_none() {
                        final_from = transformed_plan.extract_from()?;
                    }

                    final_filters = transformed_plan.extract_filters()?;
                } else {
                    // Normal case: no CTEs, extract FROM, joins, and filters normally
                    final_from = transformed_plan.extract_from()?;
                    final_filters = transformed_plan.extract_filters()?;
                }
            }
        }

        // Handle aggregation queries that don't have explicit FROM but need one
        // For queries like "MATCH (n) RETURN count(n)", we need to extract FROM from the plan
        let mut needs_aggregation_transform = false;

        let mut final_select_items = transformed_plan.extract_select_items()?;

        // If we created a node counts CTE for aggregation, transform count() to sum()
        if needs_aggregation_transform {
            for item in &mut final_select_items {
                if let RenderExpr::AggregateFnCall(ref mut agg) = item.expression {
                    if agg.name == "count" {
                        // Replace count(n) with sum(cnt)
                        agg.name = "sum".to_string();
                        agg.args = vec![RenderExpr::Column(Column(
                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                "cnt".to_string(),
                            ),
                        ))];
                    }
                }
            }
        }

        // For all denormalized patterns, apply property mapping to remap node aliases to edge aliases
        // This ensures SELECT src."id.orig_h" becomes SELECT ad62047b83."id.orig_h" when src is denormalized on edge ad62047b83
        for item in &mut final_select_items {
            apply_property_mapping_to_expr(&mut item.expression, &transformed_plan);
        }

        // For denormalized variable-length paths, rewrite SELECT items to reference CTE columns
        // Standard patterns keep a.*, b.* since they JOIN to node tables
        // Denormalized patterns need t.start_id, t.end_id since there are no node table JOINs
        // Mixed patterns: rewrite only the denormalized side
        if let Some((start_alias, end_alias)) = has_variable_length_rel(&transformed_plan) {
            let denorm_info = get_variable_length_denorm_info(&transformed_plan);
            let is_any_denormalized = denorm_info
                .as_ref()
                .map_or(false, |d| d.is_any_denormalized());
            let needs_cte = if let Some(spec) = get_variable_length_spec(&transformed_plan) {
                spec.exact_hop_count().is_none()
                    || get_shortest_path_mode(&transformed_plan).is_some()
            } else {
                false
            };

            if is_any_denormalized && needs_cte {
                // Get relationship info for rewriting f.Origin → t.start_id, f.Dest → t.end_id
                let rel_info = get_variable_length_rel_info(&transformed_plan);
                let path_var = get_path_variable(&transformed_plan);
                let start_is_denorm = denorm_info
                    .as_ref()
                    .map_or(false, |d| d.start_is_denormalized);
                let end_is_denorm = denorm_info
                    .as_ref()
                    .map_or(false, |d| d.end_is_denormalized);

                final_select_items = final_select_items
                    .into_iter()
                    .map(|item| {
                        // For mixed patterns, only rewrite the denormalized aliases
                        let rewritten = rewrite_expr_for_mixed_denormalized_cte(
                            &item.expression,
                            &start_alias,
                            &end_alias,
                            start_is_denorm,
                            end_is_denorm,
                            rel_info.as_ref().map(|r| r.rel_alias.as_str()),
                            rel_info.as_ref().map(|r| r.from_col.as_str()),
                            rel_info.as_ref().map(|r| r.to_col.as_str()),
                            path_var.as_deref(),
                        );
                        SelectItem {
                            expression: rewritten,
                            col_alias: item.col_alias,
                        }
                    })
                    .collect();
            } else if needs_cte {
                // For non-denormalized VLP: Rewrite projections from VLP internal aliases → Cypher aliases
                // VLP CTE uses start_node/end_node internally, but outer query JOINs use a/b
                // Problem: SELECT start_node.name FROM vlp_cte JOIN users AS a  ❌
                // Solution: SELECT a.name FROM vlp_cte JOIN users AS a         ✅
                log::info!("🔧 Non-denormalized VLP: Rewriting projections from VLP internal aliases to Cypher aliases");
                log::info!(
                    "   start_alias='{}' (VLP internal: 'start_node')",
                    start_alias
                );
                log::info!("   end_alias='{}' (VLP internal: 'end_node')", end_alias);

                log::debug!(
                    "📝 BEFORE rewrite: {} select items",
                    final_select_items.len()
                );
                for (idx, item) in final_select_items.iter().enumerate() {
                    if let RenderExpr::PropertyAccessExp(prop) = &item.expression {
                        log::debug!(
                            "  [{}] {}.{} AS {:?}",
                            idx,
                            prop.table_alias.0,
                            prop.column.raw(),
                            item.col_alias
                        );
                    }
                }

                final_select_items = final_select_items
                    .into_iter()
                    .map(|item| {
                        let rewritten = rewrite_vlp_internal_to_cypher_alias(
                            &item.expression,
                            &start_alias,
                            &end_alias,
                        );
                        SelectItem {
                            expression: rewritten,
                            col_alias: item.col_alias,
                        }
                    })
                    .collect();

                log::debug!(
                    "📝 AFTER rewrite: {} select items",
                    final_select_items.len()
                );
                for (idx, item) in final_select_items.iter().enumerate() {
                    if let RenderExpr::PropertyAccessExp(prop) = &item.expression {
                        log::debug!(
                            "  [{}] {}.{} AS {:?}",
                            idx,
                            prop.table_alias.0,
                            prop.column.raw(),
                            item.col_alias
                        );
                    }
                }
            }
        }

        let mut extracted_joins = transformed_plan.extract_joins(schema)?;

        log::info!("{}", "=".repeat(80));
        log::info!("🔍🔍🔍 CHECKING FOR VLP ENDPOINT JOINS");
        log::debug!(
            "🔍 Initial extracted_joins: {} total",
            extracted_joins.len()
        );
        for (idx, j) in extracted_joins.iter().enumerate() {
            log::debug!(
                "  [{}] {} AS {} (ON: {} conditions)",
                idx,
                j.table_name,
                j.table_alias,
                j.joining_on.len()
            );
        }

        // For variable-length paths, add joins to get full user data
        // FIX: Get VLP endpoint info from extracted CTEs (populated during CTE generation)
        log::info!(
            "🔍 Searching for VLP CTEs in {} extracted CTEs",
            extracted_ctes.len()
        );
        for (i, cte) in extracted_ctes.iter().enumerate() {
            log::info!("  CTE[{}]: {}", i, cte.cte_name);
        }

        let vlp_cte = extracted_ctes.iter().find(|cte| {
            cte.cte_name.starts_with("vlp_") || cte.cte_name.starts_with("chained_path_")
        });

        log::info!(
            "🔍 VLP CTE search result: {:?}",
            vlp_cte.map(|c| c.cte_name.as_str())
        );

        let has_vlp_cte = vlp_cte.is_some();

        log::info!(
            "🔍 Checking extracted_ctes ({} total) for VLP:",
            extracted_ctes.len()
        );
        for cte in &extracted_ctes {
            log::info!(
                "  - CTE: {} (recursive={}) vlp_start={:?}/{:?} vlp_end={:?}/{:?}",
                cte.cte_name,
                cte.is_recursive,
                cte.vlp_start_alias,
                cte.vlp_start_table,
                cte.vlp_end_alias,
                cte.vlp_end_table
            );
        }
        log::info!("🔍 has_vlp_cte = {}", has_vlp_cte);

        // Get VLP endpoint info from CTE (more reliable than searching plan)
        let vlp_aliases = vlp_cte.and_then(|cte| {
            if let (Some(start), Some(end)) = (&cte.vlp_start_alias, &cte.vlp_end_alias) {
                log::debug!(
                    "🎯 Got VLP aliases from CTE: start='{}', end='{}'",
                    start,
                    end
                );
                log::debug!(
                    "🎯 Got VLP tables from CTE: start_table={:?}, end_table={:?}",
                    cte.vlp_start_table,
                    cte.vlp_end_table
                );
                Some((start.clone(), end.clone()))
            } else {
                log::warn!("🎯 VLP CTE found but missing endpoint info!");
                None
            }
        });

        log::info!(
            "🔍 VLP detection: has_vlp_cte={}, vlp_aliases={:?}",
            has_vlp_cte,
            vlp_aliases
        );
        log::info!("{}", "=".repeat(80));

        // 🔧 CRITICAL: Check for WITH CTEs BEFORE any VLP endpoint processing
        // Strategy: Check if there's a WithClause in the logical plan tree
        log::warn!("🔍🔍🔍 CHECKING FOR WITH CTEs");

        // Check extracted_ctes first (for direct WITH in current plan)
        let has_with_cte_in_extracted = extracted_ctes
            .iter()
            .any(|cte| cte.cte_name.starts_with("with_"));
        log::warn!("  - extracted_ctes check: {}", has_with_cte_in_extracted);

        let has_with_clause_in_plan = has_with_clause_in_tree(&transformed_plan);
        log::warn!("  - plan tree check: {}", has_with_clause_in_plan);

        let has_with_cte = has_with_cte_in_extracted || has_with_clause_in_plan;
        log::warn!("🔍 FINAL has_with_cte = {}", has_with_cte);

        // Check if this is a multi-type VLP CTE (needs special handling - direct SELECT, no JOINs)
        let is_multi_type_vlp =
            vlp_cte.map_or(false, |cte| cte.cte_name.starts_with("vlp_multi_type_"));
        log::info!(
            "🎯 Multi-type VLP check: is_multi_type_vlp={}, vlp_cte={:?}",
            is_multi_type_vlp,
            vlp_cte.map(|c| c.cte_name.as_str())
        );

        if is_multi_type_vlp {
            log::info!("🎯 MULTI-TYPE VLP DETECTED - Skipping endpoint JOINs (CTE has all data)");
            // For multi-type VLP, don't add endpoint JOINs
            // The CTE already has everything: end_type, end_id (String), end_properties (JSON)
            // The normal RenderPlan construction below will handle it correctly
            // We just need to avoid the VLP endpoint JOIN generation code

            // ⚠️ CRITICAL: Clear extracted_joins to prevent any JOINs from being added
            // Multi-type VLP CTEs are self-contained - they have all the data we need
            let filtered_joins_count = extracted_joins.len();
            extracted_joins.clear();
            log::info!(
                "🎯 Multi-type VLP: Cleared {} extracted joins (not needed for multi-type VLP)",
                filtered_joins_count
            );

            // 🔧 CRITICAL FIX: Set FROM to use CTE with Cypher alias
            // This is THE ROOT CAUSE FIX for all alias mapping issues!
            //
            // Instead of complex rewriting logic, set the correct alias at the source:
            //   FROM vlp_multi_type_u_x AS x  (CTE name AS Cypher end alias)
            //
            // Then everything just works naturally:
            //   - x.end_type → CTE column (direct access)
            //   - x.name → property (SQL generator extracts from end_properties JSON)
            //
            if let Some(cte) = vlp_cte {
                if let Some(cypher_end_alias) = &cte.vlp_cypher_end_alias {
                    log::info!(
                        "🎯 Multi-type VLP: Setting FROM to CTE '{}' AS '{}'",
                        cte.cte_name,
                        cypher_end_alias
                    );
                    final_from = Some(FromTable::new(Some(ViewTableRef {
                        source: std::sync::Arc::new(LogicalPlan::Empty),
                        name: cte.cte_name.clone(),
                        alias: Some(cypher_end_alias.clone()), // ✨ Use Cypher alias, not CTE name!
                        use_final: false,
                    })));
                }
            }
        }

        // 🔧 CRITICAL FIX: Handle WITH CTE + VLP pattern
        // Pattern: MATCH (root)-[:KNOWS*1..2]-(friend) WITH DISTINCT friend MATCH (friend)<-[:HAS_CREATOR]-(post)
        // The WITH CTE (with_friend_cte_1) should be used as FROM for the second MATCH
        if has_with_cte && !is_multi_type_vlp {
            log::warn!("🔧🔧🔧 WITH CTE + VLP PATTERN DETECTED!");
            let with_cte = extracted_ctes
                .iter()
                .find(|cte| cte.cte_name.starts_with("with_"))
                .unwrap();
            log::warn!("   WITH CTE: {}", with_cte.cte_name);

            // Extract the WITH aliases from the CTE name
            // Format: with_friend_cte_1 → "friend"
            let with_alias_part = if let Some(stripped) = with_cte.cte_name.strip_prefix("with_") {
                if let Some(cte_pos) = stripped.rfind("_cte") {
                    &stripped[..cte_pos]
                } else {
                    stripped
                }
            } else {
                ""
            };

            log::warn!("   WITH CTE exports alias: '{}'", with_alias_part);

            // Override final_from to use the WITH CTE
            final_from = Some(FromTable::new(Some(ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: with_cte.cte_name.clone(),
                alias: Some(with_alias_part.to_string()),
                use_final: false,
            })));

            log::warn!(
                "   Set FROM to: {} AS '{}'",
                with_cte.cte_name,
                with_alias_part
            );
            log::warn!(
                "   Keeping {} extracted_joins (subsequent pattern JOINs)",
                extracted_joins.len()
            );
        } else if has_vlp_cte && vlp_aliases.is_some() && !is_multi_type_vlp {
            // Skip this entire block for multi-type VLP (handled above with extracted_joins.clear())
            let (start_alias, end_alias) = vlp_aliases.unwrap();
            log::debug!("🎯 ENTERING VLP ENDPOINT JOIN CREATION BLOCK");
            log::debug!(
                "🎯 start_alias='{}', end_alias='{}'",
                start_alias,
                end_alias
            );

            // 🔧 CRITICAL FIX: Check for WITH CTEs that should be used instead of VLP endpoint JOINs
            // Pattern: MATCH (root)-[:KNOWS*1..2]-(friend) WITH DISTINCT friend MATCH (friend)<-[:HAS_CREATOR]-(post)
            // Result: VLP creates vlp_cte1/vlp_cte2, WITH creates with_friend_cte_1
            // The second MATCH should use with_friend_cte_1 as FROM, not add JOINs to Person table!
            let with_cte = extracted_ctes
                .iter()
                .find(|cte| cte.cte_name.starts_with("with_"));

            if let Some(with_cte) = with_cte {
                log::warn!(
                    "🔧🔧🔧 DETECTED WITH CTE: {} (skipping VLP endpoint JOINs)",
                    with_cte.cte_name
                );
                log::warn!("   This CTE represents the output of MATCH+WITH, the second MATCH should use it!");

                // Extract the WITH aliases from the CTE name
                // Format: with_friend_cte_1 → "friend"
                let with_alias_part =
                    if let Some(stripped) = with_cte.cte_name.strip_prefix("with_") {
                        if let Some(cte_pos) = stripped.rfind("_cte") {
                            &stripped[..cte_pos]
                        } else {
                            stripped
                        }
                    } else {
                        ""
                    };

                log::warn!("   WITH CTE exports alias: '{}'", with_alias_part);

                // The WITH CTE should be used as FROM with the exported alias
                // Override the final_from that was set earlier
                final_from = Some(FromTable::new(Some(ViewTableRef {
                    source: std::sync::Arc::new(LogicalPlan::Empty),
                    name: with_cte.cte_name.clone(),
                    alias: Some(with_alias_part.to_string()),
                    use_final: false,
                })));

                log::warn!(
                    "   Set FROM to: {} AS '{}'",
                    with_cte.cte_name,
                    with_alias_part
                );

                // Don't add VLP endpoint JOINs - the WITH CTE already has them!
                // Just keep any subsequent pattern JOINs (like the HAS_CREATOR → Post JOIN)
                log::warn!(
                    "   Keeping {} subsequent pattern JOINs (not adding VLP endpoint JOINs)",
                    extracted_joins.len()
                );

                // Continue to the rest of the rendering (skip VLP endpoint JOIN generation)
                // Jump past the VLP endpoint JOIN code to line ~12300+ where the final RenderPlan is built
            } else {
                // No WITH CTE - normal VLP endpoint JOIN handling
                log::debug!(
                    "   No WITH CTE found, proceeding with normal VLP endpoint JOIN handling"
                );

                // ⚠️ COMPUTE VLP CTE ALIAS ONCE - use throughout this entire VLP handling section
                // Use PREFIX "vlp" instead of "t" to avoid collision with relationship aliases (t1, t2, ...)
                let vlp_cte_name = extracted_ctes
                    .iter()
                    .find(|cte| {
                        cte.cte_name.starts_with("vlp_")
                            || cte.cte_name.starts_with("chained_path_")
                    })
                    .map(|cte| cte.cte_name.clone())
                    .unwrap_or_else(|| "vlp_cte1".to_string()); // Fallback

                // Generate unique alias: vlp_cte7 → vlp7, vlp_cte12 → vlp12
                // Use "vlp" prefix to avoid collision with relationship aliases (t1, t2, ...)
                let vlp_alias = vlp_cte_name
                    .replace("vlp_cte", "vlp")
                    .replace("chained_path_", "vlp");

                log::debug!("🎯 VLP CTE: {} → alias '{}'", vlp_cte_name, vlp_alias);

                // Get the VLP relationship alias (e.g., "t2" for REPLY_OF)
                // We need to filter out JOINs that reference this alias
                let vlp_rel_alias =
                    get_variable_length_rel_info(&transformed_plan).map(|info| info.rel_alias);

                if let Some(ref rel_alias) = vlp_rel_alias {
                    log::debug!(
                        "🎯 VLP relationship alias: '{}' (will filter JOINs referencing it)",
                        rel_alias
                    );
                }

                // Check if this VLP is OPTIONAL (need this early for filtering logic)
                let vlp_is_optional = is_variable_length_optional(&transformed_plan);

                // Save subsequent pattern joins and filter invalid VLP-related JOINs
                // For OPTIONAL VLP: Keep ALL node JOINs (including start/end nodes) because they're needed
                // For REQUIRED VLP: Filter out start/end node JOINs (we'll add them manually with correct CTE references)
                let subsequent_joins: Vec<Join> = extracted_joins
                .drain(..)
                .filter(|j| {
                    // For OPTIONAL VLP: Only filter out JOINs that reference the VLP relationship alias
                    // Keep start/end node JOINs because they come from other relationships (like HAS_CREATOR)
                    if vlp_is_optional {
                        // Only filter JOINs that reference the VLP relationship alias
                        if let Some(ref vlp_rel) = vlp_rel_alias {
                            let references_vlp_rel = j.joining_on.iter().any(|op| {
                                expr_references_alias(&RenderExpr::OperatorApplicationExp(op.clone()), vlp_rel)
                            });
                            if references_vlp_rel {
                                log::debug!("🔧 OPTIONAL VLP: Filtering JOIN {} AS {} (references VLP relationship alias '{}')", j.table_name, j.table_alias, vlp_rel);
                                return false;
                            }
                        }
                        true // Keep all other JOINs for OPTIONAL VLP
                    } else {
                        // For REQUIRED VLP: Filter out endpoint node JOINs (we'll add them manually)
                        if j.table_alias == start_alias || j.table_alias == end_alias {
                            log::debug!("🔧 REQUIRED VLP: Filtering JOIN {} AS {} (endpoint node)", j.table_name, j.table_alias);
                            return false;
                        }

                        // Also filter out JOINs that reference the VLP relationship alias
                        if let Some(ref vlp_rel) = vlp_rel_alias {
                            let references_vlp_rel = j.joining_on.iter().any(|op| {
                                expr_references_alias(&RenderExpr::OperatorApplicationExp(op.clone()), vlp_rel)
                            });
                            if references_vlp_rel {
                                log::debug!("🔧 REQUIRED VLP: Filtering JOIN {} AS {} (references VLP relationship alias '{}')", j.table_name, j.table_alias, vlp_rel);
                                return false;
                            }
                        }
                        true
                    }
                })
                .collect();

                log::debug!(
                    "🔧 VLP CHAINED FIX: Preserved {} subsequent joins",
                    subsequent_joins.len()
                );

                // Determine JOIN type based on whether VLP is optional
                let vlp_join_type = if vlp_is_optional {
                    JoinType::Left
                } else {
                    JoinType::Join
                };

                // For OPTIONAL VLP, we need to add the CTE as a LEFT JOIN
                // (because FROM is now the anchor node, not the CTE)
                if vlp_is_optional {
                    // Use the pre-computed vlp_alias (computed above from CTE name)
                    let denorm_info_for_cte = get_variable_length_denorm_info(&transformed_plan);
                    let start_id_col_for_cte = denorm_info_for_cte
                    .as_ref()
                    .and_then(|d| d.start_id_col.clone())
                    .or_else(|| get_node_id_column_for_alias_with_schema(&start_alias, self, schema))
                    .unwrap_or_else(|| {
                        log::error!("❌ SCHEMA ERROR: Could not determine ID column for optional VLP start alias '{}'", start_alias);
                        format!("ERROR_NO_ID_COL_FOR_ALIAS_{}", start_alias)
                    });

                    // For OPTIONAL VLP, use the Cypher alias (not VLP internal alias)
                    // Because we're not creating a start_node JOIN (start is already in FROM from required MATCH)
                    let cypher_start_alias = vlp_cte
                        .and_then(|c| c.vlp_cypher_start_alias.clone())
                        .unwrap_or_else(|| start_alias.clone());

                    log::info!("🔧 OPTIONAL VLP CTE JOIN: Using Cypher alias '{}' instead of VLP internal alias '{}'",
                          cypher_start_alias, start_alias);

                    // LEFT JOIN vlp_cte7 AS t7 ON t7.start_id = a.user_id
                    extracted_joins.push(Join {
                        table_name: vlp_cte_name.clone(),
                        table_alias: vlp_alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(vlp_alias.clone()),
                                    column: PropertyValue::Column("start_id".to_string()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cypher_start_alias), // Use Cypher alias
                                    column: PropertyValue::Column(start_id_col_for_cte),
                                }),
                            ],
                        }],
                        join_type: JoinType::Left,
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                    });
                }

                // For denormalized edges, node properties are embedded in edge table
                // so we need to JOIN to the edge tables to access those properties.
                //
                // IMPORTANT: Even if both nodes are denormalized (is_fully_denormalized),
                // we still need JOINs because the VLP CTE only contains:
                //   - start_id, end_id (for matching)
                //   - path tracking columns (hop_count, path_edges, path_nodes, path_relationships)
                //   - edge properties (if any)
                // But it does NOT contain node properties!
                //
                // Node properties must be fetched by JOINing back to the source table(s)
                // using the start_id/end_id columns from the CTE.
                //
                // 🎯 FIX: Get denorm info from ORIGINAL plan (self), not transformed_plan (which has VLP removed)
                let denorm_info = get_variable_length_denorm_info(self);

                // REMOVED the is_fully_denormalized check that was skipping JOIN creation
                // Previously: if denorm_info.as_ref().map_or(false, |d| d.is_fully_denormalized()) { skip JOINs }
                // This was wrong because VLP CTE doesn't include node properties.

                {
                    // Get the actual table names and ID columns from:
                    // 1. Plan denorm info (extracted from original GraphRel before CTE extraction)
                    // 2. VLP CTE metadata (populated during CTE generation)
                    // 3. Schema lookup using plan context (proper fallback with schema parameter)
                    let start_table = denorm_info
                        .as_ref()
                        .and_then(|d| d.start_table.clone())
                        .or_else(|| vlp_cte.and_then(|c| c.vlp_start_table.clone()))
                        .or_else(|| {
                            get_node_table_for_alias_with_schema(&start_alias, self, schema)
                        })
                        .unwrap_or_else(|| {
                            log::error!(
                                "❌ SCHEMA ERROR: Could not determine table for alias '{}'",
                                start_alias
                            );
                            format!("ERROR_NO_TABLE_FOR_ALIAS_{}", start_alias)
                        });
                    let end_table = denorm_info
                        .as_ref()
                        .and_then(|d| d.end_table.clone())
                        .or_else(|| vlp_cte.and_then(|c| c.vlp_end_table.clone()))
                        .or_else(|| get_node_table_for_alias_with_schema(&end_alias, self, schema))
                        .unwrap_or_else(|| {
                            log::error!(
                                "❌ SCHEMA ERROR: Could not determine table for alias '{}'",
                                end_alias
                            );
                            format!("ERROR_NO_TABLE_FOR_ALIAS_{}", end_alias)
                        });
                    // 🔧 FIX: Use ID columns from VLP CTE metadata (from relationship schema) instead of denorm_info (from node schema)
                    // The VLP CTE has the authoritative column names from the relationship's from_id/to_id
                    // NOT the node schema's node_id field (e.g., DNS: rel.to_id = "query", not Domain.node_id = "name")
                    let start_id_col = vlp_cte
                        .and_then(|c| c.vlp_start_id_col.clone())
                        .or_else(|| denorm_info.as_ref().and_then(|d| d.start_id_col.clone()))
                        .or_else(|| {
                            get_node_id_column_for_alias_with_schema(&start_alias, self, schema)
                        })
                        .unwrap_or_else(|| {
                            log::error!(
                                "❌ SCHEMA ERROR: Could not determine ID column for alias '{}'",
                                start_alias
                            );
                            format!("ERROR_NO_ID_COL_FOR_ALIAS_{}", start_alias)
                        });
                    let end_id_col = vlp_cte
                        .and_then(|c| c.vlp_end_id_col.clone())
                        .or_else(|| denorm_info.as_ref().and_then(|d| d.end_id_col.clone()))
                        .or_else(|| {
                            get_node_id_column_for_alias_with_schema(&end_alias, self, schema)
                        })
                        .unwrap_or_else(|| {
                            log::error!(
                                "❌ SCHEMA ERROR: Could not determine ID column for alias '{}'",
                                end_alias
                            );
                            format!("ERROR_NO_ID_COL_FOR_ALIAS_{}", end_alias)
                        });

                    // Check denormalization status for each node
                    let start_is_denorm = denorm_info
                        .as_ref()
                        .map_or(false, |d| d.start_is_denormalized);
                    let end_is_denorm = denorm_info
                        .as_ref()
                        .map_or(false, |d| d.end_is_denormalized);

                    log::debug!("🔍 VLP endpoint JOIN conditions:");
                    log::debug!("  start_alias='{}', end_alias='{}'", start_alias, end_alias);
                    log::debug!(
                        "  start_is_denorm={}, end_is_denorm={}",
                        start_is_denorm,
                        end_is_denorm
                    );
                    log::debug!("  vlp_is_optional={}", vlp_is_optional);
                    log::debug!("  Self-loop (start == end): {}", start_alias == end_alias);

                    // Check for self-loop: start and end are the same node (e.g., (a)-[*0..]->(a))
                    if start_alias == end_alias {
                        // Self-loop: Only add ONE JOIN with compound ON condition (if not denormalized)
                        // For OPTIONAL VLP, the start node is already in FROM, so skip this JOIN
                        if !start_is_denorm && !vlp_is_optional {
                            // JOIN users AS a ON t.start_id = a.user_id AND t.end_id = a.user_id
                            extracted_joins.push(Join {
                                table_name: start_table,
                                table_alias: start_alias.clone(),
                                joining_on: vec![
                                    OperatorApplication {
                                        operator: Operator::Equal,
                                        operands: vec![
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(vlp_alias.clone()), // ✅ Use computed vlp_alias
                                                column: PropertyValue::Column(
                                                    "start_id".to_string(),
                                                ),
                                            }),
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(start_alias.clone()),
                                                column: PropertyValue::Column(start_id_col.clone()),
                                            }),
                                        ],
                                    },
                                    OperatorApplication {
                                        operator: Operator::Equal,
                                        operands: vec![
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(vlp_alias.clone()), // ✅ Use computed vlp_alias
                                                column: PropertyValue::Column("end_id".to_string()),
                                            }),
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(start_alias.clone()),
                                                column: PropertyValue::Column(start_id_col.clone()),
                                            }),
                                        ],
                                    },
                                ],
                                join_type: vlp_join_type.clone(),
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        }
                    } else {
                        // Different start and end nodes: Add JOINs to access node properties
                        // ✅ FIX: Check if nodes are denormalized - if so, skip JOINs (properties are in CTE)
                        // For denormalized schemas, both nodes point to the same edge table (e.g., flights)
                        // and the VLP CTE already includes all node properties from that table
                        let is_denormalized_vlp = denorm_info
                            .as_ref()
                            .map_or(false, |d| d.is_fully_denormalized());

                        log::info!("🔍 VLP endpoint JOIN decision: is_denormalized_vlp={}, start_table='{}', end_table='{}'",
                              is_denormalized_vlp, start_table, end_table);

                        // For OPTIONAL VLP, skip the start node JOIN (it's already in FROM)
                        if !vlp_is_optional && !is_denormalized_vlp {
                            // Only add START node JOIN if VLP is NOT fully denormalized
                            // Denormalized VLP CTE already contains node properties
                            // 🔧 FIX: Use Cypher alias from VLP metadata instead of internal VLP alias
                            let start_node_alias = vlp_cte
                                .and_then(|c| c.vlp_cypher_start_alias.clone())
                                .unwrap_or_else(|| start_alias.clone());

                            log::debug!("✅ Creating START node JOIN: {} AS {} (Cypher alias from VLP metadata)", start_table, start_node_alias);
                            extracted_joins.push(Join {
                                table_name: start_table,
                                table_alias: start_node_alias.clone(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(vlp_alias.clone()), // ✅ Use computed vlp_alias
                                            column: PropertyValue::Column("start_id".to_string()),
                                        }),
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(start_node_alias.clone()),
                                            column: PropertyValue::Column(start_id_col.clone()),
                                        }),
                                    ],
                                }],
                                join_type: vlp_join_type.clone(),
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        } else {
                            if is_denormalized_vlp {
                                log::debug!("⏭️  SKIP START node JOIN: fully denormalized VLP (properties in CTE)");
                            } else {
                                log::debug!(
                                    "⏭️  SKIP START node JOIN: vlp_is_optional={}",
                                    vlp_is_optional
                                );
                            }
                        }
                        // Add END node JOIN to access node properties (unless denormalized)
                        // For OPTIONAL and REQUIRED VLP: Always use Cypher alias from VLP metadata
                        if !is_denormalized_vlp {
                            let end_node_alias = vlp_cte
                                .and_then(|c| c.vlp_cypher_end_alias.clone())
                                .unwrap_or_else(|| end_alias.clone());

                            log::debug!("✅ Creating END node JOIN: {} AS {} (Cypher alias from VLP metadata)",
                                  end_table, end_node_alias);
                            extracted_joins.push(Join {
                                table_name: end_table,
                                table_alias: end_node_alias.clone(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(vlp_alias.clone()), // ✅ Use computed vlp_alias
                                            column: PropertyValue::Column("end_id".to_string()),
                                        }),
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(end_node_alias.clone()),
                                            column: PropertyValue::Column(end_id_col.clone()),
                                        }),
                                    ],
                                }],
                                join_type: vlp_join_type.clone(),
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                            });
                        } else {
                            log::debug!("⏭️  SKIP END node JOIN: fully denormalized VLP (properties in CTE)");
                        }
                    }
                } // End of VLP endpoint JOIN creation scope

                // Re-add the subsequent pattern joins (chained patterns after VLP)
                // These joins reference the VLP endpoint aliases (e.g., g.group_id)
                // which are now available from the VLP endpoint JOINs added above
                if !subsequent_joins.is_empty() {
                    log::info!(
                    "🔧 VLP CHAINED FIX: Re-adding {} subsequent joins after VLP endpoint JOINs",
                    subsequent_joins.len()
                );

                    // 🎯 FIX: Rewrite VLP internal aliases to Cypher aliases
                    // The subsequent join conditions reference VLP internal aliases (e.g., end_node)
                    // But the VLP endpoint JOINs create Cypher aliases (e.g., g)
                    // We need to map: VLP internal alias → Cypher alias
                    //
                    // Example: (u)-[:MEMBER_OF*1..5]->(g)-[:HAS_ACCESS]->(f)
                    // - VLP CTE uses: start_node, end_node
                    // - VLP endpoint JOINs create: u (for start), g (for end)
                    // - HAS_ACCESS join condition has: end_node.group_id = ...
                    // - We need to rewrite to: g.group_id = ...
                    //
                    // IMPORTANT: For OPTIONAL VLP, don't rewrite! The "subsequent" joins
                    // for OPTIONAL VLP are actually the anchor pattern joins (before the VLP),
                    // not chained patterns after the VLP. They should keep their original aliases.
                    let vlp_to_cypher_map: std::collections::HashMap<String, String> =
                        if !vlp_is_optional {
                            if let Some(vlp_cte_ref) = vlp_cte {
                                let mut map = std::collections::HashMap::new();

                                // Get both the Cypher aliases AND VLP internal aliases from the CTE metadata
                                // Map VLP internal aliases → Cypher aliases (REVERSE direction!)
                                if let (
                                    Some(cypher_start),
                                    Some(cypher_end),
                                    Some(vlp_start),
                                    Some(vlp_end),
                                ) = (
                                    &vlp_cte_ref.vlp_cypher_start_alias,
                                    &vlp_cte_ref.vlp_cypher_end_alias,
                                    &vlp_cte_ref.vlp_start_alias,
                                    &vlp_cte_ref.vlp_end_alias,
                                ) {
                                    log::info!(
                                        "🔄 Alias mapping (VLP→Cypher): '{}' → '{}', '{}' → '{}'",
                                        vlp_start,
                                        cypher_start,
                                        vlp_end,
                                        cypher_end
                                    );
                                    map.insert(vlp_start.clone(), cypher_start.clone()); // start_node → u
                                    map.insert(vlp_end.clone(), cypher_end.clone());
                                    // end_node → g
                                }
                                map
                            } else {
                                std::collections::HashMap::new()
                            }
                        } else {
                            log::info!("🔧 OPTIONAL VLP: Skipping alias rewriting for anchor pattern joins");
                            std::collections::HashMap::new()
                        };

                    // Rewrite subsequent joins to use Cypher aliases (only for REQUIRED VLP)
                    let rewritten_joins: Vec<Join> = subsequent_joins
                        .into_iter()
                        .map(|mut join| {
                            // Rewrite aliases in JOIN conditions
                            for cond in &mut join.joining_on {
                                for operand in &mut cond.operands {
                                    rewrite_aliases(operand, &vlp_to_cypher_map);
                                }
                            }
                            join
                        })
                        .collect();

                    for join in &rewritten_joins {
                        log::info!(
                            "  → JOIN {} AS {} ON {:?}",
                            join.table_name,
                            join.table_alias,
                            join.joining_on
                        );
                    }
                    extracted_joins.extend(rewritten_joins);
                }
            } // end of else block for normal VLP handling (no WITH CTE)
        }

        // For multiple relationship types (UNION CTE), add joins to connect nodes
        // Handle MULTIPLE polymorphic edges for multi-hop patterns like (u)-[r1]->(m)-[r2]->(t)
        let polymorphic_edges = collect_polymorphic_edges(&transformed_plan);
        let polymorphic_ctes: Vec<_> = extracted_ctes
            .iter()
            .filter(|cte| cte.cte_name.starts_with("rel_") && !cte.is_recursive)
            .collect();

        if !polymorphic_ctes.is_empty() && has_polymorphic_or_multi_rel(&transformed_plan) {
            log::info!(
                "🎯 MULTI-HOP POLYMORPHIC: Found {} CTEs and {} polymorphic edges",
                polymorphic_ctes.len(),
                polymorphic_edges.len()
            );

            // Get the FROM clause alias to exclude it from joins
            let from_alias = final_from
                .as_ref()
                .and_then(|ft| ft.table.as_ref())
                .and_then(|vt| vt.alias.clone());

            // Collect all polymorphic target aliases to filter from joins
            let polymorphic_targets: std::collections::HashSet<_> = polymorphic_edges
                .iter()
                .map(|e| e.right_connection.clone())
                .collect();

            // Filter out duplicate joins for polymorphic targets and FROM alias
            extracted_joins.retain(|j| {
                let is_polymorphic_target = polymorphic_targets.contains(&j.table_alias);
                let is_from = from_alias.as_ref().map_or(false, |fa| &j.table_alias == fa);
                if is_from {
                    log::info!(
                        "🎯 MIXED EDGE: Filtering out JOIN for FROM alias '{}'",
                        j.table_alias
                    );
                }
                if is_polymorphic_target {
                    log::info!(
                        "🎯 POLYMORPHIC: Filtering out JOIN for polymorphic target '{}'",
                        j.table_alias
                    );
                }
                !is_polymorphic_target && !is_from
            });

            // Build a map of node aliases to their source CTE (for chaining)
            // Key: right_connection (target), Value: (cte_alias, cte_name)
            let mut node_to_cte: std::collections::HashMap<String, (String, String)> =
                std::collections::HashMap::new();

            // Sort edges by processing order: edges whose left_connection is NOT a CTE target go first
            // This ensures we process `u -> middle` before `middle -> target`
            let mut sorted_edges = polymorphic_edges.clone();
            sorted_edges.sort_by(|a, b| {
                let a_is_chained = polymorphic_targets.contains(&a.left_connection);
                let b_is_chained = polymorphic_targets.contains(&b.left_connection);
                a_is_chained.cmp(&b_is_chained)
            });

            // Add JOINs for each polymorphic edge
            for edge in &sorted_edges {
                // For incoming edges (u)<-[r]-(source), the labeled node is on the right
                // and we join on to_node_id. For outgoing edges, the labeled node is on
                // the left and we join on from_node_id.
                let (cte_column, node_alias, id_column) = if edge.is_incoming {
                    // Incoming: join CTE's to_node_id to the right connection (labeled node)
                    let id_col = get_node_id_column_for_alias_with_schema(&edge.right_connection, self, schema)
                        .unwrap_or_else(|| {
                            log::error!("❌ SCHEMA ERROR: Could not determine ID column for incoming edge alias '{}'", edge.right_connection);
                            format!("ERROR_NO_ID_COL_FOR_ALIAS_{}", edge.right_connection)
                        });
                    log::info!(
                        "🎯 INCOMING EDGE: {} joins to_node_id = {}.{}",
                        edge.rel_alias,
                        edge.right_connection,
                        id_col
                    );
                    (
                        "to_node_id".to_string(),
                        edge.right_connection.clone(),
                        id_col,
                    )
                } else {
                    // Outgoing: check if source is from a previous CTE (chaining)
                    if let Some((prev_cte_alias, _)) = node_to_cte.get(&edge.left_connection) {
                        // Chained CTE: join on previous CTE's to_node_id
                        log::info!(
                            "🎯 CHAINED CTE: {} joins from previous CTE {}.to_node_id",
                            edge.rel_alias,
                            prev_cte_alias
                        );
                        (
                            "from_node_id".to_string(),
                            prev_cte_alias.clone(),
                            "to_node_id".to_string(),
                        )
                    } else {
                        // First hop: join on source node's ID column
                        let source_id_col = get_node_id_column_for_alias_with_schema(&edge.left_connection, self, schema)
                            .unwrap_or_else(|| {
                                log::error!("❌ SCHEMA ERROR: Could not determine ID column for outgoing edge alias '{}'", edge.left_connection);
                                format!("ERROR_NO_ID_COL_FOR_ALIAS_{}", edge.left_connection)
                            });
                        (
                            "from_node_id".to_string(),
                            edge.left_connection.clone(),
                            source_id_col,
                        )
                    }
                };

                log::info!(
                    "🎯 Adding CTE JOIN: {} AS {} ON {} = {}.{}",
                    edge.cte_name,
                    edge.rel_alias,
                    cte_column,
                    node_alias,
                    id_column
                );

                extracted_joins.push(Join {
                    table_name: edge.cte_name.clone(),
                    table_alias: edge.rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(edge.rel_alias.clone()),
                                column: PropertyValue::Column(cte_column),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(node_alias),
                                column: PropertyValue::Column(id_column),
                            }),
                        ],
                    }],
                    join_type: JoinType::Join,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                });

                // Record this CTE as the source for its target node (for chaining)
                // For outgoing edges: target is right_connection
                // For incoming edges: target is left_connection (the $any node)
                if edge.is_incoming {
                    node_to_cte.insert(
                        edge.left_connection.clone(),
                        (edge.rel_alias.clone(), edge.cte_name.clone()),
                    );
                } else {
                    node_to_cte.insert(
                        edge.right_connection.clone(),
                        (edge.rel_alias.clone(), edge.cte_name.clone()),
                    );
                }
            }
        }
        // For variable-length (recursive) CTEs, keep previous logic
        if let Some(last_node_cte) = transformed_plan
            .extract_last_node_cte(schema)
            .ok()
            .flatten()
        {
            if let super::CteContent::RawSql(_) = &last_node_cte.content {
                let cte_name = last_node_cte.cte_name.clone();
                if cte_name.starts_with("rel_") {
                    for join in extracted_joins.iter_mut() {
                        join.table_name = cte_name.clone();
                    }
                }
            }
        }

        // Sort JOINs by dependency order to ensure referenced tables are defined before use
        // This is critical for OPTIONAL VLP queries where vlp_cte references message.id,
        // but message table JOIN might come after vlp_cte JOIN in extracted order
        extracted_joins = sort_joins_by_dependency(extracted_joins, final_from.as_ref());

        let mut extracted_group_by_exprs = transformed_plan.extract_group_by()?;

        // Rewrite GROUP BY expressions for variable-length paths ONLY for denormalized edges
        // For non-denormalized edges, the outer query JOINs with node tables, so a.name works directly
        // For denormalized edges, there are no node table JOINs, so we need t.start_name
        if let Some((left_alias, right_alias)) = has_variable_length_rel(&transformed_plan) {
            // Only rewrite for denormalized patterns (no node table JOINs)
            if is_variable_length_denormalized(&transformed_plan) {
                let path_var = get_path_variable(&transformed_plan);
                extracted_group_by_exprs = extracted_group_by_exprs
                    .into_iter()
                    .map(|expr| {
                        rewrite_expr_for_var_len_cte(
                            &expr,
                            &left_alias,
                            &right_alias,
                            path_var.as_deref(),
                        )
                    })
                    .collect();
            }
        }

        // Rewrite path functions in GROUP BY expressions (length(p), nodes(p), relationships(p))
        // This handles both single-type and multi-type VLP patterns
        // NOTE: Use original plan (self), not transformed_plan, because GraphRel might be transformed away
        if let Some(path_var_name) = get_path_variable(self) {
            // Determine table alias based on pattern type
            let table_alias_for_path = if let Some(graph_rel) = get_graph_rel_from_plan(self) {
                if let Some(ref labels) = graph_rel.labels {
                    if labels.len() > 1 {
                        // Multi-type VLP: use end node alias
                        if let LogicalPlan::GraphNode(ref right_node) = graph_rel.right.as_ref() {
                            log::info!("🎯 GROUP BY path function rewriting: Multi-type VLP detected, using end alias '{}'", right_node.alias);
                            right_node.alias.clone()
                        } else {
                            log::info!("🎯 GROUP BY path function rewriting: Multi-type VLP but no right node, using 't'");
                            "t".to_string()
                        }
                    } else {
                        log::info!(
                            "🎯 GROUP BY path function rewriting: Single-type VLP, using 't'"
                        );
                        "t".to_string()
                    }
                } else {
                    log::info!("🎯 GROUP BY path function rewriting: No labels, using 't'");
                    "t".to_string()
                }
            } else {
                log::info!("🎯 GROUP BY path function rewriting: No GraphRel found, using 't'");
                "t".to_string()
            };

            log::info!(
                "🎯 GROUP BY path function rewriting: {} expressions with table alias '{}'",
                extracted_group_by_exprs.len(),
                table_alias_for_path
            );
            extracted_group_by_exprs = extracted_group_by_exprs
                .into_iter()
                .map(|expr| {
                    rewrite_path_functions_with_table(&expr, &path_var_name, &table_alias_for_path)
                })
                .collect();
        }

        let mut extracted_order_by = transformed_plan.extract_order_by()?;
        log::info!("🔍 Extracted {} ORDER BY items", extracted_order_by.len());

        // Rewrite ORDER BY expressions for variable-length paths ONLY for denormalized edges
        // For non-denormalized edges, the outer query JOINs with node tables, so a.name works directly
        // For denormalized edges, there are no node table JOINs, so we need t.start_name
        if let Some((left_alias, right_alias)) = has_variable_length_rel(&transformed_plan) {
            // Only rewrite ORDER BY for denormalized patterns (no node table JOINs)
            if is_variable_length_denormalized(&transformed_plan) {
                let path_var = get_path_variable(&transformed_plan);
                extracted_order_by = extracted_order_by
                    .into_iter()
                    .map(|item| OrderByItem {
                        expression: rewrite_expr_for_var_len_cte(
                            &item.expression,
                            &left_alias,
                            &right_alias,
                            path_var.as_deref(),
                        ),
                        order: item.order,
                    })
                    .collect();
            }
        }

        // 🎯 Rewrite ORDER BY expressions for multi-type VLP
        // For multi-type VLP, labels(x)[1] should become x.end_type
        // Check if any CTE is a multi-type VLP CTE
        let has_multi_type_vlp_cte = extracted_ctes
            .iter()
            .any(|cte| cte.cte_name.starts_with("vlp_multi_type_"));
        if has_multi_type_vlp_cte {
            log::info!("🎯 Multi-type VLP: Rewriting ORDER BY expressions");
            extracted_order_by = extracted_order_by
                .into_iter()
                .map(|item| {
                    let rewritten_expr =
                        rewrite_labels_subscript_for_multi_type_vlp(&item.expression);
                    OrderByItem {
                        expression: rewritten_expr,
                        order: item.order,
                    }
                })
                .collect();
        }

        let extracted_limit_item = transformed_plan.extract_limit();

        let extracted_skip_item = transformed_plan.extract_skip();

        let extracted_union = transformed_plan.extract_union(schema)?;

        // Validate render plan before construction (for CTE path)
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }

        // Check if this is a standalone RETURN query (no MATCH, only literals/parameters/functions)
        let is_standalone_return = final_from.is_none()
            && final_select_items
                .iter()
                .all(|item| is_standalone_expression(&item.expression));

        if is_standalone_return {
            // For standalone RETURN queries (e.g., "RETURN 1 + 1", "RETURN toUpper($name)"),
            // use ClickHouse's system.one table as a dummy FROM clause
            log::debug!("Detected standalone RETURN query, using system.one as FROM clause");

            // Create a ViewTableRef that references system.one
            // Use an Empty LogicalPlan since we don't need actual view resolution for system tables
            final_from = Some(FromTable::new(Some(ViewTableRef {
                source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                name: "system.one".to_string(),
                alias: None,
                use_final: false,
            })));
        }

        // CRITICAL FIX: If FROM is still None but we have WITH CTEs, use the last WITH CTE
        // This handles: MATCH (...) WITH DISTINCT x, y WITH x, CASE ... WITH x, sum(...) RETURN x.name
        // The chain of WITH clauses creates CTEs, and the final RETURN should select FROM the last CTE
        if final_from.is_none() && !extracted_ctes.is_empty() {
            // Find the last WITH CTE (not VLP CTE)
            if let Some(last_with_cte) = extracted_ctes
                .iter()
                .rev()
                .find(|cte| cte.cte_name.starts_with("with_"))
            {
                log::info!(
                    "🔧 FROM clause missing but have WITH CTEs - setting FROM to last WITH CTE: {}",
                    last_with_cte.cte_name
                );

                // Extract aliases from CTE name: "with_tag_total_cte_1" → "tag_total"
                let with_alias_part =
                    if let Some(stripped) = last_with_cte.cte_name.strip_prefix("with_") {
                        if let Some(cte_pos) = stripped.rfind("_cte") {
                            &stripped[..cte_pos]
                        } else {
                            stripped
                        }
                    } else {
                        ""
                    };

                final_from = Some(FromTable::new(Some(ViewTableRef {
                    source: std::sync::Arc::new(
                        crate::query_planner::logical_plan::LogicalPlan::Empty,
                    ),
                    name: last_with_cte.cte_name.clone(),
                    alias: Some(with_alias_part.to_string()),
                    use_final: false,
                })));

                log::info!(
                    "🔧 Set FROM to: {} AS '{}'",
                    last_with_cte.cte_name,
                    with_alias_part
                );
            }
        }

        // Handle aggregation queries that don't have explicit FROM but need one
        // For queries like "MATCH (n) RETURN count(n)", we need to extract FROM from the plan
        if final_from.is_none() {
            println!("DEBUG: final_from is None, checking for aggregations");
            // Check if we have aggregation functions in SELECT
            let has_aggregations = final_select_items.iter().any(|item| {
                let result = contains_aggregation_function(&item.expression);
                println!(
                    "DEBUG: Checking item expression, has_aggregation: {}",
                    result
                );
                result
            });

            println!("DEBUG: has_aggregations: {}", has_aggregations);
            if has_aggregations {
                println!("DEBUG: Detected aggregation query without FROM, attempting to extract FROM from plan");
                println!(
                    "DEBUG: transformed_plan type: {:?}",
                    std::any::type_name::<LogicalPlan>()
                );
                println!(
                    "DEBUG: original plan type: {:?}",
                    std::any::type_name::<LogicalPlan>()
                );

                // Try to extract FROM from the transformed plan first, then original plan
                if let Some(from_table) = transformed_plan.extract_from().ok().flatten() {
                    final_from = Some(from_table);
                    println!(
                        "✅ Set FROM for aggregation query from transformed plan: {:?}",
                        final_from
                            .as_ref()
                            .and_then(|f| f.table.as_ref().map(|v| &v.name))
                    );
                } else if let Some(from_table) = self.extract_from().ok().flatten() {
                    final_from = Some(from_table);
                    println!(
                        "✅ Set FROM for aggregation query from original plan: {:?}",
                        final_from
                            .as_ref()
                            .and_then(|f| f.table.as_ref().map(|v| &v.name))
                    );
                } else {
                    // Check if we have an unlabeled GraphNode (like MATCH (n) RETURN count(n))
                    // In this case, we need to create a UNION ALL of all node tables
                    fn has_unlabeled_graph_node(plan: &LogicalPlan) -> bool {
                        match plan {
                            LogicalPlan::GraphNode(gn) if gn.label.is_none() => true,
                            LogicalPlan::Projection(p) => has_unlabeled_graph_node(&p.input),
                            LogicalPlan::Filter(f) => has_unlabeled_graph_node(&f.input),
                            LogicalPlan::GraphRel(gr) => {
                                has_unlabeled_graph_node(&gr.left)
                                    || has_unlabeled_graph_node(&gr.right)
                            }
                            LogicalPlan::GraphJoins(gj) => has_unlabeled_graph_node(&gj.input),
                            _ => false,
                        }
                    }

                    if has_unlabeled_graph_node(&transformed_plan) {
                        println!("DEBUG: Found unlabeled GraphNode in aggregation query, creating count union of all node tables");

                        // Get all node tables from schema and create count queries
                        let count_queries: Vec<String> = schema
                            .get_nodes_schemas()
                            .values()
                            .map(|node_schema| {
                                format!(
                                    "SELECT count(*) as cnt FROM {}.{}",
                                    node_schema.database, node_schema.table_name
                                )
                            })
                            .collect();

                        if !count_queries.is_empty() {
                            // Create UNION ALL SQL for counting all node tables
                            let union_sql = count_queries.join(" UNION ALL ");

                            let cte_name = "node_counts".to_string();

                            // Create CTE with the count union
                            let count_cte = Cte {
                                cte_name: cte_name.clone(),
                                content: CteContent::RawSql(union_sql),
                                is_recursive: false,
                                vlp_start_alias: None,
                                vlp_end_alias: None,
                                vlp_start_table: None,
                                vlp_end_table: None,
                                vlp_cypher_start_alias: None,
                                vlp_cypher_end_alias: None,
                                vlp_start_id_col: None,
                                vlp_end_id_col: None,
                            };

                            // Add CTE to the render plan
                            extracted_ctes.push(count_cte);

                            // Set FROM to reference the CTE
                            final_from =
                                Some(FromTable::new(Some(ViewTableRef::new_view_with_alias(
                                    std::sync::Arc::new(LogicalPlan::Empty),
                                    cte_name.clone(),
                                    "node_counts".to_string(),
                                ))));

                            // Mark that we need to transform count() to sum() for aggregation queries
                            // We'll do this after final_select_items is assigned
                            let mut needs_aggregation_transform = true;
                        }
                    } else {
                        // Last resort: try to find any ViewScan in the plan and use it as FROM
                        fn find_any_viewscan_table(plan: &LogicalPlan) -> Option<FromTable> {
                            println!(
                                "DEBUG: Searching for ViewScan in plan type: {:?}",
                                std::mem::discriminant(plan)
                            );
                            match plan {
                                LogicalPlan::ViewScan(scan) => {
                                    println!(
                                        "DEBUG: Found ViewScan with table: {}",
                                        scan.source_table
                                    );
                                    Some(FromTable::new(Some(ViewTableRef::new_table(
                                        scan.as_ref().clone(),
                                        scan.source_table.clone(),
                                    ))))
                                }
                                LogicalPlan::Projection(p) => find_any_viewscan_table(&p.input),
                                LogicalPlan::Filter(f) => find_any_viewscan_table(&f.input),
                                LogicalPlan::GraphRel(gr) => find_any_viewscan_table(&gr.left)
                                    .or_else(|| find_any_viewscan_table(&gr.right)),
                                LogicalPlan::GraphNode(gn) => find_any_viewscan_table(&gn.input),
                                _ => None,
                            }
                        }

                        if let Some(from_table) = find_any_viewscan_table(&transformed_plan) {
                            final_from = Some(from_table);
                            println!(
                                "✅ Set FROM for aggregation query from ViewScan search: {:?}",
                                final_from
                                    .as_ref()
                                    .and_then(|f| f.table.as_ref().map(|v| &v.name))
                            );
                        } else {
                            println!("DEBUG: No ViewScan found in transformed plan, trying original plan");
                            if let Some(from_table) = find_any_viewscan_table(&self) {
                                final_from = Some(from_table);
                                println!("✅ Set FROM for aggregation query from ViewScan search in original plan: {:?}", final_from.as_ref().and_then(|f| f.table.as_ref().map(|v| &v.name)));
                            }
                        }
                    }
                }
            }
        }

        // Validate FROM clause exists (after potentially adding system.one for standalone queries)
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM clause found. This usually indicates missing table information or incomplete query planning.".to_string()
            ));
        }

        // Validate filters don't contain invalid expressions like "1 = 0"
        if let Some(filter_expr) = &final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter contains invalid expression (e.g., '1 = 0'). This indicates failed schema mapping or expression conversion.".to_string()
                ));
            }
        }

        // Deduplicate joins by (table_name, table_alias) - prevent "Multiple table expressions with same alias"
        // This can happen with variable-length paths where multiple code paths add the same CTE join
        let mut seen_joins: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        extracted_joins.retain(|join| {
            let key = (join.table_name.clone(), join.table_alias.clone());
            if seen_joins.contains(&key) {
                log::debug!(
                    "🔧 Deduplicating JOIN: {} AS {} (already exists)",
                    join.table_name,
                    join.table_alias
                );
                false
            } else {
                seen_joins.insert(key);
                true
            }
        });

        // 🔧 CRITICAL FIX: For multi-type VLP, check if we need to use default CTE columns
        // ONLY use default columns for bare node returns like "RETURN x"
        // For explicit returns like "RETURN label(x), x.name", keep the SELECT items as-is
        if is_multi_type_vlp {
            log::info!("🎯 Multi-type VLP: Checking if SELECT items need rewriting");
            log::info!(
                "🎯 Multi-type VLP: Current SELECT has {} items",
                final_select_items.len()
            );
            for (i, item) in final_select_items.iter().enumerate() {
                log::info!("  [{}] {:?} AS {:?}", i, item.expression, item.col_alias);
            }

            // Check if this is a bare node return (RETURN x)
            // Bare node returns have a single SELECT item that is the node alias without properties
            // We detect this by checking if:
            // 1. Only one SELECT item
            // 2. It's a TableAlias (not PropertyAccessExp, not Function, etc.)
            let is_bare_node_return = final_select_items.len() == 1
                && matches!(final_select_items[0].expression, RenderExpr::Raw(ref s) if s == "x" || s.contains("_node"));

            if is_bare_node_return {
                log::info!("🎯 Multi-type VLP: Bare node return detected (RETURN x), using default CTE columns");
                // Default: return full node structure (end_type, end_id, end_properties)
                final_select_items = vec![
                    SelectItem {
                        expression: RenderExpr::Raw("end_type".to_string()),
                        col_alias: Some(ColumnAlias("end_type".to_string())),
                    },
                    SelectItem {
                        expression: RenderExpr::Raw("end_id".to_string()),
                        col_alias: Some(ColumnAlias("end_id".to_string())),
                    },
                    SelectItem {
                        expression: RenderExpr::Raw("end_properties".to_string()),
                        col_alias: Some(ColumnAlias("end_properties".to_string())),
                    },
                ];
            } else {
                log::info!("🎯 Multi-type VLP: Explicit RETURN items, keeping SELECT as-is (NOT a bare node return)");
                // Keep the SELECT items as-is - they should already be properly mapped:
                // - label(x) → x.end_type
                // - x.name → JSON_VALUE(x.end_properties, '$.name')
                // - Aggregates → count(*), etc.
            }
        }

        // This fixes the path function alias bug where length(p) generates t.hop_count but t doesn't exist
        // The hardcoded "t" alias in rewrite_logical_path_functions needs to be rewritten to the actual
        // VLP CTE alias (e.g., vlp1433, vlp2)
        let mut render_plan = RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems {
                items: final_select_items,
                distinct: self.extract_distinct(),
            },
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(extracted_joins),
            array_join: ArrayJoinItem({
                // Extract ARRAY JOIN items and rewrite path functions for VLP if needed
                let mut array_joins = transformed_plan.extract_array_join()?;

                // If this is a VLP query with ARRAY JOIN, rewrite path functions
                // e.g., UNWIND nodes(p) AS n → ARRAY JOIN t.path_nodes AS n
                if !array_joins.is_empty() {
                    if let Some(ref pv) = get_path_variable(&transformed_plan) {
                        // Check if this is a VLP query that uses CTE
                        let needs_cte =
                            if let Some(spec) = get_variable_length_spec(&transformed_plan) {
                                spec.exact_hop_count().is_none()
                                    || get_shortest_path_mode(&transformed_plan).is_some()
                            } else {
                                false
                            };

                        if needs_cte {
                            // Rewrite path functions for all ARRAY JOIN items
                            for array_join in &mut array_joins {
                                array_join.expression = rewrite_path_functions_with_table(
                                    &array_join.expression,
                                    pv,
                                    "t", // CTE alias
                                );
                            }
                        }
                    }
                }
                array_joins
            }),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(extracted_group_by_exprs),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(extracted_order_by),
            skip: SkipItem(extracted_skip_item),
            limit: LimitItem(extracted_limit_item),
            union: UnionItems(extracted_union),
        };

        // Apply VLP alias rewriting to fix hardcoded "t" alias from rewrite_logical_path_functions
        rewrite_vlp_union_branch_aliases(&mut render_plan)?;

        Ok(render_plan)
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
                .map_or(false, |e| contains_aggregation_function(e))
        }
        _ => false,
    }
}
