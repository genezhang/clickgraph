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
use super::join_builder::JoinBuilder;
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
        // Delegate to the JoinBuilder trait implementation
        <LogicalPlan as JoinBuilder>::extract_joins(self, schema)
    }

    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<super::ArrayJoin>> {
        // Delegate to the JoinBuilder trait implementation
        <LogicalPlan as JoinBuilder>::extract_array_join(self)
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
        // TODO: Implement HAVING clause extraction
        Ok(None)
    }

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        // TODO: Implement ORDER BY extraction
        Ok(vec![])
    }

    fn extract_limit(&self) -> Option<i64> {
        // TODO: Implement LIMIT extraction
        None
    }

    fn extract_skip(&self) -> Option<i64> {
        // TODO: Implement SKIP extraction
        None
    }

    fn extract_union(&self, _schema: &GraphSchema) -> RenderPlanBuilderResult<Option<Union>> {
        // TODO: Implement UNION extraction
        Ok(None)
    }

    fn try_build_join_based_plan(
        &self,
        schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        // Delegate to JoinBuilder
        <LogicalPlan as JoinBuilder>::try_build_join_based_plan(self, schema)
    }

    fn build_simple_relationship_render_plan(
        &self,
        _distinct_override: Option<bool>,
        _schema: &GraphSchema,
    ) -> RenderPlanBuilderResult<RenderPlan> {
        // TODO: Implement simple relationship render plan building
        Err(RenderBuildError::InvalidRenderPlan(
            "Simple relationship render plan not implemented".to_string(),
        ))
    }

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
        // TODO: Implement full render plan conversion
        Err(RenderBuildError::InvalidRenderPlan(
            "Full render plan conversion not implemented".to_string(),
        ))
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
