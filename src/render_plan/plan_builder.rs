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
use super::from_builder::FromBuilder;
use super::group_by_builder::GroupByBuilder;
use super::join_builder::JoinBuilder;
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
        // Delegate to the SelectBuilder trait implementation
        <LogicalPlan as SelectBuilder>::extract_select_items(self)
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

    fn extract_union(&self, _schema: &GraphSchema) -> RenderPlanBuilderResult<Option<Union>> {
        // Note: UNION is handled by LogicalPlan::Union nodes in to_render_plan().
        // This method returns None for GraphJoins/GraphRel which don't directly have UNION.
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
        match self {
            LogicalPlan::GraphJoins(gj) => {
                let select_items = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self)?,
                    distinct: self.extract_distinct(),
                };
                let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));
                let joins = JoinItems(RenderPlanBuilder::extract_joins(self, schema)?);
                let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
                let filters = FilterItems(self.extract_filters()?);
                let group_by =
                    GroupByExpressions(<LogicalPlan as GroupByBuilder>::extract_group_by(self)?);
                let having_clause = self.extract_having()?;
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

                Ok(RenderPlan {
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
                })
            }
            LogicalPlan::GraphRel(gr) => {
                // For GraphRel, use the same extraction logic as GraphJoins
                let select_items = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self)?,
                    distinct: self.extract_distinct(),
                };
                let from = FromTableItem(self.extract_from()?.and_then(|ft| ft.table));
                let joins = JoinItems(RenderPlanBuilder::extract_joins(self, schema)?);
                let array_join = ArrayJoinItem(RenderPlanBuilder::extract_array_join(self)?);
                let filters = FilterItems(self.extract_filters()?);
                let group_by =
                    GroupByExpressions(<LogicalPlan as GroupByBuilder>::extract_group_by(self)?);
                let having_clause = self.extract_having()?;
                let order_by = OrderByItems(self.extract_order_by()?);

                // Extract CTEs for variable-length paths
                let mut context = super::cte_generation::CteGenerationContext::new();
                let ctes = CteItems(extract_ctes_with_context(
                    self,
                    &gr.right_connection,
                    &mut context,
                    schema,
                )?);

                let skip = SkipItem(self.extract_skip());
                let limit = LimitItem(self.extract_limit());
                let union = UnionItems(self.extract_union(schema)?);

                Ok(RenderPlan {
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
                })
            }
            LogicalPlan::Projection(p) => {
                // For Projection, convert the input plan and override the select items
                let mut render_plan = p.input.to_render_plan(schema)?;
                render_plan.select = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self)?,
                    distinct: p.distinct,
                };
                Ok(render_plan)
            }
            LogicalPlan::Filter(f) => {
                // For Filter, convert the input plan and combine filters
                let mut render_plan = f.input.to_render_plan(schema)?;

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
                // GraphNode is a wrapper around a ViewScan
                // Recursively convert the input (which should be a ViewScan)
                gn.input.to_render_plan(schema)
            }
            LogicalPlan::ViewScan(vs) => {
                // ViewScan is a simple table scan - convert to basic RenderPlan
                // This is used for standalone node queries without relationships
                let select_items = SelectItems {
                    items: <LogicalPlan as SelectBuilder>::extract_select_items(self)?,
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
                })
            }
            LogicalPlan::WithClause(_) => {
                // WithClause requires complex CTE generation and scope handling.
                // This is handled by specialized builders in plan_builder_helpers.rs.
                // Direct conversion via to_render_plan is not supported - use the
                // specialized builders: build_chained_with_match_cte_plan() or
                // build_with_aggregation_match_cte_plan() instead.
                Err(RenderBuildError::InvalidRenderPlan(
                    "WithClause requires specialized CTE builder (build_chained_with_match_cte_plan or build_with_aggregation_match_cte_plan)".to_string()
                ))
            }
            _ => todo!(
                "Render plan conversion not implemented for LogicalPlan variant: {:?}",
                std::mem::discriminant(self)
            ),
        }
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
