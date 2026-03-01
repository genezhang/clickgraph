//! Pure utility functions for plan building
//!
//! This module contains utility functions that have no dependencies on LogicalPlan
//! or complex state. These are safe to extract early in the refactoring process.
//!
//! Functions in this module should be:
//! - Pure (no side effects)
//! - Independent of LogicalPlan structure
//! - Reusable across different builder modules
//!
//! # File Size Justification (10,807 lines - Accepted)
//!
//! **Investigation Date**: January 29, 2026  
//! **Status**: ✅ Large file size accepted as reasonable
//!
//! This module resulted from splitting the original `plan_builder.rs` god module (16K+ lines)
//! during the render_plan architecture cleanup. The split improved overall code organization:
//!
//! **Before**: Single 16K line file with mixed concerns
//! **After**:
//! - `plan_builder.rs` (1,675 lines) - Core building logic
//! - `plan_builder_utils.rs` (10,807 lines) - 69 utility functions
//! - `plan_builder_helpers.rs` (4,051 lines) - Helper functions
//!
//! **Why we keep it as one file**:
//! 1. Functions are heavily interconnected (internal calls between utilities)
//! 2. All serve the common purpose of SQL generation/transformation
//! 3. Splitting would create artificial boundaries and complicate imports
//! 4. 69 functions averaging ~150 lines each is maintainable
//! 5. Clear naming patterns: `extract_*`, `rewrite_*`, `convert_*`
//!
//! **Analysis showed**:
//! - 0 truly dead functions (all are used, despite claims of "44 dead")
//! - ~20 low-use functions (1-2 calls) - specialized helpers
//! - 46+ heavily-used functions (3+ calls) - core utilities
//!
//! Future improvements should focus on better documentation and grouping comments,
//! not arbitrary file splits that would harm cohesion.
//!

#![allow(dead_code)]

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::GraphSchema;
use crate::query_planner::join_context::{
    VlpPosition, VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
};
use crate::query_planner::logical_expr::{Direction, LogicalExpr};
use crate::query_planner::logical_plan::{GraphNode, GraphRel, LogicalPlan};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::render_plan::plan_builder::RenderPlanBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::render_plan::cte_extraction::{
    extract_relationship_columns, get_path_variable, rel_type_to_table_name, table_to_id_column,
    RelationshipColumns,
};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::render_expr::{
    AggregateFnCall, Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication,
    PropertyAccess, RenderCase, RenderExpr, ScalarFnCall, TableAlias,
};
use crate::render_plan::view_table_ref::{from_table_to_view_ref, view_ref_to_from_table};
use crate::render_plan::JoinType;
use crate::render_plan::OrderByItem;
use crate::render_plan::SelectItem;
use crate::render_plan::{
    ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
    JoinItems, LimitItem, OrderByItems, OrderByOrder, RenderPlan, SelectItems, SkipItem, Union,
    UnionItems,
};
use crate::render_plan::{FromTable, ViewTableRef};
use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};
use crate::utils::cte_naming::{generate_cte_name, is_generated_cte_name};
use log::{self, debug};

// Import ALL helper functions from the dedicated helpers module using glob import
// This allows existing code to call helpers without changes (e.g., extract_table_name())
// The compiler will use the module functions when available
#[allow(unused_imports)]
use super::plan_builder_helpers::*;

// Import ALL alias utility functions from the dedicated module using glob import
// This consolidates duplicate functions and provides a single source of truth
#[allow(unused_imports)]
use super::utils::alias_utils::*;

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

/// Rewrite aggregate function arguments to use VLP CTE aliases for end nodes.
///
/// **Problem**: When `COUNT(DISTINCT b)` is used where `b` is a VLP end node:
/// - The aggregate normalizer converts `TableAlias("b")` to `PropertyAccessExp{table_alias: "b", column: "end_id"}`
/// - But in SQL, `b` doesn't exist as a table - the VLP CTE is joined as `vlp_a_b AS t`
/// - Result: `SELECT COUNT(DISTINCT b.end_id)` fails with "Identifier cannot be resolved"
///
/// **Solution**: Check if any PropertyAccessExp references a VLP end node Cypher alias,
/// and if so, replace it with the VLP CTE JOIN alias.
///
/// # Example
/// ```sql
/// -- Before rewrite:
/// SELECT COUNT(DISTINCT b.end_id)  -- ❌ b doesn't exist
/// FROM users AS a
/// LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
///
/// -- After rewrite:
/// SELECT COUNT(DISTINCT t.end_id)  -- ✅ t is the VLP CTE alias
/// FROM users AS a
/// LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
/// ```
pub fn rewrite_vlp_aggregate_aliases(plan: &mut RenderPlan) -> RenderPlanBuilderResult<()> {
    // Build mapping: VLP end node Cypher alias -> VLP CTE JOIN alias
    // Example: {"b": "t"} for `vlp_a_b AS t`
    let mut vlp_end_to_cte_alias: HashMap<String, String> = HashMap::new();

    // Extract VLP metadata from CTEs
    for cte in &plan.ctes.0 {
        if let Some(ref cypher_end_alias) = cte.vlp_cypher_end_alias {
            // Find the corresponding JOIN to get the CTE alias
            for join in &plan.joins.0 {
                if join.table_name == cte.cte_name {
                    log::info!(
                        "🔧 VLP aggregate rewrite: Mapping Cypher alias '{}' -> CTE alias '{}' (from CTE '{}')",
                        cypher_end_alias,
                        join.table_alias,
                        cte.cte_name
                    );
                    vlp_end_to_cte_alias.insert(cypher_end_alias.clone(), join.table_alias.clone());
                    break;
                }
            }
        }
    }

    // If no VLP end nodes found, nothing to rewrite
    if vlp_end_to_cte_alias.is_empty() {
        return Ok(());
    }

    log::debug!(
        "VLP aggregate rewrite: Found {} VLP end node(s) to rewrite",
        vlp_end_to_cte_alias.len()
    );

    // Rewrite SELECT items
    for item in &mut plan.select.items {
        rewrite_expr_for_vlp_end_nodes(&mut item.expression, &vlp_end_to_cte_alias);
    }

    // Rewrite GROUP BY expressions
    for expr in &mut plan.group_by.0 {
        rewrite_expr_for_vlp_end_nodes(expr, &vlp_end_to_cte_alias);
    }

    // Rewrite HAVING clause
    if let Some(ref mut having) = plan.having_clause {
        rewrite_expr_for_vlp_end_nodes(having, &vlp_end_to_cte_alias);
    }

    // Rewrite ORDER BY expressions
    for item in &mut plan.order_by.0 {
        rewrite_expr_for_vlp_end_nodes(&mut item.expression, &vlp_end_to_cte_alias);
    }

    Ok(())
}

/// Recursively rewrite a RenderExpr to replace VLP end node aliases with CTE aliases.
///
/// This function handles the conversion:
/// - `b.end_id` (where b is VLP end node) → `t.end_id` (where t is VLP CTE alias)
fn rewrite_expr_for_vlp_end_nodes(
    expr: &mut RenderExpr,
    vlp_end_to_cte_alias: &HashMap<String, String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Check if this property references a VLP end node
            if let Some(cte_alias) = vlp_end_to_cte_alias.get(&prop.table_alias.0) {
                log::info!(
                    "🔧 VLP aggregate rewrite: Replacing {}.{} with {}.{}",
                    prop.table_alias.0,
                    prop.column.raw(),
                    cte_alias,
                    prop.column.raw()
                );
                prop.table_alias = TableAlias(cte_alias.clone());
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite aggregate function arguments
            for arg in &mut agg.args {
                rewrite_expr_for_vlp_end_nodes(arg, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operator operands (handles DISTINCT)
            for operand in &mut op.operands {
                rewrite_expr_for_vlp_end_nodes(operand, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            // Recursively rewrite function arguments
            for arg in &mut func.args {
                rewrite_expr_for_vlp_end_nodes(arg, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::Case(case) => {
            // Rewrite CASE expression
            if let Some(ref mut e) = case.expr {
                rewrite_expr_for_vlp_end_nodes(e, vlp_end_to_cte_alias);
            }
            for (when, then) in &mut case.when_then {
                rewrite_expr_for_vlp_end_nodes(when, vlp_end_to_cte_alias);
                rewrite_expr_for_vlp_end_nodes(then, vlp_end_to_cte_alias);
            }
            if let Some(ref mut else_expr) = case.else_expr {
                rewrite_expr_for_vlp_end_nodes(else_expr, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            rewrite_expr_for_vlp_end_nodes(array, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(index, vlp_end_to_cte_alias);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            rewrite_expr_for_vlp_end_nodes(array, vlp_end_to_cte_alias);
            if let Some(ref mut f) = from {
                rewrite_expr_for_vlp_end_nodes(f, vlp_end_to_cte_alias);
            }
            if let Some(ref mut t) = to {
                rewrite_expr_for_vlp_end_nodes(t, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::InSubquery(subq) => {
            rewrite_expr_for_vlp_end_nodes(&mut subq.expr, vlp_end_to_cte_alias);
        }
        RenderExpr::List(items) => {
            // Recursively rewrite each element of the list
            for item in items {
                rewrite_expr_for_vlp_end_nodes(item, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            // Recursively rewrite each value expression in the map literal
            for (_key, value) in entries {
                rewrite_expr_for_vlp_end_nodes(value, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            // Recursively rewrite all subexpressions of the reduce expression
            rewrite_expr_for_vlp_end_nodes(&mut reduce.initial_value, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(&mut reduce.list, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(&mut reduce.expression, vlp_end_to_cte_alias);
        }
        // Remaining expression types are leaves and don't contain nested aliases
        _ => {}
    }
}

/// Build property mapping from select items for CTE column resolution.
/// Maps (alias, property) -> column_name for property access resolution.
///
/// This function handles three patterns:
/// 1. "alias.property" (dotted, used in VLP CTEs)
/// 2. "p{N}_alias_property" (new unambiguous CTE format)
/// 3. "alias_property" (legacy underscore, fallback for backward compat)
/// 4. No separator - aggregate column like "friends" from collect()
pub fn build_property_mapping_from_columns(
    select_items: &[SelectItem],
) -> HashMap<(String, String), String> {
    use crate::render_plan::render_expr::RenderExpr;
    let mut property_mapping = HashMap::new();

    for item in select_items {
        if let Some(col_alias) = &item.col_alias {
            let col_name = &col_alias.0;

            // Pattern 1: "alias.property" (dotted, used in VLP CTEs)
            if let Some(dot_pos) = col_name.find('.') {
                let alias = col_name[..dot_pos].to_string();
                let property = col_name[dot_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping: ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 2: "p{N}_alias_property" (new unambiguous CTE format)
            else if let Some((alias, property)) = parse_cte_column(col_name) {
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (p{{N}}): ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 3: "alias_property" (legacy underscore fallback)
            else if let Some(underscore_pos) = col_name.find('_') {
                let alias = col_name[..underscore_pos].to_string();
                let property = col_name[underscore_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (legacy underscore): ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 4: No separator - aggregate column like "friends" from collect()
            // Store with empty alias so ARRAY JOIN can find it: ("", column_name) → column_name
            else {
                property_mapping.insert(("".to_string(), col_name.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (aggregate): (\"\", {}) → {}",
                    col_name,
                    col_name
                );
            }
        }
    }

    log::info!(
        "Built property mapping with {} entries",
        property_mapping.len()
    );
    property_mapping
}

/// Placeholder for strip_database_prefix function
/// Will be moved from plan_builder.rs lines 116-124
/// Strip database prefix from table name (e.g., "db.table" -> "table")
/// Recursively rewrite RenderExpr to use VLP table aliases
pub fn rewrite_render_expr_for_vlp(expr: &mut RenderExpr, mappings: &HashMap<String, String>) {
    // This function is deprecated in favor of rewrite_render_expr_for_vlp_with_from_alias
    // which properly handles the FROM alias for VLP CTEs. Kept for backward compatibility.
    rewrite_render_expr_for_vlp_with_from_alias(expr, mappings, "t");
}

/// Enhanced version that takes the FROM alias into account.
/// For VLP CTEs, the FROM clause looks like: FROM vlp_a_b AS t
/// So we need to use the alias (t) when rendering, and also add property prefixes (start_/end_).
pub fn rewrite_render_expr_for_vlp_with_from_alias(
    expr: &mut RenderExpr,
    mappings: &HashMap<String, String>,
    vlp_from_alias: &str,
) {
    match expr {
        RenderExpr::Column(column) => {
            // Path functions use bare Column("path_nodes") that get qualified as t.path_nodes during SQL generation
            // We need to convert them to PropertyAccessExp so they can be rewritten
            // Check if this is a path function column (path_nodes, hop_count, path_relationships)
            let col_name_str = column.0.raw().to_string(); // Clone to avoid borrow issues
            if matches!(
                col_name_str.as_str(),
                "path_nodes" | "hop_count" | "path_relationships" | "path_edges"
            ) {
                log::info!(
                    "🔄 VLP: Converting Column({}) to PropertyAccessExp({}.{})",
                    col_name_str,
                    VLP_CTE_FROM_ALIAS,
                    col_name_str
                );
                // Replace Column with PropertyAccessExp using VLP FROM alias
                let _new_prop_access = PropertyAccess {
                    table_alias: TableAlias(VLP_CTE_FROM_ALIAS.to_string()),
                    column: PropertyValue::Column(col_name_str.clone()),
                };

                // Rewrite the table alias if it's in the mappings
                let rewritten_alias = mappings
                    .get(VLP_CTE_FROM_ALIAS)
                    .cloned()
                    .unwrap_or_else(|| vlp_from_alias.to_string());
                log::info!(
                    "🔄 Rewriting {}.{} → {}.{}",
                    VLP_CTE_FROM_ALIAS,
                    col_name_str,
                    rewritten_alias,
                    col_name_str
                );

                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rewritten_alias),
                    column: PropertyValue::Column(col_name_str),
                });
            }
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Check if this table alias needs rewriting
            if let Some(vlp_internal_alias) = mappings.get(&prop_access.table_alias.0) {
                // CRITICAL: Handle VLP property name rewriting with FROM alias
                // For normal VLP, the CTE has columns like:
                //   start_email, start_name, start_city, end_email, end_name, end_city
                // NOT just: email, name, city
                //
                // So when rewriting a.city → use the FROM alias + PREFIX the column:
                // 1. Keep the FROM alias (t): FROM vlp_a_b AS t
                // 2. PREFIX the column: city → start_city (for start node) or end_city (for end node)
                // 3. Final: t.start_city
                //
                // The mapping tells us the internal alias (start_node or end_node), which we use
                // to determine the prefix (start_ or end_).

                let col_name = prop_access.column.raw();

                // Determine if this is a start or end node based on the mapping
                let prefix = if vlp_internal_alias.starts_with("start_") {
                    "start_"
                } else if vlp_internal_alias.starts_with("end_") {
                    "end_"
                } else {
                    // Not a node alias, use as-is
                    ""
                };

                let prefixed_col = if !prefix.is_empty() {
                    format!("{}{}", prefix, col_name)
                } else {
                    col_name.to_string()
                };

                log::info!(
                    "🔄 VLP: Rewriting {}.{} → {}.{} (vlp_internal_alias={}, prefix={})",
                    prop_access.table_alias.0,
                    col_name,
                    vlp_from_alias,
                    prefixed_col,
                    vlp_internal_alias,
                    prefix
                );

                // Update both the alias (to FROM alias) and the column name (with prefix)
                prop_access.table_alias.0 = vlp_from_alias.to_string();
                if !prefix.is_empty() {
                    // Replace the column with the prefixed version
                    prop_access.column = PropertyValue::Column(prefixed_col);
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            for operand in &mut op_app.operands {
                rewrite_render_expr_for_vlp_with_from_alias(operand, mappings, vlp_from_alias);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                rewrite_render_expr_for_vlp_with_from_alias(arg, mappings, vlp_from_alias);
            }
        }
        RenderExpr::AggregateFnCall(func) => {
            for arg in &mut func.args {
                rewrite_render_expr_for_vlp_with_from_alias(arg, mappings, vlp_from_alias);
            }
        }
        RenderExpr::InSubquery(in_exp) => {
            rewrite_render_expr_for_vlp_with_from_alias(&mut in_exp.expr, mappings, vlp_from_alias);
        }
        RenderExpr::Case(case_exp) => {
            for (when_expr, then_expr) in &mut case_exp.when_then {
                rewrite_render_expr_for_vlp_with_from_alias(when_expr, mappings, vlp_from_alias);
                rewrite_render_expr_for_vlp_with_from_alias(then_expr, mappings, vlp_from_alias);
            }
            if let Some(else_expr) = &mut case_exp.else_expr {
                rewrite_render_expr_for_vlp_with_from_alias(else_expr, mappings, vlp_from_alias);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                rewrite_render_expr_for_vlp_with_from_alias(item, mappings, vlp_from_alias);
            }
        }
        // Other expression types don't contain table aliases
        _ => {}
    }
}

/// Enhanced version with NEW lookup-based mapping using complete metadata.
/// Maps (cypher_alias, db_column) → (cte_column_name, vlp_position)
/// NO HEURISTICS - all matching is direct and exact.
pub fn rewrite_render_expr_for_vlp_with_endpoint_info(
    expr: &mut RenderExpr,
    mappings: &HashMap<String, String>,
    vlp_from_alias: &str,
    endpoint_position: &HashMap<String, &str>,
    cte_column_mapping: &HashMap<
        (String, String),
        (String, crate::render_plan::cte_manager::VlpColumnPosition),
    >,
) {
    log::debug!("🔍 REWRITE: Processing expr with new lookup-based mapping (no splitting)");
    match expr {
        RenderExpr::TableAlias(alias) => {
            let alias_str = alias.0.clone();
            log::debug!(
                "🔍 REWRITE TableAlias: alias='{}', in_mappings={}",
                alias_str,
                mappings.contains_key(&alias_str)
            );
            // Check if this is a Cypher alias (mapping exists)
            if mappings.contains_key(&alias_str) {
                // For VLP endpoints, TableAlias should be rewritten to the CTE column
                // E.g., TableAlias("b") → Column("t.end_id")
                if let Some((cte_column_name, _position)) =
                    cte_column_mapping.get(&(alias_str.clone(), "id".to_string()))
                {
                    log::debug!(
                        "✅ REWRITE: TableAlias '{}' → Column('{}')",
                        alias_str,
                        cte_column_name
                    );
                    *expr =
                        RenderExpr::Column(Column(PropertyValue::Column(cte_column_name.clone())));
                } else {
                    log::debug!(
                        "❌ REWRITE: TableAlias '{}' not in cte_column_mapping for 'id'",
                        alias_str
                    );
                }
            }
            // No change needed if not in mappings
        }

        RenderExpr::PropertyAccessExp(prop_access) => {
            let alias = prop_access.table_alias.0.clone();
            let col_name = prop_access.column.raw();

            log::debug!(
                "🔍 REWRITE PropertyAccessExp: alias='{}', col_name='{}', in_mappings={}",
                alias,
                col_name,
                mappings.contains_key(&alias)
            );

            // Check if this is a Cypher alias (mapping exists)
            if mappings.contains_key(&alias) {
                log::debug!(
                    "✅ REWRITE: Found table_alias '{}' in mappings",
                    prop_access.table_alias.0
                );

                let col_name = prop_access.column.raw();
                let alias = prop_access.table_alias.0.clone();

                // NEW ALGORITHM: Direct lookup using DB column name
                // No splitting, no guessing - just look up the exact DB column name
                if let Some((cte_column_name, _position)) =
                    cte_column_mapping.get(&(alias.clone(), col_name.to_string()))
                {
                    log::debug!(
                        "✅ REWRITE: Direct lookup SUCCESS: ({}, {}) → {}",
                        alias,
                        col_name,
                        cte_column_name
                    );

                    // Rewrite to use the CTE column
                    prop_access.table_alias.0 = vlp_from_alias.to_string();
                    prop_access.column = PropertyValue::Column(cte_column_name.clone());
                } else {
                    // Fallback: construct from endpoint_position
                    // This handles cases where metadata wasn't fully populated
                    let prefix = match endpoint_position.get(alias.as_str()) {
                        Some(&"start") => "start_",
                        Some(&"end") => "end_",
                        _ => "",
                    };

                    let fallback_col = format!("{}{}", prefix, col_name);
                    log::debug!(
                        "⚠️ REWRITE: Lookup FAILED for ({}, {}), falling back to: {}",
                        alias,
                        col_name,
                        fallback_col
                    );

                    prop_access.table_alias.0 = vlp_from_alias.to_string();
                    prop_access.column = PropertyValue::Column(fallback_col);
                }
            }
        }
        RenderExpr::Case(case_expr) => {
            // Recursively rewrite expressions in the CASE
            for (when_expr, then_expr) in &mut case_expr.when_then {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    when_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    then_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
            if let Some(else_expr) = &mut case_expr.else_expr {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    else_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            for operand in &mut op_app.operands {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    operand,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            for arg in &mut fn_call.args {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    arg,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::AggregateFnCall(fn_call) => {
            for arg in &mut fn_call.args {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    arg,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::InSubquery(in_exp) => {
            rewrite_render_expr_for_vlp_with_endpoint_info(
                &mut in_exp.expr,
                mappings,
                vlp_from_alias,
                endpoint_position,
                cte_column_mapping,
            );
        }
        _ => {
            // Other expression types don't need rewriting
        }
    }
}

/// Legacy version kept for compatibility - redirects to new implementation
pub fn rewrite_render_expr_for_vlp_with_endpoint_info_legacy(
    _expr: &mut RenderExpr,
    _mappings: &HashMap<String, String>,
    _vlp_from_alias: &str,
    _endpoint_position: &HashMap<String, &str>,
    _old_mapping: &HashMap<(String, String), String>,
) {
    log::debug!("🔍 LEGACY REWRITE: This function is deprecated, use new lookup-based version");
}

pub fn extract_cte_references(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut refs = HashMap::new();

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::info!(
                "🔍 extract_cte_references: Found GraphJoins with {} CTE refs: {:?}",
                gj.cte_references.len(),
                gj.cte_references
            );
            refs.extend(gj.cte_references.clone());
            refs.extend(extract_cte_references(&gj.input));
        }
        LogicalPlan::GraphRel(gr) => {
            refs.extend(extract_cte_references(&gr.left));
            refs.extend(extract_cte_references(&gr.center));
            refs.extend(extract_cte_references(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            refs.extend(extract_cte_references(&gn.input));
        }
        LogicalPlan::WithClause(wc) => {
            log::info!(
                "🔍 extract_cte_references: Found WithClause with {} CTE refs: {:?}",
                wc.cte_references.len(),
                wc.cte_references
            );
            refs.extend(wc.cte_references.clone());
            refs.extend(extract_cte_references(&wc.input));
        }
        LogicalPlan::CartesianProduct(cp) => {
            refs.extend(extract_cte_references(&cp.left));
            refs.extend(extract_cte_references(&cp.right));
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                refs.extend(extract_cte_references(input));
            }
        }
        _ => {}
    }

    log::info!(
        "🔍 extract_cte_references: Returning {} refs total: {:?}",
        refs.len(),
        refs
    );
    refs
}

pub fn extract_correlation_predicates(
    plan: &LogicalPlan,
) -> Vec<crate::query_planner::logical_expr::LogicalExpr> {
    let mut predicates = vec![];

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::debug!("🔍 extract_correlation_predicates: Found GraphJoins with {} correlation predicates",
                       gj.correlation_predicates.len());
            predicates.extend(gj.correlation_predicates.clone());
            predicates.extend(extract_correlation_predicates(&gj.input));
        }
        LogicalPlan::GraphRel(gr) => {
            predicates.extend(extract_correlation_predicates(&gr.left));
            predicates.extend(extract_correlation_predicates(&gr.center));
            predicates.extend(extract_correlation_predicates(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            predicates.extend(extract_correlation_predicates(&gn.input));
        }
        LogicalPlan::WithClause(wc) => {
            predicates.extend(extract_correlation_predicates(&wc.input));
        }
        LogicalPlan::CartesianProduct(cp) => {
            // CRITICAL: Extract join_condition from CartesianProduct - this is where
            // cross-table WITH correlation predicates (e.g., a.user_id = c.user_id) are stored!
            if let Some(ref join_cond) = cp.join_condition {
                log::debug!(
            "🔍 extract_correlation_predicates: Found CartesianProduct.join_condition: {:?}",
                    join_cond
                );
                predicates.push(join_cond.clone());
            }
            predicates.extend(extract_correlation_predicates(&cp.left));
            predicates.extend(extract_correlation_predicates(&cp.right));
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                predicates.extend(extract_correlation_predicates(input));
            }
        }
        // CRITICAL: Handle wrapper types that may contain CartesianProduct
        LogicalPlan::Projection(proj) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Projection");
            predicates.extend(extract_correlation_predicates(&proj.input));
        }
        LogicalPlan::Limit(lim) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Limit");
            predicates.extend(extract_correlation_predicates(&lim.input));
        }
        LogicalPlan::OrderBy(ob) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through OrderBy");
            predicates.extend(extract_correlation_predicates(&ob.input));
        }
        LogicalPlan::Filter(f) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Filter");
            predicates.extend(extract_correlation_predicates(&f.input));
        }
        LogicalPlan::GroupBy(gb) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through GroupBy");
            predicates.extend(extract_correlation_predicates(&gb.input));
        }
        _ => {
            log::debug!("🔍 extract_correlation_predicates: Unhandled plan type, not recursing");
        }
    }

    log::info!(
        "🔍 extract_correlation_predicates: Returning {} predicates total",
        predicates.len()
    );
    predicates
}

pub fn convert_correlation_predicates_to_joins(
    predicates: &[crate::query_planner::logical_expr::LogicalExpr],
    cte_references: &HashMap<String, String>,
) -> Vec<(String, String, String, String)> {
    use crate::query_planner::logical_expr::{LogicalExpr, Operator};

    let mut conditions = vec![];

    for pred in predicates {
        if let LogicalExpr::OperatorApplicationExp(op_app) = pred {
            if matches!(op_app.operator, Operator::Equal) && op_app.operands.len() == 2 {
                let left = &op_app.operands[0];
                let right = &op_app.operands[1];

                // Check if we have a CTE reference on one side and a table reference on the other
                if let Some(cond) = extract_join_from_logical_equality(left, right, cte_references)
                {
                    log::info!(
                        "🔧 Converted correlation predicate to join: CTE {}.{} = {}.{}",
                        cond.0,
                        cond.1,
                        cond.2,
                        cond.3
                    );
                    conditions.push(cond);
                } else if let Some(cond) =
                    extract_join_from_logical_equality(right, left, cte_references)
                {
                    conditions.push(cond);
                }
            }
        }
    }

    log::info!(
        "🔧 convert_correlation_predicates_to_joins: Converted {} predicates to join conditions",
        conditions.len()
    );
    conditions
}

/// Extract join condition from a LogicalExpr equality comparison.
/// Handles patterns like: src2.ip = source_ip (where source_ip is a CTE column)
pub fn extract_join_from_logical_equality(
    left: &crate::query_planner::logical_expr::LogicalExpr,
    right: &crate::query_planner::logical_expr::LogicalExpr,
    cte_references: &HashMap<String, String>,
) -> Option<(String, String, String, String)> {
    use crate::query_planner::logical_expr::LogicalExpr;

    // Pattern 1: Left is table.column (PropertyAccess), Right is CTE variable
    // Example: src2.ip = source_ip
    if let LogicalExpr::PropertyAccessExp(prop) = left {
        if let LogicalExpr::ColumnAlias(var_name) = right {
            // Check if variable references a CTE column
            if let Some(cte_name) = cte_references.get(&var_name.0) {
                return Some((
                    cte_name.clone(),
                    var_name.0.clone(),
                    prop.table_alias.0.clone(),
                    prop.column.raw().to_string(),
                ));
            }
        }
    }

    // Pattern 2: Left is CTE variable, Right is table.column
    // Example: source_ip = src2.ip
    if let LogicalExpr::ColumnAlias(var_name) = left {
        if let LogicalExpr::PropertyAccessExp(prop) = right {
            if let Some(cte_name) = cte_references.get(&var_name.0) {
                return Some((
                    cte_name.clone(),
                    var_name.0.clone(),
                    prop.table_alias.0.clone(),
                    prop.column.raw().to_string(),
                ));
            }
        }
    }

    None
}

/// Rewrite an OperatorApplication for CTE JOIN conditions.
/// Find the ID column name in a WITH CTE for a given alias
/// Looks for columns like `{alias}_user_id`, `{alias}_id`, etc.
fn find_id_column_in_cte(cte_name: &str, cte_alias: &str, ctes: &super::CteItems) -> String {
    for cte in &ctes.0 {
        if cte.cte_name == cte_name {
            // Look for ID columns in the CTE's column metadata
            for col in &cte.columns {
                let col_name = &col.cte_column_name;
                if col_name.as_str() == format!("{}_user_id", cte_alias)
                    || col_name.as_str() == format!("{}_id", cte_alias)
                {
                    return col_name.clone();
                }
            }
            // Fallback: check for any column ending in "_id" or "_user_id"
            for col in &cte.columns {
                let col_name = &col.cte_column_name;
                if col_name.starts_with(&format!("{}_", cte_alias))
                    && (col_name.ends_with("_id") || col_name.ends_with("_user_id"))
                {
                    return col_name.clone();
                }
            }
            // If CTE is structured, look at SELECT items
            if let super::CteContent::Structured(plan) = &cte.content {
                for item in &plan.select.items {
                    if let Some(alias) = &item.col_alias {
                        let alias_str = &alias.0;
                        if alias_str == &format!("{}_user_id", cte_alias)
                            || alias_str == &format!("{}_id", cte_alias)
                        {
                            return alias_str.clone();
                        }
                    }
                }
            }
        }
    }
    // Ultimate fallback
    format!("{}_user_id", cte_alias)
}

/// Rewrite a RenderExpr to use CTE column names where applicable.
/// Converts property access expressions to use CTE column naming convention.
/// E.g., a.user_id becomes a_b.a_user_id (where a_b is the CTE alias)
fn rewrite_operator_application_for_cte_join(
    op_app: &OperatorApplication,
    cte_alias: &str,
    cte_references: &HashMap<String, String>,
) -> OperatorApplication {
    // Rewrite operands to use CTE column names
    let rewritten_operands: Vec<RenderExpr> = op_app
        .operands
        .iter()
        .map(|operand| rewrite_render_expr_for_cte_operand(operand, cte_alias, cte_references))
        .collect();

    OperatorApplication {
        operator: op_app.operator,
        operands: rewritten_operands,
    }
}

/// Public version for use by join_builder
/// Rewrites operator application to use CTE column names.
/// The table alias is kept (e.g., "o" stays "o") but column becomes "o_user_id".
pub fn rewrite_operator_application_for_cte(
    op_app: &OperatorApplication,
    cte_references: &HashMap<String, String>,
) -> OperatorApplication {
    // Rewrite operands to use CTE column names
    let rewritten_operands: Vec<RenderExpr> = op_app
        .operands
        .iter()
        .map(|operand| rewrite_render_expr_for_cte_simple(operand, cte_references))
        .collect();

    OperatorApplication {
        operator: op_app.operator,
        operands: rewritten_operands,
    }
}

/// Simple CTE expression rewriting - just prefixes column names, keeps table alias the same.
/// E.g., o.user_id -> o.o_user_id (when "o" is in cte_references)
fn rewrite_render_expr_for_cte_simple(
    expr: &RenderExpr,
    cte_references: &HashMap<String, String>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            // Check if this alias is from a CTE
            if cte_references.contains_key(&pa.table_alias.0) {
                // Rewrite column to use CTE naming: alias_column
                // Keep the same table alias (e.g., "o" stays "o")
                let cte_column = cte_column_name(&pa.table_alias.0, &pa.column.raw());
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: pa.table_alias.clone(), // Keep same table alias
                    column: PropertyValue::Column(cte_column),
                })
            } else {
                // Not a CTE reference, keep as-is
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application_for_cte(inner_op, cte_references),
        ),
        _ => expr.clone(),
    }
}

/// Rewrite a RenderExpr operand to use CTE column names where applicable.
/// Helper function that avoids needing cte_schemas parameter.
fn rewrite_render_expr_for_cte_operand(
    expr: &RenderExpr,
    cte_alias: &str,
    cte_references: &HashMap<String, String>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            // Check if this alias is from a CTE
            if cte_references.contains_key(&pa.table_alias.0) {
                // Rewrite to use CTE alias and column naming
                let cte_column = cte_column_name(&pa.table_alias.0, &pa.column.raw());
                log::info!(
                    "🔧 Rewriting property access: {}.{} -> {}.{}",
                    pa.table_alias.0,
                    pa.column.raw(),
                    cte_alias,
                    cte_column
                );
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_alias.to_string()),
                    column: PropertyValue::Column(cte_column),
                })
            } else {
                // Not a CTE reference, keep as-is
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application_for_cte_join(inner_op, cte_alias, cte_references),
        ),
        _ => expr.clone(),
    }
}

/// Rewrite a RenderExpr to use CTE column names where applicable.
fn rewrite_render_expr_for_cte(
    expr: &RenderExpr,
    cte_alias: &str,
    cte_references: &HashMap<String, String>,
    _cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderExpr {
    let ctx = crate::render_plan::expression_utils::CTERewriteContext::new(
        cte_alias.to_string(),
        cte_alias.to_string(),
        cte_references.clone(),
    );
    rewrite_render_expr_for_cte_with_context(expr, &ctx)
}

/// Rewrite render expressions using CTE context for complex JOIN scenarios
///
/// Rewrites property accesses to use CTE alias and column naming patterns.
/// Used when JOIN conditions need to reference columns from CTE-sourced aliases.
fn rewrite_render_expr_for_cte_with_context(
    expr: &RenderExpr,
    ctx: &crate::render_plan::expression_utils::CTERewriteContext,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            // Check if this alias is from a CTE
            if ctx.cte_references.contains_key(&pa.table_alias.0) {
                // Rewrite to use CTE alias and column naming
                let cte_column = cte_column_name(&pa.table_alias.0, &pa.column.raw());
                log::debug!(
                    "🔧 Rewriting property access: {}.{} -> {}.{}",
                    pa.table_alias.0,
                    pa.column.raw(),
                    ctx.cte_name,
                    cte_column
                );
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(ctx.cte_name.clone()),
                    column: PropertyValue::Column(cte_column),
                })
            } else {
                // Not a CTE reference, keep as-is
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application_for_cte_join(inner_op, &ctx.cte_name, &ctx.cte_references),
        ),
        _ => expr.clone(),
    }
}

/// Extract a JOIN condition from a filter expression.
/// Looks for equality patterns between CTE aliases and other tables.
fn extract_cte_join_condition_from_filter(
    filter_expr: &RenderExpr,
    cte_alias: &str,
    cte_aliases: &[String],
    cte_references: &HashMap<String, String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> Option<OperatorApplication> {
    match filter_expr {
        RenderExpr::OperatorApplicationExp(op_app) => {
            match op_app.operator {
                Operator::Equal if op_app.operands.len() == 2 => {
                    let left = &op_app.operands[0];
                    let right = &op_app.operands[1];

                    // Check if one side references a CTE alias
                    let left_is_cte = if let RenderExpr::PropertyAccessExp(pa) = left {
                        cte_aliases.iter().any(|a| &pa.table_alias.0 == a)
                    } else {
                        false
                    };

                    let right_is_cte = if let RenderExpr::PropertyAccessExp(pa) = right {
                        cte_aliases.iter().any(|a| &pa.table_alias.0 == a)
                    } else {
                        false
                    };

                    // If one side is CTE and other is not, this is a join condition
                    if (left_is_cte && !right_is_cte) || (!left_is_cte && right_is_cte) {
                        return Some(rewrite_operator_application_for_cte_join(
                            op_app,
                            cte_alias,
                            cte_references,
                        ));
                    }
                    None
                }
                Operator::And => {
                    // Try both operands
                    for operand in &op_app.operands {
                        if let Some(cond) = extract_cte_join_condition_from_filter(
                            operand,
                            cte_alias,
                            cte_aliases,
                            cte_references,
                            cte_schemas,
                        ) {
                            return Some(cond);
                        }
                    }
                    None
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Extract join condition from equality, for CTE correlation predicates
/// - Maps Cypher node aliases (u, x) to VLP internal aliases (start_node, end_node)
/// - Handles denormalized VLP patterns where both nodes are in the same table
/// - Maps path function aliases ("t") to actual VLP CTE aliases
/// - Skips multi-type VLP CTEs which use Cypher aliases directly
pub fn extract_vlp_alias_mappings(ctes: &crate::render_plan::CteItems) -> HashMap<String, String> {
    let mut mappings = HashMap::new();

    for (idx, cte) in ctes.0.iter().enumerate() {
        log::info!(
            "🔍 CTE[{}]: name={}, vlp_start={:?}, vlp_cypher_start={:?}",
            idx,
            cte.cte_name,
            cte.vlp_start_alias,
            cte.vlp_cypher_start_alias
        );

        // Skip alias mappings for multi-type VLP CTEs - they use Cypher aliases directly
        // and properties are extracted via JSON_VALUE() using the Cypher alias
        if cte.cte_name.starts_with("vlp_multi_type_") {
            log::debug!("🔄 VLP: Skipping alias mapping for multi-type VLP CTE (uses Cypher alias directly)");
            continue;
        }

        // Check if this is a VLP CTE with metadata
        if let Some(cypher_start) = &cte.vlp_cypher_start_alias {
            // Get the VLP internal alias, defaulting to "start_node" if not set
            let vlp_start = cte
                .vlp_start_alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "start_node".to_string());

            // Check if this is a denormalized VLP (both nodes in same table)
            // ✅ PHASE 2 APPROVED: Derives denormalization from schema structure, not flag
            let is_denormalized =
                cte.vlp_start_table == cte.vlp_end_table && cte.vlp_start_table.is_some();

            if is_denormalized {
                // For denormalized VLP, map Cypher alias directly to VLP CTE alias
                // (not to internal VLP aliases like "start_node")
                let vlp_cte_alias = cte
                    .cte_name
                    .replace("vlp_cte", "vlp")
                    .replace("chained_path_", "vlp");
                log::info!(
                    "🔄 VLP mapping (denormalized): {} → {}",
                    cypher_start,
                    vlp_cte_alias
                );
                mappings.insert(cypher_start.clone(), vlp_cte_alias.clone());
            } else {
                log::debug!("🔄 VLP mapping: {} → {}", cypher_start, vlp_start);
                mappings.insert(cypher_start.clone(), vlp_start.clone());
            }
        }

        if let Some(cypher_end) = &cte.vlp_cypher_end_alias {
            // Get the VLP internal alias, defaulting to "end_node" if not set
            let vlp_end = cte
                .vlp_end_alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "end_node".to_string());

            // Check if this is a denormalized VLP (both nodes in same table)
            // ✅ PHASE 2 APPROVED: Same structural check as above
            let is_denormalized =
                cte.vlp_start_table == cte.vlp_end_table && cte.vlp_start_table.is_some();

            if is_denormalized {
                // For denormalized VLP, map Cypher alias directly to VLP CTE alias
                let vlp_cte_alias = cte
                    .cte_name
                    .replace("vlp_cte", "vlp")
                    .replace("chained_path_", "vlp");
                log::info!(
                    "🔄 VLP mapping (denormalized): {} → {}",
                    cypher_end,
                    vlp_cte_alias
                );
                mappings.insert(cypher_end.clone(), vlp_cte_alias.clone());
            } else {
                log::debug!("🔄 VLP mapping: {} → {}", cypher_end, vlp_end);
                mappings.insert(cypher_end.clone(), vlp_end.clone());
            }
        }

        // 🔧 FIX: Map VLP FROM alias to the actual VLP CTE alias
        // When rewrite_logical_path_functions converts length(path) → t.hop_count,
        // we need to rewrite "t" to the actual VLP alias (e.g., "vlp1", "vlp2")
        if cte.cte_name.starts_with("vlp_cte") || cte.cte_name.starts_with("chained_path_") {
            // Extract VLP alias from CTE name: vlp_cte1 → vlp1, vlp_cte2 → vlp2
            let vlp_alias = cte
                .cte_name
                .replace("vlp_cte", "vlp")
                .replace("chained_path_", "vlp");
            log::debug!(
                "🔄 VLP path function mapping: {} → {}",
                VLP_CTE_FROM_ALIAS,
                vlp_alias
            );
            mappings.insert(VLP_CTE_FROM_ALIAS.to_string(), vlp_alias.clone());

            // ⚠️ TODO: REMOVE THIS FALLBACK - PROPER FIX REQUIRED
            // See notes/HOLISTIC_FIX_METHODOLOGY.md for details
            //
            // This fallback blindly maps relationship aliases (f, r, e, t1-t99) to VLP CTE aliases.
            // This is INCORRECT because:
            // 1. Relationship property filters (e.g., f.flight_number = 123) should be applied
            // ✅ HOLISTIC FIX (Dec 26, 2025): Relationship filters now properly handled in CTE generation
            // - FK-edge patterns: Map to start_node/new_start/current_node in cte_extraction.rs
            // - Standard patterns: Map to rel alias in cte_extraction.rs
            // - Denormalized patterns: Map to rel alias in cte_extraction.rs
            // No fallback mapping needed - filters are applied inside the CTE where they belong.
            log::debug!(
                "VLP relationship filters handled in CTE generation - no fallback mapping needed"
            );
        }
    }

    mappings
}

/// Try to extract a CTE join condition from an equality comparison.
/// Returns: Some((cte_name, cte_column, main_table_alias, main_column)) if found
/// Returns the alias name if found, None otherwise.
pub fn extract_alias_from_expr(expr: &LogicalExpr) -> Option<String> {
    match expr {
        LogicalExpr::ColumnAlias(ca) => {
            log::debug!("🔍 extract_with_alias: ColumnAlias: {}", ca.0);
            Some(ca.0.clone())
        }
        LogicalExpr::TableAlias(ta) => {
            log::debug!("🔍 extract_with_alias: TableAlias: {}", ta.0);
            Some(ta.0.clone())
        }
        LogicalExpr::Column(col) => {
            // A bare column name - this is often the variable name in WITH
            // e.g., WITH friend -> Column("friend")
            // Skip "*" since it's not a real variable name
            if col.0 == "*" {
                log::debug!("🔍 extract_with_alias: Skipping Column('*')");
                None
            } else {
                log::debug!("🔍 extract_with_alias: Column: {}", col.0);
                Some(col.0.clone())
            }
        }
        LogicalExpr::PropertyAccessExp(pa) => {
            // For property access like `friend.name`, use the table alias
            log::info!(
                "🔍 extract_with_alias: PropertyAccessExp: {}.{:?}",
                pa.table_alias.0,
                pa.column
            );
            Some(pa.table_alias.0.clone())
        }
        LogicalExpr::OperatorApplicationExp(op_app) => {
            // Handle operators like DISTINCT that wrap other expressions
            // Try to extract alias from the first operand
            log::debug!(
                "🔍 extract_with_alias: OperatorApplicationExp with {:?}, checking operands",
                op_app.operator
            );
            for operand in &op_app.operands {
                if let Some(alias) = extract_alias_from_expr(operand) {
                    return Some(alias);
                }
            }
            None
        }
        other => {
            log::info!(
                "🔍 extract_with_alias: Unhandled expression type in nested: {:?}",
                std::mem::discriminant(other)
            );
            None
        }
    }
}

/// Collect aliases from a single RenderExpr into a HashSet.
/// Recursively traverses PropertyAccessExp, OperatorApplicationExp, and ScalarFnCall expressions
/// to collect all table aliases referenced in the expression.
pub fn collect_aliases_from_single_render_expr(
    expr: &crate::render_plan::render_expr::RenderExpr,
    aliases: &mut std::collections::HashSet<String>,
) {
    match expr {
        crate::render_plan::render_expr::RenderExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        crate::render_plan::render_expr::RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_single_render_expr(operand, aliases);
            }
        }
        crate::render_plan::render_expr::RenderExpr::ScalarFnCall(fn_call) => {
            for arg in &fn_call.args {
                collect_aliases_from_single_render_expr(arg, aliases);
            }
        }
        _ => {}
    }
}

/// Extract CTE references from a logical plan recursively.
/// Returns a HashMap of CTE references found in the plan.
pub fn extract_cte_references_from_plan(
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

/// Extract DISTINCT flag from a logical plan recursively.
/// Returns true if any Projection in the plan has DISTINCT set.
pub fn extract_distinct(plan: &LogicalPlan) -> bool {
    // Extract distinct flag from Projection nodes
    let result = match plan {
        LogicalPlan::Projection(projection) => {
            crate::debug_println!(
                "DEBUG extract_distinct: Found Projection, distinct={}",
                projection.distinct
            );
            projection.distinct
        }
        LogicalPlan::OrderBy(order_by) => {
            crate::debug_println!("DEBUG extract_distinct: OrderBy, recursing");
            extract_distinct(&order_by.input)
        }
        LogicalPlan::Skip(skip) => {
            crate::debug_println!("DEBUG extract_distinct: Skip, recursing");
            extract_distinct(&skip.input)
        }
        LogicalPlan::Limit(limit) => {
            crate::debug_println!("DEBUG extract_distinct: Limit, recursing");
            extract_distinct(&limit.input)
        }
        LogicalPlan::GroupBy(group_by) => {
            crate::debug_println!("DEBUG extract_distinct: GroupBy, recursing");
            extract_distinct(&group_by.input)
        }
        LogicalPlan::GraphJoins(graph_joins) => {
            crate::debug_println!("DEBUG extract_distinct: GraphJoins, recursing");
            extract_distinct(&graph_joins.input)
        }
        LogicalPlan::Filter(filter) => {
            crate::debug_println!("DEBUG extract_distinct: Filter, recursing");
            extract_distinct(&filter.input)
        }
        _ => {
            crate::debug_println!("DEBUG extract_distinct: Other variant, returning false");
            false
        }
    };
    crate::debug_println!("DEBUG extract_distinct: Returning {}", result);
    result
}

/// Extract filters from a LogicalPlan node.
///
/// This function recursively traverses the plan tree to collect all filter predicates
/// that should be applied to the query, including view filters, schema filters,
/// WHERE predicates, and cycle prevention filters for variable-length paths.
pub fn extract_filters(plan: &LogicalPlan) -> RenderPlanBuilderResult<Option<RenderExpr>> {
    let filters = match plan {
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
                // Use the VLP default alias for standalone ViewScans
                // In practice, these will be wrapped in GraphNode which provides the alias
                if let Ok(sql) = schema_filter.to_sql(VLP_CTE_FROM_ALIAS) {
                    log::debug!("ViewScan: Adding schema filter: {}", sql);
                    filters.push(RenderExpr::Raw(sql));
                }
            }

            if filters.is_empty() {
                None
            } else if filters.len() == 1 {
                // Safety: len() == 1 guarantees next() returns Some
                Some(
                    filters
                        .into_iter()
                        .next()
                        .expect("filters has exactly one element"),
                )
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
                    .expect("filters is non-empty, reduce succeeds");
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
                    // Safety: len() == 1 guarantees next() returns Some
                    return Ok(Some(
                        filters
                            .into_iter()
                            .next()
                            .expect("filters has exactly one element"),
                    ));
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
                        .expect("filters is non-empty, reduce succeeds");
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
                        crate::debug_println!(
                            "DEBUG: extract_filters - Adding cycle prevention for fixed-length *{}",
                            exact_hops
                        );

                        // Check if this is a denormalized pattern
                        let is_denormalized = is_node_denormalized(&graph_rel.left)
                            && is_node_denormalized(&graph_rel.right);

                        // Extract table/column info for cycle prevention
                        // Use extract_table_name directly to avoid wrong fallbacks
                        let start_table = extract_table_name(&graph_rel.left).ok_or_else(|| {
                            RenderBuildError::MissingTableInfo(
                                "start node in cycle prevention".to_string(),
                            )
                        })?;
                        let end_table = extract_table_name(&graph_rel.right).ok_or_else(|| {
                            RenderBuildError::MissingTableInfo(
                                "end node in cycle prevention".to_string(),
                            )
                        })?;

                        let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                            RelationshipColumns {
                                from_id: Identifier::Single("from_node_id".to_string()),
                                to_id: Identifier::Single("to_node_id".to_string()),
                            },
                        );

                        // For denormalized, use relationship columns directly
                        // For normal, use node ID columns
                        let (start_id_col, end_id_col) = if is_denormalized {
                            (rel_cols.from_id.to_string(), rel_cols.to_id.to_string())
                        } else {
                            // Use extract_end_node_id_column for nested GraphRel patterns
                            // (e.g., (a)-[:R]->(b)-[:VLP*]->(c) where left is a GraphRel).
                            // extract_id_column follows rel.center (relationship table) returning FKs
                            // while extract_end_node_id_column follows rel.right (end node).
                            let start = extract_end_node_id_column(&graph_rel.left)
                                .unwrap_or_else(|| table_to_id_column(&start_table));
                            let end = extract_end_node_id_column(&graph_rel.right)
                                .unwrap_or_else(|| table_to_id_column(&end_table));
                            (start, end)
                        };

                        // Generate cycle prevention filters
                        let rel_to_id_str = rel_cols.to_id.to_string();
                        let rel_from_id_str = rel_cols.from_id.to_string();
                        if let Some(cycle_filter) =
                            crate::render_plan::cte_extraction::generate_cycle_prevention_filters(
                                exact_hops,
                                &start_id_col,
                                &rel_to_id_str,
                                &rel_from_id_str,
                                &end_id_col,
                                &graph_rel.left_connection,
                                &graph_rel.right_connection,
                            )
                        {
                            crate::debug_println!(
                                "DEBUG: extract_filters - Generated cycle prevention filter"
                            );
                            all_predicates.push(cycle_filter);
                        }
                    }
                }
            }

            if all_predicates.is_empty() {
                None
            } else if all_predicates.len() == 1 {
                log::trace!("Found 1 GraphRel predicate");
                // Safety: len() == 1 guarantees next() returns Some
                Some(
                    all_predicates
                        .into_iter()
                        .next()
                        .expect("all_predicates has exactly one element"),
                )
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
                    .expect("all_predicates is non-empty, reduce succeeds");
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
            if let Some(input_filter) = extract_filters(&filter.input)? {
                crate::debug_println!(
                    "DEBUG: extract_filters - Combining Filter predicate with input schema filter"
                );
                // Combine the Filter predicate with input's schema filter using AND
                Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![input_filter, expr],
                }))
            } else {
                crate::debug_println!(
                    "DEBUG: extract_filters - Returning Filter predicate only (no input filter)"
                );
                Some(expr)
            }
        }
        LogicalPlan::Projection(projection) => {
            crate::debug_println!(
                "DEBUG: extract_filters - Projection, recursing to input type: {:?}",
                std::mem::discriminant(&*projection.input)
            );
            extract_filters(&projection.input)?
        }
        LogicalPlan::GroupBy(group_by) => extract_filters(&group_by.input)?,
        LogicalPlan::OrderBy(order_by) => extract_filters(&order_by.input)?,
        LogicalPlan::Skip(skip) => extract_filters(&skip.input)?,
        LogicalPlan::Limit(limit) => extract_filters(&limit.input)?,
        LogicalPlan::Cte(cte) => extract_filters(&cte.input)?,
        LogicalPlan::GraphJoins(graph_joins) => extract_filters(&graph_joins.input)?,
        LogicalPlan::Union(_) => None,
        LogicalPlan::PageRank(_) => None,
        LogicalPlan::Unwind(u) => extract_filters(&u.input)?,
        LogicalPlan::CartesianProduct(cp) => {
            // Combine filters from both sides with AND
            let left_filters = extract_filters(&cp.left)?;
            let right_filters = extract_filters(&cp.right)?;

            // DEBUG: Log what we're extracting
            log::debug!("🔍 CartesianProduct extract_filters:");
            log::debug!("  Left filters: {:?}", left_filters);
            log::debug!("  Right filters: {:?}", right_filters);

            match (left_filters, right_filters) {
                (None, None) => None,
                (Some(l), None) => {
                    log::debug!("  ✅ Returning left filters only");
                    Some(l)
                }
                (None, Some(r)) => {
                    log::debug!("  ✅ Returning right filters only");
                    Some(r)
                }
                (Some(l), Some(r)) => {
                    log::debug!("  ⚠️ BOTH sides have filters - combining with AND!");
                    log::debug!("  ⚠️ This may cause duplicates if filters are the same!");
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![l, r],
                    }))
                }
            }
        }
        LogicalPlan::WithClause(wc) => extract_filters(&wc.input)?,
    };
    Ok(filters)
}

/// Extract FROM clause from a LogicalPlan
///
/// This function determines which table should be the FROM table in the generated SQL.
/// It handles various LogicalPlan types and applies different logic based on the plan structure.
pub fn extract_from(plan: &LogicalPlan) -> RenderPlanBuilderResult<Option<FromTable>> {
    log::debug!(
        "🔍 extract_from START: plan type={:?}",
        std::mem::discriminant(plan)
    );

    let from_ref = match plan {
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
                    let mut from_ref = from_table_to_view_ref(extract_from(&graph_node.input)?);
                    // Use this GraphNode's alias
                    if let Some(ref mut view_ref) = from_ref {
                        view_ref.alias = Some(graph_node.alias.clone());
                    }
                    from_ref
                }
            }
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // VARIABLE-LENGTH PATH CHECK
            // For variable-length paths, use the CTE as FROM instead of the start node
            if graph_rel.variable_length.is_some() {
                log::debug!(
                    "🔍 extract_from GraphRel: Variable-length path detected, using CTE as FROM"
                );

                // Generate CTE name consistent with CTE generation logic
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type = rel_types.len() > 1;

                let cte_name = if is_multi_type {
                    format!(
                        "vlp_multi_type_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    )
                } else {
                    // For single-type VLP, use the same naming pattern as the generator
                    format!(
                        "vlp_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    )
                };

                log::debug!("🔍 extract_from GraphRel: Using CTE name '{}'", cte_name);

                return Ok(Some(FromTable::new(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::GraphRel(graph_rel.clone())),
                    name: cte_name,
                    alias: Some(
                        graph_rel
                            .path_variable
                            .clone()
                            .unwrap_or_else(|| graph_rel.alias.clone()),
                    ),
                    use_final: false, // CTEs don't need FINAL
                }))));
            }

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
                log::debug!(
                    "✓ DENORMALIZED pattern: both nodes on edge table, using edge table as FROM"
                );

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
                extract_from(&graph_rel.left),
                extract_from(&graph_rel.right),
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
                        let nested_left_from = extract_from(&nested_graph_rel.left);
                        crate::debug_println!(
                            "DEBUG: nested_graph_rel.left = {:?}",
                            nested_graph_rel.left
                        );
                        crate::debug_println!("DEBUG: nested_left_from = {:?}", nested_left_from);

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
                        let all_connections = get_all_relationship_connections(plan);
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
            from_table_to_view_ref(extract_from(&filter.input)?)
        }
        LogicalPlan::Projection(projection) => {
            log::debug!(
                "  → Projection, recursing to input type={:?}",
                std::mem::discriminant(projection.input.as_ref())
            );
            from_table_to_view_ref(extract_from(&projection.input)?)
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
                    return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                            return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                            return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                            return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                        log::debug!("🎯 NODE-ONLY: Using node '{}' as FROM", graph_node.alias);
                        let view_ref = ViewTableRef::new_table_with_alias(
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
                        log::debug!("🎯 WITH...MATCH: FROM comes from right side");
                        return extract_from(&cp.right);
                    } else {
                        log::debug!("🎯 COMMA PATTERN: FROM comes from left side");
                        return extract_from(&cp.left);
                    }
                }

                // No valid FROM found for empty joins - this is unexpected
                log::debug!(
                    "⚠️ GraphJoins has empty joins and no recognizable pattern - returning None"
                );
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
                    return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                    return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                    return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                    log::debug!(
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
                        // Tie-breakers: prefer longer CTE names, then alphabetical for determinism
                        match &best_cte {
                            None => best_cte = Some((alias, cte_name, seq_num)),
                            Some((_, current_name, current_seq)) => {
                                if seq_num > *current_seq
                                    || (seq_num == *current_seq
                                        && cte_name.len() > current_name.len())
                                    || (seq_num == *current_seq
                                        && cte_name.len() == current_name.len()
                                        && cte_name.as_str() > current_name.as_str())
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
                        return Ok(Some(FromTable::new(Some(ViewTableRef {
                            source: std::sync::Arc::new(LogicalPlan::Empty),
                            name: cte_name.clone(),
                            alias: Some(alias.clone()),
                            use_final: false,
                        }))));
                    }
                }

                // SECONDARY FALLBACK: Pick first join as FROM table
                log::debug!(
                    "🔍 anchor_table is None and no CTE references, using first join as FROM"
                );
                if let Some(first_join) = graph_joins.joins.first() {
                    // Check if this join has a CTE reference
                    if let Some(cte_name) = graph_joins.cte_references.get(&first_join.table_alias)
                    {
                        log::info!(
                            "✅ Using first join '{}' → CTE '{}' as FROM",
                            first_join.table_alias,
                            cte_name
                        );
                        return Ok(Some(FromTable::new(Some(ViewTableRef {
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
                        return Ok(Some(FromTable::new(Some(ViewTableRef {
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
        LogicalPlan::GroupBy(group_by) => from_table_to_view_ref(extract_from(&group_by.input)?),
        LogicalPlan::OrderBy(order_by) => from_table_to_view_ref(extract_from(&order_by.input)?),
        LogicalPlan::Skip(skip) => from_table_to_view_ref(extract_from(&skip.input)?),
        LogicalPlan::Limit(limit) => from_table_to_view_ref(extract_from(&limit.input)?),
        LogicalPlan::Cte(cte) => from_table_to_view_ref(extract_from(&cte.input)?),
        LogicalPlan::Union(_) => None,
        LogicalPlan::PageRank(_) => None,
        LogicalPlan::Unwind(u) => from_table_to_view_ref(extract_from(&u.input)?),
        LogicalPlan::CartesianProduct(cp) => {
            // Try left side first (for most queries)
            let left_from = extract_from(&cp.left)?;
            if left_from.is_some() {
                // Left has a table, use it (normal case)
                from_table_to_view_ref(left_from)
            } else {
                // Left has no FROM (e.g., WITH clause creating a CTE)
                // Use right side as FROM source (e.g., new MATCH after WITH)
                log::info!(
                    "CartesianProduct: Left side has no FROM (likely CTE), using right side"
                );
                from_table_to_view_ref(extract_from(&cp.right)?)
            }
        }
        LogicalPlan::WithClause(wc) => from_table_to_view_ref(extract_from(&wc.input)?),
    };
    Ok(view_ref_to_from_table(from_ref))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_functions() {
        // Basic tests to ensure module compiles
        assert_eq!(strip_database_prefix("test"), "test");
        assert_eq!(strip_database_prefix("db.table"), "table");

        // Note: has_multi_type_vlp requires a schema, tested elsewhere
        assert_eq!(
            get_anchor_alias_from_plan(&Arc::new(LogicalPlan::Empty)),
            None
        );
    }

    /// Regression test: build_cte_column_map must use real column names from expressions,
    /// not CTE alias names like p1_a_user_id. When the FROM is a base table (e.g., social.users),
    /// correlated subqueries must reference `a.user_id`, not `a.p1_a_user_id`.
    /// See: click-to-expand regression (Code 47 ClickHouse error).
    #[test]
    fn test_build_cte_column_map_uses_expression_column_not_alias() {
        use crate::graph_catalog::expression_parser::PropertyValue;
        use crate::render_plan::render_expr::{ColumnAlias, PropertyAccess, TableAlias};

        // Build a minimal RenderPlan with a base table FROM and PropertyAccessExp SELECT items
        let render_plan = RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![
                    // `a.user_id AS p1_a_user_id` — the expression has the real column name
                    SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("a".to_string()),
                            column: PropertyValue::Column("user_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("p1_a_user_id".to_string())),
                    },
                    // `a.full_name AS p1_a_name` — property mapping: Cypher "name" → DB "full_name"
                    SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("a".to_string()),
                            column: PropertyValue::Column("full_name".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("p1_a_name".to_string())),
                    },
                ],
                distinct: false,
            },
            from: FromTableItem(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: "social.users".to_string(),
                alias: Some("a".to_string()),
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
            is_multi_label_scan: false,
            variable_registry: None,
        };

        let map = build_cte_column_map(&render_plan, "with_some_cte");

        // Must use real column "user_id", NOT the alias "p1_a_user_id"
        assert_eq!(
            map.get(&("a".to_string(), "user_id".to_string())),
            Some(&"a.user_id".to_string()),
            "Correlated subquery should reference real column a.user_id, not a.p1_a_user_id"
        );

        // Property-mapped column: Cypher "name" → real DB column "full_name"
        assert_eq!(
            map.get(&("a".to_string(), "name".to_string())),
            Some(&"a.full_name".to_string()),
            "Correlated subquery should reference real column a.full_name, not a.p1_a_name"
        );
    }

    /// Regression test: build_cte_column_map should fall back to CTE alias name
    /// when expression is not a PropertyAccessExp (e.g., aggregate or subquery).
    #[test]
    fn test_build_cte_column_map_fallback_for_non_property_expr() {
        use crate::render_plan::render_expr::ColumnAlias;

        let render_plan = RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![
                    // Non-PropertyAccessExp: e.g., COUNT(*) AS p1_a_count
                    SelectItem {
                        expression: RenderExpr::Literal(
                            crate::render_plan::render_expr::Literal::Integer(1),
                        ),
                        col_alias: Some(ColumnAlias("p1_a_count".to_string())),
                    },
                ],
                distinct: false,
            },
            from: FromTableItem(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: "with_some_cte".to_string(),
                alias: Some("cte_alias".to_string()),
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
            is_multi_label_scan: false,
            variable_registry: None,
        };

        let map = build_cte_column_map(&render_plan, "with_some_cte");

        // For non-PropertyAccess expressions, falls back to the CTE column alias name
        assert_eq!(
            map.get(&("a".to_string(), "count".to_string())),
            Some(&"cte_alias.p1_a_count".to_string()),
            "Non-PropertyAccess expressions should fall back to CTE alias column name"
        );
    }
}
pub fn extract_group_by(plan: &LogicalPlan) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
    use crate::graph_catalog::expression_parser::PropertyValue;

    log::info!(
        "🔧 GROUP BY: extract_group_by() called for plan type {:?}",
        std::mem::discriminant(plan)
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
                if let Some(result) = find_node_properties_for_rel_alias(&rel.center, rel_alias) {
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
            LogicalPlan::GroupBy(gb) => find_node_properties_for_rel_alias(&gb.input, rel_alias),
            LogicalPlan::GraphJoins(joins) => {
                find_node_properties_for_rel_alias(&joins.input, rel_alias)
            }
            LogicalPlan::OrderBy(order) => {
                find_node_properties_for_rel_alias(&order.input, rel_alias)
            }
            LogicalPlan::Skip(skip) => find_node_properties_for_rel_alias(&skip.input, rel_alias),
            LogicalPlan::Limit(limit) => {
                find_node_properties_for_rel_alias(&limit.input, rel_alias)
            }
            _ => None,
        }
    }

    let group_by = match plan {
        LogicalPlan::Limit(limit) => extract_group_by(&limit.input)?,
        LogicalPlan::Skip(skip) => extract_group_by(&skip.input)?,
        LogicalPlan::OrderBy(order_by) => extract_group_by(&order_by.input)?,
        LogicalPlan::Projection(projection) => extract_group_by(&projection.input)?,
        LogicalPlan::Filter(filter) => extract_group_by(&filter.input)?,
        LogicalPlan::GraphJoins(graph_joins) => extract_group_by(&graph_joins.input)?,
        LogicalPlan::GraphNode(node) => extract_group_by(&node.input)?,
        LogicalPlan::GraphRel(rel) => {
            // For relationships, try left first, then center, then right
            extract_group_by(&rel.left)
                .or_else(|_| extract_group_by(&rel.center))
                .or_else(|_| extract_group_by(&rel.right))?
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
                if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) = expr {
                    // OPTIMIZATION: For node aliases in GROUP BY, we only need the ID column.
                    // All other columns are functionally dependent on the ID.
                    // This reduces GROUP BY from 8+ columns to just 1, improving performance.
                    let (properties, actual_table_alias): (Vec<(String, String)>, Option<String>) =
                        match group_by.input.get_properties_with_table_alias(&alias.0) {
                            Ok(result) => result,
                            Err(_) => continue,
                        };
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
                        let id_col = group_by
                            .input
                            .find_id_column_for_alias(&alias.0)
                            .unwrap_or_else(|_| {
                                log::debug!(
                                    "⚠️ Could not find ID column for alias '{}', using fallback",
                                    alias.0
                                );
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

                // Check if this is a PropertyAccessExp with wildcard column "*"
                // This happens when ProjectionTagging converts TableAlias to PropertyAccessExp(alias.*)
                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                    prop_access,
                ) = expr
                {
                    if prop_access.column.raw() == "*" {
                        // OPTIMIZATION: For node alias wildcards in GROUP BY, we only need the ID column.
                        // All other columns are functionally dependent on the ID.
                        let (properties, actual_table_alias): (
                            Vec<(String, String)>,
                            Option<String>,
                        ) = match group_by
                            .input
                            .get_properties_with_table_alias(&prop_access.table_alias.0)
                        {
                            Ok(result) => result,
                            Err(_) => continue,
                        };
                        let table_alias_to_use =
                            actual_table_alias.unwrap_or_else(|| prop_access.table_alias.0.clone());

                        // Skip if we've already added this alias (avoid duplicates)
                        if seen_group_by_aliases.contains(&table_alias_to_use) {
                            continue;
                        }
                        seen_group_by_aliases.insert(table_alias_to_use.clone());

                        // Better approach: try to find node properties for this rel alias
                        if let Some((_node_props, table_alias)) = find_node_properties_for_rel_alias(
                            &group_by.input,
                            &prop_access.table_alias.0,
                        ) {
                            // Found denormalized node properties - get ID from schema (MUST succeed)
                            let id_col = group_by
                                .input
                                .find_id_column_for_alias(&prop_access.table_alias.0)
                                .map_err(|e| {
                                    RenderBuildError::InvalidRenderPlan(format!(
                                        "Cannot find ID column for denormalized alias '{}': {}",
                                        prop_access.table_alias.0, e
                                    ))
                                })?;

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

pub fn extract_having(plan: &LogicalPlan) -> RenderPlanBuilderResult<Option<RenderExpr>> {
    let having_clause = match plan {
        LogicalPlan::Limit(limit) => extract_having(&limit.input)?,
        LogicalPlan::Skip(skip) => extract_having(&skip.input)?,
        LogicalPlan::OrderBy(order_by) => extract_having(&order_by.input)?,
        LogicalPlan::Projection(projection) => extract_having(&projection.input)?,
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

pub fn extract_order_by(plan: &LogicalPlan) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
    let order_by = match plan {
        LogicalPlan::Limit(limit) => extract_order_by(&limit.input)?,
        LogicalPlan::Skip(skip) => extract_order_by(&skip.input)?,
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

pub fn extract_limit(plan: &LogicalPlan) -> Option<i64> {
    match plan {
        LogicalPlan::Limit(limit) => Some(limit.count),
        _ => None,
    }
}

pub fn extract_skip(plan: &LogicalPlan) -> Option<i64> {
    match plan {
        LogicalPlan::Limit(limit) => extract_skip(&limit.input),
        LogicalPlan::Skip(skip) => Some(skip.count),
        _ => None,
    }
}

pub fn get_end_table_name_or_cte(
    plan: &LogicalPlan,
) -> Result<String, crate::render_plan::errors::RenderBuildError> {
    // First, try to get source_table directly from ViewScan (handles CTE references)
    if let Some(table_name) =
        crate::render_plan::plan_builder_helpers::extract_end_node_table_name(plan)
    {
        // Check if this looks like a CTE (starts with "with_")
        if table_name.starts_with("with_") {
            return Ok(table_name);
        }
    }
    // Extract END NODE table name - handles nested GraphRel correctly
    crate::render_plan::plan_builder_helpers::extract_end_node_table_name(plan).ok_or_else(|| {
        crate::render_plan::errors::RenderBuildError::MissingTableInfo(
            "end node table in extract_joins".to_string(),
        )
    })
}

pub fn get_start_table_name_or_cte(
    plan: &LogicalPlan,
) -> Result<String, crate::render_plan::errors::RenderBuildError> {
    // First, try to get source_table directly from ViewScan (handles CTE references)
    if let Some(table_name) = crate::render_plan::plan_builder_helpers::extract_table_name(plan) {
        // Check if this looks like a CTE (starts with "with_")
        if table_name.starts_with("with_") {
            return Ok(table_name);
        }
    }
    // Extract table name from ViewScan - no fallback
    crate::render_plan::plan_builder_helpers::extract_table_name(plan).ok_or_else(|| {
        crate::render_plan::errors::RenderBuildError::MissingTableInfo(
            "start node table in extract_joins".to_string(),
        )
    })
}

pub fn extract_sorted_properties(
    property_map: &std::collections::HashMap<
        String,
        crate::graph_catalog::expression_parser::PropertyValue,
    >,
) -> Vec<(String, String)> {
    let mut properties: Vec<(String, String)> = property_map
        .iter()
        .map(|(prop_name, prop_value)| (prop_name.clone(), prop_value.raw().to_string()))
        .collect();
    properties.sort_by(|a, b| a.0.cmp(&b.0));
    properties
}

// ============================================================================
// CTE Expression Rewriting Functions
// ============================================================================

/// Apply CTE name remapping to RenderExpr recursively
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `cte_name_mapping` - Maps analyzer CTE names to actual CTE names
pub fn remap_cte_names_in_expr(
    expr: crate::render_plan::render_expr::RenderExpr,
    cte_name_mapping: &std::collections::HashMap<String, String>,
) -> crate::render_plan::render_expr::RenderExpr {
    use crate::render_plan::render_expr::*;

    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            let table_alias = &pa.table_alias.0;

            // Check if this table_alias is a CTE name that needs remapping
            if let Some(actual_cte_name) = cte_name_mapping.get(table_alias) {
                log::debug!("🔧 remap_cte_names: {} → {}", table_alias, actual_cte_name);
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(actual_cte_name.clone()),
                    column: pa.column,
                })
            } else {
                RenderExpr::PropertyAccessExp(pa)
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args = agg
                .args
                .into_iter()
                .map(|arg| remap_cte_names_in_expr(arg, cte_name_mapping))
                .collect();
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name,
                args: new_args,
            })
        }
        RenderExpr::ScalarFnCall(func) => {
            let new_args = func
                .args
                .into_iter()
                .map(|arg| remap_cte_names_in_expr(arg, cte_name_mapping))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name,
                args: new_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let new_operands = op
                .operands
                .into_iter()
                .map(|operand| remap_cte_names_in_expr(operand, cte_name_mapping))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: new_operands,
            })
        }
        RenderExpr::Case(case) => {
            let new_when_then = case
                .when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        remap_cte_names_in_expr(when, cte_name_mapping),
                        remap_cte_names_in_expr(then, cte_name_mapping),
                    )
                })
                .collect();
            let new_expr = case
                .expr
                .map(|e| Box::new(remap_cte_names_in_expr(*e, cte_name_mapping)));
            let new_else = case
                .else_expr
                .map(|e| Box::new(remap_cte_names_in_expr(*e, cte_name_mapping)));
            RenderExpr::Case(RenderCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }
        other => other,
    }
}

/// Apply CTE name remapping to all expressions in a RenderPlan
pub fn remap_cte_names_in_render_plan(
    plan: &mut crate::render_plan::RenderPlan,
    cte_name_mapping: &std::collections::HashMap<String, String>,
) {
    use crate::render_plan::render_expr::RenderExpr;

    if cte_name_mapping.is_empty() {
        return;
    }

    log::info!(
        "🔧 remap_cte_names_in_render_plan: Applying {} CTE name mappings",
        cte_name_mapping.len()
    );
    for (from, to) in cte_name_mapping {
        log::debug!("🔧   {} → {}", from, to);
    }

    // Rewrite SELECT items
    for item in &mut plan.select.items {
        item.expression = remap_cte_names_in_expr(item.expression.clone(), cte_name_mapping);
    }

    // Rewrite JOIN conditions
    for join in &mut plan.joins.0 {
        for op in &mut join.joining_on {
            // Recursively rewrite the OperatorApplication
            if let RenderExpr::OperatorApplicationExp(new_op) = remap_cte_names_in_expr(
                RenderExpr::OperatorApplicationExp(op.clone()),
                cte_name_mapping,
            ) {
                *op = new_op;
            }
        }
    }

    // Rewrite WHERE clause
    if let Some(filter) = &plan.filters.0 {
        plan.filters.0 = Some(remap_cte_names_in_expr(filter.clone(), cte_name_mapping));
    }

    // Rewrite GROUP BY
    plan.group_by.0 = plan
        .group_by
        .0
        .iter()
        .map(|expr| remap_cte_names_in_expr(expr.clone(), cte_name_mapping))
        .collect();

    // Rewrite ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = remap_cte_names_in_expr(item.expression.clone(), cte_name_mapping);
    }
}

/// Collect all `with_*_cte_*` table aliases referenced in a RenderPlan's expressions.
fn collect_with_cte_table_aliases(
    plan: &crate::render_plan::RenderPlan,
) -> std::collections::HashSet<String> {
    use crate::render_plan::render_expr::RenderExpr;

    fn collect_from_expr(expr: &RenderExpr, result: &mut std::collections::HashSet<String>) {
        match expr {
            RenderExpr::PropertyAccessExp(pa) => {
                let alias = &pa.table_alias.0;
                if alias.starts_with("with_") && alias.contains("_cte_") {
                    result.insert(alias.clone());
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                for operand in &op.operands {
                    collect_from_expr(operand, result);
                }
            }
            RenderExpr::AggregateFnCall(fc) => {
                for arg in &fc.args {
                    collect_from_expr(arg, result);
                }
            }
            RenderExpr::ScalarFnCall(fc) => {
                for arg in &fc.args {
                    collect_from_expr(arg, result);
                }
            }
            RenderExpr::Case(ce) => {
                if let Some(ref expr) = ce.expr {
                    collect_from_expr(expr, result);
                }
                for (cond, val) in &ce.when_then {
                    collect_from_expr(cond, result);
                    collect_from_expr(val, result);
                }
                if let Some(ref else_expr) = ce.else_expr {
                    collect_from_expr(else_expr, result);
                }
            }
            RenderExpr::ExistsSubquery(_) => {
                // ExistsSubquery contains pre-rendered SQL, no expressions to scan
            }
            _ => {}
        }
    }

    let mut result = std::collections::HashSet::new();
    for item in &plan.select.items {
        collect_from_expr(&item.expression, &mut result);
    }
    for join in &plan.joins.0 {
        for op in &join.joining_on {
            collect_from_expr(&RenderExpr::OperatorApplicationExp(op.clone()), &mut result);
        }
    }
    if let Some(ref filter) = plan.filters.0 {
        collect_from_expr(filter, &mut result);
    }
    for expr in &plan.group_by.0 {
        collect_from_expr(expr, &mut result);
    }
    for item in &plan.order_by.0 {
        collect_from_expr(&item.expression, &mut result);
    }
    result
}

/// Strip table alias from resolved CTE property accesses, recursively.
/// Converts `PropertyAccessExp(table_alias, column)` → `Column(column)` so ORDER BY
/// references output column aliases (visible after GROUP BY) instead of internal table references.
fn strip_table_alias_from_resolved(expr: &RenderExpr) -> RenderExpr {
    use super::render_expr::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if let PropertyValue::Column(col) = &pa.column {
                RenderExpr::Column(Column(PropertyValue::Column(col.clone())))
            } else {
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(oa) => {
            let new_ops: Vec<RenderExpr> = oa
                .operands
                .iter()
                .map(strip_table_alias_from_resolved)
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: oa.operator.clone(),
                operands: new_ops,
            })
        }
        RenderExpr::ScalarFnCall(sf) => {
            let new_args: Vec<RenderExpr> = sf
                .args
                .iter()
                .map(strip_table_alias_from_resolved)
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: sf.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(strip_table_alias_from_resolved)
                .collect();
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        _ => expr.clone(),
    }
}

/// Rewrite join conditions in a rendered plan that reference CTE aliases.
/// When a join condition uses `cte_alias.base_column` (e.g., `friend.id`),
/// replace it with the CTE's prefixed column (e.g., `friend.p6_friend_id`).
fn rewrite_join_conditions_for_cte_aliases(
    plan: &mut crate::render_plan::RenderPlan,
    cte_references: &std::collections::HashMap<String, String>,
    cte_schemas: &super::CteSchemas,
) {
    use crate::render_plan::render_expr::RenderExpr;

    fn rewrite_expr_for_cte(
        expr: RenderExpr,
        cte_references: &std::collections::HashMap<String, String>,
        cte_schemas: &super::CteSchemas,
    ) -> RenderExpr {
        match expr {
            RenderExpr::PropertyAccessExp(mut pa) => {
                let alias = &pa.table_alias.0;
                if let Some(cte_name) = cte_references.get(alias) {
                    if let Some(meta) = cte_schemas.get(cte_name) {
                        let col_name = match &pa.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(c) => {
                                c.clone()
                            }
                            crate::graph_catalog::expression_parser::PropertyValue::Expression(
                                e,
                            ) => e.clone(),
                        };
                        // Look up (alias, column) → CTE column name
                        if let Some(cte_col) = meta
                            .property_mapping
                            .get(&(alias.clone(), col_name.clone()))
                        {
                            log::info!(
                                "🔧 rewrite_join_cte: {}.{} → {}.{}",
                                alias,
                                col_name,
                                alias,
                                cte_col
                            );
                            pa.column =
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    cte_col.clone(),
                                );
                        }
                    }
                }
                RenderExpr::PropertyAccessExp(pa)
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|o| rewrite_expr_for_cte(o, cte_references, cte_schemas))
                    .collect();
                RenderExpr::OperatorApplicationExp(op)
            }
            other => other,
        }
    }

    for join in &mut plan.joins.0 {
        for op in &mut join.joining_on {
            let rewritten = rewrite_expr_for_cte(
                RenderExpr::OperatorApplicationExp(op.clone()),
                cte_references,
                cte_schemas,
            );
            if let RenderExpr::OperatorApplicationExp(new_op) = rewritten {
                *op = new_op;
            }
        }
    }

    // Also rewrite FROM table if it references a CTE
    if let crate::render_plan::FromTableItem(Some(ref mut from_ref)) = plan.from {
        if let Some(alias) = &from_ref.alias {
            if let Some(cte_name) = cte_references.get(alias) {
                if from_ref.name != *cte_name {
                    log::info!(
                        "🔧 rewrite_join_cte: Updating FROM table '{}' → '{}' for alias '{}'",
                        from_ref.name,
                        cte_name,
                        alias
                    );
                    from_ref.name = cte_name.clone();
                }
            }
        }
    }

    // CRITICAL: Also rewrite UNION branches' joins and FROM
    // BidirectionalUnion creates UNION branches that are rendered independently.
    // Their join conditions reference base table columns (e.g., person.id) which
    // need CTE column rewriting just like the main plan's joins.
    if let super::UnionItems(Some(ref mut union)) = plan.union {
        for branch in &mut union.input {
            rewrite_join_conditions_for_cte_aliases(branch, cte_references, cte_schemas);
        }
    }
}

/// Rewrite all occurrences of `old_alias` → `new_alias` in PropertyAccessExp table_alias
/// across SELECT, JOINs, WHERE, ORDER BY, and UNION branches.
/// Used when preserving the original node alias (e.g., "a") instead of
/// the combined CTE alias (e.g., "a_allNeighboursCount").
fn rewrite_table_alias_in_render_plan(
    plan: &mut crate::render_plan::RenderPlan,
    old_alias: &str,
    new_alias: &str,
) {
    use crate::render_plan::render_expr::RenderExpr;

    fn rewrite_expr(expr: RenderExpr, old: &str, new: &str) -> RenderExpr {
        match expr {
            RenderExpr::PropertyAccessExp(mut pa) => {
                if pa.table_alias.0 == old {
                    pa.table_alias.0 = new.to_string();
                }
                RenderExpr::PropertyAccessExp(pa)
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|o| rewrite_expr(o, old, new))
                    .collect();
                RenderExpr::OperatorApplicationExp(op)
            }
            RenderExpr::ScalarFnCall(mut f) => {
                f.args = f
                    .args
                    .into_iter()
                    .map(|a| rewrite_expr(a, old, new))
                    .collect();
                RenderExpr::ScalarFnCall(f)
            }
            RenderExpr::AggregateFnCall(mut f) => {
                f.args = f
                    .args
                    .into_iter()
                    .map(|a| rewrite_expr(a, old, new))
                    .collect();
                RenderExpr::AggregateFnCall(f)
            }
            RenderExpr::Case(mut c) => {
                if let Some(e) = c.expr {
                    c.expr = Some(Box::new(rewrite_expr(*e, old, new)));
                }
                c.when_then = c
                    .when_then
                    .into_iter()
                    .map(|(w, t)| (rewrite_expr(w, old, new), rewrite_expr(t, old, new)))
                    .collect();
                if let Some(e) = c.else_expr {
                    c.else_expr = Some(Box::new(rewrite_expr(*e, old, new)));
                }
                RenderExpr::Case(c)
            }
            RenderExpr::List(items) => RenderExpr::List(
                items
                    .into_iter()
                    .map(|i| rewrite_expr(i, old, new))
                    .collect(),
            ),
            other => other,
        }
    }

    // SELECT items
    for item in &mut plan.select.items {
        item.expression = rewrite_expr(item.expression.clone(), old_alias, new_alias);
    }

    // JOIN conditions
    for join in &mut plan.joins.0 {
        // Rewrite table_alias in JOIN itself
        if join.table_alias == old_alias {
            join.table_alias = new_alias.to_string();
        }
        for op in &mut join.joining_on {
            if let RenderExpr::OperatorApplicationExp(new_op) = rewrite_expr(
                RenderExpr::OperatorApplicationExp(op.clone()),
                old_alias,
                new_alias,
            ) {
                *op = new_op;
            }
        }
    }

    // WHERE
    if let Some(filter) = &plan.filters.0 {
        plan.filters.0 = Some(rewrite_expr(filter.clone(), old_alias, new_alias));
    }

    // ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite_expr(item.expression.clone(), old_alias, new_alias);
    }

    // GROUP BY
    plan.group_by.0 = plan
        .group_by
        .0
        .iter()
        .map(|e| rewrite_expr(e.clone(), old_alias, new_alias))
        .collect();

    // HAVING
    if let Some(having) = &plan.having_clause {
        plan.having_clause = Some(rewrite_expr(having.clone(), old_alias, new_alias));
    }

    // UNION branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            rewrite_table_alias_in_render_plan(branch, old_alias, new_alias);
        }
    }
}

// ============================================================================
// VLP and Scope Analysis Functions
// ============================================================================

/// Check if the plan contains a multi-type VLP pattern
/// Returns true if there's a variable-length path with multiple relationship types
pub fn has_multi_type_vlp(
    plan: &crate::query_planner::logical_plan::LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> bool {
    use crate::query_planner::logical_plan::LogicalPlan;

    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if it's a VLP pattern
            if graph_rel.variable_length.is_some() {
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                // Use the same logic as CTE extraction
                crate::render_plan::cte_extraction::should_use_join_expansion_public(
                    graph_rel, &rel_types, schema,
                )
            } else {
                false
            }
        }
        LogicalPlan::Projection(proj) => has_multi_type_vlp(&proj.input, schema),
        LogicalPlan::Filter(filter) => has_multi_type_vlp(&filter.input, schema),
        LogicalPlan::GroupBy(gb) => has_multi_type_vlp(&gb.input, schema),
        LogicalPlan::OrderBy(order) => has_multi_type_vlp(&order.input, schema),
        LogicalPlan::Limit(limit) => has_multi_type_vlp(&limit.input, schema),
        LogicalPlan::Skip(skip) => has_multi_type_vlp(&skip.input, schema),
        LogicalPlan::GraphJoins(joins) => has_multi_type_vlp(&joins.input, schema),
        _ => false,
    }
}

// ============================================================================
// Utility Functions - CTE Management
// ============================================================================

/// Hoist nested CTEs from a RenderPlan to a parent CTE list.
///
/// This is used to flatten CTE hierarchies.
pub fn hoist_nested_ctes(from: &mut RenderPlan, to: &mut Vec<Cte>) {
    let nested_ctes = std::mem::take(&mut from.ctes.0);
    if !nested_ctes.is_empty() {
        log::info!(
            "🔧 hoist_nested_ctes: Hoisting {} nested CTEs",
            nested_ctes.len()
        );
        to.extend(nested_ctes);
    }
}

/// Count WITH clause references in a logical plan for debugging
pub fn count_with_cte_refs(plan: &LogicalPlan) -> Vec<(usize, Vec<String>)> {
    match plan {
        LogicalPlan::WithClause(wc) => {
            let mut results = vec![(wc.cte_references.len(), wc.exported_aliases.clone())];
            results.extend(count_with_cte_refs(&wc.input));
            results
        }
        LogicalPlan::Projection(p) => count_with_cte_refs(&p.input),
        LogicalPlan::Limit(l) => count_with_cte_refs(&l.input),
        LogicalPlan::GraphJoins(gj) => count_with_cte_refs(&gj.input),
        _ => vec![],
    }
}

/// Check if there's a WithClause anywhere in the logical plan tree
pub fn has_with_clause_in_tree(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::WithClause(_) => true,
        LogicalPlan::ViewScan(vs) => vs
            .input
            .as_ref()
            .is_some_and(|p| has_with_clause_in_tree(p)),
        LogicalPlan::GraphNode(gn) => has_with_clause_in_tree(&gn.input),
        LogicalPlan::GraphRel(gr) => {
            has_with_clause_in_tree(&gr.left)
                || has_with_clause_in_tree(&gr.center)
                || has_with_clause_in_tree(&gr.right)
        }
        LogicalPlan::Filter(f) => has_with_clause_in_tree(&f.input),
        LogicalPlan::Projection(p) => has_with_clause_in_tree(&p.input),
        LogicalPlan::GroupBy(g) => has_with_clause_in_tree(&g.input),
        LogicalPlan::OrderBy(o) => has_with_clause_in_tree(&o.input),
        LogicalPlan::Skip(s) => has_with_clause_in_tree(&s.input),
        LogicalPlan::Limit(l) => has_with_clause_in_tree(&l.input),
        LogicalPlan::Cte(c) => has_with_clause_in_tree(&c.input),
        LogicalPlan::GraphJoins(gj) => has_with_clause_in_tree(&gj.input),
        LogicalPlan::Union(u) => u.inputs.iter().any(|p| has_with_clause_in_tree(p)),
        LogicalPlan::Unwind(u) => has_with_clause_in_tree(&u.input),
        LogicalPlan::CartesianProduct(cp) => {
            has_with_clause_in_tree(&cp.left) || has_with_clause_in_tree(&cp.right)
        }
        _ => false,
    }
}

/// Check if plan has WITH+aggregation pattern (GroupBy inside GraphRel.right)
pub fn has_with_aggregation_pattern(plan: &LogicalPlan) -> bool {
    fn has_group_by_in_graph_rel_right(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphRel(gr) => {
                // Check if right side has GroupBy
                has_group_by(&gr.right) ||
                // Also check nested GraphRels
                has_group_by_in_graph_rel_right(&gr.right)
            }
            LogicalPlan::GraphJoins(gj) => has_group_by_in_graph_rel_right(&gj.input),
            LogicalPlan::Projection(p) => has_group_by_in_graph_rel_right(&p.input),
            LogicalPlan::Filter(f) => has_group_by_in_graph_rel_right(&f.input),
            _ => false,
        }
    }

    fn has_group_by(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GroupBy(_) => true,
            LogicalPlan::Projection(p) => has_group_by(&p.input),
            LogicalPlan::Filter(f) => has_group_by(&f.input),
            LogicalPlan::GraphJoins(gj) => has_group_by(&gj.input),
            LogicalPlan::Limit(l) => has_group_by(&l.input),
            LogicalPlan::OrderBy(o) => has_group_by(&o.input),
            _ => false,
        }
    }

    has_group_by_in_graph_rel_right(plan)
}

/// Check if plan has WITH clause in GraphRel.right (WITH+MATCH pattern)
pub fn has_with_clause_in_graph_rel(plan: &LogicalPlan) -> bool {
    log::debug!(
        "🔍 has_with_clause_in_graph_rel: Called with plan type: {:?}",
        std::mem::discriminant(plan)
    );
    fn check_graph_rel_right(plan: &LogicalPlan) -> bool {
        log::debug!(
            "🔍 check_graph_rel_right: Checking plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            LogicalPlan::GraphRel(gr) => {
                log::debug!(
                    "🔍 check_graph_rel: Found GraphRel, checking left: {:?}, right: {:?}",
                    std::mem::discriminant(&*gr.left),
                    std::mem::discriminant(&*gr.right)
                );
                // Check BOTH left and right sides for WITH clauses
                let has_in_left = has_with_clause_in_tree(&gr.left);
                let has_in_right = has_with_clause_in_tree(&gr.right);
                let recursive_left = check_graph_rel_right(&gr.left);
                let recursive_right = check_graph_rel_right(&gr.right);
                log::debug!(
            "🔍 check_graph_rel: has_in_left: {}, has_in_right: {}, recursive_left: {}, recursive_right: {}",
                    has_in_left, has_in_right, recursive_left, recursive_right
                );
                has_in_left || has_in_right || recursive_left || recursive_right
            }
            LogicalPlan::GraphJoins(gj) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found GraphJoins, checking input: {:?}",
                    std::mem::discriminant(&*gj.input)
                );
                check_graph_rel_right(&gj.input)
            }
            LogicalPlan::Projection(p) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found Projection, checking input: {:?}",
                    std::mem::discriminant(&*p.input)
                );
                check_graph_rel_right(&p.input)
            }
            LogicalPlan::Filter(f) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found Filter, checking input: {:?}",
                    std::mem::discriminant(&*f.input)
                );
                check_graph_rel_right(&f.input)
            }
            // Handle the unknown Discriminant(7) case - assume it might contain WITH clauses
            _ => {
                log::debug!("🔍 check_graph_rel_right: Unknown plan type {:?}, checking with has_with_clause_in_tree", std::mem::discriminant(plan));
                has_with_clause_in_tree(plan)
            }
        }
    }
    let result = check_graph_rel_right(plan);
    log::debug!("🔍 has_with_clause_in_graph_rel: Final result: {}", result);
    result
}

/// Get node ID column for alias with schema lookup
pub fn get_node_id_column_for_alias_with_schema(
    alias: &str,
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> Option<String> {
    // First try to find the table name for this alias
    let table_name = find_table_for_alias(plan, alias)?;

    // Look up the node schema
    let node_schema = schema.node_schema(&table_name).ok()?;

    // Return first ID column
    Some(node_schema.node_id.id.first_column().to_string())
}

/// Find table name for a given alias by traversing the logical plan
pub fn find_table_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(_vs) => {
            // ViewScan doesn't have alias - this shouldn't match directly
            None
        }
        LogicalPlan::GraphNode(gn) => {
            if gn.alias == target_alias {
                if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                    Some(vs.source_table.clone())
                } else {
                    None
                }
            } else {
                find_table_for_alias(&gn.input, target_alias)
            }
        }
        LogicalPlan::GraphRel(gr) => find_table_for_alias(&gr.left, target_alias)
            .or_else(|| find_table_for_alias(&gr.center, target_alias))
            .or_else(|| find_table_for_alias(&gr.right, target_alias)),
        LogicalPlan::Filter(f) => find_table_for_alias(&f.input, target_alias),
        LogicalPlan::Projection(p) => find_table_for_alias(&p.input, target_alias),
        LogicalPlan::GraphJoins(gj) => find_table_for_alias(&gj.input, target_alias),
        LogicalPlan::Limit(l) => find_table_for_alias(&l.input, target_alias),
        LogicalPlan::OrderBy(o) => find_table_for_alias(&o.input, target_alias),
        LogicalPlan::Skip(s) => find_table_for_alias(&s.input, target_alias),
        LogicalPlan::GroupBy(g) => find_table_for_alias(&g.input, target_alias),
        LogicalPlan::Unwind(u) => find_table_for_alias(&u.input, target_alias),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .find_map(|p| find_table_for_alias(p, target_alias)),
        LogicalPlan::CartesianProduct(cp) => find_table_for_alias(&cp.left, target_alias)
            .or_else(|| find_table_for_alias(&cp.right, target_alias)),
        _ => None,
    }
}

/// Find the leftmost ViewScan node for polymorphic CTE FROM determination
pub fn find_leftmost_viewscan_node(plan: &LogicalPlan) -> Option<&GraphNode> {
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

/// Extract start filter for outer query in optional VLP
pub fn extract_start_filter_for_outer_query(plan: &LogicalPlan) -> Option<RenderExpr> {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // Use the where_predicate as the start filter
            if let Some(ref predicate) = gr.where_predicate {
                RenderExpr::try_from(predicate.clone()).ok()
            } else {
                None
            }
        }
        LogicalPlan::Projection(p) => extract_start_filter_for_outer_query(&p.input),
        LogicalPlan::Filter(f) => {
            // Also check Filter for where clause
            if let Ok(expr) = RenderExpr::try_from(f.predicate.clone()) {
                Some(expr)
            } else {
                extract_start_filter_for_outer_query(&f.input)
            }
        }
        LogicalPlan::GraphJoins(gj) => extract_start_filter_for_outer_query(&gj.input),
        LogicalPlan::GroupBy(gb) => extract_start_filter_for_outer_query(&gb.input),
        LogicalPlan::Limit(l) => extract_start_filter_for_outer_query(&l.input),
        LogicalPlan::OrderBy(o) => extract_start_filter_for_outer_query(&o.input),
        _ => None,
    }
}

/// Collect schema filter from ViewScan for a given alias
pub fn collect_schema_filter_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
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
            collect_schema_filter_for_alias(&gr.left, target_alias)
                .or_else(|| collect_schema_filter_for_alias(&gr.right, target_alias))
        }
        LogicalPlan::GraphNode(gn) => collect_schema_filter_for_alias(&gn.input, target_alias),
        LogicalPlan::Filter(f) => collect_schema_filter_for_alias(&f.input, target_alias),
        LogicalPlan::Projection(p) => collect_schema_filter_for_alias(&p.input, target_alias),
        LogicalPlan::GraphJoins(gj) => collect_schema_filter_for_alias(&gj.input, target_alias),
        LogicalPlan::Limit(l) => collect_schema_filter_for_alias(&l.input, target_alias),
        _ => None,
    }
}

/// Check if expression references only VLP aliases
pub fn references_only_vlp_aliases(
    expr: &RenderExpr,
    start_alias: &str,
    end_alias: &str,
    rel_alias: Option<&str>,
) -> bool {
    fn collect_aliases(expr: &RenderExpr, aliases: &mut HashSet<String>) {
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
    let mut aliases = HashSet::new();
    collect_aliases(expr, &mut aliases);
    // Returns true if ALL referenced aliases are VLP-related (start, end, or relationship)
    !aliases.is_empty()
        && aliases.iter().all(|a| {
            a == start_alias || a == end_alias || rel_alias.map(|r| a == r).unwrap_or(false)
        })
}

/// Split AND-connected filters into individual filters
pub fn split_and_filters(expr: RenderExpr) -> Vec<RenderExpr> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
            let mut result = Vec::new();
            for operand in op.operands {
                result.extend(split_and_filters(operand));
            }
            result
        }
        _ => vec![expr],
    }
}

/// Rewrite expression for mixed denormalized CTE
/// Rewrite VLP internal aliases to Cypher aliases
/// Check if a join is for the inner scope (part of the pre-WITH pattern).
/// This is determined by checking if the join references aliases that are
/// part of the first MATCH (before WITH).
pub fn is_join_for_inner_scope(
    _plan: &LogicalPlan,
    _join: &crate::query_planner::logical_plan::Join,
    _with_alias: &str,
) -> bool {
    // For WITH+aggregation patterns, joins with aliases p, t1 are for the inner scope
    // Joins with aliases t2, post are for the outer scope
    // We detect inner scope joins by checking if they reference aliases that are:
    // 1. Part of the first MATCH (before WITH)
    // 2. Not the with_alias itself

    // Simple heuristic: inner joins typically have aliases like p, t1
    // Outer joins have aliases like t2, post
    // A more robust approach would track which aliases are defined in which scope

    // For now, use a simple heuristic based on join table alias pattern
    // Inner scope joins are the first N joins where N is determined by examining the plan
    // This is a simplification - in production, we should track scope properly

    // Actually, let's check if the join references a table that exists in the inner scope
    // For the pattern: p -> f (inner), f -> post (outer)
    // Joins for t1 (KNOWS) and f should be inner
    // Joins for t2 (HAS_CREATOR) and post should be outer

    // For now, return false (no filtering) - this is a placeholder
    // In production, we'd need proper scope tracking
    false
}
// CTE Join Condition Extraction Functions
// ============================================================================

/// Extract CTE join conditions from WHERE clause filters
/// Analyzes filter expressions to find equality comparisons between CTE columns and table columns
/// Returns: Vec<(cte_name, cte_column, main_table_alias, main_column)>
pub fn extract_cte_join_conditions(
    filters: &Option<crate::render_plan::render_expr::RenderExpr>,
    cte_references: &std::collections::HashMap<String, String>,
) -> Vec<(String, String, String, String)> {
    let mut conditions = vec![];

    if let Some(filter_expr) = filters {
        extract_cte_conditions_recursive(filter_expr, cte_references, &mut conditions);
    }

    log::info!(
        "🔧 extract_cte_join_conditions: Found {} CTE join conditions: {:?}",
        conditions.len(),
        conditions
    );
    conditions
}

/// Recursively search filter expressions for CTE equality comparisons
pub fn extract_cte_conditions_recursive(
    expr: &crate::render_plan::render_expr::RenderExpr,
    cte_references: &std::collections::HashMap<String, String>,
    conditions: &mut Vec<(String, String, String, String)>,
) {
    use crate::render_plan::render_expr::*;

    if let RenderExpr::OperatorApplicationExp(op_app) = expr {
        // Look for Equal operator
        if matches!(op_app.operator, Operator::Equal) && op_app.operands.len() == 2 {
            let left = &op_app.operands[0];
            let right = &op_app.operands[1];

            // Check if one side is a CTE column and the other is a table column
            if let Some(cond) = extract_join_from_equality(left, right, cte_references) {
                log::info!(
                    "🔧 Found CTE join condition: CTE {}. {} = {}.{}",
                    cond.0,
                    cond.1,
                    cond.2,
                    cond.3
                );
                conditions.push(cond);
            } else if let Some(cond) = extract_join_from_equality(right, left, cte_references) {
                // Try reversed order
                conditions.push(cond);
            }
        }

        // Check for AND/OR - recurse into operands
        if matches!(op_app.operator, Operator::And | Operator::Or) {
            for operand in &op_app.operands {
                extract_cte_conditions_recursive(operand, cte_references, conditions);
            }
        }
    }
}

/// Try to extract a CTE join condition from an equality comparison (RenderExpr version)
/// Returns: Some((cte_name, cte_column, main_table_alias, main_column)) if found
pub fn extract_join_from_equality(
    left: &crate::render_plan::render_expr::RenderExpr,
    right: &crate::render_plan::render_expr::RenderExpr,
    cte_references: &std::collections::HashMap<String, String>,
) -> Option<(String, String, String, String)> {
    use crate::render_plan::render_expr::*;

    // Pattern 1: left is CTE column, right is table column
    // Example: source_ip = conn.orig_h
    if let RenderExpr::ColumnAlias(col_alias) = left {
        // Check if this column alias references a CTE
        if let Some(cte_name) = cte_references.get(&col_alias.0) {
            // Right side should be a property access (table.column)
            if let RenderExpr::PropertyAccessExp(prop) = right {
                return Some((
                    cte_name.clone(),
                    col_alias.0.clone(),
                    prop.table_alias.0.clone(),
                    match &prop.column {
                        PropertyValue::Column(col) => col.clone(),
                        _ => return None,
                    },
                ));
            }
        }
    }

    // Pattern 2: left is table column, right is CTE column
    // Example: conn.orig_h = source_ip
    if let RenderExpr::PropertyAccessExp(prop) = left {
        if let RenderExpr::ColumnAlias(col_alias) = right {
            if let Some(cte_name) = cte_references.get(&col_alias.0) {
                return Some((
                    cte_name.clone(),
                    col_alias.0.clone(),
                    prop.table_alias.0.clone(),
                    match &prop.column {
                        PropertyValue::Column(col) => col.clone(),
                        _ => return None,
                    },
                ));
            }
        }
    }

    None
}

/// Rewrite CTE column references to include alias prefix
/// Converts "friend.id" → "friend.friend_id" for consistency
pub fn rewrite_cte_column_references(expr: &mut crate::render_plan::render_expr::RenderExpr) {
    use crate::render_plan::expression_utils::MutablePropertyColumnRewriter;

    // Rewrite columns to include table alias prefix (underscore separator)
    // E.g., user.id → user.user_id (for CTE column flattening)
    MutablePropertyColumnRewriter::rewrite_column_with_prefix(expr, '_');
}

/// Find a GroupBy subplan with is_materialization_boundary=true
pub fn find_group_by_subplan(plan: &LogicalPlan) -> Option<(&LogicalPlan, String)> {
    match plan {
        LogicalPlan::Limit(limit) => find_group_by_subplan(&limit.input),
        LogicalPlan::OrderBy(order_by) => find_group_by_subplan(&order_by.input),
        LogicalPlan::Skip(skip) => find_group_by_subplan(&skip.input),
        LogicalPlan::GraphJoins(gj) => find_group_by_subplan(&gj.input),
        LogicalPlan::Projection(proj) => find_group_by_subplan(&proj.input),
        LogicalPlan::Filter(f) => find_group_by_subplan(&f.input),
        LogicalPlan::GroupBy(gb) => {
            // Found a GroupBy! Extract the exposed_alias if it's a materialization boundary
            if gb.is_materialization_boundary {
                let alias = gb
                    .exposed_alias
                    .clone()
                    .unwrap_or_else(|| "cte".to_string());
                log::debug!("🔍 find_group_by_subplan: Found GroupBy with is_materialization_boundary=true, alias='{}'", alias);
                return Some((plan, alias));
            }
            // Also recurse into the GroupBy's input in case there's a nested boundary
            find_group_by_subplan(&gb.input)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Check both branches for GroupBy
            // After boundary separation, GroupBy is typically in .left
            if let LogicalPlan::GroupBy(gb) = graph_rel.left.as_ref() {
                if gb.is_materialization_boundary {
                    let alias = gb
                        .exposed_alias
                        .clone()
                        .unwrap_or_else(|| graph_rel.left_connection.clone());
                    log::debug!("🔍 find_group_by_subplan: Found GroupBy(boundary) in GraphRel.left, alias='{}'", alias);
                    return Some((graph_rel.left.as_ref(), alias));
                }
            }
            if let LogicalPlan::GroupBy(gb) = graph_rel.right.as_ref() {
                if gb.is_materialization_boundary {
                    let alias = gb
                        .exposed_alias
                        .clone()
                        .unwrap_or_else(|| graph_rel.right_connection.clone());
                    log::debug!("🔍 find_group_by_subplan: Found GroupBy(boundary) in GraphRel.right, alias='{}'", alias);
                    return Some((graph_rel.right.as_ref(), alias));
                }
            }
            // Recurse into branches
            if let Some(found) = find_group_by_subplan(&graph_rel.left) {
                return Some(found);
            }
            if let Some(found) = find_group_by_subplan(&graph_rel.right) {
                return Some(found);
            }
            None
        }
        _ => None,
    }
}

/// Check if plan contains a WithClause node
pub fn plan_contains_with_clause(plan: &LogicalPlan) -> bool {
    match plan {
        // NEW: Handle WithClause type
        LogicalPlan::WithClause(_) => true,
        LogicalPlan::Projection(proj) => plan_contains_with_clause(&proj.input),
        LogicalPlan::Filter(filter) => plan_contains_with_clause(&filter.input),
        LogicalPlan::GroupBy(group_by) => plan_contains_with_clause(&group_by.input),
        LogicalPlan::GraphJoins(graph_joins) => plan_contains_with_clause(&graph_joins.input),
        LogicalPlan::Limit(limit) => plan_contains_with_clause(&limit.input),
        LogicalPlan::OrderBy(order_by) => plan_contains_with_clause(&order_by.input),
        LogicalPlan::Skip(skip) => plan_contains_with_clause(&skip.input),
        LogicalPlan::GraphRel(graph_rel) => {
            plan_contains_with_clause(&graph_rel.left)
                || plan_contains_with_clause(&graph_rel.right)
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| plan_contains_with_clause(input)),
        LogicalPlan::GraphNode(node) => plan_contains_with_clause(&node.input),
        LogicalPlan::Unwind(unwind) => plan_contains_with_clause(&unwind.input),
        LogicalPlan::CartesianProduct(cp) => {
            plan_contains_with_clause(&cp.left) || plan_contains_with_clause(&cp.right)
        }
        _ => false,
    }
}

/// Collect all table aliases referenced in render expressions
pub fn collect_aliases_from_render_expr(exprs: &[RenderExpr], aliases: &mut Vec<String>) {
    for expr in exprs {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                if !aliases.contains(&prop.table_alias.0) {
                    aliases.push(prop.table_alias.0.clone());
                }
            }
            RenderExpr::TableAlias(alias) => {
                if !aliases.contains(&alias.0) {
                    aliases.push(alias.0.clone());
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                collect_aliases_from_render_expr(&op.operands, aliases);
            }
            RenderExpr::ScalarFnCall(func) => {
                collect_aliases_from_render_expr(&func.args, aliases);
            }
            RenderExpr::AggregateFnCall(agg) => {
                collect_aliases_from_render_expr(&agg.args, aliases);
            }
            _ => {}
        }
    }
}

pub(crate) fn generate_swapped_joins_for_optional_match(
    graph_rel: &GraphRel,
) -> RenderPlanBuilderResult<Vec<Join>> {
    let mut joins = Vec::new();

    // Extract table names and columns with parameterized view syntax if applicable
    // CRITICAL FIX: Use extract_parameterized_table_ref for ViewScan to handle parameterized views
    let start_table = extract_parameterized_table_ref(&graph_rel.left)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("left node".to_string()))?;
    let _end_table = extract_parameterized_table_ref(&graph_rel.right)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("right node".to_string()))?;

    // For ID column lookup, we need the plain table name (without parameterized syntax)
    let start_table_plain = extract_table_name(&graph_rel.left)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("left node".to_string()))?;
    let end_table_plain = extract_table_name(&graph_rel.right)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("right node".to_string()))?;

    let start_id_col = table_to_id_column(&start_table_plain);
    let end_id_col = table_to_id_column(&end_table_plain);

    // Get relationship table with parameterized view syntax if applicable
    // If center is wrapped in a CTE (for alternate relationships), use the CTE name
    // Otherwise, derive from labels or extract from plan with parameterized view support
    let rel_table = if matches!(&*graph_rel.center, LogicalPlan::Cte(_)) {
        // CTEs don't have parameterized views
        extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
    } else if let Some(labels) = &graph_rel.labels {
        if !labels.is_empty() {
            // Labels-based lookup doesn't support parameterized views
            rel_type_to_table_name(&labels[0])
        } else {
            // Use parameterized table ref for ViewScan
            extract_parameterized_table_ref(&graph_rel.center)
                .unwrap_or_else(|| graph_rel.alias.clone())
        }
    } else {
        // Use parameterized table ref for ViewScan
        extract_parameterized_table_ref(&graph_rel.center)
            .unwrap_or_else(|| graph_rel.alias.clone())
    };

    // Get relationship columns
    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(RelationshipColumns {
        from_id: Identifier::Single("from_node_id".to_string()),
        to_id: Identifier::Single("to_node_id".to_string()),
    });

    // For OPTIONAL MATCH with swapped anchor:
    // - anchor is right_connection (post)
    // - new node is left_connection (liker)
    // - For outgoing direction (liker)-[:LIKES]->(post):
    //   - rel.to_id connects to anchor (post)
    //   - rel.from_id connects to new node (liker)

    // Determine join column based on direction
    let (rel_col_to_anchor, rel_col_to_new) = match graph_rel.direction {
        Direction::Incoming => {
            // (liker)<-[:LIKES]-(post) means rel points from post to liker
            // rel.from_id = anchor (post), rel.to_id = new (liker)
            (rel_cols.from_id.to_string(), rel_cols.to_id.to_string())
        }
        _ => {
            // Direction::Outgoing or Direction::Either
            // (liker)-[:LIKES]->(post) means rel points from liker to post
            // rel.to_id = anchor (post), rel.from_id = new (liker)
            (rel_cols.to_id.to_string(), rel_cols.from_id.to_string())
        }
    };

    crate::debug_print!("  Generating swapped joins:");
    crate::debug_print!(
        "    rel.{} = {}.{} (anchor)",
        rel_col_to_anchor,
        graph_rel.right_connection,
        end_id_col
    );
    crate::debug_print!(
        "    {}.{} = rel.{} (new node)",
        graph_rel.left_connection,
        start_id_col,
        rel_col_to_new
    );

    // JOIN 1: Relationship table connecting to anchor (right_connection)
    let rel_join_condition = OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.alias.clone()),
                column: PropertyValue::Column(rel_col_to_anchor.clone()),
            }),
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.right_connection.clone()),
                column: PropertyValue::Column(end_id_col.clone()),
            }),
        ],
    };

    joins.push(Join {
        table_name: rel_table,
        table_alias: graph_rel.alias.clone(),
        joining_on: vec![rel_join_condition],
        join_type: JoinType::Left,
        pre_filter: None,
        from_id_column: Some(rel_col_to_anchor.clone()),
        to_id_column: Some(rel_col_to_new.clone()),
        graph_rel: None,
    });

    // JOIN 2: New node (left_connection) connecting to relationship
    let new_node_join_condition = OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.left_connection.clone()),
                column: PropertyValue::Column(start_id_col),
            }),
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.alias.clone()),
                column: PropertyValue::Column(rel_col_to_new.clone()),
            }),
        ],
    };

    joins.push(Join {
        table_name: start_table,
        table_alias: graph_rel.left_connection.clone(),
        joining_on: vec![new_node_join_condition],
        join_type: JoinType::Left,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
        graph_rel: None,
    });

    Ok(joins)
}

pub(crate) fn rewrite_vlp_union_branch_aliases(
    plan: &mut RenderPlan,
) -> RenderPlanBuilderResult<()> {
    log::debug!("TRACING: rewrite_vlp_union_branch_aliases called");
    log::info!(
        "🔍 VLP Union Branch: Checking for VLP CTEs... (found {} CTEs total)",
        plan.ctes.0.len()
    );

    // Check if this plan has VLP CTEs
    let vlp_mappings = extract_vlp_alias_mappings(&plan.ctes);

    if vlp_mappings.is_empty() {
        log::debug!("🔍 VLP Union Branch: No VLP mappings found, skipping rewrite");
        return Ok(()); // No VLP CTEs, nothing to rewrite
    }

    log::info!(
        "🔄 VLP Union Branch: Found {} VLP CTE(s), checking if rewrite is needed",
        vlp_mappings.len()
    );

    // 🔧 FIX: For OPTIONAL MATCH + VLP, FROM uses anchor table (not VLP CTE)
    // In this case, anchor node properties should NOT be rewritten
    // Detection: FROM uses regular table when VLP CTEs are present
    let is_optional_vlp = if let Some(from_ref) = &plan.from.0 {
        !from_ref.name.starts_with("vlp_") && !vlp_mappings.is_empty()
    } else {
        false
    };

    if is_optional_vlp {
        log::debug!("OPTIONAL VLP detected: FROM uses anchor table '{}', skipping VLP property rewriting for anchor nodes", 
            plan.from.0.as_ref().map(|f| f.name.as_str()).unwrap_or("unknown"));
        // Continue with rewriting, but skip start aliases in cte_column_mapping
    }

    // Extract VLP column metadata for property name resolution
    // This maps (cypher_alias, property_name) → cte_column_name
    // E.g., (a, email_address) → start_email (or start_email_address depending on CTE)
    let _cte_column_mapping: HashMap<(String, String), String> = HashMap::new();
    log::debug!("🔧 VLP: Total CTEs in plan: {}", plan.ctes.0.len());

    // ✨ NEW APPROACH: Build metadata-based lookup mapping
    // Maps: (cypher_alias, db_column) → (cte_column_name, vlp_position)
    // This is lookup-based, NOT heuristic-based. No splitting needed!
    let mut cte_column_mapping: HashMap<
        (String, String),
        (String, crate::render_plan::cte_manager::VlpColumnPosition),
    > = HashMap::new();

    for (idx, cte) in plan.ctes.0.iter().enumerate() {
        log::debug!(
            "🔧 VLP: CTE[{}]: name={}, columns={}, vlp_cypher_start={:?}, vlp_cypher_end={:?}",
            idx,
            cte.cte_name,
            cte.columns.len(),
            cte.vlp_cypher_start_alias,
            cte.vlp_cypher_end_alias
        );
        if cte.cte_name.starts_with("vlp_") {
            log::debug!(
                "🔧 VLP: Processing VLP CTE '{}' with {} columns",
                cte.cte_name,
                cte.columns.len()
            );
            for (col_idx, col_meta) in cte.columns.iter().enumerate() {
                if let Some(position) = col_meta.vlp_position {
                    log::debug!(
                        "🔧   Column[{}]: cte={}, alias={}, cypher_prop={}, db_col={}, pos={:?}",
                        col_idx,
                        col_meta.cte_column_name,
                        col_meta.cypher_alias,
                        col_meta.cypher_property,
                        col_meta.db_column,
                        position
                    );
                    // Build lookup key: (cypher_alias, db_column_name)
                    // This is what we'll match against when rewriting PropertyAccessExp
                    let key = (col_meta.cypher_alias.clone(), col_meta.db_column.clone());
                    let value = (col_meta.cte_column_name.clone(), position);
                    cte_column_mapping.insert(key, value);
                    log::info!(
                        "✅ VLP Lookup Entry: ({}, {}) → ({}, {:?})",
                        col_meta.cypher_alias,
                        col_meta.db_column,
                        col_meta.cte_column_name,
                        position
                    );
                }
            }
        }
    }

    log::debug!(
        "🔧 VLP: Built {} column lookup entries from CTE metadata (NO splitting!)",
        cte_column_mapping.len()
    );

    // ✨ ARCHITECTURAL FIX: Filter mappings based on whether endpoint JOINs exist
    //
    // For NORMAL VLP:
    //   - VLP CTEs contain: start_id, end_id, hop_count, path tracking, edge properties
    //   - VLP CTEs do NOT contain node properties!
    //   - Node properties fetched by JOINing to source tables
    //   - Therefore: Exclude endpoint aliases from rewriting (a → vlp1)
    //
    // For DENORMALIZED VLP:
    //   - VLP CTEs contain: Everything above PLUS node properties (from edge table)
    //   - No separate node tables exist - no JOINs added
    //   - Therefore: INCLUDE endpoint aliases for rewriting (a → vlp1)
    //
    // Detection Strategy: Check if endpoint JOINs exist in plan.joins
    //   - If endpoint JOINs exist → Normal VLP → Exclude endpoint aliases
    //   - If endpoint JOINs missing → Denormalized VLP → Include endpoint aliases
    let mut vlp_endpoint_aliases: HashSet<String> = HashSet::new();
    let mut endpoint_has_joins: HashMap<String, bool> = HashMap::new();

    // 🔧 FIX: Detect aliases that are covered by WITH CTEs
    // If FROM references a `with_*_cte_*` CTE, the corresponding alias should NOT be rewritten
    // because the WITH CTE already provides the aliased columns, not the raw VLP CTE
    let mut aliases_covered_by_with_cte: HashSet<String> = HashSet::new();

    // Check if FROM references a WITH CTE
    if let Some(from_ref) = &plan.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            // Extract the alias from the FROM clause or the CTE name
            // CTE names are like "with_u2_cte_1" - extract "u2"
            if let Some(alias) = &from_ref.alias {
                aliases_covered_by_with_cte.insert(alias.clone());
                log::info!(
                    "🔧 VLP: FROM uses WITH CTE '{}' with alias '{}' - excluding from rewrite",
                    from_ref.name,
                    alias
                );
            } else if let Some(captured) = from_ref
                .name
                .strip_prefix("with_")
                .and_then(|s| s.split("_cte_").next())
            {
                aliases_covered_by_with_cte.insert(captured.to_string());
                log::info!(
                    "🔧 VLP: FROM uses WITH CTE '{}' covering alias '{}' - excluding from rewrite",
                    from_ref.name,
                    captured
                );
            }
        }
    }

    for cte in &plan.ctes.0 {
        if let (Some(start), Some(end)) = (&cte.vlp_cypher_start_alias, &cte.vlp_cypher_end_alias) {
            vlp_endpoint_aliases.insert(start.clone());
            vlp_endpoint_aliases.insert(end.clone());

            // Check if these endpoint aliases have corresponding JOINs
            let start_has_join = plan.joins.0.iter().any(|j| j.table_alias == *start);
            let end_has_join = plan.joins.0.iter().any(|j| j.table_alias == *end);

            endpoint_has_joins.insert(start.clone(), start_has_join);
            endpoint_has_joins.insert(end.clone(), end_has_join);

            log::info!(
                "🔍 VLP: Endpoint aliases: '{}' (has_join={}), '{}' (has_join={})",
                start,
                start_has_join,
                end,
                end_has_join
            );
        }
    }

    let filtered_mappings: HashMap<String, String> = vlp_mappings
        .clone()
        .into_iter()
        .filter(|(cypher_alias, _vlp_alias)| {
            // 🔧 FIX: For OPTIONAL VLP, exclude the start alias from rewriting
            // The start alias refers to the anchor table in FROM, not VLP CTE
            if is_optional_vlp {
                // For OPTIONAL VLP, find the start alias and exclude it
                for cte in &plan.ctes.0 {
                    if let Some(start_alias) = &cte.vlp_cypher_start_alias {
                        if cypher_alias == start_alias {
                            log::debug!(
                                "🔧 OPTIONAL VLP: Excluding start alias '{}' from rewrite (anchor table in FROM)",
                                cypher_alias
                            );
                            return false;
                        }
                    }
                }
            }

            // 🔧 FIX: Exclude aliases covered by WITH CTEs
            // These aliases reference the WITH CTE columns, not the raw VLP CTE
            if aliases_covered_by_with_cte.contains(cypher_alias) {
                log::debug!(
                    "🔧 VLP: Excluding alias '{}' from rewrite (covered by WITH CTE)",
                    cypher_alias
                );
                return false;
            }

            let is_endpoint = vlp_endpoint_aliases.contains(cypher_alias);
            if is_endpoint {
                // ✅ FIX: ALWAYS include endpoints for rewriting!
                log::debug!(
                    "✅ VLP: INCLUDING endpoint alias '{}' in rewrite (for correct column mapping)",
                    cypher_alias
                );
                return true;
            }
            true // Keep non-endpoint mappings (e.g., path variable)
        })
        .collect();

    // 🔧 ENHANCEMENT: Build a reverse mapping to infer start/end from CTE structure
    // CTE names are formatted as "vlp_{start}_{end}", so we can infer which endpoint is which
    // Example: cte_name = "vlp_u_f" means start="u", end="f"
    let mut endpoint_position: HashMap<String, &str> = HashMap::new();

    for cte in &plan.ctes.0 {
        if cte.vlp_cypher_start_alias.is_some() && cte.vlp_cypher_end_alias.is_some() {
            let start = cte.vlp_cypher_start_alias.as_ref().unwrap();
            let end = cte.vlp_cypher_end_alias.as_ref().unwrap();

            endpoint_position.insert(start.clone(), "start");
            endpoint_position.insert(end.clone(), "end");

            log::debug!(
                "🔄 VLP: Endpoint position mapping: '{}' = start, '{}' = end (from CTE {})",
                start,
                end,
                cte.cte_name
            );
        }
    }

    if filtered_mappings.is_empty() {
        log::debug!("🔍 VLP Union Branch: All mappings filtered out - nothing to rewrite");
        return Ok(());
    }

    log::info!(
        "🔄 VLP Union Branch: Applying {} filtered mapping(s) (excluded {} endpoint aliases)",
        filtered_mappings.len(),
        vlp_endpoint_aliases.len()
    );

    // Log what mappings we're applying
    for (from, to) in &filtered_mappings {
        log::debug!("   Mapping: {} → {}", from, to);
    }

    // 🔍 DEBUG: Log CTE column mapping entries
    log::debug!(
        "🔍 DEBUG: CTE column mapping has {} entries:",
        cte_column_mapping.len()
    );
    for ((alias, db_col), (cte_col, pos)) in &cte_column_mapping {
        log::debug!("   ({}, {}) → ({}, {:?})", alias, db_col, cte_col, pos);
    }

    // 🔧 CRITICAL: Check if this is a multi-type VLP (from CTE name)
    // Multi-type VLP CTEs use Cypher aliases directly in SELECT (e.g., x.end_type)
    // and properties are extracted via JSON_VALUE() - no table alias rewriting needed
    let is_multi_type_vlp = plan
        .ctes
        .0
        .iter()
        .any(|cte| cte.cte_name.starts_with("vlp_multi_type_"));

    if is_multi_type_vlp {
        log::info!(
            "🎯 VLP: Multi-type VLP detected - FROM uses Cypher alias, no rewriting needed!"
        );
        // With the correct FROM (vlp_multi_type_u_x AS x), everything works naturally:
        //   - x.end_type → CTE column (direct access)
        //   - x.name → property (SQL generator extracts from end_properties JSON)
        // No table alias rewriting needed - the FROM clause is already correct!
    } else {
        // Extract the VLP table alias to use in SELECT/WHERE/GROUP BY references
        //
        // For NON-OPTIONAL VLP:
        //   - FROM clause: FROM vlp_a_b AS t
        //   - Use FROM alias: 't'
        //
        // For OPTIONAL VLP:
        //   - FROM clause: FROM users AS a (anchor table)
        //   - JOIN clause: LEFT JOIN vlp_a_b AS t ON ...
        //   - Use JOIN alias: 't' (NOT FROM alias 'a')
        let vlp_from_alias = if is_optional_vlp {
            // For OPTIONAL VLP, find the VLP CTE JOIN alias
            plan.joins
                .0
                .iter()
                .find(|j| j.table_name.starts_with("vlp_"))
                .map(|j| j.table_alias.clone())
                .unwrap_or_else(|| "t".to_string())
        } else {
            // For non-OPTIONAL VLP, use FROM alias
            plan.from
                .0
                .as_ref()
                .and_then(|from_ref| from_ref.alias.as_ref())
                .cloned()
                .unwrap_or_else(|| "t".to_string())
        };

        log::debug!(
            "🔧 VLP: VLP table alias to use: '{}' (is_optional_vlp={})",
            vlp_from_alias,
            is_optional_vlp
        );

        // Rewrite SELECT items using filtered VLP mappings (for non-multi-type VLP)
        log::debug!("🔍 VLP: Rewriting {} SELECT items", plan.select.items.len());
        for (idx, select_item) in plan.select.items.iter_mut().enumerate() {
            log::debug!("   SELECT[{}]: {:?}", idx, select_item.expression);
            rewrite_render_expr_for_vlp_with_endpoint_info(
                &mut select_item.expression,
                &filtered_mappings,
                &vlp_from_alias,
                &endpoint_position,
                &cte_column_mapping,
            );
        }
    }

    // CRITICAL: Also rewrite WHERE clause expressions
    // The WHERE clause may contain filters on Cypher aliases (e.g., friend.firstName = 'Wei')
    // that need to be rewritten to use VLP table aliases (e.g., end_node.firstName = 'Wei')
    if let Some(where_expr) = &mut plan.filters.0 {
        log::debug!("🔄 VLP Union Branch: Rewriting WHERE clause");
        rewrite_render_expr_for_vlp_with_endpoint_info(
            where_expr,
            &filtered_mappings,
            "t",
            &endpoint_position,
            &cte_column_mapping,
        );
    }

    // 🔧 FIX #5: Also rewrite GROUP BY expressions
    // The GROUP BY clause may contain Cypher aliases (e.g., f.DestCityName)
    // that need to be rewritten to use VLP table aliases (e.g., vlp4.DestCityName)
    log::info!(
        "🔍 VLP: Rewriting {} GROUP BY expressions",
        plan.group_by.0.len()
    );
    for (idx, group_expr) in plan.group_by.0.iter_mut().enumerate() {
        log::debug!("   GROUP BY[{}]: {:?}", idx, group_expr);
        rewrite_render_expr_for_vlp_with_endpoint_info(
            group_expr,
            &filtered_mappings,
            "t",
            &endpoint_position,
            &cte_column_mapping,
        );
    }

    // 🔧 FIX #6: Also rewrite CTE bodies - BUT ONLY FOR PATH FUNCTION REWRITES (t → vlp1)
    // DO NOT rewrite endpoint aliases (u1 → start_node) in WITH CTEs!
    //
    // WITH CTEs have their own JOINs like: JOIN users AS u1 ON vlp1.start_id = u1.user_id
    // So their SELECT items should use u1/u2 (from JOINs), NOT start_node/end_node (VLP internal)
    //
    // We ONLY need to rewrite the generic "t" alias that comes from path functions
    // like length(path) → t.hop_count, which should become vlp1.hop_count
    log::info!(
        "🔍 VLP: Rewriting {} CTE bodies (PATH FUNCTIONS ONLY)",
        plan.ctes.0.len()
    );

    // Create a mapping that ONLY includes the "t" → vlp alias mapping
    // Exclude endpoint aliases (u1, u2, etc.) for CTE body rewriting
    //
    // Rationale:
    // - Normal VLP: CTE has JOINs (JOIN users AS u1), SELECT should use u1.name ✅
    // - Denormalized VLP: CTE has NO JOINs, properties from VLP CTE columns (vlp1_Origin)
    //   BUT the column names in CTE are already prefixed (u1_name), so we don't rewrite table aliases
    let path_function_mappings: HashMap<String, String> = filtered_mappings
        .iter()
        .filter(|(from_alias, _to_alias)| {
            // Only keep VLP_CTE_FROM_ALIAS mapping (for path functions like length(path))
            // Exclude endpoint node aliases
            *from_alias == VLP_CTE_FROM_ALIAS
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    log::info!(
        "🔍 VLP: Path function mappings for CTE rewrite: {:?}",
        path_function_mappings
    );

    if !path_function_mappings.is_empty() {
        for (idx, cte) in plan.ctes.0.iter_mut().enumerate() {
            // Skip VLP CTEs themselves - only rewrite CTEs that reference VLP results
            if cte.cte_name.starts_with("vlp_cte") || cte.cte_name.starts_with("chained_path_") {
                log::debug!("   CTE[{}]: Skipping VLP CTE '{}'", idx, cte.cte_name);
                continue;
            }

            log::info!(
                "   CTE[{}]: Rewriting path functions in CTE body '{}'",
                idx,
                cte.cte_name
            );

            // CTEs have a content field that can be Structured(RenderPlan) or RawSql(String)
            // We only need to rewrite Structured CTEs
            if let CteContent::Structured(ref mut cte_plan) = cte.content {
                // Rewrite SELECT items in the CTE (only t → vlp alias)
                log::info!(
                    "      CTE: Rewriting {} SELECT items (path functions only)",
                    cte_plan.select.items.len()
                );
                for (item_idx, select_item) in cte_plan.select.items.iter_mut().enumerate() {
                    log::info!(
                        "         SELECT[{}]: {:?}",
                        item_idx,
                        select_item.expression
                    );
                    rewrite_render_expr_for_vlp(
                        &mut select_item.expression,
                        &path_function_mappings,
                    );
                }

                // Rewrite WHERE clause if present
                if let Some(ref mut where_expr) = cte_plan.filters.0 {
                    log::debug!("      CTE: Rewriting WHERE clause (path functions only)");
                    rewrite_render_expr_for_vlp(where_expr, &path_function_mappings);
                }

                // Rewrite GROUP BY if present
                log::info!(
                    "      CTE: Rewriting {} GROUP BY expressions (path functions only)",
                    cte_plan.group_by.0.len()
                );
                for (group_idx, group_expr) in cte_plan.group_by.0.iter_mut().enumerate() {
                    log::debug!("         GROUP BY[{}]: {:?}", group_idx, group_expr);
                    rewrite_render_expr_for_vlp(group_expr, &path_function_mappings);
                }
            }
        }
    } else {
        log::debug!("🔍 VLP: No path function mappings - skipping CTE body rewrite");
    }

    Ok(())
}

pub(crate) fn replace_wildcards_with_group_by_columns(
    select_items: Vec<SelectItem>,
    group_by_columns: &[RenderExpr],
    with_alias: &str,
) -> Vec<SelectItem> {
    let mut new_items = Vec::new();

    for item in select_items.iter() {
        let is_wildcard = match &item.expression {
            RenderExpr::Column(col) if col.0.raw() == "*" => true,
            RenderExpr::PropertyAccessExp(pa) if pa.column.raw() == "*" => true,
            _ => false,
        };

        if is_wildcard && !group_by_columns.is_empty() {
            // Replace wildcard with the actual GROUP BY columns
            for gb_expr in group_by_columns {
                let col_alias = if let RenderExpr::PropertyAccessExp(pa) = gb_expr {
                    Some(ColumnAlias(format!(
                        "{}.{}",
                        pa.table_alias.0,
                        pa.column.raw()
                    )))
                } else {
                    None
                };
                new_items.push(SelectItem {
                    expression: gb_expr.clone(),
                    col_alias,
                });
            }
        } else if is_wildcard {
            // No GROUP BY columns - convert bare `*` to `with_alias.*` as fallback
            new_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(with_alias.to_string()),
                    column: PropertyValue::Column("*".to_string()),
                }),
                col_alias: item.col_alias.clone(),
            });
        } else {
            // Check if it's a TableAlias that needs expansion
            match &item.expression {
                RenderExpr::TableAlias(ta) => {
                    // Find corresponding GROUP BY expression for this alias
                    let group_by_expr = group_by_columns.iter().find(|expr| {
                        if let RenderExpr::PropertyAccessExp(pa) = expr {
                            pa.table_alias.0 == ta.0
                        } else {
                            false
                        }
                    });

                    if let Some(gb_expr) = group_by_expr {
                        // Use the same expression as GROUP BY
                        new_items.push(SelectItem {
                            expression: gb_expr.clone(),
                            col_alias: item.col_alias.clone(),
                        });
                    } else {
                        // Fallback: No matching GROUP BY found, keep as-is
                        new_items.push(item.clone());
                    }
                }
                _ => {
                    // Not a wildcard or TableAlias, keep as-is
                    new_items.push(item.clone());
                }
            }
        }
    }

    new_items
}

/// Detect if an alias is a VLP (Variable-Length Path) endpoint by examining the plan structure.
///
/// This is a fallback method when plan_ctx is not available during rendering.
/// It traverses the plan to find GraphRel nodes with variable_length and determines
/// if the alias is a start or end endpoint.
///
/// Returns Some(VlpEndpointInfo) if the alias is a VLP endpoint, None otherwise.
use crate::query_planner::join_context::VlpEndpointInfo;

fn detect_vlp_endpoint_from_plan(plan: &LogicalPlan, alias: &str) -> Option<VlpEndpointInfo> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this is a variable-length pattern (not fixed-length like *1, *2)
            if let Some(spec) = &rel.variable_length {
                // Fixed-length patterns (*1, *2, *3) don't use CTE column naming
                let is_fixed_length = spec.exact_hop_count().is_some();
                if is_fixed_length {
                    // Continue searching in child nodes
                    if let Some(info) = detect_vlp_endpoint_from_plan(&rel.left, alias) {
                        return Some(info);
                    }
                    if let Some(info) = detect_vlp_endpoint_from_plan(&rel.right, alias) {
                        return Some(info);
                    }
                    return None;
                }

                // Check if alias matches left_connection (start endpoint)
                if alias == rel.left_connection {
                    log::debug!(
                        "🔍 detect_vlp_endpoint_from_plan: '{}' is VLP START endpoint (rel='{}')",
                        alias,
                        rel.alias
                    );
                    return Some(VlpEndpointInfo {
                        position: VlpPosition::Start,
                        other_endpoint_alias: rel.right_connection.clone(),
                        rel_alias: rel.alias.clone(),
                        vlp_alias: crate::query_planner::join_context::VLP_CTE_FROM_ALIAS
                            .to_string(),
                    });
                }

                // Check if alias matches right_connection (end endpoint)
                if alias == rel.right_connection {
                    log::debug!(
                        "🔍 detect_vlp_endpoint_from_plan: '{}' is VLP END endpoint (rel='{}')",
                        alias,
                        rel.alias
                    );
                    return Some(VlpEndpointInfo {
                        position: VlpPosition::End,
                        other_endpoint_alias: rel.left_connection.clone(),
                        rel_alias: rel.alias.clone(),
                        vlp_alias: crate::query_planner::join_context::VLP_CTE_FROM_ALIAS
                            .to_string(),
                    });
                }
            }

            // Not a VLP endpoint in this GraphRel, search children
            if let Some(info) = detect_vlp_endpoint_from_plan(&rel.left, alias) {
                return Some(info);
            }
            if let Some(info) = detect_vlp_endpoint_from_plan(&rel.right, alias) {
                return Some(info);
            }
            None
        }
        LogicalPlan::GraphNode(_) => None,
        LogicalPlan::ViewScan(_) => None,
        LogicalPlan::Projection(proj) => detect_vlp_endpoint_from_plan(&proj.input, alias),
        LogicalPlan::Filter(filter) => detect_vlp_endpoint_from_plan(&filter.input, alias),
        LogicalPlan::Limit(limit) => detect_vlp_endpoint_from_plan(&limit.input, alias),
        LogicalPlan::GraphJoins(gj) => detect_vlp_endpoint_from_plan(&gj.input, alias),
        LogicalPlan::WithClause(wc) => detect_vlp_endpoint_from_plan(&wc.input, alias),
        LogicalPlan::OrderBy(ob) => detect_vlp_endpoint_from_plan(&ob.input, alias),
        LogicalPlan::Skip(skip) => detect_vlp_endpoint_from_plan(&skip.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(info) = detect_vlp_endpoint_from_plan(&cp.left, alias) {
                return Some(info);
            }
            detect_vlp_endpoint_from_plan(&cp.right, alias)
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                if let Some(info) = detect_vlp_endpoint_from_plan(input, alias) {
                    return Some(info);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the Cypher property name used as the node ID for a given alias.
/// For denormalized nodes, the ViewScan.id_column is the ClickHouse column (e.g., origin_code),
/// but CTE columns use Cypher property names (e.g., code). This function reverse-looks up
/// the Cypher property from from_node_properties.
fn find_cypher_id_property_for_alias(plan: &LogicalPlan, alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                if let Some(ref from_props) = scan.from_node_properties {
                    for (cypher_prop, ch_col) in from_props {
                        if ch_col.raw() == scan.id_column {
                            return Some(cypher_prop.clone());
                        }
                    }
                }
            } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                // For denormalized nodes with UNION input, check first branch
                if let Some(first) = union_plan.inputs.first() {
                    if let LogicalPlan::ViewScan(scan) = first.as_ref() {
                        if let Some(ref from_props) = scan.from_node_properties {
                            for (cypher_prop, ch_col) in from_props {
                                if ch_col.raw() == scan.id_column {
                                    return Some(cypher_prop.clone());
                                }
                            }
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::GraphRel(rel) => find_cypher_id_property_for_alias(&rel.left, alias)
            .or_else(|| find_cypher_id_property_for_alias(&rel.right, alias)),
        LogicalPlan::Projection(p) => find_cypher_id_property_for_alias(&p.input, alias),
        LogicalPlan::Filter(f) => find_cypher_id_property_for_alias(&f.input, alias),
        LogicalPlan::GroupBy(g) => find_cypher_id_property_for_alias(&g.input, alias),
        LogicalPlan::GraphJoins(j) => find_cypher_id_property_for_alias(&j.input, alias),
        LogicalPlan::OrderBy(o) => find_cypher_id_property_for_alias(&o.input, alias),
        LogicalPlan::Skip(s) => find_cypher_id_property_for_alias(&s.input, alias),
        LogicalPlan::Limit(l) => find_cypher_id_property_for_alias(&l.input, alias),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .find_map(|i| find_cypher_id_property_for_alias(i, alias)),
        LogicalPlan::CartesianProduct(cp) => find_cypher_id_property_for_alias(&cp.left, alias)
            .or_else(|| find_cypher_id_property_for_alias(&cp.right, alias)),
        LogicalPlan::WithClause(wc) => find_cypher_id_property_for_alias(&wc.input, alias),
        _ => None,
    }
}

/// Compute the CTE ID column name for an alias using the deterministic formula.
/// This should be called AFTER expand_table_alias_to_select_items generates the columns.
///
/// CTE columns use Cypher property names (e.g., a_code), not ClickHouse column names
/// (e.g., a_origin_code). For denormalized nodes where these differ, we use the
/// Cypher property name from the schema's from_node_properties mapping.
///
/// This is the single source of truth for alias→ID column mapping.
pub(crate) fn compute_cte_id_column_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    // For denormalized nodes, use the Cypher property name (e.g., "code")
    // rather than the ClickHouse column (e.g., "origin_code")
    if let Some(cypher_id) = find_cypher_id_property_for_alias(plan, alias) {
        log::info!(
            "📊 compute_cte_id: alias '{}' → Cypher ID property '{}' (denormalized)",
            alias,
            cypher_id
        );
        return Some(cte_column_name(alias, &cypher_id));
    }

    // Fallback: use find_id_column_for_alias (returns ClickHouse column name)
    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
        Some(cte_column_name(alias, &id_col))
    } else {
        None
    }
}

pub(crate) fn expand_table_alias_to_select_items(
    alias: &str,
    plan: &LogicalPlan,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    has_aggregation: bool,
    plan_ctx: Option<&PlanCtx>,
    _vlp_cte_metadata: Option<
        &HashMap<String, (String, Vec<crate::render_plan::CteColumnMetadata>)>,
    >,
) -> Vec<SelectItem> {
    log::info!(
        "🔍 expand_table_alias_to_select_items: Expanding alias '{}', cte_references={:?}",
        alias,
        cte_references
    );

    // STEP 1: Check if analyzer resolved this alias to a CTE
    if let Some(cte_name) = cte_references.get(alias) {
        log::info!(
            "✅ expand_table_alias_to_select_items: Found CTE ref '{}' -> '{}'",
            alias,
            cte_name
        );
        log::info!(
            "🔍 expand_table_alias_to_select_items: Available CTE schemas: {:?}",
            cte_schemas.keys().collect::<Vec<_>>()
        );

        // STEP 2: Get columns from that CTE with this alias prefix
        if let Some(meta) = cte_schemas.get(cte_name) {
            log::info!(
                "✅ expand_table_alias_to_select_items: Found CTE schema '{}' with {} items",
                cte_name,
                meta.select_items.len()
            );
            // Calculate the CTE alias used in FROM clause
            // Special case: __union_vlp is a pseudo-CTE representing UNION results
            // The actual subquery alias is __union
            let cte_alias = if cte_name == "__union_vlp" {
                "__union".to_string()
            } else {
                // Normal CTE: extract FROM alias (e.g., "with_a_b_cte_1" -> "a_b")
                extract_from_alias_from_cte_name(cte_name).to_string()
            };

            let is_union_reference = cte_name == "__union_vlp";

            let alias_prefix_underscore = format!("{}_", alias);
            let alias_prefix_dot = format!("{}.", alias);
            log::debug!(
                "expand_table_alias_to_select_items: CTE '{}' has {} items",
                cte_name,
                meta.select_items.len()
            );
            let filtered_items: Vec<SelectItem> = meta.select_items.iter()
                .filter(|item| {
                    if let Some(col_alias) = &item.col_alias {
                        // Match columns that belong to this alias:
                        // 1. Parse p{N}_ format (e.g., "p6_person_id" for alias "person")
                        // 2. Start with alias_ (legacy format, e.g., "friend_firstName")
                        // 3. Start with alias. (e.g., "friend.birthday" from UNION subqueries)
                        // 4. Exactly match the alias (e.g., "cnt" for alias "cnt")
                        let matches_parsed = parse_cte_column(&col_alias.0)
                            .map(|(parsed_alias, _)| parsed_alias == alias)
                            .unwrap_or(false);
                        let matches_underscore = col_alias.0.starts_with(&alias_prefix_underscore);
                        let matches_dot = col_alias.0.starts_with(&alias_prefix_dot);
                        let matches_exact = col_alias.0 == alias;
                        matches_parsed || matches_underscore || matches_dot || matches_exact
                    } else {
                        false
                    }
                })
                .map(|item| {
                    // CRITICAL: Rewrite to use CTE's column names and table alias
                    // The CTE has columns like "a_city", "a_name" (from col_alias)
                    // We need to reference them as: a_b.a_city, a_b.a_name
                    // NOT the original DB columns like: a_b.city, a_b.full_name
                    //
                    // ALSO: For UNION subquery columns with dots (e.g., "friend.birthday"),
                    // we reference them as quoted identifiers and output underscore aliases
                    let (mut rewritten_expr, output_alias) = if let Some(ref cte_col_alias) = item.col_alias {
                        // Check if column name has dots (from UNION subquery)
                        let col_name = &cte_col_alias.0;
                        if col_name.contains('.') {
                            // Column with dot notation (e.g., "friend.birthday")
                            // Handling depends on whether we're referencing a UNION or a CTE:
                            // - UNION: columns aliased as "friend.birthday" (quoted) → reference as __union."friend.birthday"
                            // - CTE: columns aliased as friend_birthday (underscore) → reference as cte_alias.friend_birthday
                            let normalized_alias = col_name.replace('.', "_");

                            if is_union_reference {
                                // UNION reference: use quoted dotted column name
                                (
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(cte_alias.to_string()),
                                        column: PropertyValue::Column(col_name.clone()),
                                    }),
                                    Some(ColumnAlias(normalized_alias)),
                                )
                            } else {
                                // CTE reference: use normalized underscore column name
                                (
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(cte_alias.to_string()),
                                        column: PropertyValue::Column(normalized_alias.clone()),
                                    }),
                                    Some(ColumnAlias(normalized_alias)),
                                )
                            }
                        } else {
                            // Normal underscore column: use as-is
                            (
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.to_string()),
                                    column: PropertyValue::Column(col_name.clone()),
                                }),
                                item.col_alias.clone(),
                            )
                        }
                    } else {
                        // Fallback: use original expression (shouldn't happen for CTE columns)
                        (item.expression.clone(), item.col_alias.clone())
                    };

                    // 🔧 FIX: Wrap with any() aggregation if needed
                    // When has_aggregation=true, non-ID columns must be wrapped with any()
                    // to be valid in SELECT with GROUP BY
                    if has_aggregation {
                        // Check if this column is an ID column
                        // ID columns end with "_id" or ".id" (e.g., "friend_id", "friend.id")
                        let is_id_column = if let Some(ref alias_obj) = output_alias {
                            let alias_str = &alias_obj.0;
                            alias_str.ends_with("_id") || alias_str.ends_with(".id")
                        } else {
                            false
                        };

                        if !is_id_column {
                            // Wrap non-ID column with anyLast() aggregation
                            // Note: Use anyLast() not any() to avoid conflict with list predicate any() function
                            rewritten_expr = RenderExpr::AggregateFnCall(AggregateFnCall {
                                name: "anyLast".to_string(),
                                args: vec![rewritten_expr],
                            });
                            log::debug!("🔧 expand_table_alias_to_select_items: Wrapped column '{:?}' with anyLast() for aggregation", output_alias);
                        }
                    }

                    SelectItem {
                        expression: rewritten_expr,
                        col_alias: output_alias,
                    }
                })
                .collect();

            if !filtered_items.is_empty() {
                log::info!(
                    "🔧 expand_table_alias_to_select_items: Found alias '{}' in CTE '{}' ({} columns), using CTE alias '{}'",
                    alias, cte_name, filtered_items.len(), cte_alias
                );
                return filtered_items;
            } else if cte_name.starts_with("vlp_") {
                // VLP CTE columns use start_*/end_* naming, not alias_* prefix.
                // Determine VLP position from metadata, then generate all properties
                // from base schema with the correct VLP column prefix.
                let mut vlp_position_prefix: Option<(String, String)> = None; // (prefix, from_alias)
                if let Some(vlp_metadata) = _vlp_cte_metadata {
                    if let Some((vlp_from_alias, col_metadata)) = vlp_metadata.get(cte_name) {
                        // Find the VLP position for this alias from any matching column
                        for col_meta in col_metadata {
                            if col_meta.cypher_alias == alias {
                                if let Some(pos) = &col_meta.vlp_position {
                                    let prefix = match pos {
                                        super::cte_manager::VlpColumnPosition::Start => "start_",
                                        super::cte_manager::VlpColumnPosition::End => "end_",
                                    };
                                    vlp_position_prefix =
                                        Some((prefix.to_string(), vlp_from_alias.clone()));
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some((prefix, from_alias)) = vlp_position_prefix {
                    // Get property list from base schema, filtered to only those
                    // actually propagated in the VLP CTE (start_*/end_* columns).
                    // Use property requirements to determine what's needed.
                    if let Ok((properties, _)) = plan.get_properties_with_table_alias(alias) {
                        if !properties.is_empty() {
                            // Use property requirements to filter if available
                            let required_props: Option<&std::collections::HashSet<String>> =
                                plan_ctx
                                    .and_then(|ctx| ctx.get_property_requirements())
                                    .and_then(|reqs| reqs.get_requirements(alias));

                            let vlp_items: Vec<SelectItem> = properties
                                .iter()
                                .filter(|(cypher_prop, _db_col)| {
                                    // Always include 'id' property; if requirements exist,
                                    // only include required ones; otherwise include all
                                    *cypher_prop == "id"
                                        || required_props
                                            .map_or(true, |r| r.contains(cypher_prop.as_str()))
                                })
                                .map(|(cypher_prop, db_col)| {
                                    // VLP CTE column: prefix + db_column (e.g., start_content)
                                    let vlp_col = format!("{}{}", prefix, db_col);
                                    let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(from_alias.clone()),
                                        column: PropertyValue::Column(vlp_col),
                                    });
                                    let col_alias_name =
                                        crate::utils::cte_column_naming::cte_column_name(
                                            alias,
                                            cypher_prop,
                                        );
                                    SelectItem {
                                        expression: expr,
                                        col_alias: Some(ColumnAlias(col_alias_name)),
                                    }
                                })
                                .collect();
                            if !vlp_items.is_empty() {
                                log::info!(
                                    "🔧 expand_table_alias_to_select_items: VLP CTE '{}' → {} columns for alias '{}' (prefix={})",
                                    cte_name, vlp_items.len(), alias, prefix
                                );
                                return vlp_items;
                            }
                        }
                    }
                }
                log::warn!(
                    "⚠️ expand_table_alias_to_select_items: VLP CTE '{}' — could not determine position for alias '{}', falling through",
                    cte_name, alias
                );
                // Continue to fallback as recovery attempt
            } else {
                // CTE exists but no columns matched the alias prefix
                // This is an INTERNAL ERROR - analyzer said this alias is from this CTE,
                // but the CTE doesn't have the expected columns!
                log::error!(
                    "❌ INTERNAL ERROR: CTE '{}' found but no columns match prefix '{}_'! Analyzer/render mismatch!",
                    cte_name, alias
                );
                log::error!(
                    "❌ CTE '{}' has {} total columns: {:?}",
                    cte_name,
                    meta.select_items.len(),
                    meta.select_items
                        .iter()
                        .filter_map(|item| item.col_alias.as_ref().map(|a| &a.0))
                        .collect::<Vec<_>>()
                );
                // Continue to fallback as recovery attempt
            }
        } else {
            // CTE not in schemas - could be legitimate if schemas not yet built for this level
            log::warn!("⚠️ expand_table_alias_to_select_items: CTE '{}' not found in cte_schemas (may not be built yet)", cte_name);
        }
    }

    // STEP 2.5: Check if this alias is a VLP endpoint (needs CTE column naming)
    // VLP endpoints like u2 in (u1)-[*1..2]->(u2) need to use columns like t.end_city
    // instead of u2.city from the base table
    //
    // CRITICAL: Only use VLP info if the CURRENT plan tree actually contains a VLP pattern
    // for this alias. PlanCtx registers VLP endpoints globally, but when building a WITH CTE
    // body, the VLP may be in a LATER scope (after the WITH). Using VLP columns from a later
    // scope contaminates the current CTE with wrong column names.
    let vlp_info_from_plan = detect_vlp_endpoint_from_plan(plan, alias);
    let vlp_info_from_ctx = if vlp_info_from_plan.is_some() {
        // Plan tree confirms VLP — prefer ctx info (more detailed) if available
        plan_ctx.and_then(|ctx| ctx.get_vlp_endpoint(alias))
    } else {
        None
    };

    if let Some(vlp_info) = vlp_info_from_ctx.or(vlp_info_from_plan.as_ref()) {
        log::info!(
            "✅ expand_table_alias_to_select_items: Alias '{}' is VLP endpoint (position={:?}), generating VLP columns",
            alias, vlp_info.position
        );

        // Get properties from the base table to know what columns to generate
        if let Ok((properties, _)) = plan.get_properties_with_table_alias(alias) {
            if !properties.is_empty() {
                // Determine the column prefix based on VLP position
                let col_prefix = match vlp_info.position {
                    VlpPosition::Start => "start",
                    VlpPosition::End => "end",
                };

                // Get property requirements for pruning optimization
                let property_requirements =
                    plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                // Generate SELECT items with VLP column naming
                // VLP CTE columns are named: start_id, end_id, start_city, end_city, etc.
                let mut items = Vec::new();

                // First, add ID column
                // For VLP endpoints, find_id_column_for_alias returns "start_id" or "end_id" directly
                // (these are the VLP CTE column names, not raw DB column names)
                // So we should NOT prefix them again - use them directly
                if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
                    // 🔧 FIX: Don't double-prefix VLP ID columns
                    // If the id_col already starts with the prefix (start_id, end_id), use it directly
                    // Otherwise, apply the prefix (e.g., user_id -> end_user_id)
                    let vlp_col_name = if id_col.starts_with(col_prefix) {
                        id_col.clone()
                    } else {
                        format!("{}_{}", col_prefix, id_col)
                    };
                    // 🔧 CRITICAL FIX (Jan 23, 2026): Don't use explicit table alias for VLP columns during WITH clause expansion
                    // During WITH clause rendering, the FROM alias isn't final yet, so we generate columns without
                    // a table qualifier. The SQL generator will add the correct alias when rendering FROM clauses.
                    items.push(SelectItem {
                        expression: RenderExpr::Column(Column(PropertyValue::Column(
                            vlp_col_name.clone(),
                        ))),
                        col_alias: Some(ColumnAlias(cte_column_name(alias, &id_col))),
                    });
                }

                // Add property columns (e.g., end_city AS u2_city)
                for (prop_name, _) in &properties {
                    // Skip ID column (already added above)
                    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
                        if prop_name == &id_col {
                            continue;
                        }
                    }

                    // Check property requirements for pruning
                    if let Some(reqs) = property_requirements {
                        // If not wildcard and has specific requirements, check if property is needed
                        if !reqs.requires_all(alias) {
                            if let Some(props_needed) = reqs.get_requirements(alias) {
                                if !props_needed.contains(prop_name) {
                                    continue;
                                }
                            }
                        }
                    }

                    // VLP CTE columns are named: end_city, end_name, etc.
                    let vlp_col_name = format!("{}_{}", col_prefix, prop_name);
                    // 🔧 CRITICAL FIX (Jan 23, 2026): Use bare Column expression instead of PropertyAccessExp with table alias
                    // This allows the column to be resolved from context (the FROM clause) rather than requiring a specific alias
                    let mut expr =
                        RenderExpr::Column(Column(PropertyValue::Column(vlp_col_name.clone())));

                    // Wrap with anyLast() if aggregation is needed
                    if has_aggregation {
                        expr = RenderExpr::AggregateFnCall(AggregateFnCall {
                            name: "anyLast".to_string(),
                            args: vec![expr],
                        });
                    }

                    items.push(SelectItem {
                        expression: expr,
                        col_alias: Some(ColumnAlias(cte_column_name(alias, prop_name))),
                    });
                }

                log::info!(
                    "🔧 expand_table_alias_to_select_items: Generated {} VLP columns for alias '{}' (prefix='{}', using bare Column expressions)",
                    items.len(), alias, col_prefix
                );

                return items;
            }
        }
    }

    // STEP 3: Not a CTE reference - it's a fresh variable from current MATCH
    match plan.get_properties_with_table_alias(alias) {
        Ok((properties, actual_table_alias)) => {
            log::debug!("🔍🔍 expand_table_alias_to_select_items: alias='{}', got {} properties, actual_table_alias={:?}",
                       alias, properties.len(), actual_table_alias);

            if !properties.is_empty() {
                // Get ID column for aggregation handling
                let id_col = plan
                    .find_id_column_for_alias(alias)
                    .unwrap_or_else(|_| "id".to_string());

                // Get property requirements for pruning optimization (Dec 2025)
                let property_requirements =
                    plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                // 🔧 FIX: For VLP queries with JOINs, use the Cypher alias (e.g., "u1") not VLP internal alias (e.g., "start_node")
                // When WITH clause has "WITH u1, u2", we want to SELECT from the JOIN aliases u1/u2,
                // not from the VLP CTE internal aliases start_node/end_node (which don't exist in FROM clause)
                //
                // IMPORTANT: actual_table_alias is None for VLP endpoints because they come from ViewScan
                // which doesn't track the internal VLP aliases. So we use the Cypher alias (u1/u2) instead.
                let table_alias_to_use = if let Some(ref table_alias) = actual_table_alias {
                    if table_alias == "start_node" || table_alias == "end_node" {
                        // VLP internal alias detected (shouldn't happen, but handle it)
                        log::debug!("🔧 expand_table_alias_to_select_items: VLP internal alias '{}' detected, using Cypher alias '{}' instead", table_alias, alias);
                        Some(alias.to_string())
                    } else {
                        log::info!(
                            "🔧 expand_table_alias_to_select_items: Using actual_table_alias '{}'",
                            table_alias
                        );
                        actual_table_alias.clone()
                    }
                } else {
                    // No table alias from plan - use the Cypher alias
                    // This is the common case for VLP endpoints where ViewScan returns None
                    log::debug!("🔧 expand_table_alias_to_select_items: No actual_table_alias, using Cypher alias '{}'", alias);
                    Some(alias.to_string())
                };

                // Use unified expansion helper with aggregation support (Dec 2025)
                use crate::render_plan::property_expansion::{
                    expand_alias_to_select_items_unified, PropertyAliasFormat,
                };
                let items = expand_alias_to_select_items_unified(
                    alias,
                    properties,
                    &id_col,
                    table_alias_to_use.clone(),
                    has_aggregation, // Enables anyLast() wrapping for non-ID columns
                    PropertyAliasFormat::Underscore,
                    property_requirements, // Enable property pruning if requirements available
                );

                log::info!(
                    "🔧 expand_table_alias_to_select_items: Found alias '{}' in base tables ({} properties), using table alias '{}'",
                    alias, items.len(), table_alias_to_use.as_deref().unwrap_or(alias)
                );

                return items;
            }
        }
        Err(e) => {
            log::debug!(
                "🔧 expand_table_alias_to_select_items: Error querying plan for alias '{}': {:?}",
                alias,
                e
            );
        }
    }

    // STEP 4: Fallback - use PlanCtx + schema to get properties
    // This handles cases where the alias exists in JOINs but its GraphNode isn't in the plan tree,
    // e.g., shared variables in comma-separated MATCH patterns:
    //   MATCH (a:Person)-[:IS_LOCATED_IN]->(:City), (a)-[:KNOWS]-(b:Person)
    //   WITH a, b  -- 'a' is in JOINs but its GraphNode has Empty input
    if let Some(ctx) = plan_ctx {
        if let Ok(table_ctx) = ctx.get_table_ctx(alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                if let Ok(node_schema) = ctx.schema().node_schema(&label) {
                    let properties = extract_sorted_properties(&node_schema.property_mappings);
                    if !properties.is_empty() {
                        let id_col = node_schema
                            .node_id
                            .id
                            .columns()
                            .first()
                            .unwrap_or(&"id")
                            .to_string();
                        let property_requirements = ctx.get_property_requirements();

                        use crate::render_plan::property_expansion::{
                            expand_alias_to_select_items_unified, PropertyAliasFormat,
                        };
                        let items = expand_alias_to_select_items_unified(
                            alias,
                            properties,
                            &id_col,
                            Some(alias.to_string()),
                            has_aggregation,
                            PropertyAliasFormat::Underscore,
                            property_requirements,
                        );

                        log::info!(
                            "🔧 expand_table_alias_to_select_items: Found alias '{}' via schema fallback (label='{}', {} properties)",
                            alias, label, items.len()
                        );

                        return items;
                    }
                }
            }
        }
    }

    log::debug!(
        "🔧 expand_table_alias_to_select_items: Alias '{}' not found (not in CTE refs, not in base tables, not in schema)",
        alias
    );
    Vec::new()
}
pub(crate) fn expand_table_alias_to_group_by_id_only(
    alias: &str,
    plan: &LogicalPlan,
    schema: &GraphSchema,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    // Optional VLP CTE metadata for deterministic lookups (Phase 3 CTE integration)
    vlp_cte_metadata: Option<&HashMap<String, (String, Vec<super::CteColumnMetadata>)>>,
) -> Vec<RenderExpr> {
    log::info!(
        "🔧 expand_table_alias_to_group_by_id_only: Looking for ID column for alias '{}'",
        alias
    );

    // ZEROTH: Check if this is a VLP endpoint alias in a GraphRel with variable_length
    // For VLP queries, the FROM clause uses "vlp_xxx AS t", so we need to use "t" as the table alias
    // not the original Cypher alias (e.g., "u2")
    if let Some(graph_rel) = get_graph_rel_from_plan(plan) {
        if graph_rel.variable_length.is_some() {
            let is_start = alias == graph_rel.left_connection;
            let is_end = alias == graph_rel.right_connection;

            if is_start || is_end {
                // PHASE 3: Use VLP CTE metadata for deterministic lookup if available
                // Otherwise fall back to semantic defaults (start_id/end_id)
                if let Some(vlp_metadata) = vlp_cte_metadata {
                    // Find the VLP CTE for this pattern
                    let vlp_cte_prefix = format!(
                        "vlp_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );
                    for (cte_name, (from_alias, columns)) in vlp_metadata {
                        if cte_name.starts_with(&vlp_cte_prefix) || cte_name.starts_with("vlp_") {
                            // Look up the ID column for this alias from the metadata
                            if let Some(col_meta) = columns
                                .iter()
                                .find(|c| c.cypher_alias == alias && c.is_id_column)
                            {
                                log::info!(
                                    "🔧 expand_table_alias_to_group_by_id_only: Using VLP CTE metadata: '{}.{}' for alias '{}'",
                                    from_alias, col_meta.cte_column_name, alias
                                );
                                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(from_alias.clone()),
                                    column: PropertyValue::Column(col_meta.cte_column_name.clone()),
                                })];
                            }
                        }
                    }
                }

                // ⚠️ FALLBACK: CTE metadata lookup FAILED - using constants from join_context.rs
                // This indicates a gap in CTE metadata propagation. The deterministic path
                // via CteColumnMetadata should have found this alias.
                let vlp_alias = VLP_CTE_FROM_ALIAS;
                let id_column = if is_start {
                    VLP_START_ID_COLUMN
                } else {
                    VLP_END_ID_COLUMN
                };

                log::debug!(
                    "⚠️ expand_table_alias_to_group_by_id_only: METADATA MISSING for VLP endpoint '{}'. \
                    Falling back to conventions: '{}.{}'. \
                    This should not happen - investigate why CteColumnMetadata lookup failed. \
                    Graph pattern: ({})--[{:?}]-->({})",
                    alias, vlp_alias, id_column,
                    graph_rel.left_connection, graph_rel.labels, graph_rel.right_connection
                );
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(vlp_alias.to_string()),
                    column: PropertyValue::Column(id_column.to_string()),
                })];
            }
        }
    }

    // FIRST: Check if this alias comes from a CTE (e.g., VLP CTE or UNION pseudo-CTE)
    if let Some(cte_name) = cte_references.get(alias) {
        log::info!(
            "🔧 expand_table_alias_to_group_by_id_only: Alias '{}' is from CTE '{}'",
            alias,
            cte_name
        );
        if let Some(meta) = cte_schemas.get(cte_name) {
            if let Some(id_col) = meta.alias_to_id.get(alias) {
                // Special case: __union_vlp is a pseudo-CTE representing UNION results
                // For UNION subqueries, GROUP BY needs to reference: __union."friend.id"
                // (table alias is __union, column name is "alias.id" with dots)
                if cte_name == "__union_vlp" {
                    // UNION subquery: use __union as table alias and "alias.id" as column
                    let dot_column_name = format!("{}.{}", alias, id_col);
                    log::debug!("🔧 expand_table_alias_to_group_by_id_only: UNION pattern - using __union.\"{}\"", dot_column_name);
                    return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("__union".to_string()),
                        column: PropertyValue::Column(dot_column_name),
                    })];
                }

                // Normal CTE: use FROM alias and id column
                let from_alias = extract_from_alias_from_cte_name(cte_name);
                log::debug!("🔧 expand_table_alias_to_group_by_id_only: Using ID column '{}' from CTE schema for alias '{}', FROM alias '{}'", id_col, alias, from_alias);
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(from_alias.to_string()),
                    column: PropertyValue::Column(id_col.clone()),
                })];
            } else if meta.column_names.contains(&alias.to_string()) {
                // Fallback: CTE has a direct column matching alias (e.g. UNWIND scalar)
                let from_alias = extract_from_alias_from_cte_name(cte_name);
                log::info!("🔧 expand_table_alias_to_group_by_id_only: Using bare column '{}' from CTE '{}' for alias '{}', FROM alias '{}'", alias, cte_name, alias, from_alias);
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(from_alias.to_string()),
                    column: PropertyValue::Column(alias.to_string()),
                })];
            } else {
                log::warn!("⚠️ expand_table_alias_to_group_by_id_only: CTE '{}' does not have ID mapping for alias '{}'", cte_name, alias);
            }
        } else {
            log::debug!(
                "⚠️ expand_table_alias_to_group_by_id_only: CTE '{}' not found in schemas",
                cte_name
            );
        }
    }

    // SECOND: Use find_id_column_for_alias which traverses the plan to find ViewScan.id_column
    // This is more reliable than find_label_for_alias because it directly gets the ID from the schema
    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
        log::debug!("🔧 expand_table_alias_to_group_by_id_only: Using ID column '{}' from ViewScan for alias '{}'", id_col, alias);
        return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(id_col),
        })];
    }

    // Fallback 1: Try to find label and look up in schema
    if let Some(label) = find_label_for_alias(plan, alias) {
        log::info!(
            "🔧 expand_table_alias_to_group_by_id_only: Found label '{}' for alias '{}'",
            label,
            alias
        );
        if let Some(node_schema) = schema.node_schema_opt(&label) {
            // Unified: columns() works for both single and composite IDs
            let cols = node_schema.node_id.columns();
            log::info!("🔧 expand_table_alias_to_group_by_id_only: Using node_id columns {:?} for alias '{}'",
                cols, alias);
            return cols
                .iter()
                .map(|col| {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(col.to_string()),
                    })
                })
                .collect();
        } else {
            log::debug!(
                "⚠️ expand_table_alias_to_group_by_id_only: Label '{}' not found in schema",
                label
            );
        }
    } else {
        log::debug!(
            "⚠️ expand_table_alias_to_group_by_id_only: Could not find label for alias '{}'",
            alias
        );
    }

    // Fallback 2: try to get properties and use first one (usually the ID)
    log::debug!(
        "⚠️ expand_table_alias_to_group_by_id_only: Using fallback for alias '{}'",
        alias
    );
    match plan.get_properties_with_table_alias(alias) {
        Ok((properties, actual_table_alias)) => {
            if !properties.is_empty() {
                let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());
                // Just use the first property (typically the ID)
                let (_, col_name) = &properties[0];
                log::warn!("⚠️ expand_table_alias_to_group_by_id_only: Fallback using first property '{}' for alias '{}'", col_name, alias);
                vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(table_alias_to_use),
                    column: PropertyValue::Column(col_name.clone()),
                })]
            } else {
                Vec::new()
            }
        }
        Err(_) => {
            // Final fallback: assume this is a scalar alias from WITH clause
            // For scalars, use the alias as a column reference
            log::warn!("⚠️ expand_table_alias_to_group_by_id_only: Final fallback - treating '{}' as scalar column", alias);
            vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: PropertyValue::Column(alias.to_string()),
            })]
        }
    }
}

/// Helper: Rewrite LogicalExpr to update PropertyAccessExp table aliases with updated CTE names
fn rewrite_logical_expr_cte_refs(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    cte_references: &std::collections::HashMap<String, String>,
    cte_property_mappings: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    >,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::query_planner::logical_expr::LogicalExpr;

    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            // Check if the table_alias references an old CTE name that needs updating
            if let Some(new_cte_name) = cte_references.get(&prop.table_alias.0) {
                // Also resolve the column name to the CTE column name if mapping exists
                let resolved_column =
                    cte_property_mappings
                        .get(&prop.table_alias.0)
                        .and_then(|mapping| {
                            let prop_name = match &prop.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(c) => {
                                c.as_str()
                            }
                            crate::graph_catalog::expression_parser::PropertyValue::Expression(
                                e,
                            ) => e.as_str(),
                        };
                            mapping.get(prop_name).cloned()
                        });

                let new_column = if let Some(ref cte_col) = resolved_column {
                    log::info!(
                        "🔧 rewrite_logical_expr_cte_refs: {}.{} → {}.{} (alias + column resolved)",
                        prop.table_alias.0,
                        prop.column.raw(),
                        new_cte_name,
                        cte_col
                    );
                    crate::graph_catalog::expression_parser::PropertyValue::Column(cte_col.clone())
                } else {
                    log::info!(
                        "🔧 rewrite_logical_expr_cte_refs: Updating table_alias '{}' → '{}' (column '{}' not in CTE mapping)",
                        prop.table_alias.0,
                        new_cte_name,
                        prop.column.raw()
                    );
                    prop.column.clone()
                };

                LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                    table_alias: crate::query_planner::logical_expr::TableAlias(
                        new_cte_name.clone(),
                    ),
                    column: new_column,
                })
            } else {
                expr.clone()
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            let new_operands: Vec<_> = op
                .operands
                .iter()
                .map(|operand| {
                    rewrite_logical_expr_cte_refs(operand, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::OperatorApplicationExp(
                crate::query_planner::logical_expr::OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                },
            )
        }
        LogicalExpr::ScalarFnCall(func) => {
            let new_args: Vec<_> = func
                .args
                .iter()
                .map(|arg| {
                    rewrite_logical_expr_cte_refs(arg, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::ScalarFnCall(crate::query_planner::logical_expr::ScalarFnCall {
                name: func.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::AggregateFnCall(agg) => {
            let new_args: Vec<_> = agg
                .args
                .iter()
                .map(|arg| {
                    rewrite_logical_expr_cte_refs(arg, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::List(items) => {
            let new_items: Vec<_> = items
                .iter()
                .map(|item| {
                    rewrite_logical_expr_cte_refs(item, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::List(new_items)
        }
        // Other expression types don't contain PropertyAccessExp, so clone as-is
        _ => expr.clone(),
    }
}

/// Helper: Rewrite RenderExpr to update PropertyAccessExp table aliases with updated CTE names
fn rewrite_render_expr_cte_refs(
    expr: &crate::render_plan::render_expr::RenderExpr,
    cte_references: &std::collections::HashMap<String, String>,
) -> crate::render_plan::render_expr::RenderExpr {
    use crate::render_plan::render_expr::RenderExpr;

    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Check if the table_alias references an old CTE name that needs updating
            if let Some(new_cte_name) = cte_references.get(&prop.table_alias.0) {
                log::info!(
                    "🔧 rewrite_render_expr_cte_refs: Updating PropertyAccessExp table_alias '{}' → '{}'",
                    prop.table_alias.0,
                    new_cte_name
                );
                RenderExpr::PropertyAccessExp(crate::render_plan::render_expr::PropertyAccess {
                    table_alias: crate::render_plan::render_expr::TableAlias(new_cte_name.clone()),
                    column: prop.column.clone(),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let new_operands: Vec<_> = op
                .operands
                .iter()
                .map(|operand| rewrite_render_expr_cte_refs(operand, cte_references))
                .collect();
            RenderExpr::OperatorApplicationExp(
                crate::render_plan::render_expr::OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                },
            )
        }
        RenderExpr::ScalarFnCall(func) => {
            let new_args: Vec<_> = func
                .args
                .iter()
                .map(|arg| rewrite_render_expr_cte_refs(arg, cte_references))
                .collect();
            RenderExpr::ScalarFnCall(crate::render_plan::render_expr::ScalarFnCall {
                name: func.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args: Vec<_> = agg
                .args
                .iter()
                .map(|arg| rewrite_render_expr_cte_refs(arg, cte_references))
                .collect();
            RenderExpr::AggregateFnCall(crate::render_plan::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        // Other expression types don't contain PropertyAccessExp, so clone as-is
        _ => expr.clone(),
    }
}

/// Find aliases that are fresh table scans (GraphNode → ViewScan) in a plan tree.
/// Used to filter CTE references when propagating into inner scopes — fresh scans
/// should use raw table columns, not CTE column names.
fn find_fresh_table_scan_aliases_in_plan(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    collect_fresh_scan_aliases(plan, &mut aliases);
    aliases
}

fn collect_fresh_scan_aliases(plan: &LogicalPlan, aliases: &mut std::collections::HashSet<String>) {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            if matches!(gn.input.as_ref(), LogicalPlan::ViewScan(_)) {
                aliases.insert(gn.alias.clone());
            }
            collect_fresh_scan_aliases(&gn.input, aliases);
        }
        LogicalPlan::GraphRel(gr) => {
            collect_fresh_scan_aliases(&gr.left, aliases);
            collect_fresh_scan_aliases(&gr.right, aliases);
        }
        LogicalPlan::Projection(p) => collect_fresh_scan_aliases(&p.input, aliases),
        LogicalPlan::Filter(f) => collect_fresh_scan_aliases(&f.input, aliases),
        LogicalPlan::GroupBy(gb) => collect_fresh_scan_aliases(&gb.input, aliases),
        LogicalPlan::OrderBy(ob) => collect_fresh_scan_aliases(&ob.input, aliases),
        LogicalPlan::CartesianProduct(cp) => {
            collect_fresh_scan_aliases(&cp.left, aliases);
            collect_fresh_scan_aliases(&cp.right, aliases);
        }
        LogicalPlan::Unwind(uw) => collect_fresh_scan_aliases(&uw.input, aliases),
        LogicalPlan::GraphJoins(gj) => collect_fresh_scan_aliases(&gj.input, aliases),
        LogicalPlan::Skip(s) => collect_fresh_scan_aliases(&s.input, aliases),
        LogicalPlan::Limit(l) => collect_fresh_scan_aliases(&l.input, aliases),
        LogicalPlan::WithClause(_) => {} // Stop at WITH boundary
        _ => {}
    }
}

pub(crate) fn update_graph_joins_cte_refs(
    plan: &LogicalPlan,
    cte_references: &std::collections::HashMap<String, String>,
    cte_property_mappings: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    >,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating GraphJoins.cte_references from {:?} to {:?}",
                gj.cte_references,
                cte_references
            );

            let new_input =
                update_graph_joins_cte_refs(&gj.input, cte_references, cte_property_mappings)?;

            // CRITICAL FIX: Update anchor_table considering WITH clause scope barriers
            // Problem: After WITH clauses, only exported variables remain in scope.
            // The anchor_table may reference a variable that's no longer accessible (scope barrier violation).
            //
            // Solution Strategy:
            // 1. If NO cte_references → no WITH clauses, keep anchor as-is (no scope barriers)
            // 2. If anchor_table is in cte_references → it's valid, keep it
            // 3. If anchor_table is NOT in cte_references → scope violation, try to find replacement:
            //    a. Look for a join whose table_alias IS in cte_references (visible variable)
            //    b. Pick the first such join as the new anchor
            //    c. If no valid replacement found, set to None (FROM will be determined from joins)
            let new_anchor_table = if cte_references.is_empty() {
                // No CTE references means no WITH clauses - keep anchor unchanged
                log::debug!("🔧 update_graph_joins_cte_refs: No CTE references, keeping anchor_table as-is: {:?}", gj.anchor_table);
                gj.anchor_table.clone()
            } else if let Some(ref anchor) = gj.anchor_table {
                if cte_references.contains_key(anchor) {
                    // Anchor IS in cte_references - it's a valid variable in current scope
                    log::info!(
                        "🔧 update_graph_joins_cte_refs: anchor_table '{}' is in scope (cte_references: {:?})",
                        anchor,
                        cte_references.keys().collect::<Vec<_>>()
                    );
                    Some(anchor.clone())
                } else {
                    // Anchor NOT in cte_references — check if it's a valid variable
                    // from the current MATCH clause (not a scope-violated WITH variable).
                    // If anchor matches a join's table_alias, it's a new variable from the
                    // second MATCH and should be kept.
                    let anchor_in_joins = gj.joins.iter().any(|j| &j.table_alias == anchor);
                    if anchor_in_joins {
                        log::info!(
                            "🔧 update_graph_joins_cte_refs: anchor_table '{}' not in CTE scope but exists in joins — keeping as valid new variable",
                            anchor,
                        );
                        Some(anchor.clone())
                    } else {
                        // Anchor NOT in joins either — scope violation
                        log::debug!(
                            "🔧 update_graph_joins_cte_refs: anchor_table '{}' NOT in scope. \
                             Scope barrier violation! Available CTEs: {:?}",
                            anchor,
                            cte_references.keys().collect::<Vec<_>>()
                        );

                        // Search joins for a valid anchor (table_alias must be in cte_references)
                        let replacement_anchor = gj.joins.iter()
                            .find(|j| cte_references.contains_key(&j.table_alias))
                            .map(|j| {
                                log::info!(
                                    "🔧 update_graph_joins_cte_refs: Found replacement anchor '{}' from joins",
                                    j.table_alias
                                );
                                j.table_alias.clone()
                            });

                        if replacement_anchor.is_none() {
                            log::debug!(
                                "🔧 update_graph_joins_cte_refs: No valid replacement anchor found in joins. \
                                 Setting to None (will be determined during extraction)."
                            );
                        }

                        replacement_anchor
                    }
                }
            } else {
                None
            };

            // 🔧 FIX: Update Join.table_name for CTEs in the joins array
            // When a CTE is finalized during rendering (e.g., "with_user_obj_cte" → "with_user_obj_cte_1"),
            // we need to update the table_name in joins that reference it.
            let updated_joins: Vec<_> = gj.joins.iter().map(|j| {
                // Check if this join's table_alias references a CTE with an updated name
                if let Some(new_cte_name) = cte_references.get(&j.table_alias) {
                    // Check if the table_name needs updating (it's a CTE reference)
                    // CTE table names don't have database prefix, regular tables do
                    if !j.table_name.contains('.') && &j.table_name != new_cte_name {
                        log::info!(
                            "🔧 update_graph_joins_cte_refs: Updating Join.table_name '{}' → '{}' for alias '{}'",
                            j.table_name,
                            new_cte_name,
                            j.table_alias
                        );
                        let mut updated_join = j.clone();
                        updated_join.table_name = new_cte_name.clone();
                        return updated_join;
                    }
                }
                j.clone()
            }).collect();

            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: updated_joins,
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: new_anchor_table,
                cte_references: cte_references.clone(), // UPDATE HERE!
                correlation_predicates: gj.correlation_predicates.clone(),
            }))
        }
        LogicalPlan::GraphRel(gr) => {
            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating GraphRel.cte_references from {:?} to {:?}",
                gr.cte_references,
                cte_references
            );

            // Recursively update children
            let new_left =
                update_graph_joins_cte_refs(&gr.left, cte_references, cte_property_mappings)?;
            let new_center =
                update_graph_joins_cte_refs(&gr.center, cte_references, cte_property_mappings)?;
            let new_right =
                update_graph_joins_cte_refs(&gr.right, cte_references, cte_property_mappings)?;

            Ok(LogicalPlan::GraphRel(GraphRel {
                left: Arc::new(new_left),
                center: Arc::new(new_center),
                right: Arc::new(new_right),
                cte_references: cte_references.clone(), // UPDATE HERE!
                ..gr.clone()
            }))
        }
        LogicalPlan::Projection(proj) => {
            let new_input =
                update_graph_joins_cte_refs(&proj.input, cte_references, cte_property_mappings)?;

            // 🔧 FIX: Update PropertyAccessExp expressions in projection items with updated CTE names
            let updated_items: Vec<_> = proj
                .items
                .iter()
                .map(|item| {
                    let updated_expr = rewrite_logical_expr_cte_refs(
                        &item.expression,
                        cte_references,
                        cte_property_mappings,
                    );
                    crate::query_planner::logical_plan::ProjectionItem {
                        expression: updated_expr,
                        col_alias: item.col_alias.clone(),
                    }
                })
                .collect();

            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: updated_items,
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::WithClause(wc) => {
            // CRITICAL: Filter CTE references for inner scope.
            // Aliases that are fresh table scans (GraphNode → ViewScan) in the inner scope
            // should NOT inherit outer CTE references. Otherwise, join conditions for fresh
            // scans get rewritten to use CTE column names (e.g., country.p7_country_id
            // instead of country.id), causing resolution failures.
            let fresh_aliases = find_fresh_table_scan_aliases_in_plan(&wc.input);
            let inner_cte_refs: std::collections::HashMap<String, String> = if fresh_aliases
                .is_empty()
            {
                cte_references.clone()
            } else {
                log::debug!(
                    "🔧 update_graph_joins_cte_refs: Filtering CTE refs for fresh scans in inner scope: {:?}",
                    fresh_aliases
                );
                cte_references
                    .iter()
                    .filter(|(alias, _)| !fresh_aliases.contains(*alias))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let inner_prop_mappings: std::collections::HashMap<
                String,
                std::collections::HashMap<String, String>,
            > = if fresh_aliases.is_empty() {
                cte_property_mappings.clone()
            } else {
                cte_property_mappings
                    .iter()
                    .filter(|(alias, _)| !fresh_aliases.contains(*alias))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let new_input =
                update_graph_joins_cte_refs(&wc.input, &inner_cte_refs, &inner_prop_mappings)?;

            // Check if this WithClause's cte_name needs updating
            let updated_cte_name = if let Some(ref old_cte_name) = wc.cte_name {
                // Check if any alias exported by this WITH has a new CTE name
                wc.exported_aliases
                    .iter()
                    .find_map(|alias| cte_references.get(alias))
                    .cloned()
                    .or(Some(old_cte_name.clone()))
            } else {
                None
            };

            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating WithClause.cte_name from {:?} to {:?}",
                wc.cte_name,
                updated_cte_name
            );

            Ok(LogicalPlan::WithClause(WithClause {
                input: Arc::new(new_input),
                cte_name: updated_cte_name,
                cte_references: cte_references.clone(), // UPDATE HERE!
                ..wc.clone()
            }))
        }
        LogicalPlan::Filter(f) => {
            let new_input =
                update_graph_joins_cte_refs(&f.input, cte_references, cte_property_mappings)?;
            let updated_predicate =
                rewrite_logical_expr_cte_refs(&f.predicate, cte_references, cte_property_mappings);
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: updated_predicate,
            }))
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input =
                update_graph_joins_cte_refs(&gb.input, cte_references, cte_property_mappings)?;
            let updated_expressions: Vec<_> = gb
                .expressions
                .iter()
                .map(|expr| {
                    rewrite_logical_expr_cte_refs(expr, cte_references, cte_property_mappings)
                })
                .collect();
            let updated_having = gb
                .having_clause
                .as_ref()
                .map(|h| rewrite_logical_expr_cte_refs(h, cte_references, cte_property_mappings));
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: updated_expressions,
                having_clause: updated_having,
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            }))
        }
        LogicalPlan::OrderBy(ob) => {
            let new_input =
                update_graph_joins_cte_refs(&ob.input, cte_references, cte_property_mappings)?;
            let updated_items: Vec<_> = ob
                .items
                .iter()
                .map(|item| crate::query_planner::logical_plan::OrderByItem {
                    expression: rewrite_logical_expr_cte_refs(
                        &item.expression,
                        cte_references,
                        cte_property_mappings,
                    ),
                    order: item.order.clone(),
                })
                .collect();
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: updated_items,
            }))
        }
        LogicalPlan::Limit(lim) => {
            let new_input =
                update_graph_joins_cte_refs(&lim.input, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: lim.count,
            }))
        }
        LogicalPlan::Skip(skip) => {
            let new_input =
                update_graph_joins_cte_refs(&skip.input, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }
        LogicalPlan::Union(union) => {
            let new_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .map(|input| {
                    update_graph_joins_cte_refs(input, cte_references, cte_property_mappings)
                        .map(|p| Arc::new(p))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: union.union_type.clone(),
            }))
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left =
                update_graph_joins_cte_refs(&cp.left, cte_references, cte_property_mappings)?;
            let new_right =
                update_graph_joins_cte_refs(&cp.right, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }
        other => Ok(other.clone()),
    }
}

/// Collect all "live" table aliases from the plan tree — aliases that appear in
/// GraphNode or GraphRel nodes that are NOT inside a ViewScan/CTE reference.
/// These are the aliases that actually need physical table joins.
fn collect_live_table_aliases(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    use std::collections::HashSet;
    fn collect(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
        match plan {
            LogicalPlan::GraphNode(n) => {
                aliases.insert(n.alias.clone());
                collect(&n.input, aliases);
            }
            LogicalPlan::GraphRel(r) => {
                if !r.alias.is_empty() {
                    aliases.insert(r.alias.clone());
                }
                collect(&r.left, aliases);
                collect(&r.center, aliases);
                collect(&r.right, aliases);
            }
            LogicalPlan::GraphJoins(gj) => collect(&gj.input, aliases),
            LogicalPlan::Projection(p) => collect(&p.input, aliases),
            LogicalPlan::Filter(f) => collect(&f.input, aliases),
            LogicalPlan::OrderBy(o) => collect(&o.input, aliases),
            LogicalPlan::Limit(l) => collect(&l.input, aliases),
            LogicalPlan::GroupBy(g) => collect(&g.input, aliases),
            LogicalPlan::Skip(s) => collect(&s.input, aliases),
            LogicalPlan::Unwind(u) => collect(&u.input, aliases),
            LogicalPlan::CartesianProduct(cp) => {
                collect(&cp.left, aliases);
                collect(&cp.right, aliases);
            }
            LogicalPlan::WithClause(wc) => collect(&wc.input, aliases),
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    collect(input, aliases);
                }
            }
            // ViewScan = CTE reference, NOT a physical table — don't collect
            LogicalPlan::ViewScan(_) => {}
            _ => {}
        }
    }
    let mut aliases = HashSet::new();
    collect(plan, &mut aliases);
    aliases
}

/// Remove pre-computed joins from GraphJoins that are stale after WITH→CTE replacement.
/// A join is stale if:
/// 1. Its table_alias is a CTE-scoped variable
/// 2. Its join conditions reference a CTE-scoped alias
/// 3. Its join conditions reference an alias not in the "live" set
///    (i.e., the alias no longer exists as a physical node in the plan tree)
fn clear_stale_joins_for_cte_aliases(
    plan: &LogicalPlan,
    cte_aliases: &std::collections::HashSet<&str>,
) -> LogicalPlan {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    // Collect all live table aliases from the plan tree
    let live_aliases = collect_live_table_aliases(plan);

    fn clear_recursive(
        plan: &LogicalPlan,
        cte_aliases: &std::collections::HashSet<&str>,
        live_aliases: &std::collections::HashSet<String>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::GraphJoins(gj) => {
                let new_input = clear_recursive(&gj.input, cte_aliases, live_aliases);

                let cleaned_joins: Vec<Join> = gj
                    .joins
                    .iter()
                    .filter(|j| {
                        // Check if this join's alias is CTE-scoped
                        let alias_is_stale = cte_aliases.contains(j.table_alias.as_str());
                        // Check if any join condition references a CTE-scoped alias
                        let condition_refs_cte = j.joining_on.iter().any(|op| {
                            op.operands.iter().any(|operand| {
                                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) = operand {
                                    cte_aliases.contains(pa.table_alias.0.as_str())
                                } else {
                                    false
                                }
                            })
                        });
                        // Check if any join condition references an alias no longer in the plan tree
                        let condition_refs_dead = j.joining_on.iter().any(|op| {
                            op.operands.iter().any(|operand| {
                                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) = operand {
                                    let alias = &pa.table_alias.0;
                                    // Skip VLP/CTE aliases (they're valid render-time
                                    // references not present in the logical plan tree)
                                    !crate::query_planner::join_context::is_vlp_or_cte_alias(alias)
                                        && !live_aliases.contains(alias.as_str())
                                } else {
                                    false
                                }
                            })
                        });
                        if alias_is_stale || condition_refs_cte || condition_refs_dead {
                            log::debug!(
                                "🔧 clear_stale_joins: Removing stale join for '{}' (alias_stale={}, cond_refs_cte={}, cond_refs_dead={})",
                                j.table_alias, alias_is_stale, condition_refs_cte, condition_refs_dead
                            );
                            false
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect();

                LogicalPlan::GraphJoins(GraphJoins {
                    input: Arc::new(new_input),
                    joins: cleaned_joins,
                    optional_aliases: gj.optional_aliases.clone(),
                    anchor_table: gj.anchor_table.clone(),
                    cte_references: gj.cte_references.clone(),
                    correlation_predicates: gj.correlation_predicates.clone(),
                })
            }
            LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection {
                input: Arc::new(clear_recursive(&p.input, cte_aliases, live_aliases)),
                items: p.items.clone(),
                distinct: p.distinct,
                pattern_comprehensions: p.pattern_comprehensions.clone(),
            }),
            LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter {
                input: Arc::new(clear_recursive(&f.input, cte_aliases, live_aliases)),
                predicate: f.predicate.clone(),
            }),
            LogicalPlan::OrderBy(o) => LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(clear_recursive(&o.input, cte_aliases, live_aliases)),
                items: o.items.clone(),
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit {
                input: Arc::new(clear_recursive(&l.input, cte_aliases, live_aliases)),
                count: l.count,
            }),
            LogicalPlan::GroupBy(g) => LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(clear_recursive(&g.input, cte_aliases, live_aliases)),
                expressions: g.expressions.clone(),
                having_clause: g.having_clause.clone(),
                is_materialization_boundary: g.is_materialization_boundary,
                exposed_alias: g.exposed_alias.clone(),
            }),
            LogicalPlan::Skip(s) => LogicalPlan::Skip(Skip {
                input: Arc::new(clear_recursive(&s.input, cte_aliases, live_aliases)),
                count: s.count,
            }),
            LogicalPlan::Unwind(u) => LogicalPlan::Unwind(Unwind {
                input: Arc::new(clear_recursive(&u.input, cte_aliases, live_aliases)),
                expression: u.expression.clone(),
                alias: u.alias.clone(),
                label: u.label.clone(),
                tuple_properties: u.tuple_properties.clone(),
            }),
            // Leaf/other nodes: no joins to clear
            other => other.clone(),
        }
    }

    clear_recursive(plan, cte_aliases, &live_aliases)
}

/// Expand TableAlias expressions in a LogicalPlan's Projection/Selection
/// This is needed when the final SELECT has `RETURN a` where `a` is from a CTE
/// The to_render_plan() method doesn't know about CTEs, so we expand here first.
fn expand_table_aliases_in_plan(
    plan: LogicalPlan,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_expr::ColumnAlias;
    use crate::query_planner::logical_plan::ProjectionItem;

    log::info!("🔍 expand_table_aliases_in_plan: Expanding TableAlias in plan");

    match plan {
        LogicalPlan::Projection(mut proj) => {
            let mut expanded_items = Vec::new();

            for proj_item in &proj.items {
                match &proj_item.expression {
                    LogicalExpr::TableAlias(ta) => {
                        // Check if this alias comes from a CTE
                        if cte_references.contains_key(&ta.0) {
                            log::info!("✅ expand_table_aliases_in_plan: Found TableAlias '{}' from CTE, expanding", ta.0);

                            // Use expand_table_alias_to_select_items to get all columns
                            let expanded_select_items = expand_table_alias_to_select_items(
                                &ta.0,
                                &proj.input,
                                cte_schemas,
                                cte_references,
                                false, // has_aggregation
                                None,  // plan_ctx
                                None,  // vlp_cte_metadata
                            );

                            if expanded_select_items.is_empty() {
                                log::warn!("⚠️ expand_table_aliases_in_plan: No columns found for alias '{}', keeping original", ta.0);
                                expanded_items.push(proj_item.clone());
                            } else {
                                log::info!("✅ expand_table_aliases_in_plan: Expanded alias '{}' to {} columns", ta.0, expanded_select_items.len());

                                // Convert SelectItem to ProjectionItem
                                for select_item in expanded_select_items {
                                    // SelectItem has RenderExpr, ProjectionItem has LogicalExpr
                                    // We need to convert the column reference to a PropertyAccess
                                    let logical_expr = if let Some(col_alias) =
                                        &select_item.col_alias
                                    {
                                        // Extract table alias and column name from col_alias
                                        // Format: "a_user_id" → table=ta.0, column="user_id"
                                        let col_name = &col_alias.0;
                                        let alias_prefix = format!("{}_", ta.0);

                                        if let Some(stripped) = col_name.strip_prefix(&alias_prefix)
                                        {
                                            // Create PropertyAccess: a.user_id
                                            LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: crate::query_planner::logical_expr::TableAlias(ta.0.clone()),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(stripped.to_string()),
                                            })
                                        } else if col_name == &ta.0 {
                                            // Exact match - likely a scalar aggregate like "cnt"
                                            LogicalExpr::Column(
                                                crate::query_planner::logical_expr::Column(
                                                    col_name.clone(),
                                                ),
                                            )
                                        } else {
                                            // Unknown format, keep as column reference
                                            LogicalExpr::Column(
                                                crate::query_planner::logical_expr::Column(
                                                    col_name.clone(),
                                                ),
                                            )
                                        }
                                    } else {
                                        // No alias, shouldn't happen but handle gracefully
                                        log::warn!("⚠️ expand_table_aliases_in_plan: SelectItem has no col_alias");
                                        continue;
                                    };

                                    // Create ProjectionItem with the column alias from SelectItem
                                    let projection_item = ProjectionItem {
                                        expression: logical_expr,
                                        col_alias: select_item
                                            .col_alias
                                            .clone()
                                            .map(|a| ColumnAlias(a.0)),
                                    };

                                    expanded_items.push(projection_item);
                                }
                            }
                        } else {
                            // Not a CTE alias, keep as is (will be handled by to_render_plan)
                            expanded_items.push(proj_item.clone());
                        }
                    }
                    _ => {
                        // Non-TableAlias expression, keep as is
                        expanded_items.push(proj_item.clone());
                    }
                }
            }

            // Replace SELECT items with expanded version
            proj.items = expanded_items;

            // Recursively process input plan
            proj.input = std::sync::Arc::new(expand_table_aliases_in_plan(
                (*proj.input).clone(),
                cte_schemas,
                cte_references,
                schema,
            )?);

            Ok(LogicalPlan::Projection(proj))
        }
        LogicalPlan::Limit(mut lim) => {
            lim.input = std::sync::Arc::new(expand_table_aliases_in_plan(
                (*lim.input).clone(),
                cte_schemas,
                cte_references,
                schema,
            )?);
            Ok(LogicalPlan::Limit(lim))
        }
        LogicalPlan::OrderBy(mut ob) => {
            ob.input = std::sync::Arc::new(expand_table_aliases_in_plan(
                (*ob.input).clone(),
                cte_schemas,
                cte_references,
                schema,
            )?);
            Ok(LogicalPlan::OrderBy(ob))
        }
        // For other plan types, just return as is
        other => Ok(other),
    }
}

/// Extract FROM alias from CTE name by stripping "with_" prefix and "_cte[_<digits>]" suffix
///
/// Examples:
/// - "with_a_follows_cte" → "a_follows"
/// - "with_a_follows_cte_1" → "a_follows"
/// - "with_a_follows_cte_999" → "a_follows"
/// - "a_follows" → "a_follows" (no prefix/suffix to strip)
fn extract_from_alias_from_cte_name(cte_name: &str) -> &str {
    // Strip optional "with_" prefix
    let base = cte_name.strip_prefix("with_").unwrap_or(cte_name);

    // Handle unnumbered suffix "_cte"
    if let Some(stripped) = base.strip_suffix("_cte") {
        return stripped;
    }

    // Handle numbered suffixes like "_cte_1", "_cte_2", ..., "_cte_<digits>"
    if let Some(pos) = base.rfind("_cte_") {
        let suffix = &base[pos + "_cte_".len()..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            return &base[..pos];
        }
    }

    base
}

/// Populate task-local CTE property mappings from a RenderPlan's SELECT items
///
/// This enables PropertyAccessExp rendering to resolve CTE column names correctly.
/// Example: `a_follows.name` → `a_follows.a_name` (because CTE has column "a_name")
///
/// # Arguments
/// * `cte_name` - Full CTE name (e.g., "with_a_follows_cte_1")
/// * `render_plan` - The CTE's RenderPlan containing SELECT items
fn populate_cte_property_mappings_from_render_plan(
    cte_name: &str,
    render_plan: &super::RenderPlan,
) {
    log::debug!(
        "ENTRY: populate_cte_property_mappings_from_render_plan called for CTE '{}'",
        cte_name
    );
    // Compute FROM alias by stripping "with_" and "_cte[_<digits>]" from CTE name
    // Example: "with_a_follows_cte_1" → "a_follows"
    // This handles arbitrary CTE numbering: _cte, _cte_1, _cte_2, ..., _cte_999, etc.
    let from_alias = extract_from_alias_from_cte_name(cte_name);

    let mut cte_mappings: std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    > = std::collections::HashMap::new();
    let mut alias_mapping: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // For denormalized unions, parent SELECT is empty; items are in union branches.
    let select_items: &[SelectItem] = if render_plan.select.items.is_empty() {
        if let UnionItems(Some(ref union)) = render_plan.union {
            if !union.input.is_empty() {
                &union.input[0].select.items
            } else {
                &render_plan.select.items
            }
        } else {
            &render_plan.select.items
        }
    } else {
        &render_plan.select.items
    };

    for select_item in select_items {
        if let Some(col_alias) = &select_item.col_alias {
            let col_name = col_alias.0.clone();

            // Skip internal discriminator columns
            if col_name == "__label__" {
                continue;
            }

            // CTE columns can be:
            // 1. New p{N} format: "p1_a_name" → alias="a", property="name"
            // 2. Legacy prefixed: "a_name", "a_user_id" → property is after underscore
            // 3. Unprefixed: "follows" (aggregate result) → property is the column name itself

            if let Some((_alias, property)) = parse_cte_column(&col_name) {
                // Case 1: New p{N} format
                log::debug!(
                    "🔧 CTE mapping (p{{N}} format): {}.{} → {}",
                    from_alias,
                    property,
                    col_name
                );
                alias_mapping.insert(property, col_name.clone());
            } else if let Some(underscore_pos) = col_name.find('_') {
                // Case 2: Legacy prefixed column like "a_name"
                let property = &col_name[underscore_pos + 1..];
                log::debug!(
                    "🔧 CTE mapping (prefixed): {}.{} → {}",
                    from_alias,
                    property,
                    col_name
                );
                alias_mapping.insert(property.to_string(), col_name.clone());
            } else {
                // Case 2: Unprefixed column like "follows" (aggregate or scalar)
                // Map the column name to itself
                log::debug!(
                    "🔧 CTE mapping (unprefixed): {}.{} → {}",
                    from_alias,
                    &col_name,
                    &col_name
                );
                alias_mapping.insert(col_name.clone(), col_name.clone());
            }
        }
    }

    // Map the FROM alias (e.g., "a_follows") to the property mappings
    cte_mappings.insert(from_alias.to_string(), alias_mapping);

    // Log before moving cte_mappings
    let num_properties = cte_mappings.get(from_alias).map(|m| m.len()).unwrap_or(0);
    log::debug!(
        "🔧 Populated CTE property mappings: CTE '{}' → FROM alias '{}' with {} properties",
        cte_name,
        from_alias,
        num_properties
    );

    // Store in task-local context for SQL rendering
    crate::server::query_context::set_cte_property_mappings(cte_mappings);
}

/// Detect `head(collect({key1: val1, key2: val2, ...})) AS alias` patterns where
/// any MapLiteral value is a bare node reference (TableAlias). ClickHouse map() requires
/// homogeneous value types, but nodes have no single SQL value. Flatten each map entry
/// into separate CTE columns:
///   - Node values → one column per schema property: `arrayElement(groupArray(prop), 1) AS alias_key_prop`
///   - Scalar values → single column: `arrayElement(groupArray(expr), 1) AS alias_key`
///
/// Returns (flattened_items, compound_key_mappings) where compound_key_mappings contains
/// entries like ("msg.id", "latestLike_msg_id") for downstream property_mapping injection.
fn try_flatten_head_collect_map_literal(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    col_alias: Option<&str>,
    plan: &LogicalPlan,
    plan_ctx: Option<&PlanCtx>,
    scope: Option<&super::variable_scope::VariableScope>,
) -> Option<(Vec<SelectItem>, Vec<(String, String)>)> {
    use crate::query_planner::logical_expr::LogicalExpr;

    let alias = col_alias?;

    log::info!(
        "🔧 try_flatten_head_collect_map_literal: checking alias='{}', expr type={:?}",
        alias,
        std::mem::discriminant(expr)
    );

    // Match: ScalarFnCall("head", [AggregateFnCall("collect", [MapLiteral(entries)])])
    let entries = match expr {
        LogicalExpr::ScalarFnCall(sf)
            if sf.name.eq_ignore_ascii_case("head") && sf.args.len() == 1 =>
        {
            match &sf.args[0] {
                LogicalExpr::AggregateFnCall(agg)
                    if agg.name.eq_ignore_ascii_case("collect") && agg.args.len() == 1 =>
                {
                    match &agg.args[0] {
                        LogicalExpr::MapLiteral(entries) => entries,
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        _ => return None,
    };

    // Check if any value is a node reference (TableAlias with multiple properties).
    // Bare variables like "likeTime" are also TableAlias but are scalars — empty/no property mapping.
    // Try multiple detection methods: plan_ctx labels, scope CTE variables, plan tree, schema.
    let is_node_alias = |alias_name: &str| -> bool {
        // Method 1: Check plan_ctx for node labels
        if let Some(ctx) = plan_ctx {
            if let Ok(tc) = ctx.get_table_ctx(alias_name) {
                if tc.get_label_opt().is_some() {
                    return true;
                }
            }
        }
        // Method 2: Check scope CTE variables for multi-property mapping
        if let Some(s) = scope {
            if let Some(cte_info) = s.cte_variables().get(alias_name) {
                log::info!(
                    "🔧 is_node_alias('{}') Method 2: property_mapping.len()={}, labels={:?}",
                    alias_name,
                    cte_info.property_mapping.len(),
                    cte_info.labels
                );
                // Node aliases have multiple properties (id, name, etc.)
                // Scalar aliases have 0 or 1 properties
                if cte_info.property_mapping.len() > 1 {
                    return true;
                }
                // Also check if there are labels
                if !cte_info.labels.is_empty() {
                    return true;
                }
            }
        }
        // Method 3: Check plan tree for GraphNode with this alias
        if let Some(label) =
            crate::render_plan::cte_extraction::get_node_label_for_alias(alias_name, plan)
        {
            if !label.is_empty() {
                return true;
            }
        }
        false
    };

    let has_node_value = entries.iter().any(|(_, v)| {
        if let LogicalExpr::TableAlias(ta) = v {
            is_node_alias(&ta.0)
        } else {
            false
        }
    });
    if !has_node_value {
        return None; // All scalar — keep using map() (preserves bi-14)
    }

    log::info!(
        "🔧 try_flatten_head_collect_map_literal: Flattening {} entries for alias '{}'",
        entries.len(),
        alias
    );

    let schema = crate::server::query_context::get_current_schema();
    let schema_ref = schema.as_deref();

    let mut flattened_items: Vec<SelectItem> = Vec::new();
    // Collect compound key mappings: ("map_key.property", "alias_mapkey_property")
    // These are stored at generation time to avoid ambiguous reverse-engineering from column names.
    let mut compound_keys: Vec<(String, String)> = Vec::new();

    for (key, value) in entries {
        // Determine if this value is a node alias by trying to get its properties.
        // If we find >1 property, it's a node; otherwise treat as scalar.
        let node_properties: Option<Vec<(String, String)>> = if let LogicalExpr::TableAlias(ta) =
            value
        {
            // Try scope CTE variables first (after WITH barriers)
            let from_scope = scope.and_then(|s| {
                s.cte_variables().get(&ta.0).and_then(|cte_info| {
                    if cte_info.property_mapping.len() > 1 {
                        Some(
                            cte_info
                                .property_mapping
                                .iter()
                                .map(|(prop, _cte_col)| (prop.clone(), prop.clone()))
                                .collect(),
                        )
                    } else {
                        None
                    }
                })
            });
            if from_scope.is_some() {
                from_scope
            } else {
                // Try schema lookup via plan_ctx
                let label = plan_ctx
                    .and_then(|ctx| {
                        ctx.get_table_ctx(&ta.0)
                            .ok()
                            .and_then(|tc| tc.get_label_opt())
                    })
                    .or_else(|| {
                        crate::render_plan::cte_extraction::get_node_label_for_alias(&ta.0, plan)
                    });
                label
                    .and_then(|l| schema_ref.map(|s| s.get_node_properties(&[l])))
                    .filter(|props| props.len() > 1)
            }
        } else {
            None
        };

        let is_node = node_properties.is_some();

        if is_node {
            let node_alias = match value {
                LogicalExpr::TableAlias(ta) => ta,
                _ => unreachable!(),
            };

            let properties = node_properties.unwrap();

            for (cypher_prop, db_col) in &properties {
                let mut prop_access = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(node_alias.0.clone()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        db_col.clone(),
                    ),
                });
                // Apply scope rewriting for CTE-backed references
                if let Some(s) = scope {
                    prop_access = super::variable_scope::rewrite_render_expr(&prop_access, s);
                }
                let group_array = RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "groupArray".to_string(),
                    args: vec![prop_access],
                });
                let head_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "arrayElement".to_string(),
                    args: vec![group_array, RenderExpr::Literal(Literal::Integer(1))],
                });

                let col_name = format!("{}_{}", key, cypher_prop);
                let full_alias = format!("{}_{}", alias, col_name);
                // Record compound key: "msg.id" → "latestLike_msg_id"
                let compound_key = format!("{}.{}", key, cypher_prop);
                compound_keys.push((compound_key, full_alias.clone()));
                flattened_items.push(SelectItem {
                    expression: head_expr,
                    col_alias: Some(ColumnAlias(full_alias)),
                });
            }
        } else {
            // Scalar value — convert to RenderExpr and wrap in arrayElement(groupArray(...), 1)
            let render_value: Option<RenderExpr> = value.clone().try_into().ok();
            if let Some(mut val_expr) = render_value {
                // Rewrite CTE-backed references
                if let Some(s) = scope {
                    val_expr = super::variable_scope::rewrite_render_expr(&val_expr, s);
                }
                let group_array = RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "groupArray".to_string(),
                    args: vec![val_expr],
                });
                let head_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "arrayElement".to_string(),
                    args: vec![group_array, RenderExpr::Literal(Literal::Integer(1))],
                });

                let full_alias = format!("{}_{}", alias, key);
                flattened_items.push(SelectItem {
                    expression: head_expr,
                    col_alias: Some(ColumnAlias(full_alias)),
                });
            }
        }
    }

    if flattened_items.is_empty() {
        return None;
    }

    Some((flattened_items, compound_keys))
}

pub(crate) fn build_chained_with_match_cte_plan(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&PlanCtx>,
    scope: Option<&super::variable_scope::VariableScope>,
) -> RenderPlanBuilderResult<RenderPlan> {
    use super::CteContent;

    log::debug!(
        "build_chained_with_match_cte_plan ENTRY: plan_ctx available: {}",
        plan_ctx.is_some()
    );
    // Safety limit to prevent infinite loops due to excessive plan tree depth
    // Complex queries with many nested structures (projections, filters, WITH clauses, etc.)
    // can create deep plan trees that require many iterations to process
    const MAX_PLAN_DEPTH: usize = 500;

    let mut current_plan = plan.clone();
    let mut all_ctes: Vec<Cte> = Vec::new();
    let mut iteration = 0;

    // Collect compound key mappings from flattened map literals.
    // Written during CTE SELECT item generation, read during property_mapping construction.
    let flattened_compound_keys: std::cell::RefCell<Vec<(String, String)>> =
        std::cell::RefCell::new(Vec::new());

    // Track CTE schemas: map CTE name to:
    // 1. Vec<SelectItem>: Column definitions
    // 2. Vec<String>: Property names
    // 3. HashMap<String, String>: alias → ID column name
    // 4. HashMap<(String, String), String>: (alias, property) → CTE column name (EXPLICIT MAPPING)
    let mut cte_schemas: crate::render_plan::CteSchemas = std::collections::HashMap::new();

    // Track VLP CTEs with column metadata for deterministic lookups
    // Maps CTE name → (Cypher alias → column metadata)
    // This replaces heuristic lookups in expand_table_alias_to_group_by_id_only
    let mut vlp_cte_metadata: std::collections::HashMap<
        String,
        (String, Vec<super::CteColumnMetadata>), // (from_alias, columns)
    > = std::collections::HashMap::new();

    // Track aliases that have been converted to CTEs across ALL iterations
    // This prevents re-processing the same alias in subsequent iterations
    // (important for chained WITH like `WITH DISTINCT fof WITH fof`)
    let mut processed_cte_aliases: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // Track sequence numbers for each alias to generate unique CTE names
    // Maps alias → next sequence number (e.g., "a" → 3 means next CTE is with_a_cte_3)
    let mut cte_sequence_numbers: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // Track CTE names we've already emitted to prevent duplicates
    let mut used_cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Track CTE references as we build them (alias → CTE name)
    // Start EMPTY and populate as each CTE is created
    // This ensures we only reference CTEs that have actually been built in previous iterations
    let mut cte_references: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // Track CTE name remapping for passthrough WITHs
    // When analyzer generates multiple CTE names for the same alias chain (e.g., with_name_cte_1, with_name_cte_2),
    // but we skip creating duplicate CTEs, we need to remap the phantom names to the actual name.
    // Maps: analyzer_cte_name → actual_cte_name (e.g., "with_name_cte_2" → "with_name_cte_1")
    let mut cte_name_remapping: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // CRITICAL: Extract correlation predicates from the ORIGINAL plan BEFORE any transformations!
    // These predicates (e.g., a.user_id = c.user_id from WHERE clause in cross-table WITH patterns)
    // are stored in CartesianProduct.join_condition and will be lost after the plan is transformed.
    // We need them later to create proper JOIN ON conditions for CTE joins.
    let original_correlation_predicates = extract_correlation_predicates(&current_plan);
    log::debug!(
        "🔧 build_chained_with_match_cte_plan: Extracted {} correlation predicates from ORIGINAL plan",
        original_correlation_predicates.len()
    );
    for (i, pred) in original_correlation_predicates.iter().enumerate() {
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Original correlation predicate[{}]: {:?}",
            i,
            pred
        );
    }

    log::debug!("🔧 build_chained_with_match_cte_plan: Starting iterative WITH processing");

    // Accumulate CTE variable info for scope-aware resolution.
    // As each WITH is processed, we record the alias → CTE property mapping.
    // This is used to build a VariableScope for rendering subsequent CTE bodies and the final plan.
    let mut scope_cte_variables: HashMap<String, super::variable_scope::CteVariableInfo> =
        HashMap::new();

    // Unified variable registry: tracks all visible variables with their types, sources,
    // and property mappings. Updated as each CTE is built. Attached to CTEs and the final
    // RenderPlan for use by the SQL renderer.
    let mut var_registry = crate::query_planner::typed_variable::VariableRegistry::new();

    fn show_plan_structure(plan: &LogicalPlan, indent: usize) {
        let prefix = "  ".repeat(indent);
        match plan {
            LogicalPlan::WithClause(wc) => {
                let key = if !wc.exported_aliases.is_empty() {
                    let mut aliases = wc.exported_aliases.clone();
                    aliases.sort();
                    aliases.join("_")
                } else {
                    "?".to_string()
                };
                log::debug!("{}WITH[{}]", prefix, key,);
                show_plan_structure(&wc.input, indent + 1);
            }
            LogicalPlan::Projection(proj) => {
                log::debug!("{}Proj({})", prefix, proj.items.len());
                show_plan_structure(&proj.input, indent + 1);
            }
            LogicalPlan::GraphJoins(gj) => {
                log::debug!("{}GJoins({})", prefix, gj.joins.len());
                show_plan_structure(&gj.input, indent + 1);
            }
            LogicalPlan::Filter(f) => {
                log::debug!("{}Filter", prefix);
                show_plan_structure(&f.input, indent + 1);
            }
            LogicalPlan::Limit(l) => {
                log::debug!("{}Limit({})", prefix, l.count);
                show_plan_structure(&l.input, indent + 1);
            }
            LogicalPlan::ViewScan(vs) => {
                log::debug!("{}VS('{}')", prefix, vs.source_table);
            }
            LogicalPlan::GraphNode(gn) => {
                log::debug!("{}GN('{}')", prefix, gn.alias);
            }
            LogicalPlan::Union(u) => {
                log::debug!("{}Union({}br)", prefix, u.inputs.len());
                for (i, input) in u.inputs.iter().enumerate() {
                    log::debug!("{}  br{}:", prefix, i);
                    show_plan_structure(input, indent + 2);
                }
            }
            LogicalPlan::GraphRel(gr) => {
                log::debug!(
                    "{}GR({}->{}, {:?})",
                    prefix,
                    gr.left_connection,
                    gr.right_connection,
                    gr.labels.as_ref().map(|l| l.join(",")).unwrap_or_default()
                );
                show_plan_structure(&gr.left, indent + 1);
                show_plan_structure(&gr.right, indent + 1);
            }
            LogicalPlan::CartesianProduct(cp) => {
                log::debug!("{}CP", prefix);
                show_plan_structure(&cp.left, indent + 1);
                show_plan_structure(&cp.right, indent + 1);
            }
            LogicalPlan::GroupBy(_) => {
                log::debug!("{}GroupBy", prefix);
            }
            LogicalPlan::OrderBy(ob) => {
                log::debug!("{}OrderBy", prefix);
                show_plan_structure(&ob.input, indent + 1);
            }
            LogicalPlan::Skip(s) => {
                log::debug!("{}Skip({})", prefix, s.count);
                show_plan_structure(&s.input, indent + 1);
            }
            LogicalPlan::Unwind(u) => {
                log::debug!("{}Unwind('{}')", prefix, u.alias);
                show_plan_structure(&u.input, indent + 1);
            }
            other => {
                log::debug!("{}{:?}", prefix, std::mem::discriminant(other));
            }
        }
    }

    // Count plan tree depth to diagnose excessive iterations.
    // Deep nesting can come from any combination of plan nodes (Projection, Filter, WITH, etc.)
    fn count_plan_depth(plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::WithClause(wc) => 1 + count_plan_depth(&wc.input),
            LogicalPlan::Projection(p) => 1 + count_plan_depth(&p.input),
            LogicalPlan::Filter(f) => 1 + count_plan_depth(&f.input),
            LogicalPlan::GroupBy(gb) => 1 + count_plan_depth(&gb.input),
            LogicalPlan::OrderBy(ob) => 1 + count_plan_depth(&ob.input),
            LogicalPlan::Limit(lim) => 1 + count_plan_depth(&lim.input),
            LogicalPlan::Skip(skip) => 1 + count_plan_depth(&skip.input),
            LogicalPlan::GraphJoins(gj) => 1 + count_plan_depth(&gj.input),
            LogicalPlan::Unwind(u) => 1 + count_plan_depth(&u.input),
            LogicalPlan::Union(u) => {
                1 + u
                    .inputs
                    .iter()
                    .map(|i| count_plan_depth(i))
                    .max()
                    .unwrap_or(0)
            }
            _ => 1, // Leaf nodes
        }
    }

    // Process WITH clauses iteratively until none remain
    while has_with_clause_in_graph_rel(&current_plan) {
        log::debug!("🔧 build_chained_with_match_cte_plan: has_with_clause_in_graph_rel(&current_plan) = true, entering loop");
        iteration += 1;
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: ========== ITERATION {} ==========",
            iteration
        );

        let plan_depth = count_plan_depth(&current_plan);
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Plan tree depth = {} (iteration {})",
            plan_depth,
            iteration
        );

        if iteration > MAX_PLAN_DEPTH {
            log::debug!("🔧 build_chained_with_match_cte_plan: HIT PLAN DEPTH LIMIT! Current plan structure:");
            show_plan_structure(&current_plan, 0);
            return Err(RenderBuildError::InvalidRenderPlan(format!(
                "Query plan too deeply nested (depth > {}). This usually indicates a bug in query planning.",
                MAX_PLAN_DEPTH
            )));
        }

        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Iteration {} - processing WITH clause",
            iteration
        );

        // Find ALL WITH clauses grouped by alias
        // This handles Union branches that each have their own WITH clause with the same alias
        // Note: We collect the data without holding references across the mutation
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: About to call find_all_with_clauses_grouped"
        );
        let grouped_withs = find_all_with_clauses_grouped(&current_plan);

        log::debug!("🔧 build_chained_with_match_cte_plan: Found {} alias groups from find_all_with_clauses_grouped", grouped_withs.len());
        for (alias, plans) in &grouped_withs {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan:   Alias '{}': {} plan(s)",
                alias,
                plans.len()
            );
            for (i, plan) in plans.iter().enumerate() {
                if let LogicalPlan::WithClause(wc) = plan {
                    log::debug!(
                        "🔧     Plan {}: WithClause with exported_aliases={:?}, items.len()={}",
                        i,
                        wc.exported_aliases,
                        wc.items.len()
                    );
                    let has_nested = plan_contains_with_clause(&wc.input);
                    log::debug!("🔧     Plan {}: has_nested_with_clause={}", i, has_nested);
                }
            }
        }

        if grouped_withs.is_empty() {
            log::debug!("🔧 build_chained_with_match_cte_plan: has_with_clause_in_graph_rel returned true but no WITH clauses found");
            break;
        }

        // CRITICAL: Collect ALL analyzer CTE names from ALL WITH clauses in the plan tree
        // This includes nested WITHs that will be collapsed later. We need to record
        // the analyzer's CTE names now so we can remap them after collapsing.
        fn collect_analyzer_cte_names(
            plan: &LogicalPlan,
            names: &mut std::collections::HashSet<String>,
        ) {
            match plan {
                LogicalPlan::WithClause(wc) => {
                    for cte_name in wc.cte_references.values() {
                        names.insert(cte_name.clone());
                    }
                    collect_analyzer_cte_names(&wc.input, names);
                }
                LogicalPlan::Projection(proj) => collect_analyzer_cte_names(&proj.input, names),
                LogicalPlan::Filter(f) => collect_analyzer_cte_names(&f.input, names),
                LogicalPlan::GroupBy(gb) => collect_analyzer_cte_names(&gb.input, names),
                LogicalPlan::OrderBy(ob) => collect_analyzer_cte_names(&ob.input, names),
                LogicalPlan::Limit(lim) => collect_analyzer_cte_names(&lim.input, names),
                LogicalPlan::Skip(skip) => collect_analyzer_cte_names(&skip.input, names),
                LogicalPlan::Union(u) => {
                    for input in &u.inputs {
                        collect_analyzer_cte_names(input, names);
                    }
                }
                _ => {}
            }
        }

        let mut all_analyzer_cte_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        collect_analyzer_cte_names(&current_plan, &mut all_analyzer_cte_names);
        log::info!(
            "🔧 build_chained_with_match_cte_plan: Collected {} analyzer CTE names: {:?}",
            all_analyzer_cte_names.len(),
            all_analyzer_cte_names
        );

        // CRITICAL FIX: For aliases with multiple WITH clauses (nested consecutive WITH with same alias),
        // we should only process the INNERMOST one per iteration. The others will be processed
        // in subsequent iterations after the inner one is converted to a CTE.
        //
        // Filter strategy: For each alias, only keep the WITH clause whose input has NO nested WITH clauses.
        // This is the "innermost" WITH that should be processed first.
        let mut filtered_grouped_withs: std::collections::HashMap<String, Vec<LogicalPlan>> =
            std::collections::HashMap::new();

        // Also track the original analyzer CTE name for each innermost WithClause
        let mut original_analyzer_cte_names: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for (alias, plans) in grouped_withs {
            // NOTE: We do NOT skip aliases that were processed in previous iterations.
            // Multiple WITH clauses can share the same alias key (e.g., two consecutive
            // "WITH DISTINCT country, a, b" barriers). Each must be processed as a separate CTE.
            // The innermost filtering below handles ordering: only WITHs whose input has
            // no nested WITH clauses are processed in each iteration.

            // Record original count before filtering
            let original_count = plans.len();

            // Find plans that are innermost (no nested WITH in their input)
            let innermost_plans: Vec<LogicalPlan> = plans
                .into_iter()
                .filter(|plan| {
                    if let LogicalPlan::WithClause(wc) = plan {
                        let has_nested = plan_contains_with_clause(&wc.input);
                        if has_nested {
                            log::debug!("🔧 build_chained_with_match_cte_plan: Skipping WITH '{}' with nested WITH clauses (will process in next iteration). Input plan type: {:?}", alias, std::mem::discriminant(wc.input.as_ref()));
                            // Show what's inside this WITH's input tree
                            show_plan_structure(&wc.input, 0);
                        } else {
                            log::debug!("🔧 build_chained_with_match_cte_plan: Keeping innermost WITH '{}' for processing", alias);
                            // Capture the original analyzer CTE name for this innermost WithClause
                            if let Some(analyzer_cte_name) = wc.cte_references.get(&alias) {
                                original_analyzer_cte_names.insert(alias.clone(), analyzer_cte_name.clone());
                                log::debug!("🔧 build_chained_with_match_cte_plan: Captured original analyzer CTE name '{}' for alias '{}'", analyzer_cte_name, alias);
                            } else {
                                log::debug!("🔧 build_chained_with_match_cte_plan: No analyzer CTE name found for innermost WITH '{}'", alias);
                            }
                        }
                        !has_nested
                    } else {
                        log::debug!("🔧 build_chained_with_match_cte_plan: Plan for alias '{}' is not WithClause: {:?}", alias, std::mem::discriminant(plan));
                        true  // Not a WithClause, keep it
                    }
                })
                .collect();

            if !innermost_plans.is_empty() {
                log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}': filtered {} plan(s) to {} innermost",
                           alias, original_count, innermost_plans.len());
                filtered_grouped_withs.insert(alias, innermost_plans);
            } else {
                log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}': NO innermost plans after filtering {} total",
                           alias, original_count);
            }
        }

        // DEBUG: Log the contents of original_analyzer_cte_names right after population
        log::debug!(
            "🔧 DEBUG: original_analyzer_cte_names after innermost filtering: {:?}",
            original_analyzer_cte_names
        );

        // Collect alias info for processing (to avoid holding references across mutation)
        let mut aliases_to_process: Vec<(String, usize)> = filtered_grouped_withs
            .iter()
            .map(|(alias, plans)| (alias.clone(), plans.len()))
            .collect();

        // Sort aliases to process innermost first (simpler names = fewer underscores = more inner)
        // This ensures "friend" is processed before "friend_post"
        aliases_to_process.sort_by(|a, b| {
            let a_depth = a.0.matches('_').count();
            let b_depth = b.0.matches('_').count();
            a_depth.cmp(&b_depth)
        });
        log::info!(
            "🔧 build_chained_with_match_cte_plan: Sorted aliases: {:?}",
            aliases_to_process
                .iter()
                .map(|(a, _)| a)
                .collect::<Vec<_>>()
        );

        // Track if any alias was actually processed in this iteration
        let mut any_processed_this_iteration = false;

        // Process each alias group
        // For aliases with multiple WITH clauses (from Union branches), combine them with UNION ALL
        'alias_loop: for (with_alias, plan_count) in aliases_to_process {
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Processing {} WITH clause(s) for alias '{}'",
                plan_count,
                with_alias
            );

            // CRITICAL: Create a snapshot of cte_references that only includes CTEs from PREVIOUS iterations
            // Do NOT include the CTE we're about to build for this alias!
            // This prevents resolve_cte_reference from using future CTEs that don't exist yet
            let mut cte_references_for_rendering = cte_references.clone();
            log::info!(
                "🔧 build_chained_with_match_cte_plan: cte_references for rendering '{}': {:?}",
                with_alias,
                cte_references_for_rendering
            );

            // Get the WITH plans from our filtered map
            let with_plans = match filtered_grouped_withs.get(&with_alias) {
                Some(plans) => {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Found {} plan(s) for alias '{}' in filtered map",
                        plans.len(),
                        with_alias
                    );
                    plans.clone() // Clone the Vec<LogicalPlan> to avoid moving from borrowed data
                }
                None => {
                    log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}' not in filtered map (all WITH clauses had nested WITH), skipping", with_alias);
                    continue;
                }
            };

            // CRITICAL: Update cte_references for ALL plans BEFORE rendering them
            // GraphRel nodes inside these plans need to know about available CTEs
            // Use the snapshot from PREVIOUS iterations only (not including current alias)
            log::debug!("🔧 build_chained_with_match_cte_plan: Updating cte_references for {} plans before rendering. Using previous CTEs: {:?}", with_plans.len(), cte_references_for_rendering);

            let with_plans: Vec<LogicalPlan> = with_plans
                .into_iter()
                .map(|plan| {
                    update_graph_joins_cte_refs(
                        &plan,
                        &cte_references_for_rendering,
                        &std::collections::HashMap::new(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Collect aliases from the pre-WITH scope (inside the WITH clauses)
            // These aliases should be filtered out from the outer query's joins
            let mut pre_with_aliases = std::collections::HashSet::new();
            for with_plan in with_plans.iter() {
                // For Projection(With), the input contains the pre-WITH pattern
                if let LogicalPlan::Projection(proj) = with_plan {
                    let inner_aliases = collect_aliases_from_plan(&proj.input);
                    pre_with_aliases.extend(inner_aliases);
                }
            }
            // Don't filter out the WITH variable itself - it's the boundary variable
            pre_with_aliases.remove(&with_alias);
            // Don't filter out aliases that are already CTEs (processed in earlier iterations)
            // These are now references to CTEs, not original tables
            for cte_alias in &processed_cte_aliases {
                if pre_with_aliases.remove(cte_alias) {
                    log::debug!("🔧 build_chained_with_match_cte_plan: Keeping '{}' (already a CTE reference)", cte_alias);
                }
            }
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Pre-WITH aliases to filter: {:?}",
                pre_with_aliases
            );

            /// Check if a plan is a CTE reference (ViewScan or GraphNode wrapping ViewScan with table starting with "with_")
            fn is_cte_reference(plan: &LogicalPlan) -> Option<String> {
                match plan {
                    LogicalPlan::ViewScan(vs) if vs.source_table.starts_with("with_") => {
                        Some(vs.source_table.clone())
                    }
                    LogicalPlan::GraphNode(gn) => {
                        if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                            if vs.source_table.starts_with("with_") {
                                return Some(vs.source_table.clone());
                            }
                        }
                        None
                    }
                    _ => None,
                }
            }

            // Render each WITH clause plan
            let mut rendered_plans: Vec<RenderPlan> = Vec::new();
            let mut inner_plans_for_id: Vec<LogicalPlan> = Vec::new();
            let mut has_optional_match_input = false;
            for with_plan in with_plans.iter() {
                log::debug!("🔧 build_chained_with_match_cte_plan: Rendering WITH plan for '{}' - plan type: {:?}",
                           with_alias, std::mem::discriminant(with_plan));

                // Check if this is a passthrough WITH whose input is already a CTE reference
                // E.g., `WITH fof` after `WITH DISTINCT fof` - the second WITH just passes through
                // Skip creating another CTE and use the existing one
                if let LogicalPlan::WithClause(wc) = with_plan {
                    if let Some(existing_cte) = is_cte_reference(&wc.input) {
                        // Check if this is a simple passthrough (same alias, no modifications)
                        let is_simple_passthrough = wc.items.len() == 1
                            && wc.order_by.is_none()
                            && wc.skip.is_none()
                            && wc.limit.is_none()
                            && !wc.distinct
                            && wc.where_clause.is_none()  // CRITICAL: WHERE clause makes it not a passthrough!
                            && matches!(
                                &wc.items[0].expression,
                                crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
                            );

                        log::debug!("🔧 build_chained_with_match_cte_plan: Checking passthrough: items={}, order_by={}, skip={}, limit={}, distinct={}, where_clause={}, is_table_alias={}, is_passthrough={}",
                                   wc.items.len(), wc.order_by.is_some(), wc.skip.is_some(), wc.limit.is_some(), wc.distinct,
                                   wc.where_clause.is_some(),
                                   matches!(&wc.items[0].expression, crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)),
                                   is_simple_passthrough);

                        if is_simple_passthrough {
                            log::debug!("TEST: This should show up");
                            log::debug!(
                                "🔧 DEBUG: ENTERING passthrough collapse for '{}'",
                                with_alias
                            );

                            // CRITICAL FIX: For passthrough WITHs, we need to collapse them too!
                            // They wrap an existing CTE reference and should be removed.
                            // For passthrough, use empty string to indicate passthrough collapse
                            let target_cte = "".to_string();
                            log::debug!(
                                "🔧 build_chained_with_match_cte_plan: Collapsing passthrough WITH for '{}' with CTE '{}'",
                                with_alias, target_cte
                            );
                            current_plan =
                                collapse_passthrough_with(&current_plan, &with_alias, &target_cte)?;
                            log::debug!(
                                "🔧 build_chained_with_match_cte_plan: After passthrough collapse, plan discriminant: {:?}",
                                std::mem::discriminant(&current_plan)
                            );

                            // CRITICAL FIX: Update cte_references to map the skipped WITH's aliases
                            // to the actual CTE name. This ensures the final SELECT uses the correct CTE.
                            //
                            // Problem: Analyzer generates unique CTE names for each WITH clause
                            //   (e.g., with_name_cte_1, with_name_cte_2), but when passthrough WITHs
                            //   are skipped, the outer expressions still reference the skipped WITH's CTE name.
                            //
                            // Solution: Map all exported aliases of the skipped WITH to the existing CTE.
                            // ALSO: Extract the analyzer's CTE name for this WITH to collapse it properly.
                            for alias in &wc.exported_aliases {
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Mapping skipped alias '{}' → existing CTE '{}'",
                                    alias, existing_cte
                                );
                                cte_references.insert(alias.clone(), existing_cte.clone());

                                // Also record CTE name remapping: analyzer's CTE name → actual CTE name
                                // The analyzer assigned a unique CTE name to this WITH, but we're skipping it.
                                // We need to remap expressions that reference the analyzer's name.
                                log::debug!(
                                    "🔧 DEBUG: wc.cte_references = {:?}, looking for alias '{}'",
                                    wc.cte_references,
                                    alias
                                );
                                if let Some(analyzer_cte_name) = wc.cte_references.get(alias) {
                                    log::debug!(
                                        "🔧 DEBUG: Found analyzer_cte_name '{}', existing_cte = '{}'",
                                        analyzer_cte_name, existing_cte
                                    );
                                    if analyzer_cte_name != &existing_cte {
                                        log::info!(
                                            "🔧 build_chained_with_match_cte_plan: Recording CTE name remap: '{}' → '{}'",
                                            analyzer_cte_name, existing_cte
                                        );
                                        cte_name_remapping.insert(
                                            analyzer_cte_name.clone(),
                                            existing_cte.clone(),
                                        );
                                    }
                                }
                            }

                            // Mark that we processed something (collapsing passthrough is processing)
                            any_processed_this_iteration = true;

                            // CRITICAL: Break out of BOTH loops to restart iteration.
                            // We modified current_plan, so we need to re-run find_all_with_clauses_grouped.
                            // Using a labeled break to exit the outer for loop too.
                            break 'alias_loop;
                        }
                    }
                }

                // Extract the plan to render, WITH items, and modifiers (ORDER BY, SKIP, LIMIT, WHERE)
                // CRITICAL: Also extract CTE references from this WITH's input - these tell us which
                // variables come from previous CTEs in the chain
                let (
                    plan_to_render,
                    with_items,
                    with_distinct,
                    with_order_by,
                    with_skip,
                    with_limit,
                    with_where_clause,
                    _with_cte_refs,
                ) = match with_plan {
                    LogicalPlan::WithClause(wc) => {
                        log::debug!("� DEBUG: Unwrapping WithClause for alias '{}'", with_alias);
                        log::debug!("🐛 DEBUG: WithClause has {} items", wc.items.len());
                        for (i, item) in wc.items.iter().enumerate() {
                            log::debug!("🐛 DEBUG: wc.items[{}]: {:?}", i, item);
                        }
                        log::debug!("�🔧 build_chained_with_match_cte_plan: Unwrapping WithClause, rendering input");

                        // Use CTE references from this WithClause (populated by analyzer)
                        let input_cte_refs = wc.cte_references.clone();
                        log::info!(
                            "🔧 build_chained_with_match_cte_plan: CTE refs from WithClause: {:?}",
                            input_cte_refs
                        );
                        // Debug: if it's GraphJoins, log the joins
                        if let LogicalPlan::GraphJoins(gj) = wc.input.as_ref() {
                            log::debug!("🔧 build_chained_with_match_cte_plan: wc.input is GraphJoins with {} joins", gj.joins.len());
                            for (i, join) in gj.joins.iter().enumerate() {
                                log::debug!("🔧 build_chained_with_match_cte_plan: GraphJoins join {}: table_name={}, table_alias={}, joining_on={:?}",
                                    i, join.table_name.as_str(), join.table_alias.as_str(), join.joining_on);
                            }
                        }
                        (
                            wc.input.as_ref(),
                            Some(wc.items.clone()),
                            wc.distinct,
                            wc.order_by.clone(),
                            wc.skip,
                            wc.limit,
                            wc.where_clause.clone(),
                            input_cte_refs,
                        )
                    }
                    LogicalPlan::Projection(proj) => {
                        log::debug!("🔧 build_chained_with_match_cte_plan: WITH projection input type: {:?}",
                                   std::mem::discriminant(proj.input.as_ref()));
                        // Check if input contains CTE reference
                        if let LogicalPlan::Filter(filter) = proj.input.as_ref() {
                            log::info!(
                                "🔧 build_chained_with_match_cte_plan: Filter input type: {:?}",
                                std::mem::discriminant(filter.input.as_ref())
                            );
                        }
                        (
                            with_plan as &LogicalPlan,
                            None,
                            false,
                            None,
                            None,
                            None,
                            None,
                            std::collections::HashMap::new(),
                        )
                    }
                    _ => (
                        with_plan as &LogicalPlan,
                        None,
                        false,
                        None,
                        None,
                        None,
                        None,
                        std::collections::HashMap::new(),
                    ),
                };

                // Save plan_to_render for ID column computation (used after loop)
                inner_plans_for_id.push(plan_to_render.clone());

                // Track whether this WITH clause's input contains an OPTIONAL MATCH.
                // This is used later for deterministic CTE body restructuring.
                if plan_to_render.is_optional_pattern() {
                    has_optional_match_input = true;
                }

                // Render the plan (even if it contains nested WITHs)
                // Instead of calling to_render_plan recursively (which causes infinite loops),
                // process the plan directly using the same logic as the main function
                //
                // Build a scope from accumulated CTE variables for this rendering pass.
                // This ensures CTE body rendering resolves variables from prior WITHs correctly.
                let body_scope = super::variable_scope::VariableScope::with_cte_variables(
                    schema,
                    plan_to_render,
                    scope_cte_variables.clone(),
                );
                let body_scope_ref = if scope_cte_variables.is_empty() && scope.is_none() {
                    None // No scope needed for first WITH (or when called without outer scope)
                } else {
                    Some(&body_scope)
                };
                let mut rendered = if has_with_clause_in_graph_rel(plan_to_render) {
                    // The plan has nested WITH clauses - process them using our own logic
                    log::debug!("🔧 build_chained_with_match_cte_plan: Plan has nested WITH clauses, processing recursively with our own logic");
                    build_chained_with_match_cte_plan(
                        plan_to_render,
                        schema,
                        plan_ctx,
                        body_scope_ref,
                    )?
                } else {
                    // No nested WITH clauses - render directly
                    log::debug!("🔧 build_chained_with_match_cte_plan: Plan has no nested WITH clauses, rendering directly with plan_ctx");
                    plan_to_render.to_render_plan_with_ctx(schema, plan_ctx, body_scope_ref)?
                };

                // CRITICAL: Extract CTE schemas from nested rendering
                // When rendering nested WITHs, the recursive call builds CTEs that we need
                // to reference. Extract their schemas and add to our cte_schemas map.
                if !rendered.ctes.0.is_empty() {
                    for cte in &rendered.ctes.0 {
                        let select_items = match &cte.content {
                            super::CteContent::Structured(plan) => match &plan.union {
                                UnionItems(Some(union)) if !union.input.is_empty() => {
                                    union.input[0].select.items.clone()
                                }
                                _ => plan.select.items.clone(),
                            },
                            super::CteContent::RawSql(_) => {
                                // VLP CTEs are RawSql - can't extract schema directly
                                // But we can infer from the UNION that uses them
                                // Skip for now, will be handled when we see the UNION
                                log::debug!("🔧 Skipping RawSql CTE '{}' (VLP CTE - schema will be inferred from UNION)", cte.cte_name);
                                continue;
                            }
                        };
                        let property_names: Vec<String> = select_items
                            .iter()
                            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                            .collect();

                        // For nested CTEs, we don't have direct access to the plan to compute ID columns
                        // deterministically. These are typically VLP CTEs with dotted notation (friend.id)
                        // which we can safely extract since they follow a fixed pattern from VLP generation.
                        let mut alias_to_id_column: HashMap<String, String> = HashMap::new();
                        for item in &select_items {
                            if let Some(col_alias) = &item.col_alias {
                                let alias_str = col_alias.0.as_str();
                                // VLP CTEs use "alias.id" pattern which is unambiguous
                                if let Some(dot_pos) = alias_str.rfind('.') {
                                    let (prefix, suffix) = alias_str.split_at(dot_pos);
                                    if suffix == ".id" {
                                        alias_to_id_column
                                            .insert(prefix.to_string(), alias_str.to_string());
                                        log::debug!(
                                            "📊 CTE '{}': Found ID column for alias '{}' -> '{}'",
                                            cte.cte_name,
                                            prefix,
                                            alias_str
                                        );
                                    }
                                }
                                // Note: We do NOT try to parse underscore patterns here as they are unreliable
                                // The caller (build_chained_with_match_cte_plan) will compute these deterministically
                            }
                        }

                        // Build explicit property mapping
                        let property_mapping = build_property_mapping_from_columns(&select_items);

                        log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Extracted nested CTE schema '{}': {} columns, {} aliases with ID, {} property mappings",
                                    cte.cte_name, property_names.len(), alias_to_id_column.len(), property_mapping.len()
                                );

                        cte_schemas.insert(
                            cte.cte_name.clone(),
                            crate::render_plan::CteSchemaMetadata {
                                select_items,
                                column_names: property_names,
                                alias_to_id: alias_to_id_column,
                                property_mapping,
                            },
                        );
                    }

                    // CRITICAL FIX (Jan 2026): Hoist CTEs from recursive call to prevent duplicates
                    // The recursive call created CTEs - we need to:
                    // 1. Add them to our all_ctes (so they appear in final SQL)
                    // 2. Track their names in used_cte_names (so we don't create duplicates)
                    // 3. Track their aliases in processed_cte_aliases (so we don't re-process them)
                    // 4. Capture VLP column metadata for deterministic lookups (Phase 3 CTE integration)
                    for cte in &rendered.ctes.0 {
                        log::debug!(
                            "🔧 build_chained_with_match_cte_plan: Hoisting CTE '{}' from recursive call",
                            cte.cte_name
                        );
                        used_cte_names.insert(cte.cte_name.clone());

                        // Capture VLP CTE metadata for deterministic column lookups
                        // This replaces heuristic lookups in expand_table_alias_to_group_by_id_only
                        if !cte.columns.is_empty() && cte.from_alias.is_some() {
                            let from_alias = cte.from_alias.clone().unwrap();
                            log::info!(
                                "🔧 Capturing VLP CTE metadata: '{}' with {} columns, from_alias='{}'",
                                cte.cte_name, cte.columns.len(), from_alias
                            );
                            vlp_cte_metadata
                                .insert(cte.cte_name.clone(), (from_alias, cte.columns.clone()));
                        }

                        // Extract aliases from the CTE's stored exported_aliases (preferred)
                        // or from CTE name (fallback, may fail for aliases with underscores)
                        let aliases = if !cte.with_exported_aliases.is_empty() {
                            cte.with_exported_aliases.clone()
                        } else if let Some(extracted) =
                            crate::utils::cte_naming::extract_aliases_from_cte_name(&cte.cte_name)
                        {
                            extracted
                        } else {
                            Vec::new()
                        };
                        for alias in aliases {
                            if !alias.is_empty() {
                                processed_cte_aliases.insert(alias.clone());
                                cte_references.insert(alias, cte.cte_name.clone());
                            }
                        }
                    }
                    // Now hoist the actual CTEs
                    hoist_nested_ctes(&mut rendered, &mut all_ctes);
                }

                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Rendered SQL FROM: {:?}",
                    rendered.from
                );
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Rendered SQL JOINs: {} join(s)",
                    rendered.joins.0.len()
                );
                for (i, join) in rendered.joins.0.iter().enumerate() {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: JOIN {}: {:?}",
                        i,
                        join
                    );
                }

                // CRITICAL: Extract schema from UNION (for VLP CTEs)
                // VLP CTEs are RawSql so we can't extract schema from them directly
                // But the UNION that uses them has SELECT items with aliases like "friend.id", "p.firstName"
                if let UnionItems(Some(union)) = &rendered.union {
                    if !union.input.is_empty() {
                        let union_select_items = &union.input[0].select.items;
                        let union_property_names: Vec<String> = union_select_items
                            .iter()
                            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                            .collect();

                        // Extract ID column mappings from UNION columns
                        // Store FULL column name (e.g., "friend.id"), not just "id"
                        let mut union_alias_to_id: HashMap<String, String> = HashMap::new();
                        for item in union_select_items {
                            if let Some(col_alias) = &item.col_alias {
                                let alias_str = col_alias.0.as_str();
                                if let Some(dot_pos) = alias_str.rfind('.') {
                                    let (prefix, suffix) = alias_str.split_at(dot_pos);
                                    if suffix == ".id" {
                                        // Store FULL column name
                                        union_alias_to_id
                                            .insert(prefix.to_string(), alias_str.to_string());
                                        log::debug!(
                                            "📊 UNION: Found ID column for alias '{}' -> '{}'",
                                            prefix,
                                            alias_str
                                        );
                                    }
                                }
                            }
                        }

                        // Build explicit property mapping for UNION (VLP results)
                        let union_property_mapping =
                            build_property_mapping_from_columns(union_select_items);

                        // Register the UNION schema as a pseudo-CTE for alias lookups
                        // This allows WITH clauses to reference VLP results
                        let union_cte_name = "__union_vlp";
                        log::info!(
                                    "🔧 Extracted UNION schema (VLP results): {} columns, {} aliases with ID: {:?}, {} property mappings",
                                    union_property_names.len(), union_alias_to_id.len(), union_alias_to_id.keys(), union_property_mapping.len()
                                );
                        cte_schemas.insert(
                            union_cte_name.to_string(),
                            crate::render_plan::CteSchemaMetadata {
                                select_items: union_select_items.clone(),
                                column_names: union_property_names,
                                alias_to_id: union_alias_to_id.clone(),
                                property_mapping: union_property_mapping,
                            },
                        );

                        // Also register for each alias that appears in the UNION
                        // This allows direct alias lookups
                        for alias in union_alias_to_id.keys() {
                            cte_references_for_rendering
                                .insert(alias.clone(), union_cte_name.to_string());
                            log::info!(
                                "🔧 Registered alias '{}' -> CTE '{}'",
                                alias,
                                union_cte_name
                            );
                        }
                    }
                }

                // 🔧 FIX (Feb 9, 2026): Pattern comprehension GROUP BY property bug
                // When FROM clause is a VLP CTE (e.g., FROM vlp_multi_type_a_t31 AS t),
                // and WITH items reference correlation variables (e.g., WITH a),
                // register the correlation variable → VLP CTE mapping so that
                // expand_table_alias_to_select_items generates t.start_* columns
                // instead of trying to reference the non-existent alias 'a'
                log::debug!(
                    "DEBUG 0: About to check FROM clause, has from_ref: {}",
                    rendered.from.0.is_some()
                );
                log::debug!("DEBUG: Checking FROM clause for VLP CTE");
                if let Some(from_ref) = &rendered.from.0 {
                    let from_name = &from_ref.name;
                    log::debug!("DEBUG: FROM name = '{}'", from_name);
                    // Check if FROM is a VLP CTE (starts with "vlp_")
                    if from_name.starts_with("vlp_") {
                        log::debug!("DEBUG: FROM is VLP CTE!");
                        log::info!("🔧 FROM is VLP CTE '{}', checking for correlation variables in WITH items", from_name);

                        // Extract correlation variables from VLP CTE name
                        // Actual format: vlp_multi_type_a_b (multiple aliases, no _t suffix)
                        // or: vlp_a (single alias)
                        log::debug!(
                            "STEP 0.1: Extracting correlation variables from '{}'",
                            from_name
                        );
                        let aliases = if from_name.starts_with("vlp_multi_type_") {
                            // Multi-type VLP: vlp_multi_type_a_b -> ["a", "b"]
                            from_name
                                .strip_prefix("vlp_multi_type_")
                                .map(|s| s.split('_').map(|a| a.to_string()).collect::<Vec<_>>())
                                .unwrap_or_default()
                        } else {
                            // Single-type VLP: vlp_a -> ["a"]
                            from_name
                                .strip_prefix("vlp_")
                                .map(|s| vec![s.to_string()])
                                .unwrap_or_default()
                        };
                        log::debug!("STEP 0.2: Extracted aliases: {:?}", aliases);

                        // Register each alias → VLP CTE mapping
                        for corr_var in &aliases {
                            log::info!("🔧 VLP CTE correlation variable: '{}'", corr_var);
                            // Register: correlation_var → VLP CTE name
                            // This tells expand_table_alias_to_select_items to use VLP CTE columns
                            cte_references_for_rendering
                                .insert(corr_var.to_string(), from_name.clone());
                            log::info!(
                                "🔧 Registered VLP correlation: '{}' → '{}'",
                                corr_var,
                                from_name
                            );
                        }

                        // Populate cte_schemas from VLP CTE metadata (CRITICAL for GROUP BY property mapping)
                        // This ensures expand_table_alias_to_select_items finds the CTE schema
                        if !aliases.is_empty() {
                            if let Some((_cypher_alias, col_metadata)) =
                                vlp_cte_metadata.get(from_name)
                            {
                                log::debug!(
                                    "STEP 1: VLP CTE '{}' found, {} columns",
                                    from_name,
                                    col_metadata.len()
                                );

                                // Convert CteColumnMetadata to SelectItem format
                                let mut select_items = Vec::new();
                                let mut property_names = Vec::new();
                                let mut alias_to_id_column: HashMap<String, String> =
                                    HashMap::new();

                                log::debug!("STEP 2: Starting column iteration");
                                for (idx, col_meta) in col_metadata.iter().enumerate() {
                                    log::debug!(
                                        "STEP 2.{}: Processing column '{}'",
                                        idx,
                                        col_meta.cte_column_name
                                    );

                                    // The CTE already exists with these columns - we just need to track them
                                    // The expression here is not used for rendering, only for metadata
                                    let col_expr = crate::render_plan::render_expr::RenderExpr::Raw(
                                        col_meta.cte_column_name.clone(),
                                    );
                                    log::debug!("STEP 2.{}.1: Created RenderExpr", idx);

                                    let select_item = SelectItem {
                                        expression: col_expr,
                                        col_alias: Some(
                                            crate::render_plan::render_expr::ColumnAlias(
                                                col_meta.cte_column_name.clone(),
                                            ),
                                        ),
                                    };
                                    log::debug!("STEP 2.{}.2: Created SelectItem", idx);

                                    select_items.push(select_item);
                                    property_names.push(col_meta.cte_column_name.clone());
                                    log::debug!("STEP 2.{}.3: Pushed to vectors", idx);

                                    // Track ID columns for GROUP BY
                                    if col_meta.is_id_column {
                                        alias_to_id_column.insert(
                                            col_meta.cypher_alias.clone(),
                                            col_meta.cte_column_name.clone(),
                                        );
                                        log::debug!("STEP 2.{}.4: Tracked ID column", idx);
                                    }
                                }

                                log::debug!(
                                    "STEP 3: Column iteration complete, building property mapping"
                                );
                                // Build property mapping using existing function
                                let property_mapping =
                                    build_property_mapping_from_columns(&select_items);
                                log::debug!(
                                    "STEP 4: Property mapping built with {} entries",
                                    property_mapping.len()
                                );

                                log::debug!("STEP 5: Inserting into cte_schemas");
                                // Insert into cte_schemas so expand_table_alias_to_select_items can find it
                                cte_schemas.insert(
                                    from_name.clone(),
                                    crate::render_plan::CteSchemaMetadata {
                                        select_items,
                                        column_names: property_names,
                                        alias_to_id: alias_to_id_column,
                                        property_mapping,
                                    },
                                );
                                log::debug!(
                                    "STEP 6: SUCCESS - Schema populated for '{}'",
                                    from_name
                                );
                            } else {
                                log::debug!(
                                    "⚠️ VLP CTE '{}' not found in vlp_cte_metadata",
                                    from_name
                                );
                            }
                        }
                    }
                }

                // Extract pattern comprehension aliases to skip in WITH item projection
                // (their results come from CTE LEFT JOINs, not from regular WITH item processing)
                // NOTE: Only skip items for LEGACY CTE+JOIN path. For the new correlated subquery
                // path (pattern_hops populated), keep items so count(*) can be replaced inline.
                let (pc_result_aliases, pc_correlated_aliases): (
                    std::collections::HashSet<String>,
                    std::collections::HashSet<String>,
                ) = with_plans
                    .first()
                    .and_then(|plan| match plan {
                        LogicalPlan::WithClause(wc) if !wc.pattern_comprehensions.is_empty() => {
                            // If any PC has pattern_hops, use correlated subquery path → don't skip
                            let has_pattern_hops = wc
                                .pattern_comprehensions
                                .iter()
                                .any(|pc| !pc.pattern_hops.is_empty());
                            if has_pattern_hops {
                                // Correlated subquery path: collect aliases that contain count(*)
                                // placeholders — these will be replaced with scalar subqueries,
                                // so they should NOT trigger has_aggregation.
                                let correlated: std::collections::HashSet<String> = wc
                                    .pattern_comprehensions
                                    .iter()
                                    .map(|pc| pc.result_alias.clone())
                                    .collect();
                                Some((std::collections::HashSet::new(), correlated))
                            } else {
                                let legacy: std::collections::HashSet<String> = wc
                                    .pattern_comprehensions
                                    .iter()
                                    .map(|pc| pc.result_alias.clone())
                                    .collect();
                                Some((legacy, std::collections::HashSet::new()))
                            }
                        }
                        _ => None,
                    })
                    .unwrap_or_default();

                // Apply WITH items projection if present
                // This handles cases like `WITH friend.firstName AS name` or `WITH count(friend) AS cnt`
                // CRITICAL: Also apply for TableAlias items (WITH a) to standardize CTE column names
                if let Some(ref items) = with_items {
                    log::debug!("🐛 DEBUG: with_items is Some, has {} items", items.len());
                    for (i, item) in items.iter().enumerate() {
                        log::debug!("🐛 DEBUG: with_item[{}]: {:?}", i, item);
                    }

                    // Filter out pattern comprehension items — their results come from CTE LEFT JOINs
                    let items: &Vec<_> = items;
                    let items_filtered: Vec<_> = if pc_result_aliases.is_empty() {
                        items.clone()
                    } else {
                        items
                            .iter()
                            .filter(|item| {
                                let alias_str = item
                                    .col_alias
                                    .as_ref()
                                    .map(|a| a.0.clone())
                                    .unwrap_or_default();
                                if pc_result_aliases.contains(&alias_str) {
                                    log::info!(
                                        "🔧 Filtering out pattern comp WITH item '{}'",
                                        alias_str
                                    );
                                    false
                                } else {
                                    true
                                }
                            })
                            .cloned()
                            .collect()
                    };
                    let _items = &items_filtered;
                    let items = &items_filtered;

                    let needs_projection = items.iter().any(|item| {
                        !matches!(
                            &item.expression,
                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
                        )
                    });

                    let has_aggregation = items.iter().any(|item| {
                        // Skip items that are pattern comprehension correlated subquery
                        // placeholders (count(*)) — they will be replaced with scalar
                        // subqueries and should not trigger aggregate mode.
                        if let Some(ref alias) = item.col_alias {
                            if pc_correlated_aliases.contains(&alias.0) {
                                return false;
                            }
                        }
                        /// Recursively check if an expression contains an aggregate function
                        fn contains_aggregate(
                            expr: &crate::query_planner::logical_expr::LogicalExpr,
                        ) -> bool {
                            use crate::query_planner::logical_expr::LogicalExpr;
                            match expr {
                                LogicalExpr::AggregateFnCall(_) => true,
                                LogicalExpr::ScalarFnCall(f) => {
                                    f.args.iter().any(contains_aggregate)
                                }
                                LogicalExpr::Operator(op)
                                | LogicalExpr::OperatorApplicationExp(op) => {
                                    op.operands.iter().any(contains_aggregate)
                                }
                                LogicalExpr::Case(c) => {
                                    c.when_then.iter().any(|(cond, val)| {
                                        contains_aggregate(cond) || contains_aggregate(val)
                                    }) || c
                                        .else_expr
                                        .as_ref()
                                        .map_or(false, |e| contains_aggregate(e))
                                }
                                LogicalExpr::List(items) => items.iter().any(contains_aggregate),
                                LogicalExpr::ArraySubscript { array, index } => {
                                    contains_aggregate(array) || contains_aggregate(index)
                                }
                                _ => false,
                            }
                        }
                        contains_aggregate(&item.expression)
                    });

                    let has_table_alias = items.iter().any(|item| {
                        matches!(
                            &item.expression,
                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
                        )
                    });

                    log::debug!(
                        "🐛 DEBUG: needs_projection={}, has_aggregation={}, has_table_alias={}",
                        needs_projection,
                        has_aggregation,
                        has_table_alias
                    );

                    // Apply projection if we have non-TableAlias items, aggregations, OR TableAlias items
                    // TableAlias items need projection to generate CTE columns with simple names
                    if needs_projection || has_aggregation || has_table_alias {
                        log::debug!("🔧 build_chained_with_match_cte_plan: Applying WITH items projection (needs_projection={}, has_aggregation={}, has_table_alias={})",
                                           needs_projection, has_aggregation, has_table_alias);

                        // Convert LogicalExpr items to RenderExpr SelectItems
                        // CRITICAL: Expand TableAlias to ALL columns (not just ID)
                        // When WITH friend appears, it means "all properties of friend"
                        //
                        // Performance optimization: Wrap non-ID columns with ANY() when aggregating
                        // This allows GROUP BY to only include ID column (more efficient)

                        // Extract UNWIND alias from plan if present — UNWIND aliases are simple
                        // ARRAY JOIN column references, not table aliases to expand.
                        // Must recurse through wrapping nodes (Filter, Projection, etc.)
                        // since UNWIND may not be at the top level.
                        fn find_unwind_alias(plan: &LogicalPlan) -> Option<&str> {
                            match plan {
                                LogicalPlan::Unwind(u) => Some(u.alias.as_str()),
                                LogicalPlan::Filter(f) => find_unwind_alias(&f.input),
                                LogicalPlan::Projection(p) => find_unwind_alias(&p.input),
                                LogicalPlan::OrderBy(ob) => find_unwind_alias(&ob.input),
                                LogicalPlan::Limit(lim) => find_unwind_alias(&lim.input),
                                LogicalPlan::Skip(s) => find_unwind_alias(&s.input),
                                LogicalPlan::GroupBy(gb) => find_unwind_alias(&gb.input),
                                _ => None,
                            }
                        }
                        let unwind_alias = find_unwind_alias(plan_to_render);

                        let select_items: Vec<SelectItem> = items.iter()
                                    .flat_map(|item| {
                                        // Check if this is a TableAlias that needs expansion to ALL columns
                                        match &item.expression {
                                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                                // UNWIND aliases are ARRAY JOIN columns — emit a simple column reference
                                                if unwind_alias == Some(alias.0.as_str()) {
                                                    log::debug!("🔧 build_chained_with_match_cte_plan: UNWIND alias '{}' — simple column reference", alias.0);
                                                    return vec![SelectItem {
                                                        expression: super::render_expr::RenderExpr::ColumnAlias(
                                                            super::render_expr::ColumnAlias(alias.0.clone()),
                                                        ),
                                                        col_alias: Some(ColumnAlias(alias.0.clone())),
                                                    }];
                                                }

                                                // Use unified expansion helper (Dec 2025)
                                                // CRITICAL: Use cte_references_for_rendering (includes ALL previous CTEs),
                                                // NOT with_cte_refs (only includes CTEs visible in this WITH's immediate input)
                                                // This allows "WITH a, b, c" to find "a" and "b" from previous CTEs
                                                //
                                                // The unified helper automatically handles anyLast() wrapping when has_aggregation=true
                                                let expanded = expand_table_alias_to_select_items(
                                                    &alias.0,
                                                    plan_to_render,
                                                    &cte_schemas,
                                                    &cte_references_for_rendering,
                                                    has_aggregation,  // Enables anyLast() wrapping in unified function
                                                    plan_ctx,  // Pass Option<&PlanCtx> for property pruning
                                                    Some(&vlp_cte_metadata)  // Pass VLP CTE metadata for FROM alias lookup
                                                );
                                                log::debug!("🔧 build_chained_with_match_cte_plan: Expanded alias '{}' to {} items (aggregation={})",
                                                           alias.0, expanded.len(), has_aggregation);

                                                expanded
                                            }
                                            _ => {
                                                // Not a TableAlias, convert normally
                                                // First, check if we need to rewrite path functions
                                                // For variable-length paths, convert length(path) → hop_count, etc.
                                                let logical_expr = if let Some(path_var_name) = get_path_variable(plan_to_render) {
                                                    // Rewrite path functions in the logical expression BEFORE converting to RenderExpr
                                                    rewrite_logical_path_functions(&item.expression, path_var_name.as_str())
                                                } else {
                                                    item.expression.clone()
                                                };

                                                // 🔧 CRITICAL FIX: Apply property mapping for WITH expressions
                                                // Maps Cypher property names (e.g., u.name) to DB columns (e.g., full_name)
                                                // This is the same rewriting that RETURN clause does
                                                // SCOPE: Use body_scope_ref to resolve CTE-scoped variables
                                                // (e.g., post.creationDate → CTE column after a prior WITH)
                                                use crate::query_planner::logical_expr::expression_rewriter::{
                                                    ExpressionRewriteContext, rewrite_expression_with_property_mapping,
                                                };
                                                let rewrite_ctx = if let Some(s) = body_scope_ref {
                                                    ExpressionRewriteContext::with_scope(plan_to_render, s)
                                                } else {
                                                    ExpressionRewriteContext::new(plan_to_render)
                                                };
                                                let rewritten_expr = rewrite_expression_with_property_mapping(&logical_expr, &rewrite_ctx);
                                                log::info!(
                                                    "🔧 build_chained_with_match_cte_plan: Rewrote WITH expression with property mapping"
                                                );

                                                // CRITICAL: Expand collect(node) to groupArray(tuple(...)) BEFORE converting to RenderExpr
                                                // This must happen in WITH context too, not just in extract_select_items()
                                                let expanded_expr = if let crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(ref agg) = rewritten_expr {
                                                    if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                                                        if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                                                            log::debug!("🔧 WITH context: Expanding collect({}) to groupArray(tuple(...))", alias.0);

                                                            // Extract property requirements for pruning
                                                            let property_requirements = plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                                                            // Get all properties for this alias
                                                            match plan_to_render.get_properties_with_table_alias(&alias.0) {
                                                                Ok((props, _actual_alias)) if !props.is_empty() => {
                                                                    log::debug!("🔧 Found {} properties for alias '{}', expanding", props.len(), alias.0);

                                                                    // For collect(node), only collect the ID property to produce groupArray(id).
                                                                    // Semantically, collect(node) gathers node identities, and groupArray(id)
                                                                    // is compatible with downstream IN/has() checks (Array(T) vs scalar T).
                                                                    // groupArray(tuple(...)) would produce Array(Tuple) which fails has() type checks.
                                                                    let id_only_props: Vec<_> = props.iter()
                                                                        .filter(|(prop_name, _)| prop_name == "id")
                                                                        .cloned()
                                                                        .collect();
                                                                    let collect_props = if id_only_props.is_empty() {
                                                                        log::debug!("🔧 collect({}): no 'id' property found, using all {} properties", alias.0, props.len());
                                                                        props
                                                                    } else {
                                                                        log::debug!("🔧 collect({}): using ID-only for groupArray (compatible with IN/has())", alias.0);
                                                                        id_only_props
                                                                    };

                                                                    // Use centralized expansion utility with property requirements
                                                                    use crate::render_plan::property_expansion::expand_collect_to_group_array;
                                                                    expand_collect_to_group_array(&alias.0, collect_props, property_requirements)
                                                                }
                                                                _ => {
                                                                    log::warn!("⚠️  Could not expand collect({}) in WITH - no properties found, keeping as-is", alias.0);
                                                                    rewritten_expr
                                                                }
                                                            }
                                                        } else {
                                                            rewritten_expr
                                                        }
                                                    } else {
                                                        rewritten_expr
                                                    }
                                                } else {
                                                    rewritten_expr
                                                };

                                                // 🔧 FIX: Flatten head(collect(MapLiteral)) with node values
                                                // ClickHouse map() requires homogeneous value types, but nodes
                                                // have no single value. Expand each map entry to separate CTE columns.
                                                log::info!("🔧 Checking for head(collect(MapLiteral)) flattening, alias={:?}, expanded_expr={:?}",
                                                    item.col_alias.as_ref().map(|a| &a.0),
                                                    std::mem::discriminant(&expanded_expr));
                                                if let Some((flattened_items, compound_keys)) = try_flatten_head_collect_map_literal(
                                                    &expanded_expr,
                                                    item.col_alias.as_ref().map(|a| a.0.as_str()),
                                                    plan_to_render,
                                                    plan_ctx,
                                                    body_scope_ref,
                                                ) {
                                                    log::info!("🔧 Flattened head(collect(MapLiteral)) into {} columns with {} compound keys",
                                                        flattened_items.len(), compound_keys.len());
                                                    flattened_compound_keys.borrow_mut().extend(compound_keys);
                                                    return flattened_items;
                                                }

                                                let expr_result: Result<RenderExpr, _> = expanded_expr.try_into();
                                                expr_result.ok().map(|mut expr| {
                                                    // 🔧 FIX: VLP CTE column rewriting for non-TableAlias WITH items
                                                    // When FROM is a VLP/multi-type CTE, PropertyAccess references
                                                    // (e.g., message.content) must be rewritten to CTE columns
                                                    // (e.g., t.start_content)
                                                    if let Some(from_ref) = &rendered.from.0 {
                                                        if from_ref.name.starts_with("vlp_") {
                                                            let from_alias = from_ref.alias.as_deref().unwrap_or("t");
                                                            // Build mappings: cypher_alias → "start_node" or "end_node"
                                                            if let Some((_from_alias_meta, col_metadata)) = vlp_cte_metadata.get(&from_ref.name) {
                                                                let mut mappings: HashMap<String, String> = HashMap::new();
                                                                for col_meta in col_metadata {
                                                                    if !mappings.contains_key(&col_meta.cypher_alias) {
                                                                        if let Some(pos) = &col_meta.vlp_position {
                                                                            let internal_alias = match pos {
                                                                                super::cte_manager::VlpColumnPosition::Start => "start_node".to_string(),
                                                                                super::cte_manager::VlpColumnPosition::End => "end_node".to_string(),
                                                                            };
                                                                            mappings.insert(col_meta.cypher_alias.clone(), internal_alias);
                                                                        }
                                                                    }
                                                                }
                                                                if !mappings.is_empty() {
                                                                    log::debug!("🔧 VLP WITH item rewrite: mappings={:?}, from_alias={}", mappings, from_alias);
                                                                    rewrite_render_expr_for_vlp_with_from_alias(&mut expr, &mappings, from_alias);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    SelectItem {
                                                        expression: expr,
                                                        col_alias: item.col_alias.as_ref().map(|a| crate::render_plan::render_expr::ColumnAlias(a.0.clone())),
                                                    }
                                                }).into_iter().collect()
                                            }
                                        }
                                    })
                                    .collect();

                        log::debug!("🔧 build_chained_with_match_cte_plan: Total select_items after expansion: {}", select_items.len());

                        if !select_items.is_empty() {
                            // Check if the logical plan has a denormalized Union.
                            // Denormalized Unions already have per-branch SELECT items with
                            // correct column resolution (origin_code vs dest_code). We must NOT
                            // overwrite them with a flat projection from one branch only.
                            // Instead, rename aliases in each branch: "code" → "a_code".
                            fn plan_has_denormalized_union(plan: &LogicalPlan) -> bool {
                                match plan {
                                    LogicalPlan::Union(u) => u.inputs.iter().any(|input| {
                                        fn has_denorm_vs(p: &LogicalPlan) -> bool {
                                            match p {
                                                LogicalPlan::ViewScan(vs) => vs.is_denormalized,
                                                LogicalPlan::GraphNode(gn) => {
                                                    has_denorm_vs(gn.input.as_ref())
                                                }
                                                LogicalPlan::Filter(f) => {
                                                    has_denorm_vs(f.input.as_ref())
                                                }
                                                LogicalPlan::Projection(p) => {
                                                    has_denorm_vs(p.input.as_ref())
                                                }
                                                _ => false,
                                            }
                                        }
                                        has_denorm_vs(input.as_ref())
                                    }),
                                    LogicalPlan::Filter(f) => {
                                        plan_has_denormalized_union(f.input.as_ref())
                                    }
                                    LogicalPlan::GraphNode(gn) => {
                                        plan_has_denormalized_union(gn.input.as_ref())
                                    }
                                    LogicalPlan::Projection(p) => {
                                        plan_has_denormalized_union(p.input.as_ref())
                                    }
                                    _ => false,
                                }
                            }
                            let is_denorm_union = plan_has_denormalized_union(plan_to_render)
                                && rendered.union.0.is_some();

                            if is_denorm_union {
                                // Denormalized Union: the RenderPlan stores first branch in
                                // (select, from, filters) and remaining branches in union.input[].
                                // For CTE content, we need ALL branches in a flat UNION DISTINCT.
                                // Move first branch into union.input and clear plan-level fields.
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Denormalized Union detected, restructuring for WITH '{}'",
                                    with_alias
                                );

                                // Use the first exported alias (the node alias) for column renaming,
                                // not the combined with_alias. This ensures columns like "code" become
                                // "a_code" (not "a_allNeighboursCount_code") for unambiguous parsing.
                                let rename_alias = with_plans
                                    .first()
                                    .and_then(|p| match p {
                                        LogicalPlan::WithClause(wc) => {
                                            wc.exported_aliases.first().cloned()
                                        }
                                        _ => None,
                                    })
                                    .unwrap_or_else(|| with_alias.clone());

                                fn rename_branch_aliases(select: &mut SelectItems, alias: &str) {
                                    use crate::utils::cte_column_naming::cte_column_name;
                                    for item in &mut select.items {
                                        if let Some(ref mut col_alias) = item.col_alias {
                                            if col_alias.0 == "__label__" {
                                                continue;
                                            }
                                            let new_name = cte_column_name(alias, &col_alias.0);
                                            col_alias.0 = new_name;
                                        }
                                    }
                                    select.distinct = true;
                                    // Sort by alias to ensure consistent column order across
                                    // UNION branches (SQL UNION maps by position, not name)
                                    select.items.sort_by(|a, b| {
                                        let a_alias = a
                                            .col_alias
                                            .as_ref()
                                            .map(|c| c.0.as_str())
                                            .unwrap_or("");
                                        let b_alias = b
                                            .col_alias
                                            .as_ref()
                                            .map(|c| c.0.as_str())
                                            .unwrap_or("");
                                        a_alias.cmp(b_alias)
                                    });
                                }

                                // Build first branch RenderPlan from the parent plan's fields
                                let mut first_branch = RenderPlan {
                                    ctes: CteItems(vec![]),
                                    select: rendered.select.clone(),
                                    from: rendered.from.clone(),
                                    joins: rendered.joins.clone(),
                                    array_join: ArrayJoinItem(Vec::new()),
                                    filters: rendered.filters.clone(),
                                    group_by: GroupByExpressions(vec![]),
                                    having_clause: None,
                                    order_by: OrderByItems(vec![]),
                                    skip: SkipItem(None),
                                    limit: LimitItem(None),
                                    union: UnionItems(None),
                                    fixed_path_info: None,
                                    is_multi_label_scan: false,
                                    variable_registry: None,
                                };
                                rename_branch_aliases(&mut first_branch.select, &rename_alias);

                                // Rename aliases in remaining branches
                                if let UnionItems(Some(ref mut union)) = rendered.union {
                                    for branch in &mut union.input {
                                        rename_branch_aliases(&mut branch.select, &rename_alias);
                                    }
                                    // Insert first branch at the beginning
                                    union.input.insert(0, first_branch);
                                }

                                // Clear plan-level fields so CTE renders union directly
                                rendered.select = SelectItems {
                                    items: vec![],
                                    distinct: false,
                                };
                                rendered.from = FromTableItem(None);
                                rendered.filters = FilterItems(None);
                                rendered.joins = JoinItems(vec![]);
                            } else {
                                // For UNION plans, we need to apply projection over the union
                                // We do this by keeping the UNION structure but replacing SELECT items
                                // The union branches already have all columns, so we wrap with our projection
                                // This creates: SELECT <with_items> FROM (SELECT * FROM table1 UNION ALL SELECT * FROM table2) AS __union

                                // For both UNION and non-UNION: apply projection to SELECT
                                rendered.select = SelectItems {
                                    items: select_items,
                                    distinct: with_distinct,
                                };
                            } // end is_denorm_union else

                            // If there's aggregation, add GROUP BY for non-aggregate expressions
                            // PERFORMANCE: Only GROUP BY the ID column(s) for TableAlias items
                            // (non-ID columns are wrapped with ANY() above, so they don't need to be grouped)
                            //
                            // This is efficient because:
                            // 1. node_id is the primary key (unique identifier)
                            // 2. ANY() picks the single value in each group (safe for PK)
                            // 3. GROUP BY 1 column is much faster than GROUP BY 7 columns
                            if has_aggregation {
                                /// Check if a LogicalExpr is a constant literal (no need to GROUP BY)
                                fn is_literal_expr(
                                    expr: &crate::query_planner::logical_expr::LogicalExpr,
                                ) -> bool {
                                    matches!(
                                        expr,
                                        crate::query_planner::logical_expr::LogicalExpr::Literal(_)
                                    )
                                }

                                /// Check if a LogicalExpr contains an aggregate function (recursively)
                                fn contains_aggregate(
                                    expr: &crate::query_planner::logical_expr::LogicalExpr,
                                ) -> bool {
                                    use crate::query_planner::logical_expr::LogicalExpr;
                                    match expr {
                                        LogicalExpr::AggregateFnCall(_) => true,
                                        LogicalExpr::OperatorApplicationExp(op) => {
                                            op.operands.iter().any(contains_aggregate)
                                        }
                                        LogicalExpr::ScalarFnCall(f) => {
                                            f.args.iter().any(contains_aggregate)
                                        }
                                        LogicalExpr::ArraySubscript { array, index } => {
                                            contains_aggregate(array) || contains_aggregate(index)
                                        }
                                        _ => false,
                                    }
                                }

                                let group_by_exprs: Vec<RenderExpr> = items.iter()
                                            .filter(|item| {
                                                // Exclude: direct aggregates, literals, and expressions containing aggregates
                                                !matches!(&item.expression, crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_))
                                                && !is_literal_expr(&item.expression)
                                                && !contains_aggregate(&item.expression)
                                            })
                                            .flat_map(|item| {
                                                // For TableAlias, only GROUP BY the ID column
                                                // (other columns are wrapped with ANY() in SELECT)
                                                match &item.expression {
                                                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                                        // Use ID-only helper for efficient GROUP BY
                                                        // Pass VLP CTE metadata for deterministic lookups
                                                        expand_table_alias_to_group_by_id_only(
                                                            &alias.0,
                                                            plan_to_render,
                                                            schema,
                                                            &cte_schemas,
                                                            &cte_references_for_rendering,
                                                            Some(&vlp_cte_metadata),
                                                        )
                                                    }
                                                    crate::query_planner::logical_expr::LogicalExpr::ArraySubscript { array, .. } => {
                                                        // For array subscripts (e.g., labels(x)[1]), only GROUP BY the array part
                                                        // ClickHouse can't GROUP BY an array element, only the array itself
                                                        let expr_vec: Vec<RenderExpr> = (**array).clone().try_into().ok().into_iter().collect();
                                                        expr_vec
                                                    }
                                                    _ => {
                                                        // Apply property mapping rewriting before converting to RenderExpr.
                                                        // This ensures CTE-scoped columns resolve correctly (e.g., message.length → p7_message_length).
                                                        use crate::query_planner::logical_expr::expression_rewriter::{
                                                            ExpressionRewriteContext, rewrite_expression_with_property_mapping,
                                                        };
                                                        let rewrite_ctx = if let Some(s) = body_scope_ref {
                                                            ExpressionRewriteContext::with_scope(plan_to_render, s)
                                                        } else {
                                                            ExpressionRewriteContext::new(plan_to_render)
                                                        };
                                                        let rewritten = rewrite_expression_with_property_mapping(&item.expression, &rewrite_ctx);
                                                        let expr_vec: Vec<RenderExpr> = rewritten.try_into().ok().into_iter().collect();
                                                        expr_vec
                                                    }
                                                }
                                            })
                                            .collect();
                                rendered.group_by = GroupByExpressions(group_by_exprs);
                            }
                        }
                    }
                }

                // Build scope-aware rewrite context for ORDER BY and WHERE/HAVING
                // from WithClause. This maps Cypher property names to CTE column names.
                use crate::query_planner::logical_expr::expression_rewriter::{
                    rewrite_expression_with_property_mapping, ExpressionRewriteContext,
                };
                let with_rewrite_ctx = if let Some(s) = body_scope_ref {
                    ExpressionRewriteContext::with_scope(plan_to_render, s)
                } else {
                    ExpressionRewriteContext::new(plan_to_render)
                };

                // Apply WithClause's ORDER BY, SKIP, LIMIT to the rendered plan
                if let Some(order_by_items) = with_order_by {
                    log::debug!(
                        "🔧 build_chained_with_match_cte_plan: Applying ORDER BY from WithClause"
                    );
                    let has_cte_scope = body_scope_ref.is_some();
                    let render_order_by: Vec<OrderByItem> = order_by_items
                        .iter()
                        .filter_map(|item| {
                            let rewritten = rewrite_expression_with_property_mapping(
                                &item.expression,
                                &with_rewrite_ctx,
                            );
                            let expr_result: Result<RenderExpr, _> = rewritten.try_into();
                            expr_result.ok().map(|expr| {
                                // Strip table aliases only when CTE scope was used.
                                // CTE scope resolves to CTE names as table aliases,
                                // which need stripping for bare output column references
                                // (especially after GROUP BY over UNION subqueries).
                                // Without scope (first WITH), keep original table aliases
                                // since they reference actual FROM/JOIN tables.
                                let final_expr = if has_cte_scope {
                                    strip_table_alias_from_resolved(&expr)
                                } else {
                                    expr
                                };
                                OrderByItem {
                                    expression: final_expr,
                                    order: match item.order {
                                        crate::query_planner::logical_plan::OrderByOrder::Asc => {
                                            OrderByOrder::Asc
                                        }
                                        crate::query_planner::logical_plan::OrderByOrder::Desc => {
                                            OrderByOrder::Desc
                                        }
                                    },
                                }
                            })
                        })
                        .collect();
                    rendered.order_by = OrderByItems(render_order_by);
                }
                if let Some(skip_count) = with_skip {
                    log::debug!(
                        "🔧 build_chained_with_match_cte_plan: Applying SKIP {} from WithClause",
                        skip_count
                    );
                    rendered.skip = SkipItem(Some(skip_count as i64));
                }
                if let Some(limit_count) = with_limit {
                    log::debug!(
                        "🔧 build_chained_with_match_cte_plan: Applying LIMIT {} from WithClause",
                        limit_count
                    );
                    rendered.limit = LimitItem(Some(limit_count as i64));
                }

                // Apply WHERE clause from WITH - becomes HAVING if we have GROUP BY
                if let Some(where_predicate) = with_where_clause {
                    log::debug!(
                        "🔧 build_chained_with_match_cte_plan: Applying WHERE clause from WITH"
                    );

                    // Rewrite through scope to map Cypher properties to CTE columns
                    let where_rewritten = rewrite_expression_with_property_mapping(
                        &where_predicate,
                        &with_rewrite_ctx,
                    );
                    let where_render_expr: RenderExpr = where_rewritten.try_into()?;

                    if !rendered.group_by.0.is_empty() {
                        // We have GROUP BY - WHERE becomes HAVING
                        log::debug!("🔧 build_chained_with_match_cte_plan: Converting WHERE to HAVING (GROUP BY present)");
                        rendered.having_clause = Some(where_render_expr);
                    } else {
                        // No GROUP BY - apply as regular WHERE filter
                        log::debug!("🔧 build_chained_with_match_cte_plan: Applying WHERE as filter predicate");

                        // Combine with existing filters
                        let new_filter = if let Some(existing_filter) = rendered.filters.0.take() {
                            // AND the new filter with existing
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing_filter, where_render_expr],
                            })
                        } else {
                            where_render_expr
                        };
                        rendered.filters = FilterItems(Some(new_filter));
                    }
                }

                // Rewrite join conditions that reference CTE aliases to use CTE column names.
                // The analyzer generates joins with base-table columns (e.g., friend.id),
                // but after a WITH barrier, "friend" is a CTE with prefixed columns (e.g., p6_friend_id).
                if !cte_references.is_empty() {
                    rewrite_join_conditions_for_cte_aliases(
                        &mut rendered,
                        &cte_references,
                        &cte_schemas,
                    );

                    // Remove orphaned JOINs: when a CTE-backed node appears as a graph
                    // endpoint, a base-table JOIN is created. After CTE rewriting, the
                    // ON condition may no longer reference the joined table (all refs
                    // rewritten to CTE columns). Remove such JOINs, folding conditions
                    // into the previous JOIN.
                    {
                        use super::expression_utils::references_alias;
                        let mut orphaned_indices: Vec<usize> = Vec::new();
                        for (i, join) in rendered.joins.0.iter().enumerate() {
                            // Only consider JOINs for CTE-backed aliases
                            if !cte_references.contains_key(&join.table_alias) {
                                continue;
                            }
                            // Skip CTE/VLP table JOINs — only remove base table JOINs
                            if join.table_name.starts_with("with_")
                                || join.table_name.starts_with("vlp_")
                            {
                                continue;
                            }
                            // Skip if JOIN has pre_filter or no conditions
                            if join.pre_filter.is_some() || join.joining_on.is_empty() {
                                continue;
                            }
                            let alias = &join.table_alias;

                            // Only remove if no MEANINGFUL non-CTE-backed JOINs follow.
                            // A trailing JOIN is "meaningful" if it's referenced in
                            // SELECT/WHERE/ORDER BY (i.e., it's not itself orphaned).
                            // This prevents removing mid-chain JOINs that downstream
                            // restructuring code (complex-5 countIf) depends on.
                            let has_meaningful_non_cte_after =
                                rendered.joins.0[i + 1..].iter().any(|j| {
                                    if cte_references.contains_key(&j.table_alias)
                                        || j.table_name.starts_with("with_")
                                        || j.table_name.starts_with("vlp_")
                                    {
                                        return false; // CTE/VLP JOINs don't block
                                    }
                                    let ja = &j.table_alias;
                                    rendered
                                        .select
                                        .items
                                        .iter()
                                        .any(|item| references_alias(&item.expression, ja))
                                        || matches!(
                                            &rendered.filters,
                                            FilterItems(Some(ref f)) if references_alias(f, ja)
                                        )
                                        || rendered
                                            .order_by
                                            .0
                                            .iter()
                                            .any(|item| references_alias(&item.expression, ja))
                                        || rendered
                                            .group_by
                                            .0
                                            .iter()
                                            .any(|item| references_alias(item, ja))
                                });
                            if has_meaningful_non_cte_after {
                                continue;
                            }

                            // Check if ON condition still references the joined table
                            let references_self = join.joining_on.iter().any(|cond| {
                                references_alias(
                                    &RenderExpr::OperatorApplicationExp(cond.clone()),
                                    alias,
                                )
                            });
                            if references_self {
                                continue;
                            }

                            // Check if alias is referenced ANYWHERE else in the query
                            let used_in_select = rendered
                                .select
                                .items
                                .iter()
                                .any(|item| references_alias(&item.expression, alias));
                            let used_in_filter = matches!(
                                &rendered.filters,
                                FilterItems(Some(ref f)) if references_alias(f, alias)
                            );
                            let used_in_order = rendered
                                .order_by
                                .0
                                .iter()
                                .any(|item| references_alias(&item.expression, alias));
                            let used_in_group_by = rendered
                                .group_by
                                .0
                                .iter()
                                .any(|item| references_alias(item, alias));
                            let used_in_having = rendered
                                .having_clause
                                .as_ref()
                                .is_some_and(|h| references_alias(h, alias));
                            let used_in_other_joins =
                                rendered.joins.0.iter().enumerate().any(|(j, jn)| {
                                    j != i
                                        && (jn.joining_on.iter().any(|c| {
                                            references_alias(
                                                &RenderExpr::OperatorApplicationExp(c.clone()),
                                                alias,
                                            )
                                        }) || jn
                                            .pre_filter
                                            .as_ref()
                                            .is_some_and(|pf| references_alias(pf, alias)))
                                });

                            if used_in_select
                                || used_in_filter
                                || used_in_order
                                || used_in_group_by
                                || used_in_having
                                || used_in_other_joins
                            {
                                log::info!(
                                    "Orphan JOIN check: keeping {} (sel={} filt={} ord={} grp={} hav={} jn={})",
                                    alias, used_in_select, used_in_filter, used_in_order,
                                    used_in_group_by, used_in_having, used_in_other_joins
                                );
                                continue;
                            }

                            log::info!(
                                "Orphan JOIN removal: removing orphaned JOIN {} (table {})",
                                alias,
                                join.table_name
                            );
                            orphaned_indices.push(i);
                        }

                        for &i in orphaned_indices.iter().rev() {
                            let removed = rendered.joins.0.remove(i);
                            // Fold conditions into previous JOIN
                            if i > 0 {
                                if let Some(prev_join) = rendered.joins.0.get_mut(i - 1) {
                                    for cond in removed.joining_on {
                                        prev_join.joining_on.push(cond);
                                    }
                                }
                            }
                        }
                    }

                    // Fix INNER→LEFT in OPTIONAL MATCH CTE bodies.
                    // When a CTE reference is LEFT JOINed (indicating OPTIONAL MATCH),
                    // any INNER JOINs after it should also be LEFT — the inference may
                    // generate INNER for endpoints (e.g., person2) that weren't in the
                    // optional_aliases set.
                    // We specifically require the CTE JOIN itself to be LEFT, not just
                    // any LEFT JOIN in the body, to avoid converting genuinely INNER JOINs
                    // in non-OPTIONAL contexts.
                    {
                        let first_left_cte_idx = rendered.joins.0.iter().position(|j| {
                            matches!(j.join_type, super::JoinType::Left)
                                && (j.table_name.starts_with("with_")
                                    || j.table_name.starts_with("vlp_"))
                        });
                        if let Some(cte_idx) = first_left_cte_idx {
                            for j in rendered.joins.0[cte_idx..].iter_mut() {
                                if matches!(j.join_type, super::JoinType::Inner) {
                                    log::info!(
                                        "OPTIONAL MATCH fix: converting INNER→LEFT for JOIN {} ({})",
                                        j.table_alias,
                                        j.table_name
                                    );
                                    j.join_type = super::JoinType::Left;
                                }
                            }
                        }
                    }

                    // Remove spurious auto-generated duplicate JOINs.
                    // When the system creates aliases like t13 and t13_1 for the same
                    // relationship table, the suffixed one (t13_1) is a duplicate.
                    // Only remove suffixed duplicates (alias_N where alias also exists),
                    // and only if the suffixed alias is NOT referenced in SELECT/WHERE/etc.
                    {
                        use super::expression_utils::references_alias;
                        let alias_set: std::collections::HashSet<String> = rendered
                            .joins
                            .0
                            .iter()
                            .map(|j| j.table_alias.clone())
                            .collect();
                        let mut dup_indices: Vec<usize> = Vec::new();
                        for (i, j) in rendered.joins.0.iter().enumerate() {
                            // Check if alias matches pattern "base_N" where "base" also exists
                            if let Some(pos) = j.table_alias.rfind('_') {
                                let base = &j.table_alias[..pos];
                                let suffix = &j.table_alias[pos + 1..];
                                if suffix.chars().all(|c| c.is_ascii_digit())
                                    && alias_set.contains(base)
                                {
                                    // Verify the suffixed alias isn't referenced anywhere
                                    let alias = &j.table_alias;
                                    let used = rendered
                                        .select
                                        .items
                                        .iter()
                                        .any(|item| references_alias(&item.expression, alias))
                                        || matches!(
                                            &rendered.filters,
                                            FilterItems(Some(ref f)) if references_alias(f, alias)
                                        )
                                        || rendered
                                            .order_by
                                            .0
                                            .iter()
                                            .any(|item| references_alias(&item.expression, alias))
                                        || rendered
                                            .group_by
                                            .0
                                            .iter()
                                            .any(|item| references_alias(item, alias))
                                        || rendered.joins.0.iter().enumerate().any(|(j2, jn)| {
                                            j2 != i
                                                && jn.joining_on.iter().any(|c| {
                                                    references_alias(
                                                        &RenderExpr::OperatorApplicationExp(
                                                            c.clone(),
                                                        ),
                                                        alias,
                                                    )
                                                })
                                        });
                                    if !used {
                                        dup_indices.push(i);
                                    }
                                }
                            }
                        }
                        if !dup_indices.is_empty() {
                            dup_indices.sort_unstable();
                            dup_indices.dedup();
                            for &i in dup_indices.iter().rev() {
                                log::info!(
                                    "Removing spurious duplicate JOIN {} ({})",
                                    rendered.joins.0[i].table_alias,
                                    rendered.joins.0[i].table_name
                                );
                                rendered.joins.0.remove(i);
                            }
                        }
                    }
                }

                // Fix composite alias references in the rendered plan.
                // CTE body plans may have expressions using composite aliases (e.g., "countWindow1_tag")
                // while the FROM/JOINs use individual aliases (e.g., "tag"). This post-processing step
                // rewrites orphaned composite alias references to match the actual FROM/JOIN aliases.
                // MUST be called AFTER all modifications to `rendered` (SELECT, GROUP BY, ORDER BY, etc.)
                //
                // Build an augmented scope that includes VLP-derived variables from all_ctes.
                // VLP CTEs have been hoisted into all_ctes by this point, so we can extract
                // variable→column mappings for bare variable rewriting (e.g., `friend` → `t.end_id`).
                let augmented_scope = {
                    let mut vars = scope_cte_variables.clone();
                    for vlp_cte in &all_ctes {
                        // Only process actual VLP CTEs (which have from_alias set).
                        // Normal WITH CTEs may have non-empty columns but no from_alias.
                        if vlp_cte.columns.is_empty() || vlp_cte.from_alias.is_none() {
                            continue;
                        }
                        let vlp_from_alias = vlp_cte.from_alias.clone().unwrap();
                        // Group columns by cypher_alias to build per-alias property mappings
                        let mut alias_props: HashMap<String, HashMap<String, String>> =
                            HashMap::new();
                        let mut alias_labels: HashMap<String, Vec<String>> = HashMap::new();
                        for col in &vlp_cte.columns {
                            if col.cypher_alias.is_empty() {
                                continue;
                            }
                            alias_props
                                .entry(col.cypher_alias.clone())
                                .or_default()
                                .insert(col.cypher_property.clone(), col.cte_column_name.clone());
                        }
                        if let Some(ref start_alias) = vlp_cte.vlp_cypher_start_alias {
                            if let Some(ref table) = vlp_cte.vlp_start_table {
                                let label = table.rsplit('.').next().unwrap_or(table);
                                alias_labels
                                    .entry(start_alias.clone())
                                    .or_default()
                                    .push(label.to_string());
                            }
                        }
                        if let Some(ref end_alias) = vlp_cte.vlp_cypher_end_alias {
                            if let Some(ref table) = vlp_cte.vlp_end_table {
                                let label = table.rsplit('.').next().unwrap_or(table);
                                alias_labels
                                    .entry(end_alias.clone())
                                    .or_default()
                                    .push(label.to_string());
                            }
                        }
                        for (alias, prop_map) in alias_props {
                            if vars.contains_key(&alias) {
                                continue; // Don't overwrite prior WITH variables
                            }
                            log::debug!(
                                "🔧 Augmenting scope with VLP variable '{}' from CTE '{}' ({} props, from_alias='{}')",
                                alias, vlp_cte.cte_name, prop_map.len(), vlp_from_alias
                            );
                            vars.insert(
                                alias.clone(),
                                super::variable_scope::CteVariableInfo {
                                    cte_name: vlp_cte.cte_name.clone(),
                                    property_mapping: prop_map,
                                    labels: alias_labels.remove(&alias).unwrap_or_default(),
                                    from_alias_override: Some(vlp_from_alias.clone()),
                                    map_keys: None,
                                },
                            );
                        }
                    }
                    // Also add composite alias entries from cte_references, but ONLY if
                    // the composite alias is actually referenced as a table prefix in the
                    // rendered plan. This avoids spurious CROSS JOINs for unreferenced CTEs.
                    // After scope_cte_variables.clear(), composite aliases from earlier WITHs
                    // are lost (only individual aliases from the current WITH are present).
                    // This allows fix_orphan_table_aliases to map composite aliases
                    // (e.g., "country_messageCount_months_zombie") to the correct FROM/JOIN alias.
                    let mut used_aliases = std::collections::HashSet::new();
                    for item in &rendered.select.items {
                        collect_aliases_from_single_render_expr(
                            &item.expression,
                            &mut used_aliases,
                        );
                    }
                    if let FilterItems(Some(ref filter)) = rendered.filters {
                        collect_aliases_from_single_render_expr(filter, &mut used_aliases);
                    }
                    for gi in &rendered.group_by.0 {
                        collect_aliases_from_single_render_expr(gi, &mut used_aliases);
                    }
                    for oi in &rendered.order_by.0 {
                        collect_aliases_from_single_render_expr(&oi.expression, &mut used_aliases);
                    }
                    if let Some(ref having) = rendered.having_clause {
                        collect_aliases_from_single_render_expr(having, &mut used_aliases);
                    }
                    for (ref_alias, ref_cte_name) in &cte_references {
                        if vars.contains_key(ref_alias) {
                            continue; // Already in scope (individual alias or VLP)
                        }
                        if !ref_cte_name.starts_with("with_") {
                            continue;
                        }
                        if !used_aliases.contains(ref_alias) {
                            continue; // Not referenced in rendered expressions
                        }
                        // Only add TRUE composite aliases (multi-alias combinations like
                        // "country_messageCount_months_zombie"). Skip individual aliases
                        // to avoid polluting the scope with stale CTE references.
                        // A composite alias has the form "alias1_alias2_..." and the CTE
                        // name is "with_{composite}_cte_{N}".
                        let expected_cte_prefix = format!("with_{}_cte_", ref_alias);
                        if !ref_cte_name.starts_with(&expected_cte_prefix) {
                            continue; // Not a composite alias for this CTE
                        }
                        // Build property mapping from the CTE's columns in all_ctes
                        let mut cte_prop_map: HashMap<String, String> = HashMap::new();
                        for cte in &all_ctes {
                            if cte.cte_name == *ref_cte_name {
                                for col in &cte.columns {
                                    if !col.cypher_property.is_empty() {
                                        cte_prop_map.insert(
                                            col.cypher_property.clone(),
                                            col.cte_column_name.clone(),
                                        );
                                    }
                                }
                                break;
                            }
                        }
                        log::debug!(
                            "🔧 Augmenting scope with composite alias '{}' → CTE '{}' ({} props)",
                            ref_alias,
                            ref_cte_name,
                            cte_prop_map.len()
                        );
                        vars.insert(
                            ref_alias.clone(),
                            super::variable_scope::CteVariableInfo {
                                cte_name: ref_cte_name.clone(),
                                property_mapping: cte_prop_map,
                                labels: vec![],
                                from_alias_override: None,
                                map_keys: None,
                            },
                        );
                    }
                    vars
                };
                let has_augmented = !augmented_scope.is_empty();
                if has_augmented || body_scope_ref.is_some() {
                    let aug_scope = super::variable_scope::VariableScope::with_cte_variables(
                        schema,
                        &current_plan,
                        augmented_scope,
                    );
                    // Order matters: rewrite_bare_variables converts bare TableAlias/ColumnAlias
                    // (e.g., "score") into PropertyAccessExp (e.g., "composite_alias.score").
                    // Then fix_orphan_table_aliases rewrites the composite alias to the actual
                    // FROM/JOIN alias (e.g., "person1.score"). Running fix_orphan first would
                    // miss expressions that rewrite_bare_variables creates later.
                    super::variable_scope::rewrite_bare_variables_in_plan(
                        &mut rendered,
                        &aug_scope,
                    );
                    super::variable_scope::fix_orphan_table_aliases(&mut rendered, &aug_scope);
                    super::variable_scope::rewrite_cte_property_columns(&mut rendered, &aug_scope);
                }

                rendered_plans.push(rendered);
            }

            if rendered_plans.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(format!(
                    "Could not render any WITH clause for alias '{}'",
                    with_alias
                )));
            }

            // Extract ALL exported aliases from the first WITH clause plan
            // Use them to generate the CTE name (not just the grouped alias)
            // This matches what the analyzer expects: with_<all_aliases>_cte_<seq>
            let exported_aliases: Vec<String> = with_plans
                .first()
                .and_then(|plan| match plan {
                    LogicalPlan::WithClause(wc) => Some(wc.exported_aliases.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| vec![with_alias.clone()]);

            // Extract pattern comprehension metadata from the WithClause
            let pattern_comprehensions: Vec<crate::query_planner::logical_plan::PatternComprehensionMeta> = with_plans
                .first()
                .and_then(|plan| match plan {
                    LogicalPlan::WithClause(wc) if !wc.pattern_comprehensions.is_empty() => {
                        log::info!(
                            "🔧 build_chained_with_match_cte_plan: Found {} pattern comprehensions for alias '{}'",
                            wc.pattern_comprehensions.len(), with_alias
                        );
                        Some(wc.pattern_comprehensions.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default();

            // Sorted aliases string used for sequence tracking and uniqueness
            let mut sorted_exported_aliases = exported_aliases.clone();
            sorted_exported_aliases.sort();
            let aliases_key = sorted_exported_aliases.join("_");

            // CRITICAL FIX: Use CTE name from analyzer's cte_references if available
            // **ARCHITECTURAL FIX (Jan 25, 2026)**: Use WithClause.cte_name directly from analysis phase
            // The CteSchemaResolver in the analyzer already generated the final CTE name with counter
            // and stored it in WithClause.cte_name. We should use it directly instead of regenerating.
            //
            // Why this is the right approach:
            // 1. CTE names are generated ONCE during analysis with consistent counters (plan_ctx.cte_counter)
            // 2. WithClause.cte_name stores the final name (e.g., "with_a_b_cte_1")
            // 3. Rendering should just USE this name, not try to regenerate with different counters
            //
            // The old approach tried to extract from cte_references HashMap, which is:
            // - Incomplete: only contains CTEs that other nodes explicitly reference
            // - Inconsistent: regenerates counters instead of using analysis phase values
            // - Source of two-phase mismatch: analysis generates "with_a_b_cte_1", rendering tries "with_a_b_cte_2"
            let mut cte_name = with_plans
                .first()
                .and_then(|plan| match plan {
                    LogicalPlan::WithClause(wc) => {
                        // Use the CTE name set by CteSchemaResolver in analysis phase
                        // This is the single source of truth for the WITH clause's CTE name
                        wc.cte_name.clone()
                    }
                    _ => None,
                })
                .unwrap_or_else(|| {
                    // FALLBACK ONLY: If cte_name somehow not set (shouldn't happen after fix)
                    // Generate unique CTE name using centralized utility
                    // Format: with_<sorted_aliases>_cte_<seq>
                    let seq_num = cte_sequence_numbers.entry(aliases_key.clone()).or_insert(1);
                    let current_seq = *seq_num;
                    let name = generate_cte_name(&sorted_exported_aliases, current_seq);
                    *seq_num += 1; // Increment for next iteration
                    log::debug!("🔧 build_chained_with_match_cte_plan: FALLBACK - WithClause.cte_name was None! Generated CTE name '{}' from aliases {:?} (sequence {}). This indicates analyzer didn't set cte_name properly.",
                               name, exported_aliases, current_seq);
                    name
                });

            // Ensure used_cte_names contains any CTEs hoisted earlier in this pass
            for existing in &all_ctes {
                used_cte_names.insert(existing.cte_name.clone());
            }

            // If analyzer provided a duplicate name (or hoisted CTE collided), generate a fresh one
            if used_cte_names.contains(&cte_name) {
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Duplicate CTE name '{}' detected, generating a unique name",
                    cte_name
                );

                let seq_entry = cte_sequence_numbers.entry(aliases_key.clone()).or_insert(1);
                let mut next_seq = *seq_entry;
                let mut candidate = generate_cte_name(&sorted_exported_aliases, next_seq);
                while used_cte_names.contains(&candidate) {
                    next_seq += 1;
                    candidate = generate_cte_name(&sorted_exported_aliases, next_seq);
                }

                // Remap the analyzer's name to the generated unique name
                cte_name_remapping.insert(cte_name.clone(), candidate.clone());

                *seq_entry = next_seq + 1;
                cte_name = candidate;
            }

            // Track this name as used and advance the sequence counter based on its suffix
            used_cte_names.insert(cte_name.clone());
            if let Some(suffix) = cte_name
                .rsplit('_')
                .next()
                .and_then(|s| s.parse::<usize>().ok())
            {
                let entry = cte_sequence_numbers
                    .entry(aliases_key.clone())
                    .or_insert(suffix + 1);
                if *entry <= suffix {
                    *entry = suffix + 1;
                }
            }

            log::debug!("🔧 build_chained_with_match_cte_plan: Using CTE name '{}' for exported aliases {:?}",
                       cte_name, exported_aliases);

            // CRITICAL: Collect CTE name remapping from analyzer's CTE names to our generated name
            // The analyzer may have generated different CTE names (e.g., with_name_cte_2) for the same aliases.
            // When expressions reference the analyzer's name, we need to remap them to our name.
            //
            // Strategy: Any analyzer CTE name with the same base alias pattern should be remapped.
            // E.g., if we generate "with_name_cte_1", then "with_name_cte_2", "with_name_cte_3" should remap to it.
            let cte_base = cte_name
                .rsplit("_cte_")
                .skip(1)
                .collect::<Vec<_>>()
                .join("_cte_");
            log::info!(
                "🔧 build_chained_with_match_cte_plan: CTE base pattern for '{}' is '{}'",
                cte_name,
                cte_base
            );

            for analyzer_name in &all_analyzer_cte_names {
                // Check if this analyzer name has the same base (e.g., "with_name")
                let analyzer_base = analyzer_name
                    .rsplit("_cte_")
                    .skip(1)
                    .collect::<Vec<_>>()
                    .join("_cte_");
                if analyzer_base == cte_base && analyzer_name != &cte_name {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Recording CTE name remap: '{}' → '{}' (same base)",
                        analyzer_name, cte_name
                    );
                    cte_name_remapping.insert(analyzer_name.clone(), cte_name.clone());
                }
            }

            // Create CTE content - if multiple renders, combine with UNION ALL
            // Extract ORDER BY, SKIP, LIMIT from first rendered plan (they should all have the same modifiers)
            // These come from the WithClause and were applied to each rendered plan earlier
            let first_order_by =
                if !rendered_plans.is_empty() && !rendered_plans[0].order_by.0.is_empty() {
                    Some(rendered_plans[0].order_by.clone())
                } else {
                    None
                };
            let first_skip = rendered_plans.first().and_then(|p| p.skip.0);
            let first_limit = rendered_plans.first().and_then(|p| p.limit.0);

            let mut with_cte_render = if rendered_plans.len() == 1 {
                // Safety: len() == 1 guarantees pop() returns Some
                rendered_plans
                    .pop()
                    .expect("rendered_plans has exactly one element")
            } else {
                // Multiple WITH clauses with same alias - create UNION ALL CTE
                log::debug!("🔧 build_chained_with_match_cte_plan: Combining {} WITH renders with UNION ALL for alias '{}'",
                           rendered_plans.len(), with_alias);

                // Clear ORDER BY/SKIP/LIMIT/HAVING from individual plans - they'll be applied to the UNION wrapper
                let first_having = rendered_plans.first().and_then(|p| p.having_clause.clone());
                for plan in &mut rendered_plans {
                    plan.order_by = OrderByItems(vec![]);
                    plan.skip = SkipItem(None);
                    plan.limit = LimitItem(None);
                    plan.having_clause = None;
                }

                // Create a wrapper RenderPlan with UnionItems, preserving ORDER BY/SKIP/LIMIT/HAVING
                RenderPlan {
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
                    having_clause: first_having,
                    order_by: first_order_by.unwrap_or_else(|| OrderByItems(vec![])),
                    skip: SkipItem(first_skip),
                    limit: LimitItem(first_limit),
                    union: UnionItems(Some(Union {
                        input: rendered_plans,
                        union_type: crate::render_plan::UnionType::All,
                    })),
                    fixed_path_info: None,
                    is_multi_label_scan: false,
                    variable_registry: None,
                }
            };

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Created CTE '{}'",
                cte_name
            );

            // Extract nested CTEs from the rendered plan (e.g., VLP recursive CTEs)
            // These need to be hoisted to the top level before the WITH CTE
            hoist_nested_ctes(&mut with_cte_render, &mut all_ctes);

            // ===== Pattern Comprehension: Correlated Subquery or CTE+LEFT JOIN =====
            // If this WithClause has pattern comprehensions, handle them.
            // NEW: If pattern_hops are populated (multi-hop/multi-correlation patterns),
            // generate inline correlated subqueries. Otherwise fall back to CTE+LEFT JOIN.
            if !pattern_comprehensions.is_empty() {
                // Check if any PC has full pattern info for correlated subquery approach
                let has_pattern_hops = pattern_comprehensions
                    .iter()
                    .any(|pc| !pc.pattern_hops.is_empty());

                if has_pattern_hops {
                    // ===== Pre-aggregated CTE + LEFT JOIN approach =====
                    // For each PC with pattern_hops and no list_constraint, generate a
                    // pre-aggregated CTE with GROUP BY on correlation columns, then LEFT JOIN
                    // from the WITH CTE to the PC CTE. This avoids ClickHouse "Cannot clone
                    // Union plan step" errors that occur with correlated subqueries + UNION ALL.

                    // Separate PCs into CTE-based (no list_constraint) and arrayCount-based
                    let cte_pcs: Vec<(
                        usize,
                        &crate::query_planner::logical_plan::PatternComprehensionMeta,
                    )> = pattern_comprehensions
                        .iter()
                        .enumerate()
                        .filter(|(_, pc)| {
                            !pc.pattern_hops.is_empty() && pc.list_constraint.is_none()
                        })
                        .collect();
                    let array_count_pcs: Vec<
                        &crate::query_planner::logical_plan::PatternComprehensionMeta,
                    > = pattern_comprehensions
                        .iter()
                        .filter(|pc| !pc.pattern_hops.is_empty() && pc.list_constraint.is_some())
                        .collect();

                    log::info!(
                        "🔧 Pattern comprehensions for '{}': {} CTE-based, {} arrayCount-based",
                        with_alias,
                        cte_pcs.len(),
                        array_count_pcs.len(),
                    );

                    // Phase A: Generate pre-aggregated CTEs for non-list-constraint PCs
                    let mut pc_cte_names: Vec<(usize, String)> = Vec::new(); // (pc_index, cte_name)
                    for (pc_idx, pc_meta) in &cte_pcs {
                        let pc_cte_name = format!("pc_{}_{}", with_alias, pc_idx);

                        if let Some(pc_result) = generate_pattern_comprehension_cte(pc_meta, schema)
                        {
                            log::info!(
                                "🔧 PC CTE '{}': {} correlation columns",
                                pc_cte_name,
                                pc_result.correlation_columns.len()
                            );

                            // Push the CTE before the WITH CTE (ordering matters)
                            all_ctes.push(Cte::new(
                                pc_cte_name.clone(),
                                CteContent::RawSql(pc_result.cte_sql),
                                false,
                            ));

                            // Build LEFT JOIN to the PC CTE
                            // ON conditions: pc_cte.corr_N = <corresponding CTE column>
                            let mut join_conditions: Vec<OperatorApplication> = Vec::new();
                            for (cv_idx, (var_name, label, corr_alias)) in
                                pc_result.correlation_columns.iter().enumerate()
                            {
                                // Find the CTE column reference for this correlation variable.
                                // We need to resolve (var_name, id) to a column in the WITH CTE body.
                                let cte_col_ref = find_pc_cte_join_column(
                                    var_name,
                                    label,
                                    schema,
                                    &with_cte_render,
                                    &cte_name,
                                );

                                if let Some(cte_ref) = cte_col_ref {
                                    // Parse "alias.column" into PropertyAccessExp for proper
                                    // dependency tracking in sort_joins_by_dependency
                                    let lhs_expr = if let Some(dot_pos) = cte_ref.find('.') {
                                        let alias_part =
                                            cte_ref[..dot_pos].trim_matches('"').to_string();
                                        let col_part =
                                            cte_ref[dot_pos + 1..].trim_matches('"').to_string();
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(alias_part),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(col_part),
                                        })
                                    } else {
                                        RenderExpr::Raw(cte_ref)
                                    };
                                    join_conditions.push(OperatorApplication {
                                        operator: Operator::Equal,
                                        operands: vec![
                                            lhs_expr,
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(pc_cte_name.clone()),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                    corr_alias.clone(),
                                                ),
                                            }),
                                        ],
                                    });
                                } else {
                                    log::warn!(
                                        "⚠️ Could not find CTE column for PC correlation var '{}' (label='{}', cv_idx={})",
                                        var_name,
                                        label,
                                        cv_idx
                                    );
                                }
                            }

                            // Guard: require all correlation predicates to be resolved.
                            // An empty or incomplete join would produce ON 1=1 (Cartesian product).
                            if join_conditions.len() != pc_result.correlation_columns.len() {
                                log::warn!(
                                    "⚠️ PC CTE '{}': only {}/{} join conditions resolved — skipping join (will use 0)",
                                    pc_cte_name,
                                    join_conditions.len(),
                                    pc_result.correlation_columns.len()
                                );
                            } else {
                                let pc_join = Join {
                                    table_name: pc_cte_name.clone(),
                                    table_alias: pc_cte_name.clone(),
                                    joining_on: join_conditions,
                                    join_type: JoinType::Left,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                };

                                // Add LEFT JOIN to the WITH CTE body.
                                // For UNION plans, add to each branch.
                                add_join_to_plan_or_union_branches(&mut with_cte_render, pc_join);

                                pc_cte_names.push((*pc_idx, pc_cte_name));
                            }
                        } else {
                            log::warn!(
                                "⚠️ Could not generate PC CTE for pattern comprehension #{} — falling back to 0",
                                pc_idx
                            );
                        }
                    }

                    // Phase B: Replace count(*) placeholders with COALESCE(pc_cte.result, 0)
                    // Build replacement expressions indexed by PC position
                    let mut pc_replacements: Vec<String> = Vec::new();
                    let mut cte_name_iter = pc_cte_names.iter();
                    let mut next_cte = cte_name_iter.next();
                    for (pc_idx, pc_meta) in pattern_comprehensions.iter().enumerate() {
                        if pc_meta.pattern_hops.is_empty() {
                            continue;
                        }
                        if pc_meta.list_constraint.is_some() {
                            // Will be handled by arrayCount path — put a placeholder
                            // that will be replaced below
                            pc_replacements.push("__arraycount_placeholder__".to_string());
                            continue;
                        }
                        if let Some((idx, ref name)) = next_cte {
                            if *idx == pc_idx {
                                pc_replacements.push(format!("COALESCE({}.result, 0)", name));
                                next_cte = cte_name_iter.next();
                            } else {
                                pc_replacements.push("0".to_string());
                            }
                        } else {
                            pc_replacements.push("0".to_string());
                        }
                    }

                    // Replace count(*) placeholders in SELECT items
                    replace_count_star_placeholders_in_select_or_union(
                        &mut with_cte_render,
                        &pc_replacements,
                    );

                    // Phase C: Handle arrayCount PCs (list_constraint patterns)
                    // These still use the inline approach since they don't need
                    // correlated subqueries.
                    if !array_count_pcs.is_empty() {
                        generate_and_replace_arraycount_pc_subqueries(
                            &mut with_cte_render,
                            &pattern_comprehensions,
                            schema,
                            &cte_name,
                        );
                    }

                    log::info!(
                        "✅ Pattern comprehension CTEs applied for '{}': {} CTEs created",
                        with_alias,
                        pc_cte_names.len(),
                    );
                } else {
                    // ===== LEGACY: CTE + LEFT JOIN approach (simple single-hop, single-correlation) =====
                    log::info!(
                        "🔧 Generating {} pattern comprehension CTE(s) for WITH alias '{}' (legacy path)",
                        pattern_comprehensions.len(),
                        with_alias
                    );

                    for (pc_idx, pc_meta) in pattern_comprehensions.iter().enumerate() {
                        let pc_cte_name = format!("pattern_comp_{}_{}", with_alias, pc_idx);

                        if let Some(pc_sql) = build_pattern_comprehension_sql(
                            &pc_meta.correlation_label,
                            &pc_meta.direction,
                            &pc_meta.rel_types,
                            &pc_meta.agg_type,
                            schema,
                            pc_meta.target_label.as_deref(),
                            pc_meta.target_property.as_deref(),
                        ) {
                            log::info!(
                                "🔧 Pattern comp CTE '{}': SQL = {}",
                                pc_cte_name,
                                &pc_sql[..pc_sql.len().min(200)]
                            );

                            let pc_cte =
                                Cte::new(pc_cte_name.clone(), CteContent::RawSql(pc_sql), false);
                            all_ctes.push(pc_cte);

                            use crate::graph_catalog::expression_parser::PropertyValue;

                            let lhs_expr = if with_cte_render.union.0.is_some()
                                && with_cte_render.from.0.is_none()
                            {
                                let id_column = find_node_id_column_from_schema(
                                    &pc_meta.correlation_var,
                                    &pc_meta.correlation_label,
                                    schema,
                                );
                                let node_alias = with_plans
                                    .first()
                                    .and_then(|p| match p {
                                        LogicalPlan::WithClause(wc) => {
                                            wc.exported_aliases.first().cloned()
                                        }
                                        _ => None,
                                    })
                                    .unwrap_or_else(|| pc_meta.correlation_var.clone());
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias("__union".to_string()),
                                    column: PropertyValue::Column(cte_column_name(
                                        &node_alias,
                                        &id_column,
                                    )),
                                })
                            } else {
                                build_node_id_expr_for_join(
                                    &pc_meta.correlation_var,
                                    &pc_meta.correlation_label,
                                    schema,
                                )
                            };

                            let pc_alias = format!("__pc_{}", pc_idx);

                            let on_clause = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    lhs_expr,
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(pc_alias.clone()),
                                        column: PropertyValue::Column("node_id".to_string()),
                                    }),
                                ],
                            };

                            let join = Join {
                                table_name: pc_cte_name.clone(),
                                table_alias: pc_alias.clone(),
                                joining_on: vec![on_clause],
                                join_type: JoinType::Left,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            };
                            with_cte_render.joins.0.push(join);

                            if with_cte_render.union.0.is_some()
                                && with_cte_render.from.0.is_none()
                                && with_cte_render.select.items.is_empty()
                            {
                                with_cte_render.select.items.push(SelectItem {
                                    expression: RenderExpr::Column(Column(PropertyValue::Column(
                                        "__union.*".to_string(),
                                    ))),
                                    col_alias: None,
                                });
                            }

                            let result_col_alias = pc_meta.result_alias.clone();
                            let result_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "coalesce".to_string(),
                                args: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(pc_alias.clone()),
                                        column: PropertyValue::Column("result".to_string()),
                                    }),
                                    RenderExpr::Literal(Literal::Integer(0)),
                                ],
                            });
                            with_cte_render.select.items.push(SelectItem {
                                expression: result_expr,
                                col_alias: Some(ColumnAlias(result_col_alias)),
                            });

                            log::info!(
                                "✅ Added pattern comp CTE '{}' with LEFT JOIN to WITH CTE",
                                pc_cte_name
                            );
                        } else {
                            log::debug!(
                                "⚠️ Could not generate pattern comp SQL for label '{}' — no matching edges in schema",
                                pc_meta.correlation_label
                            );
                        }
                    }
                }
            }

            // NOTE: Previously had intermediate_reverse_mapping block here (~180 lines)
            // that built reverse mapping from CTE columns and rewrote CTE body expressions.
            // Removed in Phase 3: scope-based resolution in CTE body rendering
            // (via VariableScope passed to to_render_plan_with_ctx) now handles this.

            // Extract SELECT items to build column metadata BEFORE creating the CTE
            // This allows the Cte to store column information for later CTE registry population
            let (select_items_for_schema, property_names_for_schema) = match &with_cte_render.union
            {
                UnionItems(Some(union)) if !union.input.is_empty() => {
                    // For UNION, take schema from first branch (all branches must have same schema)
                    let mut items = union.input[0].select.items.clone();
                    // Also include any wrapping SELECT items (e.g., pattern comprehension results)
                    // These are in with_cte_render.select alongside __union.* pass-through
                    for item in &with_cte_render.select.items {
                        if item.col_alias.is_some() {
                            items.push(item.clone());
                        }
                    }
                    let names: Vec<String> = items
                        .iter()
                        .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                        .collect();
                    (items, names)
                }
                _ => {
                    let items = with_cte_render.select.items.clone();
                    let names: Vec<String> = items
                        .iter()
                        .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                        .collect();
                    (items, names)
                }
            };

            // Build column metadata from SELECT items
            // This extracts: (cypher_alias, cypher_property) -> cte_column_name
            // Supports both new p{N} format and legacy underscore format
            let mut cte_columns: Vec<crate::render_plan::cte_manager::CteColumnMetadata> =
                Vec::new();
            for item in &select_items_for_schema {
                if let Some(col_alias) = &item.col_alias {
                    let cte_col_name = col_alias.0.clone();

                    // Try new p{N} format first
                    if let Some((parsed_alias, parsed_property)) = parse_cte_column(&cte_col_name) {
                        // Verify alias appears in with_alias
                        let alias_parts: Vec<&str> = with_alias.split('_').collect();
                        if alias_parts.contains(&parsed_alias.as_str()) {
                            cte_columns.push(crate::render_plan::cte_manager::CteColumnMetadata {
                                cypher_alias: parsed_alias.clone(),
                                cypher_property: parsed_property.clone(),
                                cte_column_name: cte_col_name.clone(),
                                db_column: parsed_property.clone(), // Approximation
                                is_id_column: parsed_property.ends_with("_id")
                                    || parsed_property == "id",
                                vlp_position: None,
                            });
                            log::debug!(
                                "  Added CTE column metadata (p{{N}}): ({}, {}) -> {}",
                                parsed_alias,
                                parsed_property,
                                cte_col_name
                            );
                        }
                    }
                    // Fallback: legacy underscore format
                    else if let Some(first_underscore) = cte_col_name.find('_') {
                        let potential_alias = &cte_col_name[..first_underscore];
                        let potential_property = &cte_col_name[first_underscore + 1..];

                        // Verify this is likely correct by checking if alias appears in with_alias
                        let alias_parts: Vec<&str> = with_alias.split('_').collect();
                        if alias_parts.contains(&potential_alias) {
                            cte_columns.push(crate::render_plan::cte_manager::CteColumnMetadata {
                                cypher_alias: potential_alias.to_string(),
                                cypher_property: potential_property.to_string(),
                                cte_column_name: cte_col_name.clone(),
                                db_column: potential_property.to_string(), // Approximation
                                is_id_column: potential_property.ends_with("_id")
                                    || potential_property == "id",
                                vlp_position: None,
                            });
                            log::debug!(
                                "  Added CTE column metadata (legacy): ({}, {}) -> {}",
                                potential_alias,
                                potential_property,
                                cte_col_name
                            );
                        }
                    }
                }
            }

            log::debug!(
                "🔧 Extracted {} column metadata entries for CTE '{}'",
                cte_columns.len(),
                cte_name
            );

            // Extract original WITH exported aliases for cte_references mapping
            let original_exported_aliases: Vec<String> = with_plans
                .iter()
                .find_map(|plan| {
                    if let LogicalPlan::WithClause(wc) = plan {
                        Some(wc.exported_aliases.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| with_alias.split('_').map(|s| s.to_string()).collect());

            // Build rename mapping: renamed_alias → original_alias
            // For "WITH u AS person", maps "person" → "u" so we can find
            // CTE columns prefixed with the original alias in property_mapping.
            let alias_rename_map: HashMap<String, String> = with_plans
                .iter()
                .find_map(|plan| {
                    if let LogicalPlan::WithClause(wc) = plan {
                        let mut renames = HashMap::new();
                        for item in &wc.items {
                            if let Some(ref col_alias) = item.col_alias {
                                let renamed = &col_alias.0;
                                // Extract original alias from expression
                                let original = match &item.expression {
                                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(ta) => Some(ta.0.clone()),
                                    crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) => Some(pa.table_alias.0.clone()),
                                    _ => None,
                                };
                                if let Some(orig) = original {
                                    if &orig != renamed {
                                        renames.insert(renamed.clone(), orig);
                                    }
                                }
                            }
                        }
                        Some(renames)
                    } else {
                        None
                    }
                })
                .unwrap_or_default();

            // ==========================================================================
            // FIX: Post-WITH OPTIONAL MATCH CTE body restructuring
            // ==========================================================================
            // When a post-WITH pattern uses OPTIONAL MATCH, the CTE body renders as:
            //   FROM pattern_table CROSS JOIN cte LEFT JOIN ...
            // This is semantically wrong: FROM should be the CTE (to preserve all rows
            // for OPTIONAL MATCH / LEFT JOIN semantics), and pattern tables should LEFT JOIN.
            //
            // Guard: Only applies when is_optional_pattern() was true on the input plan
            //   (deterministic — set from LogicalPlan::GraphRel.is_optional flag).
            // Structural requirements:
            //   - FROM is a regular table (not already a CTE or VLP)
            //   - A CTE CROSS JOIN exists in the join list (added by fix_orphan_table_aliases)
            // Transformation:
            //   1. Make CTE the FROM
            //   2. Find the "bridge join" (LEFT JOIN whose ON references the CTE alias)
            //   3. Restructure join chain: CTE → bridge_table → old_FROM → rest
            //   4. Embed WHERE predicate into countIf() aggregate
            // ==========================================================================
            if has_optional_match_input {
                if let FromTableItem(Some(ref from_ref)) = with_cte_render.from {
                    if !from_ref.name.starts_with("with_") && !from_ref.name.starts_with("vlp_") {
                        // Find a CTE CROSS JOIN anywhere in the join list
                        // fix_orphan_table_aliases adds JoinType::Join (renders as CROSS JOIN)
                        let cte_cross_join_idx = with_cte_render.joins.0.iter().position(|j| {
                            j.table_name.starts_with("with_")
                                && (matches!(j.join_type, super::JoinType::Inner)
                                    || matches!(j.join_type, super::JoinType::Join))
                        });

                        if let Some(cte_idx) = cte_cross_join_idx {
                            log::info!("🔧 OPTIONAL MATCH CTE body restructuring: has_optional_match_input=true, FROM='{}', CTE cross-join at idx {}",
                                from_ref.name, cte_idx);

                            let cte_join = with_cte_render.joins.0.remove(cte_idx);
                            let cte_table_name = cte_join.table_name.clone();
                            let cte_alias_str = cte_join.table_alias.clone();

                            // Find the "bridge join" — the LEFT JOIN whose ON condition references the CTE alias
                            let bridge_idx = with_cte_render.joins.0.iter().position(|j| {
                                j.joining_on.iter().any(|op| {
                                    op.operands.iter().any(|operand| {
                                        if let RenderExpr::PropertyAccessExp(pa) = operand {
                                            pa.table_alias.0 == cte_alias_str
                                        } else {
                                            false
                                        }
                                    })
                                })
                            });

                            if let Some(bridge_idx) = bridge_idx {
                                let bridge_join = with_cte_render.joins.0.remove(bridge_idx);
                                log::info!("🔧 OPTIONAL MATCH CTE body restructuring: bridge join '{}' connects CTE to pattern",
                                bridge_join.table_alias);

                                // Extract CTE column and pattern column from bridge join ON condition
                                let mut cte_col: Option<String> = None;
                                let mut pattern_alias: Option<String> = None;
                                let mut pattern_col: Option<String> = None;
                                for op in &bridge_join.joining_on {
                                    for operand in &op.operands {
                                        if let RenderExpr::PropertyAccessExp(pa) = operand {
                                            if pa.table_alias.0 == cte_alias_str {
                                                cte_col = Some(pa.column.raw().to_string());
                                            } else {
                                                pattern_alias = Some(pa.table_alias.0.clone());
                                                pattern_col = Some(pa.column.raw().to_string());
                                            }
                                        }
                                    }
                                }

                                if let (Some(cte_col), Some(pattern_alias), Some(pattern_col)) =
                                    (cte_col, pattern_alias, pattern_col)
                                {
                                    // Save old FROM info
                                    let old_from = with_cte_render.from.0.take().unwrap();
                                    let old_from_alias = old_from
                                        .alias
                                        .clone()
                                        .unwrap_or_else(|| old_from.name.clone());

                                    // Set FROM to CTE
                                    with_cte_render.from =
                                        FromTableItem(Some(super::ViewTableRef {
                                            source: std::sync::Arc::new(LogicalPlan::Empty),
                                            name: cte_table_name.clone(),
                                            alias: Some(cte_alias_str.clone()),
                                            use_final: false,
                                        }));

                                    // Find the pattern table join that was referenced in the bridge
                                    let pattern_join_idx = with_cte_render
                                        .joins
                                        .0
                                        .iter()
                                        .position(|j| j.table_alias == pattern_alias);

                                    if let Some(pidx) = pattern_join_idx {
                                        // Find FK column pointing to the old FROM table
                                        let mut fk_col_to_old_from: Option<String> = None;
                                        for op in &with_cte_render.joins.0[pidx].joining_on {
                                            for operand in &op.operands {
                                                if let RenderExpr::PropertyAccessExp(pa) = operand {
                                                    if pa.table_alias.0 == pattern_alias {
                                                        fk_col_to_old_from =
                                                            Some(pa.column.raw().to_string());
                                                    }
                                                }
                                            }
                                        }

                                        // Rewrite pattern join ON condition to reference CTE
                                        with_cte_render.joins.0[pidx].joining_on =
                                            vec![OperatorApplication {
                                                operator: Operator::Equal,
                                                operands: vec![
                                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(
                                                            pattern_alias.clone(),
                                                        ),
                                                        column: PropertyValue::Column(
                                                            pattern_col.clone(),
                                                        ),
                                                    }),
                                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(
                                                            cte_alias_str.clone(),
                                                        ),
                                                        column: PropertyValue::Column(
                                                            cte_col.clone(),
                                                        ),
                                                    }),
                                                ],
                                            }];

                                        // Reorder: move pattern join to position 0
                                        let pjoin = with_cte_render.joins.0.remove(pidx);
                                        with_cte_render.joins.0.insert(0, pjoin);

                                        // Add old FROM as LEFT JOIN after pattern join
                                        let old_from_join = super::Join {
                                            table_name: old_from.name.clone(),
                                            table_alias: old_from_alias.clone(),
                                            joining_on: vec![OperatorApplication {
                                                operator: Operator::Equal,
                                                operands: vec![
                                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(
                                                            old_from_alias.clone(),
                                                        ),
                                                        column: PropertyValue::Column(
                                                            "id".to_string(),
                                                        ),
                                                    }),
                                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(
                                                            pattern_alias.clone(),
                                                        ),
                                                        column: PropertyValue::Column(
                                                            fk_col_to_old_from.unwrap_or_else(
                                                                || "PostId".to_string(),
                                                            ),
                                                        ),
                                                    }),
                                                ],
                                            }],
                                            join_type: super::JoinType::Left,
                                            pre_filter: None,
                                            from_id_column: None,
                                            to_id_column: None,
                                            graph_rel: None,
                                        };
                                        with_cte_render.joins.0.insert(1, old_from_join);

                                        // Embed WHERE predicate into count() as countIf()
                                        // ClickHouse rejects complex LEFT JOIN ON expressions with join_use_nulls.
                                        // Instead of `count(x) WHERE cond`, use `countIf(x, cond)` with no WHERE.
                                        if let FilterItems(Some(where_expr)) =
                                            &with_cte_render.filters
                                        {
                                            let where_clone = where_expr.clone();
                                            // Find count() aggregate in SELECT and convert to countIf()
                                            for item in with_cte_render.select.items.iter_mut() {
                                                if let RenderExpr::AggregateFnCall(agg) =
                                                    &mut item.expression
                                                {
                                                    if agg.name == "count" && !agg.args.is_empty() {
                                                        log::info!("🔧 OPTIONAL MATCH CTE body restructuring: converting count() to countIf() with WHERE filter");
                                                        agg.name = "countIf".to_string();
                                                        agg.args.push(where_clone.clone());
                                                    }
                                                }
                                            }
                                            with_cte_render.filters = FilterItems(None);
                                        }

                                        // Remove redundant joins: bridge target table (Forum)
                                        with_cte_render
                                            .joins
                                            .0
                                            .retain(|j| j.table_alias != bridge_join.table_alias);

                                        // Remove spurious VLP CROSS JOINs
                                        with_cte_render.joins.0.retain(|j| {
                                            !(j.table_name.starts_with("vlp_")
                                                && (matches!(j.join_type, super::JoinType::Inner)
                                                    || matches!(
                                                        j.join_type,
                                                        super::JoinType::Join
                                                    )))
                                        });

                                        // Remove Person join if only used for IN/has() check — use FK instead.
                                        // The Person node (e.g., otherPerson2) is only needed to provide
                                        // its ID for the IN check. We can use the relationship table's FK
                                        // column (e.g., Post_hasCreator_Person.PersonId) directly.
                                        let person_join_idx = with_cte_render.joins.0.iter().position(|j| {
                                        matches!(j.join_type, super::JoinType::Left)
                                            && j.table_alias != old_from_alias
                                            && j.table_alias != pattern_alias
                                            // Node tables: "ldbc.Person" (contains '.' but not '_' after the db prefix)
                                            && j.table_name.split('.').last().map_or(false, |n| !n.contains('_'))
                                            && j.joining_on.iter().any(|op| {
                                                op.operands.iter().any(|operand| {
                                                    if let RenderExpr::PropertyAccessExp(pa) = operand {
                                                        pa.table_alias.0 == j.table_alias
                                                            && pa.column.raw() == "id"
                                                    } else {
                                                        false
                                                    }
                                                })
                                            })
                                    });
                                        if let Some(pidx2) = person_join_idx {
                                            let person_alias =
                                                with_cte_render.joins.0[pidx2].table_alias.clone();
                                            let mut select_aliases =
                                                std::collections::HashSet::new();
                                            for item in &with_cte_render.select.items {
                                                collect_aliases_from_single_render_expr(
                                                    &item.expression,
                                                    &mut select_aliases,
                                                );
                                            }
                                            let person_still_needed =
                                                select_aliases.contains(&person_alias);
                                            if !person_still_needed {
                                                // Find the FK column that joins this Person to the relationship table
                                                // e.g., otherPerson2.id = t2.PersonId → FK is "PersonId" on alias "t2"
                                                let mut fk_info: Option<(String, String)> = None; // (rel_alias, fk_col)
                                                for op in &with_cte_render.joins.0[pidx2].joining_on
                                                {
                                                    for operand in &op.operands {
                                                        if let RenderExpr::PropertyAccessExp(pa) =
                                                            operand
                                                        {
                                                            if pa.table_alias.0 != person_alias {
                                                                fk_info = Some((
                                                                    pa.table_alias.0.clone(),
                                                                    pa.column.raw().to_string(),
                                                                ));
                                                            }
                                                        }
                                                    }
                                                }

                                                // Rewrite IN operator references from person.id to rel.FK
                                                // At this stage, IN is OperatorApplication(In), not ScalarFnCall("has")
                                                if let Some((rel_alias, fk_col)) = &fk_info {
                                                    for j in with_cte_render.joins.0.iter_mut() {
                                                        for op in j.joining_on.iter_mut() {
                                                            // Check if this is an IN operator with person_alias ref
                                                            if matches!(op.operator, Operator::In)
                                                                && op.operands.len() == 2
                                                            {
                                                                if let RenderExpr::PropertyAccessExp(
                                                                pa,
                                                            ) = &op.operands[0]
                                                            {
                                                                if pa.table_alias.0 == person_alias
                                                                {
                                                                    op.operands[0] = RenderExpr::PropertyAccessExp(
                                                                        PropertyAccess {
                                                                            table_alias: TableAlias(rel_alias.clone()),
                                                                            column: PropertyValue::Column(fk_col.clone()),
                                                                        }
                                                                    );
                                                                    log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote IN to use {}.{}", rel_alias, fk_col);
                                                                }
                                                            }
                                                            }
                                                            // Also handle ScalarFnCall("has") form
                                                            for operand in op.operands.iter_mut() {
                                                                if let RenderExpr::ScalarFnCall(
                                                                    fn_call,
                                                                ) = operand
                                                                {
                                                                    if fn_call.name == "has"
                                                                        && fn_call.args.len() == 2
                                                                    {
                                                                        if let RenderExpr::PropertyAccessExp(pa) = &fn_call.args[1] {
                                                                        if pa.table_alias.0 == person_alias {
                                                                            fn_call.args[1] = RenderExpr::PropertyAccessExp(
                                                                                PropertyAccess {
                                                                                    table_alias: TableAlias(rel_alias.clone()),
                                                                                    column: PropertyValue::Column(fk_col.clone()),
                                                                                }
                                                                            );
                                                                            log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote has() to use FK column");
                                                                        }
                                                                    }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    // Also rewrite person alias references in SELECT items
                                                    // (e.g., inside countIf args where the WHERE filter was moved)
                                                    fn rewrite_person_to_fk(
                                                        expr: &mut RenderExpr,
                                                        person_alias: &str,
                                                        rel_alias: &str,
                                                        fk_col: &str,
                                                    )
                                                    {
                                                        match expr {
                                                            RenderExpr::PropertyAccessExp(pa)
                                                                if pa.table_alias.0
                                                                    == person_alias =>
                                                            {
                                                                pa.table_alias = TableAlias(
                                                                    rel_alias.to_string(),
                                                                );
                                                                pa.column = PropertyValue::Column(
                                                                    fk_col.to_string(),
                                                                );
                                                            }
                                                            RenderExpr::TableAlias(ta)
                                                                if ta.0 == person_alias =>
                                                            {
                                                                *expr = RenderExpr::PropertyAccessExp(
                                                                    PropertyAccess {
                                                                        table_alias: TableAlias(
                                                                            rel_alias.to_string(),
                                                                        ),
                                                                        column: PropertyValue::Column(
                                                                            fk_col.to_string(),
                                                                        ),
                                                                    },
                                                                );
                                                            }
                                                            RenderExpr::ColumnAlias(ca)
                                                                if ca.0 == person_alias =>
                                                            {
                                                                *expr = RenderExpr::PropertyAccessExp(
                                                                    PropertyAccess {
                                                                        table_alias: TableAlias(
                                                                            rel_alias.to_string(),
                                                                        ),
                                                                        column: PropertyValue::Column(
                                                                            fk_col.to_string(),
                                                                        ),
                                                                    },
                                                                );
                                                            }
                                                            RenderExpr::AggregateFnCall(agg) => {
                                                                for arg in agg.args.iter_mut() {
                                                                    rewrite_person_to_fk(
                                                                        arg,
                                                                        person_alias,
                                                                        rel_alias,
                                                                        fk_col,
                                                                    );
                                                                }
                                                            }
                                                            RenderExpr::ScalarFnCall(f) => {
                                                                for arg in f.args.iter_mut() {
                                                                    rewrite_person_to_fk(
                                                                        arg,
                                                                        person_alias,
                                                                        rel_alias,
                                                                        fk_col,
                                                                    );
                                                                }
                                                            }
                                                            RenderExpr::OperatorApplicationExp(
                                                                op,
                                                            ) => {
                                                                for operand in
                                                                    op.operands.iter_mut()
                                                                {
                                                                    rewrite_person_to_fk(
                                                                        operand,
                                                                        person_alias,
                                                                        rel_alias,
                                                                        fk_col,
                                                                    );
                                                                }
                                                            }
                                                            RenderExpr::List(items) => {
                                                                for item in items.iter_mut() {
                                                                    rewrite_person_to_fk(
                                                                        item,
                                                                        person_alias,
                                                                        rel_alias,
                                                                        fk_col,
                                                                    );
                                                                }
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                    for item in
                                                        with_cte_render.select.items.iter_mut()
                                                    {
                                                        rewrite_person_to_fk(
                                                            &mut item.expression,
                                                            &person_alias,
                                                            rel_alias,
                                                            fk_col,
                                                        );
                                                    }
                                                    log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote person refs in SELECT items");
                                                }
                                                with_cte_render.joins.0.remove(pidx2);
                                                log::info!("🔧 OPTIONAL MATCH CTE body restructuring: removed redundant Person join '{}'", person_alias);
                                            }
                                        }

                                        log::info!("🔧 OPTIONAL MATCH CTE body restructuring: complete. FROM='{}', {} joins",
                                        cte_table_name, with_cte_render.joins.0.len());
                                    }
                                }
                            } else {
                                // No bridge join found — put the CTE join back
                                with_cte_render.joins.0.insert(0, cte_join);
                            }
                        }
                    }
                }
            }

            // Create the CTE with column metadata.
            // If the UNION + correlated subquery split happened, use RawSql content.
            let pc_union_sql = with_cte_render
                .ctes
                .0
                .iter()
                .find(|c| c.cte_name == "__pc_union_sql__")
                .and_then(|c| match &c.content {
                    CteContent::RawSql(sql) => Some(sql.clone()),
                    _ => None,
                });
            let cte_content = if let Some(union_sql) = pc_union_sql {
                // Clear the marker CTE
                with_cte_render.ctes = CteItems(vec![]);
                CteContent::RawSql(union_sql)
            } else {
                CteContent::Structured(Box::new(with_cte_render.clone()))
            };
            let mut with_cte = Cte::new(cte_name.clone(), cte_content, false);
            with_cte.columns = cte_columns;
            with_cte.with_exported_aliases = original_exported_aliases.clone();

            // 🔧 CRITICAL FIX: Populate task-local CTE property mappings for SQL rendering
            // The PropertyAccessExp renderer checks get_cte_property_from_context() to resolve
            // CTE column names (e.g., "a_follows.name" → "a_follows.a_name")
            // We extract the FROM alias from the CTE name and build property → column mappings
            populate_cte_property_mappings_from_render_plan(&cte_name, &with_cte_render);

            all_ctes.push(with_cte);

            // Store CTE schema for later reference creation

            // Compute ID column mappings for this CTE using the DETERMINISTIC formula
            // Maps: alias → CTE column name that holds the ID
            // Uses compute_cte_id_column_for_alias which matches the naming convention
            // used when generating the SELECT items
            let mut alias_to_id_column: HashMap<String, String> = HashMap::new();

            // Use individual exported aliases (e.g., ["a", "allNeighboursCount"]) not combined with_alias
            // compute_cte_id_column_for_alias needs the actual node alias to find the GraphNode.
            // CRITICAL: Check CTE references FIRST — if the alias is already CTE-backed from an
            // upstream WITH, inherit its ID column. Otherwise, plan-level lookup may find stale
            // VLP endpoints (e.g., end_id) instead of the CTE's renamed column (e.g., id).
            let id_lookup_plan = inner_plans_for_id.first().unwrap_or(&current_plan);
            for alias in &exported_aliases {
                // Priority 1: Inherit from upstream CTE (most reliable for chained WITH)
                if let Some(prev_cte_name) = cte_references.get(alias) {
                    if let Some(meta) = cte_schemas.get(prev_cte_name) {
                        if let Some(prev_id) = meta.alias_to_id.get(alias) {
                            log::info!(
                                "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (inherited from CTE '{}')",
                                cte_name, alias, prev_id, prev_cte_name
                            );
                            alias_to_id_column.insert(alias.clone(), prev_id.clone());
                            continue;
                        } else if meta.column_names.contains(alias) {
                            // Fallback: CTE has a direct column matching alias (e.g. UNWIND scalar)
                            log::info!(
                                "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (bare column from CTE '{}')",
                                cte_name, alias, alias, prev_cte_name
                            );
                            alias_to_id_column.insert(alias.clone(), alias.clone());
                            continue;
                        }
                    }
                }
                // Priority 2: Compute from plan structure (inner plan first, then current)
                if let Some(id_col_name) = compute_cte_id_column_for_alias(alias, id_lookup_plan)
                    .or_else(|| compute_cte_id_column_for_alias(alias, &current_plan))
                {
                    log::info!(
                        "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (deterministic)",
                        cte_name,
                        alias,
                        id_col_name
                    );
                    alias_to_id_column.insert(alias.clone(), id_col_name.clone());
                }
            }

            // Build explicit property mapping for WITH CTE
            let mut property_mapping =
                build_property_mapping_from_columns(&select_items_for_schema);

            log::debug!(
                "🔧 DEBUG: property_mapping BEFORE dot-to-underscore transformation: {} entries",
                property_mapping.len()
            );
            for ((alias, property), cte_column) in property_mapping.iter() {
                log::debug!("🔧   BEFORE: ({}, {}) → {}", alias, property, cte_column);
            }

            // Transform dotted column names to underscores for WITH CTEs
            // (WITH CTE columns use "friend_id", not "friend.id")
            property_mapping = property_mapping
                .into_iter()
                .map(|(k, v)| (k, v.replace('.', "_")))
                .collect();

            log::debug!(
                "🔧 DEBUG: property_mapping AFTER dot-to-underscore transformation: {} entries",
                property_mapping.len()
            );
            for ((alias, property), cte_column) in property_mapping.iter() {
                log::debug!("🔧   AFTER: ({}, {}) → {}", alias, property, cte_column);
            }

            // 🔧 FIX: Add compound key mappings for flattened map literal columns.
            // These were collected at generation time by try_flatten_head_collect_map_literal()
            // to avoid ambiguous reverse-engineering from underscore-delimited column names.
            // Each entry maps ("base_alias", "map_key.property") → "base_alias_mapkey_property".
            {
                let stored_keys = flattened_compound_keys.borrow();
                for (compound_key, col_name) in stored_keys.iter() {
                    // Find which exported alias this column belongs to
                    for base_alias in &exported_aliases {
                        let prefix = format!("{}_", base_alias);
                        if col_name.starts_with(&prefix) {
                            log::info!(
                                "🔧 property_mapping compound key: ({}, {}) → {}",
                                base_alias,
                                compound_key,
                                col_name
                            );
                            property_mapping.insert(
                                (base_alias.clone(), compound_key.clone()),
                                col_name.clone(),
                            );
                            break;
                        }
                    }
                }
            }

            // Cross-reference: for bare column aliases (e.g. UNWIND scalar `person`),
            // add (alias, "id") → alias so `person.id` resolves to the "person" column
            for (alias, id_col) in &alias_to_id_column {
                if id_col == alias {
                    property_mapping
                        .entry((alias.clone(), "id".to_string()))
                        .or_insert_with(|| alias.clone());
                    log::info!(
                        "🔧 property_mapping cross-ref: ({}, id) → {} (bare column alias)",
                        alias,
                        alias
                    );
                }
            }

            log::debug!(
                "🔧 DEBUG: property_mapping AFTER dot-to-underscore transformation: {} entries",
                property_mapping.len()
            );
            for ((alias, property), cte_column) in property_mapping.iter().take(10) {
                log::debug!("🔧   ({}, {}) → {}", alias, property, cte_column);
            }

            // Store CTE schema with full property mapping
            cte_schemas.insert(
                cte_name.clone(),
                crate::render_plan::CteSchemaMetadata {
                    select_items: select_items_for_schema.clone(),
                    column_names: property_names_for_schema.clone(),
                    alias_to_id: alias_to_id_column,
                    property_mapping: property_mapping.clone(),
                },
            );

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Stored schema for CTE '{}': {:?}, {} property mappings",
                cte_name,
                property_names_for_schema, property_mapping.len()
            );

            // Replacing WITH clauses with this alias with CTE reference
            // Also pass pre_with_aliases so joins from the pre-WITH scope can be filtered out
            log::debug!("🔧 build_chained_with_match_cte_plan: Replacing WITH clauses for alias '{}' with CTE '{}'", with_alias, cte_name);
            log::debug!("🔧 build_chained_with_match_cte_plan: BEFORE replacement - plan discriminant: {:?}", std::mem::discriminant(&current_plan));

            // Debug: Show WITH structure before replacement
            fn show_with_structure(plan: &LogicalPlan, indent: usize) {
                let prefix = "  ".repeat(indent);
                match plan {
                    LogicalPlan::WithClause(wc) => {
                        let key = if !wc.exported_aliases.is_empty() {
                            let mut aliases = wc.exported_aliases.clone();
                            aliases.sort();
                            aliases.join("_")
                        } else {
                            "with_var".to_string()
                        };
                        log::debug!(
                            "{}WithClause(key='{}', cte_refs={:?})",
                            prefix,
                            key,
                            wc.cte_references
                        );
                        show_with_structure(&wc.input, indent + 1);
                    }
                    LogicalPlan::Limit(lim) => {
                        log::debug!("{}Limit({})", prefix, lim.count);
                        show_with_structure(&lim.input, indent + 1);
                    }
                    LogicalPlan::GraphJoins(gj) => {
                        log::debug!("{}GraphJoins({} joins)", prefix, gj.joins.len());
                        show_with_structure(&gj.input, indent + 1);
                    }
                    LogicalPlan::Projection(proj) => {
                        log::debug!("{}Projection({} items)", prefix, proj.items.len());
                        show_with_structure(&proj.input, indent + 1);
                    }
                    LogicalPlan::GraphNode(gn) => {
                        log::debug!("{}GraphNode(alias='{}')", prefix, gn.alias);
                    }
                    LogicalPlan::ViewScan(vs) => {
                        log::debug!("{}ViewScan(table='{}')", prefix, vs.source_table);
                    }
                    LogicalPlan::Union(u) => {
                        log::debug!("{}Union({} branches)", prefix, u.inputs.len());
                        for (i, input) in u.inputs.iter().enumerate() {
                            log::debug!("{}  Branch {}:", prefix, i);
                            show_with_structure(input, indent + 2);
                        }
                    }
                    LogicalPlan::GraphRel(gr) => {
                        log::debug!(
                            "{}GraphRel(l='{}', r='{}', dir={:?})",
                            prefix,
                            gr.left_connection,
                            gr.right_connection,
                            gr.labels.as_ref().map(|l| l.join(",")).unwrap_or_default()
                        );
                        log::debug!("{}  left:", prefix);
                        show_with_structure(&gr.left, indent + 2);
                        log::debug!("{}  right:", prefix);
                        show_with_structure(&gr.right, indent + 2);
                    }
                    LogicalPlan::Filter(f) => {
                        log::debug!("{}Filter", prefix);
                        show_with_structure(&f.input, indent + 1);
                    }
                    LogicalPlan::CartesianProduct(cp) => {
                        log::debug!("{}CartesianProduct", prefix);
                        log::debug!("{}  left:", prefix);
                        show_with_structure(&cp.left, indent + 2);
                        log::debug!("{}  right:", prefix);
                        show_with_structure(&cp.right, indent + 2);
                    }
                    LogicalPlan::GroupBy(gb) => {
                        log::debug!("{}GroupBy", prefix);
                        show_with_structure(&gb.input, indent + 1);
                    }
                    LogicalPlan::OrderBy(ob) => {
                        log::debug!("{}OrderBy", prefix);
                        show_with_structure(&ob.input, indent + 1);
                    }
                    LogicalPlan::Skip(s) => {
                        log::debug!("{}Skip({})", prefix, s.count);
                        show_with_structure(&s.input, indent + 1);
                    }
                    LogicalPlan::Unwind(u) => {
                        log::debug!("{}Unwind(alias='{}')", prefix, u.alias);
                        show_with_structure(&u.input, indent + 1);
                    }
                    other => {
                        log::debug!("{}Other({:?})", prefix, std::mem::discriminant(other));
                    }
                }
            }
            log::debug!("🔧 PLAN STRUCTURE BEFORE REPLACEMENT:");
            show_with_structure(&current_plan, 0);

            current_plan = replace_with_clause_with_cte_reference_v2(
                &current_plan,
                &with_alias,
                &cte_name,
                &pre_with_aliases,
                &cte_schemas,
            )?;
            log::info!(
                "🔧 build_chained_with_match_cte_plan: AFTER replacement - plan discriminant: {:?}",
                std::mem::discriminant(&current_plan)
            );
            log::debug!(
                "🔀 UNION_TRACE after replace_v2: has_union={}",
                current_plan.has_union_anywhere()
            );

            log::debug!("🔧 PLAN STRUCTURE AFTER REPLACEMENT:");
            show_with_structure(&current_plan, 0);

            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Replacement complete for '{}'",
                with_alias
            );

            // Track that this alias is now a CTE (so subsequent iterations don't filter it)
            // Add the full composite alias
            processed_cte_aliases.insert(with_alias.clone());

            // CRITICAL: Update cte_references to point to the NEW CTE name
            // This ensures subsequent references to this alias (in the final query or later CTEs)
            // use the MOST RECENT CTE, not the original one from the analyzer
            //
            // For composite aliases like "a_b", we need to add BOTH:
            // 1. The composite key "a_b" → CTE (for replacement logic)
            // 2. Individual aliases "a" → CTE and "b" → CTE (for expand_table_alias_to_select_items)
            //
            // This allows "WITH a, b, c" to find columns for both "a" and "b" from the "with_a_b_cte_1"
            cte_references.insert(with_alias.clone(), cte_name.clone());

            // Also add individual aliases — use exported_aliases from the WITH clause
            // (splitting with_alias by '_' fails for aliases containing underscores like "__expand")
            for alias in &original_exported_aliases {
                if !alias.is_empty() {
                    cte_references.insert(alias.clone(), cte_name.clone());
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Added individual mapping: '{}' → '{}'",
                        alias,
                        cte_name
                    );
                }
            }

            log::debug!("🔧 build_chained_with_match_cte_plan: Updated cte_references: '{}' → '{}' (plus {} individual aliases)",
                       with_alias, cte_name, original_exported_aliases.len());

            // CRITICAL: Also update cte_references_for_rendering!
            // This allows subsequent WITH clauses in THIS ITERATION to reference the new CTE
            // Example: "WITH count(*) AS total" then "WITH total, year" - second WITH needs "total" in cte_references_for_rendering
            cte_references_for_rendering = cte_references.clone();
            log::debug!("🔧 build_chained_with_match_cte_plan: Updated cte_references_for_rendering with {} entries", cte_references_for_rendering.len());

            // Update scope CTE variables: record each exported alias's property mapping
            // so downstream rendering resolves CTE variables correctly.
            //
            // WITH barrier: snapshot body registry, then clear accumulated scope
            // so only current CTE's exports are visible in the next scope.
            let body_registry = std::sync::Arc::new(var_registry.clone());
            scope_cte_variables.clear();
            var_registry.clear();

            for alias in &original_exported_aliases {
                if alias.is_empty() {
                    continue;
                }
                // For renamed aliases (e.g., "person" from "WITH u AS person"),
                // look up properties under the original alias name ("u") since
                // CTE columns are prefixed with the original alias (p1_u_*).
                let lookup_alias = alias_rename_map.get(alias).unwrap_or(alias);

                // Extract per-alias property mapping: cypher_prop → cte_column
                let per_alias_mapping: HashMap<String, String> = property_mapping
                    .iter()
                    .filter(|((a, _), _)| a == lookup_alias)
                    .map(|((_, prop), col)| (prop.clone(), col.clone()))
                    .collect();

                // Get labels from current plan tree — try renamed alias first,
                // then fall back to original alias (for renamed variables)
                let labels = crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan(
                    &current_plan, alias,
                ).or_else(|| {
                    crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan(
                        &current_plan, lookup_alias,
                    )
                }).map(|l| vec![l]).unwrap_or_default();

                scope_cte_variables.insert(
                    alias.clone(),
                    super::variable_scope::CteVariableInfo {
                        cte_name: cte_name.clone(),
                        property_mapping: per_alias_mapping.clone(),
                        labels: labels.clone(),
                        from_alias_override: None,
                        map_keys: None,
                    },
                );

                // Update unified variable registry: define/overwrite variable as CTE-sourced
                // with its property mapping so the SQL renderer can resolve properties.
                {
                    use crate::query_planner::typed_variable::VariableSource;
                    let cte_source = VariableSource::Cte {
                        cte_name: cte_name.clone(),
                        property_mapping: Box::new(per_alias_mapping.clone()),
                    };
                    if labels.is_empty() {
                        // No labels → scalar variable (e.g., computed column, count, etc.)
                        var_registry.define_scalar(alias.clone(), cte_source);
                    } else {
                        // Has labels → node variable (relationship labels would need
                        // more context; treating as node is correct for most WITH exports)
                        var_registry.define_node(alias.clone(), labels.clone(), cte_source);
                    }
                }
                log::info!(
                    "🔧 build_chained: scope_cte_variables updated for alias '{}' → CTE '{}'",
                    alias,
                    cte_name
                );
                // Detect map-typed expressions and register map keys
                for item in &select_items_for_schema {
                    if let Some(col_alias) = &item.col_alias {
                        if col_alias.0 == *alias {
                            if let Some(keys) =
                                super::variable_scope::extract_map_keys_from_expr(&item.expression)
                            {
                                if let Some(info) = scope_cte_variables.get_mut(alias) {
                                    info.map_keys = Some(keys);
                                }
                            }
                            break;
                        }
                    }
                }
            }

            // CRITICAL FIX: Also add the COMPOSITE alias (e.g., "countWindow1_tag") to scope_cte_variables.
            // The analyzer creates expressions with the composite alias as table_alias in PropertyAccessExp.
            // Without this, scope-aware rewriting in subsequent CTE bodies can't resolve composite aliases.
            // The composite alias's property_mapping merges ALL individual aliases' mappings, plus
            // identity entries for scalar aliases (which are direct CTE column names).
            if original_exported_aliases.len() > 1 {
                let mut composite_mapping: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                for alias in &original_exported_aliases {
                    if let Some(info) = scope_cte_variables.get(alias) {
                        // Merge Cypher→CTE column mappings from this individual alias
                        composite_mapping.extend(
                            info.property_mapping
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone())),
                        );
                        // Scalar variables (empty property_mapping) are direct column names in the CTE
                        if info.property_mapping.is_empty() {
                            composite_mapping.insert(alias.clone(), alias.clone());
                        }
                    }
                }
                scope_cte_variables.insert(
                    with_alias.clone(),
                    super::variable_scope::CteVariableInfo {
                        cte_name: cte_name.clone(),
                        property_mapping: composite_mapping.clone(),
                        labels: Vec::new(),
                        from_alias_override: None,
                        map_keys: None,
                    },
                );
                log::info!(
                    "🔧 build_chained: Added composite alias '{}' to scope_cte_variables with {} properties",
                    with_alias,
                    composite_mapping.len()
                );
            }

            // Attach body registry (pre-barrier snapshot) to the CTE for runtime resolution
            if let Some(last_cte) = all_ctes.last_mut() {
                if last_cte.cte_name == cte_name {
                    last_cte.variable_registry = Some(body_registry);
                }
            }

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Added '{}' to processed_cte_aliases",
                with_alias
            );

            // DON'T add individual parts - this causes issues with detecting duplicates
            // Example: "b_c" should not add "b" and "c" separately, because that would
            // prevent processing "b_c" again if it appears multiple times in the plan

            // Mark that we processed something this iteration
            any_processed_this_iteration = true;

            log::debug!("🔧 build_chained_with_match_cte_plan: Replaced WITH clauses for alias '{}' with CTE reference (processed_cte_aliases: {:?})",
                       with_alias, processed_cte_aliases);

            // CRITICAL FIX (Jan 2026): Break after processing ONE alias to re-discover plan structure.
            // Problem: When we process multiple aliases in one iteration, the `with_plans` for later
            // aliases were captured BEFORE we replaced earlier aliases. This causes:
            // 1. Nested WITH clauses to be processed twice (once by outer, once by recursive call)
            // 2. Duplicate CTE names to be generated
            //
            // Solution: Process one alias, update current_plan, then let the while loop iterate
            // again with fresh find_all_with_clauses_grouped() on the updated plan.
            log::debug!("🔧 build_chained_with_match_cte_plan: Breaking after processing '{}' to re-discover plan structure", with_alias);
            break 'alias_loop;
        }

        // DEBUG: Summary at end of iteration
        let plan_depth_after = count_plan_depth(&current_plan);
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: END ITERATION {} - Plan depth: {} → {} (processed: {})",
            iteration,
            plan_depth,
            plan_depth_after,
            any_processed_this_iteration
        );

        // If no aliases were processed this iteration, break to avoid infinite loop
        // This can happen when all remaining WITH clauses are passthrough wrappers
        if !any_processed_this_iteration {
            log::debug!("🔧 build_chained_with_match_cte_plan: No aliases processed in iteration {}, breaking out", iteration);
            break;
        }

        log::debug!("🔧 build_chained_with_match_cte_plan: Iteration {} complete, checking for more WITH clauses", iteration);
    }

    log::debug!(
        "🔀 UNION_TRACE after all WITH iterations: has_union={}",
        current_plan.has_union_anywhere()
    );

    // Verify that all WITH clauses were actually processed
    // If any remain, it means we failed to process them and should not continue
    // to avoid triggering a fresh recursive call that loses our accumulated CTEs
    if has_with_clause_in_graph_rel(&current_plan) {
        let remaining_withs = find_all_with_clauses_grouped(&current_plan);
        let remaining_aliases: Vec<_> = remaining_withs.keys().collect();
        log::error!(
            "🔧 build_chained_with_match_cte_plan: Unprocessed WITH clauses remain after {} iterations: {:?}",
            iteration, remaining_aliases
        );
        log::error!(
            "🔧 build_chained_with_match_cte_plan: Accumulated CTEs: {:?}",
            all_ctes.iter().map(|c| &c.cte_name).collect::<Vec<_>>()
        );
        return Err(RenderBuildError::InvalidRenderPlan(format!(
            "Failed to process all WITH clauses after {} iterations. Remaining aliases: {:?}. This may indicate nested WITH clauses that couldn't be resolved.",
            iteration, remaining_aliases
        )));
    }

    log::debug!("🔧 build_chained_with_match_cte_plan: All WITH clauses processed ({} CTEs), rendering final plan", all_ctes.len());

    // DEBUG: Log the full plan structure before rendering
    log::debug!("🐛 DEBUG FINAL PLAN structure (after WITH processing):");
    show_plan_structure(&current_plan, 0);

    // DEBUG: Log the current_plan structure before rendering
    log::debug!(
        "🐛 DEBUG FINAL PLAN before render: discriminant={:?}",
        std::mem::discriminant(&current_plan)
    );
    if let LogicalPlan::Projection(proj) = &current_plan {
        log::debug!(
            "🐛 DEBUG: Projection -> input discriminant={:?}",
            std::mem::discriminant(proj.input.as_ref())
        );
        if let LogicalPlan::GraphJoins(gj) = proj.input.as_ref() {
            log::debug!("🐛 DEBUG: Found GraphJoins with {} joins:", gj.joins.len());
            for (i, j) in gj.joins.iter().enumerate() {
                log::debug!(
                    "🐛 DEBUG:   JOIN {}: table='{}', alias='{}', joining_on.len()={}",
                    i,
                    j.table_name,
                    j.table_alias,
                    j.joining_on.len()
                );
            }
            log::debug!(
                "🐛 DEBUG: GraphJoins.cte_references = {:?}",
                gj.cte_references
            );
        }
    }

    // CRITICAL FIX: Before rendering, check if the final plan has GraphJoins with joins
    // that should be covered by the LAST CTE (the one with the most aliases).
    // Pattern: WITH a, b ... MATCH (b)-[]->(c)
    // The GraphJoins will have joins for: a→t1→b, b→t2→c
    // But a→t1→b is already in with_a_b_cte2, so we need to remove those joins!

    log::info!(
        "🔧 build_chained_with_match_cte_plan: PRE-RENDER CHECK - have {} CTEs",
        all_ctes.len()
    );

    if !all_ctes.is_empty() {
        // Get the last CTE's exported aliases (from its name, e.g., "with_a_b_cte2" → ["a", "b"])
        // Safety: !is_empty() guarantees last() returns Some
        let last_cte = all_ctes.last().expect("all_ctes is non-empty");
        let last_cte_name = &last_cte.cte_name;

        // Extract aliases from CTE name: "with_a_b_cte2" → "a_b"
        // Format is: with_{aliases}_cte{N}
        // Strategy: trim "with_", then remove "_cte{N}" suffix
        let alias_part = if let Some(stripped) = last_cte_name.strip_prefix("with_") {
            // Find the last occurrence of "_cte" and take everything before it
            if let Some(cte_pos) = stripped.rfind("_cte") {
                &stripped[..cte_pos]
            } else {
                stripped
            }
        } else {
            ""
        };

        log::info!(
            "🔧 build_chained_with_match_cte_plan: Last CTE '{}' exports alias_part: '{}'",
            last_cte_name,
            alias_part
        );

        // For composite aliases like "a_b", split into individual aliases
        if !alias_part.is_empty() {
            let exported_aliases: Vec<&str> = alias_part.split('_').collect();
            let exported_aliases_set: std::collections::HashSet<&str> =
                exported_aliases.iter().copied().collect();

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Exported aliases: {:?}",
                exported_aliases
            );

            // Now we need to prune joins from GraphJoins that are covered by this CTE
            // AND update any GraphNode that matches an exported alias to reference the CTE
            log::debug!(
                "🔀 UNION_TRACE before prune_joins: has_union={}",
                current_plan.has_union_anywhere()
            );
            current_plan = prune_joins_covered_by_cte(
                &current_plan,
                last_cte_name,
                &exported_aliases_set,
                &cte_schemas,
            )?;

            // CRITICAL: Update all GraphJoins.cte_references with the latest CTE mapping
            // After replacement, the plan may have GraphJoins with stale cte_references from analyzer
            // Build property mappings from scope_cte_variables for column resolution
            let cte_prop_mappings: std::collections::HashMap<
                String,
                std::collections::HashMap<String, String>,
            > = scope_cte_variables
                .iter()
                .map(|(alias, info)| (alias.clone(), info.property_mapping.clone()))
                .collect();
            log::debug!("🔧 build_chained_with_match_cte_plan: Updating GraphJoins.cte_references with latest mapping: {:?}", cte_references);
            current_plan =
                update_graph_joins_cte_refs(&current_plan, &cte_references, &cte_prop_mappings)?;
        }
    }

    // Scope-aware join cleanup: remove ALL pre-computed joins whose aliases are now CTE-scoped.
    // These joins are stale — they reference table-level tables from before the WITH barrier.
    // The CTE references in the plan tree will produce the correct FROM/JOIN via extract_joins().
    if !scope_cte_variables.is_empty() {
        let cte_aliases: std::collections::HashSet<&str> =
            scope_cte_variables.keys().map(|s| s.as_str()).collect();
        current_plan = clear_stale_joins_for_cte_aliases(&current_plan, &cte_aliases);
        log::info!(
            "🔧 build_chained: Cleared stale joins for CTE aliases: {:?}",
            cte_aliases
        );
    }

    // All WITH clauses have been processed, now render the final plan
    // Build scope from all accumulated CTE variables for the final rendering pass.
    let final_scope = super::variable_scope::VariableScope::with_cte_variables(
        schema,
        &current_plan,
        scope_cte_variables.clone(),
    );
    let final_scope_ref = if scope_cte_variables.is_empty() && scope.is_none() {
        None
    } else {
        Some(&final_scope)
    };
    // Use render_plan_with_ctx to pass plan_ctx for VLP property selection
    let mut render_plan =
        current_plan.to_render_plan_with_ctx(schema, plan_ctx, final_scope_ref)?;

    log::info!(
        "🔧 build_chained_with_match_cte_plan: Final render complete. FROM: {:?}, SELECT items: {}",
        render_plan.from,
        render_plan.select.items.len()
    );

    // CRITICAL FIX: Apply CTE name remapping for passthrough WITHs
    // When WITHs are skipped, expressions may still reference the analyzer's CTE names.
    // Remap them to the actual CTE names that were created.
    if !cte_name_remapping.is_empty() {
        log::info!(
            "🔧 build_chained_with_match_cte_plan: Applying CTE name remapping ({} entries)",
            cte_name_remapping.len()
        );
        remap_cte_names_in_render_plan(&mut render_plan, &cte_name_remapping);
    }

    // Comprehensive CTE name fixup: the analyzer assigns CTE names with its own counter
    // (e.g., _cte_5) but the renderer creates CTEs with sequential numbering (_cte_1).
    // Scan render plan for any with_*_cte_N references that don't match actual CTEs.
    {
        let actual_cte_names: std::collections::HashSet<String> =
            all_ctes.iter().map(|c| c.cte_name.clone()).collect();
        // Build base→actual mapping: strip _cte_N suffix to get base, map to actual name
        let mut base_to_actual: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for name in &actual_cte_names {
            if let Some(base) = name.rfind("_cte_").map(|pos| &name[..pos]) {
                base_to_actual.insert(base.to_string(), name.clone());
            }
        }
        // Collect all table aliases from render plan that look like CTE references
        let referenced = collect_with_cte_table_aliases(&render_plan);
        let mut auto_remap: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for ref_name in &referenced {
            if actual_cte_names.contains(ref_name) {
                continue; // Already correct
            }
            if let Some(pos) = ref_name.rfind("_cte_") {
                let base = &ref_name[..pos];
                if let Some(actual) = base_to_actual.get(base) {
                    auto_remap.insert(ref_name.clone(), actual.clone());
                }
            }
        }
        if !auto_remap.is_empty() {
            log::debug!(
                "🔧 build_chained: Auto-remapping {} stale CTE references",
                auto_remap.len()
            );
            for (from, to) in &auto_remap {
                log::debug!("🔧   {} → {}", from, to);
            }
            remap_cte_names_in_render_plan(&mut render_plan, &auto_remap);
        }
    }

    // CRITICAL FIX: If FROM references an alias that's now in a CTE, replace it with the CTE
    // This happens when WITH exports an alias that was originally from a table
    if let FromTableItem(Some(from_ref)) = &render_plan.from {
        // Check if the FROM alias is in cte_references
        if let Some(alias) = &from_ref.alias {
            if let Some(cte_name) = cte_references.get(alias) {
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: FROM alias '{}' is in CTE '{}', replacing FROM",
                    alias,
                    cte_name
                );

                // Keep the original alias (e.g., "a") as the FROM alias.
                // The rest of the rendered plan (SELECT, WHERE, JOINs) references "a.xxx",
                // so the FROM alias must match. The CTE columns are prefixed with the
                // original alias (e.g., "a_customer_id"), which works with FROM alias "a".
                let preserved_alias = alias.clone();

                // Compute what the combined alias WOULD have been (e.g., "a_allNeighboursCount")
                // so we can rewrite any stale references in SELECT/WHERE/ORDER BY
                let combined_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    String::new()
                };

                render_plan.from = FromTableItem(Some(ViewTableRef {
                    source: std::sync::Arc::new(LogicalPlan::Empty),
                    name: cte_name.clone(),
                    alias: Some(preserved_alias.clone()),
                    use_final: false,
                }));

                // Rewrite stale references: combined alias → preserved alias
                // e.g., "a_allNeighboursCount.xxx" → "a.xxx" in SELECT, WHERE, JOINs
                if combined_alias != preserved_alias && !combined_alias.is_empty() {
                    log::debug!(
                        "🔧 Rewriting stale alias '{}' → '{}' in render plan",
                        combined_alias,
                        preserved_alias
                    );
                    rewrite_table_alias_in_render_plan(
                        &mut render_plan,
                        &combined_alias,
                        &preserved_alias,
                    );
                }

                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Replaced FROM with: {} AS '{}'",
                    cte_name,
                    preserved_alias
                );
            }
        }
    } else if matches!(render_plan.from, FromTableItem(None))
        && !all_ctes.is_empty()
        && render_plan.union.0.is_none()
    {
        // FALLBACK: If FROM is None but we have CTEs, set FROM to the last CTE
        // This happens when WITH clauses are chained and all table references have been replaced with CTEs
        // Skip when Union branches exist — each branch has its own FROM
        if let Some(last_with_cte) = all_ctes
            .iter()
            .rev()
            .find(|cte| cte.cte_name.starts_with("with_"))
        {
            log::debug!("🔧 build_chained_with_match_cte_plan: FROM clause missing, setting to last CTE: {}", last_with_cte.cte_name);

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

            render_plan.from = FromTableItem(Some(ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: last_with_cte.cte_name.clone(),
                alias: Some(with_alias_part.to_string()),
                use_final: false,
            }));

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Set FROM to: {} AS '{}'",
                last_with_cte.cte_name,
                with_alias_part
            );
        }
    }

    // ==========================================================================
    // CRITICAL FIX: Cross-table WITH pattern - add CTE JOINs
    // ==========================================================================
    // When we have patterns like: WITH a, b MATCH (c)-[]->(d) WHERE a.x = c.x
    // The FROM is set to table 'c', but we need to JOIN the CTE containing 'a', 'b'
    // to make those aliases available for SELECT/WHERE.
    //
    // Detection: FROM is NOT a CTE, but cte_references contains aliases that
    // might be referenced in the query.
    // ==========================================================================
    if let FromTableItem(Some(from_ref)) = &render_plan.from {
        // Check if FROM is NOT a CTE (i.e., it's a regular table from the second MATCH)
        if !from_ref.name.starts_with("with_") && !cte_references.is_empty() {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: FROM '{}' is not a CTE, checking for CTE joins needed",
                from_ref.name
            );
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Available CTE references: {:?}",
                cte_references
            );

            // Collect all CTE aliases that need to be joined
            // Group by CTE name since multiple aliases can come from the same CTE
            let mut cte_join_needed: HashMap<String, Vec<String>> = HashMap::new();
            for (alias, cte_name) in &cte_references {
                cte_join_needed
                    .entry(cte_name.clone())
                    .or_default()
                    .push(alias.clone());
            }

            // For each CTE that's referenced, create a JOIN
            // Sort for deterministic ordering
            let mut sorted_cte_joins: Vec<_> = cte_join_needed.into_iter().collect();
            sorted_cte_joins.sort_by(|a, b| a.0.cmp(&b.0));
            for (cte_name, aliases) in sorted_cte_joins {
                // Extract CTE alias part from name: "with_a_b_cte_1" -> "a_b"
                let cte_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    cte_name.clone()
                };

                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Creating JOIN to CTE '{}' AS '{}' for aliases {:?}",
                    cte_name, cte_alias, aliases
                );

                // Use the correlation predicates that were extracted from the ORIGINAL plan
                // BEFORE transformations (stored in original_correlation_predicates)
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Using {} ORIGINAL correlation predicates",
                    original_correlation_predicates.len()
                );

                // Convert correlation predicates to join conditions using CTE column names
                let mut join_conditions: Vec<OperatorApplication> = Vec::new();

                // If we found correlation predicates, convert them to JOIN ON conditions
                for pred in &original_correlation_predicates {
                    // Convert LogicalExpr predicate to RenderExpr and then extract OperatorApplication
                    if let Ok(RenderExpr::OperatorApplicationExp(op_app)) =
                        RenderExpr::try_from(pred.clone())
                    {
                        // Rewrite the operands to use CTE column names
                        let rewritten = rewrite_operator_application_for_cte_join(
                            &op_app,
                            &cte_alias,
                            &cte_references,
                        );
                        log::debug!(
                            "🔧 build_chained_with_match_cte_plan: Added JOIN condition from correlation predicate: {:?}",
                            rewritten
                        );
                        join_conditions.push(rewritten);
                    }
                }

                // If we have no correlation conditions but have filter predicates, try those
                if join_conditions.is_empty() {
                    if let Some(filter_expr) = &render_plan.filters.0 {
                        log::debug!("🔧 build_chained_with_match_cte_plan: No correlation predicates, checking filters");
                        // Try to extract join conditions from filters
                        if let Some(join_cond) = extract_cte_join_condition_from_filter(
                            filter_expr,
                            &cte_alias,
                            &aliases,
                            &cte_references,
                            &cte_schemas,
                        ) {
                            join_conditions.push(join_cond);
                            log::debug!("🔧 build_chained_with_match_cte_plan: Extracted JOIN condition from filter");
                        }
                    }
                }

                // VLP-specific: when FROM is a VLP CTE, generate join condition
                // connecting the WITH CTE's ID column to the VLP CTE's start_id or end_id
                if join_conditions.is_empty() {
                    if let FromTableItem(Some(from_ref)) = &render_plan.from {
                        if from_ref.name.starts_with("vlp_") {
                            // Find which VLP CTE this is and determine if the alias is start or end
                            for vlp_cte in &render_plan.ctes.0 {
                                if vlp_cte.cte_name == from_ref.name {
                                    // Match when cte_alias equals or starts with the VLP alias
                                    // e.g., cte_alias="a_allNeighboursCount" matches vlp start_alias="a"
                                    let is_start = vlp_cte
                                        .vlp_cypher_start_alias
                                        .as_deref()
                                        .is_some_and(|a| {
                                            cte_alias == a
                                                || cte_alias.starts_with(&format!("{}_", a))
                                        });
                                    let is_end =
                                        vlp_cte.vlp_cypher_end_alias.as_deref().is_some_and(|a| {
                                            cte_alias == a
                                                || cte_alias.starts_with(&format!("{}_", a))
                                        });
                                    if is_start || is_end {
                                        let vlp_id_col =
                                            if is_start { "start_id" } else { "end_id" };
                                        let from_alias = from_ref.alias.as_deref().unwrap_or("t");
                                        // Find the ID column name in the WITH CTE
                                        // Use cte_schemas which has the alias_to_id_column mapping
                                        let vlp_alias = if is_start {
                                            vlp_cte
                                                .vlp_cypher_start_alias
                                                .as_deref()
                                                .unwrap_or(&cte_alias)
                                        } else {
                                            vlp_cte
                                                .vlp_cypher_end_alias
                                                .as_deref()
                                                .unwrap_or(&cte_alias)
                                        };
                                        // Try cte_schemas first: look for {vlp_alias}_{something_id} in SELECT items
                                        let id_col_name = if let Some(meta) =
                                            cte_schemas.get(&cte_name)
                                        {
                                            // First try direct alias_to_id lookup
                                            meta.alias_to_id
                                                .get(vlp_alias)
                                                .cloned()
                                                .or_else(|| {
                                                    // Search SELECT items for {vlp_alias}_*_id pattern
                                                    let prefix = format!("{}_", vlp_alias);
                                                    meta.select_items.iter().find_map(|item| {
                                                        if let Some(col_alias) = &item.col_alias {
                                                            let name = &col_alias.0;
                                                            if name.starts_with(&prefix)
                                                                && (name.ends_with("_id")
                                                                    || name.ends_with("_id"))
                                                            {
                                                                return Some(name.clone());
                                                            }
                                                        }
                                                        None
                                                    })
                                                })
                                                .unwrap_or_else(|| {
                                                    find_id_column_in_cte(
                                                        &cte_name,
                                                        vlp_alias,
                                                        &render_plan.ctes,
                                                    )
                                                })
                                        } else {
                                            find_id_column_in_cte(
                                                &cte_name,
                                                vlp_alias,
                                                &render_plan.ctes,
                                            )
                                        };

                                        // Check if this node has a composite ID — if so, generate
                                        // concat(toString(col1), '|', toString(col2)) to match
                                        // the pipe-joined start_id/end_id in the VLP CTE
                                        let rhs_expr = {
                                            use crate::server::query_context::get_current_schema;
                                            let composite_cols =
                                                get_current_schema().and_then(|schema| {
                                                    // Determine the node label from vlp_alias
                                                    let label = if is_start {
                                                        vlp_cte.vlp_cypher_start_alias.as_deref()
                                                    } else {
                                                        vlp_cte.vlp_cypher_end_alias.as_deref()
                                                    };
                                                    // Look up by label_constraints or try all schemas
                                                    for ns in schema.all_node_schemas().values() {
                                                        if ns.node_id.is_composite() {
                                                            // Check if the id_col_name matches one of this schema's columns
                                                            let prefix = format!(
                                                                "{}_",
                                                                vlp_alias.to_owned()
                                                            );
                                                            let id_cols = ns.node_id.columns();
                                                            let first_cte_col =
                                                                format!("{}{}", prefix, id_cols[0]);
                                                            if id_col_name == first_cte_col
                                                                || id_col_name == id_cols[0]
                                                            {
                                                                return Some(
                                                                    id_cols
                                                                        .iter()
                                                                        .map(|c| c.to_string())
                                                                        .collect::<Vec<_>>(),
                                                                );
                                                            }
                                                        }
                                                    }
                                                    None
                                                });

                                            if let Some(cols) = composite_cols {
                                                // Composite ID: concat(toString(cte.a_col1), '|', toString(cte.a_col2))
                                                let prefix = format!("{}_", vlp_alias);
                                                let parts: Vec<RenderExpr> = cols.iter().enumerate().flat_map(|(i, col)| {
                                                    let cte_col = format!("{}{}", prefix, col);
                                                    let mut items = Vec::new();
                                                    if i > 0 {
                                                        items.push(RenderExpr::Literal(Literal::String("|".to_string())));
                                                    }
                                                    items.push(RenderExpr::ScalarFnCall(ScalarFnCall {
                                                        name: "toString".to_string(),
                                                        args: vec![RenderExpr::Column(Column(
                                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                                format!("{}.{}", cte_alias, cte_col)
                                                            )
                                                        ))],
                                                    }));
                                                    items
                                                }).collect();
                                                log::debug!(
                                                    "🔧 VLP+WITH: Composite ID JOIN - concat {} columns for alias '{}'",
                                                    cols.len(), vlp_alias
                                                );
                                                RenderExpr::ScalarFnCall(ScalarFnCall {
                                                    name: "concat".to_string(),
                                                    args: parts,
                                                })
                                            } else {
                                                // Single ID: toString() to match the String type of
                                                // start_id / end_id stored in the VLP CTE
                                                RenderExpr::ScalarFnCall(ScalarFnCall {
                                                    name: "toString".to_string(),
                                                    args: vec![RenderExpr::Column(Column(
                                                        crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                            format!("{}.{}", cte_alias, id_col_name)
                                                        )
                                                    ))],
                                                })
                                            }
                                        };

                                        // Wrap VLP side in toString() too, ensuring both sides are String.
                                        // VLP start_id/end_id may be UInt64 or String depending on
                                        // generation path, and rhs_expr already uses toString().
                                        let lhs_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                            name: "toString".to_string(),
                                            args: vec![RenderExpr::Column(Column(
                                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                    format!("{}.{}", from_alias, vlp_id_col)
                                                )
                                            ))],
                                        });
                                        let join_cond = OperatorApplication {
                                            operator: Operator::Equal,
                                            operands: vec![lhs_expr, rhs_expr],
                                        };
                                        log::debug!(
                                            "🔧 VLP+WITH: Generated JOIN condition for alias '{}' (is_start={})",
                                            vlp_alias, is_start
                                        );
                                        join_conditions.push(join_cond);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

                // Create the JOIN (use ON 1=1 for scalar CTEs with no correlation conditions)
                let cte_join_conditions = if join_conditions.is_empty() {
                    use crate::render_plan::render_expr::Literal as RenderLiteral;
                    vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::Literal(RenderLiteral::Integer(1)),
                            RenderExpr::Literal(RenderLiteral::Integer(1)),
                        ],
                    }]
                } else {
                    join_conditions.clone()
                };
                let cte_join = super::Join {
                    table_name: cte_name.clone(),
                    table_alias: cte_alias.clone(),
                    joining_on: cte_join_conditions,
                    join_type: super::JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                };

                // Insert the CTE join at the BEGINNING of the joins list
                // (CTE should be joined first so its columns are available)
                // BUT: skip if a JOIN for this CTE alias already exists (from extract_joins)
                // OR if the FROM table already uses this alias (avoid duplicate alias error)
                let already_has_cte_join = render_plan
                    .joins
                    .0
                    .iter()
                    .any(|j| j.table_alias == cte_alias);
                let from_already_uses_alias = render_plan
                    .from
                    .0
                    .as_ref()
                    .map(|vr| vr.alias.as_deref() == Some(&cte_alias))
                    .unwrap_or(false);
                if !already_has_cte_join && !from_already_uses_alias {
                    render_plan.joins.0.insert(0, cte_join.clone());
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Added CTE JOIN: {} AS {}",
                        cte_name,
                        cte_alias
                    );
                } else {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Skipping CTE JOIN {} AS {} (already present from extract_joins)",
                        cte_name,
                        cte_alias
                    );
                }

                // Also add the WITH CTE JOIN to each Union branch
                // The main plan's joins only apply to the first branch (outgoing).
                // Incoming branches in union.input[] need their own JOIN.
                if let Some(ref mut union) = render_plan.union.0 {
                    for branch in union.input.iter_mut() {
                        // Skip if this branch already has the CTE join
                        let branch_already_has =
                            branch.joins.0.iter().any(|j| j.table_alias == cte_alias);
                        if branch_already_has {
                            continue;
                        }

                        if let FromTableItem(Some(ref branch_from)) = branch.from {
                            if branch_from.name.starts_with("vlp_") {
                                // Find the VLP CTE metadata to determine the correct join column
                                let mut branch_join_cond = Vec::new();
                                for vlp_cte in &render_plan.ctes.0 {
                                    if vlp_cte.cte_name == branch_from.name {
                                        let is_start = vlp_cte.vlp_cypher_start_alias.as_deref()
                                            == Some(cte_alias.as_str());
                                        let is_end = vlp_cte.vlp_cypher_end_alias.as_deref()
                                            == Some(cte_alias.as_str());
                                        if is_start || is_end {
                                            let vlp_id_col =
                                                if is_start { "start_id" } else { "end_id" };
                                            let from_alias =
                                                branch_from.alias.as_deref().unwrap_or("t");
                                            let vlp_alias_for_id = if is_start {
                                                vlp_cte
                                                    .vlp_cypher_start_alias
                                                    .as_deref()
                                                    .unwrap_or(&cte_alias)
                                            } else {
                                                vlp_cte
                                                    .vlp_cypher_end_alias
                                                    .as_deref()
                                                    .unwrap_or(&cte_alias)
                                            };
                                            let id_col_name = if let Some(meta) =
                                                cte_schemas.get(&cte_name)
                                            {
                                                meta.alias_to_id
                                                    .get(vlp_alias_for_id)
                                                    .cloned()
                                                    .or_else(|| {
                                                        let prefix =
                                                            format!("{}_", vlp_alias_for_id);
                                                        meta.select_items.iter().find_map(|item| {
                                                            if let Some(col_alias) = &item.col_alias
                                                            {
                                                                let name = &col_alias.0;
                                                                if name.starts_with(&prefix)
                                                                    && name.ends_with("_id")
                                                                {
                                                                    return Some(name.clone());
                                                                }
                                                            }
                                                            None
                                                        })
                                                    })
                                                    .unwrap_or_else(|| {
                                                        find_id_column_in_cte(
                                                            &cte_name,
                                                            vlp_alias_for_id,
                                                            &render_plan.ctes,
                                                        )
                                                    })
                                            } else {
                                                find_id_column_in_cte(
                                                    &cte_name,
                                                    vlp_alias_for_id,
                                                    &render_plan.ctes,
                                                )
                                            };
                                            // Wrap BOTH sides in toString() to handle type mismatches:
                                            // VLP start_id/end_id may be UInt64 or String depending on generation path.
                                            // CTE columns are typically UInt64 (raw IDs). toString() on both sides
                                            // ensures consistent String comparison regardless of input types.
                                            let cond = OperatorApplication {
                                                operator: Operator::Equal,
                                                operands: vec![
                                                    RenderExpr::ScalarFnCall(ScalarFnCall {
                                                        name: "toString".to_string(),
                                                        args: vec![RenderExpr::Column(Column(
                                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                                format!("{}.{}", from_alias, vlp_id_col)
                                                            )
                                                        ))],
                                                    }),
                                                    RenderExpr::ScalarFnCall(ScalarFnCall {
                                                        name: "toString".to_string(),
                                                        args: vec![RenderExpr::Column(Column(
                                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                                format!("{}.{}", cte_alias, id_col_name)
                                                            )
                                                        ))],
                                                    }),
                                                ],
                                            };
                                            log::debug!(
                                                "🔧 VLP+WITH (branch): Generated JOIN for '{}': {}.{} = {}.{}",
                                                branch_from.name, from_alias, vlp_id_col, cte_alias, id_col_name
                                            );
                                            branch_join_cond.push(cond);
                                        }
                                        break;
                                    }
                                }
                                let branch_cte_join = super::Join {
                                    table_name: cte_name.clone(),
                                    table_alias: cte_alias.clone(),
                                    joining_on: branch_join_cond,
                                    join_type: super::JoinType::Inner,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                };
                                branch.joins.0.insert(0, branch_cte_join);
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Added CTE JOIN to Union branch FROM '{}'",
                                    branch_from.name
                                );

                                // Rewrite Union branch SELECT items to use CTE column names
                                // Use scope-based rewriting (replaces removed rewrite_cte_expression)
                                if let Some(ref scope) = scope {
                                    use super::variable_scope::rewrite_render_expr;
                                    for item in branch.select.items.iter_mut() {
                                        rewrite_render_expr(&mut item.expression, scope);
                                    }
                                    log::info!(
                                        "🔧 build_chained_with_match_cte_plan: Rewrote Union branch SELECT via scope for CTE"
                                    );
                                }
                            } else {
                                // Non-VLP branch (regular table FROM): add CTE as cross-join (ON 1=1)
                                // This handles post-WITH MATCH patterns with undirected edges
                                // where UnionDistribution created Union branches with regular table FROM
                                use crate::render_plan::render_expr::Literal as RenderLiteral;
                                let branch_cte_join = super::Join {
                                    table_name: cte_name.clone(),
                                    table_alias: cte_alias.clone(),
                                    joining_on: if join_conditions.is_empty() {
                                        vec![OperatorApplication {
                                            operator: Operator::Equal,
                                            operands: vec![
                                                RenderExpr::Literal(RenderLiteral::Integer(1)),
                                                RenderExpr::Literal(RenderLiteral::Integer(1)),
                                            ],
                                        }]
                                    } else {
                                        join_conditions.clone()
                                    },
                                    join_type: super::JoinType::Inner,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                };
                                branch.joins.0.insert(0, branch_cte_join);
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Added CTE cross-JOIN to non-VLP Union branch FROM '{}'",
                                    branch_from.name
                                );
                            }
                        }
                    }
                }
            }

            // After adding CTE joins, we need to rewrite SELECT items that reference CTE aliases
            // to use the CTE composite alias (e.g., a.name -> a_b.a_name)
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Rewriting SELECT items for CTE references"
            );
        }
    }

    // When FROM is None (Union shell) but CTE references exist, add CTE cross-joins
    // to each Union branch directly. This handles the case where Direct Union rendering
    // moved all branches into union.input (for aggregation/GROUP BY).
    if render_plan.from.0.is_none() && !cte_references.is_empty() {
        if let Some(ref mut union_data) = render_plan.union.0 {
            for (alias, cte_name) in &cte_references {
                let cte_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    cte_name.clone()
                };

                for branch in union_data.input.iter_mut() {
                    let already_has = branch.joins.0.iter().any(|j| j.table_alias == cte_alias);
                    if !already_has {
                        use crate::render_plan::render_expr::Literal as RenderLiteral;
                        let cte_join = super::Join {
                            table_name: cte_name.clone(),
                            table_alias: cte_alias.clone(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::Literal(RenderLiteral::Integer(1)),
                                    RenderExpr::Literal(RenderLiteral::Integer(1)),
                                ],
                            }],
                            join_type: super::JoinType::Inner,
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        };
                        branch.joins.0.insert(0, cte_join);
                        log::info!(
                            "🔧 build_chained_with_match_cte_plan: Added CTE cross-JOIN '{}' AS '{}' to Union branch (FROM=None shell)",
                            cte_name, cte_alias
                        );
                    }
                }
            }
        }
    }

    // Apply bare variable rewriting to the final (outer) render plan.
    // This resolves bare node aliases (e.g., `b` → `b.id`, `a` → `cte.p1_a_id`)
    // in JOIN conditions, WHERE clauses, and SELECT expressions of the outer query.
    // Must run AFTER CTE JOINs are added (above) so JOIN conditions are rewritten too.
    if !scope_cte_variables.is_empty() {
        super::variable_scope::rewrite_bare_variables_in_plan(&mut render_plan, &final_scope);
    }

    // Fix orphan composite aliases in the outer query.
    // After WITH→CTE processing, the outer plan may still reference composite aliases
    // (e.g., "person_score.score") instead of the FROM alias ("person.score").
    // fix_orphan_table_aliases handles all expression types (AggregateFnCall, GROUP BY, etc.)
    // and rewrites them to match the actual FROM/JOIN aliases.
    if !scope_cte_variables.is_empty() {
        super::variable_scope::fix_orphan_table_aliases(&mut render_plan, &final_scope);
    }

    // Add all CTEs (innermost first, which is correct order for SQL)
    all_ctes.extend(render_plan.ctes.0);
    render_plan.ctes = CteItems(all_ctes);

    // Skip validation - CTEs are hoisted progressively through recursion
    // ClickHouse will validate CTE references when executing the SQL
    // Validation here causes false failures when nested calls reference outer CTEs
    // that haven't been hoisted yet but will be present in the final SQL

    // Apply VLP alias rewriting for path functions in WITH clauses
    // This fixes "Unknown expression identifier `t.hop_count`" errors where
    // length(path) was converted to t.hop_count but t needs to be rewritten to the actual VLP alias
    rewrite_vlp_union_branch_aliases(&mut render_plan)?;

    // 🔧 FIX: Rewrite aggregate arguments for VLP end nodes
    // Problem: COUNT(DISTINCT b) where b is VLP end node generates b.end_id
    // But b doesn't exist in SQL - the VLP CTE is joined as "t"
    // Solution: Rewrite b.end_id -> t.end_id using VLP CTE metadata
    rewrite_vlp_aggregate_aliases(&mut render_plan)?;

    // Attach the final variable registry to the outer render plan
    render_plan.variable_registry = Some(std::sync::Arc::new(var_registry));

    log::info!(
        "🔧 build_chained_with_match_cte_plan: Success - final plan has {} CTEs",
        render_plan.ctes.0.len()
    );

    Ok(render_plan)
}
pub(crate) fn build_with_aggregation_match_cte_plan(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<RenderPlan> {
    use super::CteContent;

    log::info!(
        "🔧 build_with_aggregation_match_cte_plan: Starting with plan type {:?}",
        std::mem::discriminant(plan)
    );

    // Step 1: Find the GroupBy (WITH+aggregation) subplan
    let (group_by_plan, with_alias): (&LogicalPlan, String) = find_group_by_subplan(plan)
        .ok_or_else(|| {
            RenderBuildError::InvalidRenderPlan(
                "WITH+aggregation+MATCH: Could not find GroupBy subplan".to_string(),
            )
        })?;

    log::info!(
        "🔧 build_with_aggregation_match_cte_plan: Found GroupBy for alias '{}'",
        with_alias
    );

    // Step 2: Collect aliases that are part of the inner scope (the first MATCH before WITH)
    // These are the aliases that should be in the CTE
    let inner_aliases =
        collect_inner_scope_aliases(group_by_plan, &std::collections::HashSet::new());
    log::info!(
        "🔧 build_with_aggregation_match_cte_plan: Inner scope aliases = {:?}",
        inner_aliases
    );

    // Step 3: Render the GroupBy subplan as a CTE
    let mut group_by_render = group_by_plan.to_render_plan(schema)?;

    // Note: GROUP BY optimization (reducing to ID-only) is now done in extract_group_by()
    // This happens automatically during to_render_plan() call above.

    // Step 3.5: Post-process SELECT items to fix `*` wildcards
    // The analyzer generates PropertyAccessExp(alias, "*") for WITH alias references
    // but `f.*` in SQL expands to ALL columns, which may not all be in GROUP BY.
    // Instead, we should replace `f.*` with the explicit GROUP BY columns.
    {
        // Collect GROUP BY column expressions for the WITH alias
        let group_by_columns: Vec<RenderExpr> = group_by_render
            .group_by
            .0
            .iter()
            .filter(|expr| {
                if let RenderExpr::PropertyAccessExp(pa) = expr {
                    pa.table_alias.0 == with_alias
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        // Use helper function to replace wildcards and expand TableAlias
        group_by_render.select.items = replace_wildcards_with_group_by_columns(
            group_by_render.select.items,
            &group_by_columns,
            &with_alias,
        );
    }

    // Step 4: Post-process: Remove joins that are NOT in the inner scope
    // The GraphJoinInference analyzer creates joins for the entire query,
    // but the CTE should only have joins for the first MATCH pattern
    {
        let original_join_count = group_by_render.joins.0.len();
        group_by_render.joins.0.retain(|join| {
            let alias = &join.table_alias;
            let keep = inner_aliases.contains(alias);
            log::debug!("🔧 CTE join filter: alias='{}' -> keep={}", alias, keep);
            keep
        });
        log::info!(
            "🔧 build_with_aggregation_match_cte_plan: Filtered CTE joins from {} to {}",
            original_join_count,
            group_by_render.joins.0.len()
        );
    }

    // Generate unique CTE name
    let cte_name = format!(
        "with_agg_{}_{}",
        with_alias,
        crate::query_planner::logical_plan::generate_cte_id()
    );

    log::info!(
        "🔧 build_with_aggregation_match_cte_plan: Created CTE '{}' with {} select items",
        cte_name,
        group_by_render.select.items.len()
    );

    // Step 5: Create CTE from the GroupBy render plan
    let group_by_cte = Cte::new(
        cte_name.clone(),
        CteContent::Structured(Box::new(group_by_render)),
        false,
    );

    // Step 6: Transform the plan by replacing the GroupBy with a CTE reference
    let transformed_plan = replace_group_by_with_cte_reference(plan, &with_alias, &cte_name)?;

    log::debug!("🔧 build_with_aggregation_match_cte_plan: Transformed plan to use CTE reference");

    // Step 7: Render the transformed outer query
    let mut render_plan = transformed_plan.to_render_plan(schema)?;

    // Step 8: Post-process outer query: Fix the FROM table to use CTE and fix join references
    // The outer query's FROM should be the CTE, and joins should be for the outer MATCH pattern only
    {
        // Change the FROM table to be the CTE
        if let Some(ref mut table_ref) = render_plan.from.0 {
            log::info!(
                "🔧 Changing FROM table from '{}' to CTE '{}'",
                table_ref.name,
                cte_name
            );
            table_ref.name = cte_name.clone();
            table_ref.alias = Some(with_alias.clone());
        }

        // Fix joins: remove inner scope joins and fix references to the CTE alias
        let inner_join_aliases: std::collections::HashSet<_> = inner_aliases
            .iter()
            .filter(|a| *a != &with_alias) // Don't exclude the with_alias itself
            .cloned()
            .collect();

        // Remove joins for inner scope aliases (they're now in the CTE)
        render_plan.joins.0.retain(|join| {
            let keep = !inner_join_aliases.contains(&join.table_alias);
            log::info!(
                "🔧 Outer join filter: alias='{}' -> keep={}",
                join.table_alias,
                keep
            );
            keep
        });

        // Also filter out joins where the WITH alias (f) references internal tables that no longer exist
        // These joins reference t1.Person2Id which doesn't exist in the outer query
        render_plan.joins.0.retain(|join| {
            // If this join references an alias from the inner scope in its ON condition,
            // and that alias isn't the WITH alias (which now comes from CTE), remove it
            let references_inner = join.joining_on.iter().any(|cond| {
                inner_join_aliases
                    .iter()
                    .any(|alias| operator_references_alias(cond, alias))
            });
            if references_inner && join.table_alias == with_alias {
                log::info!(
                    "🔧 Removing duplicate JOIN for WITH alias '{}' (already from CTE)",
                    join.table_alias
                );
                return false;
            }
            true
        });
    }

    // Step 9: Prepend the GroupBy CTE
    let mut all_ctes = vec![group_by_cte];
    all_ctes.extend(render_plan.ctes.0);
    render_plan.ctes = CteItems(all_ctes);

    log::info!(
        "🔧 build_with_aggregation_match_cte_plan: Success - final plan has {} CTEs",
        render_plan.ctes.0.len()
    );

    Ok(render_plan)
}

/// Replace a GroupBy subplan with a CTE reference (ViewScan pointing to CTE).
pub(crate) fn replace_group_by_with_cte_reference(
    plan: &LogicalPlan,
    with_alias: &str,
    cte_name: &str,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::{GraphNode, ViewScan};

    fn replace_recursive(
        plan: &LogicalPlan,
        with_alias: &str,
        cte_name: &str,
    ) -> RenderPlanBuilderResult<LogicalPlan> {
        use std::sync::Arc;

        match plan {
            LogicalPlan::Limit(limit) => {
                let new_input = replace_recursive(&limit.input, with_alias, cte_name)?;
                Ok(LogicalPlan::Limit(
                    crate::query_planner::logical_plan::Limit {
                        input: Arc::new(new_input),
                        count: limit.count,
                    },
                ))
            }
            LogicalPlan::OrderBy(order_by) => {
                let new_input = replace_recursive(&order_by.input, with_alias, cte_name)?;
                Ok(LogicalPlan::OrderBy(
                    crate::query_planner::logical_plan::OrderBy {
                        input: Arc::new(new_input),
                        items: order_by.items.clone(),
                    },
                ))
            }
            LogicalPlan::Skip(skip) => {
                let new_input = replace_recursive(&skip.input, with_alias, cte_name)?;
                Ok(LogicalPlan::Skip(
                    crate::query_planner::logical_plan::Skip {
                        input: Arc::new(new_input),
                        count: skip.count,
                    },
                ))
            }
            LogicalPlan::GraphJoins(gj) => {
                let new_input = replace_recursive(&gj.input, with_alias, cte_name)?;
                // Filter out joins that are for the inner subplan
                // Keep only joins for the outer MATCH pattern
                let outer_joins: Vec<_> = gj
                    .joins
                    .iter()
                    .filter(|j| !is_join_for_inner_scope(&gj.input, j, with_alias))
                    .cloned()
                    .collect();

                log::debug!("🔧 replace_group_by_with_cte_reference: Filtered joins from {} to {} (outer only)",
                    gj.joins.len(), outer_joins.len());

                Ok(LogicalPlan::GraphJoins(
                    crate::query_planner::logical_plan::GraphJoins {
                        input: Arc::new(new_input),
                        joins: outer_joins,
                        optional_aliases: gj.optional_aliases.clone(),
                        anchor_table: gj.anchor_table.clone(),
                        cte_references: gj.cte_references.clone(),
                        correlation_predicates: gj.correlation_predicates.clone(),
                    },
                ))
            }
            LogicalPlan::Projection(proj) => {
                let new_input = replace_recursive(&proj.input, with_alias, cte_name)?;
                Ok(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: Arc::new(new_input),
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                        pattern_comprehensions: proj.pattern_comprehensions.clone(),
                    },
                ))
            }
            LogicalPlan::Filter(f) => {
                let new_input = replace_recursive(&f.input, with_alias, cte_name)?;
                Ok(LogicalPlan::Filter(
                    crate::query_planner::logical_plan::Filter {
                        input: Arc::new(new_input),
                        predicate: f.predicate.clone(),
                    },
                ))
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if GroupBy is in .left (common after boundary separation)
                if let LogicalPlan::GroupBy(gb) = graph_rel.left.as_ref() {
                    if gb.is_materialization_boundary
                        && gb.exposed_alias.as_deref() == Some(with_alias)
                    {
                        log::debug!("🔧 replace_group_by_with_cte_reference: Replacing GroupBy in .left with CTE reference for alias '{}'", with_alias);

                        // Create a ViewScan pointing to the CTE
                        let cte_view_scan = ViewScan {
                            source_table: cte_name.to_string(),
                            view_filter: None,
                            property_mapping: std::collections::HashMap::new(),
                            id_column: "id".to_string(),
                            output_schema: vec!["id".to_string()],
                            projections: vec![],
                            from_id: None,
                            to_id: None,
                            input: None,
                            view_parameter_names: None,
                            view_parameter_values: None,
                            use_final: false,
                            is_denormalized: false,
                            from_node_properties: None,
                            to_node_properties: None,
                            type_column: None,
                            type_values: None,
                            from_label_column: None,
                            to_label_column: None,
                            schema_filter: None,
                            node_label: None,
                        };

                        let cte_graph_node = LogicalPlan::GraphNode(GraphNode {
                            input: Arc::new(LogicalPlan::ViewScan(Arc::new(cte_view_scan))),
                            alias: with_alias.to_string(),
                            label: None, // CTE doesn't have a label
                            is_denormalized: false,
                            projected_columns: None,
                            node_types: None,
                        });

                        // Create new GraphRel with CTE reference as .left
                        let new_right = replace_recursive(&graph_rel.right, with_alias, cte_name)?;

                        return Ok(LogicalPlan::GraphRel(
                            crate::query_planner::logical_plan::GraphRel {
                                left: Arc::new(cte_graph_node),
                                center: graph_rel.center.clone(),
                                right: Arc::new(new_right),
                                alias: graph_rel.alias.clone(),
                                direction: graph_rel.direction.clone(),
                                left_connection: graph_rel.left_connection.clone(),
                                right_connection: graph_rel.right_connection.clone(),
                                is_rel_anchor: graph_rel.is_rel_anchor,
                                variable_length: graph_rel.variable_length.clone(),
                                shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                                path_variable: graph_rel.path_variable.clone(),
                                where_predicate: graph_rel.where_predicate.clone(),
                                labels: graph_rel.labels.clone(),
                                is_optional: graph_rel.is_optional,
                                anchor_connection: graph_rel.anchor_connection.clone(),
                                cte_references: graph_rel.cte_references.clone(),
                                pattern_combinations: None,
                                was_undirected: graph_rel.was_undirected,
                            },
                        ));
                    }
                }

                // Check if GroupBy is in .right (legacy structure)
                if let LogicalPlan::GroupBy(gb) = graph_rel.right.as_ref() {
                    if gb.is_materialization_boundary
                        && gb.exposed_alias.as_deref() == Some(with_alias)
                    {
                        log::debug!("🔧 replace_group_by_with_cte_reference: Replacing GroupBy in .right with CTE reference for alias '{}'", with_alias);

                        // Create a ViewScan pointing to the CTE
                        let cte_view_scan = ViewScan {
                            source_table: cte_name.to_string(),
                            view_filter: None,
                            property_mapping: std::collections::HashMap::new(),
                            id_column: "id".to_string(),
                            output_schema: vec!["id".to_string()],
                            projections: vec![],
                            from_id: None,
                            to_id: None,
                            input: None,
                            view_parameter_names: None,
                            view_parameter_values: None,
                            use_final: false,
                            is_denormalized: false,
                            from_node_properties: None,
                            to_node_properties: None,
                            type_column: None,
                            type_values: None,
                            from_label_column: None,
                            to_label_column: None,
                            schema_filter: None,
                            node_label: None,
                        };

                        let cte_graph_node = LogicalPlan::GraphNode(GraphNode {
                            input: Arc::new(LogicalPlan::ViewScan(Arc::new(cte_view_scan))),
                            alias: with_alias.to_string(),
                            label: None, // CTE doesn't have a label
                            is_denormalized: false,
                            projected_columns: None,
                            node_types: None,
                        });

                        // Create new GraphRel with CTE reference as .right
                        let new_left = replace_recursive(&graph_rel.left, with_alias, cte_name)?;

                        return Ok(LogicalPlan::GraphRel(
                            crate::query_planner::logical_plan::GraphRel {
                                left: Arc::new(new_left),
                                center: graph_rel.center.clone(),
                                right: Arc::new(cte_graph_node),
                                alias: graph_rel.alias.clone(),
                                direction: graph_rel.direction.clone(),
                                left_connection: graph_rel.left_connection.clone(),
                                right_connection: graph_rel.right_connection.clone(),
                                is_rel_anchor: graph_rel.is_rel_anchor,
                                variable_length: graph_rel.variable_length.clone(),
                                shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                                path_variable: graph_rel.path_variable.clone(),
                                where_predicate: graph_rel.where_predicate.clone(),
                                labels: graph_rel.labels.clone(),
                                is_optional: graph_rel.is_optional,
                                anchor_connection: graph_rel.anchor_connection.clone(),
                                cte_references: graph_rel.cte_references.clone(),
                                pattern_combinations: None,
                                was_undirected: graph_rel.was_undirected,
                            },
                        ));
                    }
                }

                // Recurse into both branches
                let new_left = replace_recursive(&graph_rel.left, with_alias, cte_name)?;
                let new_right = replace_recursive(&graph_rel.right, with_alias, cte_name)?;

                Ok(LogicalPlan::GraphRel(
                    crate::query_planner::logical_plan::GraphRel {
                        left: Arc::new(new_left),
                        center: graph_rel.center.clone(),
                        right: Arc::new(new_right),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone(),
                        left_connection: graph_rel.left_connection.clone(),
                        right_connection: graph_rel.right_connection.clone(),
                        is_rel_anchor: graph_rel.is_rel_anchor,
                        variable_length: graph_rel.variable_length.clone(),
                        shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                        path_variable: graph_rel.path_variable.clone(),
                        where_predicate: graph_rel.where_predicate.clone(),
                        labels: graph_rel.labels.clone(),
                        is_optional: graph_rel.is_optional,
                        anchor_connection: graph_rel.anchor_connection.clone(),
                        cte_references: graph_rel.cte_references.clone(),
                        pattern_combinations: None,
                        was_undirected: graph_rel.was_undirected,
                    },
                ))
            }
            // Other plan types pass through unchanged
            other => Ok(other.clone()),
        }
    }

    replace_recursive(plan, with_alias, cte_name)
}

/// Find the INNERMOST WITH clause subplan in a nested plan structure.
///
/// KEY INSIGHT: With chained WITH clauses (e.g., WITH a MATCH...WITH a,b MATCH...),
/// we need to process them from innermost to outermost. The innermost WITH is
/// the one whose INPUT has NO other WITH clauses nested inside it.
///
/// This function recursively searches for WITH clauses and returns the one
/// whose input is "clean" (contains no nested WITH).
///
/// Returns (with_clause_plan, alias_name) if found.
///
/// Find all WITH clauses in a plan grouped by their alias.
///
/// Returns HashMap where each alias maps to all WITH clause plans with that alias.
/// This handles the case where Union branches each have their own WITH clause with the same alias.
/// Returns owned (cloned) LogicalPlans to avoid lifetime issues with mutations.
pub(crate) fn find_all_with_clauses_grouped(
    plan: &LogicalPlan,
) -> std::collections::HashMap<String, Vec<LogicalPlan>> {
    log::debug!(
        "🔍 find_all_with_clauses_grouped: Called with plan type: {:?}",
        std::mem::discriminant(plan)
    );
    use crate::query_planner::logical_expr::LogicalExpr;
    use crate::query_planner::logical_plan::ProjectionItem;
    use std::collections::HashMap;

    /// Extract the alias from a WITH projection item.
    /// Priority: explicit col_alias > inferred from expression (variable name, table alias)
    /// Note: Strips ".*" suffix from col_alias (e.g., "friend.*" -> "friend")
    fn extract_with_alias(item: &ProjectionItem) -> Option<String> {
        // First check for explicit alias
        if let Some(ref alias) = item.col_alias {
            // Strip ".*" suffix if present (added by projection_tagging.rs for node expansions)
            let clean_alias = alias.0.strip_suffix(".*").unwrap_or(&alias.0).to_string();
            log::info!(
                "🔍 extract_with_alias: Found explicit col_alias: {} -> {}",
                alias.0,
                clean_alias
            );
            return Some(clean_alias);
        }

        // Helper to extract alias from nested expression
        fn extract_alias_from_expr(expr: &LogicalExpr) -> Option<String> {
            match expr {
                LogicalExpr::ColumnAlias(ca) => {
                    log::debug!("🔍 extract_with_alias: ColumnAlias: {}", ca.0);
                    Some(ca.0.clone())
                }
                LogicalExpr::TableAlias(ta) => {
                    log::debug!("🔍 extract_with_alias: TableAlias: {}", ta.0);
                    Some(ta.0.clone())
                }
                LogicalExpr::Column(col) => {
                    // A bare column name - this is often the variable name in WITH
                    // e.g., WITH friend -> Column("friend")
                    // Skip "*" since it's not a real variable name
                    if col.0 == "*" {
                        log::debug!("🔍 extract_with_alias: Skipping Column('*')");
                        None
                    } else {
                        log::debug!("🔍 extract_with_alias: Column: {}", col.0);
                        Some(col.0.clone())
                    }
                }
                LogicalExpr::PropertyAccessExp(pa) => {
                    // For property access like `friend.name`, use the table alias
                    log::info!(
                        "🔍 extract_with_alias: PropertyAccessExp: {}.{:?}",
                        pa.table_alias.0,
                        pa.column
                    );
                    Some(pa.table_alias.0.clone())
                }
                LogicalExpr::OperatorApplicationExp(op_app) => {
                    // Handle operators like DISTINCT that wrap other expressions
                    // Try to extract alias from the first operand
                    log::debug!("🔍 extract_with_alias: OperatorApplicationExp with {:?}, checking operands", op_app.operator);
                    for operand in &op_app.operands {
                        if let Some(alias) = extract_alias_from_expr(operand) {
                            return Some(alias);
                        }
                    }
                    None
                }
                other => {
                    log::info!(
                        "🔍 extract_with_alias: Unhandled expression type in nested: {:?}",
                        std::mem::discriminant(other)
                    );
                    None
                }
            }
        }

        // Try to infer from expression
        log::info!(
            "🔍 extract_with_alias: Expression type: {:?}",
            std::mem::discriminant(&item.expression)
        );
        extract_alias_from_expr(&item.expression)
    }

    /// Generate a unique key for a WITH clause based on all its projection items.
    /// This allows distinguishing "WITH friend" from "WITH friend, post".
    /// Generate a unique key for a WithClause based on its exported aliases or projection items.
    fn generate_with_key_from_with_clause(
        wc: &crate::query_planner::logical_plan::WithClause,
    ) -> String {
        // First try exported_aliases (preferred, already computed)
        if !wc.exported_aliases.is_empty() {
            let mut aliases = wc.exported_aliases.clone();
            aliases.sort();
            return aliases.join("_");
        }
        // Fall back to extracting from items
        let mut aliases: Vec<String> = wc
            .items
            .iter()
            .filter_map(extract_with_alias)
            .filter(|a| a != "*")
            .collect();
        aliases.sort();
        if aliases.is_empty() {
            "with_var".to_string()
        } else {
            aliases.join("_")
        }
    }

    /// Find the first WITH clause key in a plan subtree (non-recursive into Union)
    fn find_first_with_key(plan: &LogicalPlan) -> Option<String> {
        log::debug!(
            "🔍 find_first_with_key: plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            // NEW: Handle WithClause type
            LogicalPlan::WithClause(wc) => Some(generate_with_key_from_with_clause(wc)),
            LogicalPlan::GraphRel(graph_rel) => {
                // Check for WithClause in right
                if let LogicalPlan::WithClause(wc) = graph_rel.right.as_ref() {
                    return Some(generate_with_key_from_with_clause(wc));
                }
                // Check for WithClause in left
                if let LogicalPlan::WithClause(wc) = graph_rel.left.as_ref() {
                    return Some(generate_with_key_from_with_clause(wc));
                }
                if let LogicalPlan::GraphJoins(gj) = graph_rel.right.as_ref() {
                    if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                        return Some(generate_with_key_from_with_clause(wc));
                    }
                }
                None
            }
            LogicalPlan::GraphJoins(gj) => find_first_with_key(&gj.input),
            LogicalPlan::Projection(p) => find_first_with_key(&p.input),
            LogicalPlan::Filter(f) => find_first_with_key(&f.input),
            _ => None,
        }
    }

    fn find_all_with_clauses_impl(plan: &LogicalPlan, results: &mut Vec<(LogicalPlan, String)>) {
        log::debug!(
            "🔍 find_all_with_clauses_impl: Checking plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            // NEW: Handle WithClause type directly
            LogicalPlan::WithClause(wc) => {
                let alias = generate_with_key_from_with_clause(wc);
                log::debug!(
                    "🔍 find_all_with_clauses_impl: Found WithClause directly, key='{}'",
                    alias
                );
                results.push((plan.clone(), alias));
                // Recurse into input to find nested WITH clauses
                // They will be processed innermost-first due to sorting by underscore count
                find_all_with_clauses_impl(&wc.input, results);
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::debug!(
                    "🔍 find_all_with_clauses_impl: GraphRel - right type: {:?}, left type: {:?}",
                    std::mem::discriminant(graph_rel.right.as_ref()),
                    std::mem::discriminant(graph_rel.left.as_ref())
                );

                // Track which branches we've already recursed into to avoid duplicates
                let mut handled_right = false;
                let mut handled_left = false;

                // Check for WithClause in right
                if let LogicalPlan::WithClause(wc) = graph_rel.right.as_ref() {
                    let key = generate_with_key_from_with_clause(wc);
                    let alias = if key == "with_var" {
                        graph_rel.right_connection.clone()
                    } else {
                        key
                    };
                    log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphRel.right, key='{}' (connection='{}')",
                               alias, graph_rel.right_connection);
                    results.push((graph_rel.right.as_ref().clone(), alias));
                    find_all_with_clauses_impl(&wc.input, results);
                    handled_right = true;
                }
                // Check for WithClause in left
                if let LogicalPlan::WithClause(wc) = graph_rel.left.as_ref() {
                    let key = generate_with_key_from_with_clause(wc);
                    let alias = if key == "with_var" {
                        graph_rel.left_connection.clone()
                    } else {
                        key
                    };
                    log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphRel.left, key='{}' (connection='{}')",
                               alias, graph_rel.left_connection);
                    results.push((graph_rel.left.as_ref().clone(), alias));
                    find_all_with_clauses_impl(&wc.input, results);
                    handled_left = true;
                }
                // Also check GraphJoins wrapped inside GraphRel
                if !handled_right {
                    if let LogicalPlan::GraphJoins(gj) = graph_rel.right.as_ref() {
                        if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                            let key = generate_with_key_from_with_clause(wc);
                            let alias = if key == "with_var" {
                                graph_rel.right_connection.clone()
                            } else {
                                key
                            };
                            log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphJoins inside GraphRel.right, key='{}' (connection='{}')",
                                       alias, graph_rel.right_connection);
                            results.push((gj.input.as_ref().clone(), alias));
                            find_all_with_clauses_impl(&wc.input, results);
                            handled_right = true;
                        }
                    }
                }
                if !handled_left {
                    if let LogicalPlan::GraphJoins(gj) = graph_rel.left.as_ref() {
                        if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                            let key = generate_with_key_from_with_clause(wc);
                            let alias = if key == "with_var" {
                                graph_rel.left_connection.clone()
                            } else {
                                key
                            };
                            log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphJoins inside GraphRel.left, key='{}' (connection='{}')",
                                       alias, graph_rel.left_connection);
                            results.push((gj.input.as_ref().clone(), alias));
                            find_all_with_clauses_impl(&wc.input, results);
                            handled_left = true;
                        }
                    }
                }

                // Continue traversal on branches not already handled
                if !handled_left {
                    find_all_with_clauses_impl(&graph_rel.left, results);
                }
                find_all_with_clauses_impl(&graph_rel.center, results);
                if !handled_right {
                    find_all_with_clauses_impl(&graph_rel.right, results);
                }
            }
            LogicalPlan::Projection(proj) => {
                find_all_with_clauses_impl(&proj.input, results);
            }
            LogicalPlan::Filter(filter) => find_all_with_clauses_impl(&filter.input, results),
            LogicalPlan::GroupBy(group_by) => find_all_with_clauses_impl(&group_by.input, results),
            LogicalPlan::GraphJoins(graph_joins) => {
                find_all_with_clauses_impl(&graph_joins.input, results)
            }
            LogicalPlan::Limit(limit) => find_all_with_clauses_impl(&limit.input, results),
            LogicalPlan::OrderBy(order_by) => find_all_with_clauses_impl(&order_by.input, results),
            LogicalPlan::Skip(skip) => find_all_with_clauses_impl(&skip.input, results),
            LogicalPlan::Union(union) => {
                // For Union (bidirectional patterns), check if WITH clauses exist inside.
                // If so, the entire Union should be treated as a single WITH-bearing structure,
                // not collected multiple times from each branch.
                //
                // Strategy: Check if all branches have matching WITH clauses (same key).
                // If yes, collect the WITH key but note that the Union itself needs to be rendered.
                // If branches have different WITH structures, recurse into each.

                let mut branch_with_keys: Vec<Option<String>> = Vec::new();
                for (i, input) in union.inputs.iter().enumerate() {
                    log::debug!(
                        "🔍 find_all_with_clauses_impl: Union branch {} plan type: {:?}",
                        i,
                        std::mem::discriminant(input.as_ref())
                    );
                    // Find the first Projection(With) in this branch
                    if let Some(key) = find_first_with_key(input) {
                        branch_with_keys.push(Some(key));
                    } else {
                        branch_with_keys.push(None);
                    }
                }

                // Check if all branches have the same WITH key
                let first_key = branch_with_keys.first().and_then(|k| k.clone());
                let all_same = branch_with_keys.iter().all(|k| k == &first_key);

                if all_same {
                    if let Some(key) = first_key.as_ref() {
                        // All branches have the same WITH key - this is a bidirectional pattern
                        // Collect from just the first branch to avoid duplicates
                        // The Union structure will be preserved when we render the parent GraphRel
                        log::debug!("🔍 find_all_with_clauses_impl: Union has matching WITH key '{}' in all branches, collecting from first only", key);
                        if let Some(first_input) = union.inputs.first() {
                            find_all_with_clauses_impl(first_input, results);
                        }
                    } else {
                        // All branches have None key — WITH clauses may be deeper in the tree
                        // Recurse into the first branch to find them
                        log::debug!("🔍 find_all_with_clauses_impl: Union branches have no top-level WITH key, recursing into first branch");
                        if let Some(first_input) = union.inputs.first() {
                            find_all_with_clauses_impl(first_input, results);
                        }
                    }
                } else {
                    // Branches have different WITH structures - recurse into each
                    for input in &union.inputs {
                        find_all_with_clauses_impl(input, results);
                    }
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                // CartesianProduct is used for WITH...MATCH patterns where aliases don't overlap
                // Check both sides for WITH clauses
                log::info!(
                    "🔍 find_all_with_clauses_impl: Checking CartesianProduct left and right"
                );
                find_all_with_clauses_impl(&cp.left, results);
                find_all_with_clauses_impl(&cp.right, results);
            }
            LogicalPlan::ViewScan(vs) => {
                if let Some(input) = &vs.input {
                    find_all_with_clauses_impl(input, results);
                }
            }
            LogicalPlan::GraphNode(gn) => find_all_with_clauses_impl(&gn.input, results),
            LogicalPlan::Cte(c) => find_all_with_clauses_impl(&c.input, results),
            LogicalPlan::Unwind(u) => find_all_with_clauses_impl(&u.input, results),
            _ => {}
        }
    }

    let mut all_withs: Vec<(LogicalPlan, String)> = Vec::new();
    find_all_with_clauses_impl(plan, &mut all_withs);

    // Group by alias
    let mut grouped: HashMap<String, Vec<LogicalPlan>> = HashMap::new();
    for (plan, alias) in all_withs {
        grouped.entry(alias).or_default().push(plan);
    }

    grouped
}

/// Prune joins from GraphJoins that are already covered by a CTE.
///
/// Collapse a passthrough WITH clause by replacing it with its input.
/// A passthrough WITH is one that simply wraps a CTE reference without any transformations:
/// - Single item that's just a TableAlias
/// - No DISTINCT, ORDER BY, SKIP, LIMIT, WHERE
///
/// This function finds the passthrough WITH for the given alias and replaces it with its input.
/// Uses the analyzer's CTE name to distinguish between multiple consecutive WITHs with same alias.
pub(crate) fn collapse_passthrough_with(
    plan: &LogicalPlan,
    target_alias: &str,
    target_cte_name: &str, // Analyzer's CTE name (e.g., "with_lnm_cte_4")
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    log::debug!(
        "🔧 collapse_passthrough_with: ENTERING with plan type {:?}, target_alias='{}', target_cte_name='{}'",
        std::mem::discriminant(plan), target_alias, target_cte_name
    );

    /// Generate a key for a WithClause (same logic as find_all_with_clauses_grouped)
    fn get_with_key(wc: &WithClause) -> String {
        if !wc.exported_aliases.is_empty() {
            let mut aliases = wc.exported_aliases.clone();
            aliases.sort();
            return aliases.join("_");
        }
        "with_var".to_string()
    }

    match plan {
        LogicalPlan::WithClause(wc) => {
            let key = get_with_key(wc);
            let this_cte_name = wc
                .cte_references
                .get(target_alias)
                .map(|s| s.as_str())
                .unwrap_or("");
            log::debug!(
                "🔧 collapse_passthrough_with: ENTERING WithClause match, wc.cte_references={:?}, exported_aliases={:?}",
                wc.cte_references, wc.exported_aliases
            );
            log::debug!(
                "🔧 collapse_passthrough_with: Checking WithClause key='{}' target='{}' this_cte='{}' target_cte='{}'",
                key, target_alias, this_cte_name, target_cte_name
            );
            if key == target_alias {
                // FORCE COLLAPSE for passthrough WITHs
                log::debug!(
                    "🔧 collapse_passthrough_with: FORCE COLLAPSING WithClause key='{}' target='{}'",
                    key, target_alias
                );
                Ok(wc.input.as_ref().clone())
            } else {
                // Not the target - recurse into input
                let new_input =
                    collapse_passthrough_with(&wc.input, target_alias, target_cte_name)?;
                Ok(LogicalPlan::WithClause(WithClause {
                    cte_name: None,
                    input: Arc::new(new_input),
                    items: wc.items.clone(),
                    order_by: wc.order_by.clone(),
                    skip: wc.skip,
                    limit: wc.limit,
                    where_clause: wc.where_clause.clone(),
                    distinct: wc.distinct,
                    exported_aliases: wc.exported_aliases.clone(),
                    cte_references: wc.cte_references.clone(),
                    pattern_comprehensions: wc.pattern_comprehensions.clone(),
                }))
            }
        }
        LogicalPlan::Projection(proj) => {
            let new_input = collapse_passthrough_with(&proj.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: proj.items.clone(),
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::Filter(f) => {
            let new_input = collapse_passthrough_with(&f.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: f.predicate.clone(),
            }))
        }
        LogicalPlan::Limit(lim) => {
            let new_input = collapse_passthrough_with(&lim.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: lim.count,
            }))
        }
        LogicalPlan::GraphJoins(gj) => {
            let new_input = collapse_passthrough_with(&gj.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: gj.joins.clone(),
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: gj.anchor_table.clone(),
                cte_references: gj.cte_references.clone(),
                correlation_predicates: gj.correlation_predicates.clone(),
            }))
        }
        LogicalPlan::Skip(skip) => {
            let new_input = collapse_passthrough_with(&skip.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }
        LogicalPlan::OrderBy(ob) => {
            let new_input = collapse_passthrough_with(&ob.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: ob.items.clone(),
            }))
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input = collapse_passthrough_with(&gb.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: gb.expressions.clone(),
                having_clause: gb.having_clause.clone(),
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            }))
        }
        // For other node types that don't contain WITH clauses, return unchanged
        other => Ok(other.clone()),
    }
}

/// When we have a query like:
///   WITH a MATCH (a)-[:F]->(b) WITH a,b MATCH (b)-[:F]->(c)
///
/// After processing, we have:
/// - CTE: with_a_b_cte2 (contains the pattern for a→b)
/// - Final plan: GraphJoins with joins for [a→t1→b, b→t2→c]
///
/// The joins [a→t1→b] are already materialized in the CTE, so they should be removed.
/// Only [b→t2→c] should remain in the final query.
///
/// This function:
/// 1. Traverses the plan to find GraphJoins nodes
/// 2. Builds an adjacency graph from join ON conditions (alias connectivity)
/// 3. Seeds the removable set with CTE-backed aliases (exported_aliases)
/// 4. Fixed-point expansion: non-CTE joins are removable if ALL neighbors are removable
/// 5. Keeps joins whose alias is NOT in the removable set
pub(crate) fn prune_joins_covered_by_cte(
    plan: &LogicalPlan,
    cte_name: &str,
    exported_aliases: &std::collections::HashSet<&str>,
    _cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    log::info!(
        "🔧 prune_joins_covered_by_cte: Processing plan for CTE '{}' with aliases {:?}",
        cte_name,
        exported_aliases
    );

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::info!(
                "🔧 prune_joins_covered_by_cte: Found GraphJoins with {} joins and anchor '{:?}'",
                gj.joins.len(),
                gj.anchor_table
            );

            // Build adjacency graph from join ON conditions, then use fixed-point
            // expansion to find all joins fully internal to the CTE subgraph.

            // Helper: extract table aliases from join condition operands
            fn extract_condition_aliases(
                operands: &[crate::query_planner::logical_expr::LogicalExpr],
                aliases: &mut std::collections::HashSet<String>,
            ) {
                for operand in operands {
                    match operand {
                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) => {
                            aliases.insert(pa.table_alias.0.clone());
                        }
                        crate::query_planner::logical_expr::LogicalExpr::OperatorApplicationExp(
                            nested,
                        ) => {
                            extract_condition_aliases(&nested.operands, aliases);
                        }
                        _ => {}
                    }
                }
            }

            // 1. Build adjacency graph from join conditions
            let mut adjacency: std::collections::HashMap<
                String,
                std::collections::HashSet<String>,
            > = std::collections::HashMap::new();

            // Register all join aliases and anchor
            if let Some(ref anchor) = gj.anchor_table {
                adjacency
                    .entry(anchor.clone())
                    .or_insert_with(std::collections::HashSet::new);
            }
            for join in &gj.joins {
                adjacency
                    .entry(join.table_alias.clone())
                    .or_insert_with(std::collections::HashSet::new);
            }

            // Add edges from join conditions
            for join in &gj.joins {
                let mut condition_aliases = std::collections::HashSet::new();
                for op in &join.joining_on {
                    extract_condition_aliases(&op.operands, &mut condition_aliases);
                }
                // Add bidirectional edges between join alias and all aliases in its conditions
                for alias in &condition_aliases {
                    if alias != &join.table_alias {
                        adjacency
                            .entry(join.table_alias.clone())
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(alias.clone());
                        adjacency
                            .entry(alias.clone())
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(join.table_alias.clone());
                    }
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Adjacency graph: {:?}",
                adjacency
            );

            // 2. Seed removable set with CTE-backed aliases
            let mut removable = std::collections::HashSet::new();
            for join in &gj.joins {
                if exported_aliases.contains(join.table_alias.as_str()) {
                    removable.insert(join.table_alias.clone());
                }
            }
            if let Some(ref anchor) = gj.anchor_table {
                if exported_aliases.contains(anchor.as_str()) {
                    removable.insert(anchor.clone());
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Initial removable set (CTE-backed): {:?}",
                removable
            );

            // 3. Fixed-point expansion: a non-CTE join is removable if ALL its neighbors
            //    are already removable
            loop {
                let mut changed = false;
                for join in &gj.joins {
                    if removable.contains(&join.table_alias) {
                        continue;
                    }
                    if let Some(neighbors) = adjacency.get(&join.table_alias) {
                        if !neighbors.is_empty() && neighbors.iter().all(|n| removable.contains(n))
                        {
                            removable.insert(join.table_alias.clone());
                            changed = true;
                        }
                    }
                }
                if !changed {
                    break;
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Final removable set: {:?}",
                removable
            );

            // 4. Partition joins into kept/removed
            let mut kept_joins = Vec::new();
            let mut removed_joins = Vec::new();
            for (idx, join) in gj.joins.iter().enumerate() {
                if removable.contains(&join.table_alias) {
                    log::debug!(
                        "🔧 prune_joins_covered_by_cte: REMOVING join {} to '{}'",
                        idx,
                        join.table_alias
                    );
                    removed_joins.push(join.clone());
                } else {
                    log::info!(
                        "🔧 prune_joins_covered_by_cte: KEEPING join {} to '{}'",
                        idx,
                        join.table_alias
                    );
                    kept_joins.push(join.clone());
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Kept {} joins, removed {} joins",
                kept_joins.len(),
                removed_joins.len()
            );

            // If we removed joins, update the anchor_table to use the GraphNode alias that references the CTE
            // The anchor should be the alias of the GraphNode whose ViewScan.source_table matches cte_name
            let new_anchor = if !removed_joins.is_empty() {
                // Find the GraphNode that references this CTE
                if let Some(cte_ref_alias) = find_cte_reference_alias(&gj.input, cte_name) {
                    log::debug!("🔧 prune_joins_covered_by_cte: Updating anchor from '{:?}' to CTE reference alias '{}'",
                               gj.anchor_table, cte_ref_alias);
                    Some(cte_ref_alias)
                } else {
                    log::debug!("🔧 prune_joins_covered_by_cte: Could not find GraphNode referencing CTE '{}'", cte_name);
                    gj.anchor_table.clone()
                }
            } else {
                gj.anchor_table.clone()
            };

            // Recursively process the input
            let new_input =
                prune_joins_covered_by_cte(&gj.input, cte_name, exported_aliases, _cte_schemas)?;

            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: kept_joins,
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: new_anchor,
                cte_references: gj.cte_references.clone(),
                correlation_predicates: vec![],
            }))
        }
        LogicalPlan::Projection(proj) => {
            let new_input =
                prune_joins_covered_by_cte(&proj.input, cte_name, exported_aliases, _cte_schemas)?;
            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: proj.items.clone(),
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::Limit(limit) => {
            let new_input =
                prune_joins_covered_by_cte(&limit.input, cte_name, exported_aliases, _cte_schemas)?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: limit.count,
            }))
        }
        LogicalPlan::OrderBy(order) => {
            let new_input =
                prune_joins_covered_by_cte(&order.input, cte_name, exported_aliases, _cte_schemas)?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: order.items.clone(),
            }))
        }
        _ => {
            log::debug!(
                "🔧 prune_joins_covered_by_cte: No pruning needed for plan type {:?}",
                std::mem::discriminant(plan)
            );
            Ok(plan.clone())
        }
    }
}

/// Helper function to hoist nested CTEs from a rendered plan to a parent CTE list.
///
/// This is used after rendering a plan that may contain nested CTEs (e.g., from
/// variable-length path queries) to pull those CTEs up to the parent level so they
/// can be defined BEFORE the main CTE that references them.
///
/// # Arguments
/// * `from` - The RenderPlan to extract CTEs from (will be emptied)
/// * `to` - The destination vector to append the CTEs to
///
/// # Example
/// ```rust
/// // Create sample plan and schema (would be provided in real usage)
/// // let plan = Arc::new(LogicalPlan::default()); // Placeholder
/// // let schema = GraphSchema::default(); // Placeholder
/// // let mut with_cte_render = render_without_with_detection(plan, &schema)?;
/// // let mut all_ctes = Vec::new();
/// // hoist_nested_ctes(&mut with_cte_render, &mut all_ctes);
/// // all_ctes now contains any VLP CTEs that were nested in with_cte_render
/// ```
///
/// Replace the WITH clause subplan with a CTE reference (ViewScan pointing to CTE).
///
/// This transforms the plan so the WITH clause output comes from the CTE instead of
/// recomputing it.
///
/// IMPORTANT: We look for WithClause nodes which mark the true scope boundary.
/// When found, we replace them with a CTE reference.
///
/// CRITICAL: We only replace a WithClause if its INPUT has NO nested WITH clauses.
/// This ensures we replace the INNERMOST WITH first, then the next one, etc.
///
/// V2 of replace_with_clause_with_cte_reference that also filters out pre-WITH joins.
///
/// When we replace a WITH clause with a CTE reference, the joins from before the WITH
/// boundary should be removed from GraphJoins in the outer query - they're now inside the CTE.
///
/// `pre_with_aliases` contains the table aliases that were defined INSIDE the WITH clause
/// (before the boundary). These should be filtered out from outer GraphJoins.
///
/// Check if a join is for the inner scope (part of the pre-WITH pattern)
///
/// This is determined by checking if the join references aliases that are
/// NOT in the post-WITH scope (i.e., they're part of the CTE content).
/// Find the INNERMOST WITH clause subplan in a nested plan structure.
///
/// KEY INSIGHT: With chained WITH clauses (e.g., WITH a MATCH...WITH a,b MATCH...),
/// we need to process them from innermost to outermost. The innermost WITH is
/// the one whose INPUT has NO other WITH clauses nested inside it.
///
/// This function recursively searches for WITH clauses and returns the one
/// whose input is "clean" (contains no nested WITH).
///
/// Returns (with_clause_plan, alias_name) if found.
/// Find all WITH clauses in a plan grouped by their alias.
/// Returns HashMap where each alias maps to all WITH clause plans with that alias.
/// This handles the case where Union branches each have their own WITH clause with the same alias.
/// Returns owned (cloned) LogicalPlans to avoid lifetime issues with mutations.
/// Prune joins from GraphJoins that are already covered by a CTE.
///
/// Collapse a passthrough WITH clause by replacing it with its input.
/// A passthrough WITH is one that simply wraps a CTE reference without any transformations:
/// - Single item that's just a TableAlias
/// - No DISTINCT, ORDER BY, SKIP, LIMIT, WHERE
///
/// This function finds the passthrough WITH for the given alias and replaces it with its input.
/// Uses the analyzer's CTE name to distinguish between multiple consecutive WITHs with same alias.
/// When we have a query like:
///   WITH a MATCH (a)-[:F]->(b) WITH a,b MATCH (b)-[:F]->(c)
///
/// After processing, we have:
/// - CTE: with_a_b_cte2 (contains the pattern for a→b)
/// - Final plan: GraphJoins with joins for [a→t1→b, b→t2→c]
///
/// The joins [a→t1→b] are already materialized in the CTE, so they should be removed.
/// Only [b→t2→c] should remain in the final query.
///
/// This function:
/// 1. Traverses the plan to find GraphJoins nodes
/// 2. Identifies CTE-backed joins and uses position-aware pruning
///    (see `prune_joins_covered_by_cte` for details)
/// 3. Replaces the WITH clause with a CTE reference
pub(crate) fn replace_with_clause_with_cte_reference_v2(
    plan: &LogicalPlan,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    log::debug!(
        "🔧 replace_v2: Processing plan type {:?} for alias '{}'",
        std::mem::discriminant(plan),
        with_alias
    );

    /// Extract the node label from a plan tree by traversing through wrapper nodes
    fn extract_node_label_from_plan(plan: &Arc<LogicalPlan>) -> Option<String> {
        match plan.as_ref() {
            LogicalPlan::GraphNode(gn) => gn.label.clone(),
            LogicalPlan::Filter(f) => extract_node_label_from_plan(&f.input),
            LogicalPlan::Projection(p) => extract_node_label_from_plan(&p.input),
            LogicalPlan::WithClause(wc) => extract_node_label_from_plan(&wc.input),
            _ => None,
        }
    }

    /// Check if a plan is a CTE reference (GraphNode wrapping ViewScan with CTE table name)
    /// and the given WithClause is a simple passthrough (no modifications).
    fn is_simple_cte_passthrough(
        new_input: &LogicalPlan,
        wc: &crate::query_planner::logical_plan::WithClause,
    ) -> bool {
        // Check if new_input is a CTE reference
        let is_cte_ref = match new_input {
            LogicalPlan::GraphNode(gn) => {
                if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                    vs.source_table.starts_with("with_")
                } else {
                    false
                }
            }
            LogicalPlan::ViewScan(vs) => vs.source_table.starts_with("with_"),
            _ => false,
        };

        if !is_cte_ref {
            return false;
        }

        // Check if this WithClause is a simple passthrough (no modifications)
        // - Single item that's just a TableAlias
        // - No DISTINCT (already applied in inner CTE)
        // - No ORDER BY, SKIP, LIMIT modifiers
        wc.items.len() == 1
            && wc.order_by.is_none()
            && wc.skip.is_none()
            && wc.limit.is_none()
            && !wc.distinct
            && wc.where_clause.is_none()
            && matches!(
                &wc.items[0].expression,
                crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
            )
    }

    // Helper to generate a key for a WithClause (matches the key generation in find_all_with_clauses_grouped)
    fn get_with_clause_key(wc: &crate::query_planner::logical_plan::WithClause) -> String {
        if !wc.exported_aliases.is_empty() {
            let mut aliases = wc.exported_aliases.clone();
            aliases.sort();
            return aliases.join("_");
        }
        "with_var".to_string()
    }

    // Helper to remap PropertyAccess expressions to use CTE column names
    // CRITICAL: After creating a CTE reference, PropertyAccess expressions in downstream nodes
    // (like Projection) still have the OLD column names from FilterTagging (which used the
    // original ViewScan's property_mapping). FilterTagging may have resolved Cypher properties
    // to DB columns already, so we need to REVERSE that using db_to_cypher mapping.
    fn remap_property_access_for_cte(
        expr: crate::query_planner::logical_expr::LogicalExpr,
        cte_alias: &str,
        property_mapping: &HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
        db_to_cypher: &HashMap<String, String>,
    ) -> crate::query_planner::logical_expr::LogicalExpr {
        use crate::query_planner::logical_expr::LogicalExpr;

        match expr {
            LogicalExpr::PropertyAccessExp(mut prop) => {
                // Check if this PropertyAccess references the CTE alias
                if prop.table_alias.0 == cte_alias {
                    let current_col = prop.column.raw();

                    // CRITICAL: FilterTagging ALWAYS resolves Cypher properties to DB columns
                    // So current_col is almost certainly a DB column name, not a Cypher property
                    //
                    // Strategy:
                    // 1. PRIMARY: Try reverse mapping (DB column → Cypher property → CTE column)
                    // 2. FALLBACK: Direct lookup (handles identity mappings where Cypher name = DB name)

                    if let Some(cypher_prop) = db_to_cypher.get(current_col) {
                        // Found! current_col is a DB column - reverse it to Cypher property
                        if let Some(cte_col) = property_mapping.get(cypher_prop) {
                            log::debug!(
                                "🔧 remap_property_access: Remapped {}.{} → {} (DB '{}' → Cypher '{}' → CTE)",
                                cte_alias, current_col, cte_col.raw(), current_col, cypher_prop
                            );
                            prop.column = cte_col.clone();
                        } else {
                            log::debug!(
                                "🔧 remap_property_access: Reverse mapped DB '{}' to Cypher '{}' but no CTE column found!",
                                current_col, cypher_prop
                            );
                        }
                    } else if let Some(cte_col) = property_mapping.get(current_col) {
                        // Fallback: Identity mapping where Cypher property = DB column
                        // Example: user_id: user_id → both "user_id" (Cypher) and "user_id" (DB)
                        log::debug!(
                            "🔧 remap_property_access: Remapped {}.{} → {} (direct/identity mapping)",
                            cte_alias, current_col, cte_col.raw()
                        );
                        prop.column = cte_col.clone();
                    } else {
                        log::debug!(
                            "🔧 remap_property_access: Could not remap {}.{} - not in db_to_cypher or property_mapping",
                            cte_alias, current_col
                        );
                    }
                }
                LogicalExpr::PropertyAccessExp(prop)
            }
            LogicalExpr::OperatorApplicationExp(mut op) => {
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|operand| {
                        remap_property_access_for_cte(
                            operand,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect();
                LogicalExpr::OperatorApplicationExp(op)
            }
            LogicalExpr::AggregateFnCall(mut agg) => {
                agg.args = agg
                    .args
                    .into_iter()
                    .map(|arg| {
                        remap_property_access_for_cte(
                            arg,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect();
                LogicalExpr::AggregateFnCall(agg)
            }
            LogicalExpr::ScalarFnCall(mut func) => {
                func.args = func
                    .args
                    .into_iter()
                    .map(|arg| {
                        remap_property_access_for_cte(
                            arg,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect();
                LogicalExpr::ScalarFnCall(func)
            }
            LogicalExpr::List(list) => LogicalExpr::List(
                list.into_iter()
                    .map(|item| {
                        remap_property_access_for_cte(
                            item,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect(),
            ),
            LogicalExpr::Case(mut case_expr) => {
                if let Some(expr) = case_expr.expr {
                    case_expr.expr = Some(Box::new(remap_property_access_for_cte(
                        *expr,
                        cte_alias,
                        property_mapping,
                        db_to_cypher,
                    )));
                }
                case_expr.when_then = case_expr
                    .when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            remap_property_access_for_cte(
                                when,
                                cte_alias,
                                property_mapping,
                                db_to_cypher,
                            ),
                            remap_property_access_for_cte(
                                then,
                                cte_alias,
                                property_mapping,
                                db_to_cypher,
                            ),
                        )
                    })
                    .collect();
                if let Some(else_expr) = case_expr.else_expr {
                    case_expr.else_expr = Some(Box::new(remap_property_access_for_cte(
                        *else_expr,
                        cte_alias,
                        property_mapping,
                        db_to_cypher,
                    )));
                }
                LogicalExpr::Case(case_expr)
            }
            // Other expressions don't contain PropertyAccess
            other => other,
        }
    }

    // Helper to remap PropertyAccess in a ProjectionItem
    fn remap_projection_item(
        item: crate::query_planner::logical_plan::ProjectionItem,
        cte_alias: &str,
        property_mapping: &HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
        db_to_cypher: &HashMap<String, String>,
    ) -> crate::query_planner::logical_plan::ProjectionItem {
        crate::query_planner::logical_plan::ProjectionItem {
            expression: remap_property_access_for_cte(
                item.expression,
                cte_alias,
                property_mapping,
                db_to_cypher,
            ),
            col_alias: item.col_alias,
        }
    }

    // Helper to create a CTE reference node with proper property_mapping
    fn create_cte_reference(
        cte_name: &str,
        with_alias: &str,
        cte_schemas: &crate::render_plan::CteSchemas,
    ) -> LogicalPlan {
        use crate::graph_catalog::expression_parser::PropertyValue;

        // CRITICAL: Use the original WITH alias (e.g., "a") as the GraphNode alias
        // This ensures property references like "a.user_id" work correctly
        // The FROM clause will render as: FROM with_a_cte1 AS a
        let table_alias = with_alias.to_string();

        // Build property_mapping using CYPHER PROPERTY NAMES ONLY
        // Store the ViewScan's DB mapping separately so we can reverse-resolve DB columns
        let (property_mapping, _db_to_cypher_mapping) = if let Some(meta) =
            cte_schemas.get(cte_name)
        {
            let mut mapping = HashMap::new();
            let mut db_to_cypher = HashMap::new(); // Reverse: DB column → Cypher property

            // Parse the composite with_alias into individual aliases
            // e.g., "fids_p" → ["fids", "p"] (from exported_aliases tracked earlier)
            // We need individual aliases to match CTE column names like "p1_p_id"

            // Build mappings from SelectItems
            for item in &meta.select_items {
                if let Some(cte_col_alias) = &item.col_alias {
                    let cte_col_name = &cte_col_alias.0;

                    // Use the proper p{N} CTE column naming parser for unambiguous decoding
                    if let Some((col_alias, cypher_prop)) =
                        crate::utils::cte_column_naming::parse_cte_column(cte_col_name)
                    {
                        // Primary: Cypher property → CTE column
                        // Key format: "alias.property" so downstream property access works
                        mapping.insert(
                            cypher_prop.to_string(),
                            PropertyValue::Column(cte_col_name.clone()),
                        );

                        // Reverse: DB column → Cypher property (for resolving FilterTagging's DB columns)
                        if let RenderExpr::PropertyAccessExp(prop_access) = &item.expression {
                            let db_col = prop_access.column.raw();

                            // Detect conflicts: multiple Cypher properties using same DB column
                            if let Some(existing_cypher) = db_to_cypher.get(db_col) {
                                if existing_cypher != &cypher_prop {
                                    log::debug!(
                                        "🔧 create_cte_reference: CONFLICT - DB column '{}' used by both Cypher '{}' and '{}'. \
                                         Using '{}' (last wins). Queries using '{}.{}' may get wrong column!",
                                        db_col, existing_cypher, cypher_prop, cypher_prop, col_alias, existing_cypher
                                    );
                                }
                            }

                            db_to_cypher.insert(db_col.to_string(), cypher_prop.to_string());

                            if db_col != cypher_prop {
                                log::debug!(
                                    "🔧 create_cte_reference: Reverse mapping for '{}': DB '{}' ← Cypher '{}' → CTE '{}'",
                                    col_alias, db_col, cypher_prop, cte_col_name
                                );
                            }
                        }
                    } else if let Some(cypher_prop) =
                        cte_col_name.strip_prefix(&format!("{}_", with_alias))
                    {
                        // Legacy fallback: try stripping composite alias prefix
                        mapping.insert(
                            cypher_prop.to_string(),
                            PropertyValue::Column(cte_col_name.clone()),
                        );
                    } else {
                        // Fallback: identity mapping (for non-property columns like "fids")
                        mapping.insert(
                            cte_col_name.clone(),
                            PropertyValue::Column(cte_col_name.clone()),
                        );
                    }
                }
            }

            // CRITICAL FIX: Add DB column mappings from stored_property_mapping
            // The stored_property_mapping has entries like ((u, full_name), u_name)
            // which tells us: DB column "full_name" should map to CTE column "u_name"
            // We need to add these to the ViewScan property_mapping as:
            // ("full_name", Column("u_name"))
            for ((alias, db_prop), cte_column) in meta.property_mapping.iter() {
                if alias == with_alias {
                    // This is a mapping for our alias (e.g., "u")
                    // Add it to the mapping if not already present
                    if !mapping.contains_key(db_prop) {
                        mapping.insert(db_prop.clone(), PropertyValue::Column(cte_column.clone()));
                        log::debug!(
                            "🔧 create_cte_reference: Added DB column mapping from stored: ({}, {}) → {}",
                            alias,
                            db_prop,
                            cte_column
                        );
                    }
                }
            }

            log::info!(
                "🔧 create_cte_reference: Built mappings for '{}': {} Cypher→CTE + {} DB→Cypher",
                cte_name,
                mapping.len(),
                db_to_cypher.len()
            );
            (mapping, db_to_cypher)
        } else {
            log::debug!(
                "🔧 create_cte_reference (v2): No schema found for CTE '{}', using empty property_mapping",
                cte_name
            );
            (HashMap::new(), HashMap::new())
        };

        // Look up the actual ID column from cte_schemas (alias → ID column mapping)
        // The alias_to_id stores prefixed names like "a_code", but ViewScan.id_column
        // should be unprefixed (e.g., "code") because resolve_cte_reference adds the prefix.
        let cte_id_column = cte_schemas
            .get(cte_name)
            .and_then(|meta| {
                // Try direct lookup first
                meta.alias_to_id
                    .get(with_alias)
                    .or_else(|| {
                        // Combined alias (e.g., "a_allNeighboursCount") won't match
                        // individual aliases (e.g., "a"). Try first matching key.
                        meta.alias_to_id
                            .keys()
                            .next()
                            .and_then(|k| meta.alias_to_id.get(k))
                    })
                    .map(|prefixed| {
                        // Strip any alias prefix: "a_code" → "code"
                        // Try with_alias first, then each key in alias_to_id
                        let stripped = prefixed
                            .strip_prefix(&format!("{}_", with_alias))
                            .or_else(|| {
                                meta.alias_to_id
                                    .keys()
                                    .find_map(|k| prefixed.strip_prefix(&format!("{}_", k)))
                            })
                            .unwrap_or(prefixed);
                        stripped.to_string()
                    })
            })
            .unwrap_or_else(|| "id".to_string());
        log::info!(
            "🔧 create_cte_reference: CTE '{}' alias '{}' → id_column '{}'",
            cte_name,
            with_alias,
            cte_id_column
        );

        LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
                source_table: cte_name.to_string(),
                view_filter: None,
                property_mapping,
                id_column: cte_id_column.clone(),
                output_schema: vec!["id".to_string()],
                projections: vec![],
                from_id: None,
                to_id: None,
                input: None,
                view_parameter_names: None,
                view_parameter_values: None,
                use_final: false,
                is_denormalized: false,
                from_node_properties: None,
                to_node_properties: None,
                type_column: None,
                type_values: None,
                from_label_column: None,
                to_label_column: None,
                schema_filter: None,
                node_label: None,
            }))),
            alias: table_alias,
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        })
    }

    match plan {
        // NEW: Handle WithClause type
        // Key insight: Check if this WithClause's generated key matches the alias we're looking for
        LogicalPlan::WithClause(wc) => {
            // Generate key same way as find_all_with_clauses_grouped does
            let this_wc_key = get_with_clause_key(wc);
            let is_target_with = this_wc_key == with_alias;
            let has_nested = plan_contains_with_clause(&wc.input);
            log::debug!(
                "🔧 replace_v2: WithClause with key '{}', looking for '{}', is_target: {}, has_nested: {}",
                this_wc_key,
                with_alias,
                is_target_with,
                has_nested
            );

            if is_target_with && !plan_contains_with_clause(&wc.input) {
                // This is THE WithClause we're replacing, and it's innermost
                log::debug!(
                    "🔧 replace_v2: FOUND AND REPLACING target innermost WithClause with key '{}' for alias '{}' with CTE '{}'",
                    this_wc_key, with_alias, cte_name
                );
                log::debug!(
                    "🔧 replace_v2: WithClause exported_aliases={:?}, input type={:?}",
                    wc.exported_aliases,
                    std::mem::discriminant(wc.input.as_ref())
                );
                let mut cte_ref = create_cte_reference(cte_name, with_alias, cte_schemas);
                // Preserve the original node label from the WithClause's underlying plan
                // so VLP CTE extraction can determine the start/end node type
                if let LogicalPlan::GraphNode(ref mut gn) = cte_ref {
                    gn.label = extract_node_label_from_plan(&wc.input);
                }
                Ok(cte_ref)
            } else if is_target_with {
                // This is THE WithClause, but it has nested WITH clauses - error case
                // (We should be processing inner ones first)
                log::debug!("🔧 replace_v2: Target WithClause has nested WITH - should process inner first!");
                let new_input = replace_with_clause_with_cte_reference_v2(
                    &wc.input,
                    with_alias,
                    cte_name,
                    pre_with_aliases,
                    cte_schemas,
                )?;

                // DISABLED: Don't collapse passthrough WITHs here (same reason as above)
                // Let the iteration loop handle them properly
                //
                // // Check if after recursion, the new_input is a CTE reference
                // // and this WITH is a simple passthrough - if so, collapse it
                // if is_simple_cte_passthrough(&new_input, wc) {
                //     log::debug!(
                //         "🔧 replace_v2: Collapsing passthrough WithClause to CTE reference"
                //     );
                //     return Ok(new_input);
                // }

                log::debug!("🔧 DEBUG replace_v2: Creating new outer WithClause with wc.cte_references = {:?}", wc.cte_references);

                Ok(LogicalPlan::WithClause(
                    crate::query_planner::logical_plan::WithClause {
                        cte_name: None,
                        input: Arc::new(new_input),
                        items: wc.items.clone(),
                        distinct: wc.distinct,
                        order_by: wc.order_by.clone(),
                        skip: wc.skip,
                        limit: wc.limit,
                        where_clause: wc.where_clause.clone(),
                        exported_aliases: wc.exported_aliases.clone(),
                        cte_references: wc.cte_references.clone(),
                        pattern_comprehensions: wc.pattern_comprehensions.clone(),
                    },
                ))
            } else {
                // This is NOT the WithClause we're looking for, but we need to recurse
                // to find and replace the inner one
                log::debug!("🔧 replace_v2: Not target WithClause (key='{}') - recursing into input to find '{}'", this_wc_key, with_alias);
                log::debug!(
                    "🔧 DEBUG replace_v2: outer wc.cte_references = {:?}",
                    wc.cte_references
                );
                let new_input = replace_with_clause_with_cte_reference_v2(
                    &wc.input,
                    with_alias,
                    cte_name,
                    pre_with_aliases,
                    cte_schemas,
                )?;

                // DISABLED: Don't collapse passthrough WITHs here.
                // Instead, let the iteration loop handle them. When the outer WITH
                // is processed in the next iteration, its cte_references will tell us
                // the CTE name to use, and we can properly handle expression remapping.
                //
                // Previously, collapsing here caused expressions that reference the
                // collapsed WITH's CTE name to become stale (the CTE was never created).
                //
                // // Check if after recursion, the new_input is a CTE reference
                // // and this WITH is a simple passthrough - if so, collapse it
                // if is_simple_cte_passthrough(&new_input, wc) {
                //     log::debug!("🔧 replace_v2: Collapsing passthrough WithClause (not target) to CTE reference");
                //     return Ok(new_input);
                // }

                Ok(LogicalPlan::WithClause(
                    crate::query_planner::logical_plan::WithClause {
                        cte_name: None,
                        input: Arc::new(new_input),
                        items: wc.items.clone(),
                        distinct: wc.distinct,
                        order_by: wc.order_by.clone(),
                        skip: wc.skip,
                        limit: wc.limit,
                        where_clause: wc.where_clause.clone(),
                        exported_aliases: wc.exported_aliases.clone(),
                        cte_references: wc.cte_references.clone(),
                        pattern_comprehensions: wc.pattern_comprehensions.clone(),
                    },
                ))
            }
        }

        LogicalPlan::GraphRel(graph_rel) => {
            // Helper to check if we need to process this branch
            // We need to process it if:
            // 1. It contains a WITH clause, OR
            // 2. It has a GraphNode with the matching alias
            fn needs_processing(plan: &LogicalPlan, with_alias: &str) -> bool {
                let result = match plan {
                    LogicalPlan::GraphNode(node) => node.alias == with_alias,
                    LogicalPlan::WithClause(wc) => needs_processing(&wc.input, with_alias),
                    LogicalPlan::GraphRel(rel) => {
                        needs_processing(&rel.left, with_alias)
                            || needs_processing(&rel.right, with_alias)
                    }
                    LogicalPlan::Projection(proj) => needs_processing(&proj.input, with_alias),
                    LogicalPlan::GraphJoins(gj) => needs_processing(&gj.input, with_alias),
                    LogicalPlan::Filter(f) => needs_processing(&f.input, with_alias),
                    LogicalPlan::Unwind(u) => needs_processing(&u.input, with_alias),
                    LogicalPlan::CartesianProduct(cp) => {
                        needs_processing(&cp.left, with_alias)
                            || needs_processing(&cp.right, with_alias)
                    }
                    _ => plan_contains_with_clause(plan),
                };
                log::debug!(
                    "🔧 replace_v2: needs_processing({:?}, '{}') = {}",
                    std::mem::discriminant(plan),
                    with_alias,
                    result
                );
                result
            }
            // Always recurse for WithClause - the WithClause case will handle replacement
            // Don't shortcut with is_innermost_with_clause check because the WithClause's input
            // might contain a GraphNode that needs updating from a previous iteration
            let new_left: Arc<LogicalPlan> = if plan_contains_with_clause(&graph_rel.left)
                || needs_processing(&graph_rel.left, with_alias)
            {
                Arc::new(replace_with_clause_with_cte_reference_v2(
                    &graph_rel.left,
                    with_alias,
                    cte_name,
                    pre_with_aliases,
                    cte_schemas,
                )?)
            } else {
                graph_rel.left.clone()
            };

            let new_right: Arc<LogicalPlan> = if plan_contains_with_clause(&graph_rel.right)
                || needs_processing(&graph_rel.right, with_alias)
            {
                Arc::new(replace_with_clause_with_cte_reference_v2(
                    &graph_rel.right,
                    with_alias,
                    cte_name,
                    pre_with_aliases,
                    cte_schemas,
                )?)
            } else {
                graph_rel.right.clone()
            };

            Ok(LogicalPlan::GraphRel(GraphRel {
                left: new_left,
                center: graph_rel.center.clone(),
                right: new_right,
                alias: graph_rel.alias.clone(),
                direction: graph_rel.direction.clone(),
                left_connection: graph_rel.left_connection.clone(),
                right_connection: graph_rel.right_connection.clone(),
                is_rel_anchor: graph_rel.is_rel_anchor,
                variable_length: graph_rel.variable_length.clone(),
                shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                path_variable: graph_rel.path_variable.clone(),
                where_predicate: graph_rel.where_predicate.clone(),
                labels: graph_rel.labels.clone(),
                is_optional: graph_rel.is_optional,
                anchor_connection: graph_rel.anchor_connection.clone(),
                cte_references: std::collections::HashMap::new(),
                pattern_combinations: None,
                was_undirected: graph_rel.was_undirected,
            }))
        }

        LogicalPlan::Projection(proj) => {
            log::info!(
                "🔀 replace_v2: Processing Projection, input type: {:?}",
                std::mem::discriminant(proj.input.as_ref())
            );
            let new_input = replace_with_clause_with_cte_reference_v2(
                &proj.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            log::info!(
                "🔀 replace_v2: Projection new_input type: {:?}",
                std::mem::discriminant(&new_input)
            );

            // CRITICAL: Check if new_input is a CTE reference (GraphNode wrapping ViewScan for CTE)
            // If so, remap PropertyAccess expressions in projection items to use CTE column names
            let should_remap = match &new_input {
                LogicalPlan::GraphNode(gn) => {
                    if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                        vs.source_table.starts_with("with_") && gn.alias == with_alias
                    } else {
                        false
                    }
                }
                _ => false,
            };

            let remapped_items = if should_remap {
                // Extract property_mapping from the CTE reference and rebuild per-alias mappings
                if let LogicalPlan::GraphNode(gn) = &new_input {
                    if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                        // Build per-alias property mappings from CTE columns
                        // For composite alias "fids_p", individual aliases are "fids" and "p"
                        // CTE column "p1_p_id" maps to alias="p", property="id"
                        let mut per_alias_mappings: HashMap<
                            String,
                            HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
                        > = HashMap::new();
                        let mut per_alias_db_to_cypher: HashMap<String, HashMap<String, String>> =
                            HashMap::new();

                        if let Some(meta) = cte_schemas.get(&vs.source_table) {
                            for item in &meta.select_items {
                                if let Some(cte_col_alias) = &item.col_alias {
                                    let cte_col_name = &cte_col_alias.0;
                                    if let Some((col_alias, cypher_prop)) =
                                        crate::utils::cte_column_naming::parse_cte_column(
                                            cte_col_name,
                                        )
                                    {
                                        // Add to per-alias property mapping
                                        per_alias_mappings
                                            .entry(col_alias.to_string())
                                            .or_default()
                                            .insert(
                                                cypher_prop.to_string(),
                                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                    cte_col_name.clone(),
                                                ),
                                            );

                                        // Build reverse DB→Cypher mapping per alias
                                        if let RenderExpr::PropertyAccessExp(prop_access) =
                                            &item.expression
                                        {
                                            let db_col = prop_access.column.raw();
                                            per_alias_db_to_cypher
                                                .entry(col_alias.to_string())
                                                .or_default()
                                                .insert(
                                                    db_col.to_string(),
                                                    cypher_prop.to_string(),
                                                );
                                        }
                                    }
                                }
                            }
                        }

                        log::info!(
                            "🔧 replace_v2: Remapping Projection items for CTE '{}' (alias='{}') with {} per-alias mappings: {:?}",
                            vs.source_table,
                            with_alias,
                            per_alias_mappings.len(),
                            per_alias_mappings.keys().collect::<Vec<_>>()
                        );

                        // Remap each projection item against each individual alias
                        let mut items: Vec<crate::query_planner::logical_plan::ProjectionItem> =
                            proj.items.clone();
                        for (alias, alias_mapping) in &per_alias_mappings {
                            let alias_db_to_cypher = per_alias_db_to_cypher
                                .get(alias)
                                .cloned()
                                .unwrap_or_default();
                            items = items
                                .into_iter()
                                .map(|item| {
                                    remap_projection_item(
                                        item,
                                        alias,
                                        alias_mapping,
                                        &alias_db_to_cypher,
                                    )
                                })
                                .collect();
                        }

                        // Also remap against composite alias for non-property columns (e.g., "fids")
                        let composite_db_to_cypher = HashMap::new();
                        items = items
                            .into_iter()
                            .map(|item| {
                                remap_projection_item(
                                    item,
                                    with_alias,
                                    &vs.property_mapping,
                                    &composite_db_to_cypher,
                                )
                            })
                            .collect();

                        items
                    } else {
                        proj.items.clone()
                    }
                } else {
                    proj.items.clone()
                }
            } else {
                proj.items.clone()
            };

            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: remapped_items,
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }

        LogicalPlan::Filter(filter) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &filter.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: filter.predicate.clone(),
            }))
        }

        LogicalPlan::GroupBy(group_by) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &group_by.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: group_by.expressions.clone(),
                having_clause: group_by.having_clause.clone(),
                is_materialization_boundary: group_by.is_materialization_boundary,
                exposed_alias: group_by.exposed_alias.clone(),
            }))
        }

        LogicalPlan::GraphJoins(graph_joins) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &graph_joins.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;

            // Helper to check if a join condition references any stale alias
            fn condition_has_stale_refs(
                join: &crate::query_planner::logical_plan::Join,
                stale_aliases: &std::collections::HashSet<String>,
            ) -> bool {
                for op in &join.joining_on {
                    for operand in &op.operands {
                        if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                            pa,
                        ) = operand
                        {
                            if stale_aliases.contains(&pa.table_alias.0) {
                                return true;
                            }
                        }
                    }
                }
                false
            }

            // Filter out joins from the pre-WITH scope AND update joins for the WITH alias
            // Also filter out joins that have stale references in their conditions
            let updated_joins: Vec<crate::query_planner::logical_plan::Join> = graph_joins
                .joins
                .iter()
                .filter_map(|j| {
                    // Filter out joins that are from the pre-WITH scope
                    if pre_with_aliases.contains(&j.table_alias) {
                        log::debug!(
                            "🔧 replace_v2: Filtering out pre-WITH join for alias '{}'",
                            j.table_alias
                        );
                        return None;
                    }

                    // Filter out joins whose conditions reference stale aliases
                    if condition_has_stale_refs(j, pre_with_aliases) {
                        log::debug!(
                            "🔧 replace_v2: Filtering out join with stale condition for alias '{}'",
                            j.table_alias
                        );
                        return None;
                    }

                    // Update joins that reference the WITH alias to use the CTE
                    if j.table_alias == with_alias {
                        log::debug!(
                            "🔧 replace_v2: Updating join for alias '{}' to use CTE '{}'",
                            with_alias,
                            cte_name
                        );
                        Some(crate::query_planner::logical_plan::Join {
                            table_name: cte_name.to_string(),
                            table_alias: j.table_alias.clone(),
                            joining_on: j.joining_on.clone(),
                            join_type: j.join_type.clone(),
                            pre_filter: j.pre_filter.clone(),
                            from_id_column: j.from_id_column.clone(),
                            to_id_column: j.to_id_column.clone(),
                            graph_rel: None,
                        })
                    } else {
                        Some(j.clone())
                    }
                })
                .collect();

            // Update anchor_table if it was in pre-WITH scope
            let new_anchor = if let Some(ref anchor) = graph_joins.anchor_table {
                if pre_with_aliases.contains(anchor) {
                    log::debug!(
                        "🔧 replace_v2: Updating anchor from '{}' to '{}'",
                        anchor,
                        with_alias
                    );
                    Some(with_alias.to_string())
                } else {
                    Some(anchor.clone())
                }
            } else {
                None
            };

            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: updated_joins,
                optional_aliases: graph_joins.optional_aliases.clone(),
                anchor_table: new_anchor,
                cte_references: graph_joins.cte_references.clone(),
                correlation_predicates: vec![],
            }))
        }

        LogicalPlan::Limit(limit) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &limit.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: limit.count,
            }))
        }

        LogicalPlan::OrderBy(order_by) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &order_by.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: order_by.items.clone(),
            }))
        }

        LogicalPlan::Skip(skip) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &skip.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }

        LogicalPlan::Union(union) => {
            log::info!(
                "🔀 replace_v2: Processing Union with {} branches for alias '{}'",
                union.inputs.len(),
                with_alias
            );
            let new_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .enumerate()
                .map(|(i, input)| {
                    log::info!(
                        "🔀 replace_v2: Processing Union branch {} type: {:?}",
                        i,
                        std::mem::discriminant(input.as_ref())
                    );
                    replace_with_clause_with_cte_reference_v2(
                        input,
                        with_alias,
                        cte_name,
                        pre_with_aliases,
                        cte_schemas,
                    )
                    .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            log::info!(
                "🔀 replace_v2: Union result has {} branches",
                new_inputs.len()
            );
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: union.union_type.clone(),
            }))
        }

        LogicalPlan::GraphNode(node) => {
            // CRITICAL FIX: Check if this GraphNode's alias is exported from the CTE
            // This handles patterns like: WITH a, b ... MATCH (b)-[]->(c)
            // where 'b' should come from the CTE, not a fresh table scan

            // First recurse into the input to handle nested structures
            let new_input = replace_with_clause_with_cte_reference_v2(
                &node.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;

            // Check if this node's alias matches an exported alias from the CTE
            // For composite aliases like "friend_post", we need to check all parts
            let with_parts: Vec<&str> = with_alias.split('_').collect();
            let node_matches_cte = with_parts.contains(&node.alias.as_str());

            if node_matches_cte {
                log::debug!(
                    "🔧 replace_v2: GraphNode '{}' matches CTE exported alias '{}' - replacing with CTE reference '{}'",
                    node.alias, with_alias, cte_name
                );

                // Replace this GraphNode with a CTE reference
                // The CTE contains all the columns for the exported aliases
                Ok(create_cte_reference(cte_name, &node.alias, cte_schemas))
            } else {
                log::debug!(
                    "🔧 replace_v2: GraphNode '{}' does NOT match CTE - keeping with recursed input",
                    node.alias
                );
                // This GraphNode doesn't match - keep it but use the recursed input
                Ok(LogicalPlan::GraphNode(GraphNode {
                    input: Arc::new(new_input),
                    alias: node.alias.clone(),
                    label: node.label.clone(),
                    is_denormalized: node.is_denormalized,
                    projected_columns: None,
                    node_types: None,
                }))
            }
        }

        LogicalPlan::CartesianProduct(cp) => {
            // CartesianProduct is used for WITH...MATCH patterns where aliases don't overlap
            // Recurse into both sides to replace WITH clauses
            log::debug!(
                "🔧 replace_v2: Processing CartesianProduct - recursing into left and right"
            );
            let new_left = Arc::new(replace_with_clause_with_cte_reference_v2(
                &cp.left,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            let new_right = Arc::new(replace_with_clause_with_cte_reference_v2(
                &cp.right,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            Ok(LogicalPlan::CartesianProduct(CartesianProduct {
                left: new_left,
                right: new_right,
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }

        LogicalPlan::Unwind(unwind) => {
            let new_input = Arc::new(replace_with_clause_with_cte_reference_v2(
                &unwind.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            Ok(LogicalPlan::Unwind(Unwind {
                input: new_input,
                expression: unwind.expression.clone(),
                alias: unwind.alias.clone(),
                label: unwind.label.clone(),
                tuple_properties: unwind.tuple_properties.clone(),
            }))
        }

        other => Ok(other.clone()),
    }
}

/// Build a column map from a WITH CTE render plan's SELECT items.
/// Maps (cypher_alias, cypher_property) → "cte_from_alias.cte_column_name"
/// Used to resolve outer-scope variable references inside correlated subqueries.
fn build_cte_column_map(
    render_plan: &RenderPlan,
    cte_from_alias: &str,
) -> HashMap<(String, String), String> {
    let mut map = HashMap::new();

    // Use the actual FROM alias for column qualification, not the CTE name.
    // Within a CTE body, correlated subqueries reference the FROM table alias
    // (e.g., person_tag.p3_tag_id), not the CTE being defined
    // (e.g., with_person_score_tag_cte_1.p3_tag_id).
    let effective_alias = if let FromTableItem(Some(ref from)) = render_plan.from {
        from.alias.as_deref().unwrap_or(cte_from_alias).to_string()
    } else {
        cte_from_alias.to_string()
    };

    // Determine the source of SELECT items to scan
    let select_items_list: Vec<&Vec<SelectItem>> =
        if let UnionItems(Some(ref union)) = render_plan.union {
            // For UNION, scan all branches
            union.input.iter().map(|b| &b.select.items).collect()
        } else {
            vec![&render_plan.select.items]
        };

    for select_items in &select_items_list {
        for item in *select_items {
            if let Some(ref col_alias) = item.col_alias {
                let cte_col_name = &col_alias.0;
                if let Some((parsed_alias, parsed_property)) = parse_cte_column(cte_col_name) {
                    // Determine the real column to use in correlated subqueries.
                    // If the SELECT expression is a PropertyAccess (e.g., `a.user_id AS p1_a_user_id`),
                    // use the actual column from the expression (user_id), not the alias (p1_a_user_id).
                    // Uses PropertyValue::to_sql() which handles:
                    // - simple columns (with proper quoting),
                    // - expression-based mappings (e.g., toYear(FlightDate)),
                    // - base tables (where p{N} names don't exist as real columns),
                    // - CTE references (where the expression itself will use the CTE column name).
                    let qualified = if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                        pa.column.to_sql(&effective_alias)
                    } else {
                        // Non-property expressions (aggregates, subqueries, etc.):
                        // fall back to the CTE column alias name
                        format!("{}.{}", effective_alias, cte_col_name)
                    };
                    map.insert((parsed_alias, parsed_property), qualified);
                } else {
                    // Non-p{N} columns: treat the alias itself as a bare variable
                    let qualified = format!("{}.\"{}\"", effective_alias, cte_col_name);
                    map.insert((cte_col_name.clone(), "id".to_string()), qualified.clone());
                    map.insert((cte_col_name.clone(), cte_col_name.clone()), qualified);
                }
            }
        }
    }

    // Also scan FROM table to find CTE-backed columns
    // For UNION branches, scan each branch's FROM
    let from_tables: Vec<&RenderPlan> = if let UnionItems(Some(ref union)) = render_plan.union {
        union.input.iter().collect()
    } else {
        vec![render_plan]
    };

    for branch in &from_tables {
        if let FromTableItem(Some(ref from)) = branch.from {
            if let Some(ref from_alias) = from.alias {
                // If FROM is a CTE reference, scan its SELECT items for p{N} columns
                // The from_alias is what's used in the SQL body
                for item in &branch.select.items {
                    if let Some(ref col_alias) = item.col_alias {
                        let cte_col_name = &col_alias.0;
                        if let Some((parsed_alias, parsed_property)) =
                            parse_cte_column(cte_col_name)
                        {
                            // Map the parsed alias/property to the FROM alias qualified reference
                            let qualified = format!("{}.{}", from_alias, cte_col_name);
                            map.entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        }
                    }
                    // Also look for PropertyAccess expressions that reference FROM alias
                    scan_expr_for_aliases(&item.expression, from_alias, &mut map);
                }

                // Scan JOINs for table aliases (e.g., friend from Person table)
                for join in &branch.joins.0 {
                    let join_alias = &join.table_alias;
                    // Map join alias to itself (it's directly available in scope)
                    map.entry((join_alias.clone(), "id".to_string()))
                        .or_insert_with(|| format!("{}.id", join_alias));
                }
            }
        }
    }

    map
}

/// Scan a RenderExpr for PropertyAccess patterns that reference a FROM alias,
/// and add them to the column map for correlated subquery resolution.
fn scan_expr_for_aliases(
    expr: &RenderExpr,
    from_alias: &str,
    map: &mut HashMap<(String, String), String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == from_alias {
                if let crate::graph_catalog::expression_parser::PropertyValue::Column(ref col) =
                    pa.column
                {
                    // This is a reference like `from_alias.some_cte_column`
                    if let Some((parsed_alias, parsed_property)) = parse_cte_column(col) {
                        let qualified = format!("{}.{}", from_alias, col);
                        map.entry((parsed_alias, parsed_property))
                            .or_insert(qualified);
                    }
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                scan_expr_for_aliases(operand, from_alias, map);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                scan_expr_for_aliases(arg, from_alias, map);
            }
        }
        RenderExpr::AggregateFnCall(a) => {
            for arg in &a.args {
                scan_expr_for_aliases(arg, from_alias, map);
            }
        }
        _ => {}
    }
}

/// Generate a correlated subquery SQL string for a single pattern comprehension.
///
/// For a pattern like `(tag)<-[:HAS_INTEREST]-(person)`:
/// ```sql
/// (SELECT COUNT(*) FROM ldbc.Person_hasInterest_Tag
///  WHERE TagId = cte_alias.p3_tag_id AND PersonId = cte_alias.p6_person_id)
/// ```
///
/// For multi-hop like `(tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person)`:
/// ```sql
/// (SELECT COUNT(*) FROM ldbc.Message_hasTag_Tag AS __r0
///  INNER JOIN ldbc.Message_hasCreator_Person AS __r1 ON __r0.MessageId = __r1.MessageId
///  INNER JOIN ldbc.Message AS __m0 ON __m0.id = __r0.MessageId
///  WHERE __r0.TagId = cte_alias.p3_tag_id AND __r1.PersonId = cte_alias.p6_person_id
///    AND $startDate < __m0.creationDate AND __m0.creationDate < $endDate)
/// ```
fn generate_pattern_comprehension_correlated_subquery(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::{ConnectedPatternInfo, PatternPosition};

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    // For list comprehension patterns (e.g., size([p IN posts WHERE pattern])),
    // use arrayCount() instead of correlated subquery to avoid ClickHouse
    // "Cannot clone Union plan step" error when outer query has UNION ALL.
    if pc_meta.list_constraint.is_some() {
        return generate_list_comp_array_count(pc_meta, schema, cte_column_map);
    }

    log::info!(
        "🔧 generate_pc_correlated_subquery: {} hops, {} correlation vars, where={:?}",
        pc_meta.pattern_hops.len(),
        pc_meta.correlation_vars.len(),
        pc_meta.where_clause.is_some()
    );

    // Check if any hop uses Direction::Either (undirected edge).
    // For undirected edges, we need to generate a UNION of both directions.
    let has_either_direction = pc_meta
        .pattern_hops
        .iter()
        .any(|h| h.direction == crate::query_planner::logical_expr::Direction::Either);

    if has_either_direction {
        // Delegate to specialized handler that generates UNION for undirected hops
        return generate_pc_correlated_subquery_with_either(pc_meta, schema, cte_column_map);
    }

    // For each hop, find the matching edge table in schema
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new(); // (db_table, alias, hop_info)

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        // Determine from/to labels based on direction
        let (mut from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    // Incoming: (end)<-[:REL]-(start) → in schema terms from=start, to=end
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => {
                    // Outgoing: (start)-[:REL]->(end)
                    (hop.start_label.clone(), hop.end_label.clone())
                }
            };

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();

        let rel_type = hop.rel_type.as_deref();

        // Find matching edge table
        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            // Join this hop to the previous one through the shared node
            // The previous hop's to_id connects to this hop's from_id
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;

            // Find the join column: prev hop's "to" side = this hop's "from" side
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);

            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Add WHERE conditions for each correlation variable
    for cv in &pc_meta.correlation_vars {
        // Find which hop and which side (from/to) this variable connects to
        let (hop_idx, is_start) = match &cv.pattern_position {
            PatternPosition::StartOfHop(idx) => (*idx, true),
            PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];

        // Determine which edge column to use for this correlation variable
        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        // Find the CTE column for this correlation variable's ID
        let cte_col =
            find_cte_column_for_correlation_var(&cv.var_name, &cv.label, schema, cte_column_map);

        if let Some(cte_ref) = cte_col {
            where_conditions.push(format!("{}.{} = {}", edge_alias, edge_col, cte_ref));
        } else {
            log::warn!(
                "⚠️ Could not find CTE column for correlation var '{}' (label='{}')",
                cv.var_name,
                cv.label
            );
        }
    }

    // Handle WHERE clause from pattern comprehension
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Build final SQL
    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    Some(format!(
        "(SELECT COUNT(*) FROM {}{}{})",
        from_clause, joins_str, where_str
    ))
}

/// Result of generating a pre-aggregated CTE for a pattern comprehension.
struct PcCteResult {
    /// The CTE body SQL (SELECT ... FROM ... GROUP BY ...)
    cte_sql: String,
    /// Correlation columns: (var_name, var_label, cte_col_alias) — e.g., ("tag", "Tag", "corr_0")
    correlation_columns: Vec<(String, String, String)>,
    /// The result column name in the CTE (always "result")
    result_column: String,
}

/// Generate a pre-aggregated CTE SQL for a pattern comprehension.
///
/// Instead of a correlated subquery like:
/// ```sql
/// (SELECT COUNT(*) FROM edge WHERE TagId = outer.tag_id AND PersonId = outer.person_id)
/// ```
///
/// Generates a non-correlated CTE:
/// ```sql
/// SELECT TagId AS corr_0, PersonId AS corr_1, COUNT(*) AS result
/// FROM edge_table AS __r0
/// INNER JOIN ... AS __r1 ON ...
/// WHERE date_filters...
/// GROUP BY TagId, PersonId
/// ```
///
/// The caller then adds a LEFT JOIN from the main CTE to this PC CTE.
fn generate_pattern_comprehension_cte(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
) -> Option<PcCteResult> {
    use crate::query_planner::logical_plan::{ConnectedPatternInfo, PatternPosition};

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    // Skip list comprehension patterns — they use arrayCount() instead
    if pc_meta.list_constraint.is_some() {
        return None;
    }

    log::info!(
        "🔧 generate_pattern_comprehension_cte: {} hops, {} correlation vars",
        pc_meta.pattern_hops.len(),
        pc_meta.correlation_vars.len(),
    );

    // Check for Direction::Either — need UNION ALL of both directions
    let has_either_direction = pc_meta
        .pattern_hops
        .iter()
        .any(|h| h.direction == crate::query_planner::logical_expr::Direction::Either);

    if has_either_direction {
        return generate_pc_cte_with_either(pc_meta, schema);
    }

    // For each hop, find the matching edge table in schema
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new();

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        let (from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => (hop.start_label.clone(), hop.end_label.clone()),
            };

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();
        let rel_type = hop.rel_type.as_deref();

        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for CTE hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);
            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Build SELECT columns for correlation variables (these become GROUP BY columns)
    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();
    let mut correlation_columns: Vec<(String, String, String)> = Vec::new();

    for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
        let (hop_idx, is_start) = match &cv.pattern_position {
            PatternPosition::StartOfHop(idx) => (*idx, true),
            PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];

        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        let corr_alias = format!("corr_{}", cv_idx);
        select_cols.push(format!("{}.{} AS {}", edge_alias, edge_col, corr_alias));
        group_by_cols.push(format!("{}.{}", edge_alias, edge_col));
        correlation_columns.push((cv.var_name.clone(), cv.label.clone(), corr_alias));
    }

    // Handle WHERE clause from pattern comprehension
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Build final CTE SQL
    select_cols.push("COUNT(*) AS result".to_string());

    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    let group_by_str = if group_by_cols.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_by_cols.join(", "))
    };

    let cte_sql = format!(
        "SELECT {} FROM {}{}{}{}",
        select_cols.join(", "),
        from_clause,
        joins_str,
        where_str,
        group_by_str
    );

    log::info!("🔧 PC CTE SQL: {}", &cte_sql[..cte_sql.len().min(300)]);

    Some(PcCteResult {
        cte_sql,
        correlation_columns,
        result_column: "result".to_string(),
    })
}

/// Generate a pre-aggregated CTE for pattern comprehensions with Direction::Either.
/// Creates UNION ALL of both direction variants, wrapped in an outer GROUP BY.
fn generate_pc_cte_with_either(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
) -> Option<PcCteResult> {
    use crate::query_planner::logical_plan::PatternPosition;

    // Build direction variants (same logic as correlated subquery path)
    let mut direction_variants: Vec<Vec<crate::query_planner::logical_expr::Direction>> =
        vec![vec![]];

    for hop in &pc_meta.pattern_hops {
        if hop.direction == crate::query_planner::logical_expr::Direction::Either {
            let mut new_variants = Vec::new();
            for variant in &direction_variants {
                let mut v_out = variant.clone();
                v_out.push(crate::query_planner::logical_expr::Direction::Outgoing);
                new_variants.push(v_out);

                let mut v_in = variant.clone();
                v_in.push(crate::query_planner::logical_expr::Direction::Incoming);
                new_variants.push(v_in);
            }
            direction_variants = new_variants;
        } else {
            for variant in &mut direction_variants {
                variant.push(hop.direction.clone());
            }
        }
    }

    // Determine correlation column aliases (consistent across all variants)
    let mut correlation_columns: Vec<(String, String, String)> = Vec::new();
    for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
        let corr_alias = format!("corr_{}", cv_idx);
        correlation_columns.push((cv.var_name.clone(), cv.label.clone(), corr_alias));
    }

    let mut union_parts: Vec<String> = Vec::new();

    for directions in &direction_variants {
        let mut edge_tables: Vec<(
            String,
            String,
            &crate::query_planner::logical_plan::ConnectedPatternInfo,
            crate::query_planner::logical_expr::Direction,
        )> = Vec::new();

        let mut all_found = true;
        for (hop_idx, (hop, dir)) in pc_meta
            .pattern_hops
            .iter()
            .zip(directions.iter())
            .enumerate()
        {
            let rel_alias = format!("__r{}", hop_idx);

            let (from_label, to_label) = match dir {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.as_deref(), hop.start_label.as_deref())
                }
                _ => (hop.start_label.as_deref(), hop.end_label.as_deref()),
            };

            let rel_type = hop.rel_type.as_deref();
            let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

            if let Some(table) = db_table {
                edge_tables.push((table, rel_alias, hop, dir.clone()));
            } else {
                if *dir == crate::query_planner::logical_expr::Direction::Outgoing {
                    let db_table_rev =
                        find_edge_table_in_schema(schema, rel_type, to_label, from_label);
                    if let Some(table) = db_table_rev {
                        edge_tables.push((
                            table,
                            format!("__r{}", hop_idx),
                            hop,
                            crate::query_planner::logical_expr::Direction::Incoming,
                        ));
                        continue;
                    }
                }
                all_found = false;
                break;
            }
        }

        if !all_found {
            continue;
        }

        // Build FROM + JOINs for this direction variant
        let mut from_clause = String::new();
        let mut join_clauses: Vec<String> = Vec::new();
        let mut where_conditions: Vec<String> = Vec::new();

        for (idx, (db_table, alias, _hop, _dir)) in edge_tables.iter().enumerate() {
            if idx == 0 {
                from_clause = format!("{} AS {}", db_table, alias);
            } else {
                let prev_alias = &edge_tables[idx - 1].1;
                let prev_hop = edge_tables[idx - 1].2;
                let prev_dir = &edge_tables[idx - 1].3;

                let prev_to_col = find_edge_id_column_with_direction(
                    schema,
                    &edge_tables[idx - 1].0,
                    false,
                    prev_hop,
                    prev_dir,
                );
                let curr_from_col =
                    find_edge_id_column_with_direction(schema, db_table, true, _hop, _dir);

                join_clauses.push(format!(
                    "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                    db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
                ));
            }
        }

        // SELECT columns for correlation variables
        let mut select_cols: Vec<String> = Vec::new();
        for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
            let (hop_idx, is_start) = match &cv.pattern_position {
                PatternPosition::StartOfHop(idx) => (*idx, true),
                PatternPosition::EndOfHop(idx) => (*idx, false),
            };

            if hop_idx >= edge_tables.len() {
                continue;
            }

            let (_, edge_alias, hop_info, dir) = &edge_tables[hop_idx];
            let edge_col = find_edge_id_column_with_direction(
                schema,
                &edge_tables[hop_idx].0,
                is_start,
                hop_info,
                dir,
            );

            let corr_alias = format!("corr_{}", cv_idx);
            select_cols.push(format!("{}.{} AS {}", edge_alias, edge_col, corr_alias));
        }

        // WHERE clause
        if let Some(ref where_expr) = pc_meta.where_clause {
            if let Some(where_sql) = render_pc_where_clause(
                where_expr,
                &pc_meta.pattern_hops,
                &edge_tables
                    .iter()
                    .map(|(t, a, h, _)| (t.clone(), a.clone(), *h))
                    .collect::<Vec<_>>(),
                schema,
                &mut join_clauses,
            ) {
                where_conditions.push(where_sql);
            }
        }

        let where_str = if where_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_conditions.join(" AND "))
        };

        let joins_str = if join_clauses.is_empty() {
            String::new()
        } else {
            format!(" {}", join_clauses.join(" "))
        };

        union_parts.push(format!(
            "SELECT {} FROM {}{}{}",
            select_cols.join(", "),
            from_clause,
            joins_str,
            where_str,
        ));
    }

    if union_parts.is_empty() {
        return None;
    }

    // Build the corr column aliases for the outer GROUP BY
    let corr_aliases: Vec<String> = correlation_columns
        .iter()
        .map(|(_, _, alias)| alias.clone())
        .collect();

    let cte_sql = if union_parts.len() == 1 {
        // Single variant — wrap in subquery with outer aggregation
        format!(
            "SELECT {}, COUNT(*) AS result FROM ({}) AS __u GROUP BY {}",
            corr_aliases.join(", "),
            union_parts[0],
            corr_aliases.join(", ")
        )
    } else {
        // Multiple variants — wrap UNION ALL in subquery with outer GROUP BY
        let inner_union = union_parts.join(" UNION ALL ");
        format!(
            "SELECT {}, COUNT(*) AS result FROM ({}) AS __u GROUP BY {}",
            corr_aliases.join(", "),
            inner_union,
            corr_aliases.join(", ")
        )
    };

    log::info!(
        "🔧 PC CTE (Either) SQL: {}",
        &cte_sql[..cte_sql.len().min(300)]
    );

    Some(PcCteResult {
        cte_sql,
        correlation_columns,
        result_column: "result".to_string(),
    })
}

/// Generate an `arrayCount()` expression for list comprehension patterns.
///
/// For `size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])`:
/// ```sql
/// arrayCount(
///   x -> (x, person_id_col) IN (
///     SELECT __r0.PostId, __r1.PersonId
///     FROM Post_hasTag_Tag AS __r0
///     INNER JOIN Person_hasInterest_Tag AS __r1 ON __r0.TagId = __r1.TagId
///   ),
///   posts_col
/// )
/// ```
///
/// This avoids correlated subqueries, which fail with ClickHouse's
/// "Cannot clone Union plan step" error when the outer query has UNION ALL.
fn generate_list_comp_array_count(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::ConnectedPatternInfo;

    let lc = pc_meta.list_constraint.as_ref()?;

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    log::info!(
        "🔧 generate_list_comp_array_count: {} hops, list_alias='{}', source_label={:?}",
        pc_meta.pattern_hops.len(),
        lc.list_alias,
        lc.source_label
    );

    // Build edge tables chain (same logic as correlated subquery path)
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new();

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        let (mut from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => (hop.start_label.clone(), hop.end_label.clone()),
            };

        // Override first hop's from_label with list source label
        if hop_idx == 0 && from_label_owned.is_none() {
            if let Some(ref src_label) = lc.source_label {
                from_label_owned = Some(src_label.clone());
            }
        }

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();
        let rel_type = hop.rel_type.as_deref();

        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);
        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for arrayCount hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);
            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Find the list element column (first hop's from_id — the iteration variable)
    let list_element_col = {
        let (_, first_alias, first_hop) = &edge_tables[0];
        let col = find_edge_id_column(schema, &edge_tables[0].0, true, first_hop);
        format!("{}.{}", first_alias, col)
    };

    // Find correlation variable columns (from edge tables) — go into the tuple
    let mut corr_edge_cols: Vec<String> = Vec::new();
    let mut corr_outer_cols: Vec<String> = Vec::new();

    for cv in &pc_meta.correlation_vars {
        let (hop_idx, is_start) = match &cv.pattern_position {
            crate::query_planner::logical_plan::PatternPosition::StartOfHop(idx) => (*idx, true),
            crate::query_planner::logical_plan::PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];
        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        corr_edge_cols.push(format!("{}.{}", edge_alias, edge_col));

        // Find outer column reference for this correlation variable
        let cte_col =
            find_cte_column_for_correlation_var(&cv.var_name, &cv.label, schema, cte_column_map);
        if let Some(cte_ref) = cte_col {
            corr_outer_cols.push(cte_ref);
        } else {
            log::warn!(
                "⚠️ No CTE column for correlation var '{}' in arrayCount — falling back",
                cv.var_name
            );
            return None;
        }
    }

    // Find list array column reference
    let list_col = find_cte_column_for_list_alias(&lc.list_alias, cte_column_map);
    let list_col = match list_col {
        Some(c) => c,
        None => {
            log::warn!(
                "⚠️ No CTE column for list alias '{}' in arrayCount",
                lc.list_alias
            );
            return None;
        }
    };

    // Handle additional WHERE clause from pattern
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Build the inner SELECT columns: list_element_col, then correlation columns
    let mut select_cols = vec![list_element_col.clone()];
    select_cols.extend(corr_edge_cols);

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    // Build the tuple for arrayCount lambda
    let lambda_tuple = if corr_outer_cols.is_empty() {
        "x".to_string()
    } else {
        let mut parts = vec!["x".to_string()];
        parts.extend(corr_outer_cols);
        format!("({})", parts.join(", "))
    };

    let inner_select = format!(
        "SELECT {} FROM {}{}{}",
        select_cols.join(", "),
        from_clause,
        joins_str,
        where_str
    );

    let result = format!(
        "arrayCount(x -> {} IN ({}), {})",
        lambda_tuple, inner_select, list_col
    );

    log::info!(
        "🔧 arrayCount expression: {}",
        &result[..result.len().min(300)]
    );

    Some(result)
}

/// Generate a correlated subquery for pattern comprehensions that contain
/// `Direction::Either` (undirected) hops. For each undirected hop, we generate
/// two subqueries (forward and reverse direction) and UNION ALL them, then
/// wrap with an outer SELECT COUNT(*).
///
/// For a single-hop pattern like `(person)-[:KNOWS]-(friend)`:
/// ```sql
/// (SELECT COUNT(*) FROM (
///     SELECT 1 FROM ldbc.Person_knows_Person AS __r0
///     WHERE __r0.Person1Id = cte.person_id
///   UNION ALL
///     SELECT 1 FROM ldbc.Person_knows_Person AS __r0
///     WHERE __r0.Person2Id = cte.person_id
/// ) AS __u)
/// ```
fn generate_pc_correlated_subquery_with_either(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::PatternPosition;

    // For Direction::Either, we create two copies of the pattern:
    // one treating it as Outgoing, one as Incoming.
    // Both share the same edge table but use different from/to ID columns.
    let mut direction_variants: Vec<Vec<crate::query_planner::logical_expr::Direction>> =
        vec![vec![]];

    for hop in &pc_meta.pattern_hops {
        if hop.direction == crate::query_planner::logical_expr::Direction::Either {
            // Fork: each existing variant gets duplicated with Outgoing and Incoming
            let mut new_variants = Vec::new();
            for variant in &direction_variants {
                let mut v_out = variant.clone();
                v_out.push(crate::query_planner::logical_expr::Direction::Outgoing);
                new_variants.push(v_out);

                let mut v_in = variant.clone();
                v_in.push(crate::query_planner::logical_expr::Direction::Incoming);
                new_variants.push(v_in);
            }
            direction_variants = new_variants;
        } else {
            for variant in &mut direction_variants {
                variant.push(hop.direction.clone());
            }
        }
    }

    log::info!(
        "🔧 Direction::Either expansion: {} direction variants for {} hops",
        direction_variants.len(),
        pc_meta.pattern_hops.len()
    );

    let mut union_parts: Vec<String> = Vec::new();

    for directions in &direction_variants {
        // Build edge_tables for this direction variant
        let mut edge_tables: Vec<(
            String,
            String,
            &crate::query_planner::logical_plan::ConnectedPatternInfo,
            crate::query_planner::logical_expr::Direction,
        )> = Vec::new();

        let mut all_found = true;
        for (hop_idx, (hop, dir)) in pc_meta
            .pattern_hops
            .iter()
            .zip(directions.iter())
            .enumerate()
        {
            let rel_alias = format!("__r{}", hop_idx);

            let (from_label, to_label) = match dir {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.as_deref(), hop.start_label.as_deref())
                }
                _ => (hop.start_label.as_deref(), hop.end_label.as_deref()),
            };

            let rel_type = hop.rel_type.as_deref();
            let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

            if let Some(table) = db_table {
                edge_tables.push((table, rel_alias, hop, dir.clone()));
            } else {
                // Try reversed labels for Either direction that couldn't find forward match
                if *dir == crate::query_planner::logical_expr::Direction::Outgoing {
                    let db_table_rev =
                        find_edge_table_in_schema(schema, rel_type, to_label, from_label);
                    if let Some(table) = db_table_rev {
                        // Found with reversed labels — treat as Incoming for this variant
                        edge_tables.push((
                            table,
                            format!("__r{}", hop_idx),
                            hop,
                            crate::query_planner::logical_expr::Direction::Incoming,
                        ));
                        continue;
                    }
                }
                log::warn!(
                    "⚠️ No edge table for Either hop {}: rel={:?}, from={:?}, to={:?}",
                    hop_idx,
                    rel_type,
                    from_label,
                    to_label
                );
                all_found = false;
                break;
            }
        }

        if !all_found {
            continue;
        }

        // Build FROM + JOINs + WHERE for this direction variant
        let mut from_clause = String::new();
        let mut join_clauses: Vec<String> = Vec::new();
        let mut where_conditions: Vec<String> = Vec::new();

        for (idx, (db_table, alias, _hop, _dir)) in edge_tables.iter().enumerate() {
            if idx == 0 {
                from_clause = format!("{} AS {}", db_table, alias);
            } else {
                let prev_alias = &edge_tables[idx - 1].1;
                let prev_hop = edge_tables[idx - 1].2;
                let prev_dir = &edge_tables[idx - 1].3;

                // Create a temporary ConnectedPatternInfo with the effective direction
                let prev_to_col = find_edge_id_column_with_direction(
                    schema,
                    &edge_tables[idx - 1].0,
                    false,
                    prev_hop,
                    prev_dir,
                );
                let curr_from_col =
                    find_edge_id_column_with_direction(schema, db_table, true, _hop, _dir);

                join_clauses.push(format!(
                    "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                    db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
                ));
            }
        }

        // WHERE conditions for correlation variables
        for cv in &pc_meta.correlation_vars {
            let (hop_idx, is_start) = match &cv.pattern_position {
                PatternPosition::StartOfHop(idx) => (*idx, true),
                PatternPosition::EndOfHop(idx) => (*idx, false),
            };

            if hop_idx >= edge_tables.len() {
                continue;
            }

            let (_, edge_alias, hop_info, dir) = &edge_tables[hop_idx];
            let edge_col = find_edge_id_column_with_direction(
                schema,
                &edge_tables[hop_idx].0,
                is_start,
                hop_info,
                dir,
            );

            let cte_col = find_cte_column_for_correlation_var(
                &cv.var_name,
                &cv.label,
                schema,
                cte_column_map,
            );

            if let Some(cte_ref) = cte_col {
                where_conditions.push(format!("{}.{} = {}", edge_alias, edge_col, cte_ref));
            }
        }

        // WHERE clause from pattern comprehension
        if let Some(ref where_expr) = pc_meta.where_clause {
            if let Some(where_sql) = render_pc_where_clause(
                where_expr,
                &pc_meta.pattern_hops,
                &edge_tables
                    .iter()
                    .map(|(t, a, h, _)| (t.clone(), a.clone(), *h))
                    .collect::<Vec<_>>(),
                schema,
                &mut join_clauses,
            ) {
                where_conditions.push(where_sql);
            }
        }

        let where_str = if where_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_conditions.join(" AND "))
        };

        let joins_str = if join_clauses.is_empty() {
            String::new()
        } else {
            format!(" {}", join_clauses.join(" "))
        };

        union_parts.push(format!(
            "SELECT 1 FROM {}{}{}",
            from_clause, joins_str, where_str
        ));
    }

    if union_parts.is_empty() {
        return None;
    }

    if union_parts.len() == 1 {
        // Single variant (no Either hops were actually encountered, shouldn't happen but safe)
        Some(format!(
            "(SELECT COUNT(*) FROM ({}) AS __u)",
            union_parts[0]
        ))
    } else {
        let union_sql = union_parts.join(" UNION ALL ");
        Some(format!("(SELECT COUNT(*) FROM ({}) AS __u)", union_sql))
    }
}

/// Like `find_edge_id_column` but takes an explicit direction parameter
/// instead of reading from the hop's direction field. Used by the Either
/// direction handler which overrides the hop's original direction.
fn find_edge_id_column_with_direction(
    schema: &GraphSchema,
    db_table: &str,
    is_from: bool,
    _hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
    direction: &crate::query_planner::logical_expr::Direction,
) -> String {
    for (_, rel_schema) in schema.get_relationships_schemas() {
        let table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        if table == db_table {
            let effective_is_from = match direction {
                crate::query_planner::logical_expr::Direction::Incoming => !is_from,
                _ => is_from,
            };

            return if effective_is_from {
                rel_schema.from_id.first_column().to_string()
            } else {
                rel_schema.to_id.first_column().to_string()
            };
        }
    }

    if is_from {
        "from_id".to_string()
    } else {
        "to_id".to_string()
    }
}

/// Find an edge table in schema matching the given rel_type, from_label, and to_label.
/// Returns fully qualified table name (database.table).
fn find_edge_table_in_schema(
    schema: &GraphSchema,
    rel_type: Option<&str>,
    from_label: Option<&str>,
    to_label: Option<&str>,
) -> Option<String> {
    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());

    let mut best_match: Option<String> = None;

    for (rel_key, rel_schema) in &sorted_rels {
        let key_rel_name = rel_key.split("::").next().unwrap_or(rel_key);

        // Filter by relationship type
        if let Some(rt) = rel_type {
            if !key_rel_name.eq_ignore_ascii_case(rt) {
                continue;
            }
        }

        let db_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);

        // If from_label/to_label specified, match them
        let from_matches = from_label.is_none()
            || from_label.is_some_and(|fl| {
                rel_schema.from_node.eq_ignore_ascii_case(fl) || rel_schema.from_node == "$any"
            });
        let to_matches = to_label.is_none()
            || to_label.is_some_and(|tl| {
                rel_schema.to_node.eq_ignore_ascii_case(tl) || rel_schema.to_node == "$any"
            });

        if from_matches && to_matches {
            // Prefer exact match over $any
            let is_exact = from_label
                .is_some_and(|fl| rel_schema.from_node.eq_ignore_ascii_case(fl))
                && to_label.is_some_and(|tl| rel_schema.to_node.eq_ignore_ascii_case(tl));

            if is_exact {
                return Some(db_table);
            }
            if best_match.is_none() {
                best_match = Some(db_table);
            }
        }
    }

    best_match
}

/// Find the from_id or to_id column name for an edge table.
/// `is_from` = true → from_id column, false → to_id column.
fn find_edge_id_column(
    schema: &GraphSchema,
    db_table: &str,
    is_from: bool,
    hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
) -> String {
    let sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();

    for (_, rel_schema) in &sorted_rels {
        let table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        if table == db_table {
            // Account for direction: for Incoming, the AST start is actually the schema's to
            let effective_is_from = match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => !is_from,
                _ => is_from,
            };

            return if effective_is_from {
                rel_schema.from_id.first_column().to_string()
            } else {
                rel_schema.to_id.first_column().to_string()
            };
        }
    }

    // Fallback
    if is_from {
        "from_id".to_string()
    } else {
        "to_id".to_string()
    }
}

/// Find the CTE column reference for a correlation variable.
/// Looks up in cte_column_map by (var_name, "id") pattern.
fn find_cte_column_for_correlation_var(
    var_name: &str,
    label: &str,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    // Try common ID column patterns
    let id_col = if let Ok(ns) = schema.node_schema(label) {
        ns.node_id.id.first_column().to_string()
    } else {
        "id".to_string()
    };

    // Look up (var_name, id_col) in the CTE column map
    if let Some(cte_ref) = cte_column_map.get(&(var_name.to_string(), id_col.clone())) {
        return Some(cte_ref.clone());
    }

    // Also try just "id"
    if id_col != "id" {
        if let Some(cte_ref) = cte_column_map.get(&(var_name.to_string(), "id".to_string())) {
            return Some(cte_ref.clone());
        }
    }

    log::debug!(
        "🔍 CTE column map lookup failed for var='{}', id_col='{}'. Map keys: {:?}",
        var_name,
        id_col,
        cte_column_map.keys().collect::<Vec<_>>()
    );

    None
}

/// Find the CTE column reference for a list alias (e.g., "posts" from `collect(post) AS posts`).
/// Looks for the alias as a direct key in the CTE column map — list aliases are scalar columns
/// (arrays), not node aliases with property sub-columns.
fn find_cte_column_for_list_alias(
    list_alias: &str,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    // List aliases like "posts" are stored as (alias, "*") or (alias, alias) in CTE column maps.
    // Try several patterns used by the CTE naming system.

    // Pattern 1: (alias, "*") — wildcard property
    if let Some(cte_ref) = cte_column_map.get(&(list_alias.to_string(), "*".to_string())) {
        return Some(cte_ref.clone());
    }

    // Pattern 2: (alias, alias) — self-reference
    if let Some(cte_ref) = cte_column_map.get(&(list_alias.to_string(), list_alias.to_string())) {
        return Some(cte_ref.clone());
    }

    // Pattern 3: Search for any key where the first element matches the alias.
    // Collect all matches to detect ambiguity — only return if exactly one match.
    let matches: Vec<&String> = cte_column_map
        .iter()
        .filter(|((alias, _prop), _)| alias == list_alias)
        .map(|(_, cte_ref)| cte_ref)
        .collect();
    match matches.len() {
        1 => return Some(matches[0].clone()),
        n if n > 1 => {
            log::warn!(
                "Ambiguous CTE column for list alias '{}': {} matches found. Skipping.",
                list_alias,
                n
            );
        }
        _ => {}
    }

    log::debug!(
        "🔍 List alias CTE column lookup failed for '{}'. Map keys: {:?}",
        list_alias,
        cte_column_map.keys().collect::<Vec<_>>()
    );

    None
}

/// Render a WHERE clause from a LogicalExpr for use inside a correlated subquery.
/// Resolves property accesses to schema-mapped column names on edge table aliases.
/// May add additional JOINs for intermediate node tables referenced in the WHERE clause.
fn render_pc_where_clause(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    schema: &GraphSchema,
    join_clauses: &mut Vec<String>,
) -> Option<String> {
    // Build a map of alias → (label, node table alias) for intermediate nodes in the pattern
    // that might be referenced in the WHERE clause
    let mut node_alias_map: HashMap<String, (String, String)> = HashMap::new(); // alias → (label, sql_alias)
    let mut node_joins_added: HashSet<String> = HashSet::new();

    for (hop_idx, hop) in pattern_hops.iter().enumerate() {
        // Check start node
        if let (Some(ref alias), Some(ref label)) = (&hop.start_alias, &hop.start_label) {
            if !node_alias_map.contains_key(alias.as_str()) {
                let sql_alias = format!("__n{}s", hop_idx);
                node_alias_map.insert(alias.clone(), (label.clone(), sql_alias));
            }
        }
        // Check end node
        if let (Some(ref alias), Some(ref label)) = (&hop.end_alias, &hop.end_label) {
            if !node_alias_map.contains_key(alias.as_str()) {
                let sql_alias = format!("__n{}e", hop_idx);
                node_alias_map.insert(alias.clone(), (label.clone(), sql_alias));
            }
        }
    }

    let sql = render_logical_expr_to_sql(
        expr,
        &node_alias_map,
        pattern_hops,
        edge_tables,
        schema,
        join_clauses,
        &mut node_joins_added,
    );

    if sql.is_empty() {
        None
    } else {
        Some(sql)
    }
}

/// Recursively render a LogicalExpr to SQL for use in a correlated subquery WHERE clause.
fn render_logical_expr_to_sql(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    node_alias_map: &HashMap<String, (String, String)>,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    schema: &GraphSchema,
    join_clauses: &mut Vec<String>,
    node_joins_added: &mut HashSet<String>,
) -> String {
    use crate::query_planner::logical_expr::LogicalExpr;

    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            let alias = &pa.table_alias.0;
            let prop_name = match &pa.column {
                crate::graph_catalog::expression_parser::PropertyValue::Column(col) => col.clone(),
                other => format!("{:?}", other),
            };

            // Look up alias in node_alias_map to find the label and SQL alias
            if let Some((label, sql_alias)) = node_alias_map.get(alias) {
                // Add JOIN for this node table if not already added
                if !node_joins_added.contains(alias) {
                    if let Ok(ns) = schema.node_schema(label) {
                        let node_table = format!("{}.{}", ns.database, ns.table_name);
                        let node_id_col = ns.node_id.id.first_column();

                        // Find which edge table connects to this node
                        if let Some(join_condition) = find_node_edge_join_condition(
                            alias,
                            pattern_hops,
                            edge_tables,
                            node_id_col,
                            sql_alias,
                            schema,
                        ) {
                            join_clauses.push(format!(
                                "INNER JOIN {} AS {} ON {}",
                                node_table, sql_alias, join_condition
                            ));
                            node_joins_added.insert(alias.clone());
                        }
                    }
                }

                // Resolve property name through schema
                let db_col = if let Ok(ns) = schema.node_schema(label) {
                    ns.property_mappings
                        .get(&prop_name)
                        .map(|pv| pv.raw().to_string())
                        .unwrap_or(prop_name)
                } else {
                    prop_name
                };

                format!("{}.{}", sql_alias, db_col)
            } else {
                // Not a pattern node - might be an outer reference, use raw
                format!("{}.{}", alias, prop_name)
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            use crate::query_planner::logical_expr::Operator as Op;
            let op_str = match op.operator {
                Op::And => " AND ",
                Op::Or => " OR ",
                Op::LessThan => " < ",
                Op::GreaterThan => " > ",
                Op::Equal => " = ",
                Op::NotEqual => " <> ",
                Op::LessThanEqual => " <= ",
                Op::GreaterThanEqual => " >= ",
                Op::Addition => " + ",
                Op::Subtraction => " - ",
                Op::Multiplication => " * ",
                Op::Division => " / ",
                Op::ModuloDivision => " % ",
                Op::Exponentiation => " ^ ",
                Op::In => " IN ",
                Op::NotIn => " NOT IN ",
                Op::StartsWith | Op::EndsWith | Op::Contains | Op::RegexMatch => {
                    // These are function-like operators handled specially below
                    ""
                }
                Op::Not => "NOT ",
                Op::IsNull => " IS NULL",
                Op::IsNotNull => " IS NOT NULL",
                Op::Distinct => " ?? ",
            };

            // Handle function-like operators
            if matches!(
                op.operator,
                Op::StartsWith | Op::EndsWith | Op::Contains | Op::RegexMatch
            ) && op.operands.len() == 2
            {
                let left = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                let right = render_logical_expr_to_sql(
                    &op.operands[1],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return match op.operator {
                    Op::StartsWith => format!("startsWith({}, {})", left, right),
                    Op::EndsWith => format!("endsWith({}, {})", left, right),
                    Op::Contains => format!("position({}, {}) > 0", left, right),
                    Op::RegexMatch => format!("match({}, {})", left, right),
                    _ => unreachable!(),
                };
            }

            // Unary postfix operators (IS NULL, IS NOT NULL)
            if matches!(op.operator, Op::IsNull | Op::IsNotNull) && op.operands.len() == 1 {
                let operand = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return format!("{}{}", operand, op_str);
            }

            // Unary prefix operator (NOT)
            if op.operator == Op::Not && op.operands.len() == 1 {
                let operand = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return format!("{}{}", op_str, operand);
            }

            if op.operands.len() == 2 {
                let left = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                let right = render_logical_expr_to_sql(
                    &op.operands[1],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                format!("{}{}{}", left, op_str, right)
            } else {
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|o| {
                        render_logical_expr_to_sql(
                            o,
                            node_alias_map,
                            pattern_hops,
                            edge_tables,
                            schema,
                            join_clauses,
                            node_joins_added,
                        )
                    })
                    .collect();
                rendered.join(op_str)
            }
        }
        LogicalExpr::Literal(lit) => match lit {
            crate::query_planner::logical_expr::Literal::Integer(i) => i.to_string(),
            crate::query_planner::logical_expr::Literal::Float(f) => f.to_string(),
            crate::query_planner::logical_expr::Literal::String(s) => {
                format!("'{}'", s.replace('\'', "\\'"))
            }
            crate::query_planner::logical_expr::Literal::Boolean(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            _ => "NULL".to_string(),
        },
        LogicalExpr::Parameter(p) => format!("${}", p),
        _ => {
            log::warn!(
                "⚠️ Unhandled LogicalExpr variant in PC WHERE clause: {:?}",
                expr
            );
            String::new()
        }
    }
}

/// Find the JOIN condition to connect a node table to an edge table in the pattern.
fn find_node_edge_join_condition(
    node_alias: &str,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    node_id_col: &str,
    node_sql_alias: &str,
    schema: &GraphSchema,
) -> Option<String> {
    for (hop_idx, hop) in pattern_hops.iter().enumerate() {
        if hop_idx >= edge_tables.len() {
            continue;
        }
        let (ref db_table, ref edge_alias, _) = edge_tables[hop_idx];

        // Check if this node is the start or end of this hop
        let is_start = hop.start_alias.as_deref() == Some(node_alias);
        let is_end = hop.end_alias.as_deref() == Some(node_alias);

        if is_start || is_end {
            let edge_col = find_edge_id_column_for_node(schema, db_table, hop, is_start);
            return Some(format!(
                "{}.{} = {}.{}",
                node_sql_alias, node_id_col, edge_alias, edge_col
            ));
        }
    }
    None
}

/// Find the edge ID column that corresponds to a node position (start/end) in a hop.
fn find_edge_id_column_for_node(
    schema: &GraphSchema,
    db_table: &str,
    hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
    is_start: bool,
) -> String {
    let sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();

    for (_, rel_schema) in &sorted_rels {
        let table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        if table == db_table {
            // For Incoming direction, start/end are swapped relative to schema from/to
            let effective_is_from = match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => !is_start,
                _ => is_start,
            };

            return if effective_is_from {
                rel_schema.from_id.first_column().to_string()
            } else {
                rel_schema.to_id.first_column().to_string()
            };
        }
    }

    if is_start {
        "from_id".to_string()
    } else {
        "to_id".to_string()
    }
}

/// Replace count(*) placeholder expressions in a RenderPlan's SELECT items
/// with correlated subquery SQL strings.
///
/// Walks SELECT items in order, finds AggregateFnCall("count", [Star|Raw("*")])
/// expressions that were placeholder replacements from pattern comprehension rewriting,
/// and replaces them with Raw(subquery_sql).
fn replace_count_star_placeholders_in_select(
    select_items: &mut [SelectItem],
    pc_subqueries: &[String],
) {
    let mut pc_idx = 0;

    for item in select_items.iter_mut() {
        replace_count_star_in_expr(&mut item.expression, pc_subqueries, &mut pc_idx);
    }
}

/// Add correlated columns from pattern comprehensions to a render plan's SELECT.
/// ClickHouse needs all correlated columns in the outer SELECT for decorrelation.
/// This adds columns referenced by correlated subqueries that aren't already present.
fn add_correlated_columns_to_select(
    plan: &mut RenderPlan,
    pattern_comprehensions: &[crate::query_planner::logical_plan::PatternComprehensionMeta],
) {
    // Collect needed columns from pattern comprehension metadata
    let mut needed_cols: Vec<(String, String)> = Vec::new(); // (cte_col_ref, alias)
    let from_alias = if let FromTableItem(Some(ref from)) = plan.from {
        from.alias.clone()
    } else {
        None
    };
    let from_alias = match from_alias {
        Some(a) => a,
        None => return,
    };

    // Hoist schema lookup once for all correlation vars (avoids repeated lookups per SELECT item)
    let schema = crate::server::query_context::get_current_schema_with_fallback();

    for pc in pattern_comprehensions {
        // Correlation variables need their ID column in outer SELECT.
        // The ID column might already be present under its real schema name
        // (e.g., p1_a_user_id for User.user_id) rather than the generic p1_a_id.
        for cv in &pc.correlation_vars {
            let generic_id_col =
                crate::utils::cte_column_naming::cte_column_name(&cv.var_name, "id");

            // Resolve the schema's ID column names once per correlation var.
            // node_id.id.columns() returns actual DB column names (e.g., ["user_id"]).
            let id_col_names: Vec<String> = schema
                .as_ref()
                .and_then(|s| s.node_schema(&cv.label).ok())
                .map(|ns| {
                    ns.node_id
                        .id
                        .columns()
                        .into_iter()
                        .map(|c| c.to_string())
                        .collect()
                })
                .unwrap_or_default();

            // Check if ANY ID column for this alias is already in SELECT.
            // Uses the parsed CTE alias property portion (e.g., "user_id" from "p1_a_user_id")
            // to match against schema ID columns, avoiding reliance on RenderExpr internals.
            let already_has_id = plan.select.items.iter().any(|item| {
                if let Some(ref alias) = item.col_alias {
                    // Check generic "id" form (p1_a_id)
                    if alias.0 == generic_id_col {
                        return true;
                    }
                    // Check real ID column form (p1_a_user_id, p1_a_post_id, etc.)
                    // by parsing the alias and comparing the property portion to schema ID columns
                    if let Some((parsed_alias, property)) =
                        crate::utils::cte_column_naming::parse_cte_column(&alias.0)
                    {
                        if parsed_alias == cv.var_name
                            && id_col_names.iter().any(|c| *c == property)
                        {
                            return true;
                        }
                    }
                }
                false
            });

            if !already_has_id {
                // Find the real ID column name from schema instead of using generic "id"
                let (real_col, col_alias) = id_col_names
                    .first()
                    .map(|id_col| {
                        let real = format!("{}.{}", from_alias, id_col);
                        let alias =
                            crate::utils::cte_column_naming::cte_column_name(&cv.var_name, id_col);
                        (real, alias)
                    })
                    .unwrap_or_else(|| {
                        (
                            format!("{}.{}", from_alias, generic_id_col),
                            generic_id_col.clone(),
                        )
                    });
                needed_cols.push((real_col, col_alias));
            }
        }
    }

    // Add missing columns to SELECT
    for (qualified, alias) in needed_cols {
        log::info!(
            "🔧 Adding correlated column to SELECT: {} AS \"{}\"",
            qualified,
            alias
        );
        plan.select.items.push(SelectItem {
            expression: RenderExpr::Raw(qualified),
            col_alias: Some(ColumnAlias(alias)),
        });
    }
}

/// Recursively find and replace count(*) placeholders in a RenderExpr tree.
fn replace_count_star_in_expr(expr: &mut RenderExpr, pc_subqueries: &[String], pc_idx: &mut usize) {
    match expr {
        RenderExpr::AggregateFnCall(agg) => {
            let is_count_star = agg.name.eq_ignore_ascii_case("count") && agg.args.len() == 1 && {
                let arg = &agg.args[0];
                matches!(arg, RenderExpr::Star)
                    || matches!(arg, RenderExpr::Raw(s) if s == "*")
                    || matches!(arg, RenderExpr::Literal(Literal::String(s)) if s == "*")
            };

            if is_count_star && *pc_idx < pc_subqueries.len() {
                log::info!(
                    "🔧 Replacing count(*) placeholder #{} with correlated subquery",
                    pc_idx
                );
                *expr = RenderExpr::Raw(pc_subqueries[*pc_idx].clone());
                *pc_idx += 1;
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                replace_count_star_in_expr(operand, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &mut f.args {
                replace_count_star_in_expr(arg, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref mut e) = case.expr {
                replace_count_star_in_expr(e, pc_subqueries, pc_idx);
            }
            for (ref mut when, ref mut then) in &mut case.when_then {
                replace_count_star_in_expr(when, pc_subqueries, pc_idx);
                replace_count_star_in_expr(then, pc_subqueries, pc_idx);
            }
            if let Some(ref mut e) = case.else_expr {
                replace_count_star_in_expr(e, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                replace_count_star_in_expr(item, pc_subqueries, pc_idx);
            }
        }
        _ => {}
    }
}

/// Build raw SQL for a pattern comprehension CTE.
///
/// Given a node label and direction, finds all matching edge tables in the schema
/// and generates a UNION ALL query that counts/aggregates connections, grouped by node_id.
///
/// Returns SQL like:
/// ```sql
/// SELECT node_id, COUNT(*) AS result FROM (
///   SELECT follower_id AS node_id FROM brahmand.user_follows_bench  -- outgoing
///   UNION ALL
///   SELECT followed_id AS node_id FROM brahmand.user_follows_bench  -- incoming
///   UNION ALL ...
/// ) GROUP BY node_id
/// ```
pub(super) fn build_pattern_comprehension_sql(
    correlation_label: &str,
    direction: &crate::open_cypher_parser::ast::Direction,
    rel_types: &Option<Vec<String>>,
    agg_type: &crate::query_planner::logical_plan::AggregationType,
    schema: &GraphSchema,
    target_label: Option<&str>,
    target_property: Option<&str>,
) -> Option<String> {
    use crate::open_cypher_parser::ast::Direction;
    use crate::query_planner::logical_plan::AggregationType;

    // Resolve target node table/column for property-based aggregation (e.g., collect(f.name))
    let target_join_info = target_label.and_then(|tl| {
        target_property.and_then(|tp| {
            schema.node_schema(tl).ok().map(|ns| {
                let target_table = format!("{}.{}", ns.database, ns.table_name);
                let target_id = ns.node_id.id.to_pipe_joined_sql("__tgt");
                let db_column = ns
                    .property_mappings
                    .get(tp)
                    .map(|pv| pv.raw().to_string())
                    .unwrap_or_else(|| tp.to_string());
                (target_table, target_id, db_column, tl.to_string())
            })
        })
    });

    let mut branches: Vec<String> = Vec::new();

    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());
    for (rel_key, rel_schema) in sorted_rels {
        // Extract base relationship type from key (keys may be "TYPE::From::To")
        let rel_name = rel_key.split("::").next().unwrap_or(rel_key);
        // If specific rel types are requested, filter
        if let Some(types) = rel_types {
            if !types.iter().any(|t| t.eq_ignore_ascii_case(rel_name)) {
                continue;
            }
        }

        let db_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);

        // Build optional type_column filter for polymorphic edges
        let mut where_clauses = Vec::new();
        if let Some(ref type_col) = rel_schema.type_column {
            where_clauses.push(format!("{}.{} = '{}'", db_table, type_col, rel_name));
        }

        // Handle $any (polymorphic) from_node/to_node matching
        let from_matches = rel_schema.from_node.eq_ignore_ascii_case(correlation_label)
            || rel_schema.from_node == "$any";
        let to_matches = rel_schema.to_node.eq_ignore_ascii_case(correlation_label)
            || rel_schema.to_node == "$any";

        // Check outgoing: correlation_label is the from_node
        if (matches!(direction, Direction::Outgoing | Direction::Either)) && from_matches {
            let mut branch_where = where_clauses.clone();
            if rel_schema.from_node == "$any" {
                if let Some(ref from_label_col) = rel_schema.from_label_column {
                    branch_where.push(format!(
                        "{}.{} = '{}'",
                        db_table, from_label_col, correlation_label
                    ));
                }
            }
            let where_str = if branch_where.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", branch_where.join(" AND "))
            };
            // For property aggregation, JOIN the target node table
            if let Some((ref tgt_table, ref _tgt_id, ref tgt_col, ref _tgt_label)) =
                target_join_info
            {
                // Build JOIN condition: edge.to_id = target_node.node_id
                let join_cond = {
                    let edge_cols = rel_schema.to_id.columns();
                    let tgt_ns = schema.node_schema(&rel_schema.to_node).ok();
                    let tgt_cols = tgt_ns.map(|ns| ns.node_id.id.columns()).unwrap_or_default();
                    edge_cols
                        .iter()
                        .zip(tgt_cols.iter())
                        .map(|(e, t)| format!("{} = __tgt.{}", e, t))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                };
                branches.push(format!(
                    "SELECT {} AS node_id, __tgt.{} AS target_prop FROM {} INNER JOIN {} AS __tgt ON {}{}",
                    rel_schema.from_id.to_pipe_joined_sql(""),
                    tgt_col,
                    db_table,
                    tgt_table,
                    join_cond,
                    where_str
                ));
            } else {
                branches.push(format!(
                    "SELECT {} AS node_id FROM {}{}",
                    rel_schema.from_id.to_pipe_joined_sql(""),
                    db_table,
                    where_str
                ));
            }
        }

        // Check incoming: correlation_label is the to_node
        if (matches!(direction, Direction::Incoming | Direction::Either)) && to_matches {
            let mut branch_where = where_clauses.clone();
            if rel_schema.to_node == "$any" {
                if let Some(ref to_label_col) = rel_schema.to_label_column {
                    branch_where.push(format!(
                        "{}.{} = '{}'",
                        db_table, to_label_col, correlation_label
                    ));
                }
            }
            let where_str = if branch_where.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", branch_where.join(" AND "))
            };
            // For property aggregation, JOIN the target (from) node table
            if let Some((ref tgt_table, ref _tgt_id, ref tgt_col, ref _tgt_label)) =
                target_join_info
            {
                let join_cond = {
                    let edge_cols = rel_schema.from_id.columns();
                    let tgt_ns = schema.node_schema(&rel_schema.from_node).ok();
                    let tgt_cols = tgt_ns.map(|ns| ns.node_id.id.columns()).unwrap_or_default();
                    edge_cols
                        .iter()
                        .zip(tgt_cols.iter())
                        .map(|(e, t)| format!("{} = __tgt.{}", e, t))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                };
                branches.push(format!(
                    "SELECT {} AS node_id, __tgt.{} AS target_prop FROM {} INNER JOIN {} AS __tgt ON {}{}",
                    rel_schema.to_id.to_pipe_joined_sql(""),
                    tgt_col,
                    db_table,
                    tgt_table,
                    join_cond,
                    where_str
                ));
            } else {
                branches.push(format!(
                    "SELECT {} AS node_id FROM {}{}",
                    rel_schema.to_id.to_pipe_joined_sql(""),
                    db_table,
                    where_str
                ));
            }
        }
    }

    if branches.is_empty() {
        return None;
    }

    // All branches output a single uniform column (node_id), so UNION ALL is safe.
    // Aggregate outside: COUNT(*) counts all rows per node_id across all edge tables.
    let union_sql = branches.join(" UNION ALL ");
    let agg_fn = match agg_type {
        AggregationType::Count => "COUNT(*)".to_string(),
        AggregationType::GroupArray => {
            if target_join_info.is_some() {
                "groupArray(target_prop)".to_string()
            } else {
                "groupArray(1)".to_string()
            }
        }
        AggregationType::Sum => "SUM(1)".to_string(),
        AggregationType::Avg => "AVG(1)".to_string(),
        AggregationType::Min => "MIN(1)".to_string(),
        AggregationType::Max => "MAX(1)".to_string(),
    };

    Some(format!(
        "SELECT node_id, {} AS result FROM ({}) GROUP BY node_id",
        agg_fn, union_sql
    ))
}

/// Build a RenderExpr for a node's ID, handling composite keys.
/// For single IDs: `alias.col` (PropertyAccess)
/// For composite IDs: `concat(toString(alias.col1), '|', toString(alias.col2))`
pub(super) fn build_node_id_expr_for_join(
    from_alias: &str,
    label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    if let Ok(ns) = schema.node_schema(label) {
        return build_id_render_expr(&ns.node_id.id, from_alias);
    }

    // Fallback: use find_node_id_column_from_schema
    let id_col = find_node_id_column_from_schema("", label, schema);
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(from_alias.to_string()),
        column: PropertyValue::Column(id_col),
    })
}

/// Convert an Identifier to a RenderExpr with the given alias.
/// Single: `alias.col`, Composite: `concat(toString(alias.c1), '|', toString(alias.c2))`
pub(super) fn build_id_render_expr(
    id: &crate::graph_catalog::config::Identifier,
    alias: &str,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    if id.is_composite() {
        let parts: Vec<RenderExpr> = id
            .columns()
            .iter()
            .enumerate()
            .flat_map(|(i, col)| {
                let mut items = Vec::new();
                if i > 0 {
                    items.push(RenderExpr::Literal(Literal::String("|".to_string())));
                }
                items.push(RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "toString".to_string(),
                    args: vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(col.to_string()),
                    })],
                }));
                items
            })
            .collect();
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "concat".to_string(),
            args: parts,
        })
    } else {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(id.first_column().to_string()),
        })
    }
}

/// Find the ID column for a node label in the schema.
/// E.g., for label "User" in the social benchmark, returns "user_id".
pub(super) fn find_node_id_column_from_schema(
    _alias: &str,
    label: &str,
    schema: &GraphSchema,
) -> String {
    // Look through node schemas to find the ID column
    if let Ok(node_schema) = schema.node_schema(label) {
        return node_schema.node_id.id.first_column().to_string();
    }

    // Fallback: look through relationship schemas for from_node/to_node matching
    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());
    for (_, rel_schema) in sorted_rels {
        if rel_schema.from_node.eq_ignore_ascii_case(label) {
            return rel_schema.from_id.first_column().to_string();
        }
        if rel_schema.to_node.eq_ignore_ascii_case(label) {
            return rel_schema.to_id.first_column().to_string();
        }
    }

    // Last resort: generic "id"
    log::debug!(
        "⚠️  Could not find ID column for label '{}', defaulting to 'id'",
        label
    );
    "id".to_string()
}

/// Find the CTE column reference for a correlation variable in the WITH CTE body.
/// Used to build LEFT JOIN ON conditions between the WITH CTE and a PC CTE.
///
/// The correlation variable (e.g., "tag" with label "Tag") needs to resolve to
/// a qualified column reference (e.g., "person_tag.p3_tag_id") in the WITH CTE body.
fn find_pc_cte_join_column(
    var_name: &str,
    label: &str,
    schema: &GraphSchema,
    with_cte_render: &RenderPlan,
    cte_name: &str,
) -> Option<String> {
    // Build the CTE column map from the WITH CTE body
    let cte_col_map = build_cte_column_map(with_cte_render, cte_name);

    // Also include direct JOINs as available references
    let mut col_map = cte_col_map;
    for join in &with_cte_render.joins.0 {
        let join_alias = &join.table_alias;
        col_map
            .entry((join_alias.clone(), "id".to_string()))
            .or_insert_with(|| format!("{}.id", join_alias));
    }

    // Scan FROM's ViewScan property_mapping
    if let FromTableItem(Some(ref from)) = with_cte_render.from {
        if let Some(ref from_alias) = from.alias {
            if let LogicalPlan::ViewScan(ref scan) = from.source.as_ref() {
                for (_prop, col_value) in &scan.property_mapping {
                    if let crate::graph_catalog::expression_parser::PropertyValue::Column(
                        ref col_name,
                    ) = col_value
                    {
                        if let Some((parsed_alias, parsed_property)) = parse_cte_column(col_name) {
                            let qualified = format!("{}.{}", from_alias, col_name);
                            col_map
                                .entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        } else {
                            let qualified = format!("{}.\"{}\"", from_alias, col_name);
                            col_map
                                .entry((col_name.clone(), "id".to_string()))
                                .or_insert(qualified.clone());
                            col_map
                                .entry((col_name.clone(), col_name.clone()))
                                .or_insert(qualified);
                        }
                    }
                }
            }
        }
    }

    // Augment with correlation variable CTE column names
    if let FromTableItem(Some(ref from)) = with_cte_render.from {
        if let Some(ref from_alias) = from.alias {
            let key = (var_name.to_string(), "id".to_string());
            if !col_map.contains_key(&key) {
                let cte_col = crate::utils::cte_column_naming::cte_column_name(var_name, "id");
                col_map.insert(key, format!("{}.{}", from_alias, cte_col));
            }
        }
    }

    find_cte_column_for_correlation_var(var_name, label, schema, &col_map)
}

/// Add a JOIN to a render plan, handling UNION branches.
/// If the plan has UNION branches, the join is cloned into each branch.
fn add_join_to_plan_or_union_branches(plan: &mut RenderPlan, join: Join) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        // Add to each UNION branch AND the outer plan.
        // The outer plan's FROM+JOINs form the first UNION ALL branch
        // in CTE body rendering, so it needs the join too.
        for (bi, branch) in union.input.iter_mut().enumerate() {
            log::debug!(
                "🔧 add_join_to_plan_or_union_branches: branch {} has {} existing joins, adding '{}'",
                bi,
                branch.joins.0.len(),
                join.table_alias
            );
            branch.joins.0.push(join.clone());
        }
        plan.joins.0.push(join);
    } else {
        log::debug!(
            "🔧 add_join_to_plan_or_union_branches: non-union plan has {} existing joins, adding '{}'",
            plan.joins.0.len(),
            join.table_alias
        );
        plan.joins.0.push(join);
    }
}

/// Replace count(*) placeholders in SELECT items, handling UNION branches.
fn replace_count_star_placeholders_in_select_or_union(
    plan: &mut RenderPlan,
    pc_replacements: &[String],
) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in union.input.iter_mut() {
            replace_count_star_placeholders_in_select(&mut branch.select.items, pc_replacements);
        }
    }
    // Always replace in the main plan's SELECT too
    replace_count_star_placeholders_in_select(&mut plan.select.items, pc_replacements);
}

/// Generate and replace arrayCount subqueries for list-constraint pattern comprehensions.
/// This handles only PCs with list_constraint (e.g., `size([p IN posts WHERE pattern])`).
/// Non-list PCs are handled by the CTE approach and should not be passed here.
fn generate_and_replace_arraycount_pc_subqueries(
    plan: &mut RenderPlan,
    pattern_comprehensions: &[crate::query_planner::logical_plan::PatternComprehensionMeta],
    schema: &GraphSchema,
    cte_name: &str,
) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in union.input.iter_mut() {
            generate_and_replace_arraycount_pc_subqueries(
                branch,
                pattern_comprehensions,
                schema,
                cte_name,
            );
        }
        return;
    }

    if plan.select.items.is_empty() {
        return;
    }

    let cte_col_map = build_cte_column_map(plan, cte_name);
    let mut branch_col_map = cte_col_map;

    // Include JOINs as available references
    for join in &plan.joins.0 {
        let join_alias = &join.table_alias;
        branch_col_map
            .entry((join_alias.clone(), "id".to_string()))
            .or_insert_with(|| format!("{}.id", join_alias));
    }

    // Scan FROM's ViewScan property_mapping
    if let FromTableItem(Some(ref from)) = plan.from {
        if let Some(ref from_alias) = from.alias {
            if let LogicalPlan::ViewScan(ref scan) = from.source.as_ref() {
                for (_prop, col_value) in &scan.property_mapping {
                    if let crate::graph_catalog::expression_parser::PropertyValue::Column(
                        ref col_name,
                    ) = col_value
                    {
                        if let Some((parsed_alias, parsed_property)) = parse_cte_column(col_name) {
                            let qualified = format!("{}.{}", from_alias, col_name);
                            branch_col_map
                                .entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        } else {
                            let qualified = format!("{}.\"{}\"", from_alias, col_name);
                            branch_col_map
                                .entry((col_name.clone(), "id".to_string()))
                                .or_insert(qualified.clone());
                            branch_col_map
                                .entry((col_name.clone(), col_name.clone()))
                                .or_insert(qualified);
                        }
                    }
                }
            }
        }
    }

    // Augment with correlation variables and list aliases
    if let FromTableItem(Some(ref from)) = plan.from {
        if let Some(ref from_alias) = from.alias {
            for pc in pattern_comprehensions.iter() {
                for cv in &pc.correlation_vars {
                    let key = (cv.var_name.clone(), "id".to_string());
                    if !branch_col_map.contains_key(&key) {
                        let cte_col =
                            crate::utils::cte_column_naming::cte_column_name(&cv.var_name, "id");
                        branch_col_map.insert(key, format!("{}.{}", from_alias, cte_col));
                    }
                }
                if let Some(ref lc) = pc.list_constraint {
                    let key1 = (lc.list_alias.clone(), "id".to_string());
                    if !branch_col_map.contains_key(&key1) {
                        let qualified = format!("{}.\"{}\"", from_alias, lc.list_alias);
                        branch_col_map.insert(key1, qualified.clone());
                        branch_col_map
                            .insert((lc.list_alias.clone(), lc.list_alias.clone()), qualified);
                    }
                }
            }
        }
    }

    // Only generate subqueries for list_constraint PCs (arrayCount path)
    let mut pc_subqueries: Vec<String> = Vec::new();
    for pc_meta in pattern_comprehensions {
        if pc_meta.pattern_hops.is_empty() {
            continue;
        }
        if pc_meta.list_constraint.is_some() {
            if let Some(subquery_sql) =
                generate_pattern_comprehension_correlated_subquery(pc_meta, schema, &branch_col_map)
            {
                pc_subqueries.push(subquery_sql);
            } else {
                pc_subqueries.push("0".to_string());
            }
        }
        // Non-list PCs: already handled as COALESCE — use placeholder markers
        // that won't match count(*) patterns
    }

    // Only replace __arraycount_placeholder__ entries with arrayCount subqueries
    let mut ac_idx = 0;
    for item in plan.select.items.iter_mut() {
        replace_arraycount_placeholders_in_expr(&mut item.expression, &pc_subqueries, &mut ac_idx);
    }
}

/// Replace __arraycount_placeholder__ markers with actual arrayCount subquery SQL.
fn replace_arraycount_placeholders_in_expr(
    expr: &mut RenderExpr,
    pc_subqueries: &[String],
    ac_idx: &mut usize,
) {
    match expr {
        RenderExpr::Raw(s) if s == "__arraycount_placeholder__" => {
            if *ac_idx < pc_subqueries.len() {
                *expr = RenderExpr::Raw(pc_subqueries[*ac_idx].clone());
                *ac_idx += 1;
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                replace_arraycount_placeholders_in_expr(operand, pc_subqueries, ac_idx);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in f.args.iter_mut() {
                replace_arraycount_placeholders_in_expr(arg, pc_subqueries, ac_idx);
            }
        }
        _ => {}
    }
}
