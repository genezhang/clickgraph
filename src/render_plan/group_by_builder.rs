//! GROUP BY clause builder for logical plans
//!
//! This module provides the `GroupByBuilder` trait and its implementation for extracting
//! GROUP BY clauses from logical query plans. The builder handles:
//!
//! - Recursive extraction through the plan tree
//! - Optimization: Using only ID columns instead of all node properties
//! - Table alias expansion and property mapping
//! - Denormalized edge patterns where node properties are in edge table
//! - Wildcard column handling (e.g., `a.*`)
//!
//! ## Architecture
//!
//! The trait-based design allows:
//! - Separation of GROUP BY logic from the main plan builder
//! - Clean delegation pattern for plan traversal
//! - Explicit handling of all LogicalPlan variants
//!
//! ## Key Optimization
//!
//! When a node alias appears in GROUP BY (e.g., `GROUP BY a` or `GROUP BY a.*`),
//! instead of grouping by all node properties (8+ columns), we only group by the
//! ID column. This is sound because all other properties are functionally dependent
//! on the ID. This significantly improves query performance.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::{GroupBy, LogicalPlan};
use std::collections::HashSet;

use super::errors::RenderBuildError;
use super::plan_builder::RenderPlanBuilder;
use super::plan_builder_helpers::apply_property_mapping_to_expr;
use super::render_expr::{PropertyAccess, RenderExpr, TableAlias};

/// Result type for GROUP BY builder operations
pub type GroupByBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting GROUP BY clauses from logical plans
///
/// Implemented by `LogicalPlan` to enable recursive GROUP BY extraction
/// through the plan tree with proper delegation to child plans.
pub trait GroupByBuilder {
    /// Extract GROUP BY expressions from this plan node
    ///
    /// Returns a vector of `RenderExpr` representing the GROUP BY clause.
    /// Returns an empty vector if no GROUP BY is found.
    ///
    /// # Behavior by Plan Type
    ///
    /// - **GroupBy**: Processes expressions, applies ID column optimization
    /// - **Pass-through plans** (Limit, Skip, OrderBy, Projection, Filter, etc.):
    ///   Delegates to input plan
    /// - **GraphRel**: Tries left, then center, then right inputs
    /// - **Others**: Returns empty vector (no GROUP BY)
    fn extract_group_by(&self) -> GroupByBuilderResult<Vec<RenderExpr>>;
}

impl GroupByBuilder for LogicalPlan {
    fn extract_group_by(&self) -> GroupByBuilderResult<Vec<RenderExpr>> {
        log::info!(
            "🔧 GROUP BY: extract_group_by() called for plan type {:?}",
            std::mem::discriminant(self)
        );

        let group_by = match &self {
            // Pass-through plans - delegate to input
            LogicalPlan::Limit(limit) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&limit.input)?
            }
            LogicalPlan::Skip(skip) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&skip.input)?
            }
            LogicalPlan::OrderBy(order_by) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&order_by.input)?
            }
            LogicalPlan::Projection(projection) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&projection.input)?
            }
            LogicalPlan::Filter(filter) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&filter.input)?
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&graph_joins.input)?
            }
            LogicalPlan::GraphNode(node) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&node.input)?
            }

            // GraphRel - try left, center, right in order
            LogicalPlan::GraphRel(rel) => {
                // For relationships, try left first, then center, then right
                <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.left)
                    .or_else(|_| <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.center))
                    .or_else(|_| <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.right))?
            }

            // GroupBy - main processing logic
            LogicalPlan::GroupBy(group_by) => process_group_by_expressions(group_by, self)?,

            // All other plans have no GROUP BY
            _ => vec![],
        };

        Ok(group_by)
    }
}

/// Process GROUP BY expressions with optimization and property mapping
///
/// This function handles the core GROUP BY logic:
/// 1. Expands table aliases to their properties
/// 2. Applies ID column optimization for node aliases
/// 3. Handles wildcard columns (e.g., `a.*`)
/// 4. Manages denormalized edge patterns
/// 5. Converts logical expressions to render expressions
fn process_group_by_expressions(
    group_by: &GroupBy,
    plan: &LogicalPlan,
) -> GroupByBuilderResult<Vec<RenderExpr>> {
    log::info!(
        "🔧 GROUP BY: Found GroupBy plan, processing {} expressions",
        group_by.expressions.len()
    );

    let mut result: Vec<RenderExpr> = Vec::new();
    let mut seen_group_by_aliases: HashSet<String> = HashSet::new();

    for expr in &group_by.expressions {
        // Case 1: TableAlias - expand to ID column only (optimization)
        if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) = expr {
            if handle_table_alias_group_by(
                &group_by.input,
                &alias.0,
                &mut result,
                &mut seen_group_by_aliases,
            )? {
                continue; // Successfully handled
            }
        }

        // Case 2: PropertyAccessExp with wildcard "*" - expand to ID column only
        if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(prop_access) =
            expr
        {
            if prop_access.column.raw() == "*" {
                if handle_wildcard_group_by(
                    &group_by.input,
                    prop_access,
                    &mut result,
                    &mut seen_group_by_aliases,
                )? {
                    continue; // Successfully handled
                }
            }
        }

        // Case 3: Regular expression - convert and apply property mapping
        let mut render_expr: RenderExpr = expr.clone().try_into()?;
        apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
        result.push(render_expr);
    }

    Ok(result)
}

/// Handle GROUP BY for table alias expressions (e.g., `GROUP BY a`)
///
/// Applies the ID column optimization: instead of grouping by all node properties,
/// we only group by the ID column since all other properties are functionally
/// dependent on it.
///
/// Returns `true` if the alias was successfully handled, `false` otherwise.
fn handle_table_alias_group_by(
    input: &LogicalPlan,
    alias: &str,
    result: &mut Vec<RenderExpr>,
    seen_aliases: &mut HashSet<String>,
) -> GroupByBuilderResult<bool> {
    // Get properties for this alias
    let (properties, actual_table_alias) = match input.get_properties_with_table_alias(alias) {
        Ok(info) => info,
        Err(_) => return Ok(false), // Cannot resolve - let caller handle
    };

    if properties.is_empty() {
        return Ok(false); // No properties - not a node alias
    }

    let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());

    // Skip if we've already added this alias (avoid duplicates)
    if seen_aliases.contains(&table_alias_to_use) {
        return Ok(true); // Already handled
    }
    seen_aliases.insert(table_alias_to_use.clone());

    // Get the ID column from the schema (via ViewScan.id_column)
    let id_col = input.find_id_column_for_alias(alias).unwrap_or_else(|_| {
        log::warn!(
            "⚠️ Could not find ID column for alias '{}', using fallback",
            alias
        );
        "id".to_string()
    });

    log::debug!(
        "🔧 GROUP BY optimization: Using ID column '{}' from schema instead of {} properties for alias '{}'",
        id_col,
        properties.len(),
        table_alias_to_use
    );

    result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(table_alias_to_use.clone()),
        column: PropertyValue::Column(id_col),
    }));

    Ok(true)
}

/// Handle GROUP BY for wildcard property access (e.g., `GROUP BY a.*`)
///
/// Applies the same ID column optimization as table aliases, but also handles
/// denormalized edge patterns where node properties are stored in the edge table.
///
/// Returns `true` if successfully handled, `false` otherwise.
fn handle_wildcard_group_by(
    input: &LogicalPlan,
    prop_access: &crate::query_planner::logical_expr::PropertyAccess,
    result: &mut Vec<RenderExpr>,
    seen_aliases: &mut HashSet<String>,
) -> GroupByBuilderResult<bool> {
    // Get properties for this alias
    let (properties, actual_table_alias) =
        match input.get_properties_with_table_alias(&prop_access.table_alias.0) {
            Ok(info) => info,
            Err(_) => return Ok(false), // Cannot resolve - let caller handle
        };

    let table_alias_to_use =
        actual_table_alias.unwrap_or_else(|| prop_access.table_alias.0.clone());

    // Skip if we've already added this alias (avoid duplicates)
    if seen_aliases.contains(&table_alias_to_use) {
        return Ok(true); // Already handled
    }
    seen_aliases.insert(table_alias_to_use.clone());

    // Case A: Denormalized edge pattern - find node properties in relationship
    if let Some((node_props, table_alias)) =
        find_node_properties_for_rel_alias(input, &prop_access.table_alias.0)
    {
        // Found denormalized node properties - get ID from schema (MUST succeed)
        let id_col = input
            .find_id_column_for_alias(&prop_access.table_alias.0)
            .map_err(|e| {
                RenderBuildError::InvalidRenderPlan(format!(
                    "Cannot find ID column for denormalized alias '{}': {}",
                    prop_access.table_alias.0, e
                ))
            })?;

        log::debug!(
            "🔧 GROUP BY optimization: Using ID column '{}' from schema for denormalized alias '{}'",
            id_col,
            table_alias
        );

        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias.clone()),
            column: PropertyValue::Column(id_col),
        }));

        return Ok(true);
    }

    // Case B: Regular node alias - use ID column
    if !properties.is_empty() {
        let id_col = input
            .find_id_column_for_alias(&prop_access.table_alias.0)
            .map_err(|e| {
                RenderBuildError::InvalidRenderPlan(format!(
                    "Cannot find ID column for alias '{}': {}",
                    prop_access.table_alias.0, e
                ))
            })?;

        log::debug!(
            "🔧 GROUP BY optimization: Using ID column '{}' instead of {} properties for alias '{}'",
            id_col,
            properties.len(),
            table_alias_to_use
        );

        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias_to_use.clone()),
            column: PropertyValue::Column(id_col),
        }));

        return Ok(true);
    }

    Ok(false) // Could not handle - let caller handle
}

/// Find node properties when the alias is a relationship alias with "*" column
///
/// For denormalized schemas, the node alias gets remapped to the relationship alias,
/// so we need to look up which node this represents and get its properties.
///
/// Returns `Some((properties, table_alias))` if found, `None` otherwise.
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
            // Not this relationship - search children recursively
            if let Some(result) = find_node_properties_for_rel_alias(&rel.left, rel_alias) {
                return Some(result);
            }
            if let Some(result) = find_node_properties_for_rel_alias(&rel.center, rel_alias) {
                return Some(result);
            }
            find_node_properties_for_rel_alias(&rel.right, rel_alias)
        }
        // Pass-through plans - search input
        LogicalPlan::Projection(proj) => find_node_properties_for_rel_alias(&proj.input, rel_alias),
        LogicalPlan::Filter(filter) => find_node_properties_for_rel_alias(&filter.input, rel_alias),
        LogicalPlan::GroupBy(gb) => find_node_properties_for_rel_alias(&gb.input, rel_alias),
        LogicalPlan::GraphJoins(joins) => {
            find_node_properties_for_rel_alias(&joins.input, rel_alias)
        }
        LogicalPlan::OrderBy(order) => find_node_properties_for_rel_alias(&order.input, rel_alias),
        LogicalPlan::Skip(skip) => find_node_properties_for_rel_alias(&skip.input, rel_alias),
        LogicalPlan::Limit(limit) => find_node_properties_for_rel_alias(&limit.input, rel_alias),
        _ => None,
    }
}
