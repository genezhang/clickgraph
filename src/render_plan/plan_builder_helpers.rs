//! Helper functions for plan building
//!
//! This module contains utility functions used by the RenderPlanBuilder trait implementation.
//! These functions assist with:
//! - Plan tree traversal and table/column extraction
//! - Expression rendering and SQL string generation
//! - Relationship and node information lookup
//! - Path function rewriting
//! - Schema lookups
//! - Polymorphic edge filter generation
//!
//! Note: Some functions in this module are reserved for future features or used only
//! in specific code paths. The allow(dead_code) directive suppresses warnings for these.

#![allow(dead_code)]

use super::render_expr::{
    AggregateFnCall, Column, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr,
    ScalarFnCall, TableAlias,
};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
use crate::render_plan::cte_extraction::get_node_label_for_alias;
use crate::render_plan::expression_utils::{
    flatten_addition_operands, has_string_operand, ExprVisitor,
};
// Note: Direction import commented out until Issue #1 (Undirected Multi-Hop SQL) is fixed
// use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::LogicalPlan;
use std::collections::HashSet;

/// Recursively rewrite TableAlias references that are in `with_aliases` to reference the CTE.
/// This handles the case where `AVG(follows)` needs to become `AVG(grouped_data.follows)`.
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `with_aliases` - Set of WITH alias names that should be rewritten
/// * `cte_name` - The name of the CTE to reference (e.g., "grouped_data")
///
/// # Returns
/// A tuple of (rewritten_expression, all_from_with) where `all_from_with` is true
/// if all leaf references in the expression came from WITH aliases.
pub(super) fn rewrite_with_aliases_to_cte(
    expr: RenderExpr,
    with_aliases: &HashSet<String>,
    cte_name: &str,
) -> (RenderExpr, bool) {
    match expr {
        RenderExpr::TableAlias(alias) => {
            if with_aliases.contains(&alias.0) {
                // Rewrite to CTE reference: grouped_data.follows
                let rewritten = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_name.to_string()),
                    column: PropertyValue::Column(alias.0.clone()),
                });
                (rewritten, true)
            } else {
                (RenderExpr::TableAlias(alias), false)
            }
        }
        RenderExpr::ColumnAlias(alias) => {
            if with_aliases.contains(&alias.0) {
                // Rewrite to CTE reference
                let rewritten = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_name.to_string()),
                    column: PropertyValue::Column(alias.0.clone()),
                });
                (rewritten, true)
            } else {
                (RenderExpr::ColumnAlias(alias), false)
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments
            let mut all_from_with = true;
            let new_args: Vec<RenderExpr> = agg
                .args
                .into_iter()
                .map(|arg| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(arg, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name,
                    args: new_args,
                }),
                all_from_with,
            )
        }
        RenderExpr::ScalarFnCall(func) => {
            // Recursively rewrite arguments
            let mut all_from_with = true;
            let new_args: Vec<RenderExpr> = func
                .args
                .into_iter()
                .map(|arg| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(arg, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: func.name,
                    args: new_args,
                }),
                all_from_with,
            )
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let mut all_from_with = true;
            let new_operands: Vec<RenderExpr> = op
                .operands
                .into_iter()
                .map(|operand| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(operand, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                }),
                all_from_with,
            )
        }
        RenderExpr::Case(case) => {
            // Recursively rewrite CASE expression
            use super::render_expr::RenderCase;
            let mut all_from_with = true;

            // Rewrite the optional CASE expression (for simple CASE syntax)
            let new_expr = case.expr.map(|e| {
                let (rewritten, from_with) =
                    rewrite_with_aliases_to_cte(*e, with_aliases, cte_name);
                if !from_with {
                    all_from_with = false;
                }
                Box::new(rewritten)
            });

            let new_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .into_iter()
                .map(|(cond, result)| {
                    let (new_cond, cond_from_with) =
                        rewrite_with_aliases_to_cte(cond, with_aliases, cte_name);
                    let (new_result, result_from_with) =
                        rewrite_with_aliases_to_cte(result, with_aliases, cte_name);
                    if !cond_from_with || !result_from_with {
                        all_from_with = false;
                    }
                    (new_cond, new_result)
                })
                .collect();

            let new_else = case.else_expr.map(|e| {
                let (new_else, else_from_with) =
                    rewrite_with_aliases_to_cte(*e, with_aliases, cte_name);
                if !else_from_with {
                    all_from_with = false;
                }
                Box::new(new_else)
            });

            (
                RenderExpr::Case(RenderCase {
                    expr: new_expr,
                    when_then: new_when_then,
                    else_expr: new_else,
                }),
                all_from_with,
            )
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // Property access doesn't come from WITH directly,
            // but we pass through (handled by rewrite_table_aliases_to_cte if needed)
            (RenderExpr::PropertyAccessExp(prop), false)
        }
        RenderExpr::List(items) => {
            let mut all_from_with = true;
            let new_items: Vec<RenderExpr> = items
                .into_iter()
                .map(|item| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(item, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();
            (RenderExpr::List(new_items), all_from_with)
        }
        // Literals, Star, Column, Parameter, Raw don't need rewriting and don't come from WITH
        other => (other, false),
    }
}

/// Rewrite expressions that reference table aliases from WITH clause to CTE references.
/// For example: `count(person.id)` where `person` was passed through WITH
/// becomes `count(with_result."person.id")` since the CTE includes `person.id AS "person.id"`
pub(super) fn rewrite_table_aliases_to_cte(
    expr: RenderExpr,
    with_table_aliases: &HashSet<String>,
    cte_name: &str,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            if with_table_aliases.contains(&prop.table_alias.0) {
                // 🔧 FIX: CTE columns use underscore naming (a_name), not dot notation (a.name)
                // Rewrite a.name -> with_result."a_name" (not "a.name")
                let col_name = format!("{}_{}", prop.table_alias.0, prop.column.raw());
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_name.to_string()),
                    column: PropertyValue::Column(col_name),
                })
            } else {
                RenderExpr::PropertyAccessExp(prop)
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args: Vec<RenderExpr> = agg
                .args
                .into_iter()
                .map(|arg| rewrite_table_aliases_to_cte(arg, with_table_aliases, cte_name))
                .collect();
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name,
                args: new_args,
            })
        }
        RenderExpr::ScalarFnCall(func) => {
            let new_args: Vec<RenderExpr> = func
                .args
                .into_iter()
                .map(|arg| rewrite_table_aliases_to_cte(arg, with_table_aliases, cte_name))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name,
                args: new_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let new_operands: Vec<RenderExpr> = op
                .operands
                .into_iter()
                .map(|operand| rewrite_table_aliases_to_cte(operand, with_table_aliases, cte_name))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: new_operands,
            })
        }
        RenderExpr::Case(case) => {
            use super::render_expr::RenderCase;
            let new_expr = case.expr.map(|e| {
                Box::new(rewrite_table_aliases_to_cte(
                    *e,
                    with_table_aliases,
                    cte_name,
                ))
            });
            let new_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .into_iter()
                .map(|(cond, result)| {
                    (
                        rewrite_table_aliases_to_cte(cond, with_table_aliases, cte_name),
                        rewrite_table_aliases_to_cte(result, with_table_aliases, cte_name),
                    )
                })
                .collect();
            let new_else = case.else_expr.map(|e| {
                Box::new(rewrite_table_aliases_to_cte(
                    *e,
                    with_table_aliases,
                    cte_name,
                ))
            });
            RenderExpr::Case(RenderCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }
        RenderExpr::List(items) => {
            let new_items: Vec<RenderExpr> = items
                .into_iter()
                .map(|item| rewrite_table_aliases_to_cte(item, with_table_aliases, cte_name))
                .collect();
            RenderExpr::List(new_items)
        }
        // Other expressions pass through unchanged
        other => other,
    }
}

/// Helper function to check if a LogicalPlan node represents a denormalized node
///
/// ✅ PHASE 2 APPROVED: This is a structural query helper, not property resolution logic.
/// It reads flags set by analyzer passes to determine JOIN requirements.
/// For denormalized nodes, the node data lives on the edge table, not a separate node table
/// For nested GraphRels, we recursively check the leaf nodes
pub(super) fn is_node_denormalized(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // Check the GraphNode's own is_denormalized flag first
            if node.is_denormalized {
                return true;
            }
            // Fall back to checking ViewScan input
            if let LogicalPlan::ViewScan(view_scan) = node.input.as_ref() {
                view_scan.is_denormalized
            } else {
                false
            }
        }
        // For nested GraphRel, check if the innermost node is denormalized
        LogicalPlan::GraphRel(graph_rel) => {
            // Recursively check the left side to find the leftmost GraphNode
            is_node_denormalized(&graph_rel.left)
        }
        _ => false,
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
///
/// NOTE: For GraphRel, this returns the relationship table (center), which is correct
/// for most use cases. If you need the END NODE table from a nested GraphRel,
/// use `extract_end_node_table_name` instead.
pub(super) fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        // For CTEs, return the CTE name directly (don't recurse into input)
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_table_name(&node.input),
        LogicalPlan::GraphRel(rel) => extract_table_name(&rel.center),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        // For WithClause, return the CTE name (always set by analysis phase)
        LogicalPlan::WithClause(wc) => wc.cte_name.clone(),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_table_name(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to extract the END NODE table name from a LogicalPlan node.
///
/// CRITICAL: For nested GraphRel patterns (multi-hop traversals), this extracts
/// the rightmost/terminal node's table, NOT the relationship table.
///
/// Example: For `(a)-[:REL1]-(b)-[:REL2]-(c)` represented as:
///   GraphRel { left: GraphNode(a), center: REL1, right: GraphRel { left: b, center: REL2, right: c } }
///
/// - `extract_table_name` on the outer GraphRel would return REL1's table (WRONG for end node)
/// - `extract_end_node_table_name` on the outer GraphRel.right would return c's table (CORRECT)
pub(super) fn extract_end_node_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_end_node_table_name(&node.input),
        // CRITICAL: For GraphRel, extract from the RIGHT side (end node), not CENTER (relationship)
        LogicalPlan::GraphRel(rel) => extract_end_node_table_name(&rel.right),
        LogicalPlan::Filter(filter) => extract_end_node_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_end_node_table_name(&proj.input),
        // For WithClause, return the CTE name (always set by analysis phase)
        LogicalPlan::WithClause(wc) => wc.cte_name.clone(),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_end_node_table_name(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the ID column of the END NODE in a potentially nested GraphRel pattern.
///
/// Similar to `extract_end_node_table_name`, but for ID columns.
/// For nested patterns like (a)-[r1]->(b)-[r2]->(c), when called on the outer GraphRel.right,
/// this traverses through inner GraphRels to find the actual end node's ID column.
///
/// The difference from `extract_id_column` is:
/// - `extract_id_column(&GraphRel)` returns rel.center's ID (relationship table's ID)
/// - `extract_end_node_id_column(&GraphRel)` returns the actual end node's ID (via rel.right)
pub(super) fn extract_end_node_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_end_node_id_column(&node.input),
        // CRITICAL: For GraphRel, extract from the RIGHT side (end node), not CENTER (relationship)
        LogicalPlan::GraphRel(rel) => extract_end_node_id_column(&rel.right),
        LogicalPlan::Filter(filter) => extract_end_node_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_end_node_id_column(&proj.input),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_end_node_id_column(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to extract the table reference with parameterized view syntax if applicable.
/// For a ViewScan with view_parameter_names, returns `table_name(param1='value1', param2='value2')`.
/// For other cases, returns just the table name.
///
/// This is used for JOINs where parameterized views need to be called with parameters.
/// Example: `JOIN friendships_by_tenant(tenant_id='acme') AS f ON ...`
pub(super) fn extract_parameterized_table_ref(plan: &LogicalPlan) -> Option<String> {
    match plan {
        // For CTEs, return the CTE name directly (no parameters)
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = (
                &view_scan.view_parameter_names,
                &view_scan.view_parameter_values,
            ) {
                if !param_names.is_empty() {
                    // Generate parameterized view call with actual values: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                // Escape single quotes in value for SQL safety
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();

                    if param_pairs.is_empty() {
                        log::warn!(
                            "extract_parameterized_table_ref: ViewScan '{}' expects parameters {:?} but none matched in values",
                            view_scan.source_table, param_names
                        );
                        return Some(view_scan.source_table.clone());
                    }

                    log::debug!(
                        "extract_parameterized_table_ref: ViewScan '{}' generating: {}({})",
                        view_scan.source_table,
                        view_scan.source_table,
                        param_pairs.join(", ")
                    );
                    return Some(format!(
                        "{}({})",
                        view_scan.source_table,
                        param_pairs.join(", ")
                    ));
                }
            }
            // No parameters - return plain table name
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => extract_parameterized_table_ref(&node.input),
        LogicalPlan::GraphRel(rel) => extract_parameterized_table_ref(&rel.center),
        LogicalPlan::Filter(filter) => extract_parameterized_table_ref(&filter.input),
        LogicalPlan::Projection(proj) => extract_parameterized_table_ref(&proj.input),
        _ => None,
    }
}

/// Extract a mapping of alias → parameterized table reference from a LogicalPlan tree.
///
/// This traverses the plan and builds a HashMap where:
/// - Keys are aliases (from GraphNode.alias or GraphRel.alias)
/// - Values are table references with parameterized view syntax if applicable
///
/// For parameterized views, the value will be `table(param = $param)` format.
/// For regular tables, the value is just the table name.
///
/// This is used to fix JOINs generated from GraphJoins, ensuring that
/// parameterized views are called correctly in all JOIN clauses.
pub(super) fn extract_rel_and_node_tables(
    plan: &LogicalPlan,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();

    match plan {
        LogicalPlan::GraphRel(gr) => {
            // Use the centralized helper to get parameterized table reference
            if let Some(parameterized_ref) = extract_parameterized_table_ref(&gr.center) {
                log::debug!(
                    "extract_rel_and_node_tables: GraphRel alias='{}' → '{}'",
                    gr.alias,
                    parameterized_ref
                );
                map.insert(gr.alias.clone(), parameterized_ref);
            }

            // Recursively check left and right nodes
            map.extend(extract_rel_and_node_tables(&gr.left));
            map.extend(extract_rel_and_node_tables(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            // Use the centralized helper to get parameterized table reference
            if let Some(parameterized_ref) = extract_parameterized_table_ref(&gn.input) {
                log::debug!(
                    "extract_rel_and_node_tables: GraphNode alias='{}' → '{}'",
                    gn.alias,
                    parameterized_ref
                );
                map.insert(gn.alias.clone(), parameterized_ref);
            }
        }
        LogicalPlan::Projection(p) => {
            map.extend(extract_rel_and_node_tables(&p.input));
        }
        LogicalPlan::Filter(f) => {
            map.extend(extract_rel_and_node_tables(&f.input));
        }
        LogicalPlan::CartesianProduct(cp) => {
            map.extend(extract_rel_and_node_tables(&cp.left));
            map.extend(extract_rel_and_node_tables(&cp.right));
        }
        LogicalPlan::GraphJoins(gj) => {
            map.extend(extract_rel_and_node_tables(&gj.input));
        }
        _ => {}
    }

    map
}

/// Helper function to find the table name for a given alias by recursively searching the plan tree
/// Used to find the anchor node's table in multi-hop queries
/// Find the table name for a given alias by traversing the LogicalPlan tree.
/// This is used to determine the correct FROM table in CTE patterns where
/// the grouping key alias (e.g., "g" in "WITH g, COUNT(u)") needs to be
/// resolved to its underlying table (e.g., "sec_groups").
///
/// IMPORTANT: This function is EXHAUSTIVE - all LogicalPlan variants must be
/// handled explicitly. This ensures we don't silently miss new plan types.
pub(super) fn find_table_name_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        // === Terminal nodes that can match ===
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                // Found the matching GraphNode, extract table name from its input
                match &*node.input {
                    LogicalPlan::ViewScan(scan) => Some(scan.source_table.clone()),
                    _ => None,
                }
            } else {
                // Not a match, recurse into input
                find_table_name_for_alias(&node.input, target_alias)
            }
        }
        LogicalPlan::GraphRel(rel) => {
            // Check if the target is a relationship alias (e.g., "f1" for denormalized edges)
            if rel.alias == target_alias {
                // The relationship alias matches - get table from its center ViewScan
                if let LogicalPlan::ViewScan(scan) = &*rel.center {
                    return Some(scan.source_table.clone());
                }
            }
            // Search in both left and right branches
            find_table_name_for_alias(&rel.left, target_alias)
                .or_else(|| find_table_name_for_alias(&rel.right, target_alias))
        }

        // === Wrapper nodes - recurse into input ===
        LogicalPlan::Cte(cte) => find_table_name_for_alias(&cte.input, target_alias),
        LogicalPlan::Projection(proj) => find_table_name_for_alias(&proj.input, target_alias),
        LogicalPlan::GroupBy(group_by) => find_table_name_for_alias(&group_by.input, target_alias),
        LogicalPlan::Filter(filter) => find_table_name_for_alias(&filter.input, target_alias),
        LogicalPlan::OrderBy(order) => find_table_name_for_alias(&order.input, target_alias),
        LogicalPlan::GraphJoins(joins) => find_table_name_for_alias(&joins.input, target_alias),
        LogicalPlan::Skip(skip) => find_table_name_for_alias(&skip.input, target_alias),
        LogicalPlan::Limit(limit) => find_table_name_for_alias(&limit.input, target_alias),
        LogicalPlan::Unwind(unwind) => find_table_name_for_alias(&unwind.input, target_alias),

        // === Union - search all branches ===
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(table) = find_table_name_for_alias(input, target_alias) {
                    return Some(table);
                }
            }
            None
        }

        // === Terminal nodes that cannot contain aliases ===
        LogicalPlan::Empty => None,
        LogicalPlan::ViewScan(_) => None, // ViewScan itself doesn't have alias, GraphNode wraps it
        LogicalPlan::PageRank(_) => None, // PageRank is a computed result, no direct table alias

        // === CartesianProduct - search both branches ===
        LogicalPlan::CartesianProduct(cp) => find_table_name_for_alias(&cp.left, target_alias)
            .or_else(|| find_table_name_for_alias(&cp.right, target_alias)),

        // === WithClause - search input ===
        LogicalPlan::WithClause(wc) => find_table_name_for_alias(&wc.input, target_alias),
    }
}

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
pub(super) fn render_expr_to_sql_string(
    expr: &RenderExpr,
    alias_mapping: &[(String, String)],
) -> String {
    match expr {
        RenderExpr::Column(col) => col.raw().to_string(),
        RenderExpr::TableAlias(alias) => alias.0.clone(),
        RenderExpr::ColumnAlias(alias) => alias.0.clone(),
        RenderExpr::Literal(lit) => match lit {
            Literal::String(s) => format!("'{}'", s.replace("'", "''")),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
            Literal::Null => "NULL".to_string(),
        },
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            let table_alias = alias_mapping
                .iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
            format!("{}.{}", table_alias, prop.column.raw())
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let operands: Vec<String> = op
                .operands
                .iter()
                .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                .collect();
            match op.operator {
                Operator::Equal => format!("{} = {}", operands[0], operands[1]),
                Operator::NotEqual => format!("{} != {}", operands[0], operands[1]),
                Operator::LessThan => format!("{} < {}", operands[0], operands[1]),
                Operator::GreaterThan => format!("{} > {}", operands[0], operands[1]),
                Operator::LessThanEqual => format!("{} <= {}", operands[0], operands[1]),
                Operator::GreaterThanEqual => format!("{} >= {}", operands[0], operands[1]),
                Operator::And => format!("({})", operands.join(" AND ")),
                Operator::Or => format!("({})", operands.join(" OR ")),
                Operator::Not => format!("NOT ({})", operands[0]),
                Operator::Addition => {
                    // Use concat() for string concatenation
                    // Flatten nested + operations for cases like: a + ' - ' + b
                    if has_string_operand(&op.operands) {
                        let flattened: Vec<String> = op
                            .operands
                            .iter()
                            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
                            .collect();
                        format!("concat({})", flattened.join(", "))
                    } else {
                        format!("{} + {}", operands[0], operands[1])
                    }
                }
                Operator::Subtraction => format!("{} - {}", operands[0], operands[1]),
                Operator::Multiplication => format!("{} * {}", operands[0], operands[1]),
                Operator::Division => format!("{} / {}", operands[0], operands[1]),
                Operator::ModuloDivision => format!("{} % {}", operands[0], operands[1]),
                _ => format!("{} {:?} {}", operands[0], op.operator, operands[1]), // fallback
            }
        }
        RenderExpr::Parameter(param) => format!("${}", param),
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", func.name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        RenderExpr::List(list) => {
            let items: Vec<String> = list
                .iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            format!("({})", items.join(", "))
        }
        _ => "TRUE".to_string(), // fallback for unsupported expressions
    }
}

/// Helper to extract ID column name from ViewScan
pub(super) fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::GraphRel(rel) => extract_id_column(&rel.center),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_id_column(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to check if a plan tree contains a GraphRel with multiple relationships
pub(super) fn has_multiple_relationships(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                labels.len() > 1
            } else {
                false
            }
        }
        LogicalPlan::Projection(proj) => has_multiple_relationships(&proj.input),
        LogicalPlan::Filter(filter) => has_multiple_relationships(&filter.input),
        LogicalPlan::GraphJoins(graph_joins) => has_multiple_relationships(&graph_joins.input),
        LogicalPlan::GraphNode(graph_node) => has_multiple_relationships(&graph_node.input),
        _ => false,
    }
}

/// Helper function to extract multiple relationship info from a plan tree
pub(super) fn get_multiple_rel_info(plan: &LogicalPlan) -> Option<(String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                if labels.len() > 1 {
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );
                    Some((
                        graph_rel.left_connection.clone(),
                        graph_rel.right_connection.clone(),
                        cte_name,
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        LogicalPlan::Projection(proj) => get_multiple_rel_info(&proj.input),
        LogicalPlan::Filter(filter) => get_multiple_rel_info(&filter.input),
        LogicalPlan::GraphJoins(graph_joins) => get_multiple_rel_info(&graph_joins.input),
        LogicalPlan::GraphNode(graph_node) => get_multiple_rel_info(&graph_node.input),
        _ => None,
    }
}

/// Helper function to check if an expression is standalone (doesn't reference any table columns)
/// Returns true for literals, parameters, and functions that only use standalone expressions
pub(super) fn is_standalone_expression(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) | RenderExpr::Star => true,
        RenderExpr::ScalarFnCall(fn_call) => {
            // Function is standalone if all its arguments are standalone
            fn_call.args.iter().all(is_standalone_expression)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Operator application is standalone if all operands are standalone
            op.operands.iter().all(is_standalone_expression)
        }
        RenderExpr::Case(case_expr) => {
            // CASE is standalone if all branches are standalone
            let when_then_standalone = case_expr.when_then.iter().all(|(cond, result)| {
                is_standalone_expression(cond) && is_standalone_expression(result)
            });
            let else_standalone = case_expr
                .else_expr
                .as_ref()
                .is_none_or(|e| is_standalone_expression(e));
            when_then_standalone && else_standalone
        }
        RenderExpr::List(list) => {
            // List is standalone if all elements are standalone
            list.iter().all(is_standalone_expression)
        }
        // Any reference to columns, properties, or aliases means it's not standalone
        RenderExpr::Column(_)
        | RenderExpr::PropertyAccessExp(_)
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::AggregateFnCall(_)
        | RenderExpr::InSubquery(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_) => false, // Pattern count references outer context
        RenderExpr::ReduceExpr(reduce) => {
            // ReduceExpr is standalone if all its sub-expressions are standalone
            is_standalone_expression(&reduce.initial_value)
                && is_standalone_expression(&reduce.list)
                && is_standalone_expression(&reduce.expression)
        }
        RenderExpr::ArraySubscript { array, index } => {
            // ArraySubscript is standalone if both array and index are standalone
            is_standalone_expression(array) && is_standalone_expression(index)
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            // ArraySlicing is standalone if array and optional bounds are standalone
            is_standalone_expression(array)
                && from.as_ref().is_none_or(|f| is_standalone_expression(f))
                && to.as_ref().is_none_or(|t| is_standalone_expression(t))
        }
        RenderExpr::MapLiteral(entries) => {
            // MapLiteral is standalone if all values are standalone
            entries.iter().all(|(_, v)| is_standalone_expression(v))
        }
        RenderExpr::Raw(_) => false, // Be conservative with raw SQL
        // CteEntityRef references CTE columns, so not standalone
        RenderExpr::CteEntityRef(_) => false,
    }
}

/// Helper function to extract all relationship connections from a plan tree
/// Returns a vector of (left_connection, right_connection, relationship_alias) tuples
pub(super) fn get_all_relationship_connections(
    plan: &LogicalPlan,
) -> Vec<(String, String, String)> {
    let mut connections = vec![];

    fn collect_connections(plan: &LogicalPlan, connections: &mut Vec<(String, String, String)>) {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                connections.push((
                    graph_rel.left_connection.clone(),
                    graph_rel.right_connection.clone(),
                    graph_rel.alias.clone(),
                ));
                // Recurse into nested GraphRels (multi-hop chains)
                collect_connections(&graph_rel.left, connections);
                collect_connections(&graph_rel.right, connections);
            }
            LogicalPlan::Projection(proj) => collect_connections(&proj.input, connections),
            LogicalPlan::Filter(filter) => collect_connections(&filter.input, connections),
            LogicalPlan::GraphJoins(graph_joins) => {
                collect_connections(&graph_joins.input, connections)
            }
            LogicalPlan::GraphNode(graph_node) => {
                collect_connections(&graph_node.input, connections)
            }
            _ => {}
        }
    }

    collect_connections(plan, &mut connections);
    connections
}

/// Helper function to collect all denormalized node aliases from a plan tree
///
/// ✅ PHASE 2 APPROVED: Structural query helper for plan tree traversal.
/// Queries ViewScan's is_denormalized flag (set from schema during ViewScan creation)
/// Returns a set of aliases where the node is stored on the relationship table
pub(super) fn get_denormalized_aliases(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    let mut denormalized = std::collections::HashSet::new();

    fn collect_denormalized(
        plan: &LogicalPlan,
        denormalized: &mut std::collections::HashSet<String>,
    ) {
        match plan {
            LogicalPlan::GraphNode(node) => {
                // Check if the ViewScan has is_denormalized flag set
                if let LogicalPlan::ViewScan(view_scan) = node.input.as_ref() {
                    if view_scan.is_denormalized {
                        println!(
                            "DEBUG: Node '{}' is denormalized (table: {})",
                            node.alias, view_scan.source_table
                        );
                        denormalized.insert(node.alias.clone());
                    }
                }
                collect_denormalized(&node.input, denormalized);
            }
            LogicalPlan::GraphRel(rel) => {
                collect_denormalized(&rel.left, denormalized);
                collect_denormalized(&rel.center, denormalized);
                collect_denormalized(&rel.right, denormalized);
            }
            LogicalPlan::Projection(proj) => collect_denormalized(&proj.input, denormalized),
            LogicalPlan::Filter(filter) => collect_denormalized(&filter.input, denormalized),
            LogicalPlan::GraphJoins(joins) => collect_denormalized(&joins.input, denormalized),
            LogicalPlan::OrderBy(order) => collect_denormalized(&order.input, denormalized),
            LogicalPlan::Limit(limit) => collect_denormalized(&limit.input, denormalized),
            LogicalPlan::Skip(skip) => collect_denormalized(&skip.input, denormalized),
            LogicalPlan::Unwind(u) => collect_denormalized(&u.input, denormalized),
            _ => {}
        }
    }

    collect_denormalized(plan, &mut denormalized);
    crate::debug_println!("DEBUG: get_denormalized_aliases found: {:?}", denormalized);
    denormalized
}

/// Helper function to find the anchor/first node in a multi-hop pattern
/// The anchor is the node that should be in the FROM clause
/// Strategy: Prefer required (non-optional) nodes over optional nodes
/// When mixing MATCH and OPTIONAL MATCH, the required node should be the anchor (FROM table)
///
/// Algorithm:
/// 1. PRIORITY: Find ANY required node (handles MATCH (n) + OPTIONAL MATCH patterns around n)
/// 2. Find true leftmost node (left-only) among required nodes
/// 3. Fall back to any required node if no leftmost required found
/// 4. Fall back to traditional anchor pattern for all-optional cases
/// 5. CRITICAL: Skip denormalized aliases (extracted from GraphNode.is_denormalized in plan tree)
pub(super) fn find_anchor_node(
    connections: &[(String, String, String)],
    optional_aliases: &std::collections::HashSet<String>,
    denormalized_aliases: &std::collections::HashSet<String>,
) -> Option<String> {
    if connections.is_empty() {
        return None;
    }

    // CRITICAL FIX FOR OPTIONAL MATCH BUG:
    // When we have MATCH (n:User) OPTIONAL MATCH (n)-[:FOLLOWS]->(out) OPTIONAL MATCH (in)-[:FOLLOWS]->(n)
    // The connections are: [(n, out, FOLLOWS), (in, n, FOLLOWS)]
    // Traditional leftmost logic would choose 'in' (left-only), but 'in' is optional!
    // We must prioritize 'n' (required) even though it appears on both sides.

    // Strategy 0: Collect all unique nodes (left and right)
    let mut all_nodes: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (left, right, _) in connections {
        all_nodes.insert(left.clone());
        all_nodes.insert(right.clone());
    }

    // Strategy 1: Find ANY required node - this handles the OPTIONAL MATCH around required node case
    // If there's a required node anywhere in the pattern, use it as anchor
    // CRITICAL: Filter out denormalized aliases (virtual nodes on edge tables)
    let required_nodes: Vec<String> = all_nodes
        .iter()
        .filter(|node| {
            let is_optional = optional_aliases.contains(*node);
            let is_denormalized = denormalized_aliases.contains(*node);
            log::debug!(
                "🔍 find_anchor_node: node='{}' optional={} denormalized={}",
                node,
                is_optional,
                is_denormalized
            );
            !is_optional && !is_denormalized
        })
        .cloned()
        .collect();

    log::info!(
        "🔍 find_anchor_node: required_nodes after filtering: {:?}",
        required_nodes
    );

    if !required_nodes.is_empty() {
        // We have required nodes - prefer one that's truly leftmost (left-only)
        let right_nodes: std::collections::HashSet<_> = connections
            .iter()
            .map(|(_, right, _)| right.clone())
            .collect();

        // Check if any required node is leftmost (left-only)
        // CRITICAL: Also skip denormalized aliases
        for (left, _, _) in connections {
            if !right_nodes.contains(left)
                && !optional_aliases.contains(left)
                && !denormalized_aliases.contains(left)
            {
                log::info!(
                    "✓ Found REQUIRED leftmost anchor: {} (required + left-only)",
                    left
                );
                return Some(left.clone());
            }
        }

        // No required node is leftmost, just use the first required node we find
        let anchor = required_nodes[0].clone();
        log::info!(
            "✓ Found REQUIRED anchor (not leftmost): {} (required node in mixed pattern)",
            anchor
        );
        return Some(anchor);
    }

    // CRITICAL: If required_nodes is EMPTY (all nodes are denormalized or optional),
    // return None to signal that the relationship table should be used as anchor!
    log::warn!(
        "🔍 find_anchor_node: All nodes filtered out (denormalized/optional), returning None"
    );
    if all_nodes.iter().all(|n| denormalized_aliases.contains(n)) {
        log::warn!(
            "🔍 find_anchor_node: All nodes are denormalized - use relationship table as FROM!"
        );
        return None;
    }

    // Strategy 2: No required nodes found - all optional. Use traditional leftmost logic.
    let right_nodes: std::collections::HashSet<_> = connections
        .iter()
        .map(|(_, right, _)| right.clone())
        .collect();

    for (left, _, _) in connections {
        if !right_nodes.contains(left) && !denormalized_aliases.contains(left) {
            log::info!(
                "✓ Found leftmost anchor (all optional): {} (left-only)",
                left
            );
            return Some(left.clone());
        }
    }

    // Strategy 3: Fallback to first left_connection (circular or complex pattern)
    let fallback = connections.first().map(|(left, _, _)| left.clone());
    if let Some(ref alias) = fallback {
        log::warn!("⚠️ No clear anchor, using fallback: {}", alias);
    }
    fallback
}

/// Helper function to check if a condition references an end node alias
pub(super) fn references_end_node_alias(
    condition: &OperatorApplication,
    connections: &[(String, String, String)],
) -> bool {
    let end_aliases: std::collections::HashSet<String> = connections
        .iter()
        .map(|(_, right_alias, _)| right_alias.clone())
        .collect();

    // Check if any operand in the condition references an end node alias
    condition.operands.iter().any(|operand| match operand {
        RenderExpr::PropertyAccessExp(prop) => end_aliases.contains(&prop.table_alias.0),
        _ => false,
    })
}

/// Check if a condition references a specific node alias
pub(super) fn references_node_alias(condition: &OperatorApplication, node_alias: &str) -> bool {
    condition.operands.iter().any(|operand| match operand {
        RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == node_alias,
        _ => false,
    })
}

/// Visitor for rewriting path function calls to CTE column references
/// Converts: length(p) → hop_count, nodes(p) → path_nodes, relationships(p) → path_relationships
struct PathFunctionRewriter {
    path_var_name: String,
    table_alias: String,
}

impl ExprVisitor for PathFunctionRewriter {
    fn transform_scalar_fn_call(&mut self, name: &str, args: Vec<RenderExpr>) -> RenderExpr {
        // Check if this is a path function call with the path variable as argument
        if args.len() == 1 {
            if let RenderExpr::TableAlias(TableAlias(alias)) = &args[0] {
                if alias == &self.path_var_name {
                    // Convert path functions to CTE column references
                    let column_name = match name {
                        "length" => Some("hop_count"),
                        "nodes" => Some("path_nodes"),
                        "relationships" => Some("path_relationships"),
                        _ => None,
                    };

                    if let Some(col_name) = column_name {
                        return if self.table_alias.is_empty() {
                            RenderExpr::Column(Column(PropertyValue::Column(col_name.to_string())))
                        } else {
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(self.table_alias.clone()),
                                column: PropertyValue::Column(col_name.to_string()),
                            })
                        };
                    }
                }
            }
        }

        // Default: recursively handled args + rebuild call
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: name.to_string(),
            args,
        })
    }
}

/// Rewrite path function calls (length, nodes, relationships) to CTE column references
/// Converts: length(p) → hop_count, nodes(p) → path_nodes, relationships(p) → path_relationships
pub(super) fn rewrite_path_functions(expr: &RenderExpr, path_var_name: &str) -> RenderExpr {
    rewrite_path_functions_with_table(expr, path_var_name, "")
}

use super::cte_extraction::FixedPathInfo;

/// Rewrite path function calls for FIXED multi-hop patterns (no variable length)
/// For fixed patterns, we know the hop count and aliases at compile time
/// Converts:
/// - length(p) → literal hop_count value
/// - nodes(p) → [r1.from_id, r1.to_id, r2.to_id, ...] array of node IDs
/// - relationships(p) → [r1, r2, ...] tuple of relationship aliases
pub(super) fn rewrite_fixed_path_functions_with_info(
    expr: &RenderExpr,
    path_info: &FixedPathInfo,
) -> RenderExpr {
    match expr {
        RenderExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function call with the path variable as argument
            if fn_call.args.len() == 1 {
                if let RenderExpr::TableAlias(TableAlias(alias)) = &fn_call.args[0] {
                    if alias == &path_info.path_var_name {
                        match fn_call.name.as_str() {
                            "length" => {
                                // Convert length(p) to literal hop count
                                return RenderExpr::Literal(super::render_expr::Literal::Integer(
                                    path_info.hop_count as i64,
                                ));
                            }
                            "nodes" => {
                                // Build array of node ID references: [r1.Origin, r1.Dest, r2.Dest]
                                let node_args: Vec<RenderExpr> = path_info
                                    .node_aliases
                                    .iter()
                                    .filter_map(|node_alias| {
                                        // Look up the ID column for this node
                                        path_info.node_id_columns.get(node_alias).map(
                                            |(rel_alias, id_col)| {
                                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                                    table_alias: TableAlias(rel_alias.clone()),
                                                    column: PropertyValue::Column(id_col.clone()),
                                                })
                                            },
                                        )
                                    })
                                    .collect();

                                // Use array() function for ClickHouse arrays
                                if node_args.is_empty() {
                                    // Fallback: if no ID columns found, return tuple of aliases
                                    let fallback_args: Vec<RenderExpr> = path_info
                                        .node_aliases
                                        .iter()
                                        .map(|a| RenderExpr::TableAlias(TableAlias(a.clone())))
                                        .collect();
                                    return RenderExpr::ScalarFnCall(ScalarFnCall {
                                        name: "tuple".to_string(),
                                        args: fallback_args,
                                    });
                                }

                                return RenderExpr::ScalarFnCall(ScalarFnCall {
                                    name: "array".to_string(),
                                    args: node_args,
                                });
                            }
                            "relationships" => {
                                // For relationships, return tuple of relationship aliases
                                // These will resolve to the full row data via * or specific columns
                                let rel_args: Vec<RenderExpr> = path_info
                                    .rel_aliases
                                    .iter()
                                    .map(|alias| RenderExpr::TableAlias(TableAlias(alias.clone())))
                                    .collect();
                                return RenderExpr::ScalarFnCall(ScalarFnCall {
                                    name: "tuple".to_string(),
                                    args: rel_args,
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Recursively rewrite arguments for nested calls
            let rewritten_args: Vec<RenderExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_fixed_path_functions_with_info(arg, path_info))
                .collect();

            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands: Vec<RenderExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_fixed_path_functions_with_info(operand, path_info))
                .collect();

            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: rewritten_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments for aggregate functions
            let rewritten_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_fixed_path_functions_with_info(arg, path_info))
                .collect();

            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(), // For other expression types, return as-is
    }
}

/// Legacy version that only handles length(p)
/// Kept for backward compatibility
pub(super) fn rewrite_fixed_path_functions(
    expr: &RenderExpr,
    path_var_name: &str,
    hop_count: u32,
) -> RenderExpr {
    // Create minimal path info for length-only rewriting
    let path_info = FixedPathInfo {
        path_var_name: path_var_name.to_string(),
        node_aliases: vec![],
        rel_aliases: vec![],
        hop_count,
        node_id_columns: std::collections::HashMap::new(),
    };
    rewrite_fixed_path_functions_with_info(expr, &path_info)
}

/// Rewrite path function calls with optional table alias
/// table_alias: if provided, generates PropertyAccessExp (table.column), otherwise Column
pub(super) fn rewrite_path_functions_with_table(
    expr: &RenderExpr,
    path_var_name: &str,
    table_alias: &str,
) -> RenderExpr {
    let mut rewriter = PathFunctionRewriter {
        path_var_name: path_var_name.to_string(),
        table_alias: table_alias.to_string(),
    };
    rewriter.transform_expr(expr)
}

/// Rewrite path function calls on LogicalExpr (before conversion to RenderExpr)
/// This is used for WITH clause expressions that need path function rewriting
/// Converts: length(p) → PropertyAccess(t, hop_count), nodes(p) → PropertyAccess(t, path_nodes)
pub(super) fn rewrite_logical_path_functions(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    path_var_name: &str,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{
        LogicalExpr, PropertyAccess, ScalarFnCall, TableAlias,
    };

    match expr {
        LogicalExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function call with the path variable as argument
            if fn_call.args.len() == 1 {
                if let LogicalExpr::TableAlias(TableAlias(alias)) = &fn_call.args[0] {
                    if alias == path_var_name {
                        // Convert path functions to CTE column references
                        // 🔧 FIX (Jan 23, 2026): Generate bare Column, not PropertyAccess with "t"
                        // In WITH clause contexts, the VLP CTE may be aliased differently (e.g., "path" instead of "t")
                        // Using bare columns lets the SQL renderer add the correct table alias later
                        let column_name = match fn_call.name.as_str() {
                            "length" => Some("hop_count"),
                            "nodes" => Some("path_nodes"),
                            "relationships" => Some("path_relationships"),
                            _ => None,
                        };

                        if let Some(col_name) = column_name {
                            // Generate a bare PropertyAccess without table alias
                            // This will be converted to RenderExpr::Column later,
                            // which the SQL renderer recognizes as a VLP column
                            return LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias("__vlp_bare_col".to_string()), // Special marker for bare column
                                column: PropertyValue::Column(col_name.to_string()),
                            });
                        }
                    }
                }
            }

            // Recursively rewrite function arguments
            let rewritten_args: Vec<LogicalExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_logical_path_functions(arg, path_var_name))
                .collect();

            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        LogicalExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments for aggregate functions
            let rewritten_args: Vec<LogicalExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_logical_path_functions(arg, path_var_name))
                .collect();

            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands: Vec<LogicalExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_logical_path_functions(operand, path_var_name))
                .collect();

            LogicalExpr::OperatorApplicationExp(
                crate::query_planner::logical_expr::OperatorApplication {
                    operator: op.operator,
                    operands: rewritten_operands,
                },
            )
        }
        _ => expr.clone(), // For other expression types, return as-is
    }
}

/// Helper function to get node table name for a given alias
/// DEPRECATED: This function uses GLOBAL_SCHEMAS which may not have correct schema.
/// Prefer using schema parameter passed through the call chain.
pub(super) fn get_node_table_for_alias(alias: &str) -> String {
    // Try to get from global schema - look for "default" or first available
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            // Try "default" first, then fall back to first schema
            let schema_opt = schemas.get("default").or_else(|| schemas.values().next());

            if let Some(schema) = schema_opt {
                // Look up the node type from the alias - this is a simplified lookup
                // In a real implementation, we'd need to track node types per alias
                // For now, assume "User" type for common cases
                if let Some(user_node) = schema.node_schema_opt("User") {
                    // Return fully qualified table name: database.table_name
                    return format!("{}.{}", user_node.database, user_node.table_name);
                }
            } else {
                log::error!("❌ SCHEMA ERROR: No schemas loaded in GLOBAL_SCHEMAS. Cannot resolve alias '{}'.", alias);
                return format!("ERROR_NO_SCHEMA_FOR_ALIAS_{}", alias);
            }
        }
    }

    // No GLOBAL_SCHEMAS available at all
    log::error!(
        "❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized. Cannot resolve alias '{}'.",
        alias
    );
    format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", alias)
}

/// Helper function to get node ID columns for a given alias
/// Returns Vec of column names (single element for simple ID, multiple for composite)
/// DEPRECATED: This function uses GLOBAL_SCHEMAS which may not have correct schema.
/// Prefer using schema parameter passed through the call chain.
pub(super) fn get_node_id_columns_for_alias(alias: &str) -> Vec<String> {
    // Try to get from global schema - look for "default" or first available
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            // Try "default" first, then fall back to first schema
            let schema_opt = schemas.get("default").or_else(|| schemas.values().next());

            if let Some(schema) = schema_opt {
                // Look up the node type from the alias - this is a simplified lookup
                if let Some(user_node) = schema.node_schema_opt("User") {
                    return user_node
                        .node_id
                        .columns()
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                }
            } else {
                log::error!(
                    "❌ SCHEMA ERROR: No schemas loaded. Cannot get ID columns for alias '{}'.",
                    alias
                );
                return vec![format!("ERROR_NO_SCHEMA_FOR_{}", alias)];
            }
        }
    }

    // No GLOBAL_SCHEMAS available
    log::error!(
        "❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized. Cannot get ID columns for alias '{}'.",
        alias
    );
    vec![format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", alias)]
}

/// Backwards compatibility wrapper - returns first column only
/// TODO: Update VLP code to use get_node_id_columns_for_alias directly
///
/// Returns "id" as default if no specific column found - this is a safe default
/// for most graph schemas. However, callers should verify the column exists.
pub(super) fn get_node_id_column_for_alias(alias: &str) -> String {
    get_node_id_columns_for_alias(alias)
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            // Log warning but return "id" as conventional default
            log::warn!(
                "No specific ID column found for alias '{}', using 'id' as default. \
                 If query fails with 'column id not found', check schema defines id_column.",
                alias
            );
            "id".to_string()
        })
}

/// Get relationship columns from schema by relationship type
/// Returns (from_column, to_column) for a given relationship type
/// DEPRECATED: Uses GLOBAL_SCHEMAS. Prefer passing schema through call chain.
pub(super) fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            // Try "default" first, then fall back to first schema
            let schema_opt = schemas.get("default").or_else(|| schemas.values().next());

            if let Some(schema) = schema_opt {
                if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                    return Some((
                        rel_schema.from_id.clone(), // Use column names, not node types!
                        rel_schema.to_id.clone(),
                    ));
                }
            } else {
                log::error!("❌ SCHEMA ERROR: No schemas loaded. Cannot get relationship columns for type '{}'.", rel_type);
            }
        }
    }
    None
}

/// Get relationship columns from schema by table name
/// Searches all relationship schemas to find one with matching table name
/// DEPRECATED: Uses GLOBAL_SCHEMAS. Prefer passing schema through call chain.
pub(super) fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            // Try "default" first, then fall back to first schema
            let schema_opt = schemas.get("default").or_else(|| schemas.values().next());

            if let Some(schema) = schema_opt {
                // Search through all relationship schemas for one with matching table name
                for (_key, rel_schema) in schema.get_relationships_schemas().iter() {
                    if rel_schema.table_name == table_name {
                        return Some((
                            rel_schema.from_id.clone(), // Use column names!
                            rel_schema.to_id.clone(),
                        ));
                    }
                }
            } else {
                log::error!(
                    "❌ SCHEMA ERROR: No schemas loaded. Cannot get columns for table '{}'.",
                    table_name
                );
            }
        }
    }
    None
}

/// Get node table name and ID columns from schema
/// Returns (table_name, id_columns) for a given node label
/// DEPRECATED: Uses GLOBAL_SCHEMAS. Prefer passing schema through call chain.
pub(super) fn get_node_info_from_schema(node_label: &str) -> Option<(String, Vec<String>)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            // Try "default" first, then fall back to first schema
            let schema_opt = schemas.get("default").or_else(|| schemas.values().next());

            if let Some(schema) = schema_opt {
                if let Ok(node_schema) = schema.node_schema(node_label) {
                    return Some((
                        node_schema.table_name.clone(),
                        node_schema
                            .node_id
                            .columns()
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ));
                }
            } else {
                log::error!(
                    "❌ SCHEMA ERROR: No schemas loaded. Cannot get node info for label '{}'.",
                    node_label
                );
            }
        }
    }
    None
}

// =============================================================================
// PROPER SCHEMA-PARAMETERIZED VERSIONS
// These functions take schema as a parameter and should be used instead of the
// deprecated versions above that access GLOBAL_SCHEMAS directly.
// =============================================================================

/// Get node table name for a given alias using plan context and schema
/// This is the CORRECT way - uses plan to get label, then schema for table lookup
pub(super) fn get_node_table_for_alias_with_schema(
    alias: &str,
    plan: &LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Option<String> {
    // Get the node label from the plan
    let label = get_node_label_for_alias(alias, plan)?;

    // Look up the table from schema
    let node_schema = schema.node_schema(&label).ok()?;

    // Return fully qualified table name
    Some(format!(
        "{}.{}",
        node_schema.database, node_schema.table_name
    ))
}

/// Get node ID column for a given alias using plan context and schema
/// This is the CORRECT way - uses plan to get label, then schema for column lookup
pub(super) fn get_node_id_column_for_alias_with_schema(
    alias: &str,
    plan: &LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Option<String> {
    // Get the node label from the plan
    let label = get_node_label_for_alias(alias, plan)?;

    // Look up the node schema
    let node_schema = schema.node_schema(&label).ok()?;

    // Return first ID column
    node_schema.node_id.columns().first().map(|s| s.to_string())
}

/// Get node ID columns (for composite keys) using plan context and schema
pub(super) fn get_node_id_columns_for_alias_with_schema(
    alias: &str,
    plan: &LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Option<Vec<String>> {
    // Get the node label from the plan
    let label = get_node_label_for_alias(alias, plan)?;

    // Look up the node schema
    let node_schema = schema.node_schema(&label).ok()?;

    // Return all ID columns
    Some(
        node_schema
            .node_id
            .columns()
            .iter()
            .map(|s| s.to_string())
            .collect(),
    )
}

/// Get node info (table name and ID columns) for a given label using schema
pub(super) fn get_node_info_from_schema_with_schema(
    node_label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Option<(String, Vec<String>)> {
    let node_schema = schema.node_schema(node_label).ok()?;
    Some((
        format!("{}.{}", node_schema.database, node_schema.table_name),
        node_schema
            .node_id
            .columns()
            .iter()
            .map(|s| s.to_string())
            .collect(),
    ))
}

/// Get relationship columns using schema directly
pub(super) fn get_relationship_columns_with_schema(
    rel_type: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Option<(String, String)> {
    let rel_schema = schema.get_rel_schema(rel_type).ok()?;
    Some((rel_schema.from_id.clone(), rel_schema.to_id.clone()))
}

/// Check if a node with the given alias is polymorphic ($any)
/// A polymorphic node is represented by a GraphNode whose input is a Scan with no table_name
pub(super) fn is_node_polymorphic(plan: &LogicalPlan, target_alias: &str) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                // Check if input is Empty ($any wildcard)
                if let LogicalPlan::Empty = node.input.as_ref() {
                    return true;
                }
            }
            is_node_polymorphic(&node.input, target_alias)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            is_node_polymorphic(&graph_rel.left, target_alias)
                || is_node_polymorphic(&graph_rel.right, target_alias)
        }
        LogicalPlan::GraphJoins(joins) => is_node_polymorphic(&joins.input, target_alias),
        LogicalPlan::Projection(proj) => is_node_polymorphic(&proj.input, target_alias),
        LogicalPlan::Filter(filter) => is_node_polymorphic(&filter.input, target_alias),
        LogicalPlan::GroupBy(gb) => is_node_polymorphic(&gb.input, target_alias),
        LogicalPlan::OrderBy(ob) => is_node_polymorphic(&ob.input, target_alias),
        LogicalPlan::Limit(limit) => is_node_polymorphic(&limit.input, target_alias),
        LogicalPlan::Skip(skip) => is_node_polymorphic(&skip.input, target_alias),
        LogicalPlan::Unwind(u) => is_node_polymorphic(&u.input, target_alias),
        _ => false,
    }
}

/// Check if a logical plan contains any GraphRel with multiple relationship types
/// NOTE: Deduplicates labels first - [:FOLLOWS|FOLLOWS] is treated as single type
pub(super) fn has_multiple_relationship_types(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                // Normalize composite keys to base types before deduplication
                // "REQUESTED::IP::Domain" and "REQUESTED" refer to the same relationship type
                let normalized_labels: Vec<&str> = labels
                    .iter()
                    .map(|label| {
                        // Extract base type from composite key: "TYPE::Node1::Node2" -> "TYPE"
                        label.split("::").next().unwrap_or(label.as_str())
                    })
                    .collect();

                // Deduplicate - [:FOLLOWS|FOLLOWS] or ["REQUESTED", "REQUESTED::IP::Domain"] treated as single type
                let unique_base_types: std::collections::HashSet<_> =
                    normalized_labels.into_iter().collect();
                if unique_base_types.len() > 1 {
                    return true;
                }
            }
            // Check child plans
            has_multiple_relationship_types(&graph_rel.left)
                || has_multiple_relationship_types(&graph_rel.right)
        }
        LogicalPlan::GraphJoins(joins) => has_multiple_relationship_types(&joins.input),
        LogicalPlan::Projection(proj) => has_multiple_relationship_types(&proj.input),
        LogicalPlan::Filter(filter) => has_multiple_relationship_types(&filter.input),
        LogicalPlan::GraphNode(node) => has_multiple_relationship_types(&node.input),
        LogicalPlan::GroupBy(gb) => has_multiple_relationship_types(&gb.input),
        LogicalPlan::OrderBy(ob) => has_multiple_relationship_types(&ob.input),
        LogicalPlan::Limit(limit) => has_multiple_relationship_types(&limit.input),
        LogicalPlan::Skip(skip) => has_multiple_relationship_types(&skip.input),
        LogicalPlan::Unwind(u) => has_multiple_relationship_types(&u.input),
        _ => false,
    }
}

/// Check if a logical plan contains a polymorphic edge (CTE with rel_ prefix)
/// OR multiple relationship types (for backward compat)
pub(super) fn has_polymorphic_or_multi_rel(plan: &LogicalPlan) -> bool {
    // Check for multi-rel patterns
    if has_multiple_relationship_types(plan) {
        return true;
    }

    // Check for polymorphic edge patterns - look for $any nodes in GraphRel
    has_polymorphic_edge(plan)
}

// =============================================================================
// TODO: Relationship Uniqueness Filtering for Undirected Multi-Hop Patterns
// =============================================================================
// The following structs and functions are prepared for implementing Issue #2
// (Undirected Patterns - Relationship Uniqueness) from KNOWN_ISSUES.md.
//
// However, they cannot be used yet because Issue #1 (Undirected Multi-Hop
// Patterns Generate Broken SQL) must be fixed first. The BidirectionalUnion
// optimizer transforms Direction::Either patterns into Union nodes, which
// breaks the multi-hop JOIN inference that these filters depend on.
//
// Once Issue #1 is fixed, uncomment and integrate these helpers.
// =============================================================================

/*
/// Information about an undirected relationship for uniqueness filtering
#[derive(Debug, Clone)]
pub struct UndirectedRelInfo {
    pub alias: String,          // Relationship alias (e.g., "r1")
    pub from_id_col: String,    // FROM ID column name
    pub to_id_col: String,      // TO ID column name
    pub edge_id_cols: Vec<String>, // Edge ID columns (for composite uniqueness)
}

/// Collect all undirected (Direction::Either) relationships from a logical plan.
/// Returns info needed to generate pairwise uniqueness filters.
pub(super) fn collect_undirected_relationships(plan: &LogicalPlan) -> Result<Vec<UndirectedRelInfo>, RenderBuildError> {
    fn collect(plan: &LogicalPlan, result: &mut Vec<UndirectedRelInfo>) -> Result<(), RenderBuildError> {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if this relationship is undirected
                if graph_rel.direction == Direction::Either {
                    // Extract relationship columns from the center (ViewScan)
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        // ViewScan should have these populated by query planner
                        let from_col = scan.from_id.clone()
                            .ok_or_else(|| RenderBuildError::ViewScanMissingRelationshipColumn("from_id".to_string()))?;
                        let to_col = scan.to_id.clone()
                            .ok_or_else(|| RenderBuildError::ViewScanMissingRelationshipColumn("to_id".to_string()))?;

                        // Try to get edge_id columns from schema
                        // First, try to look up the relationship schema by type
                        let edge_id_cols = if let Some(labels) = &graph_rel.labels {
                            if let Some(rel_type) = labels.first() {
                                // Look up relationship schema from GLOBAL_SCHEMAS
                                if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                    if let Ok(schemas) = schemas_lock.try_read() {
                                        if let Some(default_schema) = schemas.get("default") {
                                            if let Some(rel_schema) = default_schema.get_relationships_schema_opt(rel_type) {
                                                match &rel_schema.edge_id {
                                                    Some(id) => id.columns().iter().map(|s| s.to_string()).collect(),
                                                    None => vec![from_col.clone(), to_col.clone()],
                                                }
                                            } else {
                                                vec![from_col.clone(), to_col.clone()]
                                            }
                                        } else {
                                            vec![from_col.clone(), to_col.clone()]
                                        }
                                    } else {
                                        vec![from_col.clone(), to_col.clone()]
                                    }
                                } else {
                                    vec![from_col.clone(), to_col.clone()]
                                }
                            } else {
                                vec![from_col.clone(), to_col.clone()]
                            }
                        } else {
                            vec![from_col.clone(), to_col.clone()]
                        };

                        result.push(UndirectedRelInfo {
                            alias: graph_rel.alias.clone(),
                            from_id_col: from_col,
                            to_id_col: to_col,
                            edge_id_cols,
                        });
                    }
                }

                // Recursively check children (for multi-hop patterns)
                collect(&graph_rel.left, result)?;
                collect(&graph_rel.center, result)?;
                collect(&graph_rel.right, result)?;
            }
            LogicalPlan::GraphNode(node) => collect(&node.input, result)?,
            LogicalPlan::GraphJoins(joins) => collect(&joins.input, result)?,
            LogicalPlan::Projection(proj) => collect(&proj.input, result)?,
            LogicalPlan::Filter(filter) => collect(&filter.input, result)?,
            LogicalPlan::GroupBy(gb) => collect(&gb.input, result)?,
            LogicalPlan::OrderBy(ob) => collect(&ob.input, result)?,
            LogicalPlan::Limit(limit) => collect(&limit.input, result)?,
            LogicalPlan::Skip(skip) => collect(&skip.input, result)?,
            LogicalPlan::Unwind(u) => collect(&u.input, result)?,
            _ => {}
        }
        Ok(())
    }

    let mut result = Vec::new();
    collect(plan, &mut result)?;
    Ok(result)
}

/// Generate pairwise relationship uniqueness filters for undirected patterns.
///
/// For undirected multi-hop patterns like `(a)-[r1]-(b)-[r2]-(c)`, we need to prevent
/// the same physical edge from being traversed twice (once in each direction).
///
/// For each pair (r1, r2), generates:
/// ```sql
/// NOT (
///     tuple(r1.col1, r1.col2, ...) = tuple(r2.col1, r2.col2, ...)
/// )
/// ```
pub(super) fn generate_undirected_uniqueness_filters(
    undirected_rels: &[UndirectedRelInfo],
) -> Option<RenderExpr> {
    if undirected_rels.len() < 2 {
        return None; // Need at least 2 relationships for pairwise comparison
    }

    let mut filters = Vec::new();

    // Generate pairwise filters for all combinations
    for i in 0..undirected_rels.len() {
        for j in (i + 1)..undirected_rels.len() {
            let r1 = &undirected_rels[i];
            let r2 = &undirected_rels[j];

            // Generate: NOT (tuple(r1.cols...) = tuple(r2.cols...))
            // This prevents the same physical edge from being used twice

            // Build tuple expressions for each relationship's edge_id columns
            let r1_tuple_args: Vec<RenderExpr> = r1.edge_id_cols.iter().map(|col| {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r1.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                })
            }).collect();

            let r2_tuple_args: Vec<RenderExpr> = r2.edge_id_cols.iter().map(|col| {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r2.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                })
            }).collect();

            // Create tuple expressions
            let r1_tuple = if r1_tuple_args.len() == 1 {
                r1_tuple_args.into_iter().next().unwrap()
            } else {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: r1_tuple_args,
                })
            };

            let r2_tuple = if r2_tuple_args.len() == 1 {
                r2_tuple_args.into_iter().next().unwrap()
            } else {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: r2_tuple_args,
                })
            };

            // Generate: NOT (r1_tuple = r2_tuple)
            let equality_check = RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![r1_tuple, r2_tuple],
            });

            let not_equal = RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![equality_check],
            });

            filters.push(not_equal);
        }
    }

    if filters.is_empty() {
        return None;
    }

    // Combine all filters with AND
    Some(filters.into_iter().reduce(|acc, filter| {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![acc, filter],
        })
    }).unwrap())
}
*/

/// Check if a logical plan contains any polymorphic edge (right node is $any)
/// A polymorphic edge is detected when the right GraphNode has a Scan with no table_name
pub(super) fn has_polymorphic_edge(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if right node is polymorphic ($any)
            if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                if let LogicalPlan::Empty = right_node.input.as_ref() {
                    log::debug!(
                        "has_polymorphic_edge: Found $any right node '{}'",
                        right_node.alias
                    );
                    return true;
                }
            }
            // Check child plans
            has_polymorphic_edge(&graph_rel.left) || has_polymorphic_edge(&graph_rel.right)
        }
        LogicalPlan::GraphJoins(joins) => has_polymorphic_edge(&joins.input),
        LogicalPlan::Projection(proj) => has_polymorphic_edge(&proj.input),
        LogicalPlan::Filter(filter) => has_polymorphic_edge(&filter.input),
        LogicalPlan::GraphNode(node) => has_polymorphic_edge(&node.input),
        LogicalPlan::GroupBy(gb) => has_polymorphic_edge(&gb.input),
        LogicalPlan::OrderBy(ob) => has_polymorphic_edge(&ob.input),
        LogicalPlan::Limit(limit) => has_polymorphic_edge(&limit.input),
        LogicalPlan::Skip(skip) => has_polymorphic_edge(&skip.input),
        LogicalPlan::Unwind(u) => has_polymorphic_edge(&u.input),
        _ => false,
    }
}

/// Get the relationship alias from a POLYMORPHIC GraphRel pattern
/// Only returns alias if the right node is polymorphic ($any)
pub(super) fn get_polymorphic_relationship_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if right node is polymorphic ($any)
            if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                if let LogicalPlan::Empty = right_node.input.as_ref() {
                    // This is a polymorphic edge - return its alias
                    if !graph_rel.alias.is_empty() {
                        return Some(graph_rel.alias.clone());
                    }
                }
            }
            // Check child plans
            get_polymorphic_relationship_alias(&graph_rel.left)
                .or_else(|| get_polymorphic_relationship_alias(&graph_rel.right))
        }
        LogicalPlan::GraphJoins(joins) => get_polymorphic_relationship_alias(&joins.input),
        LogicalPlan::Projection(proj) => get_polymorphic_relationship_alias(&proj.input),
        LogicalPlan::Filter(filter) => get_polymorphic_relationship_alias(&filter.input),
        LogicalPlan::GraphNode(node) => get_polymorphic_relationship_alias(&node.input),
        LogicalPlan::GroupBy(gb) => get_polymorphic_relationship_alias(&gb.input),
        LogicalPlan::OrderBy(ob) => get_polymorphic_relationship_alias(&ob.input),
        LogicalPlan::Limit(limit) => get_polymorphic_relationship_alias(&limit.input),
        LogicalPlan::Skip(skip) => get_polymorphic_relationship_alias(&skip.input),
        LogicalPlan::Unwind(u) => get_polymorphic_relationship_alias(&u.input),
        _ => None,
    }
}

/// Information about a polymorphic edge for CTE processing
#[derive(Debug, Clone)]
pub(super) struct PolymorphicEdgeInfo {
    pub rel_alias: String,        // e.g., "r1" or "r2"
    pub left_connection: String,  // e.g., "u" or "source"
    pub right_connection: String, // e.g., "middle" or "u"
    pub cte_name: String,         // e.g., "rel_u_middle" or "rel_source_u"
    pub is_incoming: bool,        // true for (a)<-[r]-(b), false for (a)-[r]->(b)
}

/// Collect ALL polymorphic edges from the logical plan
/// Returns a list of PolymorphicEdgeInfo for each polymorphic edge found
pub(super) fn collect_polymorphic_edges(plan: &LogicalPlan) -> Vec<PolymorphicEdgeInfo> {
    fn collect_inner(plan: &LogicalPlan, edges: &mut Vec<PolymorphicEdgeInfo>) {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if right node is polymorphic ($any) - for outgoing edges
                let right_is_polymorphic =
                    if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                        matches!(right_node.input.as_ref(), LogicalPlan::Empty)
                    } else {
                        false
                    };

                // Check if left node is polymorphic ($any) - for incoming edges
                let left_is_polymorphic =
                    if let LogicalPlan::GraphNode(left_node) = graph_rel.left.as_ref() {
                        matches!(left_node.input.as_ref(), LogicalPlan::Empty)
                    } else {
                        false
                    };

                let is_polymorphic = right_is_polymorphic || left_is_polymorphic;

                if is_polymorphic && !graph_rel.alias.is_empty() {
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );
                    edges.push(PolymorphicEdgeInfo {
                        rel_alias: graph_rel.alias.clone(),
                        left_connection: graph_rel.left_connection.clone(),
                        right_connection: graph_rel.right_connection.clone(),
                        cte_name,
                        // Track which side is polymorphic for proper JOIN direction
                        is_incoming: left_is_polymorphic && !right_is_polymorphic,
                    });
                }
                // Recurse into children
                collect_inner(&graph_rel.left, edges);
                collect_inner(&graph_rel.right, edges);
            }
            LogicalPlan::GraphJoins(joins) => collect_inner(&joins.input, edges),
            LogicalPlan::Projection(proj) => collect_inner(&proj.input, edges),
            LogicalPlan::Filter(filter) => collect_inner(&filter.input, edges),
            LogicalPlan::GraphNode(node) => collect_inner(&node.input, edges),
            LogicalPlan::GroupBy(gb) => collect_inner(&gb.input, edges),
            LogicalPlan::OrderBy(ob) => collect_inner(&ob.input, edges),
            LogicalPlan::Limit(limit) => collect_inner(&limit.input, edges),
            LogicalPlan::Skip(skip) => collect_inner(&skip.input, edges),
            LogicalPlan::Unwind(u) => collect_inner(&u.input, edges),
            _ => {}
        }
    }

    let mut edges = Vec::new();
    collect_inner(plan, &mut edges);
    edges
}

/// Get the relationship alias from a GraphRel pattern (e.g., for MATCH (a)-[r]->(b), returns "r")
pub(super) fn get_relationship_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Return the alias if it exists and is not empty
            if graph_rel.alias.is_empty() {
                None
            } else {
                Some(graph_rel.alias.clone())
            }
        }
        LogicalPlan::GraphJoins(joins) => get_relationship_alias(&joins.input),
        LogicalPlan::Projection(proj) => get_relationship_alias(&proj.input),
        LogicalPlan::Filter(filter) => get_relationship_alias(&filter.input),
        LogicalPlan::GraphNode(node) => get_relationship_alias(&node.input),
        LogicalPlan::GroupBy(gb) => get_relationship_alias(&gb.input),
        LogicalPlan::OrderBy(ob) => get_relationship_alias(&ob.input),
        LogicalPlan::Limit(limit) => get_relationship_alias(&limit.input),
        LogicalPlan::Skip(skip) => get_relationship_alias(&skip.input),
        LogicalPlan::Unwind(u) => get_relationship_alias(&u.input),
        _ => None,
    }
}

/// Check if a logical plan contains any variable-length path or shortest path pattern
/// These require CTE-based processing (recursive CTEs)
pub(super) fn has_variable_length_or_shortest_path(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check for variable-length patterns that need CTEs
            if let Some(spec) = &graph_rel.variable_length {
                // Fixed-length (exact hops, no shortest path) can use inline JOINs
                let is_fixed_length =
                    spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

                if !is_fixed_length {
                    // Variable-length or shortest path needs CTE
                    return true;
                }
            }
            // Also check shortest path without variable_length (edge case)
            if graph_rel.shortest_path_mode.is_some() {
                return true;
            }
            // Check child plans
            has_variable_length_or_shortest_path(&graph_rel.left)
                || has_variable_length_or_shortest_path(&graph_rel.right)
        }
        LogicalPlan::GraphJoins(joins) => has_variable_length_or_shortest_path(&joins.input),
        LogicalPlan::Projection(proj) => has_variable_length_or_shortest_path(&proj.input),
        LogicalPlan::Filter(filter) => has_variable_length_or_shortest_path(&filter.input),
        LogicalPlan::GraphNode(node) => has_variable_length_or_shortest_path(&node.input),
        LogicalPlan::GroupBy(gb) => has_variable_length_or_shortest_path(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_or_shortest_path(&ob.input),
        LogicalPlan::Limit(limit) => has_variable_length_or_shortest_path(&limit.input),
        LogicalPlan::Skip(skip) => has_variable_length_or_shortest_path(&skip.input),
        LogicalPlan::Unwind(u) => has_variable_length_or_shortest_path(&u.input),
        _ => false,
    }
}

/// Convert RenderExpr to SQL string with node alias mapping for CTE generation
/// Maps Cypher aliases (e.g., "a", "b") to SQL table aliases (e.g., "start_node", "end_node")
pub(super) fn render_expr_to_sql_for_cte(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
) -> String {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column;

            // Map Cypher alias to SQL table alias
            if table_alias == start_cypher_alias {
                format!("start_node.{}", column.raw())
            } else if table_alias == end_cypher_alias {
                format!("end_node.{}", column.raw()) // end_node.name, end_node.email, etc.
            } else {
                // Fallback: use as-is
                format!("{}.{}", table_alias, column.raw())
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let operator_sql = match op.operator {
                Operator::Equal => "=",
                Operator::NotEqual => "!=",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::Not => "NOT",
                _ => "=", // Fallback
            };

            if op.operands.len() == 2 {
                format!(
                    "{} {} {}",
                    render_expr_to_sql_for_cte(
                        &op.operands[0],
                        start_cypher_alias,
                        end_cypher_alias
                    ),
                    operator_sql,
                    render_expr_to_sql_for_cte(
                        &op.operands[1],
                        start_cypher_alias,
                        end_cypher_alias
                    )
                )
            } else if op.operands.len() == 1 {
                format!(
                    "{} {}",
                    operator_sql,
                    render_expr_to_sql_for_cte(
                        &op.operands[0],
                        start_cypher_alias,
                        end_cypher_alias
                    )
                )
            } else {
                // Multiple operands with AND/OR
                let operand_sqls: Vec<String> = op
                    .operands
                    .iter()
                    .map(|operand| {
                        render_expr_to_sql_for_cte(operand, start_cypher_alias, end_cypher_alias)
                    })
                    .collect();
                format!("({})", operand_sqls.join(&format!(" {} ", operator_sql)))
            }
        }
        RenderExpr::Literal(lit) => match lit {
            Literal::String(s) => format!("'{}'", s),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
            Literal::Null => "NULL".to_string(),
        },
        _ => expr.to_sql(), // Fallback to default to_sql()
    }
}

/// Generate polymorphic edge type filters for a GraphRel
///
/// When a relationship table uses type discrimination columns (type_column, from_label_column,
/// to_label_column), this function generates filters to select the correct edge types.
///
/// # Arguments
/// * `rel_alias` - The alias for the relationship table (e.g., "r", "f")
/// * `rel_type` - The Cypher relationship type (e.g., "FOLLOWS")
/// * `from_label` - The source node label (e.g., "User")
/// * `to_label` - The target node label (e.g., "Post")
///
/// # Returns
/// A RenderExpr representing the combined filters, or None if not a polymorphic edge
///
/// # Example
/// For a polymorphic relationship table:
/// ```yaml
/// relationships:
///   - polymorphic: true
///     table: interactions
///     type_column: interaction_type
///     from_label_column: from_type
///     to_label_column: to_type
/// ```
///
/// Query: `MATCH (u:User)-[:FOLLOWS]->(other:User)`
///
/// Generates: `r.interaction_type = 'FOLLOWS' AND r.from_type = 'User' AND r.to_type = 'User'`
/// DEPRECATED: Uses GLOBAL_SCHEMAS. Should be refactored to accept schema parameter.
pub(super) fn generate_polymorphic_edge_filters(
    rel_alias: &str,
    rel_type: &str,
    from_label: &str,
    to_label: &str,
) -> Option<RenderExpr> {
    use crate::server::GLOBAL_SCHEMAS;

    // Get the relationship schema to check if it's polymorphic
    let schema_lock = GLOBAL_SCHEMAS.get()?;
    let schemas = schema_lock.try_read().ok()?;
    // Try "default" first, then fall back to first schema
    let schema = schemas.get("default").or_else(|| {
        log::warn!(
            "No 'default' schema found, using first available schema for polymorphic filters"
        );
        schemas.values().next()
    })?;
    let rel_schema = schema.get_rel_schema(rel_type).ok()?;

    // Check if this is a polymorphic edge
    let type_col = rel_schema.type_column.as_ref()?;
    let from_label_col = rel_schema.from_label_column.as_ref();
    let to_label_col = rel_schema.to_label_column.as_ref();

    let mut filters = Vec::new();

    // Filter 1: type_column = 'FOLLOWS'
    let type_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(type_col.clone()),
            }),
            RenderExpr::Literal(Literal::String(rel_type.to_string())),
        ],
    });
    filters.push(type_filter);

    // Filter 2: from_label_column = 'User' (if present)
    if let Some(from_col) = from_label_col {
        let from_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias.to_string()),
                    column: PropertyValue::Column(from_col.clone()),
                }),
                RenderExpr::Literal(Literal::String(from_label.to_string())),
            ],
        });
        filters.push(from_filter);
    }

    // Filter 3: to_label_column = 'Post' (if present)
    if let Some(to_col) = to_label_col {
        let to_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias.to_string()),
                    column: PropertyValue::Column(to_col.clone()),
                }),
                RenderExpr::Literal(Literal::String(to_label.to_string())),
            ],
        });
        filters.push(to_filter);
    }

    // Combine filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        }))
    }
}

// ============================================================================
// Plan utilities - moved from plan_builder.rs for better organization
// ============================================================================

/// Get human-readable name of a LogicalPlan variant
pub(super) fn plan_type_name(plan: &LogicalPlan) -> &'static str {
    match plan {
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
}

/// Apply property mapping to an expression
///
/// Main purpose: Convert TableAlias expressions to PropertyAccess for denormalized schemas.
/// For GROUP BY with a node alias like `b` in `(a)-[r1]->(b)-[r2]->(c)`, this converts
/// the TableAlias("b") to PropertyAccess { table_alias: "r2", column: "Origin" }
///
/// Also remaps PropertyAccess table aliases for nodes denormalized on edges.
/// For cross-table patterns like zeek logs, where `src` is denormalized on the DNS_REQUESTED
/// edge, this changes `src."id.orig_h"` to use the edge alias.
///
/// Note: Regular PropertyAccess property name mapping is handled in the FilterTagging analyzer pass.
pub(super) fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    match expr {
        RenderExpr::TableAlias(alias) => {
            // For denormalized schemas, convert TableAlias to the proper ID column reference
            // Example: TableAlias("b") -> PropertyAccess { table_alias: "r2", column: "Origin" }
            if let Some((rel_alias, id_column)) = get_denormalized_node_id_reference(&alias.0, plan)
            {
                use crate::graph_catalog::expression_parser::PropertyValue;
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias),
                    column: PropertyValue::Column(id_column),
                });
            }
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // First, try to map the property name to the correct column name
            // This is essential for queries like: WHERE n.name = ...
            // where 'name' might map to 'full_name' in the schema
            if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                log::warn!(
                    "🔍 PROPERTY MAPPING: Alias '{}' -> Label '{}', Property '{}' (before mapping)",
                    prop.table_alias.0,
                    node_label,
                    prop.column.raw()
                );

                // Map the property to the correct column
                let mapped_column = crate::render_plan::cte_generation::map_property_to_column_with_relationship_context(
                    prop.column.raw(),
                    &node_label,
                    None, // relationship_type
                    None, // node_role
                    None, // schema_name will be resolved from task-local
                ).unwrap_or_else(|_| prop.column.raw().to_string());

                log::warn!(
                    "🔍 PROPERTY MAPPING: '{}' -> '{}'",
                    prop.column.raw(),
                    mapped_column
                );

                prop.column = PropertyValue::Column(mapped_column);
            }

            // For denormalized nodes, remap the table alias to the edge alias
            // Example: PropertyAccess { table_alias: "src", column: "id.orig_h" }
            //       -> PropertyAccess { table_alias: "ad62047b83", column: "id.orig_h" }
            if let Some((rel_alias, _id_column)) =
                get_denormalized_node_id_reference(&prop.table_alias.0, plan)
            {
                prop.table_alias = TableAlias(rel_alias);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                apply_property_mapping_to_expr(operand, plan);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        RenderExpr::ScalarFnCall(scalar) => {
            for arg in &mut scalar.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        _ => {
            // PropertyAccess, Column, Literal, etc. don't need modification
        }
    }
}

/// Get the relationship alias and ID column for a denormalized node alias
/// For example, if `b` is the "to" node of `r1` or the "from" node of `r2`,
/// this returns (rel_alias, id_column_name).
///
/// IMPORTANT: This function should ONLY return a result for truly denormalized nodes
/// (where node properties are stored on the edge table, indicated by from_node_properties/to_node_properties).
/// For standard schemas where nodes have their own tables, this should return None so
/// the node alias stays pointing to the node table.
fn get_denormalized_node_id_reference(alias: &str, plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this node alias matches left or right connection
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // For multi-hop patterns like (a)-[r1]->(b)-[r2]->(c), we prefer
                // the "from" position because in GROUP BY b, we want r2.Origin
                // (where b is the origin/source of r2)

                // Check if node is the "from" node (left_connection) - this takes precedence
                // ONLY if the edge has from_node_properties (denormalized schema)
                if alias == rel.left_connection {
                    // Only remap if this is a denormalized node (properties on edge table)
                    if scan.from_node_properties.is_some() {
                        if let Some(from_id) = &scan.from_id {
                            return Some((rel.alias.clone(), from_id.clone()));
                        }
                    }
                }
                // Check if node is the "to" node (right_connection)
                // ONLY if the edge has to_node_properties (denormalized schema)
                if alias == rel.right_connection {
                    // Only remap if this is a denormalized node (properties on edge table)
                    if scan.to_node_properties.is_some() {
                        if let Some(to_id) = &scan.to_id {
                            return Some((rel.alias.clone(), to_id.clone()));
                        }
                    }
                }
            }

            // Recursively check branches (right first for more recent relationships)
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.right) {
                return Some(result);
            }
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.left) {
                return Some(result);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Check if this is a denormalized node
            if node.is_denormalized && node.alias == alias {
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if let Some(from_id) = &scan.from_id {
                        return Some((alias.to_string(), from_id.clone()));
                    }
                }
            }
            get_denormalized_node_id_reference(alias, &node.input)
        }
        LogicalPlan::Filter(filter) => get_denormalized_node_id_reference(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_denormalized_node_id_reference(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_denormalized_node_id_reference(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => {
            get_denormalized_node_id_reference(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => get_denormalized_node_id_reference(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_denormalized_node_id_reference(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => {
            get_denormalized_node_id_reference(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => get_denormalized_node_id_reference(alias, &cte.input),
        _ => None,
    }
}

/// Check if a filter expression appears to be invalid (e.g., "1 = 0")
pub(super) fn is_invalid_filter_expression(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::OperatorApplicationExp(op) => {
            if matches!(op.operator, Operator::Equal) && op.operands.len() == 2 {
                matches!(
                    (&op.operands[0], &op.operands[1]),
                    (
                        RenderExpr::Literal(Literal::Integer(1)),
                        RenderExpr::Literal(Literal::Integer(0))
                    ) | (
                        RenderExpr::Literal(Literal::Integer(0)),
                        RenderExpr::Literal(Literal::Integer(1))
                    )
                )
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Normalize UNION branch SELECT items so all branches have the same columns.
/// For denormalized node queries where from_node_properties and to_node_properties
/// might have different property sets, we need to:
/// 1. Collect all unique column aliases across all branches
/// 2. For each branch, add NULL for any missing columns
///
/// Returns normalized RenderPlans with consistent SELECT items.
pub(super) fn normalize_union_branches(
    union_plans: Vec<super::RenderPlan>,
) -> Vec<super::RenderPlan> {
    use super::{RenderPlan, SelectItem, SelectItems};
    use std::collections::BTreeSet;

    if union_plans.is_empty() {
        return union_plans;
    }

    // Collect all unique column aliases across all branches (sorted for deterministic order)
    let all_aliases: BTreeSet<String> = union_plans
        .iter()
        .flat_map(|plan| {
            plan.select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
        })
        .collect();

    println!(
        "DEBUG: normalize_union_branches - {} branches, {} total unique aliases: {:?}",
        union_plans.len(),
        all_aliases.len(),
        all_aliases
    );

    // If all branches have the same aliases, no normalization needed
    let all_same = union_plans.iter().all(|plan| {
        let branch_aliases: BTreeSet<String> = plan
            .select
            .items
            .iter()
            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
            .collect();
        branch_aliases == all_aliases
    });

    if all_same {
        crate::debug_println!("DEBUG: normalize_union_branches - all branches have same aliases, no normalization needed");
        return union_plans;
    }

    crate::debug_println!(
        "DEBUG: normalize_union_branches - branches have different aliases, normalizing..."
    );

    // Normalize each branch
    union_plans
        .into_iter()
        .map(|plan| {
            // Build a map of existing column aliases in this branch
            let existing: std::collections::HashMap<String, SelectItem> = plan
                .select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| (a.0.clone(), item.clone())))
                .collect();

            // Build normalized SELECT items in consistent order
            let normalized_items: Vec<SelectItem> = all_aliases
                .iter()
                .map(|alias| {
                    if let Some(item) = existing.get(alias) {
                        item.clone()
                    } else {
                        // Missing column - use NULL
                        SelectItem {
                            expression: RenderExpr::Literal(Literal::Null),
                            col_alias: Some(super::ColumnAlias(alias.clone())),
                        }
                    }
                })
                .collect();

            RenderPlan {
                select: SelectItems {
                    items: normalized_items,
                    distinct: plan.select.distinct,
                },
                ..plan
            }
        })
        .collect()
}

/// Find a Union node nested inside a plan
pub(super) fn find_nested_union(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::Union> {
    match plan {
        LogicalPlan::Union(union) => Some(union),
        LogicalPlan::GraphJoins(graph_joins) => find_nested_union(&graph_joins.input),
        LogicalPlan::Projection(projection) => find_nested_union(&projection.input),
        LogicalPlan::Filter(filter) => find_nested_union(&filter.input),
        LogicalPlan::GroupBy(group_by) => find_nested_union(&group_by.input),
        _ => None,
    }
}

/// Check if a GraphRel has a WithClause as its right side.
/// This indicates a "WITH ... MATCH" pattern that requires CTE-based processing.
/// The WITH clause creates a derived table that the subsequent MATCH must join against.
///
/// Note: The Projection(With) may have been transformed to Projection(Return) by analyzer passes,
/// but the structure is still identifiable by having GraphJoins/Union inside GraphRel.right
/// that contains a separate pattern (the first MATCH).
pub(super) fn has_with_clause_in_graph_rel(plan: &LogicalPlan) -> bool {
    // Helper to check if a plan contains actual WITH clause )
    fn contains_actual_with_clause(plan: &LogicalPlan) -> bool {
        match plan {
            // New WithClause type takes precedence
            LogicalPlan::WithClause(_wc) => {
                log::info!("🔍 contains_actual_with_clause: Found WithClause node");
                true
            }
            LogicalPlan::Projection(proj) => contains_actual_with_clause(&proj.input),
            LogicalPlan::GraphJoins(gj) => contains_actual_with_clause(&gj.input),
            LogicalPlan::GraphRel(gr) => {
                contains_actual_with_clause(&gr.left) || contains_actual_with_clause(&gr.right)
            }
            LogicalPlan::Filter(f) => contains_actual_with_clause(&f.input),
            LogicalPlan::GroupBy(gb) => contains_actual_with_clause(&gb.input),
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| contains_actual_with_clause(i)),
            LogicalPlan::CartesianProduct(cp) => {
                contains_actual_with_clause(&cp.left) || contains_actual_with_clause(&cp.right)
            }
            LogicalPlan::GraphNode(gn) => contains_actual_with_clause(&gn.input),
            LogicalPlan::Limit(l) => contains_actual_with_clause(&l.input),
            LogicalPlan::OrderBy(o) => contains_actual_with_clause(&o.input),
            LogicalPlan::Skip(s) => contains_actual_with_clause(&s.input),
            LogicalPlan::ViewScan(_) => false, // ViewScan is a leaf - no WITH here
            _ => false,
        }
    }

    match plan {
        // NEW: Direct WithClause at any level in the plan
        LogicalPlan::WithClause(_wc) => {
            log::info!("🔍 has_with_clause_in_graph_rel: Found WithClause at plan root");
            true
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if right side contains a Union or GraphJoins with nested patterns
            // This indicates a WITH+MATCH structure where the WITH clause output
            // was wrapped in Union (for undirected patterns) or GraphJoins
            let right_has_nested_pattern = match graph_rel.right.as_ref() {
                // NEW: Direct WithClause in GraphRel.right
                LogicalPlan::WithClause(_wc) => {
                    log::info!(
                        "🔍 has_with_clause_in_graph_rel: Found WithClause in GraphRel.right"
                    );
                    true
                }
                // Union containing GraphJoins - check if it actually contains WITH clause
                LogicalPlan::Union(union) => {
                    // Only flag as WITH pattern if there's an actual WITH clause inside
                    let has_with_inside = union
                        .inputs
                        .iter()
                        .any(|input| contains_actual_with_clause(input));
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found Union with WITH clause inside in GraphRel.right - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                // GraphJoins directly - check if it actually contains WITH clause
                LogicalPlan::GraphJoins(gj) => {
                    let has_with_inside = contains_actual_with_clause(&gj.input);
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found GraphJoins with WITH clause inside in GraphRel.right - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                _ => false,
            };

            if right_has_nested_pattern {
                return true;
            }

            // Also check left side (for incoming patterns)
            let left_has_nested_pattern = match graph_rel.left.as_ref() {
                // NEW: Direct WithClause in GraphRel.left
                LogicalPlan::WithClause(_wc) => {
                    log::info!(
                        "🔍 has_with_clause_in_graph_rel: Found WithClause in GraphRel.left"
                    );
                    true
                }
                LogicalPlan::Union(union) => {
                    let has_with_inside = union
                        .inputs
                        .iter()
                        .any(|input| contains_actual_with_clause(input));
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found Union with WITH clause inside in GraphRel.left - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                LogicalPlan::GraphJoins(gj) => {
                    let has_with_inside = contains_actual_with_clause(&gj.input);
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found GraphJoins with WITH clause inside in GraphRel.left - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                _ => false,
            };

            if left_has_nested_pattern {
                return true;
            }

            // Recursively check nested GraphRels
            has_with_clause_in_graph_rel(&graph_rel.left)
                || has_with_clause_in_graph_rel(&graph_rel.right)
        }
        LogicalPlan::Projection(proj) => has_with_clause_in_graph_rel(&proj.input),
        LogicalPlan::Filter(filter) => has_with_clause_in_graph_rel(&filter.input),
        LogicalPlan::GroupBy(group_by) => has_with_clause_in_graph_rel(&group_by.input),
        LogicalPlan::GraphJoins(graph_joins) => has_with_clause_in_graph_rel(&graph_joins.input),
        LogicalPlan::Limit(limit) => has_with_clause_in_graph_rel(&limit.input),
        LogicalPlan::OrderBy(order_by) => has_with_clause_in_graph_rel(&order_by.input),
        LogicalPlan::Skip(skip) => has_with_clause_in_graph_rel(&skip.input),
        // Check Union at top level - WITH clauses might be inside Union branches
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| has_with_clause_in_graph_rel(input)),
        // Check CartesianProduct - WITH clauses might be in either branch
        LogicalPlan::CartesianProduct(cp) => {
            has_with_clause_in_graph_rel(&cp.left) || has_with_clause_in_graph_rel(&cp.right)
        }
        _ => false,
    }
}

/// Check if a plan contains WITH+aggregation followed by MATCH pattern.
/// This happens when:
/// 1. WITH clause contains aggregations (count, sum, etc.)
/// 2. Followed by another MATCH clause
///
/// The analyzer transforms this to: GraphJoins(joins=[...], input=GroupBy(...))
/// Where the GroupBy came from the WITH aggregation.
///
/// This pattern requires CTE-based processing because:
/// - The aggregation must be computed first (materialized as a subquery)
/// - The subsequent MATCH joins against the aggregated results
pub(super) fn has_with_aggregation_pattern(plan: &LogicalPlan) -> bool {
    // The pattern we're looking for is:
    // - Outer GraphJoins with real joins (from the second MATCH)
    // - Inside that, a GroupBy with is_materialization_boundary=true
    //
    // After GraphJoinInference respects boundaries, the structure is:
    // GraphJoins(outer joins) -> Projection -> GraphRel(left: GroupBy(boundary=true), ...)

    // Step 1: Find the outer GraphJoins at the top level (unwrapping Limit/OrderBy/etc)
    fn find_top_level_graph_joins(
        plan: &LogicalPlan,
    ) -> Option<&crate::query_planner::logical_plan::GraphJoins> {
        match plan {
            LogicalPlan::GraphJoins(gj) => Some(gj),
            LogicalPlan::Limit(l) => find_top_level_graph_joins(&l.input),
            LogicalPlan::OrderBy(o) => find_top_level_graph_joins(&o.input),
            LogicalPlan::Skip(s) => find_top_level_graph_joins(&s.input),
            LogicalPlan::Projection(p) => find_top_level_graph_joins(&p.input),
            LogicalPlan::Filter(f) => find_top_level_graph_joins(&f.input),
            _ => None,
        }
    }

    // Step 2: Check if plan contains a GroupBy with is_materialization_boundary=true
    fn has_materialization_boundary_group_by(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GroupBy(gb) => {
                if gb.is_materialization_boundary {
                    return true;
                }
                // Also recurse into GroupBy's input
                has_materialization_boundary_group_by(&gb.input)
            }
            LogicalPlan::GraphRel(gr) => {
                // Check both left and right branches
                has_materialization_boundary_group_by(&gr.left)
                    || has_materialization_boundary_group_by(&gr.right)
            }
            LogicalPlan::Projection(p) => has_materialization_boundary_group_by(&p.input),
            LogicalPlan::Filter(f) => has_materialization_boundary_group_by(&f.input),
            LogicalPlan::GraphNode(gn) => has_materialization_boundary_group_by(&gn.input),
            LogicalPlan::GraphJoins(gj) => has_materialization_boundary_group_by(&gj.input),
            _ => false,
        }
    }

    // Check for the pattern: outer GraphJoins with joins + GroupBy(boundary) inside
    if let Some(graph_joins) = find_top_level_graph_joins(plan) {
        // Must have actual joins (from the outer MATCH pattern)
        if graph_joins.joins.is_empty() {
            println!(
                "DEBUG has_with_aggregation_pattern: GraphJoins has no joins, returning false"
            );
            return false;
        }

        // Check if there's a GroupBy with materialization boundary inside
        let has_boundary_groupby = has_materialization_boundary_group_by(&graph_joins.input);

        println!(
            "DEBUG has_with_aggregation_pattern: has_joins={}, has_boundary_groupby={}",
            !graph_joins.joins.is_empty(),
            has_boundary_groupby
        );

        if has_boundary_groupby {
            log::info!("🔍 has_with_aggregation_pattern: Detected GraphJoins + GroupBy(boundary=true) - WITH aggregation followed by MATCH");
            return true;
        }
    } else {
        println!(
            "DEBUG has_with_aggregation_pattern: No top-level GraphJoins found, returning false"
        );
    }

    false
}

/// Extract outer aggregation info from a plan that wraps a Union
/// Handles two possible structures:
/// 1. Projection(GroupBy(Union(...))) - older structure
/// 2. GroupBy(Projection(Union(...))) - from GroupByBuilding analyzer
pub(super) fn extract_outer_aggregation_info(
    plan: &LogicalPlan,
) -> Option<(Vec<super::SelectItem>, Vec<RenderExpr>)> {
    use super::{ColumnAlias, SelectItem};

    crate::debug_println!(
        "🔍 extract_outer_aggregation_info: plan type = {:?}",
        std::mem::discriminant(plan)
    );

    let (projection, group_by) = match plan {
        // Pattern 1: GraphJoins(Projection(GroupBy(Union)))
        LogicalPlan::GraphJoins(graph_joins) => {
            crate::debug_println!("🔍 extract_outer_aggregation_info: GraphJoins case");
            if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                if let LogicalPlan::GroupBy(gb) = proj.input.as_ref() {
                    if find_nested_union(&gb.input).is_some() {
                        (Some(proj), Some(gb))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            // Pattern 2: GraphJoins(GroupBy(Projection(Union))) - from GroupByBuilding
            } else if let LogicalPlan::GroupBy(gb) = graph_joins.input.as_ref() {
                crate::debug_println!(
                    "🔍 extract_outer_aggregation_info: GraphJoins(GroupBy) case"
                );
                if let LogicalPlan::Projection(proj) = gb.input.as_ref() {
                    crate::debug_println!("🔍 extract_outer_aggregation_info: Found Projection inside GroupBy, proj.input type = {:?}", std::mem::discriminant(proj.input.as_ref()));
                    if find_nested_union(&proj.input).is_some() {
                        crate::debug_println!("🔍 extract_outer_aggregation_info: Found Union! Returning projection and group_by");
                        (Some(proj), Some(gb))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        }
        // Pattern 1: Projection(GroupBy(Union))
        LogicalPlan::Projection(proj) => {
            crate::debug_println!("🔍 extract_outer_aggregation_info: Projection case");
            if let LogicalPlan::GroupBy(gb) = proj.input.as_ref() {
                if find_nested_union(&gb.input).is_some() {
                    (Some(proj), Some(gb))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        }
        // Pattern 2: GroupBy(Projection(Union)) - from GroupByBuilding
        // Also handles: GroupBy(GraphJoins(Projection(Union))) - after GraphJoinInference
        LogicalPlan::GroupBy(gb) => {
            crate::debug_println!(
                "🔍 extract_outer_aggregation_info: GroupBy case, input type = {:?}",
                std::mem::discriminant(gb.input.as_ref())
            );
            // Direct case: GroupBy(Projection(Union))
            if let LogicalPlan::Projection(proj) = gb.input.as_ref() {
                crate::debug_println!(
                    "🔍 extract_outer_aggregation_info: Found Projection, proj.input type = {:?}",
                    std::mem::discriminant(proj.input.as_ref())
                );
                if find_nested_union(&proj.input).is_some() {
                    crate::debug_println!(
                        "🔍 extract_outer_aggregation_info: Found Union inside Projection.input!"
                    );
                    (Some(proj), Some(gb))
                } else {
                    crate::debug_println!(
                        "🔍 extract_outer_aggregation_info: No Union found in Projection.input"
                    );
                    (None, None)
                }
            // Indirect case: GroupBy(GraphJoins(Projection(Union))) - after GraphJoinInference
            } else if let LogicalPlan::GraphJoins(graph_joins) = gb.input.as_ref() {
                crate::debug_println!("🔍 extract_outer_aggregation_info: Found GraphJoins inside GroupBy, looking for Projection...");
                if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                    crate::debug_println!("🔍 extract_outer_aggregation_info: Found Projection inside GraphJoins, proj.input type = {:?}", std::mem::discriminant(proj.input.as_ref()));
                    if find_nested_union(&proj.input).is_some() {
                        crate::debug_println!("🔍 extract_outer_aggregation_info: ✓ Found Union inside Projection.input!");
                        (Some(proj), Some(gb))
                    } else {
                        crate::debug_println!(
                            "🔍 extract_outer_aggregation_info: No Union found in Projection.input"
                        );
                        (None, None)
                    }
                } else {
                    crate::debug_println!("🔍 extract_outer_aggregation_info: GraphJoins.input is NOT Projection, it's {:?}", std::mem::discriminant(graph_joins.input.as_ref()));
                    (None, None)
                }
            } else {
                crate::debug_println!("🔍 extract_outer_aggregation_info: GroupBy.input is NOT Projection or GraphJoins");
                (None, None)
            }
        }
        _ => {
            crate::debug_println!(
                "🔍 extract_outer_aggregation_info: Unknown plan type, returning None"
            );
            (None, None)
        }
    };

    let (projection, group_by) = (projection?, group_by?);

    let has_aggregation = projection.items.iter().any(|item| {
        matches!(
            &item.expression,
            crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)
        )
    });

    if !has_aggregation {
        return None;
    }

    let outer_select: Vec<SelectItem> = projection
        .items
        .iter()
        .map(|item| {
            let expr: RenderExpr = match &item.expression {
                crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                    RenderExpr::Raw(format!("\"{}\"", alias.0))
                }
                crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(agg) => {
                    let args: Vec<RenderExpr> = agg
                        .args
                        .iter()
                        .map(|arg| match arg {
                            crate::query_planner::logical_expr::LogicalExpr::Star => {
                                RenderExpr::Raw("*".to_string())
                            }
                            crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                                RenderExpr::Raw(format!("\"{}\"", alias.0))
                            }
                            other => other
                                .clone()
                                .try_into()
                                .unwrap_or(RenderExpr::Raw("?".to_string())),
                        })
                        .collect();
                    RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: agg.name.clone(),
                        args,
                    })
                }
                other => other
                    .clone()
                    .try_into()
                    .unwrap_or(RenderExpr::Raw("?".to_string())),
            };
            SelectItem {
                expression: expr,
                col_alias: item.col_alias.as_ref().map(|a| ColumnAlias(a.0.clone())),
            }
        })
        .collect();

    let outer_group_by: Vec<RenderExpr> = group_by
        .expressions
        .iter()
        .map(|expr| match expr {
            crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                RenderExpr::Raw(format!("\"{}\"", alias.0))
            }
            other => other
                .clone()
                .try_into()
                .unwrap_or(RenderExpr::Raw("?".to_string())),
        })
        .collect();

    Some((outer_select, outer_group_by))
}

/// Check if joining_on references a UNION CTE
pub(super) fn references_union_cte_in_join(
    joining_on: &[OperatorApplication],
    cte_name: &str,
) -> bool {
    for op_app in joining_on {
        if op_app.operands.len() >= 2
            && (references_union_cte_in_operand(&op_app.operands[0], cte_name)
                || references_union_cte_in_operand(&op_app.operands[1], cte_name))
        {
            return true;
        }
    }
    false
}

fn references_union_cte_in_operand(operand: &RenderExpr, cte_name: &str) -> bool {
    match operand {
        RenderExpr::PropertyAccessExp(prop_access) => {
            prop_access.column.raw() == "from_id" || prop_access.column.raw() == "to_id"
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            references_union_cte_in_join(std::slice::from_ref(op_app), cte_name)
        }
        _ => false,
    }
}

/// Update JOIN expressions to use standardized column names for UNION CTEs
pub(super) fn update_join_expression_for_union_cte(
    op_app: &mut OperatorApplication,
    table_alias: &str,
) {
    for operand in op_app.operands.iter_mut() {
        update_operand_for_union_cte(operand, table_alias);
    }
}

fn update_operand_for_union_cte(operand: &mut RenderExpr, table_alias: &str) {
    match operand {
        RenderExpr::Column(col) => {
            if col.raw() == "from_id" {
                *operand =
                    RenderExpr::Column(Column(PropertyValue::Column("from_node_id".to_string())));
            } else if col.raw() == "to_id" {
                *operand =
                    RenderExpr::Column(Column(PropertyValue::Column("to_node_id".to_string())));
            }
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            if prop_access.column.raw() == "from_id" {
                prop_access.column = PropertyValue::Column("from_node_id".to_string());
            } else if prop_access.column.raw() == "to_id" {
                prop_access.column = PropertyValue::Column("to_node_id".to_string());
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op_app) => {
            update_join_expression_for_union_cte(inner_op_app, table_alias);
        }
        _ => {}
    }
}

// ============================================================================
// Predicate Analysis Helpers
// These functions help analyze and manipulate logical expressions (predicates)
// ============================================================================

use crate::query_planner::logical_expr::{
    LogicalExpr, Operator as LogicalOperator, OperatorApplication as LogicalOpApp,
};

/// Collect all table aliases referenced in a LogicalExpr.
/// Used to determine which aliases a predicate depends on.
pub(super) fn collect_aliases_from_logical_expr(expr: &LogicalExpr, aliases: &mut HashSet<String>) {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_logical_expr(operand, aliases);
            }
        }
        LogicalExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_aliases_from_logical_expr(arg, aliases);
            }
        }
        LogicalExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_aliases_from_logical_expr(arg, aliases);
            }
        }
        LogicalExpr::Case(case) => {
            if let Some(expr) = &case.expr {
                collect_aliases_from_logical_expr(expr, aliases);
            }
            for (cond, result) in &case.when_then {
                collect_aliases_from_logical_expr(cond, aliases);
                collect_aliases_from_logical_expr(result, aliases);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_aliases_from_logical_expr(else_expr, aliases);
            }
        }
        LogicalExpr::List(items) => {
            for item in items {
                collect_aliases_from_logical_expr(item, aliases);
            }
        }
        _ => {}
    }
}

/// Check if a LogicalExpr references ONLY the specified alias.
/// Returns true if the expression contains exactly one alias and it matches `alias`.
pub(super) fn references_only_alias_logical(expr: &LogicalExpr, alias: &str) -> bool {
    let mut aliases = HashSet::new();
    collect_aliases_from_logical_expr(expr, &mut aliases);
    aliases.len() == 1 && aliases.contains(alias)
}

/// Split an AND-connected LogicalExpr into individual predicates.
/// For example: `a AND b AND c` becomes `[a, b, c]`.
pub(super) fn split_and_predicates_logical(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::OperatorApplicationExp(op) if matches!(op.operator, LogicalOperator::And) => {
            let mut result = Vec::new();
            for operand in &op.operands {
                result.extend(split_and_predicates_logical(operand));
            }
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Combine multiple LogicalExpr predicates with AND.
/// Returns None if the input is empty.
pub(super) fn combine_predicates_with_and_logical(
    predicates: Vec<LogicalExpr>,
) -> Option<LogicalExpr> {
    if predicates.is_empty() {
        None
    } else if predicates.len() == 1 {
        Some(predicates.into_iter().next().unwrap())
    } else {
        Some(LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::And,
            operands: predicates,
        }))
    }
}

/// Extract predicates from a where_predicate that reference ONLY a specific alias.
/// Returns (predicates_for_alias, remaining_predicates).
/// This is used to move optional-alias predicates into LEFT JOIN pre_filter.
pub(super) fn extract_predicates_for_alias_logical(
    where_predicate: &Option<LogicalExpr>,
    target_alias: &str,
) -> (Option<RenderExpr>, Option<LogicalExpr>) {
    let predicate = match where_predicate {
        Some(p) => p,
        None => return (None, None),
    };

    let all_predicates = split_and_predicates_logical(predicate);
    let mut for_alias = Vec::new();
    let mut remaining = Vec::new();

    for pred in all_predicates {
        if references_only_alias_logical(&pred, target_alias) {
            for_alias.push(pred);
        } else {
            remaining.push(pred);
        }
    }

    // Convert for_alias predicates to RenderExpr
    let alias_filter = if for_alias.is_empty() {
        None
    } else {
        let combined = combine_predicates_with_and_logical(for_alias).unwrap();
        RenderExpr::try_from(combined).ok()
    };

    (alias_filter, combine_predicates_with_and_logical(remaining))
}

// ============================================================================
// JOIN Extraction Helpers
// These functions assist with extracting JOIN clauses from the logical plan
// ============================================================================

/// Extract schema filter from a LogicalPlan for LEFT JOIN pre_filter.
/// This ensures schema filters are applied BEFORE the LEFT JOIN (correct semantics).
pub(super) fn get_schema_filter_for_node(plan: &LogicalPlan, alias: &str) -> Option<RenderExpr> {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                if let Some(ref sf) = vs.schema_filter {
                    if let Ok(sql) = sf.to_sql(alias) {
                        return Some(RenderExpr::Raw(sql));
                    }
                }
            }
            None
        }
        LogicalPlan::ViewScan(vs) => {
            if let Some(ref sf) = vs.schema_filter {
                if let Ok(sql) = sf.to_sql(alias) {
                    return Some(RenderExpr::Raw(sql));
                }
            }
            None
        }
        _ => None,
    }
}

/// Generate polymorphic edge type filter for JOIN clauses.
/// For polymorphic edges, adds: r.type_column IN ('TYPE1', 'TYPE2') AND r.from_label = 'NodeType' AND r.to_label = 'NodeType'
/// For single type: r.type_column = 'EDGE_TYPE'
pub(super) fn get_polymorphic_edge_filter_for_join(
    center: &LogicalPlan,
    alias: &str,
    rel_types: &[String],
    from_label: &Option<String>,
    to_label: &Option<String>,
) -> Option<RenderExpr> {
    // Extract ViewScan from center (might be wrapped in GraphNode)
    let view_scan = match center {
        LogicalPlan::ViewScan(vs) => Some(vs.as_ref()),
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                Some(vs.as_ref())
            } else {
                None
            }
        }
        _ => None,
    }?;

    // Check if this is a polymorphic edge (has type_column, from_label_column, or to_label_column)
    let has_polymorphic_fields = view_scan.type_column.is_some()
        || view_scan.from_label_column.is_some()
        || view_scan.to_label_column.is_some();

    if !has_polymorphic_fields {
        return None;
    }

    log::debug!(
        "Generating polymorphic edge filter for alias='{}', rel_types={:?}, type_col={:?}, from_label_col={:?}, to_label_col={:?}",
        alias, rel_types, view_scan.type_column, view_scan.from_label_column, view_scan.to_label_column
    );

    let mut filters = Vec::new();

    // Filter 1: type_column = 'EDGE_TYPE' (single) OR type_column IN ('TYPE1', 'TYPE2') (multiple)
    if let Some(type_col) = &view_scan.type_column {
        if rel_types.len() == 1 {
            filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(type_col.clone()),
                    }),
                    RenderExpr::Literal(Literal::String(rel_types[0].clone())),
                ],
            }));
        } else if rel_types.len() > 1 {
            let type_list: Vec<RenderExpr> = rel_types
                .iter()
                .map(|t| RenderExpr::Literal(Literal::String(t.clone())))
                .collect();
            filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::In,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(type_col.clone()),
                    }),
                    RenderExpr::List(type_list),
                ],
            }));
        }
    }

    // Filter 2: from_label_column = 'FromNodeType' (if label provided and not $any)
    if let Some(from_label_col) = &view_scan.from_label_column {
        if let Some(from_label_str) = from_label {
            if !from_label_str.is_empty() && from_label_str != "$any" {
                filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(alias.to_string()),
                            column: PropertyValue::Column(from_label_col.clone()),
                        }),
                        RenderExpr::Literal(Literal::String(from_label_str.clone())),
                    ],
                }));
            }
        }
    }

    // Filter 3: to_label_column = 'ToNodeType' (if label provided and not $any)
    if let Some(to_label_col) = &view_scan.to_label_column {
        if let Some(to_label_str) = to_label {
            if !to_label_str.is_empty() && to_label_str != "$any" {
                filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(alias.to_string()),
                            column: PropertyValue::Column(to_label_col.clone()),
                        }),
                        RenderExpr::Literal(Literal::String(to_label_str.clone())),
                    ],
                }));
            }
        }
    }

    // Combine filters with AND
    combine_render_exprs_with_and(filters)
}

/// Collect all WHERE predicates from GraphRel nodes in the plan tree.
/// For optional patterns, filters out predicates that reference ONLY optional aliases
/// (those are moved to pre_filter for correct LEFT JOIN semantics).
/// For VLP patterns (variable_length is Some), predicates are already handled in the CTE,
/// so they are skipped here to avoid duplication in the outer query.
pub(super) fn collect_graphrel_predicates(plan: &LogicalPlan) -> Vec<RenderExpr> {
    let mut predicates = Vec::new();
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // 🔧 VLP FIX: Skip predicates from VLP GraphRel nodes - they're already in the CTE
            // This prevents duplicate filters like "WHERE u1.user_id = 1" appearing in both
            // the CTE and the outer query when combining VLP with additional relationships.
            if gr.variable_length.is_some() {
                log::debug!(
                    "collect_graphrel_predicates: Skipping VLP GraphRel '{}' predicates (already in CTE)",
                    gr.alias
                );
                // Still recurse into children to collect non-VLP predicates
                predicates.extend(collect_graphrel_predicates(&gr.left));
                predicates.extend(collect_graphrel_predicates(&gr.center));
                predicates.extend(collect_graphrel_predicates(&gr.right));
                return predicates;
            }

            // Add this GraphRel's predicate, but filter out optional-only predicates
            if let Some(ref pred) = gr.where_predicate {
                let is_optional = gr.is_optional.unwrap_or(false);

                if is_optional {
                    // For OPTIONAL MATCH patterns, determine anchor vs optional aliases
                    let anchor_alias = gr.anchor_connection.as_ref();
                    let optional_alias = if anchor_alias == Some(&gr.left_connection) {
                        Some(&gr.right_connection)
                    } else if anchor_alias == Some(&gr.right_connection) {
                        Some(&gr.left_connection)
                    } else {
                        None
                    };

                    if let (Some(_anchor), Some(optional)) = (anchor_alias, optional_alias) {
                        let all_preds = split_and_predicates_logical(pred);
                        for p in all_preds {
                            let refs_only_rel = references_only_alias_logical(&p, &gr.alias);
                            let refs_only_optional = references_only_alias_logical(&p, optional);

                            // Keep if it references anchor or multiple aliases
                            // Filter out if it references ONLY rel or ONLY optional node
                            if !refs_only_rel && !refs_only_optional {
                                if let Ok(render_expr) = RenderExpr::try_from(p) {
                                    predicates.push(render_expr);
                                }
                            }
                        }
                    } else {
                        // No anchor determined - keep all predicates (conservative)
                        if let Ok(render_expr) = RenderExpr::try_from(pred.clone()) {
                            predicates.push(render_expr);
                        }
                    }
                } else {
                    // Non-optional: include all predicates
                    if let Ok(render_expr) = RenderExpr::try_from(pred.clone()) {
                        predicates.push(render_expr);
                    }
                }
            }
            // Recursively collect from children
            predicates.extend(collect_graphrel_predicates(&gr.left));
            predicates.extend(collect_graphrel_predicates(&gr.center));
            predicates.extend(collect_graphrel_predicates(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            predicates.extend(collect_graphrel_predicates(&gn.input));
        }
        LogicalPlan::ViewScan(_scan) => {
            // ViewScan.view_filter should be empty after CleanupViewScanFilters optimizer
        }
        _ => {}
    }
    predicates
}

/// Collect schema filters from all ViewScans in the plan tree.
/// These are filters defined in the YAML schema configuration.
pub(super) fn collect_schema_filters(
    plan: &LogicalPlan,
    alias_hint: Option<&str>,
) -> Vec<RenderExpr> {
    let mut filters = Vec::new();
    match plan {
        LogicalPlan::ViewScan(scan) => {
            if let Some(ref schema_filter) = scan.schema_filter {
                let table_alias = alias_hint.unwrap_or(VLP_CTE_FROM_ALIAS);
                if let Ok(sql) = schema_filter.to_sql(table_alias) {
                    log::debug!(
                        "Collected schema filter for table '{}' with alias '{}': {}",
                        scan.source_table,
                        table_alias,
                        sql
                    );
                    filters.push(RenderExpr::Raw(sql));
                }
            }
        }
        LogicalPlan::GraphRel(gr) => {
            filters.extend(collect_schema_filters(&gr.left, Some(&gr.left_connection)));
            filters.extend(collect_schema_filters(&gr.center, Some(&gr.alias)));
            filters.extend(collect_schema_filters(
                &gr.right,
                Some(&gr.right_connection),
            ));
        }
        LogicalPlan::GraphNode(gn) => {
            filters.extend(collect_schema_filters(&gn.input, Some(&gn.alias)));
        }
        _ => {}
    }
    filters
}

/// Combine multiple RenderExpr filters with AND operator.
/// Returns None if empty, the single expr if one, or AND-combined if multiple.
pub(super) fn combine_render_exprs_with_and(filters: Vec<RenderExpr>) -> Option<RenderExpr> {
    match filters.len() {
        0 => None,
        1 => Some(filters.into_iter().next().unwrap()),
        _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        })),
    }
}

/// Combine multiple optional RenderExpr filters with AND operator.
/// Flattens the options first, then combines non-None values.
/// Returns None if all inputs are None.
pub(super) fn combine_optional_filters_with_and(
    filters: Vec<Option<RenderExpr>>,
) -> Option<RenderExpr> {
    let active: Vec<RenderExpr> = filters.into_iter().flatten().collect();
    combine_render_exprs_with_and(active)
}

/// Sort JOINs by dependency order to ensure referenced tables are defined before use.
///
/// For example, if JOIN A references table B in its ON clause, then B must appear
/// before A in the JOIN list. This is critical for OPTIONAL VLP queries where:
/// `LEFT JOIN vlp_cte AS vlp1 ON vlp1.start_id = message.id`
/// requires that `message` be defined in an earlier JOIN.
///
/// # Arguments
/// * `joins` - Vector of JOINs to sort
/// * `from_table` - Optional FROM table (already defined, can be referenced by JOINs)
///
/// # Returns
/// Sorted vector of JOINs in dependency order
pub(super) fn sort_joins_by_dependency(
    mut joins: Vec<super::Join>,
    from_table: Option<&super::FromTable>,
) -> Vec<super::Join> {
    use std::collections::{HashMap, HashSet};

    println!(
        "🔍 DEBUG sort_joins_by_dependency: Sorting {} JOINs by dependency",
        joins.len()
    );

    // Build a set of available aliases (FROM table + already processed JOINs)
    let mut available: HashSet<String> = HashSet::new();

    // Add FROM table alias if present
    if let Some(from) = from_table {
        if let Some(table_ref) = &from.table {
            if let Some(alias) = &table_ref.alias {
                available.insert(alias.clone());
                println!("  DEBUG FROM alias: {}", alias);
            } else {
                // Use table name as implicit alias
                available.insert(table_ref.name.clone());
                println!("  DEBUG FROM table (implicit alias): {}", table_ref.name);
            }
        }
    }

    // Build dependency map: JOIN -> set of aliases it references in ON clause
    let mut dependencies: HashMap<usize, HashSet<String>> = HashMap::new();

    for (idx, join) in joins.iter().enumerate() {
        let mut refs = HashSet::new();

        // Extract all aliases referenced in joining_on conditions
        for condition in &join.joining_on {
            extract_referenced_aliases_from_op(condition, &mut refs);
        }

        // Remove self-reference (the JOIN's own alias)
        refs.remove(&join.table_alias);

        println!(
            "  DEBUG JOIN[{}] {} AS {} depends on: {:?}",
            idx, join.table_name, join.table_alias, refs
        );

        dependencies.insert(idx, refs);
    }

    // Topological sort: repeatedly find JOINs whose dependencies are all available
    let mut sorted = Vec::new();
    let mut remaining: Vec<usize> = (0..joins.len()).collect();
    let mut max_iterations = joins.len() * 2; // Prevent infinite loops

    println!(
        "  DEBUG Starting topological sort with {} JOINs",
        remaining.len()
    );

    while !remaining.is_empty() && max_iterations > 0 {
        max_iterations -= 1;

        // Find next JOIN that can be added (all dependencies available)
        let ready_idx = remaining.iter().position(|&idx| {
            dependencies
                .get(&idx)
                .map(|deps| deps.iter().all(|dep| available.contains(dep)))
                .unwrap_or(true)
        });

        if let Some(pos) = ready_idx {
            let idx = remaining.remove(pos);

            // Add this JOIN's alias to available set
            available.insert(joins[idx].table_alias.clone());
            println!(
                "  DEBUG Added JOIN[{}] {} AS {} to sorted list (available now: {:?})",
                idx, joins[idx].table_name, joins[idx].table_alias, available
            );

            sorted.push(idx);
        } else {
            // No progress possible - break to avoid infinite loop
            // This can happen with circular dependencies (shouldn't occur in practice)
            println!(
                "WARNING: Could not fully sort JOINs by dependency - {} remaining with circular dependencies",
                remaining.len()
            );
            println!(
                "  DEBUG Remaining JOINs: {:?}",
                remaining
                    .iter()
                    .map(|&idx| format!("{} AS {}", joins[idx].table_name, joins[idx].table_alias))
                    .collect::<Vec<_>>()
            );
            println!("  DEBUG Available aliases: {:?}", available);
            for &idx in &remaining {
                if let Some(deps) = dependencies.get(&idx) {
                    println!(
                        "    JOIN[{}] {} AS {} needs: {:?}",
                        idx, joins[idx].table_name, joins[idx].table_alias, deps
                    );
                }
            }
            break;
        }
    }

    // Add any remaining JOINs that couldn't be sorted
    for idx in remaining {
        sorted.push(idx);
    }

    println!(
        "  DEBUG Sorted order: {:?}",
        sorted
            .iter()
            .map(|&idx| format!("{} AS {}", joins[idx].table_name, joins[idx].table_alias))
            .collect::<Vec<_>>()
    );

    // Rebuild JOIN vector in sorted order
    let original_joins = joins.clone();
    joins.clear();
    for idx in sorted {
        joins.push(original_joins[idx].clone());
    }

    joins
}

/// Extract all table aliases referenced in an OperatorApplication's operands
fn extract_referenced_aliases_from_op(op: &OperatorApplication, refs: &mut HashSet<String>) {
    for operand in &op.operands {
        extract_referenced_aliases_from_expr(operand, refs);
    }
}

/// Extract all table aliases referenced in a RenderExpr
fn extract_referenced_aliases_from_expr(expr: &RenderExpr, refs: &mut HashSet<String>) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            refs.insert(prop.table_alias.0.clone());
        }
        RenderExpr::OperatorApplicationExp(op) => {
            extract_referenced_aliases_from_op(op, refs);
        }
        RenderExpr::ScalarFnCall(call) => {
            for arg in &call.args {
                extract_referenced_aliases_from_expr(arg, refs);
            }
        }
        RenderExpr::AggregateFnCall(call) => {
            for arg in &call.args {
                extract_referenced_aliases_from_expr(arg, refs);
            }
        }
        RenderExpr::TableAlias(alias) => {
            refs.insert(alias.0.clone());
        }
        // Literals, Star, CastExpr, etc. don't reference aliases
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{
        ColumnAlias, Literal, Operator, OperatorApplication, TableAlias,
    };

    /// Test for TODO-8: rewrite_with_aliases_to_cte should rewrite TableAlias references
    /// that are in the with_aliases set to CTE references.
    #[test]
    fn test_rewrite_with_aliases_to_cte_basic() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // A simple TableAlias reference should be rewritten
        let expr = RenderExpr::TableAlias(TableAlias("follows".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(
            from_with,
            "Expression should be recognized as coming from WITH"
        );

        // Should be rewritten to grouped_data.follows
        match rewritten {
            RenderExpr::PropertyAccessExp(prop) => {
                assert_eq!(prop.table_alias.0, "grouped_data");
                assert_eq!(prop.column.raw(), "follows");
            }
            _ => panic!("Expected PropertyAccessExp, got {:?}", rewritten),
        }
    }

    /// Test that non-WITH aliases are NOT rewritten
    #[test]
    fn test_rewrite_with_aliases_to_cte_non_with_alias() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // A TableAlias NOT in with_aliases should NOT be rewritten
        let expr = RenderExpr::TableAlias(TableAlias("other_alias".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(!from_with, "Expression should NOT be from WITH");

        // Should remain unchanged
        match rewritten {
            RenderExpr::TableAlias(alias) => {
                assert_eq!(alias.0, "other_alias");
            }
            _ => panic!("Expected unchanged TableAlias, got {:?}", rewritten),
        }
    }

    /// Test that rewrite_with_aliases_to_cte handles aggregates with WITH aliases
    #[test]
    fn test_rewrite_with_aliases_to_cte_aggregate() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // AVG(follows) should become AVG(grouped_data.follows)
        let expr = RenderExpr::AggregateFnCall(AggregateFnCall {
            name: "AVG".to_string(),
            args: vec![RenderExpr::TableAlias(TableAlias("follows".to_string()))],
        });

        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(from_with, "Aggregate argument should be from WITH");

        // Should be AVG(grouped_data.follows)
        match rewritten {
            RenderExpr::AggregateFnCall(agg) => {
                assert_eq!(agg.name, "AVG");
                assert_eq!(agg.args.len(), 1);
                match &agg.args[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "grouped_data");
                        assert_eq!(prop.column.raw(), "follows");
                    }
                    _ => panic!(
                        "Expected PropertyAccessExp inside aggregate, got {:?}",
                        agg.args[0]
                    ),
                }
            }
            _ => panic!("Expected AggregateFnCall, got {:?}", rewritten),
        }
    }

    /// Test that nested expressions are rewritten correctly
    #[test]
    fn test_rewrite_with_aliases_to_cte_nested_operator() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("a".to_string());
        with_aliases.insert("b".to_string());

        // a + b should become cte.a + cte.b
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::TableAlias(TableAlias("a".to_string())),
                RenderExpr::TableAlias(TableAlias("b".to_string())),
            ],
        });

        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        assert!(from_with, "Both operands are from WITH");

        match rewritten {
            RenderExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operands.len(), 2);
                // Check first operand
                match &op.operands[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "a");
                    }
                    _ => panic!("Expected PropertyAccessExp for first operand"),
                }
                // Check second operand
                match &op.operands[1] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "b");
                    }
                    _ => panic!("Expected PropertyAccessExp for second operand"),
                }
            }
            _ => panic!("Expected OperatorApplicationExp, got {:?}", rewritten),
        }
    }

    /// Test that mixed expressions (WITH alias + non-WITH alias) are handled correctly
    #[test]
    fn test_rewrite_with_aliases_to_cte_mixed() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("from_with".to_string());

        // from_with + not_from_with should partially rewrite
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::TableAlias(TableAlias("from_with".to_string())),
                RenderExpr::TableAlias(TableAlias("not_from_with".to_string())),
            ],
        });

        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        // from_with should be false because not all operands are from WITH
        assert!(!from_with, "Mixed expression should not be fully from WITH");

        match rewritten {
            RenderExpr::OperatorApplicationExp(op) => {
                // First operand should be rewritten
                match &op.operands[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "from_with");
                    }
                    _ => panic!("Expected first operand to be rewritten to PropertyAccessExp"),
                }
                // Second operand should NOT be rewritten
                match &op.operands[1] {
                    RenderExpr::TableAlias(alias) => {
                        assert_eq!(alias.0, "not_from_with");
                    }
                    _ => panic!("Expected second operand to remain as TableAlias"),
                }
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }

    /// Test that ColumnAlias references are also rewritten (not just TableAlias)
    #[test]
    fn test_rewrite_with_aliases_to_cte_column_alias() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("my_alias".to_string());

        let expr = RenderExpr::ColumnAlias(ColumnAlias("my_alias".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(
            from_with,
            "ColumnAlias should also be recognized as from WITH"
        );

        match rewritten {
            RenderExpr::PropertyAccessExp(prop) => {
                assert_eq!(prop.table_alias.0, "grouped_data");
                assert_eq!(prop.column.raw(), "my_alias");
            }
            _ => panic!("Expected PropertyAccessExp, got {:?}", rewritten),
        }
    }

    /// Test that literals are not rewritten and don't claim to be from WITH
    #[test]
    fn test_rewrite_with_aliases_to_cte_literal() {
        let with_aliases = HashSet::new();

        let expr = RenderExpr::Literal(Literal::Integer(42));
        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        assert!(!from_with, "Literal should not be from WITH");

        match rewritten {
            RenderExpr::Literal(Literal::Integer(n)) => assert_eq!(n, 42),
            _ => panic!("Expected unchanged Literal"),
        }
    }

    // ==========================================================================
    // Tests for predicate analysis helpers
    // ==========================================================================

    use crate::graph_catalog::expression_parser::PropertyValue as LogicalPropertyValue;
    use crate::query_planner::logical_expr::{
        Literal as LogicalLiteral, LogicalExpr, Operator as LogicalOperator,
        OperatorApplication as LogicalOpApp, PropertyAccess as LogicalPropertyAccess,
        TableAlias as LogicalTableAlias,
    };

    /// Test collect_aliases_from_logical_expr with simple property access
    #[test]
    fn test_collect_aliases_from_logical_expr_property() {
        let expr = LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
            table_alias: LogicalTableAlias("user".to_string()),
            column: LogicalPropertyValue::Column("name".to_string()),
        });

        let mut aliases = HashSet::new();
        collect_aliases_from_logical_expr(&expr, &mut aliases);

        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains("user"));
    }

    /// Test collect_aliases_from_logical_expr with operator containing multiple aliases
    #[test]
    fn test_collect_aliases_from_logical_expr_operator() {
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("a".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("b".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
            ],
        });

        let mut aliases = HashSet::new();
        collect_aliases_from_logical_expr(&expr, &mut aliases);

        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains("a"));
        assert!(aliases.contains("b"));
    }

    /// Test references_only_alias_logical - returns true when only one alias
    #[test]
    fn test_references_only_alias_logical_single() {
        let expr = LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
            table_alias: LogicalTableAlias("user".to_string()),
            column: LogicalPropertyValue::Column("name".to_string()),
        });

        assert!(references_only_alias_logical(&expr, "user"));
        assert!(!references_only_alias_logical(&expr, "other"));
    }

    /// Test references_only_alias_logical - returns false when multiple aliases
    #[test]
    fn test_references_only_alias_logical_multiple() {
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("a".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("b".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
            ],
        });

        assert!(!references_only_alias_logical(&expr, "a"));
        assert!(!references_only_alias_logical(&expr, "b"));
    }

    /// Test split_and_predicates_logical
    #[test]
    fn test_split_and_predicates_logical() {
        // Create: a.x = 1 AND b.y = 2
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::And,
            operands: vec![
                LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: LogicalOperator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                            table_alias: LogicalTableAlias("a".to_string()),
                            column: LogicalPropertyValue::Column("x".to_string()),
                        }),
                        LogicalExpr::Literal(LogicalLiteral::Integer(1)),
                    ],
                }),
                LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: LogicalOperator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                            table_alias: LogicalTableAlias("b".to_string()),
                            column: LogicalPropertyValue::Column("y".to_string()),
                        }),
                        LogicalExpr::Literal(LogicalLiteral::Integer(2)),
                    ],
                }),
            ],
        });

        let predicates = split_and_predicates_logical(&expr);
        assert_eq!(predicates.len(), 2);
    }

    /// Test combine_predicates_with_and_logical
    #[test]
    fn test_combine_predicates_with_and_logical() {
        // Empty list
        assert!(combine_predicates_with_and_logical(vec![]).is_none());

        // Single predicate
        let single = LogicalExpr::Literal(LogicalLiteral::Boolean(true));
        let combined = combine_predicates_with_and_logical(vec![single.clone()]);
        assert_eq!(combined, Some(single));

        // Multiple predicates
        let p1 = LogicalExpr::Literal(LogicalLiteral::Boolean(true));
        let p2 = LogicalExpr::Literal(LogicalLiteral::Boolean(false));
        let combined = combine_predicates_with_and_logical(vec![p1.clone(), p2.clone()]);

        match combined {
            Some(LogicalExpr::OperatorApplicationExp(op)) => {
                assert!(matches!(op.operator, LogicalOperator::And));
                assert_eq!(op.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplicationExp with AND"),
        }
    }
}

/// Recursively find GraphRel in a logical plan tree
/// Used to detect multi-type VLP patterns for correct table alias resolution
pub(super) fn get_graph_rel_from_plan(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::GraphRel> {
    use crate::query_planner::logical_plan::LogicalPlan;

    match plan {
        LogicalPlan::GraphRel(rel) => Some(rel),
        LogicalPlan::Filter(filter) => get_graph_rel_from_plan(&filter.input),
        LogicalPlan::Projection(proj) => get_graph_rel_from_plan(&proj.input),
        LogicalPlan::OrderBy(order) => get_graph_rel_from_plan(&order.input),
        LogicalPlan::Limit(limit) => get_graph_rel_from_plan(&limit.input),
        LogicalPlan::Skip(skip) => get_graph_rel_from_plan(&skip.input),
        LogicalPlan::GroupBy(group) => get_graph_rel_from_plan(&group.input),
        LogicalPlan::WithClause(with_clause) => get_graph_rel_from_plan(&with_clause.input),
        LogicalPlan::GraphJoins(joins) => get_graph_rel_from_plan(&joins.input),
        _ => None,
    }
}

/// Convert path UNION branches to JSON format for consistent schema
///
/// For path queries like `MATCH p=()-->() RETURN p`, each branch may have different
/// node/relationship types with different property counts. Convert to fixed schema:
/// - `p`: path tuple (unchanged)
/// - `_start_properties`: JSON with start node properties
/// - `_end_properties`: JSON with end node properties
/// - `_rel_properties`: JSON with relationship properties
pub(super) fn convert_path_branches_to_json(
    union_plans: Vec<super::RenderPlan>,
) -> Vec<super::RenderPlan> {
    use super::render_expr::{Literal, RenderExpr, ScalarFnCall};
    use super::{ColumnAlias, RenderPlan, SelectItem, SelectItems};

    log::warn!(
        "🔧 convert_path_branches_to_json: Processing {} branches",
        union_plans.len()
    );

    union_plans
        .into_iter()
        .enumerate()
        .map(|(branch_idx, plan)| {
            // First, find the path tuple and extract aliases from it
            let mut path_item = None;
            let mut start_alias = String::new();
            let mut end_alias = String::new();
            let mut rel_alias = String::new();

            // Find path tuple and extract aliases
            for item in &plan.select.items {
                if matches!(&item.expression, RenderExpr::ScalarFnCall(fn_call) if fn_call.name == "tuple") {
                    if let RenderExpr::ScalarFnCall(fn_call) = &item.expression {
                        // tuple('fixed_path', start_alias, end_alias, rel_alias)
                        // Arguments are: [Literal("fixed_path"), Literal(start), Literal(end), Literal(rel)]
                        if fn_call.args.len() >= 4 {
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[1] {
                                start_alias = s.clone();
                            }
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[2] {
                                end_alias = s.clone();
                            }
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[3] {
                                rel_alias = s.clone();
                            }
                        }
                    }
                }
            }

            log::warn!("  Branch {}: start='{}', end='{}', rel='{}'",
                      branch_idx, start_alias, end_alias, rel_alias);

            let mut start_items = Vec::new();
            let mut end_items = Vec::new();
            let mut rel_items = Vec::new();

            // Now group items by their table alias prefix
            for item in plan.select.items {
                if let Some(alias) = &item.col_alias {
                    let alias_str = &alias.0;

                    // Path tuple: ScalarFnCall to tuple() function
                    if matches!(&item.expression, RenderExpr::ScalarFnCall(fn_call) if fn_call.name == "tuple") {
                        path_item = Some(item);
                    }
                    // Check if alias starts with start node table alias
                    else if !start_alias.is_empty() && alias_str.starts_with(&format!("{}.", start_alias)) {
                        start_items.push(item);
                    }
                    // Check if alias starts with end node table alias
                    else if !end_alias.is_empty() && alias_str.starts_with(&format!("{}.", end_alias)) {
                        end_items.push(item);
                    }
                    // Check if alias starts with relationship table alias
                    else if !rel_alias.is_empty() && alias_str.starts_with(&format!("{}.", rel_alias)) {
                        rel_items.push(item);
                    }
                }
            }

            log::warn!("  Branch {}: found {} start, {} end, {} rel items",
                      branch_idx, start_items.len(), end_items.len(), rel_items.len());

            let mut new_items = Vec::new();

            // 1. Keep path tuple as-is
            if let Some(p) = path_item {
                new_items.push(p);
            }

            // 2. Convert start node properties to JSON (prefix: _s_)
            if !start_items.is_empty() {
                let json_expr = build_format_row_json(&start_items, "_s_");
                new_items.push(SelectItem {
                    expression: json_expr,
                    col_alias: Some(ColumnAlias("_start_properties".to_string())),
                });
            }

            // 3. Convert end node properties to JSON (prefix: _e_)
            if !end_items.is_empty() {
                let json_expr = build_format_row_json(&end_items, "_e_");
                new_items.push(SelectItem {
                    expression: json_expr,
                    col_alias: Some(ColumnAlias("_end_properties".to_string())),
                });
            }

            // 4. Convert relationship properties to JSON (prefix: _r_) or empty object if none
            if !rel_items.is_empty() {
                let json_expr = build_format_row_json(&rel_items, "_r_");
                new_items.push(SelectItem {
                    expression: json_expr,
                    col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                });
            } else {
                // No relationship properties (denormalized) - empty JSON object
                new_items.push(SelectItem {
                    expression: RenderExpr::Literal(Literal::String("{}".to_string())),
                    col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                });
            }

            RenderPlan {
                select: SelectItems {
                    items: new_items,
                    distinct: plan.select.distinct,
                },
                ..plan
            }
        })
        .collect()
}

/// Helper to build JSON object from select items using formatRowNoNewline('JSONEachRow', ...)
/// Uses column aliases (AS prefix+clean_name) so JSON keys have unique prefixes
/// to avoid ClickHouse alias collision when same property names appear in both nodes.
/// The prefix (_s_, _e_, _r_) is stripped in the Bolt transformer.
fn build_format_row_json(items: &[super::SelectItem], prefix: &str) -> RenderExpr {
    use super::render_expr::{Literal, RenderExpr};
    use crate::graph_catalog::expression_parser::PropertyValue;

    if items.is_empty() {
        return RenderExpr::Literal(Literal::String("{}".to_string()));
    }

    // Build aliased column expressions: t1_0.city AS _s_city, t1_0.full_name AS _s_name, ...
    // Prefix (_s_, _e_, _r_) ensures unique aliases even when start/end have same properties
    let mut aliased_cols = Vec::new();
    for item in items {
        if let Some(alias) = &item.col_alias {
            let alias_str = &alias.0;
            // Extract clean property name (after the dot, if any)
            let clean_name = if let Some(dot_pos) = alias_str.find('.') {
                &alias_str[dot_pos + 1..]
            } else {
                alias_str.as_str()
            };

            // Get the column expression
            if let RenderExpr::PropertyAccessExp(prop_access) = &item.expression {
                if let PropertyValue::Column(col_name) = &prop_access.column {
                    let col_expr = format!("{}.{}", prop_access.table_alias.0, col_name);
                    // Use prefixed alias to avoid collision (e.g., _s_city, _e_city)
                    aliased_cols.push(format!("{} AS {}{}", col_expr, prefix, clean_name));
                }
            }
        }
    }

    if aliased_cols.is_empty() {
        return RenderExpr::Literal(Literal::String("{}".to_string()));
    }

    // formatRowNoNewline('JSONEachRow', t1.col AS _s_city, t1.name AS _s_name, ...)
    // Prefixed aliases ensure no collision in ClickHouse scope
    let format_expr = format!(
        "formatRowNoNewline('JSONEachRow', {})",
        aliased_cols.join(", ")
    );
    RenderExpr::Raw(format_expr)
}
