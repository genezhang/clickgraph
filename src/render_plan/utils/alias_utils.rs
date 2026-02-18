//! Alias and table utilities
//!
//! This module contains functions for handling table aliases,
//! table name resolution, and alias management.

use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::RenderExpr;
use std::sync::Arc;

/// Strip database prefix from table name.
/// Removes database prefixes like "database.table" to get just "table".
pub fn strip_database_prefix(table_name: &str) -> String {
    if let Some(dot_pos) = table_name.rfind('.') {
        table_name[dot_pos + 1..].to_string()
    } else {
        table_name.to_string()
    }
}

/// Find the label (node/relationship type) for a given alias.
/// Searches the plan to determine what type of entity the alias represents.
pub fn find_label_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                // label is Option<String>, unwrap it
                node.label.clone()
            } else {
                None
            }
        }
        LogicalPlan::GraphRel(_rel) => {
            // Check left and right connections
            // Note: GraphRel doesn't have nested plans, just connection strings
            None
        }
        LogicalPlan::Filter(filter) => find_label_for_alias(&filter.input, target_alias),
        LogicalPlan::Cte(cte) => find_label_for_alias(&cte.input, target_alias),
        LogicalPlan::Projection(proj) => find_label_for_alias(&proj.input, target_alias),
        _ => None,
    }
}

/// Get the anchor alias from a plan.
/// Returns the primary alias that serves as the anchor for the query.
pub fn get_anchor_alias_from_plan(plan: &Arc<LogicalPlan>) -> Option<String> {
    match plan.as_ref() {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::GraphRel(rel) => Some(rel.left_connection.clone()),
        LogicalPlan::Projection(proj) => get_anchor_alias_from_plan(&proj.input),
        LogicalPlan::Filter(filter) => get_anchor_alias_from_plan(&filter.input),
        LogicalPlan::GroupBy(gb) => get_anchor_alias_from_plan(&gb.input),
        LogicalPlan::GraphJoins(gj) => get_anchor_alias_from_plan(&gj.input),
        _ => None,
    }
}

/// Collect aliases from inner scopes.
/// Finds aliases that are defined within nested scopes of the plan.
pub fn collect_inner_scope_aliases(
    plan: &LogicalPlan,
    outer_aliases: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    let mut inner_aliases = std::collections::HashSet::new();

    fn traverse_plan(
        plan: &LogicalPlan,
        outer_aliases: &std::collections::HashSet<String>,
        inner_aliases: &mut std::collections::HashSet<String>,
    ) {
        match plan {
            LogicalPlan::GraphNode(node) => {
                if !outer_aliases.contains(&node.alias) {
                    inner_aliases.insert(node.alias.clone());
                }
                traverse_plan(&node.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::GraphRel(rel) => {
                if !rel.alias.is_empty() && !outer_aliases.contains(&rel.alias) {
                    inner_aliases.insert(rel.alias.clone());
                }
                traverse_plan(&rel.left, outer_aliases, inner_aliases);
                traverse_plan(&rel.center, outer_aliases, inner_aliases);
                traverse_plan(&rel.right, outer_aliases, inner_aliases);
            }
            LogicalPlan::Filter(filter_plan) => {
                traverse_plan(&filter_plan.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::Projection(proj_plan) => {
                traverse_plan(&proj_plan.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::GroupBy(gb_plan) => {
                if let Some(ref alias) = gb_plan.exposed_alias {
                    if !outer_aliases.contains(alias) {
                        inner_aliases.insert(alias.clone());
                    }
                }
                traverse_plan(&gb_plan.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::OrderBy(sort_plan) => {
                traverse_plan(&sort_plan.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::Limit(limit_plan) => {
                traverse_plan(&limit_plan.input, outer_aliases, inner_aliases);
            }
            LogicalPlan::Union(union_plan) => {
                for input in &union_plan.inputs {
                    traverse_plan(input, outer_aliases, inner_aliases);
                }
            }
            _ => {}
        }
    }

    traverse_plan(plan, outer_aliases, &mut inner_aliases);
    inner_aliases
}

/// Check if a condition references a specific alias.
/// Returns true if the render expression references the given alias.
pub fn cond_references_alias(expr: &RenderExpr, alias: &str) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
        RenderExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|operand| cond_references_alias(operand, alias)),
        RenderExpr::AggregateFnCall(agg) => {
            agg.args.iter().any(|arg| cond_references_alias(arg, alias))
        }
        RenderExpr::ScalarFnCall(scalar) => scalar
            .args
            .iter()
            .any(|arg| cond_references_alias(arg, alias)),
        RenderExpr::Case(case_expr) => {
            case_expr.when_then.iter().any(|(when, then)| {
                cond_references_alias(when, alias) || cond_references_alias(then, alias)
            }) || case_expr
                .else_expr
                .as_ref()
                .is_some_and(|else_expr| cond_references_alias(else_expr, alias))
        }
        // Other expression types don't reference aliases
        RenderExpr::ColumnAlias(_)
        | RenderExpr::Literal(_)
        | RenderExpr::Parameter(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::List(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::ReduceExpr(_)
        | RenderExpr::MapLiteral(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::ArraySubscript { .. }
        | RenderExpr::ArraySlicing { .. }
        | RenderExpr::InSubquery(_)
        | RenderExpr::CteEntityRef(_) => false,
    }
}

/// Check if an operator application references a specific alias.
/// Returns true if any operand references the given alias.
pub fn operator_references_alias(
    op: &crate::render_plan::render_expr::OperatorApplication,
    alias: &str,
) -> bool {
    op.operands
        .iter()
        .any(|operand| cond_references_alias(operand, alias))
}

/// Find the alias that references a specific CTE.
/// Searches the plan for expressions that reference the given CTE name.
pub fn find_cte_reference_alias(plan: &LogicalPlan, cte_name: &str) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(scan_plan) => {
            // Check if this scan references our CTE
            if scan_plan.source_table == cte_name {
                // ViewScan doesn't have an alias field, so we need to find it differently
                // This might need to be determined from context
                None
            } else {
                None
            }
        }
        LogicalPlan::Cte(cte_plan) => {
            // Check the inner plan
            find_cte_reference_alias(&cte_plan.input, cte_name)
        }
        LogicalPlan::GraphJoins(join_plan) => {
            // Check the input of the join
            find_cte_reference_alias(&join_plan.input, cte_name)
        }
        LogicalPlan::Filter(filter_plan) => find_cte_reference_alias(&filter_plan.input, cte_name),
        LogicalPlan::Projection(proj_plan) => find_cte_reference_alias(&proj_plan.input, cte_name),
        LogicalPlan::GroupBy(gb_plan) => find_cte_reference_alias(&gb_plan.input, cte_name),
        LogicalPlan::OrderBy(sort_plan) => find_cte_reference_alias(&sort_plan.input, cte_name),
        LogicalPlan::Limit(limit_plan) => find_cte_reference_alias(&limit_plan.input, cte_name),
        LogicalPlan::Union(union_plan) => {
            for input in &union_plan.inputs {
                if let Some(alias) = find_cte_reference_alias(input, cte_name) {
                    return Some(alias);
                }
            }
            None
        }
        _ => None,
    }
}

/// Collect all aliases from a logical plan.
/// Returns a set of all table aliases referenced in the plan.
pub fn collect_aliases_from_plan(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    use std::collections::HashSet;

    fn collect_recursive(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
        match plan {
            LogicalPlan::GraphNode(node) => {
                aliases.insert(node.alias.clone());
                collect_recursive(&node.input, aliases);
            }
            LogicalPlan::GraphRel(rel) => {
                // GraphRel alias is the relationship alias (e.g., "t1")
                if !rel.alias.is_empty() {
                    aliases.insert(rel.alias.clone());
                }
                collect_recursive(&rel.left, aliases);
                collect_recursive(&rel.center, aliases);
                collect_recursive(&rel.right, aliases);
            }
            LogicalPlan::Projection(proj) => {
                collect_recursive(&proj.input, aliases);
            }
            LogicalPlan::Filter(filter) => {
                collect_recursive(&filter.input, aliases);
            }
            LogicalPlan::GroupBy(gb) => {
                if let Some(ref alias) = gb.exposed_alias {
                    aliases.insert(alias.clone());
                }
                collect_recursive(&gb.input, aliases);
            }
            LogicalPlan::OrderBy(order_by) => {
                collect_recursive(&order_by.input, aliases);
            }
            LogicalPlan::Limit(limit) => {
                collect_recursive(&limit.input, aliases);
            }
            LogicalPlan::Union(union) => {
                for input in &union.inputs {
                    collect_recursive(input, aliases);
                }
            }
            LogicalPlan::GraphJoins(gj) => {
                // Collect join table aliases (e.g., "t224" for Person_knows_Person)
                for join in &gj.joins {
                    if !join.table_alias.is_empty() {
                        aliases.insert(join.table_alias.clone());
                    }
                }
                collect_recursive(&gj.input, aliases);
            }
            LogicalPlan::CartesianProduct(cp) => {
                collect_recursive(&cp.left, aliases);
                collect_recursive(&cp.right, aliases);
            }
            LogicalPlan::WithClause(wc) => {
                collect_recursive(&wc.input, aliases);
            }
            LogicalPlan::ViewScan(vs) => {
                // ViewScan is a leaf — no aliases to collect from it directly
            }
            _ => {}
        }
    }

    let mut aliases = HashSet::new();
    collect_recursive(plan, &mut aliases);
    aliases
}
