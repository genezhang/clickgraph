//! Scoping-Only WITH Clause Collapse
//!
//! Detects multi-variable WITH clauses that purely pass variables through for scoping
//! (e.g., `WITH country, zombie` — no aggregation, computation, DISTINCT, ORDER BY,
//! SKIP, LIMIT, or WHERE) and removes them from the plan tree.
//!
//! This must run BEFORE GraphJoinInference and CteSchemaResolver so that:
//! 1. GraphJoinInference computes correct joins for the merged patterns
//! 2. CteSchemaResolver doesn't register stale CTE schemas for collapsed WITHs
//!
//! The optimization improves FROM/JOIN ordering in downstream CTEs. Without it,
//! pass-through CTEs force downstream OPTIONAL MATCH CTEs to use cross-joins
//! and wrong FROM table selection (e.g., scanning 25M messages instead of
//! starting from 500 filtered zombies in LDBC bi-13).

use std::sync::Arc;

use crate::query_planner::logical_plan::{LogicalPlan, WithClause};

/// Check if a WithClause is scoping-only (no data transformation).
/// All items must be simple variable references (TableAlias or ColumnAlias)
/// with no renames, and no modifiers (DISTINCT, ORDER BY, SKIP, LIMIT, WHERE).
fn is_scoping_only_with(wc: &WithClause) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;

    // Must have multiple items (single-item WITHs are cheap and collapsing
    // them can lose filters from undirected edge Union branches)
    if wc.items.len() <= 1 {
        return false;
    }

    // Must have no modifiers
    if wc.distinct
        || wc.order_by.is_some()
        || wc.skip.is_some()
        || wc.limit.is_some()
        || wc.where_clause.is_some()
        || !wc.pattern_comprehensions.is_empty()
    {
        return false;
    }

    // All items must be simple variable references with no renames
    for item in &wc.items {
        match &item.expression {
            LogicalExpr::TableAlias(ta) => {
                if let Some(ref alias) = item.col_alias {
                    let clean = alias.0.strip_suffix(".*").unwrap_or(&alias.0);
                    if clean != ta.0 {
                        return false; // Rename — needs CTE
                    }
                }
            }
            LogicalExpr::ColumnAlias(ca) => {
                if let Some(ref alias) = item.col_alias {
                    let clean = alias.0.strip_suffix(".*").unwrap_or(&alias.0);
                    if clean != ca.0 {
                        return false;
                    }
                }
            }
            _ => return false, // Complex expression
        }
    }

    true
}

/// Recursively collapse scoping-only WITH clauses in the plan tree.
/// Replaces qualifying WithClause nodes with their input subtrees.
///
/// Skips the optimization for plans containing variable-length paths (VLPs),
/// because VLP property detection depends on WITH clause boundaries.
pub fn collapse_scoping_only_withs(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    // Skip optimization entirely if the plan contains VLPs
    if plan.contains_variable_length_path() {
        return plan;
    }

    Arc::new(collapse_recursive(&plan))
}

fn collapse_recursive(plan: &LogicalPlan) -> LogicalPlan {
    use crate::query_planner::logical_plan::*;

    match plan {
        LogicalPlan::WithClause(wc) => {
            // First recurse into input
            let new_input = collapse_recursive(&wc.input);

            // Check if this is a scoping-only WITH
            if is_scoping_only_with(wc) {
                log::info!(
                    "ScopingWithCollapse: Collapsing scoping-only WITH ({} items: {:?})",
                    wc.items.len(),
                    wc.exported_aliases
                );
                // Replace WITH with its (already-recursed) input
                new_input
            } else {
                // Keep the WithClause with recursed input
                LogicalPlan::WithClause(WithClause {
                    cte_name: wc.cte_name.clone(),
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
                })
            }
        }
        LogicalPlan::GraphRel(gr) => {
            let new_left = collapse_recursive(&gr.left);
            let new_right = collapse_recursive(&gr.right);
            LogicalPlan::GraphRel(GraphRel {
                left: Arc::new(new_left),
                center: gr.center.clone(),
                right: Arc::new(new_right),
                alias: gr.alias.clone(),
                direction: gr.direction.clone(),
                left_connection: gr.left_connection.clone(),
                right_connection: gr.right_connection.clone(),
                is_rel_anchor: gr.is_rel_anchor,
                variable_length: gr.variable_length.clone(),
                shortest_path_mode: gr.shortest_path_mode.clone(),
                path_variable: gr.path_variable.clone(),
                where_predicate: gr.where_predicate.clone(),
                labels: gr.labels.clone(),
                is_optional: gr.is_optional,
                anchor_connection: gr.anchor_connection.clone(),
                cte_references: gr.cte_references.clone(),
                pattern_combinations: gr.pattern_combinations.clone(),
                was_undirected: gr.was_undirected,
            })
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left = collapse_recursive(&cp.left);
            let new_right = collapse_recursive(&cp.right);
            LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            })
        }
        LogicalPlan::Filter(f) => {
            let new_input = collapse_recursive(&f.input);
            LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: f.predicate.clone(),
            })
        }
        LogicalPlan::Projection(proj) => {
            let new_input = collapse_recursive(&proj.input);
            LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: proj.items.clone(),
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            })
        }
        LogicalPlan::Union(u) => {
            let new_inputs = u
                .inputs
                .iter()
                .map(|i| Arc::new(collapse_recursive(i)))
                .collect();
            LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: u.union_type.clone(),
            })
        }
        LogicalPlan::Unwind(uw) => {
            let new_input = collapse_recursive(&uw.input);
            LogicalPlan::Unwind(Unwind {
                input: Arc::new(new_input),
                expression: uw.expression.clone(),
                alias: uw.alias.clone(),
                label: uw.label.clone(),
                tuple_properties: uw.tuple_properties.clone(),
            })
        }
        LogicalPlan::OrderBy(ob) => {
            let new_input = collapse_recursive(&ob.input);
            LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: ob.items.clone(),
            })
        }
        LogicalPlan::Limit(lim) => {
            let new_input = collapse_recursive(&lim.input);
            LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: lim.count,
            })
        }
        LogicalPlan::Skip(skip) => {
            let new_input = collapse_recursive(&skip.input);
            LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            })
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input = collapse_recursive(&gb.input);
            LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: gb.expressions.clone(),
                having_clause: gb.having_clause.clone(),
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            })
        }
        LogicalPlan::Cte(c) => {
            let new_input = collapse_recursive(&c.input);
            LogicalPlan::Cte(Cte {
                input: Arc::new(new_input),
                name: c.name.clone(),
            })
        }
        LogicalPlan::GraphJoins(gj) => {
            // GraphJoins shouldn't exist at this stage (before GraphJoinInference),
            // but handle it for safety
            let new_input = collapse_recursive(&gj.input);
            LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: gj.joins.clone(),
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: gj.anchor_table.clone(),
                cte_references: gj.cte_references.clone(),
                correlation_predicates: gj.correlation_predicates.clone(),
            })
        }
        // Leaf nodes — return unchanged
        other => other.clone(),
    }
}
