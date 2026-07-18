//! Analyzer pass that rewrites property access expressions to use tuple indices
//! when the property access references an UNWIND variable backed by tuple_properties.
//!
//! Example:
//! ```cypher
//! UNWIND users as user RETURN user.name
//! ```
//!
//! Before: `user.name` → PropertyAccess { table_alias: "user", column: "full_name" }
//! After: `user.name` → PropertyAccess { table_alias: "user", column: "5" }
//!
//! This enables tuple index access after ARRAY JOIN: `user.5` instead of `user.full_name`

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::{logical_expr::LogicalExpr, logical_plan::LogicalPlan};
use std::sync::Arc;

/// Main entry point: rewrites property access expressions throughout the plan tree
pub fn rewrite_unwind_properties(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    rewrite_plan(plan)
}

/// Recursively rewrite property accesses in the plan tree
fn rewrite_plan(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    match plan.as_ref() {
        LogicalPlan::Unwind(u) => {
            // First, recurse into input
            let new_input = rewrite_plan(u.input.clone());

            // Keep the Unwind unchanged (it already has tuple_properties)
            Arc::new(LogicalPlan::Unwind(
                crate::query_planner::logical_plan::Unwind {
                    input: new_input,
                    expression: u.expression.clone(),
                    alias: u.alias.clone(),
                    label: u.label.clone(),
                    tuple_properties: u.tuple_properties.clone(),
                },
            ))
        }

        LogicalPlan::Projection(p) => {
            let new_input = rewrite_plan(p.input.clone());

            // Build the new Projection first so we can search from its root
            let new_projection = Arc::new(LogicalPlan::Projection(
                crate::query_planner::logical_plan::Projection {
                    input: new_input.clone(),
                    items: vec![], // Temporary, will be replaced
                    distinct: p.distinct,
                    pattern_comprehensions: p.pattern_comprehensions.clone(),
                },
            ));

            // Rewrite expressions in projection items
            // IMPORTANT: Search from new_projection (which includes Unwind in its subtree),
            // not just from new_input (which might be Limit, missing the Unwind context)
            let new_items = p
                .items
                .iter()
                .map(|item| {
                    let new_expr = rewrite_expr(&item.expression, &new_projection);
                    crate::query_planner::logical_plan::ProjectionItem {
                        expression: new_expr,
                        col_alias: item.col_alias.clone(),
                    }
                })
                .collect();

            Arc::new(LogicalPlan::Projection(
                crate::query_planner::logical_plan::Projection {
                    input: new_input,
                    items: new_items,
                    distinct: p.distinct,
                    pattern_comprehensions: p.pattern_comprehensions.clone(),
                },
            ))
        }

        LogicalPlan::Filter(f) => {
            let new_input = rewrite_plan(f.input.clone());
            let new_predicate = rewrite_expr(&f.predicate, &new_input);

            Arc::new(LogicalPlan::Filter(
                crate::query_planner::logical_plan::Filter {
                    input: new_input,
                    predicate: new_predicate,
                },
            ))
        }

        LogicalPlan::GroupBy(g) => {
            let new_input = rewrite_plan(g.input.clone());

            let new_expressions = g
                .expressions
                .iter()
                .map(|expr| rewrite_expr(expr, &new_input))
                .collect();

            let new_having = g
                .having_clause
                .as_ref()
                .map(|h| rewrite_expr(h, &new_input));

            Arc::new(LogicalPlan::GroupBy(
                crate::query_planner::logical_plan::GroupBy {
                    input: new_input,
                    expressions: new_expressions,
                    having_clause: new_having,
                    ..g.clone()
                },
            ))
        }

        LogicalPlan::OrderBy(o) => {
            let new_input = rewrite_plan(o.input.clone());

            let new_items = o
                .items
                .iter()
                .map(|item| crate::query_planner::logical_plan::OrderByItem {
                    expression: rewrite_expr(&item.expression, &new_input),
                    order: item.order.clone(),
                })
                .collect();

            Arc::new(LogicalPlan::OrderBy(
                crate::query_planner::logical_plan::OrderBy {
                    input: new_input,
                    items: new_items,
                },
            ))
        }

        LogicalPlan::GraphRel(gr) => {
            // NOTE: only `left`/`right` are walked here — `center` (the
            // relationship's own scan) is intentionally left untouched,
            // matching the pre-existing (pre-migration) behavior exactly.
            let mut new_gr = gr.clone();
            new_gr.left = rewrite_plan(gr.left.clone());
            new_gr.right = rewrite_plan(gr.right.clone());
            Arc::new(LogicalPlan::GraphRel(new_gr))
        }

        // Base cases - no children to rewrite
        LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => plan.clone(),

        // Write variants — UNWIND-property rewriting only operates on read plans.
        LogicalPlan::Create(_)
        | LogicalPlan::SetProperties(_)
        | LogicalPlan::Delete(_)
        | LogicalPlan::Remove(_) => plan.clone(),

        // Everything else (Unwind, Limit, Skip, WithClause, GraphNode, GraphJoins,
        // Cte, Union, CartesianProduct) is pure structural recursion — walk direct
        // children via the exhaustive `LogicalPlan::map_children` API instead of a
        // hand-rolled per-variant rebuild.
        _ => Arc::new(
            plan.as_ref()
                .map_children(|c| rewrite_plan(Arc::new(c.clone())).as_ref().clone()),
        ),
    }
}

/// Rewrite a single expression, replacing property accesses with tuple indices where applicable
fn rewrite_expr(expr: &LogicalExpr, plan: &Arc<LogicalPlan>) -> LogicalExpr {
    use crate::query_planner::logical_expr::visitors::{map_expression, ExprRewrite};
    // Route through the exhaustive `map_expression` combinator: rewrite tuple
    // property accesses, recurse structurally into everything else. The former
    // hand-rolled walk handled only Operator/ScalarFn/Case (and CLONED
    // AggregateFnCall without recursing — an admitted gap), falling through
    // `_ => expr.clone()` for List/MapLiteral/ArraySubscript/ReduceExpr/…,
    // silently skipping the rewrite inside those wrappers. Latent (no corpus
    // query reached it; verified byte-identical on migration), but now
    // structurally impossible.
    map_expression(expr, &mut |node| {
        if let LogicalExpr::PropertyAccessExp(pa) = node {
            if let Some((new_column, _tuple_props)) =
                find_tuple_property_index(&pa.table_alias.0, &pa.column, plan)
            {
                log::debug!(
                    "Rewrote property access: {}.{:?} → {}.{}",
                    pa.table_alias.0,
                    pa.column,
                    pa.table_alias.0,
                    new_column
                );
                return ExprRewrite::Replace(LogicalExpr::PropertyAccessExp(
                    crate::query_planner::logical_expr::PropertyAccess {
                        table_alias: pa.table_alias.clone(),
                        column: PropertyValue::Column(new_column),
                    },
                ));
            }
        }
        ExprRewrite::Recurse
    })
}

/// Find tuple property index for a given alias.property, returns (column_index_as_string, tuple_properties)
fn find_tuple_property_index(
    alias: &str,
    column: &PropertyValue,
    plan: &Arc<LogicalPlan>,
) -> Option<(String, Vec<(String, usize)>)> {
    match plan.as_ref() {
        LogicalPlan::Unwind(u) => {
            if u.alias == alias {
                if let Some(tuple_props) = &u.tuple_properties {
                    // Extract property name from PropertyValue
                    let prop_name = match column {
                        PropertyValue::Column(name) => name.as_str(),
                        PropertyValue::Expression(_) => return None, // Can't rewrite expressions
                    };

                    // Find the index for this property
                    for (stored_prop, idx) in tuple_props {
                        if stored_prop == prop_name {
                            return Some((idx.to_string(), tuple_props.clone()));
                        }
                    }

                    log::debug!(
                        "Property '{}' not found in tuple_properties for alias '{}'",
                        prop_name,
                        alias
                    );
                }
            }

            // Recurse to input
            find_tuple_property_index(alias, column, &u.input)
        }

        // Recurse through all other plan types
        LogicalPlan::Projection(p) => find_tuple_property_index(alias, column, &p.input),
        LogicalPlan::Filter(f) => find_tuple_property_index(alias, column, &f.input),
        LogicalPlan::GroupBy(g) => find_tuple_property_index(alias, column, &g.input),
        LogicalPlan::OrderBy(o) => find_tuple_property_index(alias, column, &o.input),
        LogicalPlan::Limit(l) => find_tuple_property_index(alias, column, &l.input),
        LogicalPlan::Skip(s) => find_tuple_property_index(alias, column, &s.input),
        LogicalPlan::WithClause(wc) => find_tuple_property_index(alias, column, &wc.input),
        LogicalPlan::GraphNode(gn) => find_tuple_property_index(alias, column, &gn.input),
        LogicalPlan::GraphRel(gr) => {
            // Check both sides
            if let Some(result) = find_tuple_property_index(alias, column, &gr.left) {
                return Some(result);
            }
            find_tuple_property_index(alias, column, &gr.right)
        }
        LogicalPlan::GraphJoins(gj) => find_tuple_property_index(alias, column, &gj.input),
        LogicalPlan::Cte(cte) => {
            // CTEs have their definitions in a separate field, but we don't traverse into them
            // (they're independent scopes). Just check the input.
            find_tuple_property_index(alias, column, &cte.input)
        }
        LogicalPlan::Union(u) => {
            // Check all inputs
            for input in &u.inputs {
                if let Some(result) = find_tuple_property_index(alias, column, input) {
                    return Some(result);
                }
            }
            None
        }
        LogicalPlan::CartesianProduct(cp) => {
            // Check both sides
            if let Some(result) = find_tuple_property_index(alias, column, &cp.left) {
                return Some(result);
            }
            find_tuple_property_index(alias, column, &cp.right)
        }

        // Base cases
        LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => None,

        // Write variants — no tuple-property indices to find.
        LogicalPlan::Create(_)
        | LogicalPlan::SetProperties(_)
        | LogicalPlan::Delete(_)
        | LogicalPlan::Remove(_) => None,
    }
}
