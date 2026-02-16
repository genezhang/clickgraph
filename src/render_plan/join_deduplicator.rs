//! Join Alias Deduplicator
//!
//! Ensures all table aliases in JOIN clauses are unique within a query.
//! When the same table is joined multiple times (e.g., in complex VLP patterns),
//! duplicate aliases can occur. This module detects and resolves such collisions.

use crate::render_plan::render_expr::{
    AggregateFnCall, InSubquery, OperatorApplication, PropertyAccess, ReduceExpr, RenderCase,
    RenderExpr, ScalarFnCall, TableAlias,
};
use crate::render_plan::Join;
use std::collections::HashSet;

/// Deduplicate table aliases in a list of joins
/// Simple approach: track used aliases and rename duplicates with sequential suffix
pub fn deduplicate_join_aliases(joins: Vec<Join>) -> Vec<Join> {
    let mut used_aliases: HashSet<String> = HashSet::new();
    let mut result = Vec::new();

    for mut join in joins {
        let original_alias = join.table_alias.clone();

        // If alias is unique, use it
        if used_aliases.insert(original_alias.clone()) {
            result.push(join);
        } else {
            // Alias collision - find next available sequential number
            let mut counter = 1;
            loop {
                let new_alias = format!("{}_{}", original_alias, counter);
                if used_aliases.insert(new_alias.clone()) {
                    log::warn!(
                        "🔧 Alias collision: Renaming '{}' → '{}'",
                        original_alias,
                        new_alias
                    );

                    // Update the join's alias
                    join.table_alias = new_alias.clone();

                    // Update all references to the old alias in JOIN conditions
                    join.joining_on =
                        rewrite_conditions(&join.joining_on, &original_alias, &new_alias);
                    if let Some(ref filter) = join.pre_filter {
                        join.pre_filter =
                            Some(rewrite_render_expr(filter, &original_alias, &new_alias));
                    }

                    result.push(join);
                    break;
                }
                counter += 1;
            }
        }
    }

    result
}

/// Rewrite JOIN conditions to use new aliases
fn rewrite_conditions(
    conditions: &[OperatorApplication],
    old_alias: &str,
    new_alias: &str,
) -> Vec<OperatorApplication> {
    conditions
        .iter()
        .map(|cond| rewrite_operator_application(cond, old_alias, new_alias))
        .collect()
}

/// Rewrite a single OperatorApplication with new aliases
fn rewrite_operator_application(
    op: &OperatorApplication,
    old_alias: &str,
    new_alias: &str,
) -> OperatorApplication {
    OperatorApplication {
        operator: op.operator.clone(),
        operands: op
            .operands
            .iter()
            .map(|expr| rewrite_render_expr(expr, old_alias, new_alias))
            .collect(),
    }
}

/// Rewrite RenderExpr to use new aliases
fn rewrite_render_expr(expr: &RenderExpr, old_alias: &str, new_alias: &str) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == old_alias {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(new_alias.to_string()),
                    column: pa.column.clone(),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::TableAlias(ta) => {
            if ta.0 == old_alias {
                RenderExpr::TableAlias(TableAlias(new_alias.to_string()))
            } else {
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application(op, old_alias, new_alias),
        ),
        // Handle function calls with nested expressions
        RenderExpr::ScalarFnCall(func) => {
            let rewritten_args = func
                .args
                .iter()
                .map(|arg| rewrite_render_expr(arg, old_alias, new_alias))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                args: rewritten_args,
                ..func.clone()
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let rewritten_args = agg
                .args
                .iter()
                .map(|arg| rewrite_render_expr(arg, old_alias, new_alias))
                .collect();
            RenderExpr::AggregateFnCall(AggregateFnCall {
                args: rewritten_args,
                ..agg.clone()
            })
        }
        // Handle CASE expressions
        RenderExpr::Case(case) => {
            let rewritten_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_render_expr(e, old_alias, new_alias)));
            let rewritten_when_then = case
                .when_then
                .iter()
                .map(|(when, then)| {
                    (
                        rewrite_render_expr(when, old_alias, new_alias),
                        rewrite_render_expr(then, old_alias, new_alias),
                    )
                })
                .collect();
            let rewritten_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_render_expr(e, old_alias, new_alias)));
            RenderExpr::Case(RenderCase {
                expr: rewritten_expr,
                when_then: rewritten_when_then,
                else_expr: rewritten_else,
            })
        }
        // Handle IN subqueries
        RenderExpr::InSubquery(subq) => RenderExpr::InSubquery(InSubquery {
            expr: Box::new(rewrite_render_expr(&subq.expr, old_alias, new_alias)),
            ..subq.clone()
        }),
        // Handle array operations
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(rewrite_render_expr(array, old_alias, new_alias)),
            index: Box::new(rewrite_render_expr(index, old_alias, new_alias)),
        },
        RenderExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
            array: Box::new(rewrite_render_expr(array, old_alias, new_alias)),
            from: from
                .as_ref()
                .map(|f| Box::new(rewrite_render_expr(f, old_alias, new_alias))),
            to: to
                .as_ref()
                .map(|t| Box::new(rewrite_render_expr(t, old_alias, new_alias))),
        },
        RenderExpr::List(items) => {
            let rewritten_items = items
                .iter()
                .map(|item| rewrite_render_expr(item, old_alias, new_alias))
                .collect();
            RenderExpr::List(rewritten_items)
        }
        // Handle reduce expressions
        RenderExpr::ReduceExpr(reduce) => RenderExpr::ReduceExpr(ReduceExpr {
            initial_value: Box::new(rewrite_render_expr(
                &reduce.initial_value,
                old_alias,
                new_alias,
            )),
            list: Box::new(rewrite_render_expr(&reduce.list, old_alias, new_alias)),
            expression: Box::new(rewrite_render_expr(
                &reduce.expression,
                old_alias,
                new_alias,
            )),
            ..reduce.clone()
        }),
        // Handle map literals
        RenderExpr::MapLiteral(entries) => {
            let rewritten_entries = entries
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        rewrite_render_expr(value, old_alias, new_alias),
                    )
                })
                .collect();
            RenderExpr::MapLiteral(rewritten_entries)
        }
        // Leaf nodes with no nested expressions
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::Column(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_)
        | RenderExpr::Parameter(_) => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{Column, Operator};
    use crate::render_plan::{Join, JoinType};

    #[test]
    fn test_no_duplicates() {
        let joins = vec![
            Join {
                table_name: "table1".to_string(),
                table_alias: "t1".to_string(),
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
            Join {
                table_name: "table2".to_string(),
                table_alias: "t2".to_string(),
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
        ];

        let result = deduplicate_join_aliases(joins.clone());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].table_alias, "t1");
        assert_eq!(result[1].table_alias, "t2");
    }

    #[test]
    fn test_duplicate_aliases() {
        let joins = vec![
            Join {
                table_name: "table1".to_string(),
                table_alias: "t143".to_string(),
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
            Join {
                table_name: "table2".to_string(),
                table_alias: "t143".to_string(), // Duplicate!
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
        ];

        let result = deduplicate_join_aliases(joins);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].table_alias, "t143"); // First occurrence keeps original
        assert_eq!(result[1].table_alias, "t143_1"); // Second gets renamed
    }

    #[test]
    fn test_triple_duplicate() {
        let joins = vec![
            Join {
                table_name: "table1".to_string(),
                table_alias: "t1".to_string(),
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
            Join {
                table_name: "table2".to_string(),
                table_alias: "t1".to_string(), // Duplicate!
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
            Join {
                table_name: "table3".to_string(),
                table_alias: "t1".to_string(), // Triple!
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
        ];

        let result = deduplicate_join_aliases(joins);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].table_alias, "t1");
        assert_eq!(result[1].table_alias, "t1_1");
        assert_eq!(result[2].table_alias, "t1_2");
    }
}
