//! Join Alias Deduplicator
//!
//! Ensures all table aliases in JOIN clauses are unique within a query.
//! When the same table is joined multiple times (e.g., in complex VLP patterns),
//! duplicate aliases can occur. This module detects and resolves such collisions.

use crate::render_plan::render_expr::{OperatorApplication, PropertyAccess, RenderExpr, TableAlias};
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
                    log::warn!("🔧 Alias collision: Renaming '{}' → '{}'", original_alias, new_alias);
                    
                    // Update the join's alias
                    join.table_alias = new_alias.clone();
                    
                    // Update all references to the old alias in JOIN conditions
                    join.joining_on = rewrite_conditions(&join.joining_on, &original_alias, &new_alias);
                    if let Some(ref filter) = join.pre_filter {
                        join.pre_filter = Some(rewrite_render_expr(filter, &original_alias, &new_alias));
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
        operands: op.operands.iter().map(|expr| rewrite_render_expr(expr, old_alias, new_alias)).collect(),
    }
}

/// Rewrite RenderExpr to use new aliases
fn rewrite_render_expr(
    expr: &RenderExpr,
    old_alias: &str,
    new_alias: &str,
) -> RenderExpr {
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
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(rewrite_operator_application(op, old_alias, new_alias))
        }
        // For other expr types, return as-is (no alias references)
        _ => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::{JoinType, Join};
    use crate::render_plan::render_expr::{Column, Operator};

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
                table_alias: "t143".to_string(),  // Duplicate!
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
        assert_eq!(result[0].table_alias, "t143");   // First occurrence keeps original
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
                table_alias: "t1".to_string(),  // Duplicate!
                joining_on: vec![],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            },
            Join {
                table_name: "table3".to_string(),
                table_alias: "t1".to_string(),  // Triple!
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
