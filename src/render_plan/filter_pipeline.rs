use super::render_expr::{
    AggregateFnCall, Column, Operator, OperatorApplication, RenderExpr,
    ScalarFnCall, TableAlias,
};
use crate::graph_catalog::expression_parser::PropertyValue;

/// Represents categorized filters for different parts of a query
#[derive(Debug, Clone)]
pub struct CategorizedFilters {
    pub start_node_filters: Option<RenderExpr>,
    pub end_node_filters: Option<RenderExpr>,
    pub relationship_filters: Option<RenderExpr>,
    pub path_function_filters: Option<RenderExpr>, // Filters on path functions like length(p), nodes(p)
}

/// Categorize filters based on which nodes/relationships they reference
pub fn categorize_filters(
    filter_expr: Option<&RenderExpr>,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    _rel_alias: &str, // For future relationship filtering
) -> CategorizedFilters {
    log::debug!(
        "Categorizing filters for start alias '{}' and end alias '{}'",
        start_cypher_alias,
        end_cypher_alias
    );

    let mut result = CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
        path_function_filters: None,
    };

    if filter_expr.is_none() {
        log::trace!("No filter expression provided");
        return result;
    }

    log::trace!("Filter expression: {:?}", filter_expr.unwrap());

    let filter = filter_expr.unwrap();

    // Helper to check if an expression references a specific alias (checks both Cypher and SQL aliases)
    fn references_alias(expr: &RenderExpr, cypher_alias: &str, sql_alias: &str) -> bool {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                let table_alias = &prop.table_alias.0;
                table_alias == cypher_alias || table_alias == sql_alias
            }
            RenderExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .any(|operand| references_alias(operand, cypher_alias, sql_alias)),
            _ => false,
        }
    }

    // Helper to check if an expression contains path function calls
    fn contains_path_function(expr: &RenderExpr) -> bool {
        match expr {
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check if this is a path function (length, nodes, relationships)
                matches!(
                    fn_call.name.to_lowercase().as_str(),
                    "length" | "nodes" | "relationships"
                )
            }
            RenderExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .any(|operand| contains_path_function(operand)),
            _ => false,
        }
    }

    // Split AND-connected filters into individual predicates
    fn split_and_filters(expr: &RenderExpr) -> Vec<RenderExpr> {
        match expr {
            RenderExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
                let mut filters = Vec::new();
                for operand in &op.operands {
                    filters.extend(split_and_filters(operand));
                }
                filters
            }
            _ => vec![expr.clone()],
        }
    }

    // Split the filter into individual predicates
    let predicates = split_and_filters(filter);

    let mut start_filters = Vec::new();
    let mut end_filters = Vec::new();
    let mut rel_filters = Vec::new();
    let mut path_fn_filters = Vec::new();

    for predicate in predicates {
        let refs_start = references_alias(&predicate, start_cypher_alias, "start_node");
        let refs_end = references_alias(&predicate, end_cypher_alias, "end_node");
        let has_path_fn = contains_path_function(&predicate);

        crate::debug_println!("DEBUG: Categorizing predicate: {:?}", predicate);
        println!(
            "DEBUG: refs_start (alias '{}'): {}",
            start_cypher_alias, refs_start
        );
        println!(
            "DEBUG: refs_end (alias '{}'): {}",
            end_cypher_alias, refs_end
        );
        crate::debug_println!("DEBUG: has_path_fn: {}", has_path_fn);

        if has_path_fn {
            // Path function filters (e.g., WHERE length(p) <= 3) go in path function filters
            crate::debug_println!("DEBUG: Going to path_fn_filters");
            path_fn_filters.push(predicate);
        } else if refs_start && refs_end {
            // Filter references both nodes - can't categorize simply
            // For now, treat as start filter (will be in base case)
            crate::debug_println!("DEBUG: Going to start_filters (refs both)");
            start_filters.push(predicate);
        } else if refs_start {
            crate::debug_println!("DEBUG: Going to start_filters");
            start_filters.push(predicate);
        } else if refs_end {
            crate::debug_println!("DEBUG: Going to end_filters");
            end_filters.push(predicate);
        } else {
            // Doesn't reference nodes - might be relationship filter or constant
            crate::debug_println!("DEBUG: Going to rel_filters");
            rel_filters.push(predicate);
        }
    }

    // Combine filters with AND
    fn combine_with_and(filters: Vec<RenderExpr>) -> Option<RenderExpr> {
        if filters.is_empty() {
            return None;
        }
        if filters.len() == 1 {
            return Some(filters.into_iter().next().unwrap());
        }
        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        }))
    }

    result.start_node_filters = combine_with_and(start_filters);
    result.end_node_filters = combine_with_and(end_filters);
    result.relationship_filters = combine_with_and(rel_filters);
    result.path_function_filters = combine_with_and(path_fn_filters);

    log::trace!("Filter categorization result:");
    log::trace!("  Start filters: {:?}", result.start_node_filters);
    log::trace!("  End filters: {:?}", result.end_node_filters);
    log::trace!("  Rel filters: {:?}", result.relationship_filters);
    log::trace!(
        "  Path function filters: {:?}",
        result.path_function_filters
    );

    result
}

/// Clean last node filters by removing InSubquery expressions
pub fn clean_last_node_filters(filter_opt: Option<RenderExpr>) -> Option<RenderExpr> {
    if let Some(filter_expr) = filter_opt {
        match filter_expr {
            // remove InSubquery as we have added it in graph_traversal_planning phase. Since this is for last node, we are going to select that node directly
            // we do not need this InSubquery
            RenderExpr::InSubquery(_sq) => None,
            RenderExpr::OperatorApplicationExp(op) => {
                let mut stripped = Vec::new();
                for operand in op.operands {
                    if let Some(e) = clean_last_node_filters(Some(operand)) {
                        stripped.push(e);
                    }
                }
                match stripped.len() {
                    0 => None,
                    1 => Some(stripped.into_iter().next().unwrap()),
                    _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator,
                        operands: stripped,
                    })),
                }
            }
            RenderExpr::List(list) => {
                let mut stripped = Vec::new();
                for inner in list {
                    if let Some(e) = clean_last_node_filters(Some(inner)) {
                        stripped.push(e);
                    }
                }
                match stripped.len() {
                    0 => None,
                    1 => Some(stripped.into_iter().next().unwrap()),
                    _ => Some(RenderExpr::List(stripped)),
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                let mut stripped_args = Vec::new();
                for arg in agg.args {
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
                        stripped_args.push(e);
                    }
                }
                if stripped_args.is_empty() {
                    None
                } else {
                    Some(RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: agg.name,
                        args: stripped_args,
                    }))
                }
            }
            RenderExpr::ScalarFnCall(func) => {
                let mut stripped_args = Vec::new();
                for arg in func.args {
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
                        stripped_args.push(e);
                    }
                }
                if stripped_args.is_empty() {
                    None
                } else {
                    Some(RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: func.name,
                        args: stripped_args,
                    }))
                }
            }
            other => Some(other),
        }
    } else {
        None
    }
}

/// Rewrite expressions for variable-length CTE outer query
/// Converts Cypher property accesses to CTE column references for SELECT clauses
pub fn rewrite_expr_for_var_len_cte(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    _path_var: Option<&str>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let mut new_prop = prop.clone();
            if prop.table_alias.0 == start_cypher_alias {
                new_prop.table_alias = TableAlias("t".to_string());
                if prop.column.0.raw() == "*" {
                    new_prop.column = prop.column.clone();
                } else {
                    new_prop.column = Column(
                        PropertyValue::Column(
                            format!("start_{}", prop.column.0.raw())
                        )
                    );
                }
            } else if prop.table_alias.0 == end_cypher_alias {
                // End node properties stay as is
            } else {
                // Other properties stay as is
            }
            RenderExpr::PropertyAccessExp(new_prop)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let rewritten_operands = op
                .operands
                .iter()
                .map(|operand| {
                    rewrite_expr_for_var_len_cte(
                        operand,
                        start_cypher_alias,
                        end_cypher_alias,
                        _path_var,
                    )
                })
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            let rewritten_args = fn_call
                .args
                .iter()
                .map(|arg| {
                    rewrite_expr_for_var_len_cte(
                        arg,
                        start_cypher_alias,
                        end_cypher_alias,
                        _path_var,
                    )
                })
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(),
    }
}

/// Rewrite expressions for mixed denormalized patterns
/// Only rewrites properties for the side that is denormalized
/// Standard side properties are left unchanged (they'll be resolved by JOINs)
#[allow(clippy::too_many_arguments)]
pub fn rewrite_expr_for_mixed_denormalized_cte(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    start_is_denormalized: bool,
    end_is_denormalized: bool,
    rel_alias: Option<&str>,
    from_col: Option<&str>,
    to_col: Option<&str>,
    _path_var: Option<&str>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let mut new_prop = prop.clone();
            let raw_col = prop.column.0.raw();
            
            // Check if this is a relationship alias access (e.g., f.Origin, f.Dest)
            if let (Some(rel), Some(from), Some(to)) = (rel_alias, from_col, to_col) {
                if prop.table_alias.0 == rel {
                    new_prop.table_alias = TableAlias("t".to_string());
                    if raw_col == from {
                        // from_col (e.g., Origin) → start_id
                        new_prop.column = Column(PropertyValue::Column("start_id".to_string()));
                    } else if raw_col == to {
                        // to_col (e.g., Dest) → end_id
                        new_prop.column = Column(PropertyValue::Column("end_id".to_string()));
                    }
                    return RenderExpr::PropertyAccessExp(new_prop);
                }
            }
            
            // Rewrite only for denormalized nodes
            if prop.table_alias.0 == start_cypher_alias && start_is_denormalized {
                // Start node is denormalized → rewrite to t.start_id
                new_prop.table_alias = TableAlias("t".to_string());
                if raw_col != "*" {
                    new_prop.column = Column(PropertyValue::Column("start_id".to_string()));
                }
            } else if prop.table_alias.0 == end_cypher_alias && end_is_denormalized {
                // End node is denormalized → rewrite to t.end_id
                new_prop.table_alias = TableAlias("t".to_string());
                if raw_col != "*" {
                    new_prop.column = Column(PropertyValue::Column("end_id".to_string()));
                }
            }
            // Standard nodes are left unchanged - they'll be resolved by JOINs
            RenderExpr::PropertyAccessExp(new_prop)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let rewritten_operands = op
                .operands
                .iter()
                .map(|operand| {
                    rewrite_expr_for_mixed_denormalized_cte(
                        operand,
                        start_cypher_alias,
                        end_cypher_alias,
                        start_is_denormalized,
                        end_is_denormalized,
                        rel_alias,
                        from_col,
                        to_col,
                        _path_var,
                    )
                })
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            let rewritten_args = fn_call
                .args
                .iter()
                .map(|arg| {
                    rewrite_expr_for_mixed_denormalized_cte(
                        arg,
                        start_cypher_alias,
                        end_cypher_alias,
                        start_is_denormalized,
                        end_is_denormalized,
                        rel_alias,
                        from_col,
                        to_col,
                        _path_var,
                    )
                })
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(),
    }
}
