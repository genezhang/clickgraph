use super::expression_utils::references_alias;
use super::render_expr::{
    AggregateFnCall, Column, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr,
    ScalarFnCall, TableAlias,
};

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

        println!("DEBUG: Categorizing predicate: {:?}", predicate);
        println!(
            "DEBUG: refs_start (alias '{}'): {}",
            start_cypher_alias, refs_start
        );
        println!(
            "DEBUG: refs_end (alias '{}'): {}",
            end_cypher_alias, refs_end
        );
        println!("DEBUG: has_path_fn: {}", has_path_fn);

        if has_path_fn {
            // Path function filters (e.g., WHERE length(p) <= 3) go in path function filters
            println!("DEBUG: Going to path_fn_filters");
            path_fn_filters.push(predicate);
        } else if refs_start && refs_end {
            // Filter references both nodes - can't categorize simply
            // For now, treat as start filter (will be in base case)
            println!("DEBUG: Going to start_filters (refs both)");
            start_filters.push(predicate);
        } else if refs_start {
            println!("DEBUG: Going to start_filters");
            start_filters.push(predicate);
        } else if refs_end {
            println!("DEBUG: Going to end_filters");
            end_filters.push(predicate);
        } else {
            // Doesn't reference nodes - might be relationship filter or constant
            println!("DEBUG: Going to rel_filters");
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

/// Extract start and end filters from a filter expression
pub fn extract_start_end_filters(
    filter_expr: &RenderExpr,
    left_alias: &str,
    right_alias: &str,
) -> (Option<String>, Option<String>, Option<RenderExpr>) {
    match filter_expr {
        RenderExpr::OperatorApplicationExp(op_app) if op_app.operator == Operator::And => {
            // AND expression - check each operand
            let mut start_filters = vec![];
            let mut end_filters = vec![];
            let mut other_filters = vec![];

            for operand in &op_app.operands {
                let (start_f, end_f, other_f) =
                    extract_start_end_filters(operand, left_alias, right_alias);
                if let Some(sf) = start_f {
                    start_filters.push(sf);
                }
                if let Some(ef) = end_f {
                    end_filters.push(ef);
                }
                if let Some(of) = other_f {
                    other_filters.push(of);
                }
            }

            let start_filter = if start_filters.is_empty() {
                None
            } else {
                Some(start_filters.join(" AND "))
            };
            let end_filter = if end_filters.is_empty() {
                None
            } else {
                Some(end_filters.join(" AND "))
            };
            let remaining_filter = if other_filters.is_empty() {
                None
            } else if other_filters.len() == 1 {
                Some(other_filters.into_iter().next().unwrap())
            } else {
                Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: other_filters,
                }))
            };

            (start_filter, end_filter, remaining_filter)
        }
        _ => {
            // Check if this filter references start or end node
            if references_alias(filter_expr, left_alias) {
                // Convert to SQL string for start filter
                (
                    Some(filter_expr_to_sql(filter_expr, left_alias, "start_")),
                    None,
                    None,
                )
            } else if references_alias(filter_expr, right_alias) {
                // Convert to SQL string for end filter
                (
                    None,
                    Some(filter_expr_to_sql(filter_expr, right_alias, "end_")),
                    None,
                )
            } else {
                // Keep as general filter
                (None, None, Some(filter_expr.clone()))
            }
        }
    }
}

/// Convert a filter expression to SQL string for CTE filters
pub fn filter_expr_to_sql(expr: &RenderExpr, alias: &str, prefix: &str) -> String {
    match expr {
        RenderExpr::OperatorApplicationExp(op_app)
            if op_app.operator == Operator::Equal && op_app.operands.len() == 2 =>
        {
            if let (RenderExpr::PropertyAccessExp(prop), RenderExpr::Literal(lit)) =
                (&op_app.operands[0], &op_app.operands[1])
            {
                if prop.table_alias.0 == alias {
                    let column = format!("{}{}", prefix, prop.column.0);
                    match lit {
                        Literal::String(s) => format!("{} = '{}'", column, s),
                        Literal::Integer(n) => format!("{} = {}", column, n),
                        Literal::Float(f) => format!("{} = {}", column, f),
                        _ => "true".to_string(), // fallback
                    }
                } else {
                    "true".to_string() // fallback
                }
            } else {
                "true".to_string() // fallback
            }
        }
        _ => "true".to_string(), // fallback for complex expressions
    }
}

/// Convert RenderExpr to SQL string for end node filters in outer CTE
/// Maps Cypher aliases to database column references (e.g., "a.name" -> "end_node.full_name")
pub fn render_end_filter_to_column_alias(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    start_node_label: &str,
    end_node_label: &str,
) -> String {
    println!(
        "DEBUG: render_end_filter_to_column_alias called with start_cypher_alias='{}', end_cypher_alias='{}'",
        start_cypher_alias, end_cypher_alias
    );

    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column.0;
            println!(
                "DEBUG: PropertyAccessExp: table_alias='{}', column='{}', start_cypher_alias='{}', end_cypher_alias='{}'",
                table_alias, column, start_cypher_alias, end_cypher_alias
            );

            // Map Cypher aliases to database column references
            if table_alias == end_cypher_alias {
                let mapped_column = super::cte_generation::map_property_to_column_with_schema(
                    column,
                    end_node_label,
                )
                .unwrap_or_else(|_| column.to_string());
                let result = format!("end_node.{}", mapped_column);
                println!("DEBUG: Mapped to end column reference: {}", result);
                result
            } else if table_alias == start_cypher_alias {
                let mapped_column = super::cte_generation::map_property_to_column_with_schema(
                    column,
                    start_node_label,
                )
                .unwrap_or_else(|_| column.to_string());
                let result = format!("start_node.{}", mapped_column);
                println!("DEBUG: Mapped to start column reference: {}", result);
                result
            } else {
                // Fallback: use as-is
                let result = format!("{}.{}", table_alias, column);
                println!("DEBUG: Fallback: {}", result);
                result
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

            if op.operands.len() == 1 {
                format!(
                    "{} {}",
                    operator_sql,
                    render_end_filter_to_column_alias(
                        &op.operands[0],
                        start_cypher_alias,
                        end_cypher_alias,
                        start_node_label,
                        end_node_label
                    )
                )
            } else if op.operands.len() == 2 {
                format!(
                    "{} {} {}",
                    render_end_filter_to_column_alias(
                        &op.operands[0],
                        start_cypher_alias,
                        end_cypher_alias,
                        start_node_label,
                        end_node_label
                    ),
                    operator_sql,
                    render_end_filter_to_column_alias(
                        &op.operands[1],
                        start_cypher_alias,
                        end_cypher_alias,
                        start_node_label,
                        end_node_label
                    )
                )
            } else {
                // Fallback for complex expressions
                format!(
                    "{}({})",
                    operator_sql,
                    op.operands
                        .iter()
                        .map(|operand| render_end_filter_to_column_alias(
                            operand,
                            start_cypher_alias,
                            end_cypher_alias,
                            start_node_label,
                            end_node_label
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
        RenderExpr::Literal(lit) => match lit {
            Literal::String(s) => format!("'{}'", s),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
            _ => "NULL".to_string(),
        },
        RenderExpr::ScalarFnCall(fn_call) => {
            let args_sql = fn_call
                .args
                .iter()
                .map(|arg| {
                    render_end_filter_to_column_alias(
                        arg,
                        start_cypher_alias,
                        end_cypher_alias,
                        start_node_label,
                        end_node_label,
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", fn_call.name, args_sql)
        }
        _ => {
            println!("DEBUG: Unhandled RenderExpr type: {:?}", expr);
            "true".to_string() // Fallback
        }
    }
}

/// Rewrite end filters for variable-length CTE context
pub fn rewrite_end_filters_for_variable_length_cte(
    expr: &RenderExpr,
    cte_table_alias: &str,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column.0;

            // Map Cypher aliases to CTE column references
            if table_alias == end_cypher_alias {
                // b.name -> t.end_name
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_table_alias.to_string()),
                    column: Column(format!("end_{}", column)),
                })
            } else if table_alias == start_cypher_alias {
                // a.name -> t.start_name
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_table_alias.to_string()),
                    column: Column(format!("start_{}", column)),
                })
            } else {
                // Fallback: keep as-is
                expr.clone()
            }
        }
        RenderExpr::TableAlias(alias) => expr.clone(),
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: op
                    .operands
                    .iter()
                    .map(|operand| {
                        rewrite_end_filters_for_variable_length_cte(
                            operand,
                            cte_table_alias,
                            start_cypher_alias,
                            end_cypher_alias,
                        )
                    })
                    .collect(),
            })
        }
        _ => expr.clone(), // Literals, etc. stay the same
    }
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

/// Rewrite expressions for outer query context in variable-length paths
/// Maps Cypher aliases to table aliases (start_node, end_node)
pub fn rewrite_expr_for_outer_query(
    expr: &RenderExpr,
    left_alias: &str,
    right_alias: &str,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop_access) => {
            let node_alias = &prop_access.table_alias.0;
            let property = &prop_access.column.0;

            // Check if this is referencing the left or right node
            if node_alias == left_alias {
                // Left node reference -> start_node
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("start_node".to_string()),
                    column: Column(property.clone()),
                });
            } else if node_alias == right_alias {
                // Right node reference -> end_node
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("end_node".to_string()),
                    column: Column(property.clone()),
                });
            }
            expr.clone()
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands = op
                .operands
                .iter()
                .map(|operand| rewrite_expr_for_outer_query(operand, left_alias, right_alias))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            // Recursively rewrite function arguments
            let rewritten_args = fn_call
                .args
                .iter()
                .map(|arg| rewrite_expr_for_outer_query(arg, left_alias, right_alias))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(),
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
                if prop.column.0 == "*" {
                    new_prop.column = prop.column.clone();
                } else {
                    new_prop.column = Column(format!("start_{}", prop.column.0));
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
