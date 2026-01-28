use super::errors::ClickhouseQueryGeneratorError;
use super::function_registry::get_function_mapping;
use super::function_translator::translate_scalar_function;
use crate::query_planner::logical_expr::{Literal, LogicalExpr, Operator};
use crate::query_planner::logical_plan::LogicalPlan;
use std::sync::Arc;

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal_logical(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Literal(Literal::String(_)) => true,
        LogicalExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .any(|o| contains_string_literal_logical(o)),
        _ => false,
    }
}

/// Check if any operand in the expression is a string literal
fn has_string_operand_logical(operands: &[LogicalExpr]) -> bool {
    operands
        .iter()
        .any(|op| contains_string_literal_logical(op))
}

/// Flatten nested + operations into a list of SQL strings for concat()
fn flatten_addition_operands_logical(
    expr: &LogicalExpr,
) -> Result<Vec<String>, ClickhouseQueryGeneratorError> {
    match expr {
        LogicalExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            let mut result = Vec::new();
            for operand in &op.operands {
                result.extend(flatten_addition_operands_logical(operand)?);
            }
            Ok(result)
        }
        _ => {
            // Use the ToSql trait impl for LogicalExpr
            Ok(vec![ToSql::to_sql(expr)?])
        }
    }
}

/// Convert a plan node to SQL
#[allow(dead_code)]
pub trait ToSql {
    /// Convert this node to a SQL string
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError>;
}

impl ToSql for Arc<LogicalPlan> {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        self.as_ref().to_sql()
    }
}

impl ToSql for LogicalExpr {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        match self {
            LogicalExpr::Literal(lit) => match lit {
                Literal::Integer(i) => Ok(i.to_string()),
                Literal::Float(f) => Ok(f.to_string()),
                Literal::Boolean(b) => Ok(b.to_string()),
                Literal::String(s) => Ok(format!("'{}'", s)),
                Literal::Null => Ok("NULL".to_string()),
            },
            LogicalExpr::Raw(raw) => Ok(raw.clone()),
            LogicalExpr::Star => Ok("*".into()),
            LogicalExpr::TableAlias(alias) => Ok(alias.0.clone()),
            LogicalExpr::ColumnAlias(col) => Ok(col.0.clone()),
            LogicalExpr::Column(col) => Ok(col.0.clone()),
            LogicalExpr::Parameter(param) => Ok(format!("${}", param)),
            LogicalExpr::List(items) => {
                // Use array syntax [...] for Cypher compatibility
                // ClickHouse arrays support = comparison and work with mixed types via tuple()
                let items_sql: Result<Vec<String>, _> = items.iter().map(|e| e.to_sql()).collect();
                // Use tuple() for comparison to handle mixed types (date, int, string, etc.)
                Ok(format!("tuple({})", items_sql?.join(", ")))
            }
            LogicalExpr::AggregateFnCall(fn_call) => {
                let args_sql: Result<Vec<String>, _> =
                    fn_call.args.iter().map(|e| e.to_sql()).collect();
                let args_sql = args_sql?;

                // Use function registry to translate Neo4j -> ClickHouse function names
                // Note: We use ClickHouse-specific aggregate names (like "anyLast") to avoid
                // conflicts with Cypher functions (like "any" for array predicates)
                let fn_name_lower = fn_call.name.to_lowercase();
                if let Some(mapping) = get_function_mapping(&fn_name_lower) {
                    // Apply argument transformation if provided
                    let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                        transform_fn(&args_sql)
                    } else {
                        args_sql
                    };
                    Ok(format!(
                        "{}({})",
                        mapping.clickhouse_name,
                        transformed_args.join(", ")
                    ))
                } else {
                    // No mapping, use name directly (standard SQL functions like count, sum, etc.)
                    Ok(format!("{}({})", fn_call.name, args_sql.join(", ")))
                }
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                // Use function translator for Neo4j -> ClickHouse mapping
                translate_scalar_function(fn_call)
            }
            LogicalExpr::PropertyAccessExp(prop) => {
                // Property has been resolved from schema during query planning.
                // PropertyValue already contains the correct mapping:
                // - Column(_): Direct column mapping (e.g., year: Year)
                // - Expression(_): Expression mapping (e.g., year: toYear(FlightDate))
                // Just render it as-is.
                Ok(prop.column.to_sql(&prop.table_alias.0))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // ⚠️ TODO: Operator rendering consolidation (Phase 3)
                // This code is duplicated in to_sql_query.rs with very similar operator handling.
                // Root cause: Two different Operator types (logical_expr::Operator vs render_expr::Operator)
                // prevent simple consolidation. Phase 3 strategy:
                // 1. Create OperatorRenderer trait with operator_symbol() and render_special_cases()
                // 2. Implement trait for LogicalExpr operators (in this module)
                // 3. Implement trait for RenderExpr operators (in to_sql_query.rs)
                // 4. Create unified render_operator() function in common.rs
                // See notes/OPERATOR_RENDERING_ANALYSIS.md for detailed analysis.
                // Estimated effort: 4-6 hours for full consolidation
                let operands_sql: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Result<Vec<String>, _>>()?;
                match op.operator {
                    Operator::Addition => {
                        // Use concat() for string concatenation, + for numeric
                        // Flatten nested + operations for cases like: a + ' - ' + b
                        if has_string_operand_logical(&op.operands) {
                            let flattened: Vec<String> = op
                                .operands
                                .iter()
                                .map(|o| flatten_addition_operands_logical(o))
                                .collect::<Result<Vec<Vec<String>>, _>>()?
                                .into_iter()
                                .flatten()
                                .collect();
                            Ok(format!("concat({})", flattened.join(", ")))
                        } else {
                            Ok(format!("({} + {})", operands_sql[0], operands_sql[1]))
                        }
                    }
                    Operator::Subtraction => {
                        Ok(format!("({} - {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::Multiplication => {
                        Ok(format!("({} * {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::Division => {
                        Ok(format!("({} / {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::ModuloDivision => {
                        Ok(format!("({} % {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::Exponentiation => {
                        Ok(format!("power({}, {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::Equal => Ok(format!("({} = {})", operands_sql[0], operands_sql[1])),
                    Operator::NotEqual => {
                        Ok(format!("({} != {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::LessThan => {
                        Ok(format!("({} < {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::GreaterThan => {
                        Ok(format!("({} > {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::LessThanEqual => {
                        Ok(format!("({} <= {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::GreaterThanEqual => {
                        Ok(format!("({} >= {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::RegexMatch => {
                        // ClickHouse uses match() function for regex matching
                        Ok(format!("match({}, {})", operands_sql[0], operands_sql[1]))
                    }
                    Operator::And => Ok(format!("({} AND {})", operands_sql[0], operands_sql[1])),
                    Operator::Or => Ok(format!("({} OR {})", operands_sql[0], operands_sql[1])),
                    Operator::In => {
                        // Check if right operand is a property access (array column) vs literal list
                        // Cypher: x IN array_property → ClickHouse: has(array, x)
                        // Cypher: x IN [1, 2, 3] → ClickHouse: x IN (1, 2, 3)
                        if matches!(&op.operands[1], LogicalExpr::PropertyAccessExp(_)) {
                            // Array column membership: use has(array, value)
                            Ok(format!("has({}, {})", operands_sql[1], operands_sql[0]))
                        } else {
                            // Literal list: use standard IN
                            Ok(format!("({} IN {})", operands_sql[0], operands_sql[1]))
                        }
                    }
                    Operator::NotIn => {
                        // Same logic for NOT IN
                        if matches!(&op.operands[1], LogicalExpr::PropertyAccessExp(_)) {
                            // Array column: NOT has(array, value)
                            Ok(format!("NOT has({}, {})", operands_sql[1], operands_sql[0]))
                        } else {
                            // Literal list: standard NOT IN
                            Ok(format!("({} NOT IN {})", operands_sql[0], operands_sql[1]))
                        }
                    }
                    Operator::StartsWith => {
                        // ClickHouse: startsWith(haystack, prefix)
                        Ok(format!(
                            "startsWith({}, {})",
                            operands_sql[0], operands_sql[1]
                        ))
                    }
                    Operator::EndsWith => {
                        // ClickHouse: endsWith(haystack, suffix)
                        Ok(format!(
                            "endsWith({}, {})",
                            operands_sql[0], operands_sql[1]
                        ))
                    }
                    Operator::Contains => {
                        // ClickHouse: position(haystack, needle) > 0 or like(haystack, '%needle%')
                        // Using position() for efficiency
                        Ok(format!(
                            "(position({}, {}) > 0)",
                            operands_sql[0], operands_sql[1]
                        ))
                    }
                    Operator::Not => Ok(format!("NOT ({})", operands_sql[0])),
                    Operator::Distinct => Ok(format!("DISTINCT {}", operands_sql[0])),
                    Operator::IsNull => Ok(format!("({} IS NULL)", operands_sql[0])),
                    Operator::IsNotNull => Ok(format!("({} IS NOT NULL)", operands_sql[0])),
                }
            }
            LogicalExpr::Case(case_expr) => {
                let mut sql = String::from("CASE ");

                // Simple CASE (CASE x WHEN ...)
                if let Some(expr) = &case_expr.expr {
                    sql.push_str(&format!("{} ", expr.to_sql()?));
                }

                // WHEN ... THEN ... clauses
                for (when_expr, then_expr) in &case_expr.when_then {
                    sql.push_str(&format!(
                        "WHEN {} THEN {} ",
                        when_expr.to_sql()?,
                        then_expr.to_sql()?
                    ));
                }

                // ELSE clause
                if let Some(else_expr) = &case_expr.else_expr {
                    sql.push_str(&format!("ELSE {} ", else_expr.to_sql()?));
                }

                sql.push_str("END");
                Ok(sql)
            }
            LogicalExpr::InSubquery(in_subquery) => {
                let expr_sql = in_subquery.expr.to_sql()?;
                let subquery_sql = in_subquery.subplan.to_sql()?;
                Ok(format!("{} IN ({})", expr_sql, subquery_sql))
            }
            LogicalExpr::ExistsSubquery(exists_subquery) => {
                // Generate EXISTS (SELECT 1 FROM ... WHERE ...)
                let subquery_sql = exists_subquery.subplan.to_sql()?;
                Ok(format!("EXISTS ({})", subquery_sql))
            }
            LogicalExpr::ReduceExpr(reduce) => {
                // Convert Cypher reduce(acc = init, x IN list | expr) to ClickHouse arrayFold
                // ClickHouse syntax: arrayFold((acc, x) -> expr, list, init)
                // Note: Cast init to Int64 to avoid type mismatch issues when the lambda
                // expression returns a wider type than the inferred initial value type
                let init_sql = reduce.initial_value.to_sql()?;
                let list_sql = reduce.list.to_sql()?;
                let expr_sql = reduce.expression.to_sql()?;

                // Wrap numeric init values in toInt64() to prevent type mismatch
                let init_cast = if matches!(
                    *reduce.initial_value,
                    LogicalExpr::Literal(Literal::Integer(_))
                ) {
                    format!("toInt64({})", init_sql)
                } else {
                    init_sql
                };

                Ok(format!(
                    "arrayFold({}, {} -> {}, {}, {})",
                    reduce.variable, reduce.accumulator, expr_sql, list_sql, init_cast
                ))
            }
            LogicalExpr::MapLiteral(entries) => {
                // Map literals are handled specially by function translator
                // If we reach here directly, just format as key-value pairs for debugging
                // In practice, duration({days: 5}) is handled by translate_scalar_function
                let pairs: Result<Vec<String>, _> = entries
                    .iter()
                    .map(|(k, v)| {
                        let val_sql = v.to_sql()?;
                        Ok(format!("'{}': {}", k, val_sql))
                    })
                    .collect();
                Ok(format!("{{{}}}", pairs?.join(", ")))
            }
            LogicalExpr::LabelExpression { variable, label } => {
                // Label expression should typically be resolved at planning time
                // If we reach here, it means we couldn't determine the label statically
                // Generate a fallback that returns false (unknown label)
                // In a more sophisticated implementation, this could query a type column
                log::warn!(
                    "LabelExpression {}:{} reached SQL generation - returning false",
                    variable,
                    label
                );
                Ok("false".to_string())
            }
            LogicalExpr::PathPattern(_) => {
                // Path patterns are handled at the logical plan level, not expression level
                Err(ClickhouseQueryGeneratorError::UnsupportedItemInWhereClause)
            }
            _ => todo!("Not yet implemented"),
        }
    }
}

impl ToSql for LogicalPlan {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        match self {
            LogicalPlan::ViewScan(scan) => {
                // Add FINAL keyword if enabled
                let final_keyword = if scan.use_final { " FINAL" } else { "" };
                let mut sql = format!("SELECT * FROM {}{}", scan.source_table, final_keyword);

                // Add WHERE clause if view_filter is present
                if let Some(ref filter) = scan.view_filter {
                    sql.push_str(" WHERE ");
                    sql.push_str(&filter.to_sql()?);
                }
                Ok(sql)
            }
            LogicalPlan::Projection(proj) => {
                let mut sql = String::new();
                sql.push_str("SELECT ");

                let mut projections = Vec::new();
                for item in &proj.items {
                    projections.push(item.expression.to_sql()?);
                }
                sql.push_str(&projections.join(", "));

                sql.push_str("\nFROM ");
                sql.push_str(&proj.input.to_sql()?);

                Ok(sql)
            }
            _ => Err(ClickhouseQueryGeneratorError::UnsupportedDDLQuery),
        }
    }
}
