use super::errors::ClickhouseQueryGeneratorError;
use super::function_translator::translate_scalar_function;
use crate::query_planner::logical_expr::{
    Literal, LogicalExpr, Operator,
};
use crate::query_planner::logical_plan::LogicalPlan;
use std::sync::Arc;

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal_logical(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Literal(Literal::String(_)) => true,
        LogicalExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(|o| contains_string_literal_logical(o))
        }
        _ => false,
    }
}

/// Check if any operand in the expression is a string literal
fn has_string_operand_logical(operands: &[LogicalExpr]) -> bool {
    operands.iter().any(|op| contains_string_literal_logical(op))
}

/// Flatten nested + operations into a list of SQL strings for concat()
fn flatten_addition_operands_logical(expr: &LogicalExpr) -> Result<Vec<String>, ClickhouseQueryGeneratorError> {
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
                Ok(format!("{}({})", fn_call.name, args_sql?.join(", ")))
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                // Use function translator for Neo4j -> ClickHouse mapping
                translate_scalar_function(fn_call)
            }
            LogicalExpr::PropertyAccessExp(prop) => {
                // PropertyValue already knows if it's an expression or simple column
                // Use its to_sql() method which handles both cases efficiently
                Ok(prop.column.to_sql(&prop.table_alias.0))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
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
                            let flattened: Vec<String> = op.operands.iter()
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
                    Operator::In => Ok(format!("({} IN {})", operands_sql[0], operands_sql[1])),
                    Operator::NotIn => {
                        Ok(format!("({} NOT IN {})", operands_sql[0], operands_sql[1]))
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
