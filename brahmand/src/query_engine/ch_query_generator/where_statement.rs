use crate::open_cypher_parser::ast::{Expression, Operator, OperatorApplication};

use super::{common::get_literal_to_string, errors::ChQueryGeneratorError};

fn process_where_expression_string(
    expression: Expression,
    is_final_node: bool,
) -> Result<String, ChQueryGeneratorError> {
    match expression {
        Expression::OperatorApplicationExp(op) => {
            let operator_string: String = op.operator.into();

            if op.operands.len() == 1 {
                // it could be unary or postfix
                // e.g unary = Not, Distinct & postfix = IsNull, IsNotNull
                // e.g distinct name |  e.g. city IS NULL

                let operand = op
                    .operands
                    .first()
                    .ok_or(ChQueryGeneratorError::NoOperandFoundInWhereClause)?;
                let operand_string =
                    process_where_expression_string(operand.clone(), is_final_node)?;

                if op.operator == Operator::Distinct || op.operator == Operator::Not {
                    // process unary
                    let condition_string = format!("{} {}", operator_string, operand_string);
                    return Ok(condition_string);
                }
                // else if op.operator == Operator::IsNull || op.operator == Operator::IsNotNull {
                // process postfix
                let condition_string = format!("{} {}", operand_string, operator_string);
                return Ok(condition_string);
                // }
            }

            let first_operand = op
                .operands
                .first()
                .ok_or(ChQueryGeneratorError::NoOperandFoundInWhereClause)?;
            let first_operand_string =
                process_where_expression_string(first_operand.clone(), is_final_node)?;

            let second_operand = op
                .operands
                .get(1)
                .ok_or(ChQueryGeneratorError::NoOperandFoundInWhereClause)?;
            let second_operand_string =
                process_where_expression_string(second_operand.clone(), is_final_node)?;

            let condition_string = format!(
                "{} {} {}",
                first_operand_string, operator_string, second_operand_string
            );
            Ok(condition_string)
        }
        Expression::Literal(literal) => Ok(get_literal_to_string(&literal)),
        Expression::List(expressions) => {
            let mut new_exprs = Vec::new();
            for sub_expr in expressions {
                let new_expr = process_where_expression_string(sub_expr, is_final_node)?;
                new_exprs.push(new_expr);
            }
            let list_string = format!("[{}]", new_exprs.join(","));
            Ok(list_string)
        }
        Expression::FunctionCallExp(fn_call) => {
            let mut new_args = Vec::new();
            for arg in fn_call.args {
                let new_expr = process_where_expression_string(arg, is_final_node)?;
                new_args.push(new_expr);
            }
            let fn_call_string = format!("{}({})", fn_call.name, new_args.join(","));
            Ok(fn_call_string)
        }
        Expression::PropertyAccessExp(property_access) => {
            if is_final_node {
                Ok(format!("{}.{}", property_access.base, property_access.key))
            } else {
                Ok(property_access.key.to_string())
            }
        }
        // variables are usually column names
        Expression::Variable(var) => Ok(var.to_string()),
        _ => Err(ChQueryGeneratorError::UnsupportedItemInWhereClause), // Expression::Parameter(_) => todo!(),
                                                                       // Expression::PathPattern(path_pattern) => todo!(),
    }
}

pub fn generate_where_statements(
    where_conditions: Vec<OperatorApplication>,
    is_final_node: bool,
) -> Result<String, ChQueryGeneratorError> {
    // println!("\n where_conditions {:?} \n ", where_conditions);

    let mut where_condition_strings: Vec<String> = vec![];

    for where_condition in where_conditions {
        let where_condition_string = process_where_expression_string(
            Expression::OperatorApplicationExp(where_condition),
            is_final_node,
        )?;
        where_condition_strings.push(where_condition_string);
    }

    if !where_condition_strings.is_empty() {
        Ok(format!("WHERE {}", where_condition_strings.join(" AND ")))
    } else {
        Ok("".to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{
        FunctionCall, Literal, OperatorApplication, PropertyAccess,
    };

    use super::*;

    // Helper to build an OperatorApplicationExp
    fn op_app(op: Operator, operands: Vec<Expression>) -> Expression {
        Expression::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands,
        })
    }

    // Helper to build a FunctionCallExp
    fn fn_call<'a>(name: &'a str, args: Vec<Expression<'a>>) -> Expression<'a> {
        Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args,
        })
    }

    // process_where_expression_string

    #[test]
    fn literal_integer_in_where() {
        let expr = Expression::Literal(Literal::Integer(42));
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn list_expression() {
        let expr = Expression::List(vec![
            Expression::Literal(Literal::Integer(1)),
            Expression::Literal(Literal::Integer(2)),
        ]);
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn function_call_in_where() {
        let expr = fn_call("max", vec![Expression::Literal(Literal::Integer(5))]);
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "max(5)");
    }

    #[test]
    fn binary_operator_in_where() {
        let expr = op_app(
            Operator::Equal,
            vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(2)),
            ],
        );
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "1 = 2");
    }

    #[test]
    fn unary_not() {
        let expr = op_app(Operator::Not, vec![Expression::Variable("x")]);
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "NOT x");
    }

    #[test]
    fn postfix_is_null_in_where() {
        let expr = op_app(Operator::IsNull, vec![Expression::Variable("city")]);
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "city IS NULL");
    }

    #[test]
    fn property_access_non_final_in_where() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "k");
    }

    #[test]
    fn property_access_final_in_where() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let out = process_where_expression_string(expr, true).unwrap();
        assert_eq!(out, "n.k");
    }

    #[test]
    fn variable_expression() {
        let expr = Expression::Variable("v");
        let out = process_where_expression_string(expr, false).unwrap();
        assert_eq!(out, "v");
    }

    #[test]
    fn unsupported_expression() {
        let expr = Expression::Parameter("p");
        let err = process_where_expression_string(expr, false).unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInWhereClause
        ));
    }

    #[test]
    fn generate_where_empty() {
        let sql = generate_where_statements(vec![], false).unwrap();
        assert_eq!(sql, "");
    }

    #[test]
    fn generate_where_multiple_conditions() {
        let cond1 = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(1)),
            ],
        };
        let cond2 = OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                Expression::Variable("a"),
                Expression::Literal(Literal::Integer(5)),
            ],
        };
        let sql = generate_where_statements(vec![cond1.clone(), cond2.clone()], false).unwrap();
        assert_eq!(sql, "WHERE 1 = 1 AND a > 5");
    }
}
