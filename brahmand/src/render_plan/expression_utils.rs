//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees,
//! including visitor patterns, transformation helpers, and validation functions.

use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderCase, RenderExpr, ScalarFnCall, TableAlias,
};
use super::errors::RenderBuildError;

/// Visitor trait for traversing RenderExpr trees
pub trait RenderExprVisitor<T> {
    /// Visit a RenderExpr and return a result
    fn visit(&mut self, expr: &RenderExpr) -> Result<T, RenderBuildError>;

    /// Visit a list of expressions
    fn visit_all(&mut self, exprs: &[RenderExpr]) -> Result<Vec<T>, RenderBuildError> {
        exprs.iter().map(|expr| self.visit(expr)).collect()
    }
}

/// Common expression visitor that can be extended for specific use cases
pub struct ExpressionVisitor<F, T>
where
    F: Fn(&RenderExpr) -> Result<T, RenderBuildError>,
{
    visitor_fn: F,
}

impl<F, T> ExpressionVisitor<F, T>
where
    F: Fn(&RenderExpr) -> Result<T, RenderBuildError>,
{
    pub fn new(visitor_fn: F) -> Self {
        Self { visitor_fn }
    }
}

impl<F, T> RenderExprVisitor<T> for ExpressionVisitor<F, T>
where
    F: Fn(&RenderExpr) -> Result<T, RenderBuildError>,
{
    fn visit(&mut self, expr: &RenderExpr) -> Result<T, RenderBuildError> {
        (self.visitor_fn)(expr)
    }
}

/// Check if a RenderExpr references a specific table alias
pub fn references_alias(expr: &RenderExpr, alias: &str) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
        RenderExpr::OperatorApplicationExp(op_app) => {
            op_app.operands.iter().any(|op| references_alias(op, alias))
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            fn_call.args.iter().any(|arg| references_alias(arg, alias))
        }
        RenderExpr::AggregateFnCall(agg) => {
            agg.args.iter().any(|arg| references_alias(arg, alias))
        }
        RenderExpr::List(exprs) => exprs.iter().any(|expr| references_alias(expr, alias)),
        RenderExpr::Case(case_expr) => {
            case_expr.when_then.iter().any(|(when, then)| {
                references_alias(when, alias) || references_alias(then, alias)
            }) || case_expr
                .else_expr
                .as_ref()
                .map_or(false, |else_expr| references_alias(else_expr, alias))
        }
        RenderExpr::InSubquery(subquery) => references_alias(&subquery.expr, alias),
        // Simple expressions that don't contain aliases
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => false,
    }
}

/// Extract all table aliases referenced in an expression
pub fn extract_table_aliases(expr: &RenderExpr) -> Vec<String> {
    let mut aliases = Vec::new();

    fn collect_aliases(expr: &RenderExpr, aliases: &mut Vec<String>) {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                aliases.push(prop.table_alias.0.clone());
            }
            RenderExpr::OperatorApplicationExp(op_app) => {
                for operand in &op_app.operands {
                    collect_aliases(operand, aliases);
                }
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                for arg in &fn_call.args {
                    collect_aliases(arg, aliases);
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                for arg in &agg.args {
                    collect_aliases(arg, aliases);
                }
            }
            RenderExpr::List(exprs) => {
                for expr in exprs {
                    collect_aliases(expr, aliases);
                }
            }
            RenderExpr::Case(case_expr) => {
                for (when, then) in &case_expr.when_then {
                    collect_aliases(when, aliases);
                    collect_aliases(then, aliases);
                }
                if let Some(else_expr) = &case_expr.else_expr {
                    collect_aliases(else_expr, aliases);
                }
            }
            RenderExpr::InSubquery(subquery) => {
                collect_aliases(&subquery.expr, aliases);
            }
            // Simple expressions that don't contain aliases
            RenderExpr::Literal(_)
            | RenderExpr::Star
            | RenderExpr::TableAlias(_)
            | RenderExpr::ColumnAlias(_)
            | RenderExpr::Column(_)
            | RenderExpr::Parameter(_) => {}
        }
    }

    collect_aliases(expr, &mut aliases);
    aliases.sort();
    aliases.dedup();
    aliases
}

/// Validate that an expression is well-formed
pub fn validate_expression(expr: &RenderExpr) -> Result<(), RenderBuildError> {
    match expr {
        RenderExpr::OperatorApplicationExp(op_app) => {
            // Validate operator has correct number of operands
            match op_app.operator {
                Operator::And | Operator::Or => {
                    if op_app.operands.len() < 2 {
                        return Err(RenderBuildError::UnsupportedFeature(
                            "Logical operators need at least 2 operands".to_string(),
                        ));
                    }
                }
                Operator::Not => {
                    if op_app.operands.len() != 1 {
                        return Err(RenderBuildError::UnsupportedFeature(
                            "NOT operator needs exactly 1 operand".to_string(),
                        ));
                    }
                }
                _ => {
                    if op_app.operands.len() != 2 {
                        return Err(RenderBuildError::UnsupportedFeature(
                            "NOT operator needs exactly 1 operand".to_string(),
                        ));
                    }
                }
            }

            // Recursively validate operands
            for operand in &op_app.operands {
                validate_expression(operand)?;
            }
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            // Validate function has required arguments
            match fn_call.name.as_str() {
                "map" => {
                    if fn_call.args.len() < 2 || fn_call.args.len() % 2 != 0 {
                        return Err(RenderBuildError::UnsupportedFeature(
                            "map() function requires key-value pairs".to_string(),
                        ));
                    }
                }
                _ => {} // Other functions have variable argument counts
            }

            // Recursively validate arguments
            for arg in &fn_call.args {
                validate_expression(arg)?;
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Validate aggregate function has arguments
            if agg.args.is_empty() {
                return Err(RenderBuildError::UnsupportedFeature(
                    "Aggregate functions require at least one argument".to_string(),
                ));
            }

            // Recursively validate arguments
            for arg in &agg.args {
                validate_expression(arg)?;
            }
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // Validate property access has both table and column
            if prop.table_alias.0.is_empty() || prop.column.0.is_empty() {
                return Err(RenderBuildError::UnsupportedFeature(
                    "Property access requires both table and column".to_string(),
                ));
            }
        }
        RenderExpr::Case(case_expr) => {
            // Validate CASE has at least one WHEN clause
            if case_expr.when_then.is_empty() {
                return Err(RenderBuildError::UnsupportedFeature(
                    "CASE expression requires at least one WHEN clause".to_string(),
                ));
            }

            // Recursively validate all expressions
            for (when, then) in &case_expr.when_then {
                validate_expression(when)?;
                validate_expression(then)?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                validate_expression(else_expr)?;
            }
        }
        RenderExpr::InSubquery(subquery) => {
            validate_expression(&subquery.expr)?;
        }
        RenderExpr::List(exprs) => {
            for expr in exprs {
                validate_expression(expr)?;
            }
        }
        // Simple expressions that are always valid
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => {}
    }

    Ok(())
}

/// Transform expressions by applying a function to each node
pub fn transform_expression<F>(expr: &RenderExpr, transformer: &F) -> Result<RenderExpr, RenderBuildError>
where
    F: Fn(&RenderExpr) -> Result<Option<RenderExpr>, RenderBuildError>,
{
    // Check if transformer wants to modify this node
    if let Some(transformed) = transformer(expr)? {
        return Ok(transformed);
    }

    // Recursively transform children
    match expr {
        RenderExpr::OperatorApplicationExp(op_app) => {
            let transformed_operands = op_app
                .operands
                .iter()
                .map(|op| transform_expression(op, transformer))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op_app.operator.clone(),
                operands: transformed_operands,
            }))
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            let transformed_args = fn_call
                .args
                .iter()
                .map(|arg| transform_expression(arg, transformer))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: transformed_args,
            }))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let transformed_args = agg
                .args
                .iter()
                .map(|arg| transform_expression(arg, transformer))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: transformed_args,
            }))
        }
        RenderExpr::List(exprs) => {
            let transformed_exprs = exprs
                .iter()
                .map(|expr| transform_expression(expr, transformer))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(RenderExpr::List(transformed_exprs))
        }
        RenderExpr::Case(case_expr) => {
            let transformed_when_clauses = case_expr
                .when_then
                .iter()
                .map(|(when, then)| {
                    Ok((
                        transform_expression(when, transformer)?,
                        transform_expression(then, transformer)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let transformed_else = case_expr
                .else_expr
                .as_ref()
                .map(|else_expr| transform_expression(else_expr, transformer))
                .transpose()?;

            Ok(RenderExpr::Case(RenderCase {
                expr: case_expr.expr.clone(),
                when_then: transformed_when_clauses,
                else_expr: transformed_else.map(Box::new),
            }))
        }
        RenderExpr::InSubquery(subquery) => {
            let transformed_expr = transform_expression(&subquery.expr, transformer)?;

            Ok(RenderExpr::InSubquery(InSubquery {
                expr: Box::new(transformed_expr),
                subplan: subquery.subplan.clone(),
            }))
        }
        // Leaf nodes that can't be transformed further
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => Ok(expr.clone()),
        RenderExpr::PropertyAccessExp(_) => Ok(expr.clone()), // Could be transformed if needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_references_alias() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("users".to_string()),
            column: Column("name".to_string()),
        });

        assert!(references_alias(&expr, "users"));
        assert!(!references_alias(&expr, "posts"));
    }

    #[test]
    fn test_extract_table_aliases() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("users".to_string()),
                    column: Column("name".to_string()),
                }),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("posts".to_string()),
                    column: Column("title".to_string()),
                }),
            ],
        });

        let aliases = extract_table_aliases(&expr);
        assert_eq!(aliases, vec!["posts", "users"]);
    }

    #[test]
    fn test_validate_expression_valid() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("users".to_string()),
            column: Column("name".to_string()),
        });

        assert!(validate_expression(&expr).is_ok());
    }

    #[test]
    fn test_validate_expression_invalid_property_access() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("".to_string()),
            column: Column("name".to_string()),
        });

        assert!(validate_expression(&expr).is_err());
    }
}