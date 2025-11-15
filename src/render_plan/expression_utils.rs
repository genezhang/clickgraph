//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees.

use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderCase, RenderExpr, ScalarFnCall, TableAlias,
};
use super::errors::RenderBuildError;

/// Check if a RenderExpr references a specific table alias (USED by tests)
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
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => false,
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