//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees.

use super::render_expr::RenderExpr;

/// Check if a RenderExpr references a specific table alias
/// Used by tests for validation
#[allow(dead_code)]
pub fn references_alias(expr: &RenderExpr, alias: &str) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
        RenderExpr::OperatorApplicationExp(op_app) => {
            op_app.operands.iter().any(|op| references_alias(op, alias))
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            fn_call.args.iter().any(|arg| references_alias(arg, alias))
        }
        RenderExpr::AggregateFnCall(agg) => agg.args.iter().any(|arg| references_alias(arg, alias)),
        RenderExpr::List(exprs) => exprs.iter().any(|expr| references_alias(expr, alias)),
        RenderExpr::Case(case_expr) => {
            case_expr
                .when_then
                .iter()
                .any(|(when, then)| references_alias(when, alias) || references_alias(then, alias))
                || case_expr
                    .else_expr
                    .as_ref()
                    .map_or(false, |else_expr| references_alias(else_expr, alias))
        }
        RenderExpr::InSubquery(subquery) => references_alias(&subquery.expr, alias),
        // EXISTS subqueries don't reference aliases in the outer scope directly
        RenderExpr::ExistsSubquery(_) => false,
        // PatternCount is a self-contained subquery, no outer alias references
        RenderExpr::PatternCount(_) => false,
        // ReduceExpr may contain aliases in its sub-expressions
        RenderExpr::ReduceExpr(reduce) => {
            references_alias(&reduce.initial_value, alias)
                || references_alias(&reduce.list, alias)
                || references_alias(&reduce.expression, alias)
        }
        // Simple expressions that don't contain aliases
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => false,
        // MapLiteral may contain aliases in its values
        RenderExpr::MapLiteral(entries) => {
            entries.iter().any(|(_, v)| references_alias(v, alias))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{PropertyAccess, TableAlias, Column};

    #[test]
    fn test_references_alias() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("users".to_string()),
            column: Column(crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string())),
        });

        assert!(references_alias(&expr, "users"));
        assert!(!references_alias(&expr, "posts"));
    }
}
