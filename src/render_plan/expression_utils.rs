//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees.

use super::render_expr::{RenderExpr, TableAlias};

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
        RenderExpr::MapLiteral(entries) => entries.iter().any(|(_, v)| references_alias(v, alias)),
        // ArraySubscript may contain aliases in array or index
        RenderExpr::ArraySubscript { array, index } => {
            references_alias(array, alias) || references_alias(index, alias)
        }
    }
}

/// Rewrite table aliases in a RenderExpr according to a mapping
/// Used to translate Cypher aliases to VLP internal aliases
pub fn rewrite_aliases(
    expr: &mut RenderExpr,
    alias_map: &std::collections::HashMap<String, String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            if let Some(new_alias) = alias_map.get(&prop.table_alias.0) {
                log::debug!("🔄 Rewriting alias '{}' → '{}'", prop.table_alias.0, new_alias);
                prop.table_alias = TableAlias(new_alias.clone());
            }
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            for operand in &mut op_app.operands {
                rewrite_aliases(operand, alias_map);
            }
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            for arg in &mut fn_call.args {
                rewrite_aliases(arg, alias_map);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                rewrite_aliases(arg, alias_map);
            }
        }
        RenderExpr::List(exprs) => {
            for expr in exprs {
                rewrite_aliases(expr, alias_map);
            }
        }
        RenderExpr::Case(case_expr) => {
            for (when, then) in &mut case_expr.when_then {
                rewrite_aliases(when, alias_map);
                rewrite_aliases(then, alias_map);
            }
            if let Some(else_expr) = &mut case_expr.else_expr {
                rewrite_aliases(else_expr, alias_map);
            }
        }
        RenderExpr::InSubquery(subquery) => {
            rewrite_aliases(&mut subquery.expr, alias_map);
        }
        RenderExpr::ReduceExpr(reduce) => {
            rewrite_aliases(&mut reduce.initial_value, alias_map);
            rewrite_aliases(&mut reduce.list, alias_map);
            rewrite_aliases(&mut reduce.expression, alias_map);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                rewrite_aliases(v, alias_map);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            rewrite_aliases(array, alias_map);
            rewrite_aliases(index, alias_map);
        }
        // Simple expressions that don't contain aliases - no rewriting needed
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{Column, PropertyAccess, TableAlias};

    #[test]
    fn test_references_alias() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("users".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
        });

        assert!(references_alias(&expr, "users"));
        assert!(!references_alias(&expr, "posts"));
    }
}
