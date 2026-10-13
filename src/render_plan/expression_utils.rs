//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees, including
//! a visitor trait for implementing expression transformations.

use super::render_expr::{Literal, Operator, RenderExpr};

/// Check if a RenderExpr references a specific table alias.
/// Used by the #462 post-WITH OPTIONAL restructure to route cross-alias WHERE
/// conjuncts into the LEFT JOIN ON condition, and by tests for validation.
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
            // #576: a simple CASE (`CASE x WHEN … `) references its scrutinee
            // `case_expr.expr`; missing it made an alias used ONLY there look
            // unreferenced, so remove_unreferenced_joins pruned its join and the
            // scrutinee then referenced a dropped table (Code 47).
            case_expr
                .expr
                .as_ref()
                .is_some_and(|scrutinee| references_alias(scrutinee, alias))
                || case_expr.when_then.iter().any(|(when, then)| {
                    references_alias(when, alias) || references_alias(then, alias)
                })
                || case_expr
                    .else_expr
                    .as_ref()
                    .is_some_and(|else_expr| references_alias(else_expr, alias))
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
        // Raw expressions may contain alias references as text (e.g., "(country.type = 'Country')")
        RenderExpr::Raw(raw) => {
            raw.contains(&format!("{}.", alias)) || raw.contains(&format!("{} ", alias))
        }
        // TableAlias may directly reference an alias
        RenderExpr::TableAlias(ta) => ta.0 == alias,
        // Simple expressions that don't contain aliases
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => false,
        // MapLiteral may contain aliases in its values
        RenderExpr::MapLiteral(entries) => entries.iter().any(|(_, v)| references_alias(v, alias)),
        // ArraySubscript may contain aliases in array or index
        RenderExpr::ArraySubscript { array, index } => {
            references_alias(array, alias) || references_alias(index, alias)
        }
        // ArraySlicing may contain aliases in array, from, and to
        RenderExpr::ArraySlicing { array, from, to } => {
            references_alias(array, alias)
                || from.as_ref().is_some_and(|f| references_alias(f, alias))
                || to.as_ref().is_some_and(|t| references_alias(t, alias))
        }
        // CteEntityRef doesn't reference aliases directly - it references CTE columns
        RenderExpr::CteEntityRef(_) => false,
    }
}

/// Check if a render expression is a string-valued operand of a Cypher `+` (so
/// the `+` should render as `concat(...)`). Detects a bare string literal (fast
/// path, recursing through nested `+`) AND any operand the render-site classifier
/// proves is String-typed — a string-returning function call, or a
/// schema-declared string column (#871). Mirrors `is_string_operand` in the
/// clickhouse emitter (`to_sql_query.rs`); both must stay in lockstep so string
/// `+` renders as concat on every path (this copy backs the VLP bound-node /
/// relationship WHERE-filter path via `cte_extraction::render_expr_to_sql_string`).
/// Conservative: unknown → not-string, so numeric `+` is never misrouted.
pub fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(contains_string_literal)
        }
        // #969: a bare reduce lambda binder typed String task-locally — consulted
        // ONLY by the string-concat detectors (mirrors `is_string_operand` in the
        // clickhouse emitter), never via the shared `infer_render_type`.
        RenderExpr::TableAlias(ta) => {
            crate::server::query_context::get_reduce_binder_type(&ta.0)
                == Some(
                    crate::sql_generator::emitters::clickhouse::type_inference::RenderType::String,
                )
        }
        RenderExpr::Column(col) => {
            crate::server::query_context::get_reduce_binder_type(col.raw())
                == Some(
                    crate::sql_generator::emitters::clickhouse::type_inference::RenderType::String,
                )
        }
        _ => {
            crate::sql_generator::emitters::clickhouse::type_inference::infer_render_type(expr)
                == Some(
                    crate::sql_generator::emitters::clickhouse::type_inference::RenderType::String,
                )
        }
    }
}

/// Check if any operand is string-valued (for string concatenation detection).
pub fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(contains_string_literal)
}

/// Flatten nested + operations into a list of operands for concat()
pub fn flatten_addition_operands(
    expr: &RenderExpr,
    alias_mapping: &[(String, String)],
) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
            .collect(),
        _ => vec![super::filter_sql::render_expr_to_sql_string(
            expr,
            alias_mapping,
        )],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{PropertyAccess, TableAlias};

    #[test]
    fn test_references_alias() {
        let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("users".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                "name".to_string(),
            ),
        });

        assert!(references_alias(&expr, "users"));
        assert!(!references_alias(&expr, "posts"));
    }
}
