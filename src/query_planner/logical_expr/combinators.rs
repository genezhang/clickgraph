//! Predicate Combinators for LogicalExpr
//!
//! This module provides utility functions for combining predicates with boolean operators.
//! These helpers eliminate duplicate code across analyzer passes (graph_join_inference.rs,
//! filter_tagging.rs, schema_inference.rs, query_validation.rs).
//!
//! # Example
//! ```ignore
//! use crate::query_planner::logical_expr::combinators::{and, or};
//!
//! let predicates = vec![pred1, pred2, pred3];
//! let combined = and(predicates);  // Returns Some(pred1 AND pred2 AND pred3)
//! ```

use super::{LogicalExpr, Operator, OperatorApplication};

/// Combine predicates with AND operator.
///
/// - Empty vec → None
/// - Single predicate → Some(predicate)
/// - Multiple → Some(pred1 AND pred2 AND ...)
///
/// # Example
/// ```ignore
/// let combined = and(vec![has_name, is_active, in_region]);
/// // Result: has_name AND is_active AND in_region
/// ```
pub fn and(predicates: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    combine_predicates(predicates, Operator::And)
}

/// Combine predicates with OR operator.
///
/// - Empty vec → None
/// - Single predicate → Some(predicate)
/// - Multiple → Some(pred1 OR pred2 OR ...)
///
/// # Example
/// ```ignore
/// let combined = or(vec![is_admin, is_moderator, is_owner]);
/// // Result: is_admin OR is_moderator OR is_owner
/// ```
pub fn or(predicates: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    combine_predicates(predicates, Operator::Or)
}

/// Core combinator function.
fn combine_predicates(predicates: Vec<LogicalExpr>, op: Operator) -> Option<LogicalExpr> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands: predicates,
        })),
    }
}

/// Check if an expression is a comparison operator (can be used as a standalone filter).
///
/// Returns true for: =, <>, <, >, <=, >=, =~, IN, NOT IN, STARTS WITH, etc.
/// Returns false for: +, -, *, /, %, ^ (arithmetic operators)
pub fn is_comparison(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::OperatorApplicationExp(op_app) => op_app.operator.is_filter_extractable(),
        _ => false,
    }
}

/// Flatten nested AND/OR expressions into a flat list of operands.
///
/// Useful for normalizing deeply nested boolean expressions:
/// `(a AND (b AND c))` → `[a, b, c]`
pub fn flatten_boolean_op(expr: &LogicalExpr, op: Operator) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::OperatorApplicationExp(op_app) if op_app.operator == op => op_app
            .operands
            .iter()
            .flat_map(|operand| flatten_boolean_op(operand, op))
            .collect(),
        other => vec![other.clone()],
    }
}

/// Negate an expression with NOT operator.
///
/// Creates: NOT(expr)
pub fn not(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Not,
        operands: vec![expr],
    })
}

/// Create an equality comparison: lhs = rhs
pub fn eq(lhs: LogicalExpr, rhs: LogicalExpr) -> LogicalExpr {
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![lhs, rhs],
    })
}

/// Create a column equality condition: left_alias.left_col = right_alias.right_col
///
/// Convenience function that combines PropertyAccess creation with eq().
///
/// # Example
/// ```ignore
/// let condition = col_eq("u", "id", "r", "user_id");
/// // Equivalent to: u.id = r.user_id
/// ```
pub fn col_eq(
    left_alias: impl Into<String>,
    left_col: impl Into<String>,
    right_alias: impl Into<String>,
    right_col: impl Into<String>,
) -> OperatorApplication {
    use super::{PropertyAccess, TableAlias};
    use crate::graph_catalog::expression_parser::PropertyValue;

    OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(left_alias.into()),
                column: PropertyValue::Column(left_col.into()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(right_alias.into()),
                column: PropertyValue::Column(right_col.into()),
            }),
        ],
    }
}

/// Create a PropertyAccess expression: alias.column
///
/// # Example
/// ```ignore
/// let expr = prop("u", "name");
/// // Equivalent to: u.name
/// ```
pub fn prop(alias: impl Into<String>, col: impl Into<String>) -> LogicalExpr {
    use super::{PropertyAccess, TableAlias};
    use crate::graph_catalog::expression_parser::PropertyValue;

    LogicalExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(alias.into()),
        column: PropertyValue::Column(col.into()),
    })
}

/// Create a not-equal comparison: lhs <> rhs
pub fn neq(lhs: LogicalExpr, rhs: LogicalExpr) -> LogicalExpr {
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::NotEqual,
        operands: vec![lhs, rhs],
    })
}

/// Create an IS NULL check
pub fn is_null(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::IsNull,
        operands: vec![expr],
    })
}

/// Create an IS NOT NULL check
pub fn is_not_null(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::IsNotNull,
        operands: vec![expr],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::Literal;

    #[test]
    fn test_and_empty() {
        assert!(and(vec![]).is_none());
    }

    #[test]
    fn test_and_single() {
        let pred = LogicalExpr::Literal(Literal::Boolean(true));
        let result = and(vec![pred.clone()]);
        assert_eq!(result, Some(pred));
    }

    #[test]
    fn test_and_multiple() {
        let pred1 = LogicalExpr::Literal(Literal::Boolean(true));
        let pred2 = LogicalExpr::Literal(Literal::Boolean(false));
        let result = and(vec![pred1.clone(), pred2.clone()]).unwrap();

        match result {
            LogicalExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operator, Operator::And);
                assert_eq!(op.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }

    #[test]
    fn test_or_multiple() {
        let pred1 = LogicalExpr::Literal(Literal::Boolean(true));
        let pred2 = LogicalExpr::Literal(Literal::Boolean(false));
        let result = or(vec![pred1, pred2]).unwrap();

        match result {
            LogicalExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operator, Operator::Or);
                assert_eq!(op.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }

    #[test]
    fn test_flatten_nested_and() {
        // Create (a AND (b AND c))
        let a = LogicalExpr::Literal(Literal::Integer(1));
        let b = LogicalExpr::Literal(Literal::Integer(2));
        let c = LogicalExpr::Literal(Literal::Integer(3));

        let inner = and(vec![b.clone(), c.clone()]).unwrap();
        let outer = and(vec![a.clone(), inner]).unwrap();

        let flattened = flatten_boolean_op(&outer, Operator::And);
        assert_eq!(flattened.len(), 3);
    }

    #[test]
    fn test_not() {
        let pred = LogicalExpr::Literal(Literal::Boolean(true));
        let negated = not(pred);

        match negated {
            LogicalExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operator, Operator::Not);
                assert_eq!(op.operands.len(), 1);
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }
}
