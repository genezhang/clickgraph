//! WHERE Property Extractor
//!
//! Extracts property references from WHERE clauses for property-based optimization.
//!
//! Key principle: ANY property reference in a WHERE clause implies that property
//! must exist in the schema. This enables pruning of UNION branches that don't
//! have the required properties.
//!
//! Examples:
//! - `WHERE n.bytes_sent IS NOT NULL` → requires bytes_sent
//! - `WHERE n.bytes_sent > 100` → requires bytes_sent (implicit NOT NULL)
//! - `WHERE n.x = 1 AND n.y = 2` → requires x AND y

use crate::open_cypher_parser::ast::{Expression, WhereClause};
use std::collections::{HashMap, HashSet};

/// Extracts property references from WHERE clauses
pub struct WherePropertyExtractor;

impl WherePropertyExtractor {
    /// Extract ALL property references from WHERE clause
    ///
    /// Returns: HashMap mapping alias → set of property names
    ///
    /// # Examples
    /// ```
    /// // WHERE n.bytes_sent > 100
    /// // Returns: {"n": {"bytes_sent"}}
    ///
    /// // WHERE n.x = 1 AND n.y = 2
    /// // Returns: {"n": {"x", "y"}}
    ///
    /// // WHERE n.x > 1 OR r.y < 10
    /// // Returns: {"n": {"x"}, "r": {"y"}}
    /// ```
    pub fn extract_property_references(
        where_clause: &WhereClause,
    ) -> HashMap<String, HashSet<String>> {
        let mut properties: HashMap<String, HashSet<String>> = HashMap::new();
        Self::walk_expression(&where_clause.conditions, &mut properties);
        properties
    }

    /// Recursively walk expression tree to find all property accesses
    fn walk_expression(expr: &Expression, properties: &mut HashMap<String, HashSet<String>>) {
        match expr {
            Expression::PropertyAccessExp(prop) => {
                // Found a property access: n.bytes_sent
                // Extract alias (base) and property (key)
                let alias = prop.base.to_string();
                let property = prop.key.to_string();

                properties.entry(alias).or_default().insert(property);
            }

            Expression::OperatorApplicationExp(op) => {
                // Recurse into operator operands
                // Handles: AND, OR, comparisons (=, >, <, etc.), IS NOT NULL, etc.
                for operand in &op.operands {
                    Self::walk_expression(operand, properties);
                }
            }

            Expression::FunctionCallExp(func) => {
                // Recurse into function arguments
                // Example: size(n.tags) > 5 → extracts n.tags
                for arg in &func.args {
                    Self::walk_expression(arg, properties);
                }
            }

            Expression::List(list) => {
                // Recurse into list elements
                // Example: n.value IN [1, 2, n.other]
                for elem in list {
                    Self::walk_expression(elem, properties);
                }
            }

            Expression::Case(case_expr) => {
                // Recurse into CASE expression parts
                // CASE expr
                if let Some(expr) = &case_expr.expr {
                    Self::walk_expression(expr, properties);
                }

                // WHEN conditions and THEN results
                for (when_expr, then_expr) in &case_expr.when_then {
                    Self::walk_expression(when_expr, properties);
                    Self::walk_expression(then_expr, properties);
                }

                // ELSE result
                if let Some(else_expr) = &case_expr.else_expr {
                    Self::walk_expression(else_expr, properties);
                }
            }

            Expression::ExistsExpression(exists) => {
                // Recurse into EXISTS subquery
                if let Some(where_clause) = &exists.where_clause {
                    Self::walk_expression(&where_clause.conditions, properties);
                }
            }

            Expression::ReduceExp(reduce) => {
                // Recurse into reduce expression parts
                Self::walk_expression(&reduce.initial_value, properties);
                Self::walk_expression(&reduce.list, properties);
                Self::walk_expression(&reduce.expression, properties);
            }

            Expression::MapLiteral(map) => {
                // Recurse into map values
                // Example: {key: n.value}
                for (_, value_expr) in map {
                    Self::walk_expression(value_expr, properties);
                }
            }

            Expression::ListComprehension(lc) => {
                Self::walk_expression(&lc.list_expr, properties);
                if let Some(ref wc) = lc.where_clause {
                    Self::walk_expression(wc, properties);
                }
                if let Some(ref proj) = lc.projection {
                    Self::walk_expression(proj, properties);
                }
            }

            // Base cases - no property references to extract
            Expression::Literal(_)
            | Expression::Variable(_)
            | Expression::Parameter(_)
            | Expression::PathPattern(_)
            | Expression::LabelExpression { .. }
            | Expression::Lambda(_)
            | Expression::PatternComprehension(_)
            | Expression::ArraySubscript { .. }
            | Expression::ArraySlicing { .. } => {
                // No property access in these expression types (or not supported yet)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast::{
        Expression, Literal, Operator, OperatorApplication, PropertyAccess, WhereClause,
    };

    fn make_property_access<'a>(base: &'a str, key: &'a str) -> Expression<'a> {
        Expression::PropertyAccessExp(PropertyAccess { base, key })
    }

    fn make_literal(value: i64) -> Expression<'static> {
        Expression::Literal(Literal::Integer(value))
    }

    #[test]
    fn test_simple_property_access() {
        // WHERE n.bytes_sent IS NOT NULL
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::IsNotNull,
                operands: vec![make_property_access("n", "bytes_sent")],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 1);
        assert!(result.contains_key("n"));
        assert!(result.get("n").unwrap().contains("bytes_sent"));
    }

    #[test]
    fn test_comparison_operator() {
        // WHERE n.bytes_sent > 100
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![make_property_access("n", "bytes_sent"), make_literal(100)],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 1);
        assert!(result.contains_key("n"));
        assert!(result.get("n").unwrap().contains("bytes_sent"));
    }

    #[test]
    fn test_multiple_properties_and() {
        // WHERE n.x = 1 AND n.y = 2
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![make_property_access("n", "x"), make_literal(1)],
                    }),
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![make_property_access("n", "y"), make_literal(2)],
                    }),
                ],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 1);
        assert!(result.contains_key("n"));
        assert_eq!(result.get("n").unwrap().len(), 2);
        assert!(result.get("n").unwrap().contains("x"));
        assert!(result.get("n").unwrap().contains("y"));
    }

    #[test]
    fn test_multiple_aliases() {
        // WHERE n.x > 1 OR r.y < 10
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Or,
                operands: vec![
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::GreaterThan,
                        operands: vec![make_property_access("n", "x"), make_literal(1)],
                    }),
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::LessThan,
                        operands: vec![make_property_access("r", "y"), make_literal(10)],
                    }),
                ],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 2);
        assert!(result.contains_key("n"));
        assert!(result.contains_key("r"));
        assert!(result.get("n").unwrap().contains("x"));
        assert!(result.get("r").unwrap().contains("y"));
    }

    #[test]
    fn test_no_properties() {
        // WHERE 1 = 1 (no property references)
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![make_literal(1), make_literal(1)],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_nested_function() {
        // WHERE size(n.tags) > 5 - property inside function
        use crate::open_cypher_parser::ast::FunctionCall;

        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    Expression::FunctionCallExp(FunctionCall {
                        name: "size".to_string(),
                        args: vec![make_property_access("n", "tags")],
                    }),
                    make_literal(5),
                ],
            }),
        };

        let result = WherePropertyExtractor::extract_property_references(&where_clause);

        assert_eq!(result.len(), 1);
        assert!(result.contains_key("n"));
        assert!(result.get("n").unwrap().contains("tags"));
    }
}
