//! Expression Visitor Pattern
//!
//! This module provides a visitor trait for traversing LogicalExpr trees.
//! It eliminates duplicate traversal logic found across multiple analyzer passes.
//!
//! # Architecture
//!
//! The visitor pattern separates the traversal logic from the operation performed
//! at each node. This allows different analyzer passes to reuse the same traversal
//! while implementing different visit behaviors.
//!
//! # Example
//!
//! ```ignore
//! use crate::query_planner::logical_expr::visitors::{ExpressionVisitor, walk_expression};
//!
//! struct PropertyCollector {
//!     properties: Vec<(String, String)>,  // (alias, property)
//! }
//!
//! impl ExpressionVisitor for PropertyCollector {
//!     type Output = ();
//!
//!     fn visit_property_access(&mut self, prop: &PropertyAccess) {
//!         self.properties.push((prop.table_alias.0.clone(), prop.column.to_string()));
//!     }
//! }
//!
//! let mut collector = PropertyCollector { properties: vec![] };
//! walk_expression(&expr, &mut collector);
//! // collector.properties now contains all property accesses
//! ```

use super::{
    AggregateFnCall, LogicalCase, LogicalExpr, OperatorApplication, PropertyAccess, ReduceExpr,
    ScalarFnCall,
};

/// Trait for visiting LogicalExpr nodes.
///
/// Implementors can override specific `visit_*` methods to handle nodes of interest.
/// The default implementations do nothing, allowing visitors to be selective.
pub trait ExpressionVisitor {
    /// The output type of the visitor (often `()` for mutation-based visitors)
    type Output: Default;

    /// Called for each PropertyAccess expression (e.g., `u.name`)
    fn visit_property_access(&mut self, _prop: &PropertyAccess) -> Self::Output {
        Self::Output::default()
    }

    /// Called for each scalar function call (e.g., `length(s)`)
    fn visit_scalar_fn(&mut self, _fn_call: &ScalarFnCall) -> Self::Output {
        Self::Output::default()
    }

    /// Called for each aggregate function call (e.g., `count(x)`)
    fn visit_aggregate_fn(&mut self, _agg_call: &AggregateFnCall) -> Self::Output {
        Self::Output::default()
    }

    /// Called for each operator application (e.g., `a + b`, `x = y`)
    fn visit_operator(&mut self, _op_app: &OperatorApplication) -> Self::Output {
        Self::Output::default()
    }

    /// Called for each CASE expression
    fn visit_case(&mut self, _case: &LogicalCase) -> Self::Output {
        Self::Output::default()
    }

    /// Called for each reduce expression
    fn visit_reduce(&mut self, _reduce: &ReduceExpr) -> Self::Output {
        Self::Output::default()
    }

    /// Called for table alias references (e.g., bare `u` in RETURN)
    fn visit_table_alias(&mut self, _alias: &str) -> Self::Output {
        Self::Output::default()
    }

    /// Called for leaf expressions not handled by specific methods
    fn visit_leaf(&mut self, _expr: &LogicalExpr) -> Self::Output {
        Self::Output::default()
    }
}

/// Walk an expression tree, calling visitor methods for each node.
///
/// This function handles the recursive traversal, calling appropriate visitor methods
/// and descending into child expressions.
pub fn walk_expression<V: ExpressionVisitor>(expr: &LogicalExpr, visitor: &mut V) -> V::Output {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => visitor.visit_property_access(prop),

        LogicalExpr::ScalarFnCall(fn_call) => {
            // First visit the function itself
            let result = visitor.visit_scalar_fn(fn_call);
            // Then recursively visit arguments
            for arg in &fn_call.args {
                walk_expression(arg, visitor);
            }
            result
        }

        LogicalExpr::AggregateFnCall(agg_call) => {
            let result = visitor.visit_aggregate_fn(agg_call);
            for arg in &agg_call.args {
                walk_expression(arg, visitor);
            }
            result
        }

        LogicalExpr::OperatorApplicationExp(op_app) => {
            let result = visitor.visit_operator(op_app);
            for operand in &op_app.operands {
                walk_expression(operand, visitor);
            }
            result
        }

        // Legacy Operator variant - same handling
        LogicalExpr::Operator(op_app) => {
            let result = visitor.visit_operator(op_app);
            for operand in &op_app.operands {
                walk_expression(operand, visitor);
            }
            result
        }

        LogicalExpr::Case(case) => {
            let result = visitor.visit_case(case);
            if let Some(case_expr) = &case.expr {
                walk_expression(case_expr, visitor);
            }
            for (when, then) in &case.when_then {
                walk_expression(when, visitor);
                walk_expression(then, visitor);
            }
            if let Some(else_expr) = &case.else_expr {
                walk_expression(else_expr, visitor);
            }
            result
        }

        LogicalExpr::ReduceExpr(reduce) => {
            let result = visitor.visit_reduce(reduce);
            walk_expression(&reduce.initial_value, visitor);
            walk_expression(&reduce.list, visitor);
            walk_expression(&reduce.expression, visitor);
            result
        }

        LogicalExpr::TableAlias(alias) => visitor.visit_table_alias(&alias.0),

        LogicalExpr::List(items) => {
            for item in items {
                walk_expression(item, visitor);
            }
            V::Output::default()
        }

        LogicalExpr::MapLiteral(entries) => {
            for (_, value) in entries {
                walk_expression(value, visitor);
            }
            V::Output::default()
        }

        LogicalExpr::ArraySubscript { array, index } => {
            walk_expression(array, visitor);
            walk_expression(index, visitor);
            V::Output::default()
        }

        LogicalExpr::ArraySlicing { array, from, to } => {
            walk_expression(array, visitor);
            if let Some(f) = from {
                walk_expression(f, visitor);
            }
            if let Some(t) = to {
                walk_expression(t, visitor);
            }
            V::Output::default()
        }

        LogicalExpr::Lambda(lambda) => {
            walk_expression(&lambda.body, visitor);
            V::Output::default()
        }

        LogicalExpr::InSubquery(subq) => {
            walk_expression(&subq.expr, visitor);
            V::Output::default()
        }

        // Leaf nodes - no children to traverse
        LogicalExpr::Literal(_)
        | LogicalExpr::Raw(_)
        | LogicalExpr::Star
        | LogicalExpr::ColumnAlias(_)
        | LogicalExpr::Column(_)
        | LogicalExpr::Parameter(_)
        | LogicalExpr::PathPattern(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::LabelExpression { .. }
        | LogicalExpr::PatternCount(_)
        | LogicalExpr::PatternComprehension(_)
        | LogicalExpr::CteEntityRef(_) => visitor.visit_leaf(expr),
    }
}

// =============================================================================
// Common Visitor Implementations
// =============================================================================

/// Collects all property accesses from an expression tree.
///
/// # Example
/// ```ignore
/// let props = collect_property_accesses(&expr);
/// // props = [("u", "name"), ("u", "age"), ("p", "title")]
/// ```
pub struct PropertyAccessCollector {
    /// Collected property accesses: (table_alias, property_name)
    pub properties: Vec<(String, String)>,
}

impl PropertyAccessCollector {
    pub fn new() -> Self {
        Self { properties: vec![] }
    }

    /// Collect all property accesses from an expression
    pub fn collect(expr: &LogicalExpr) -> Vec<(String, String)> {
        let mut collector = Self::new();
        walk_expression(expr, &mut collector);
        collector.properties
    }
}

impl Default for PropertyAccessCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionVisitor for PropertyAccessCollector {
    type Output = ();

    fn visit_property_access(&mut self, prop: &PropertyAccess) {
        let property_name = match &prop.column {
            crate::graph_catalog::expression_parser::PropertyValue::Column(col) => col.clone(),
            crate::graph_catalog::expression_parser::PropertyValue::Expression(expr) => expr.clone(),
        };
        self.properties
            .push((prop.table_alias.0.clone(), property_name));
    }
}

/// Collects all table aliases referenced in an expression.
pub struct TableAliasCollector {
    /// Collected aliases
    pub aliases: Vec<String>,
}

impl TableAliasCollector {
    pub fn new() -> Self {
        Self { aliases: vec![] }
    }

    /// Collect all table aliases from an expression
    pub fn collect(expr: &LogicalExpr) -> Vec<String> {
        let mut collector = Self::new();
        walk_expression(expr, &mut collector);
        collector.aliases
    }
}

impl Default for TableAliasCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionVisitor for TableAliasCollector {
    type Output = ();

    fn visit_table_alias(&mut self, alias: &str) {
        if !self.aliases.contains(&alias.to_string()) {
            self.aliases.push(alias.to_string());
        }
    }

    fn visit_property_access(&mut self, prop: &PropertyAccess) {
        let alias = &prop.table_alias.0;
        if !self.aliases.contains(alias) {
            self.aliases.push(alias.clone());
        }
    }
}

/// Checks if an expression contains any aggregate functions.
pub struct HasAggregateCheck {
    pub has_aggregate: bool,
}

impl HasAggregateCheck {
    pub fn check(expr: &LogicalExpr) -> bool {
        let mut checker = Self {
            has_aggregate: false,
        };
        walk_expression(expr, &mut checker);
        checker.has_aggregate
    }
}

impl ExpressionVisitor for HasAggregateCheck {
    type Output = ();

    fn visit_aggregate_fn(&mut self, _agg_call: &AggregateFnCall) {
        self.has_aggregate = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, TableAlias};

    #[test]
    fn test_property_access_collector() {
        let expr = LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("u".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                "name".to_string(),
            ),
        });

        let props = PropertyAccessCollector::collect(&expr);
        assert_eq!(props.len(), 1);
        assert_eq!(props[0], ("u".to_string(), "name".to_string()));
    }

    #[test]
    fn test_table_alias_collector() {
        let expr = LogicalExpr::TableAlias(TableAlias("user".to_string()));

        let aliases = TableAliasCollector::collect(&expr);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "user");
    }

    #[test]
    fn test_has_aggregate_check() {
        // Expression without aggregate
        let no_agg = LogicalExpr::Literal(Literal::Integer(42));
        assert!(!HasAggregateCheck::check(&no_agg));

        // Expression with aggregate
        let with_agg = LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![LogicalExpr::Star],
        });
        assert!(HasAggregateCheck::check(&with_agg));
    }

    #[test]
    fn test_nested_expression_traversal() {
        use crate::query_planner::logical_expr::{Operator, OperatorApplication};

        // Create: u.age > 18 AND u.active = true
        let prop1 = LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("u".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                "age".to_string(),
            ),
        });
        let prop2 = LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("u".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                "active".to_string(),
            ),
        });

        let cond1 = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![prop1, LogicalExpr::Literal(Literal::Integer(18))],
        });

        let cond2 = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![prop2, LogicalExpr::Literal(Literal::Boolean(true))],
        });

        let and_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![cond1, cond2],
        });

        let props = PropertyAccessCollector::collect(&and_expr);
        assert_eq!(props.len(), 2);
        assert!(props.contains(&("u".to_string(), "age".to_string())));
        assert!(props.contains(&("u".to_string(), "active".to_string())));
    }
}
