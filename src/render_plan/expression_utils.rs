//! Expression processing utilities for RenderExpr
//!
//! This module provides common utilities for working with RenderExpr trees, including
//! a visitor trait for implementing expression transformations.

use super::render_expr::{Literal, Operator, PropertyAccess, RenderExpr, TableAlias};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::join_context::{VLP_CTE_FROM_ALIAS, VLP_START_ID_COLUMN, VLP_END_ID_COLUMN};

/// Trait for visiting/transforming RenderExpr trees
/// Implements visitor pattern to avoid duplicating recursive traversal logic across multiple functions
///
/// Implementations should override only the specific variants they need to handle, and
/// let default_transform_expr handle the recursive cases for other variants.
pub trait ExprVisitor {
    /// Transform a single RenderExpr, dispatching to specific methods based on type
    fn transform_expr(&mut self, expr: &RenderExpr) -> RenderExpr {
        match expr {
            RenderExpr::ScalarFnCall(fn_call) => {
                let rewritten_args: Vec<RenderExpr> = fn_call
                    .args
                    .iter()
                    .map(|arg| self.transform_expr(arg))
                    .collect();

                self.transform_scalar_fn_call(
                    &fn_call.name,
                    rewritten_args,
                )
            }
            RenderExpr::OperatorApplicationExp(op_app) => {
                let rewritten_operands: Vec<RenderExpr> = op_app
                    .operands
                    .iter()
                    .map(|operand| self.transform_expr(operand))
                    .collect();

                self.transform_operator_application(&op_app.operator, rewritten_operands)
            }
            RenderExpr::PropertyAccessExp(prop) => {
                self.transform_property_access(prop)
            }
            RenderExpr::AggregateFnCall(agg) => {
                let rewritten_args: Vec<RenderExpr> = agg
                    .args
                    .iter()
                    .map(|arg| self.transform_expr(arg))
                    .collect();

                self.transform_aggregate_fn_call(&agg.name, rewritten_args)
            }
            RenderExpr::List(items) => {
                let rewritten_items: Vec<RenderExpr> = items
                    .iter()
                    .map(|item| self.transform_expr(item))
                    .collect();

                self.transform_list(rewritten_items)
            }
            RenderExpr::Case(case_expr) => {
                let new_expr = case_expr.expr.as_ref().map(|e| Box::new(self.transform_expr(e)));
                let new_when_then: Vec<(RenderExpr, RenderExpr)> = case_expr
                    .when_then
                    .iter()
                    .map(|(when, then)| {
                        (self.transform_expr(when), self.transform_expr(then))
                    })
                    .collect();
                let new_else = case_expr.else_expr.as_ref().map(|e| Box::new(self.transform_expr(e)));

                self.transform_case(new_expr, new_when_then, new_else)
            }
            RenderExpr::InSubquery(subquery) => {
                let new_expr = Box::new(self.transform_expr(&subquery.expr));
                self.transform_in_subquery(new_expr, &subquery.subplan)
            }
            RenderExpr::ReduceExpr(reduce) => {
                let new_init = Box::new(self.transform_expr(&reduce.initial_value));
                let new_list = Box::new(self.transform_expr(&reduce.list));
                let new_expr = Box::new(self.transform_expr(&reduce.expression));

                self.transform_reduce_expr(
                    &reduce.accumulator,
                    &reduce.variable,
                    new_init,
                    new_list,
                    new_expr,
                )
            }
            RenderExpr::MapLiteral(entries) => {
                let new_entries: Vec<(String, RenderExpr)> = entries
                    .iter()
                    .map(|(k, v)| (k.clone(), self.transform_expr(v)))
                    .collect();

                self.transform_map_literal(new_entries)
            }
            RenderExpr::ArraySubscript { array, index } => {
                let new_array = Box::new(self.transform_expr(array));
                let new_index = Box::new(self.transform_expr(index));
                self.transform_array_subscript(new_array, new_index)
            }
            RenderExpr::ArraySlicing { array, from, to } => {
                let new_array = Box::new(self.transform_expr(array));
                let new_from = from.as_ref().map(|f| Box::new(self.transform_expr(f)));
                let new_to = to.as_ref().map(|t| Box::new(self.transform_expr(t)));
                self.transform_array_slicing(new_array, new_from, new_to)
            }
            // Leaf nodes - no transformation needed by default
            RenderExpr::Literal(_)
            | RenderExpr::Raw(_)
            | RenderExpr::Star
            | RenderExpr::TableAlias(_)
            | RenderExpr::ColumnAlias(_)
            | RenderExpr::Column(_)
            | RenderExpr::Parameter(_)
            | RenderExpr::ExistsSubquery(_)
            | RenderExpr::PatternCount(_)
            | RenderExpr::CteEntityRef(_) => expr.clone(),
        }
    }

    // Hook methods for subclasses to override specific cases
    
    fn transform_scalar_fn_call(
        &mut self,
        name: &str,
        args: Vec<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
            name: name.to_string(),
            args,
        })
    }

    fn transform_operator_application(
        &mut self,
        operator: &Operator,
        operands: Vec<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::OperatorApplicationExp(super::render_expr::OperatorApplication {
            operator: operator.clone(),
            operands,
        })
    }

    fn transform_property_access(
        &mut self,
        prop: &PropertyAccess,
    ) -> RenderExpr {
        RenderExpr::PropertyAccessExp(prop.clone())
    }

    fn transform_aggregate_fn_call(
        &mut self,
        name: &str,
        args: Vec<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
            name: name.to_string(),
            args,
        })
    }

    fn transform_list(
        &mut self,
        items: Vec<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::List(items)
    }

    fn transform_case(
        &mut self,
        expr: Option<Box<RenderExpr>>,
        when_then: Vec<(RenderExpr, RenderExpr)>,
        else_expr: Option<Box<RenderExpr>>,
    ) -> RenderExpr {
        RenderExpr::Case(super::render_expr::RenderCase {
            expr,
            when_then,
            else_expr,
        })
    }

    fn transform_in_subquery(
        &mut self,
        expr: Box<RenderExpr>,
        subplan: &super::RenderPlan,
    ) -> RenderExpr {
        RenderExpr::InSubquery(super::render_expr::InSubquery {
            expr,
            subplan: Box::new(subplan.clone()),
        })
    }

    fn transform_reduce_expr(
        &mut self,
        accumulator: &str,
        variable: &str,
        initial_value: Box<RenderExpr>,
        list: Box<RenderExpr>,
        expression: Box<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::ReduceExpr(super::render_expr::ReduceExpr {
            accumulator: accumulator.to_string(),
            variable: variable.to_string(),
            initial_value,
            list,
            expression,
        })
    }

    fn transform_map_literal(
        &mut self,
        entries: Vec<(String, RenderExpr)>,
    ) -> RenderExpr {
        RenderExpr::MapLiteral(entries)
    }

    fn transform_array_subscript(
        &mut self,
        array: Box<RenderExpr>,
        index: Box<RenderExpr>,
    ) -> RenderExpr {
        RenderExpr::ArraySubscript { array, index }
    }

    fn transform_array_slicing(
        &mut self,
        array: Box<RenderExpr>,
        from: Option<Box<RenderExpr>>,
        to: Option<Box<RenderExpr>>,
    ) -> RenderExpr {
        RenderExpr::ArraySlicing { array, from, to }
    }
}

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
        // ArraySlicing may contain aliases in array, from, and to
        RenderExpr::ArraySlicing { array, from, to } => {
            references_alias(array, alias)
                || from.as_ref().map_or(false, |f| references_alias(f, alias))
                || to.as_ref().map_or(false, |t| references_alias(t, alias))
        }
        // CteEntityRef doesn't reference aliases directly - it references CTE columns
        RenderExpr::CteEntityRef(_) => false,
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
                log::debug!(
                    "🔄 Rewriting alias '{}' → '{}'",
                    prop.table_alias.0,
                    new_alias
                );
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
        RenderExpr::ArraySlicing { array, from, to } => {
            rewrite_aliases(array, alias_map);
            if let Some(f) = from {
                rewrite_aliases(f, alias_map);
            }
            if let Some(t) = to {
                rewrite_aliases(t, alias_map);
            }
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
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_) => {}
    }
}

/// Check if a render expression contains a string literal
pub fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(|o| contains_string_literal(o))
        }
        _ => false,
    }
}

/// Check if any operand is a string literal (for string concatenation detection)
pub fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(|op| contains_string_literal(op))
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
        _ => vec![super::cte_extraction::render_expr_to_sql_string(
            expr,
            alias_mapping,
        )],
    }
}

/// Visitor for rewriting variable-length path (VLP) expressions
/// Handles property prefix rewriting for start/end nodes in VLP CTEs
pub struct VLPExprRewriter {
    pub start_cypher_alias: String,
    pub end_cypher_alias: String,
    pub start_is_denormalized: bool,
    pub end_is_denormalized: bool,
    pub rel_alias: Option<String>,
    pub from_col: Option<String>,
    pub to_col: Option<String>,
}

impl ExprVisitor for VLPExprRewriter {
    fn transform_property_access(
        &mut self,
        prop: &PropertyAccess,
    ) -> RenderExpr {
        let mut new_prop = prop.clone();
        let raw_col = prop.column.raw();

        // Check if this is a relationship alias access (e.g., f.Origin, f.Dest)
        if let (Some(rel), Some(from), Some(to)) = (self.rel_alias.as_ref(), self.from_col.as_ref(), self.to_col.as_ref()) {
            if prop.table_alias.0 == *rel {
                new_prop.table_alias = TableAlias(VLP_CTE_FROM_ALIAS.to_string());
                if raw_col == *from {
                    new_prop.column = PropertyValue::Column(VLP_START_ID_COLUMN.to_string());
                } else if raw_col == *to {
                    new_prop.column = PropertyValue::Column(VLP_END_ID_COLUMN.to_string());
                }
                return RenderExpr::PropertyAccessExp(new_prop);
            }
        }

        // Rewrite only for denormalized nodes
        if prop.table_alias.0 == self.start_cypher_alias && self.start_is_denormalized {
            new_prop.table_alias = TableAlias(VLP_CTE_FROM_ALIAS.to_string());
            if raw_col != "*" {
                new_prop.column = PropertyValue::Column(format!("start_{}", raw_col));
            }
        } else if prop.table_alias.0 == self.end_cypher_alias && self.end_is_denormalized {
            new_prop.table_alias = TableAlias(VLP_CTE_FROM_ALIAS.to_string());
            if raw_col != "*" {
                new_prop.column = PropertyValue::Column(VLP_END_ID_COLUMN.to_string());
            }
        }

        RenderExpr::PropertyAccessExp(new_prop)
    }
}

/// Visitor for rewriting alias references in expressions
/// Used for mapping Cypher aliases to VLP internal aliases
pub struct AliasRewriter {
    pub alias_map: std::collections::HashMap<String, String>,
}

impl ExprVisitor for AliasRewriter {
    fn transform_property_access(
        &mut self,
        prop: &PropertyAccess,
    ) -> RenderExpr {
        let mut new_prop = prop.clone();
        if let Some(new_alias) = self.alias_map.get(&prop.table_alias.0) {
            log::debug!(
                "🔄 Rewriting alias '{}' → '{}'",
                prop.table_alias.0,
                new_alias
            );
            new_prop.table_alias = TableAlias(new_alias.clone());
        }
        RenderExpr::PropertyAccessExp(new_prop)
    }
}

/// Visitor for adding column name prefixes based on table alias
/// Mutates expressions in-place to rewrite simple column references to aliased versions
/// Used for CTE column flattening: "id" → "alias_id"
pub struct MutablePropertyColumnRewriter;

impl MutablePropertyColumnRewriter {
    /// Rewrite column references to include table alias prefix
    /// E.g., user.id → user.user_id (mutates expr in-place)
    pub fn rewrite_column_with_prefix(expr: &mut RenderExpr, prefix_char: char) {
        match expr {
            RenderExpr::PropertyAccessExp(pa) => {
                let table_alias = &pa.table_alias.0;
                let column_name = match &pa.column {
                    PropertyValue::Column(col) => col.clone(),
                    _ => return,
                };

                // Only rewrite if column doesn't already have the prefix
                let prefix = format!("{}{}", table_alias, prefix_char);
                if !column_name.starts_with(&prefix) {
                    let new_column = format!("{}{}{}", table_alias, prefix_char, column_name);
                    log::debug!(
                        "🔧 Rewriting column: {}.{} → {}.{}",
                        table_alias,
                        column_name,
                        table_alias,
                        new_column
                    );
                    pa.column = PropertyValue::Column(new_column);
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                for arg in &mut agg.args {
                    Self::rewrite_column_with_prefix(arg, prefix_char);
                }
            }
            RenderExpr::ScalarFnCall(func) => {
                for arg in &mut func.args {
                    Self::rewrite_column_with_prefix(arg, prefix_char);
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                for operand in &mut op.operands {
                    Self::rewrite_column_with_prefix(operand, prefix_char);
                }
            }
            _ => {
                // Other expression types don't need rewriting
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
