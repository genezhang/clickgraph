//! Expression rewriting utilities for property mapping
//!
//! This module provides functions to rewrite LogicalExpr trees, particularly for:
//! - Mapping Cypher property names to database column names
//! - Handling nested expressions (functions, operators, etc.)
//!
//! These utilities are shared between WITH and RETURN clause processing to ensure
//! consistent expression handling throughout the query pipeline.

use super::{
    AggregateFnCall, LogicalCase, LogicalExpr, OperatorApplication, PropertyAccess, ReduceExpr,
    ScalarFnCall,
};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::variable_scope::VariableScope;

/// Context for expression rewriting.
///
/// Contains the information needed to resolve property mappings:
/// - The input plan (for alias → label resolution)
/// - Optional scope for CTE-aware resolution
pub struct ExpressionRewriteContext<'a> {
    /// The input plan from which aliases can be resolved to labels
    pub input_plan: &'a LogicalPlan,
    /// Optional scope for CTE-aware variable resolution.
    /// When Some, CTE variables resolve to CTE columns (forward resolution).
    /// When None, falls back to schema-only resolution (backward compatible).
    pub scope: Option<&'a VariableScope<'a>>,
}

impl<'a> ExpressionRewriteContext<'a> {
    pub fn new(input_plan: &'a LogicalPlan) -> Self {
        log::debug!(
            "🔍 ExpressionRewriteContext: Created with plan type: {:?}",
            std::mem::discriminant(input_plan)
        );
        Self {
            input_plan,
            scope: None,
        }
    }

    pub fn with_scope(input_plan: &'a LogicalPlan, scope: &'a VariableScope<'a>) -> Self {
        log::debug!(
            "🔍 ExpressionRewriteContext: Created with scope, plan type: {:?}",
            std::mem::discriminant(input_plan)
        );
        Self {
            input_plan,
            scope: Some(scope),
        }
    }

    /// Find the label for a given alias by searching the input plan
    pub fn find_label_for_alias(&self, alias: &str) -> Option<String> {
        let result = find_label_for_alias_in_plan(self.input_plan, alias);
        log::debug!(
            "🔍 ExpressionRewriteContext: find_label_for_alias('{}') = {:?}",
            alias,
            result
        );
        result
    }
}

/// Find the label for an alias by recursively searching the plan tree
pub(crate) fn find_label_for_alias_in_plan(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                node.label.clone()
            } else {
                find_label_for_alias_in_plan(&node.input, target_alias)
            }
        }
        // ViewScan doesn't have an alias field - alias is in the wrapping GraphNode
        LogicalPlan::ViewScan(_) => None,
        LogicalPlan::GraphRel(rel) => {
            // Check left and right plans
            find_label_for_alias_in_plan(&rel.left, target_alias)
                .or_else(|| find_label_for_alias_in_plan(&rel.right, target_alias))
                .or_else(|| find_label_for_alias_in_plan(&rel.center, target_alias))
        }
        LogicalPlan::CartesianProduct(cp) => find_label_for_alias_in_plan(&cp.left, target_alias)
            .or_else(|| find_label_for_alias_in_plan(&cp.right, target_alias)),
        LogicalPlan::GraphJoins(gj) => find_label_for_alias_in_plan(&gj.input, target_alias),
        LogicalPlan::Filter(filter) => find_label_for_alias_in_plan(&filter.input, target_alias),
        LogicalPlan::Projection(proj) => find_label_for_alias_in_plan(&proj.input, target_alias),
        LogicalPlan::GroupBy(gb) => find_label_for_alias_in_plan(&gb.input, target_alias),
        LogicalPlan::OrderBy(ob) => find_label_for_alias_in_plan(&ob.input, target_alias),
        LogicalPlan::Skip(s) => find_label_for_alias_in_plan(&s.input, target_alias),
        LogicalPlan::Limit(l) => find_label_for_alias_in_plan(&l.input, target_alias),
        LogicalPlan::Cte(cte) => find_label_for_alias_in_plan(&cte.input, target_alias),
        LogicalPlan::Unwind(u) => find_label_for_alias_in_plan(&u.input, target_alias),
        LogicalPlan::WithClause(wc) => find_label_for_alias_in_plan(&wc.input, target_alias),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(label) = find_label_for_alias_in_plan(input, target_alias) {
                    return Some(label);
                }
            }
            None
        }
        LogicalPlan::Empty | LogicalPlan::PageRank(_) => None,
    }
}

/// Map a Cypher property to its corresponding database column using the schema.
///
/// This is the core property mapping function that consults GLOBAL_SCHEMAS.
pub(crate) fn map_property_to_db_column(
    cypher_property: &str,
    node_label: &str,
) -> Result<String, PropertyMappingError> {
    // Use the existing map_property_to_column_with_schema function
    crate::render_plan::cte_generation::map_property_to_column_with_schema(
        cypher_property,
        node_label,
    )
    .map_err(PropertyMappingError::MappingFailed)
}

/// Errors that can occur during property mapping
#[derive(Debug, Clone)]
pub enum PropertyMappingError {
    /// Label not found for the given alias
    LabelNotFound(String),
    /// Property mapping failed (schema lookup error)
    MappingFailed(String),
}

impl std::fmt::Display for PropertyMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyMappingError::LabelNotFound(alias) => {
                write!(f, "Could not find label for alias '{}'", alias)
            }
            PropertyMappingError::MappingFailed(msg) => {
                write!(f, "Property mapping failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for PropertyMappingError {}

/// Rewrite a LogicalExpr, mapping Cypher properties to database columns.
///
/// This function recursively traverses the expression tree and for each
/// `PropertyAccessExp`, it:
/// 1. Looks up the label for the table alias
/// 2. Maps the Cypher property name to the database column name
/// 3. Returns a new expression with the mapped column
///
/// Other expression types are recursively processed to handle nested expressions
/// like `substring(u.name, 1, 10)` or `u.age + 1`.
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `ctx` - Context containing the input plan for alias resolution
///
/// # Returns
/// A new LogicalExpr with properties mapped to database columns.
/// If a property cannot be mapped (alias not found or schema lookup fails),
/// the original expression is returned unchanged with a warning logged.
pub fn rewrite_expression_with_property_mapping(
    expr: &LogicalExpr,
    ctx: &ExpressionRewriteContext,
) -> LogicalExpr {
    match expr {
        // PropertyAccessExp: Map Cypher property to DB column (or CTE column if scope-aware)
        LogicalExpr::PropertyAccessExp(prop) => {
            let alias = &prop.table_alias.0;
            let cypher_property = prop.column.raw();

            log::debug!(
                "🔍 Expression rewriter: Processing PropertyAccessExp {}.{}",
                alias,
                cypher_property
            );

            // Scope-aware resolution: check scope FIRST if available
            if let Some(scope) = ctx.scope {
                use crate::render_plan::variable_scope::ResolvedProperty;
                match scope.resolve(alias, cypher_property) {
                    ResolvedProperty::CteColumn { column, .. } => {
                        log::debug!(
                            "✓ Scope resolution (CTE): {}.{} → {}.{}",
                            alias, cypher_property, alias, column
                        );
                        return LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: prop.table_alias.clone(),
                            column: PropertyValue::Column(column),
                        });
                    }
                    ResolvedProperty::DbColumn(db_col) => {
                        log::debug!(
                            "✓ Scope resolution (DB): {}.{} → {}.{}",
                            alias, cypher_property, alias, db_col
                        );
                        if db_col == cypher_property {
                            return expr.clone();
                        }
                        return LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: prop.table_alias.clone(),
                            column: PropertyValue::Column(db_col),
                        });
                    }
                    ResolvedProperty::Unresolved => {
                        log::debug!(
                            "🔍 Scope could not resolve {}.{}, falling through to schema",
                            alias, cypher_property
                        );
                    }
                }
            }

            // Fallback: schema-only resolution (original behavior)
            match ctx.find_label_for_alias(alias) {
                Some(label) => {
                    log::debug!("🔍 TRACING: Expression rewriter found label '{}' for alias '{}', property '{}'", label, alias, cypher_property);
                    // Map the property to DB column
                    match map_property_to_db_column(cypher_property, &label) {
                        Ok(db_column) => {
                            log::debug!(
                                "🔍 TRACING: Mapped property '{}' to DB column '{}' for label '{}'",
                                cypher_property,
                                db_column,
                                label
                            );
                            // IDEMPOTENCY CHECK: If the mapped column is the same as the input,
                            // the property is already a DB column name. Return as-is.
                            // This handles cases where the expression was already rewritten.
                            if db_column == cypher_property {
                                log::debug!(
                                    "🔍 Expression rewriter: '{}' maps to itself - already a DB column, keeping as-is",
                                    cypher_property
                                );
                                return expr.clone();
                            }

                            log::debug!(
                                "✓ Property mapping: {}.{} → {}.{} (label={})",
                                alias,
                                cypher_property,
                                alias,
                                db_column,
                                label
                            );
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: prop.table_alias.clone(),
                                column: PropertyValue::Column(db_column),
                            })
                        }
                        Err(e) => {
                            log::warn!(
                                "⚠️ Property mapping failed for {}.{}: {}. Using original.",
                                alias,
                                cypher_property,
                                e
                            );
                            expr.clone()
                        }
                    }
                }
                None => {
                    log::warn!(
                        "⚠️ Could not find label for alias '{}'. Using original expression.",
                        alias
                    );
                    expr.clone()
                }
            }
        }

        // ScalarFnCall: Recursively rewrite arguments
        LogicalExpr::ScalarFnCall(fn_call) => {
            let rewritten_args: Vec<LogicalExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_expression_with_property_mapping(arg, ctx))
                .collect();

            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }

        // AggregateFnCall: Recursively rewrite arguments
        LogicalExpr::AggregateFnCall(agg) => {
            let rewritten_args: Vec<LogicalExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_expression_with_property_mapping(arg, ctx))
                .collect();

            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }

        // OperatorApplicationExp: Recursively rewrite operands
        LogicalExpr::OperatorApplicationExp(op) => {
            let rewritten_operands: Vec<LogicalExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_expression_with_property_mapping(operand, ctx))
                .collect();

            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: rewritten_operands,
            })
        }

        // Case: Recursively rewrite all parts
        LogicalExpr::Case(case) => {
            let new_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_expression_with_property_mapping(e, ctx)));

            let new_when_then: Vec<(LogicalExpr, LogicalExpr)> = case
                .when_then
                .iter()
                .map(|(when, then)| {
                    (
                        rewrite_expression_with_property_mapping(when, ctx),
                        rewrite_expression_with_property_mapping(then, ctx),
                    )
                })
                .collect();

            let new_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_expression_with_property_mapping(e, ctx)));

            LogicalExpr::Case(LogicalCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }

        // List: Recursively rewrite elements
        LogicalExpr::List(items) => {
            let rewritten_items: Vec<LogicalExpr> = items
                .iter()
                .map(|item| rewrite_expression_with_property_mapping(item, ctx))
                .collect();

            LogicalExpr::List(rewritten_items)
        }

        // ReduceExpr: Recursively rewrite components
        LogicalExpr::ReduceExpr(reduce) => LogicalExpr::ReduceExpr(ReduceExpr {
            accumulator: reduce.accumulator.clone(),
            initial_value: Box::new(rewrite_expression_with_property_mapping(
                &reduce.initial_value,
                ctx,
            )),
            variable: reduce.variable.clone(),
            list: Box::new(rewrite_expression_with_property_mapping(&reduce.list, ctx)),
            expression: Box::new(rewrite_expression_with_property_mapping(
                &reduce.expression,
                ctx,
            )),
        }),

        // MapLiteral: Recursively rewrite values
        LogicalExpr::MapLiteral(entries) => {
            let rewritten_entries: Vec<(String, LogicalExpr)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), rewrite_expression_with_property_mapping(v, ctx)))
                .collect();

            LogicalExpr::MapLiteral(rewritten_entries)
        }

        // ArraySubscript: Recursively rewrite array and index
        LogicalExpr::ArraySubscript { array, index } => LogicalExpr::ArraySubscript {
            array: Box::new(rewrite_expression_with_property_mapping(array, ctx)),
            index: Box::new(rewrite_expression_with_property_mapping(index, ctx)),
        },

        // ArraySlicing: Recursively rewrite array and bounds
        LogicalExpr::ArraySlicing { array, from, to } => LogicalExpr::ArraySlicing {
            array: Box::new(rewrite_expression_with_property_mapping(array, ctx)),
            from: from
                .as_ref()
                .map(|f| Box::new(rewrite_expression_with_property_mapping(f, ctx))),
            to: to
                .as_ref()
                .map(|t| Box::new(rewrite_expression_with_property_mapping(t, ctx))),
        },

        // InSubquery: Rewrite the expression part (subplan is a full plan, not expr)
        LogicalExpr::InSubquery(subq) => {
            LogicalExpr::InSubquery(crate::query_planner::logical_expr::InSubquery {
                expr: Box::new(rewrite_expression_with_property_mapping(&subq.expr, ctx)),
                subplan: subq.subplan.clone(),
            })
        }

        // Leaf expressions that don't need rewriting
        LogicalExpr::Literal(_)
        | LogicalExpr::Raw(_)
        | LogicalExpr::Star
        | LogicalExpr::TableAlias(_)
        | LogicalExpr::ColumnAlias(_)
        | LogicalExpr::Column(_)
        | LogicalExpr::Parameter(_)
        | LogicalExpr::PathPattern(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::LabelExpression { .. }
        | LogicalExpr::Lambda(_)
        | LogicalExpr::CteEntityRef(_)
        | LogicalExpr::Operator(_)
        | LogicalExpr::PatternCount(_)
        | LogicalExpr::PatternComprehension(_) => expr.clone(),
    }
}

/// Rewrite all expressions in a list of ProjectionItems.
///
/// This is a convenience function for WITH/RETURN clause processing.
pub fn rewrite_projection_items_with_property_mapping(
    items: &[crate::query_planner::logical_plan::ProjectionItem],
    ctx: &ExpressionRewriteContext,
) -> Vec<crate::query_planner::logical_plan::ProjectionItem> {
    items
        .iter()
        .map(|item| crate::query_planner::logical_plan::ProjectionItem {
            expression: rewrite_expression_with_property_mapping(&item.expression, ctx),
            col_alias: item.col_alias.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, TableAlias};

    #[test]
    fn test_rewrite_literal_unchanged() {
        // Literals should pass through unchanged
        let expr = LogicalExpr::Literal(Literal::Integer(42));
        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&expr, &ctx);

        assert_eq!(result, expr);
    }

    #[test]
    fn test_rewrite_table_alias_unchanged() {
        // TableAlias (variable reference) should pass through unchanged
        let expr = LogicalExpr::TableAlias(TableAlias("u".to_string()));
        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&expr, &ctx);

        assert_eq!(result, expr);
    }

    #[test]
    fn test_rewrite_nested_function_call() {
        // Function calls should have their arguments recursively processed
        let inner_expr = LogicalExpr::Literal(Literal::Integer(1));
        let fn_call = LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: "abs".to_string(),
            args: vec![inner_expr.clone()],
        });

        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&fn_call, &ctx);

        // The result should be structurally identical (literals don't change)
        match result {
            LogicalExpr::ScalarFnCall(sf) => {
                assert_eq!(sf.name, "abs");
                assert_eq!(sf.args.len(), 1);
            }
            _ => panic!("Expected ScalarFnCall"),
        }
    }
}
