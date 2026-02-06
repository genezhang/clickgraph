//! WITH clause processing.
//!
//! Handles Cypher's WITH clause which creates scope boundaries and
//! intermediate projections between query segments.
//!
//! # Key Semantics
//!
//! - **Scope Boundary**: Only exported aliases visible downstream
//! - **Materialization**: Maps to SQL CTE in rendering
//! - **Aggregation**: When containing aggregates, transformed to GroupBy
//! - **Modifiers**: Supports ORDER BY, SKIP, LIMIT, WHERE within WITH
//!
//! # SQL Translation
//!
//! ```text
//! WITH a, count(b) AS follows ORDER BY follows DESC LIMIT 10
//! -> WITH cte AS (SELECT a, count(b) AS follows ... GROUP BY a ORDER BY follows DESC LIMIT 10)
//! ```
//!
//! # Difference from RETURN
//!
//! Unlike RETURN (final projection), WITH:
//! - Bridges to continuation (next MATCH/RETURN)
//! - Creates scope isolation
//! - Has ORDER BY, SKIP, LIMIT, WHERE as part of its syntax

use crate::{
    open_cypher_parser::ast::{Case, Expression, WithClause as AstWithClause, WithItem},
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{
            errors::LogicalPlanError,
            optional_match_clause::evaluate_optional_match_clause, LogicalPlan,
            OrderByItem, ProjectionItem,
        },
        plan_ctx::PlanCtx,
    },
};
use std::sync::Arc;

/// Type alias for pattern comprehension tuple (same as return_clause.rs)
type PatternComprehension<'a> = (
    crate::open_cypher_parser::ast::PathPattern<'a>,
    Option<Box<Expression<'a>>>,
    Box<Expression<'a>>,
);

/// Evaluate a WITH clause by creating a WithClause node.
///
/// WITH semantics in Cypher (per OpenCypher spec):
/// - WITH uses <return statement body> - same as RETURN
/// - Specifies intermediate results to pass to the next part of the query
/// - Creates a scope boundary - downstream clauses only see exported aliases
/// - When WITH contains aggregations → later transformed to GroupBy by GroupByBuilding pass
///
/// OpenCypher syntax: WITH [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT m] [WHERE ...]
///
/// Example: `WITH a, COUNT(b) as follows` creates:
/// - WithClause with items: [a, COUNT(b) as follows], exported_aliases: [a, follows]
/// - GroupByBuilding later converts to: GroupBy with grouping: [a], projection: [a, COUNT(b)]
///
/// Example: `WITH a, b.name as name` creates:
/// - WithClause with items: [a, b.name as name], exported_aliases: [a, name]
/// - GroupByBuilding leaves as WithClause (no aggregations)
///
/// Returns an error if any WITH item lacks a required alias (complex expressions must have aliases).
pub fn evaluate_with_clause<'a>(
    with_clause: &AstWithClause<'a>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Result<Arc<LogicalPlan>, LogicalPlanError> {
    log::warn!("🔍 evaluate_with_clause: Starting");
    log::warn!("🔍 evaluate_with_clause: Input plan type = {:?}", std::mem::discriminant(&*plan));
    
    // Print the full plan structure to understand what we're receiving
    if let LogicalPlan::Filter(f) = plan.as_ref() {
        log::warn!("🔍 evaluate_with_clause: Input is Filter with predicate: {:?}", f.predicate);
        log::warn!("🔍 evaluate_with_clause: Filter input type: {:?}", std::mem::discriminant(&*f.input));
    }
    
    log::debug!("evaluate_with_clause: Starting with {} items", with_clause.with_items.len());
    
    // Rewrite pattern comprehensions before converting to ProjectionItems
    // This handles patterns like: WITH a, size([(a)--() | 1]) AS neighborCount
    let (rewritten_with_items, plan) =
        rewrite_with_pattern_comprehensions(with_clause.with_items.clone(), plan, plan_ctx)?;

    log::warn!("🔍 evaluate_with_clause: After pattern comprehension rewrite, plan type = {:?}", std::mem::discriminant(&*plan));

    log::debug!("evaluate_with_clause: After rewrite, have {} items", rewritten_with_items.len());

    let projection_items: Vec<ProjectionItem> = rewritten_with_items
        .iter()
        .map(|item| ProjectionItem::try_from(item.clone()))
        .collect::<Result<Vec<_>, _>>()?;

    log::debug!(
        "WITH clause: Creating WithClause with {} items, distinct={}, order_by={:?}, skip={:?}, limit={:?}",
        projection_items.len(),
        with_clause.distinct,
        with_clause.order_by.is_some(),
        with_clause.skip.is_some(),
        with_clause.limit.is_some()
    );

    // Create the new WithClause type with all modifiers - returns error if items lack required aliases
    let mut with_node =
        crate::query_planner::logical_plan::WithClause::new(plan, projection_items)?
            .with_distinct(with_clause.distinct);

    // Add ORDER BY if present
    if let Some(ref order_by_ast) = with_clause.order_by {
        let order_by_items: Result<Vec<OrderByItem>, _> = order_by_ast
            .order_by_items
            .iter()
            .map(|item| OrderByItem::try_from(item.clone()))
            .collect();
        let order_by_items = order_by_items.map_err(|e| {
            LogicalPlanError::QueryPlanningError(format!(
                "Failed to convert WITH ORDER BY item: {}",
                e
            ))
        })?;
        with_node = with_node.with_order_by(order_by_items);
    }

    // Add SKIP if present
    if let Some(ref skip_ast) = with_clause.skip {
        with_node = with_node.with_skip(skip_ast.skip_item as u64);
    }

    // Add LIMIT if present
    if let Some(ref limit_ast) = with_clause.limit {
        with_node = with_node.with_limit(limit_ast.limit_item as u64);
    }

    // Add WHERE if present
    if let Some(ref where_ast) = with_clause.where_clause {
        let predicate: LogicalExpr =
            LogicalExpr::try_from(where_ast.conditions.clone()).map_err(|e| {
                LogicalPlanError::QueryPlanningError(format!(
                    "Failed to convert WITH WHERE expression: {}",
                    e
                ))
            })?;
        with_node = with_node.with_where(predicate);
    }

    Ok(Arc::new(LogicalPlan::WithClause(with_node)))
}

/// Rewrite pattern comprehensions in WITH items before converting to ProjectionItems.
/// This mirrors the rewriting done in return_clause.rs for RETURN items.
///
/// Pattern comprehensions like `[(a)--() | 1]` are converted to:
/// 1. An OPTIONAL MATCH for the pattern
/// 2. collect(projection) to aggregate the results
fn rewrite_with_pattern_comprehensions<'a>(
    with_items: Vec<WithItem<'a>>,
    mut plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Result<(Vec<WithItem<'a>>, Arc<LogicalPlan>), LogicalPlanError> {
    let mut rewritten_items = Vec::new();
    log::debug!("rewrite_with_pattern_comprehensions: Processing {} items", with_items.len());
    log::warn!("🔍 rewrite_with_pattern_comprehensions: Input plan type = {:?}", std::mem::discriminant(&*plan));

    for (idx, item) in with_items.into_iter().enumerate() {
        log::debug!("rewrite_with_pattern_comprehensions: Item[{}] alias={:?}, expr_type={:?}", 
                   idx, item.alias, std::mem::discriminant(&item.expression));
        
        // Recursively rewrite pattern comprehensions in the expression
        let (rewritten_expr, pattern_comprehensions) =
            rewrite_expression_pattern_comprehensions(item.expression.clone());
        
        log::debug!("rewrite_with_pattern_comprehensions: Item[{}] found {} pattern comprehensions", 
                   idx, pattern_comprehensions.len());

        // Add OPTIONAL MATCH nodes for each pattern comprehension found
        for (pattern, where_clause, _projection) in pattern_comprehensions {
            let optional_match = crate::open_cypher_parser::ast::OptionalMatchClause {
                path_patterns: vec![pattern],
                where_clause: where_clause.as_ref().map(|w| {
                    crate::open_cypher_parser::ast::WhereClause {
                        conditions: (**w).clone(),
                    }
                }),
            };

            plan = match evaluate_optional_match_clause(&optional_match, plan.clone(), plan_ctx) {
                Ok(new_plan) => new_plan,
                Err(e) => {
                    // Don't silently fail - propagate the error
                    // Pattern comprehension requires the OPTIONAL MATCH to work correctly
                    log::error!(
                        "Pattern comprehension OPTIONAL MATCH failed: {:?}",
                        e
                    );
                    return Err(LogicalPlanError::QueryPlanningError(
                        format!("Failed to plan pattern comprehension: {}", e)
                    ));
                }
            };
        }

        // Create new WITH item with rewritten expression
        let new_item = WithItem {
            expression: rewritten_expr,
            alias: item.alias,
        };
        rewritten_items.push(new_item);
    }

    Ok((rewritten_items, plan))
}

/// Recursively rewrite pattern comprehensions in an expression.
/// Returns the rewritten expression and a list of extracted pattern comprehensions.
fn rewrite_expression_pattern_comprehensions<'a>(
    expr: Expression<'a>,
) -> (Expression<'a>, Vec<PatternComprehension<'a>>) {
    use crate::open_cypher_parser::ast::*;

    log::debug!("🔄 rewrite_expression_pattern_comprehensions: expr_type={:?}", std::mem::discriminant(&expr));

    match expr {
        Expression::PatternComprehension(pc) => {
            log::info!("🔄 Found PatternComprehension, replacing with collect()");
            // Found a pattern comprehension - collect it and replace with collect(projection)
            let collect_call = Expression::FunctionCallExp(FunctionCall {
                name: "collect".to_string(),
                args: vec![(*pc.projection).clone()],
            });
            (
                collect_call,
                vec![(
                    (*pc.pattern).clone(),
                    pc.where_clause.clone(),
                    pc.projection.clone(),
                )],
            )
        }
        Expression::FunctionCallExp(func) => {
            log::debug!("🔄 Checking FunctionCallExp '{}' for size(PatternComprehension)", func.name);
            // Special case: size(PatternComprehension) should become count(*)
            // NOT size(collect(projection)) which is semantically wrong
            let func_lower = func.name.to_lowercase();
            if func_lower == "size" || func_lower == "length" {
                log::debug!("🔄 Found size/length with {} args", func.args.len());
                if func.args.len() == 1 {
                    log::debug!("🔄 Checking if arg is PatternComprehension: {:?}", std::mem::discriminant(&func.args[0]));
                    if let Expression::PatternComprehension(pc) = &func.args[0] {
                        log::info!("🔄 size(PatternComprehension) detected, replacing with count(*)");
                        // Replace size([(pattern) | proj]) with count(*)
                        // The pattern will be added as OPTIONAL MATCH
                        let count_call = Expression::FunctionCallExp(FunctionCall {
                            name: "count".to_string(),
                            args: vec![Expression::Literal(crate::open_cypher_parser::ast::Literal::String("*"))],
                        });
                        return (
                            count_call,
                            vec![(
                                (*pc.pattern).clone(),
                                pc.where_clause.clone(),
                                pc.projection.clone(),
                            )],
                        );
                    }
                }
            }
            
            // Default: Recursively process function arguments
            let mut all_pcs = Vec::new();
            let new_args: Vec<Expression<'a>> = func
                .args
                .into_iter()
                .map(|arg| {
                    let (new_arg, pcs) = rewrite_expression_pattern_comprehensions(arg);
                    all_pcs.extend(pcs);
                    new_arg
                })
                .collect();
            (
                Expression::FunctionCallExp(FunctionCall {
                    name: func.name,
                    args: new_args,
                }),
                all_pcs,
            )
        }
        Expression::OperatorApplicationExp(op) => {
            let mut all_pcs = Vec::new();
            let new_operands: Vec<Expression<'a>> = op
                .operands
                .into_iter()
                .map(|operand| {
                    let (new_op, pcs) = rewrite_expression_pattern_comprehensions(operand);
                    all_pcs.extend(pcs);
                    new_op
                })
                .collect();
            (
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                }),
                all_pcs,
            )
        }
        Expression::List(items) => {
            let mut all_pcs = Vec::new();
            let new_items: Vec<Expression<'a>> = items
                .into_iter()
                .map(|item| {
                    let (new_item, pcs) = rewrite_expression_pattern_comprehensions(item);
                    all_pcs.extend(pcs);
                    new_item
                })
                .collect();
            (Expression::List(new_items), all_pcs)
        }
        Expression::Case(case_expr) => {
            let mut all_pcs = Vec::new();

            let new_expr = case_expr.expr.map(|e| {
                let (new_e, pcs) = rewrite_expression_pattern_comprehensions(*e);
                all_pcs.extend(pcs);
                Box::new(new_e)
            });

            let new_when_then: Vec<(Expression<'a>, Expression<'a>)> = case_expr
                .when_then
                .into_iter()
                .map(|(when, then)| {
                    let (new_when, pcs1) = rewrite_expression_pattern_comprehensions(when);
                    let (new_then, pcs2) = rewrite_expression_pattern_comprehensions(then);
                    all_pcs.extend(pcs1);
                    all_pcs.extend(pcs2);
                    (new_when, new_then)
                })
                .collect();

            let new_else = case_expr.else_expr.map(|e| {
                let (new_e, pcs) = rewrite_expression_pattern_comprehensions(*e);
                all_pcs.extend(pcs);
                Box::new(new_e)
            });

            (
                Expression::Case(Case {
                    expr: new_expr,
                    when_then: new_when_then,
                    else_expr: new_else,
                }),
                all_pcs,
            )
        }
        // All other expressions don't contain pattern comprehensions
        _ => (expr, vec![]),
    }
}
