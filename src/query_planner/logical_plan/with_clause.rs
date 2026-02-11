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
    open_cypher_parser::ast::{Expression, WithClause as AstWithClause, WithItem},
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{errors::LogicalPlanError, LogicalPlan, OrderByItem, ProjectionItem},
        plan_ctx::PlanCtx,
    },
};
use std::sync::Arc;

/// Type alias for pattern comprehension tuple with aggregation type
type PatternComprehensionInfo<'a> = (
    crate::open_cypher_parser::ast::PathPattern<'a>,
    Option<Box<Expression<'a>>>,
    Box<Expression<'a>>,
    crate::query_planner::logical_plan::AggregationType, // Aggregation type
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
    log::warn!(
        "🔍 evaluate_with_clause: Input plan type = {:?}",
        std::mem::discriminant(&*plan)
    );

    // Print the full plan structure to understand what we're receiving
    if let LogicalPlan::Filter(f) = plan.as_ref() {
        log::warn!(
            "🔍 evaluate_with_clause: Input is Filter with predicate: {:?}",
            f.predicate
        );
        log::warn!(
            "🔍 evaluate_with_clause: Filter input type: {:?}",
            std::mem::discriminant(&*f.input)
        );
    }

    log::debug!(
        "evaluate_with_clause: Starting with {} items",
        with_clause.with_items.len()
    );

    // Rewrite pattern comprehensions before converting to ProjectionItems
    // This handles patterns like: WITH a, size([(a)--() | 1]) AS neighborCount
    // Instead of creating Projection(GroupBy) nodes (which break in VLP path),
    // we extract metadata to be consumed at render time as CTE + LEFT JOIN.
    let (rewritten_with_items, pattern_comp_metas) = rewrite_with_pattern_comprehensions(
        with_clause.with_items.clone(),
        plan.clone(),
        plan_ctx,
    )?;

    log::warn!(
        "🔍 evaluate_with_clause: After pattern comprehension rewrite, plan type = {:?}",
        std::mem::discriminant(&*plan)
    );

    log::debug!(
        "evaluate_with_clause: After rewrite, have {} items",
        rewritten_with_items.len()
    );

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

    // Attach pattern comprehension metadata for render-time CTE generation
    with_node.pattern_comprehensions = pattern_comp_metas;

    Ok(Arc::new(LogicalPlan::WithClause(with_node)))
}

/// Rewrite pattern comprehensions in WITH items.
///
/// Instead of creating Projection(GroupBy) logical plan nodes (which break in the VLP path),
/// this extracts metadata that the render phase uses to generate CTE + LEFT JOIN SQL.
///
/// For example: `WITH a, size([(a)--() | 1]) AS allNeighboursCount`
/// - Rewrites `size([(a)--() | 1])` → `Variable("__pc_0")` in the WITH expression
/// - Returns `PatternComprehensionMeta` with correlation_var="a", agg_type=Count, etc.
/// - At render time, this becomes a CTE scanning all edge tables + LEFT JOIN
fn rewrite_with_pattern_comprehensions<'a>(
    with_items: Vec<WithItem<'a>>,
    _plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Result<
    (
        Vec<WithItem<'a>>,
        Vec<crate::query_planner::logical_plan::PatternComprehensionMeta>,
    ),
    LogicalPlanError,
> {
    let mut rewritten_items = Vec::new();
    let mut all_metas = Vec::new();
    let mut pc_counter = 0usize;

    log::debug!(
        "rewrite_with_pattern_comprehensions: Processing {} items",
        with_items.len()
    );

    // FIRST PASS: Pre-register simple variable pass-throughs
    for (idx, item) in with_items.iter().enumerate() {
        if let Expression::Variable(var_name) = &item.expression {
            if plan_ctx.get_table_ctx(var_name).is_ok() {
                log::info!(
                    "🔧 Pattern comprehension pre-registration: Variable '{}' from item[{}] already in context",
                    var_name, idx
                );
            }
        }
    }

    // SECOND PASS: Extract pattern comprehensions as metadata
    for (idx, item) in with_items.into_iter().enumerate() {
        let (rewritten_expr, pattern_comprehensions) =
            rewrite_expression_pattern_comprehensions(item.expression.clone());

        log::debug!(
            "rewrite_with_pattern_comprehensions: Item[{}] found {} pattern comprehensions",
            idx,
            pattern_comprehensions.len()
        );

        for (pattern, _where_clause, _projection, agg_type) in pattern_comprehensions {
            let correlation_var = extract_correlation_variable_from_pattern(&pattern, plan_ctx);
            if correlation_var.is_none() {
                log::warn!("⚠️  Pattern comprehension has no correlation variable - skipping");
                continue;
            }
            let correlation_var = correlation_var.unwrap();

            // Extract correlation variable's label from plan context
            let correlation_label = plan_ctx
                .get_table_ctx(&correlation_var)
                .ok()
                .and_then(|ctx| ctx.get_labels().cloned())
                .and_then(|labels| labels.into_iter().next())
                .unwrap_or_default();

            // Extract direction and relationship types from the pattern
            let (direction, rel_types) = extract_direction_and_rel_types(&pattern);

            // Determine the result alias from the WITH item
            let result_alias = item
                .alias
                .map(|a| a.to_string())
                .unwrap_or_else(|| format!("__pc_{}", pc_counter));

            log::info!(
                "🔧 Pattern comprehension meta: var='{}', label='{}', dir={:?}, rels={:?}, agg={:?}, alias='{}'",
                correlation_var, correlation_label, direction, rel_types, agg_type, result_alias
            );

            all_metas.push(
                crate::query_planner::logical_plan::PatternComprehensionMeta {
                    correlation_var: correlation_var.clone(),
                    correlation_label,
                    direction,
                    rel_types,
                    agg_type,
                    result_alias: result_alias.clone(),
                },
            );

            pc_counter += 1;
        }

        let new_item = WithItem {
            expression: rewritten_expr,
            alias: item.alias,
        };
        rewritten_items.push(new_item);
    }

    Ok((rewritten_items, all_metas))
}

/// Recursively rewrite pattern comprehensions in an expression.
/// Returns the rewritten expression and a list of extracted pattern comprehensions with aggregation types.
fn rewrite_expression_pattern_comprehensions<'a>(
    expr: Expression<'a>,
) -> (Expression<'a>, Vec<PatternComprehensionInfo<'a>>) {
    use crate::open_cypher_parser::ast::*;

    log::debug!(
        "🔄 rewrite_expression_pattern_comprehensions: expr_type={:?}",
        std::mem::discriminant(&expr)
    );

    match expr {
        Expression::PatternComprehension(pc) => {
            log::info!("🔄 Found bare PatternComprehension, replacing with collect()");
            // Found a bare pattern comprehension - collect it and replace with collect(projection)
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
                    crate::query_planner::logical_plan::AggregationType::GroupArray, // Bare list uses groupArray
                )],
            )
        }
        Expression::FunctionCallExp(func) => {
            log::debug!(
                "🔄 Checking FunctionCallExp '{}' for pattern comprehensions",
                func.name
            );

            let func_lower = func.name.to_lowercase();

            // Check for aggregation functions with pattern comprehensions
            if func.args.len() == 1 {
                if let Expression::PatternComprehension(pc) = &func.args[0] {
                    use crate::query_planner::logical_plan::AggregationType;

                    let agg_type = match func_lower.as_str() {
                        "size" | "length" => {
                            log::info!("🔄 size/length(PatternComprehension) detected → COUNT");
                            AggregationType::Count
                        }
                        "collect" => {
                            log::info!("🔄 collect(PatternComprehension) detected → GroupArray");
                            AggregationType::GroupArray
                        }
                        "sum" => {
                            log::info!("🔄 sum(PatternComprehension) detected → Sum");
                            AggregationType::Sum
                        }
                        "avg" => {
                            log::info!("🔄 avg(PatternComprehension) detected → Avg");
                            AggregationType::Avg
                        }
                        "min" => {
                            log::info!("🔄 min(PatternComprehension) detected → Min");
                            AggregationType::Min
                        }
                        "max" => {
                            log::info!("🔄 max(PatternComprehension) detected → Max");
                            AggregationType::Max
                        }
                        _ => {
                            // Not a recognized aggregation function - process normally
                            log::debug!(
                                "🔄 Function '{}' with PatternComprehension - processing args",
                                func.name
                            );
                            let mut all_pcs = Vec::new();
                            let new_args: Vec<Expression<'a>> = func
                                .args
                                .into_iter()
                                .map(|arg| {
                                    let (new_arg, pcs) =
                                        rewrite_expression_pattern_comprehensions(arg);
                                    all_pcs.extend(pcs);
                                    new_arg
                                })
                                .collect();
                            return (
                                Expression::FunctionCallExp(FunctionCall {
                                    name: func.name,
                                    args: new_args,
                                }),
                                all_pcs,
                            );
                        }
                    };

                    // For size/length, replace with count(*), for others replace with the function call
                    let replacement_expr = if matches!(agg_type, AggregationType::Count) {
                        Expression::FunctionCallExp(FunctionCall {
                            name: "count".to_string(),
                            args: vec![Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::String("*"),
                            )],
                        })
                    } else {
                        Expression::FunctionCallExp(FunctionCall {
                            name: func.name.clone(),
                            args: vec![(*pc.projection).clone()],
                        })
                    };

                    return (
                        replacement_expr,
                        vec![(
                            (*pc.pattern).clone(),
                            pc.where_clause.clone(),
                            pc.projection.clone(),
                            agg_type,
                        )],
                    );
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

// NOTE: WITH clause CTE column and alias tracking is handled exclusively via
// `PlanCtx`'s internal CTE registry when the logical plan is constructed, based on
// the actual WITH projection items. Previous attempt to build mappings here by
// introspecting node schemas was unsafe because it:
// 1. Assumed all properties were projected
// 2. Hard-coded node type-specific id columns (e.g., `user_id`)
// 3. Wrote to render-time context during logical planning
// 4. Could diverge from actual columns/aliases produced during SQL rendering

/// Extract the correlation variable from a pattern.
/// The correlation variable is a node alias from outer scope used in the pattern.
/// Example: [(a)--() | 1] → "a" is the correlation variable
pub(crate) fn extract_correlation_variable_from_pattern<'a>(
    pattern: &crate::open_cypher_parser::ast::PathPattern<'a>,
    plan_ctx: &PlanCtx,
) -> Option<String> {
    use crate::open_cypher_parser::ast::PathPattern;

    // Extract variables from pattern
    let mut variables = Vec::new();

    match pattern {
        PathPattern::Node(node) => {
            if let Some(name) = node.name {
                variables.push(name);
            }
        }
        PathPattern::ConnectedPattern(connected) => {
            for conn in connected {
                let start_node = conn.start_node.borrow();
                if let Some(name) = start_node.name {
                    variables.push(name);
                }
                let end_node = conn.end_node.borrow();
                if let Some(name) = end_node.name {
                    variables.push(name);
                }
            }
        }
        PathPattern::ShortestPath(inner) | PathPattern::AllShortestPaths(inner) => {
            return extract_correlation_variable_from_pattern(inner, plan_ctx);
        }
    }

    // Find the first variable that exists in parent context
    for var in variables {
        if plan_ctx.get_table_ctx(var).is_ok() {
            log::debug!("🔍 Found correlation variable '{}' in parent context", var);
            return Some(var.to_string());
        }
    }

    log::warn!("⚠️  No correlation variable found in pattern");
    None
}

/// Extract direction and relationship types from a pattern.
/// Returns (Direction, Option<Vec<rel_type_names>>).
pub(crate) fn extract_direction_and_rel_types(
    pattern: &crate::open_cypher_parser::ast::PathPattern<'_>,
) -> (
    crate::open_cypher_parser::ast::Direction,
    Option<Vec<String>>,
) {
    use crate::open_cypher_parser::ast::{Direction, PathPattern};

    match pattern {
        PathPattern::ConnectedPattern(connected) => {
            if let Some(conn) = connected.first() {
                let direction = conn.relationship.direction.clone();
                let rel_types = conn
                    .relationship
                    .labels
                    .as_ref()
                    .map(|labels| labels.iter().map(|l| l.to_string()).collect());
                (direction, rel_types)
            } else {
                (Direction::Either, None)
            }
        }
        PathPattern::ShortestPath(inner) | PathPattern::AllShortestPaths(inner) => {
            extract_direction_and_rel_types(inner)
        }
        _ => (Direction::Either, None),
    }
}
