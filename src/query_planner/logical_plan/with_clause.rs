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

/// Extracted info from a pattern comprehension (or list comprehension with pattern predicate).
/// Used to track the pattern, projection, aggregation type, and optional list constraint
/// during WITH/RETURN clause rewriting.
struct PatternComprehensionInfo<'a> {
    /// The path pattern from the comprehension (e.g., `(p)-[:HAS_TAG]->()`)
    pattern: crate::open_cypher_parser::ast::PathPattern<'a>,
    /// Optional WHERE clause from the comprehension
    where_clause: Option<Box<Expression<'a>>>,
    /// The projection expression (what gets collected/counted)
    projection: Box<Expression<'a>>,
    /// How this comprehension is aggregated (Count, GroupArray, Sum, etc.)
    aggregation_type: crate::query_planner::logical_plan::AggregationType,
    /// For list comprehensions: (iteration_var, list_alias) — e.g., ("p", "posts")
    list_constraint: Option<(String, String)>,
}

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

        for pc_info in pattern_comprehensions {
            let correlation_var =
                extract_correlation_variable_from_pattern(&pc_info.pattern, plan_ctx);
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
            let (direction, rel_types) = extract_direction_and_rel_types(&pc_info.pattern);

            // Determine the result alias from the WITH item
            let result_alias = item
                .alias
                .map(|a| a.to_string())
                .unwrap_or_else(|| format!("__pc_{}", pc_counter));

            log::info!(
                "🔧 Pattern comprehension meta: var='{}', label='{}', dir={:?}, rels={:?}, agg={:?}, alias='{}'",
                correlation_var, correlation_label, direction, rel_types, pc_info.aggregation_type, result_alias
            );

            // Extract full pattern info for correlated subquery generation
            let correlation_vars_info =
                extract_all_correlation_variables_from_pattern(&pc_info.pattern, plan_ctx);
            let mut pattern_hops_info = extract_connected_pattern_info(&pc_info.pattern);
            let pc_where_clause = pc_info
                .where_clause
                .as_ref()
                .and_then(|w| LogicalExpr::try_from(w.as_ref().clone()).ok());

            // Enrich hop labels from plan_ctx for variables without explicit labels
            for hop in &mut pattern_hops_info {
                if hop.start_label.is_none() {
                    if let Some(ref alias) = hop.start_alias {
                        if let Ok(ctx) = plan_ctx.get_table_ctx(alias) {
                            if let Some(labels) = ctx.get_labels() {
                                hop.start_label = labels.iter().next().cloned();
                            }
                        }
                    }
                }
                if hop.end_label.is_none() {
                    if let Some(ref alias) = hop.end_alias {
                        if let Ok(ctx) = plan_ctx.get_table_ctx(alias) {
                            if let Some(labels) = ctx.get_labels() {
                                hop.end_label = labels.iter().next().cloned();
                            }
                        }
                    }
                }
            }

            // For list comprehension, try to infer the iteration variable's label
            // from the list source. e.g., [p IN posts WHERE (p)-[:HAS_TAG]->()...] where
            // posts = collect(post:Post) -> source_label = "Post"
            let source_label = if let Some(ref lc) = pc_info.list_constraint {
                // Strategy 1: Look up list alias directly in plan_ctx
                plan_ctx
                    .get_table_ctx(&lc.1)
                    .ok()
                    .and_then(|ctx| ctx.get_labels().cloned())
                    .and_then(|labels| labels.into_iter().next())
                    .or_else(|| {
                        // Strategy 2: Scan input plan for previous WITH items containing
                        // collect(X) AS <list_alias>, then look up X's label in plan_ctx
                        find_collect_source_label(&_plan, &lc.1, plan_ctx)
                    })
                    .or_else(|| {
                        // Strategy 3: Check pattern hops first hop start_label
                        pattern_hops_info
                            .first()
                            .and_then(|h| h.start_label.clone())
                    })
            } else {
                None
            };

            all_metas.push(
                crate::query_planner::logical_plan::PatternComprehensionMeta {
                    correlation_var: correlation_var.clone(),
                    correlation_label,
                    direction,
                    rel_types,
                    agg_type: pc_info.aggregation_type,
                    result_alias: result_alias.clone(),
                    target_label: None,
                    target_property: None,
                    correlation_vars: correlation_vars_info,
                    pattern_hops: pattern_hops_info,
                    where_clause: pc_where_clause,
                    position_index: pc_counter,
                    list_constraint: pc_info.list_constraint.map(|(var, alias)| {
                        crate::query_planner::logical_plan::ListConstraint {
                            variable: var,
                            list_alias: alias,
                            source_label: source_label.clone(),
                        }
                    }),
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
                vec![PatternComprehensionInfo {
                    pattern: (*pc.pattern).clone(),
                    where_clause: pc.where_clause.clone(),
                    projection: pc.projection.clone(),
                    aggregation_type:
                        crate::query_planner::logical_plan::AggregationType::GroupArray,
                    list_constraint: None,
                }],
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
                        vec![PatternComprehensionInfo {
                            pattern: (*pc.pattern).clone(),
                            where_clause: pc.where_clause.clone(),
                            projection: pc.projection.clone(),
                            aggregation_type: agg_type,
                            list_constraint: None,
                        }],
                    );
                }
            }

            // Check for size/length of ListComprehension with pattern predicate
            // e.g., size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])
            if func.args.len() == 1 {
                if let Expression::ListComprehension(lc) = &func.args[0] {
                    if let Some(ref where_expr) = lc.where_clause {
                        if func_lower == "size" || func_lower == "length" {
                            // Check if the WHERE clause contains a path pattern
                            if let Some(path_pattern) =
                                extract_path_pattern_from_expression(where_expr)
                            {
                                log::info!(
                                    "🔄 size(ListComprehension) with pattern predicate detected"
                                );

                                // Extract the list alias from the list expression
                                let list_alias = match &*lc.list_expr {
                                    Expression::Variable(v) => v.to_string(),
                                    other => {
                                        log::warn!(
                                            "ListComprehension list expression is not a simple variable: {:?}. \
                                             Only variable references (e.g., [p IN posts WHERE ...]) are supported.",
                                            other
                                        );
                                        // Fall through to default function processing
                                        // instead of generating a bogus alias
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

                                let iteration_var = lc.variable.to_string();

                                // The identity projection (just the variable itself)
                                let projection = Box::new(Expression::Variable(lc.variable));

                                // Replace with count(*)
                                let replacement_expr = Expression::FunctionCallExp(FunctionCall {
                                    name: "count".to_string(),
                                    args: vec![Expression::Literal(
                                        crate::open_cypher_parser::ast::Literal::String("*"),
                                    )],
                                });

                                return (
                                    replacement_expr,
                                    vec![PatternComprehensionInfo {
                                        pattern: path_pattern,
                                        where_clause: None, // WHERE is already in the pattern
                                        projection,
                                        aggregation_type: crate::query_planner::logical_plan::AggregationType::Count,
                                        list_constraint: Some((iteration_var, list_alias)),
                                    }],
                                );
                            }
                        }
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

/// Extract ALL correlation variables from a pattern (not just the first one).
/// A correlation variable is a named node that already exists in the outer scope (plan_ctx).
/// Returns a Vec with position info for each correlated variable.
fn extract_all_correlation_variables_from_pattern(
    pattern: &crate::open_cypher_parser::ast::PathPattern<'_>,
    plan_ctx: &PlanCtx,
) -> Vec<crate::query_planner::logical_plan::CorrelationVarInfo> {
    use crate::open_cypher_parser::ast::PathPattern;
    use crate::query_planner::logical_plan::{CorrelationVarInfo, PatternPosition};

    let mut result = Vec::new();

    match pattern {
        PathPattern::Node(node) => {
            if let Some(name) = node.name {
                if plan_ctx.get_table_ctx(name).is_ok() {
                    let label = plan_ctx
                        .get_table_ctx(name)
                        .ok()
                        .and_then(|ctx| ctx.get_labels().cloned())
                        .and_then(|labels| labels.into_iter().next())
                        .unwrap_or_default();
                    result.push(CorrelationVarInfo {
                        var_name: name.to_string(),
                        label,
                        pattern_position: PatternPosition::StartOfHop(0),
                    });
                }
            }
        }
        PathPattern::ConnectedPattern(connected) => {
            for (hop_idx, conn) in connected.iter().enumerate() {
                let start_node = conn.start_node.borrow();
                if let Some(name) = start_node.name {
                    if plan_ctx.get_table_ctx(name).is_ok() {
                        let label = plan_ctx
                            .get_table_ctx(name)
                            .ok()
                            .and_then(|ctx| ctx.get_labels().cloned())
                            .and_then(|labels| labels.into_iter().next())
                            .unwrap_or_else(|| {
                                // Fall back to pattern label
                                start_node.first_label().unwrap_or("").to_string()
                            });
                        // Avoid duplicates (same var can appear as end of hop N-1 and start of hop N)
                        if !result.iter().any(|r| r.var_name == name) {
                            result.push(CorrelationVarInfo {
                                var_name: name.to_string(),
                                label,
                                pattern_position: PatternPosition::StartOfHop(hop_idx),
                            });
                        }
                    }
                }
                let end_node = conn.end_node.borrow();
                if let Some(name) = end_node.name {
                    if plan_ctx.get_table_ctx(name).is_ok() {
                        let label = plan_ctx
                            .get_table_ctx(name)
                            .ok()
                            .and_then(|ctx| ctx.get_labels().cloned())
                            .and_then(|labels| labels.into_iter().next())
                            .unwrap_or_else(|| end_node.first_label().unwrap_or("").to_string());
                        if !result.iter().any(|r| r.var_name == name) {
                            result.push(CorrelationVarInfo {
                                var_name: name.to_string(),
                                label,
                                pattern_position: PatternPosition::EndOfHop(hop_idx),
                            });
                        }
                    }
                }
            }
        }
        PathPattern::ShortestPath(inner) | PathPattern::AllShortestPaths(inner) => {
            return extract_all_correlation_variables_from_pattern(inner, plan_ctx);
        }
    }

    result
}

/// Convert AST PathPattern into serializable ConnectedPatternInfo vec.
/// Preserves labels, aliases, directions, and relationship types for each hop.
fn extract_connected_pattern_info(
    pattern: &crate::open_cypher_parser::ast::PathPattern<'_>,
) -> Vec<crate::query_planner::logical_plan::ConnectedPatternInfo> {
    use crate::open_cypher_parser::ast::PathPattern;
    use crate::query_planner::logical_plan::ConnectedPatternInfo;

    match pattern {
        PathPattern::ConnectedPattern(connected) => connected
            .iter()
            .map(|conn| {
                let start_node = conn.start_node.borrow();
                let end_node = conn.end_node.borrow();
                ConnectedPatternInfo {
                    start_label: start_node.first_label().map(|s| s.to_string()),
                    start_alias: start_node.name.map(|s| s.to_string()),
                    rel_type: conn
                        .relationship
                        .labels
                        .as_ref()
                        .and_then(|l| l.first())
                        .map(|s| s.to_string()),
                    rel_alias: conn.relationship.name.map(|s| s.to_string()),
                    direction: crate::query_planner::logical_expr::Direction::from(
                        conn.relationship.direction.clone(),
                    ),
                    end_label: end_node.first_label().map(|s| s.to_string()),
                    end_alias: end_node.name.map(|s| s.to_string()),
                }
            })
            .collect(),
        PathPattern::ShortestPath(inner) | PathPattern::AllShortestPaths(inner) => {
            extract_connected_pattern_info(inner)
        }
        PathPattern::Node(_) => vec![],
    }
}

/// Extract a PathPattern from an expression.
/// Used for list comprehension WHERE clauses that contain graph patterns.
/// e.g., `(p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)` → PathPattern
fn extract_path_pattern_from_expression<'a>(
    expr: &Expression<'a>,
) -> Option<crate::open_cypher_parser::ast::PathPattern<'a>> {
    match expr {
        Expression::PathPattern(pp) => Some(pp.clone()),
        Expression::OperatorApplicationExp(op) => {
            // Check operands for path patterns (e.g., AND/OR combinations)
            for operand in &op.operands {
                if let Some(pp) = extract_path_pattern_from_expression(operand) {
                    return Some(pp);
                }
            }
            None
        }
        _ => None,
    }
}

/// Scan the input plan for a previous WithClause that contains `collect(X) AS list_alias`,
/// then look up X's label in plan_ctx. This traces the source node type through collect()
/// for list comprehension patterns like `[p IN posts WHERE ...]`.
fn find_collect_source_label(
    plan: &LogicalPlan,
    list_alias: &str,
    plan_ctx: &PlanCtx,
) -> Option<String> {
    match plan {
        LogicalPlan::WithClause(wc) => {
            // Check this WithClause's items for collect(X) AS list_alias
            for item in &wc.items {
                let alias_name = item.col_alias.as_ref().map(|a| a.0.as_str()).or_else(|| {
                    if let LogicalExpr::TableAlias(ref v) = item.expression {
                        Some(v.0.as_str())
                    } else if let LogicalExpr::ColumnAlias(ref v) = item.expression {
                        Some(v.0.as_str())
                    } else {
                        None
                    }
                });
                if alias_name == Some(list_alias) {
                    // Found the item producing list_alias. Check if it's collect(X).
                    if let Some(source_var) = extract_collect_source_var(&item.expression) {
                        log::info!(
                            "🔧 find_collect_source_label: '{}' = collect('{}'), looking up label",
                            list_alias,
                            source_var
                        );
                        // Strategy A: Look up in plan_ctx
                        if let Ok(ctx) = plan_ctx.get_table_ctx(&source_var) {
                            if let Some(labels) = ctx.get_labels() {
                                if let Some(label) = labels.iter().next().cloned() {
                                    return Some(label);
                                }
                            }
                        }
                        // Strategy B: Scan the plan tree for a GraphNode with this alias
                        if let Some(label) = find_graph_node_label(&wc.input, &source_var) {
                            log::info!(
                                "🔧 find_collect_source_label: found label '{}' from plan tree for '{}'",
                                label, source_var
                            );
                            return Some(label);
                        }
                    }
                }
            }
            // Recurse into input plan
            find_collect_source_label(&wc.input, list_alias, plan_ctx)
        }
        LogicalPlan::Filter(f) => find_collect_source_label(&f.input, list_alias, plan_ctx),
        LogicalPlan::GraphJoins(gj) => find_collect_source_label(&gj.input, list_alias, plan_ctx),
        LogicalPlan::OrderBy(o) => find_collect_source_label(&o.input, list_alias, plan_ctx),
        LogicalPlan::Limit(l) => find_collect_source_label(&l.input, list_alias, plan_ctx),
        LogicalPlan::GraphRel(gr) => {
            if let Some(label) = find_collect_source_label(&gr.left, list_alias, plan_ctx) {
                return Some(label);
            }
            find_collect_source_label(&gr.right, list_alias, plan_ctx)
        }
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(label) = find_collect_source_label(&cp.left, list_alias, plan_ctx) {
                return Some(label);
            }
            find_collect_source_label(&cp.right, list_alias, plan_ctx)
        }
        _ => None,
    }
}

/// Extract the source variable from a collect/groupArray expression.
/// e.g., `AggregateFnCall { name: "collect", args: [Variable("post")] }` → "post"
fn extract_collect_source_var(expr: &LogicalExpr) -> Option<String> {
    match expr {
        LogicalExpr::AggregateFnCall(agg) => {
            let name_lower = agg.name.to_lowercase();
            if name_lower == "collect" || name_lower == "grouparray" {
                if let Some(first_arg) = agg.args.first() {
                    return match first_arg {
                        LogicalExpr::TableAlias(v) => Some(v.0.clone()),
                        LogicalExpr::ColumnAlias(v) => Some(v.0.clone()),
                        _ => None,
                    };
                }
            }
            None
        }
        LogicalExpr::ScalarFnCall(func) => {
            let name_lower = func.name.to_lowercase();
            if name_lower == "collect" || name_lower == "grouparray" {
                if let Some(first_arg) = func.args.first() {
                    return match first_arg {
                        LogicalExpr::TableAlias(v) => Some(v.0.clone()),
                        LogicalExpr::ColumnAlias(v) => Some(v.0.clone()),
                        _ => None,
                    };
                }
            }
            None
        }
        _ => None,
    }
}

/// Scan a logical plan tree for a GraphNode with the given alias and return its label.
fn find_graph_node_label(plan: &LogicalPlan, alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            if gn.alias == alias {
                return gn.label.clone();
            }
            find_graph_node_label(&gn.input, alias)
        }
        LogicalPlan::GraphRel(gr) => {
            if let Some(label) = find_graph_node_label(&gr.left, alias) {
                return Some(label);
            }
            find_graph_node_label(&gr.right, alias)
        }
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(label) = find_graph_node_label(&cp.left, alias) {
                return Some(label);
            }
            find_graph_node_label(&cp.right, alias)
        }
        LogicalPlan::Filter(f) => find_graph_node_label(&f.input, alias),
        LogicalPlan::GraphJoins(gj) => find_graph_node_label(&gj.input, alias),
        LogicalPlan::WithClause(wc) => find_graph_node_label(&wc.input, alias),
        LogicalPlan::Union(u) => {
            for branch in &u.inputs {
                if let Some(label) = find_graph_node_label(branch, alias) {
                    return Some(label);
                }
            }
            None
        }
        _ => None,
    }
}
