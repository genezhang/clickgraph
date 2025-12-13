use crate::{
    open_cypher_parser::ast::WithClause as AstWithClause,
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{errors::LogicalPlanError, LogicalPlan, OrderByItem, ProjectionItem, WithClause},
    },
};
use std::sync::Arc;

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
) -> Result<Arc<LogicalPlan>, LogicalPlanError> {
    let projection_items: Vec<ProjectionItem> = with_clause
        .with_items
        .iter()
        .map(|item| item.clone().into())
        .collect();

    println!(
        "WITH clause: Creating WithClause with {} items, distinct={}, order_by={:?}, skip={:?}, limit={:?}",
        projection_items.len(),
        with_clause.distinct,
        with_clause.order_by.is_some(),
        with_clause.skip.is_some(),
        with_clause.limit.is_some()
    );

    // Create the new WithClause type with all modifiers - returns error if items lack required aliases
    let mut with_node = WithClause::new(plan, projection_items)?.with_distinct(with_clause.distinct);

    // Add ORDER BY if present
    if let Some(ref order_by_ast) = with_clause.order_by {
        let order_by_items: Vec<OrderByItem> = order_by_ast
            .order_by_items
            .iter()
            .map(|item| item.clone().into())
            .collect();
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
        let predicate: LogicalExpr = where_ast.conditions.clone().into();
        with_node = with_node.with_where(predicate);
    }

    Ok(Arc::new(LogicalPlan::WithClause(with_node)))
}
