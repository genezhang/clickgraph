use crate::{
    open_cypher_parser::ast::WithClause as AstWithClause,
    query_planner::logical_plan::{LogicalPlan, ProjectionItem, WithClause},
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
/// NOTE: ORDER BY, SKIP, LIMIT, WHERE are part of WITH syntax but currently parsed
/// at the query level. TODO: Move these into WithClause during Phase 2 parser updates.
///
/// Example: `WITH a, COUNT(b) as follows` creates:
/// - WithClause with items: [a, COUNT(b) as follows], exported_aliases: [a, follows]
/// - GroupByBuilding later converts to: GroupBy with grouping: [a], projection: [a, COUNT(b)]
///
/// Example: `WITH a, b.name as name` creates:
/// - WithClause with items: [a, b.name as name], exported_aliases: [a, name]
/// - GroupByBuilding leaves as WithClause (no aggregations)
pub fn evaluate_with_clause<'a>(
    with_clause: &AstWithClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let projection_items: Vec<ProjectionItem> = with_clause
        .with_items
        .iter()
        .map(|item| item.clone().into())
        .collect();

    println!(
        "WITH clause: Creating WithClause with {} items",
        projection_items.len()
    );

    // Create the new WithClause type
    let with_node = WithClause::new(plan, projection_items);

    Arc::new(LogicalPlan::WithClause(with_node))
}
