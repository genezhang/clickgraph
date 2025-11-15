use crate::{
    open_cypher_parser::ast::WithClause,
    query_planner::logical_expr::LogicalExpr,
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem, ProjectionKind},
};
use std::sync::Arc;

/// Evaluate a WITH clause by creating a Projection node with ProjectionKind::With.
///
/// WITH semantics in Cypher (per OpenCypher spec):
/// - WITH uses <return statement body> - same as RETURN
/// - Specifies intermediate results to pass to the next part of the query
/// - When WITH contains aggregations → implicit GROUP BY on non-aggregated expressions
/// - When WITH has NO aggregations → just a projection/transformation
///
/// The Projection(kind=With) is later transformed by the GroupByBuilding analyzer pass:
/// - has aggregations → converts to GroupBy
/// - no aggregations → remains as Projection
/// - WHERE after WITH with aggregations → becomes HAVING clause
///
/// Example: `WITH a, COUNT(b) as follows` creates:
/// - Projection with kind: ProjectionKind::With, items: [a, COUNT(b) as follows]
/// - GroupByBuilding converts to: GroupBy with grouping: [a], projection: [a, COUNT(b)]
///
/// Example: `WITH a, b.name as name` creates:
/// - Projection with kind: ProjectionKind::With, items: [a, b.name as name]
/// - GroupByBuilding leaves as Projection (no aggregations)
pub fn evaluate_with_clause<'a>(
    with_clause: &WithClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let projection_items: Vec<ProjectionItem> = with_clause
        .with_items
        .iter()
        .map(|item| item.clone().into())
        .collect();

    println!(
        "WITH clause: Creating Projection(kind=With) with {} items",
        projection_items.len()
    );

    Arc::new(LogicalPlan::Projection(Projection {
        input: plan,
        items: projection_items,
        kind: ProjectionKind::With,
    }))
}
