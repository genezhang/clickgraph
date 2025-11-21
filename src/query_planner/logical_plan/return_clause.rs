use crate::{
    open_cypher_parser::ast::ReturnClause,
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem, ProjectionKind},
};
use std::sync::Arc;

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    println!("========================================");
    println!("⚠️ RETURN CLAUSE DISTINCT = {}", return_clause.distinct);
    println!("========================================");
    let projection_items: Vec<ProjectionItem> = return_clause
        .return_items
        .iter()
        .map(|item| item.clone().into())
        .collect();
    let result = Arc::new(LogicalPlan::Projection(Projection {
        input: plan,
        items: projection_items,
        kind: ProjectionKind::Return,
        distinct: return_clause.distinct,
    }));
    println!("DEBUG evaluate_return_clause: Created Projection with distinct={}", 
        if let LogicalPlan::Projection(p) = result.as_ref() { p.distinct } else { false });
    result
}
