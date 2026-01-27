use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::OrderByClause,
    query_planner::logical_plan::{errors::LogicalPlanError, LogicalPlan, OrderBy, OrderByItem},
};

pub fn evaluate_order_by_clause<'a>(
    order_by_clause: &OrderByClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Result<Arc<LogicalPlan>, LogicalPlanError> {
    let predicates: Result<Vec<OrderByItem>, _> = order_by_clause
        .order_by_items
        .iter()
        .map(|item| OrderByItem::try_from(item.clone()))
        .collect();
    let predicates = predicates.map_err(|e| {
        LogicalPlanError::QueryPlanningError(format!("Failed to convert ORDER BY item: {}", e))
    })?;
    Ok(Arc::new(LogicalPlan::OrderBy(OrderBy {
        input: plan,
        items: predicates,
    })))
}
