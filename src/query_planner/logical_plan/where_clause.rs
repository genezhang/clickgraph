use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::WhereClause,
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{errors::LogicalPlanError, Filter, LogicalPlan, Union},
    },
};

pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Result<Arc<LogicalPlan>, LogicalPlanError> {
    let predicates: LogicalExpr =
        LogicalExpr::try_from(where_clause.conditions.clone()).map_err(|e| {
            LogicalPlanError::QueryPlanningError(format!(
                "Failed to convert WHERE clause expression: {}",
                e
            ))
        })?;
    log::debug!(
        "evaluate_where_clause: WHERE predicate after conversion: {:?}",
        predicates
    );

    // If input is a Union, push Filter into each branch
    // Each branch needs its own copy of the filter (will be mapped to correct columns by FilterTagging)
    if let LogicalPlan::Union(union) = plan.as_ref() {
        let filtered_branches: Vec<Arc<LogicalPlan>> = union
            .inputs
            .iter()
            .map(|branch| {
                Arc::new(LogicalPlan::Filter(Filter {
                    input: branch.clone(),
                    predicate: predicates.clone(),
                }))
            })
            .collect();

        return Ok(Arc::new(LogicalPlan::Union(Union {
            inputs: filtered_branches,
            union_type: union.union_type.clone(),
        })));
    }

    Ok(Arc::new(LogicalPlan::Filter(Filter {
        input: plan,
        predicate: predicates,
    })))
}
