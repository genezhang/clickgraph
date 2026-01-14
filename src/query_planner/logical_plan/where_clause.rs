use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::WhereClause,
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{Filter, LogicalPlan, Union},
    },
};

pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let predicates: LogicalExpr = LogicalExpr::try_from(where_clause.conditions.clone()).unwrap();
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

        return Arc::new(LogicalPlan::Union(Union {
            inputs: filtered_branches,
            union_type: union.union_type.clone(),
        }));
    }

    Arc::new(LogicalPlan::Filter(Filter {
        input: plan,
        predicate: predicates,
    }))
}
