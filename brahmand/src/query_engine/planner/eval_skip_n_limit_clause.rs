use crate::{
    open_cypher_parser::ast::{LimitClause, SkipClause},
    query_engine::types::LogicalPlan,
};

pub fn evaluate_skip_clause(
    mut logical_plan: LogicalPlan<'_>,
    skip_clause: SkipClause,
) -> LogicalPlan<'_> {
    logical_plan.skip = Some(skip_clause.skip_item);

    logical_plan
}

pub fn evaluate_limit_clause(
    mut logical_plan: LogicalPlan<'_>,
    limit_clause: LimitClause,
) -> LogicalPlan<'_> {
    logical_plan.limit = Some(limit_clause.limit_item);

    logical_plan
}
