use errors::PlannerError;

use crate::open_cypher_parser::ast::OpenCypherQueryAst;

use super::types::LogicalPlan;

pub mod errors;
mod eval_match_clause;
mod eval_order_by_clause;
mod eval_return_clause;
mod eval_skip_n_limit_clause;
mod eval_where_clause;
mod logical_plan;

pub fn evaluate_query(query_ast: OpenCypherQueryAst<'_>) -> Result<LogicalPlan<'_>, PlannerError> {
    logical_plan::evaluate_query(query_ast)
}
