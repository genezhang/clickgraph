use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_plan::{
            errors::LogicalPlanError,
            match_clause::evaluate_match_clause,
            plan_builder::LogicalPlanResult,
            LogicalPlan,
        },
        plan_ctx::PlanCtx,
    },
};

/// Evaluate an OPTIONAL MATCH clause
/// 
/// OPTIONAL MATCH uses LEFT JOIN semantics - all rows from the input are preserved,
/// with NULL values for unmatched optional patterns.
/// 
/// Strategy:
/// 1. Process patterns similar to regular MATCH
/// 2. Convert generated INNER JOINs to LEFT JOINs
/// 3. Preserve null values for unmatched patterns
pub fn evaluate_optional_match_clause<'a>(
    optional_match_clause: &ast::OptionalMatchClause<'a>,
    input_plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    // Create a temporary MatchClause from the OptionalMatchClause
    // This allows us to reuse the existing match clause logic
    let temp_match_clause = ast::MatchClause {
        path_patterns: optional_match_clause.path_patterns.clone(),
    };

    // Process the patterns using the regular match clause evaluator
    let mut plan = evaluate_match_clause(&temp_match_clause, input_plan, plan_ctx)?;

    // TODO: Convert INNER JOINs to LEFT JOINs
    // This will be done in the SQL generation phase for now
    // We need to mark these patterns as "optional" in the plan context

    // If there's a WHERE clause specific to this OPTIONAL MATCH,
    // it should be applied as part of the JOIN condition, not as a final filter
    if let Some(where_clause) = &optional_match_clause.where_clause {
        // Store the WHERE clause in the plan context for later processing
        // During SQL generation, this will become part of the LEFT JOIN ON condition
        // For now, we'll add it as a regular filter
        // TODO: Properly handle WHERE clauses in OPTIONAL MATCH
        use crate::query_planner::logical_plan::where_clause::evaluate_where_clause;
        plan = evaluate_where_clause(where_clause, plan);
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast;

    #[test]
    fn test_evaluate_optional_match_simple_node() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                label: Some("User"),
                properties: None,
            })],
            where_clause: None,
        };

        let input_plan = Arc::new(LogicalPlan::Empty);
        let mut plan_ctx = PlanCtx::default();

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluate_optional_match_with_where() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                label: Some("User"),
                properties: None,
            })],
            where_clause: Some(ast::WhereClause {
                conditions: ast::Expression::OperatorApplicationExp(ast::OperatorApplication {
                    operator: ast::Operator::GreaterThan,
                    operands: vec![
                        ast::Expression::PropertyAccessExp(ast::PropertyAccess {
                            base: "a",
                            key: "age",
                        }),
                        ast::Expression::Literal(ast::Literal::Integer(25)),
                    ],
                }),
            }),
        };

        let input_plan = Arc::new(LogicalPlan::Empty);
        let mut plan_ctx = PlanCtx::default();

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }
}
