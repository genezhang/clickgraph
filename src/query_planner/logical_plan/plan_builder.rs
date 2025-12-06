use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::{
        logical_plan::{
            LogicalPlan, errors::LogicalPlanError, match_clause, optional_match_clause,
            order_by_clause, return_clause, skip_n_limit_clause, unwind_clause, where_clause, with_clause,
        },
        plan_ctx::PlanCtx,
    },
};

pub type LogicalPlanResult<T> = Result<T, LogicalPlanError>;

pub fn build_logical_plan(
    query_ast: &OpenCypherQueryAst,
    schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    let mut logical_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Empty);
    let mut plan_ctx =
        PlanCtx::with_parameters(Arc::new(schema.clone()), tenant_id, view_parameter_values);

    log::debug!(
        "build_logical_plan: Processing query with {} optional_match_clauses",
        query_ast.optional_match_clauses.len()
    );

    if let Some(match_clause) = &query_ast.match_clause {
        logical_plan =
            match_clause::evaluate_match_clause(match_clause, logical_plan, &mut plan_ctx)?;
    }

    // Process OPTIONAL MATCH clauses after regular MATCH
    log::debug!(
        "build_logical_plan: About to process {} OPTIONAL MATCH clauses",
        query_ast.optional_match_clauses.len()
    );
    for (idx, optional_match) in query_ast.optional_match_clauses.iter().enumerate() {
        log::debug!(
            "build_logical_plan: Processing OPTIONAL MATCH clause {}",
            idx
        );
        logical_plan = optional_match_clause::evaluate_optional_match_clause(
            optional_match,
            logical_plan,
            &mut plan_ctx,
        )?;
    }

    // Process UNWIND clause after MATCH/OPTIONAL MATCH, before WITH
    // UNWIND transforms array values into individual rows
    if let Some(unwind_clause_ast) = &query_ast.unwind_clause {
        log::debug!(
            "build_logical_plan: Processing UNWIND clause with alias {}",
            unwind_clause_ast.alias
        );
        logical_plan = unwind_clause::evaluate_unwind_clause(unwind_clause_ast, logical_plan, &mut plan_ctx);
    }

    // Process WITH clause before WHERE to create intermediate projections
    // WITH creates a projection that can be referenced by subsequent clauses (including WHERE)
    if let Some(with_clause_ast) = &query_ast.with_clause {
        log::debug!(
            "build_logical_plan: Processing WITH clause with {} items",
            with_clause_ast.with_items.len()
        );
        logical_plan = with_clause::evaluate_with_clause(with_clause_ast, logical_plan);
        
        // Process subsequent UNWIND clause if present (e.g., WITH d, rip UNWIND rip.ips AS ip)
        if let Some(subsequent_unwind) = &with_clause_ast.subsequent_unwind {
            log::debug!("build_logical_plan: Processing subsequent UNWIND clause after WITH");
            logical_plan = unwind_clause::evaluate_unwind_clause(subsequent_unwind, logical_plan, &mut plan_ctx);
        }
        
        // Process subsequent MATCH clause if present (e.g., WITH u MATCH (u)-[:FOLLOWS]->(f))
        if let Some(subsequent_match) = &with_clause_ast.subsequent_match {
            log::debug!("build_logical_plan: Processing subsequent MATCH clause after WITH");
            logical_plan =
                match_clause::evaluate_match_clause(subsequent_match, logical_plan, &mut plan_ctx)?;
        }
        
        // Process subsequent OPTIONAL MATCH clauses if present
        for (idx, optional_match) in with_clause_ast.subsequent_optional_matches.iter().enumerate() {
            log::debug!(
                "build_logical_plan: Processing subsequent OPTIONAL MATCH clause {} after WITH",
                idx
            );
            logical_plan = optional_match_clause::evaluate_optional_match_clause(
                optional_match,
                logical_plan,
                &mut plan_ctx,
            )?;
        }
    }

    // Process WHERE clause after WITH so it can reference WITH projection aliases
    // For "WITH a, COUNT(b) as follows WHERE follows > 1", the WHERE can now reference "follows"
    if let Some(where_clause) = &query_ast.where_clause {
        logical_plan = where_clause::evaluate_where_clause(where_clause, logical_plan);
    }

    if let Some(return_clause) = &query_ast.return_clause {
        logical_plan = return_clause::evaluate_return_clause(return_clause, logical_plan);
    }

    if let Some(order_clause) = &query_ast.order_by_clause {
        logical_plan = order_by_clause::evaluate_order_by_clause(order_clause, logical_plan);
    }

    if let Some(skip_clause) = &query_ast.skip_clause {
        logical_plan = skip_n_limit_clause::evaluate_skip_clause(skip_clause, logical_plan);
    }

    if let Some(limit_clause) = &query_ast.limit_clause {
        logical_plan = skip_n_limit_clause::evaluate_limit_clause(limit_clause, logical_plan);
    }

    Ok((logical_plan, plan_ctx))
}
