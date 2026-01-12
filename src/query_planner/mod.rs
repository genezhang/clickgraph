use std::collections::HashMap;
use std::sync::Arc;

use errors::QueryPlannerError;
use types::QueryType;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::{CypherStatement, OpenCypherQueryAst},
    query_planner::logical_plan::{LogicalPlan, PageRank},
};

pub mod analyzer;
mod errors;
pub mod logical_expr;
pub mod logical_plan;
pub mod optimizer;
pub mod plan_ctx;
pub mod transformed;
pub mod translator;
pub mod types;

pub fn get_query_type(query_ast: &OpenCypherQueryAst) -> QueryType {
    if query_ast.call_clause.is_some() {
        QueryType::Call
    } else if query_ast.delete_clause.is_some() {
        QueryType::Delete
    } else if query_ast.set_clause.is_some() || query_ast.remove_clause.is_some() {
        QueryType::Update
    } else {
        QueryType::Read
    }
}

/// Get query type from a CypherStatement (checks the first query)
pub fn get_statement_query_type(statement: &CypherStatement) -> QueryType {
    get_query_type(&statement.query)
}

pub fn evaluate_read_query(
    query_ast: OpenCypherQueryAst,
    current_graph_schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
) -> Result<LogicalPlan, QueryPlannerError> {
    let (logical_plan, mut plan_ctx) = logical_plan::evaluate_query(
        query_ast,
        current_graph_schema,
        tenant_id,
        view_parameter_values,
        None, // default max_inferred_types
    )?;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;

    // Validation now happens in initial_analyzing, so errors propagate cleanly
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;

    let logical_plan =
        analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    // println!("\n\n plan_ctx after \n {}",plan_ctx);
    // println!("\n plan after{}", logical_plan);

    let logical_plan =
        Arc::into_inner(logical_plan).ok_or(QueryPlannerError::LogicalPlanExtractor)?;
    Ok(logical_plan)
}

/// Evaluate a complete Cypher statement which may contain UNION clauses
pub fn evaluate_read_statement(
    statement: CypherStatement,
    current_graph_schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
    max_inferred_types: Option<usize>,
) -> Result<LogicalPlan, QueryPlannerError> {
    let (logical_plan, mut plan_ctx) = logical_plan::evaluate_cypher_statement(
        statement,
        current_graph_schema,
        tenant_id,
        view_parameter_values,
        max_inferred_types,
    )?;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;

    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
    
    // DEBUG: Check cte_references after intermediate_analyzing
    fn count_cte_refs(plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::WithClause(wc) => {
                wc.cte_references.len() + count_cte_refs(&wc.input)
            }
            LogicalPlan::Projection(p) => count_cte_refs(&p.input),
            LogicalPlan::Limit(l) => count_cte_refs(&l.input),
            LogicalPlan::GraphJoins(gj) => count_cte_refs(&gj.input),  // ADDED
            _ => 0,
        }
    }
    eprintln!("🔬 After intermediate_analyzing: {} cte_references total", count_cte_refs(&logical_plan));

    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;
    eprintln!("🔬 After final_optimization: {} cte_references total", count_cte_refs(&logical_plan));

    let logical_plan =
        analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
    eprintln!("🔬 After final_analyzing: {} cte_references total", count_cte_refs(&logical_plan));

    let logical_plan =
        Arc::into_inner(logical_plan).ok_or(QueryPlannerError::LogicalPlanExtractor)?;
    Ok(logical_plan)
}

pub fn evaluate_call_query(
    query_ast: OpenCypherQueryAst,
    _current_graph_schema: &GraphSchema,
) -> Result<LogicalPlan, QueryPlannerError> {
    if let Some(call_clause) = query_ast.call_clause {
        match call_clause.procedure_name {
            "pagerank" | "pagerank.graph" => {
                // Parse PageRank arguments
                let mut graph_name = None;
                let mut iterations = 10;
                let mut damping_factor = 0.85;
                let mut node_labels = None;
                let mut relationship_types = None;

                for arg in call_clause.arguments {
                    match arg.name {
                        "graph" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::String(s),
                            ) = arg.value
                            {
                                graph_name = Some(s.to_string());
                            }
                        }
                        "nodeLabels" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::String(s),
                            ) = arg.value
                            {
                                // Parse comma-separated list
                                let labels: Vec<String> =
                                    s.split(',').map(|s| s.trim().to_string()).collect();
                                node_labels = Some(labels);
                            }
                        }
                        "relationshipTypes" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::String(s),
                            ) = arg.value
                            {
                                // Parse comma-separated list
                                let types: Vec<String> =
                                    s.split(',').map(|s| s.trim().to_string()).collect();
                                relationship_types = Some(types);
                            }
                        }
                        "maxIterations" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::Integer(i),
                            ) = arg.value
                            {
                                iterations = i as usize;
                            }
                        }
                        "dampingFactor" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::Float(f),
                            ) = arg.value
                            {
                                damping_factor = f;
                            }
                        }
                        // Backward compatibility - also accept old parameter names
                        "iterations" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::Integer(i),
                            ) = arg.value
                            {
                                iterations = i as usize;
                            }
                        }
                        "damping" => {
                            if let crate::open_cypher_parser::ast::Expression::Literal(
                                crate::open_cypher_parser::ast::Literal::Float(f),
                            ) = arg.value
                            {
                                damping_factor = f;
                            }
                        }
                        _ => {}
                    }
                }

                // Create PageRank logical plan
                Ok(LogicalPlan::PageRank(PageRank {
                    graph_name,
                    iterations,
                    damping_factor,
                    node_labels,
                    relationship_types,
                }))
            }
            _ => Err(QueryPlannerError::UnsupportedProcedure {
                procedure: call_clause.procedure_name.to_string(),
            }),
        }
    } else {
        Err(QueryPlannerError::InvalidQuery(
            "No CALL clause found".to_string(),
        ))
    }
}
