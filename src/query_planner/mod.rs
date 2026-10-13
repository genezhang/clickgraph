pub use errors::QueryPlannerError;
use types::QueryType;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::{CypherStatement, OpenCypherQueryAst},
    query_planner::logical_plan::{LogicalPlan, PageRank},
};

pub mod ast_transform;
mod errors;
pub mod join_context;
pub mod logical_expr;
pub mod logical_plan;
pub mod transformed;
pub mod typed_variable;
pub mod types;

pub fn get_query_type(query_ast: &OpenCypherQueryAst) -> QueryType {
    // CALL with RETURN (e.g., CALL db.labels() YIELD label RETURN ...) is a read query
    // Standalone CALL without RETURN is handled separately as CypherStatement::ProcedureCall
    let has_call = query_ast.call_clause.is_some();
    let has_return = query_ast.return_clause.is_some();

    log::debug!(
        "get_query_type: has_call={}, has_return={}",
        has_call,
        has_return
    );

    if has_call && !has_return {
        log::debug!("  -> Classified as Call");
        QueryType::Call
    } else if query_ast.delete_clause.is_some() {
        QueryType::Delete
    } else if query_ast.set_clause.is_some() || query_ast.remove_clause.is_some() {
        QueryType::Update
    } else {
        log::debug!("  -> Classified as Read");
        QueryType::Read
    }
}

/// Get query type from a CypherStatement (checks the first query)
pub fn get_statement_query_type(statement: &CypherStatement) -> QueryType {
    match statement {
        CypherStatement::Query { query, .. } => get_query_type(query),
        CypherStatement::ProcedureCall(_) => QueryType::Procedure,
        CypherStatement::CopyTo(_) => QueryType::Read,
    }
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
