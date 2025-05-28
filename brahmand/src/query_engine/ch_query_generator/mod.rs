use errors::ChQueryGeneratorError;

use crate::open_cypher_parser::ast::OpenCypherQueryAst;

use super::types::{GraphSchema, GraphSchemaElement, QueryIR, TraversalMode};

mod common;
mod ddl_query;
pub mod errors;
mod graph_traversal;
mod order_by_statement;
mod read_query;
mod select_statement;
mod where_statement;

pub fn generate_read_query(
    query_ir: QueryIR,
    travesal_mode: &TraversalMode,
) -> Result<Vec<String>, ChQueryGeneratorError> {
    read_query::generate_query(query_ir, travesal_mode)
}

pub fn generate_ddl_query(
    query_ast: OpenCypherQueryAst,
    current_graph_schema: &GraphSchema,
) -> Result<(Vec<String>, GraphSchemaElement), ChQueryGeneratorError> {
    ddl_query::generate_query(query_ast, current_graph_schema)
}
