mod common;
#[cfg(test)]
mod edge_uniqueness_tests;
mod errors;
mod function_registry;
mod function_translator;
pub mod id_gen;
pub mod pagerank;
pub mod to_sql_query; // Made public for EXISTS subquery generation with WITH clauses
pub(crate) mod type_inference;
pub mod variable_length_cte;
pub mod write_to_sql;

pub use id_gen::{auto_id_decision, IdInsertDecision, IdStrategy};
pub use write_to_sql::write_render_to_sql;
#[cfg(test)]
mod where_clause_tests;

pub use common::{
    contains_predicate, dialect_function_name, ends_with_predicate, list_higher_order_fn_sql,
    qualified_column, quote_identifier, reduce_binder_type_map, reduce_fold_sql,
    regex_match_predicate, round_half_up_sql, starts_with_predicate,
};
pub use errors::ClickhouseQueryGeneratorError;
pub use function_translator::{
    get_supported_functions, is_ch_aggregate_function, is_function_supported, CH_AGG_PREFIX,
    CH_PASSTHROUGH_PREFIX,
};
pub use variable_length_cte::{NodeProperty, VariableLengthCteGenerator};

/// Convert a Cypher query string directly to ClickHouse SQL.
///
/// This is a convenience function for library consumers (e.g., `clickgraph-embedded`)
/// that need to run the full Cypher→SQL pipeline without calling internal APIs.
///
/// # Returns
/// Generated ClickHouse SQL string, or an error message string.
pub fn cypher_to_sql(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<String, String> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    translate_text_for_library(&cleaned, schema).map(|t| t.sql)
}

/// [`cypher_to_sql`], and the result columns holding an element id that a
/// library returns as Bolt's number for it (`id(r)`, P-4c S11l;
/// `bolt_protocol::result_transformer::element_id_number`).
pub fn cypher_to_sql_with_id_columns(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<(String, Vec<String>), String> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let t = translate_text_for_library(&cleaned, schema)?;
    let id_columns = t
        .shape
        .iter()
        .filter(|c| matches!(c.kind, crate::bound_plan::lower::ResultKind::RelId))
        .map(|c| c.name.clone())
        .collect();
    Ok((t.sql, id_columns))
}

/// Convert a Cypher query string to ClickHouse SQL, also returning what each
/// RETURN item is (nodes, relationships, scalars) for downstream graph output.
///
/// This is used by `query_graph()` in the embedded crate, which builds nodes
/// and edges from the rows with this metadata.
pub fn cypher_to_sql_with_metadata(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<
    (
        String,
        Vec<crate::server::bolt_protocol::result_transformer::ReturnItemMetadata>,
    ),
    String,
> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let t = translate_text_for_library(&cleaned, schema)?;
    let metadata = t.return_metadata();
    Ok((t.sql, metadata))
}

/// Parse `cleaned` and translate it through the read seam with default
/// options; text only the clause-list parser reads (P-4c S10b) goes to the
/// bound plan alone, and the legacy parse error is the answer when that
/// does not translate it.
fn translate_text_for_library(
    cleaned: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<crate::translate::ReadTranslation, String> {
    let options = || crate::translate::ReadOptions {
        cypher: cleaned.to_string(),
        ..Default::default()
    };
    match crate::open_cypher_parser::parse_cypher_statement(cleaned) {
        Ok((_remaining, statement)) => {
            crate::translate::translate_read(statement, schema, options()).map_err(library_error)
        }
        Err(e) => {
            let parse_error = format!("Parse error: {:?}", e);
            match crate::translate::translate_text_read(cleaned, schema, options()) {
                Ok(Some(t)) => Ok(t),
                Ok(None) => Err(parse_error),
                Err(e) => Err(library_error(e)),
            }
        }
    }
}

/// The library entry points' error strings.
fn library_error(e: crate::translate::TranslateError) -> String {
    match e {
        // The caller reports it as a query error (`EmbeddedError::Query`).
        crate::translate::TranslateError::Invalid(e) => e,
        crate::translate::TranslateError::Unsupported(e) => format!("Not supported: {}", e),
        crate::translate::TranslateError::Internal(e) => format!("Internal error: {}", e),
    }
}
