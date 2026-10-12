use crate::render_plan::RenderPlan;

mod common;
#[cfg(test)]
mod edge_uniqueness_tests;
mod errors;
mod function_registry;
mod function_translator;
pub mod id_gen;
pub mod json_builder; // Type-preserving JSON construction utilities
pub mod multi_type_vlp_joins; // Multi-type VLP JOIN expansion (Part 1D)
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
pub use json_builder::{
    generate_json_properties_from_schema, generate_json_properties_sql,
    generate_multi_type_union_sql,
};
pub use multi_type_vlp_joins::MultiTypeVlpJoinGenerator; // Export for cte_extraction.rs
pub use variable_length_cte::{NodeProperty, VariableLengthCteGenerator, WeightCteConfig};

// pub fn generate_sql(plan: RenderPlan) -> String{
//     let mut sql = String::new();
//     sql.push_str(&plan.ctes.to_sql());
//     sql.push_str(&plan.select.to_sql());
//     sql.push_str(&plan.from.to_sql());
//     sql.push_str(&plan.joins.to_sql());
//     sql.push_str(&plan.filters.to_sql());
//     sql.push_str(&plan.group_by.to_sql());
//     sql.push_str(&plan.order_by.to_sql());
//     sql.push_str(&plan.limit.to_sql());
//     sql.push_str(&plan.skip.to_sql());
//     println!("\n\n sql - \n{}", sql);
//     return sql
// }

pub fn generate_sql(plan: RenderPlan, max_cte_depth: u32) -> String {
    to_sql_query::render_plan_to_sql(plan, max_cte_depth)
}

/// Convert a Cypher query string directly to ClickHouse SQL.
///
/// This is a convenience function for library consumers (e.g., `clickgraph-embedded`)
/// that need to run the full Cypher→SQL pipeline without calling internal APIs.
///
/// # Arguments
/// * `cypher` — Cypher query string
/// * `schema` — resolved graph schema
/// * `max_cte_depth` — maximum recursion depth for variable-length paths
///
/// # Returns
/// Generated ClickHouse SQL string, or an error message string.
pub fn cypher_to_sql(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<String, String> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    translate_text_for_library(&cleaned, schema, max_cte_depth).map(|t| t.sql)
}

/// [`cypher_to_sql`], and the result columns holding an element id that a
/// library returns as Bolt's number for it (`id(r)`, P-4c S11l;
/// `bolt_protocol::result_transformer::element_id_number`).
pub fn cypher_to_sql_with_id_columns(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<(String, Vec<String>), String> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let t = translate_text_for_library(&cleaned, schema, max_cte_depth)?;
    let id_columns = t
        .shape
        .iter()
        .flatten()
        .filter(|c| matches!(c.kind, crate::bound_plan::lower::ResultKind::RelId))
        .map(|c| c.name.clone())
        .collect();
    Ok((t.sql, id_columns))
}

/// [`cypher_to_sql`] on the legacy route (`BoundPlanMode::Off`): the
/// tests of the legacy pipeline, which stays the fallback for what the
/// bound plan does not support until it is deleted (P-4c S11).
#[cfg(test)]
pub(crate) fn cypher_to_sql_legacy(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<String, String> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let (_remaining, statement) = crate::open_cypher_parser::parse_cypher_statement(&cleaned)
        .map_err(|e| format!("Parse error: {:?}", e))?;
    translate_for_library_with(
        statement,
        schema,
        max_cte_depth,
        Some(&cleaned),
        Some(crate::translate::BoundPlanMode::Off),
    )
    .map(|t| t.sql)
}

/// Convert a Cypher query to ClickHouse SQL after clearing all write
/// clauses (CREATE / SET / DELETE / REMOVE) from the parsed AST.
///
/// This is the read-pipeline half of write+RETURN execution: after the
/// embedded executor has run the write portion, the same statement is
/// rendered as if it were a pure read query so the user-visible
/// row payload reflects the post-write state. Stripping the clauses on
/// the AST avoids re-parsing fragile Cypher strings — the planner
/// re-derives the read pipeline naturally once those AST nodes are gone.
pub fn cypher_to_sql_read_only(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<String, String> {
    use crate::open_cypher_parser::ast::CypherStatement;
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let (_remaining, mut statement) = crate::open_cypher_parser::parse_cypher_statement(&cleaned)
        .map_err(|e| format!("Parse error: {:?}", e))?;

    if let CypherStatement::Query {
        ref mut query,
        ref mut union_clauses,
    } = statement
    {
        query.create_clause = None;
        query.set_clause = None;
        query.remove_clause = None;
        query.delete_clause = None;
        for u in union_clauses.iter_mut() {
            u.query.create_clause = None;
            u.query.set_clause = None;
            u.query.remove_clause = None;
            u.query.delete_clause = None;
        }
    }

    // The statement no longer matches the text (write clauses removed).
    translate_for_library(statement, schema, max_cte_depth, None).map(|t| t.sql)
}

/// Convert a Cypher query string to ClickHouse SQL, also returning what each
/// RETURN item is (nodes, relationships, scalars) for downstream graph output.
///
/// This is used by `query_graph()` in the embedded crate, which builds nodes
/// and edges from the rows with this metadata.
pub fn cypher_to_sql_with_metadata(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<
    (
        String,
        Vec<crate::server::bolt_protocol::result_transformer::ReturnItemMetadata>,
    ),
    String,
> {
    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let t = translate_text_for_library(&cleaned, schema, max_cte_depth)?;
    let metadata = t
        .return_metadata()
        .map_err(|e| format!("Failed to extract return metadata: {}", e))?;
    Ok((t.sql, metadata))
}

/// The library entry points' translation: the read seam with default
/// options, errors rendered as the `Plan error:` / `Render error:` strings
/// these functions have always returned.
fn translate_for_library(
    statement: crate::open_cypher_parser::ast::CypherStatement<'_>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
    cypher: Option<&str>,
) -> Result<crate::translate::ReadTranslation, String> {
    translate_for_library_with(statement, schema, max_cte_depth, cypher, None)
}

/// Parse `cleaned` and translate it; text only the clause-list parser reads
/// (P-4c S10b) goes to the bound plan alone, and the legacy parse error is
/// the answer when that does not translate it.
fn translate_text_for_library(
    cleaned: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<crate::translate::ReadTranslation, String> {
    match crate::open_cypher_parser::parse_cypher_statement(cleaned) {
        Ok((_remaining, statement)) => {
            translate_for_library(statement, schema, max_cte_depth, Some(cleaned))
        }
        Err(e) => {
            let parse_error = format!("Parse error: {:?}", e);
            match crate::translate::translate_text_read(
                cleaned,
                schema,
                crate::translate::ReadOptions {
                    max_cte_depth,
                    cypher: Some(cleaned.to_string()),
                    ..Default::default()
                },
            ) {
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
        crate::translate::TranslateError::Planning(e) => format!("Plan error: {}", e),
        crate::translate::TranslateError::Render(e) => format!("Render error: {}", e),
        // The caller reports it as a query error (`EmbeddedError::Query`).
        crate::translate::TranslateError::Invalid(e) => e,
        crate::translate::TranslateError::Unsupported(e) => format!("Not supported: {}", e),
        crate::translate::TranslateError::Internal(e) => format!("Internal error: {}", e),
    }
}

/// [`translate_for_library`] on a given route (`None`: the configured one).
fn translate_for_library_with(
    statement: crate::open_cypher_parser::ast::CypherStatement<'_>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
    cypher: Option<&str>,
    bound_plan: Option<crate::translate::BoundPlanMode>,
) -> Result<crate::translate::ReadTranslation, String> {
    crate::translate::translate_read(
        statement,
        schema,
        crate::translate::ReadOptions {
            max_cte_depth,
            cypher: cypher.map(str::to_string),
            bound_plan,
            ..Default::default()
        },
    )
    .map_err(library_error)
}
