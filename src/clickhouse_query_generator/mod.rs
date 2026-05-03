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
pub mod to_sql; // Made public for EXISTS subquery support
pub mod to_sql_query; // Made public for EXISTS subquery generation with WITH clauses
pub mod variable_length_cte;
pub mod write_to_sql;

pub use id_gen::{auto_id_decision, IdInsertDecision, IdStrategy};
pub use write_to_sql::write_render_to_sql;
mod view_query;
mod view_scan;
#[cfg(test)]
mod where_clause_tests;

pub use common::{qualified_column, quote_identifier};
pub use errors::ClickhouseQueryGeneratorError;
pub use function_translator::{
    get_ch_function_name, get_supported_functions, is_ch_aggregate_function, is_ch_passthrough,
    is_ch_passthrough_aggregate, is_explicit_ch_aggregate, is_function_supported,
    translate_scalar_function, CH_AGG_PREFIX, CH_PASSTHROUGH_PREFIX,
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
    use crate::render_plan::plan_builder::RenderPlanBuilder;

    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let (_remaining, statement) = crate::open_cypher_parser::parse_cypher_statement(&cleaned)
        .map_err(|e| format!("Parse error: {:?}", e))?;

    let (logical_plan, plan_ctx) =
        crate::query_planner::evaluate_read_statement(statement, schema, None, None, None)
            .map_err(|e| format!("Plan error: {}", e))?;

    let render_plan = logical_plan
        .to_render_plan_with_ctx(schema, Some(&plan_ctx), None)
        .map_err(|e| format!("Render error: {}", e))?;

    Ok(generate_sql(render_plan, max_cte_depth))
}

/// Convert a Cypher query string to ClickHouse SQL, also returning the
/// LogicalPlan and PlanCtx for downstream metadata extraction (e.g., graph output).
///
/// This is used by `query_graph()` in the embedded crate, which needs the plan
/// metadata to classify return items as nodes vs relationships vs scalars.
pub fn cypher_to_sql_with_metadata(
    cypher: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    max_cte_depth: u32,
) -> Result<
    (
        String,
        crate::query_planner::logical_plan::LogicalPlan,
        crate::query_planner::plan_ctx::PlanCtx,
    ),
    String,
> {
    use crate::render_plan::plan_builder::RenderPlanBuilder;

    let cleaned = crate::open_cypher_parser::strip_comments(cypher);
    let (_remaining, statement) = crate::open_cypher_parser::parse_cypher_statement(&cleaned)
        .map_err(|e| format!("Parse error: {:?}", e))?;

    let (logical_plan, plan_ctx) =
        crate::query_planner::evaluate_read_statement(statement, schema, None, None, None)
            .map_err(|e| format!("Plan error: {}", e))?;

    let render_plan = logical_plan
        .to_render_plan_with_ctx(schema, Some(&plan_ctx), None)
        .map_err(|e| format!("Render error: {}", e))?;

    let sql = generate_sql(render_plan, max_cte_depth);
    Ok((sql, logical_plan, plan_ctx))
}
