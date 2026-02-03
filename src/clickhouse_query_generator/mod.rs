use crate::render_plan::RenderPlan;

mod common;
#[cfg(test)]
mod edge_uniqueness_tests;
mod errors;
mod function_registry;
mod function_translator;
pub mod json_builder; // Type-preserving JSON construction utilities
pub mod multi_type_vlp_joins; // Multi-type VLP JOIN expansion (Part 1D)
pub mod pagerank;
pub mod to_sql; // Made public for EXISTS subquery support
pub mod to_sql_query; // Made public for EXISTS subquery generation with WITH clauses
pub mod variable_length_cte;
mod view_query;
mod view_scan;
#[cfg(test)]
mod where_clause_tests;

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
pub use variable_length_cte::{NodeProperty, VariableLengthCteGenerator};

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
