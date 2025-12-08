use crate::render_plan::RenderPlan;

mod common;
mod errors;
mod function_registry;
mod function_translator;
pub mod pagerank;
pub mod to_sql;  // Made public for EXISTS subquery support
mod to_sql_query;
pub mod variable_length_cte;
mod view_query;
mod view_scan;
#[cfg(test)]
mod where_clause_tests;
#[cfg(test)]
mod edge_uniqueness_tests;

pub use errors::ClickhouseQueryGeneratorError;
pub use function_translator::{
    get_ch_function_name, get_supported_functions, is_ch_aggregate_function,
    is_ch_passthrough, is_ch_passthrough_aggregate, is_function_supported,
    translate_scalar_function, CH_PASSTHROUGH_PREFIX,
};
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
