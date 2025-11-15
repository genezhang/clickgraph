use crate::{
    render_plan::{RenderPlan, ToSql as RenderPlanToSql},
};

mod common;
mod errors;
mod function_registry;
mod function_translator;
pub mod pagerank;
mod to_sql;
mod to_sql_query;
pub mod variable_length_cte;
mod view_query;
mod view_scan;
#[cfg(test)]
mod where_clause_tests;

pub use errors::ClickhouseQueryGeneratorError;
pub use variable_length_cte::{VariableLengthCteGenerator, NodeProperty};
pub use function_translator::{translate_scalar_function, is_function_supported, get_supported_functions};


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
