use errors::OptimizerError;

use crate::query_engine::types::PhysicalPlan;

use super::types::{GraphSchema, LogicalPlan};

mod anchor_node;
pub mod errors;
mod physical_plan;
mod traversal_sequence;
mod schema_inference;

pub fn generate_physical_plan<'a>(
    logical_plan: LogicalPlan<'a>,
    graph_schema: &GraphSchema,
) -> Result<PhysicalPlan<'a>, OptimizerError> {
    physical_plan::generate_physical_plan(logical_plan, graph_schema)
}
