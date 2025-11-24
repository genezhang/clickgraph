use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::{
        denormalized_edge_optimizer::DenormalizedEdgeOptimizer,
        filter_into_graph_rel::FilterIntoGraphRel,
        filter_push_down::FilterPushDown,
        optimizer_pass::{OptimizerPass, OptimizerResult},
        projection_push_down::ProjectionPushDown,
        view_optimizer::ViewOptimizer,
    },
};

use super::plan_ctx::PlanCtx;
pub mod errors;
mod denormalized_edge_optimizer;
mod filter_into_graph_rel;
mod filter_push_down;
mod optimizer_pass;
mod projection_push_down;
mod view_optimizer;

// Helper to recursively print plan structure for debugging (TRACE level)
fn log_plan_structure(plan: &LogicalPlan, indent: usize) {
    if !log::log_enabled!(log::Level::Trace) {
        return; // Skip if TRACE not enabled
    }

    let prefix = "  ".repeat(indent);
    match plan {
        LogicalPlan::Filter(f) => {
            log::trace!("{}Filter", prefix);
            log_plan_structure(&f.input, indent + 1);
        }
        LogicalPlan::Projection(p) => {
            log::trace!("{}Projection", prefix);
            log_plan_structure(&p.input, indent + 1);
        }
        LogicalPlan::GraphRel(g) => {
            log::trace!(
                "{}GraphRel(has_filter={})",
                prefix,
                g.where_predicate.is_some()
            );
        }
        LogicalPlan::GraphNode(n) => {
            log::trace!("{}GraphNode", prefix);
            log_plan_structure(&n.input, indent + 1);
        }
        other => {
            log::trace!("{}{:?}", prefix, std::mem::discriminant(other));
        }
    }
}

pub fn initial_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>> {
    log::trace!("Initial optimization: Plan structure before FilterIntoGraphRel:");
    log_plan_structure(&plan, 1);

    // Push filters from plan_ctx into GraphRel nodes
    let filter_into_graph_rel = FilterIntoGraphRel::new();
    let transformed_plan = filter_into_graph_rel.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    Ok(plan)
}

pub fn final_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>> {
    // FIRST: Mark denormalized nodes before any other optimization
    // This allows subsequent passes to see which nodes are denormalized
    let denormalized_optimizer = DenormalizedEdgeOptimizer::new();
    let transformed_plan = denormalized_optimizer.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let projection_push_down = ProjectionPushDown::new();
    let transformed_plan = projection_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // IMPORTANT: Push filters into GraphRel.where_predicate BEFORE FilterPushDown runs
    // This ensures we catch Filter nodes that wrap GraphRel patterns
    let filter_into_graph_rel = FilterIntoGraphRel::new();
    let transformed_plan = filter_into_graph_rel.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let filter_push_down = FilterPushDown::new();
    let transformed_plan = filter_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // Apply view-specific optimizations
    let view_optimizer = ViewOptimizer::new();
    let transformed_plan = view_optimizer.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // println!("\n plan_ctx After {} \n\n", plan_ctx);
    // println!("\n PLAN After {} \n\n", plan);

    // println!("\n DEBUG PLAN After:\n{:#?}", plan);

    Ok(plan)
}
