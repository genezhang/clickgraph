use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::{
        cartesian_join_extraction::CartesianJoinExtraction,
        cleanup_viewscan_filters::CleanupViewScanFilters,
        filter_into_graph_rel::FilterIntoGraphRel,
        filter_push_down::FilterPushDown,
        optimizer_pass::{OptimizerPass, OptimizerResult},
        projection_push_down::ProjectionPushDown,
        view_optimizer::ViewOptimizer,
    },
};

use super::plan_ctx::PlanCtx;
pub mod errors;
pub mod cartesian_join_extraction;
mod cleanup_viewscan_filters;
mod filter_into_graph_rel;
mod filter_push_down;
pub mod optimizer_pass;
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
    crate::debug_print!("🔥 INITIAL_OPTIMIZATION CALLED 🔥");
    log::trace!("Initial optimization: Plan structure before FilterIntoGraphRel:");
    log_plan_structure(&plan, 1);

    // Debug: Check if there's a Filter above CartesianProduct
    fn check_filter_cartesian(p: &LogicalPlan, depth: usize) {
        let _indent = "  ".repeat(depth);
        match p {
            LogicalPlan::Filter(f) => {
                if let LogicalPlan::CartesianProduct(_) = f.input.as_ref() {
                    crate::debug_print!("{}🎯 initial_optimization: Found Filter above CartesianProduct!", _indent);
                    crate::debug_print!("{}   predicate: {:?}", _indent, f.predicate);
                }
                check_filter_cartesian(&f.input, depth + 1);
            }
            LogicalPlan::Projection(proj) => check_filter_cartesian(&proj.input, depth + 1),
            LogicalPlan::CartesianProduct(cp) => {
                crate::debug_print!("{}📦 CartesianProduct: join_condition={:?}", _indent, cp.join_condition);
                check_filter_cartesian(&cp.left, depth + 1);
                check_filter_cartesian(&cp.right, depth + 1);
            }
            _ => {}
        }
    }
    check_filter_cartesian(&plan, 0);

    // Extract cross-pattern filters from CartesianProduct and move to join_condition
    // This must run BEFORE FilterIntoGraphRel to prevent the cross-pattern filter
    // from being pushed into a single GraphRel
    let cartesian_join_extraction = CartesianJoinExtraction::new();
    let transformed_plan = cartesian_join_extraction.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // Debug: Check result after CartesianJoinExtraction
    crate::debug_println!("🔍 After CartesianJoinExtraction:");
    check_filter_cartesian(&plan, 0);

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
    let projection_push_down = ProjectionPushDown::new();
    let transformed_plan = projection_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // FilterIntoGraphRel already ran in initial_optimization - don't run it again!
    // Running it twice causes duplicate filters.

    // CRITICAL: Clean up ViewScan.view_filter after FilterIntoGraphRel (from initial_optimization)
    // This prevents duplicate filter collection during rendering
    let cleanup_viewscan = CleanupViewScanFilters;
    let transformed_plan = cleanup_viewscan.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let filter_push_down = FilterPushDown::new();
    let transformed_plan = filter_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // Apply view-specific optimizations
    let view_optimizer = ViewOptimizer::new();
    let transformed_plan = view_optimizer.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    Ok(plan)
}
