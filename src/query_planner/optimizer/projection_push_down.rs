//! Projection pushdown optimization pass.
//!
//! Eliminates unused columns early in the plan by pushing projection
//! requirements down toward data sources. This reduces I/O and memory usage.
//!
//! # Optimization Strategy
//!
//! - Tracks required columns through the plan
//! - Eliminates unreferenced columns at scan level
//! - Preserves columns needed for filters, joins, and output
//!
//! # Example
//!
//! ```text
//! Before: Projection([a.x], ViewScan(a, columns=[x, y, z]))
//! After:  Projection([a.x], ViewScan(a, columns=[x]))
//! ```
//!
//! # Current status (P1.3 migration note)
//!
//! The pass currently performs NO rewrites: before the `transform_up`
//! migration it was a ~130-line hand-rolled walker whose every arm only
//! recursed and rebuilt — no arm ever produced a transformation, so it
//! always returned `Transformed::No` for the whole tree. That identity
//! behavior is preserved here verbatim; the rewrite hook below is where
//! actual pushdown logic goes when implemented.

use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct ProjectionPushDown;

impl OptimizerPass for ProjectionPushDown {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        LogicalPlan::transform_up(&logical_plan, &mut |node| {
            // No per-node rewrite yet (see module docs). transform_up
            // recurses exhaustively via map_children_arc, so every variant
            // — including write-op inputs — is visited by construction.
            Ok(Transformed::No(Arc::clone(node)))
        })
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        ProjectionPushDown
    }
}
