//! Filter pushdown optimization pass.
//!
//! Pushes [`Filter`] nodes down through the plan tree toward data sources,
//! enabling earlier row elimination and better predicate pushdown to scans.
//!
//! # Optimization Strategy
//!
//! - Pushes filters through GraphNode, GraphRel, GroupBy, etc.
//! - Merges adjacent filters with AND
//! - Stops at boundaries that change filter semantics (aggregations, joins)
//!
//! # Example
//!
//! ```text
//! Before: Filter(a.x > 10, GraphRel(...))
//! After:  GraphRel(Filter(a.x > 10, left), center, right)
//! ```
//!
//! # Current status (P1.3 migration note)
//!
//! The pass currently performs NO rewrites: before the `transform_up`
//! migration it was a ~150-line hand-rolled walker whose every arm only
//! recursed and rebuilt — no arm ever produced a transformation (the
//! ViewScan arm's filter-merging was an acknowledged TODO), so it always
//! returned `Transformed::No` for the whole tree. That identity behavior
//! is preserved here verbatim; the rewrite hook below is where actual
//! pushdown logic goes when implemented.

use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct FilterPushDown;

impl OptimizerPass for FilterPushDown {
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

impl FilterPushDown {
    pub fn new() -> Self {
        FilterPushDown
    }
}
