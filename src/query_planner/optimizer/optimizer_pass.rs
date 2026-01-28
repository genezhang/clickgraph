//! Optimizer pass trait and result types.
//!
//! Defines the [`OptimizerPass`] trait that all optimization passes implement,
//! enabling a uniform interface for plan transformation.
//!
//! # Implementing a Pass
//!
//! ```ignore
//! impl OptimizerPass for MyPass {
//!     fn optimize(&self, plan: Arc<LogicalPlan>, ctx: &mut PlanCtx)
//!         -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
//!         // Transform plan here
//!     }
//! }
//! ```

use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan, optimizer::errors::OptimizerError, plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub type OptimizerResult<T> = Result<T, OptimizerError>;

pub trait OptimizerPass {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>>;
}
