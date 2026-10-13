use thiserror::Error;

use crate::query_planner::logical_plan::errors::LogicalPlanError;

#[derive(Debug, Error)]
pub enum QueryPlannerError {
    #[error("LogicalPlanError: {0}")]
    LogicalPlan(#[from] LogicalPlanError),
    // #[error("RenderBuildError: {0}")]
    // RenderBuild(#[from] RenderBuildError),
    #[error("Logical Plan Extractor")]
    LogicalPlanExtractor,
    #[error("Unsupported procedure: {procedure}")]
    UnsupportedProcedure { procedure: String },
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}
