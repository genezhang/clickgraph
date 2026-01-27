//! Error types for MATCH clause processing.
//!
//! This module re-exports the main LogicalPlanError since match clause
//! errors are part of the broader logical plan error hierarchy.

pub use crate::query_planner::logical_plan::errors::LogicalPlanError;
pub use crate::query_planner::logical_plan::plan_builder::LogicalPlanResult;
