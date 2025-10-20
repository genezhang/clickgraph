use std::sync::Arc;
use crate::query_planner::{
    logical_plan::{LogicalPlan, ViewScan},
    transformed::Transformed,
    analyzer::analyzer_pass::AnalyzerResult
};

/// Helper function to handle ViewScan case in analyzers
pub fn handle_view_scan(scan: &ViewScan, plan: Arc<LogicalPlan>) -> AnalyzerResult<LogicalPlan> {
    // ViewScans are leaf nodes, no transformation needed
    Ok(LogicalPlan::ViewScan(scan.clone()))
}