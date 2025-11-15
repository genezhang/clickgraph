use std::sync::Arc;

use super::{LogicalPlan, ViewScan, Filter};
use crate::query_planner::logical_expr::{LogicalExpr, OperatorApplication, Operator};

impl Filter {
    /// Create a new filter on a view scan
    pub fn with_view_scan(scan: ViewScan, predicate: LogicalExpr) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Filter(Filter {
            input: Arc::new(LogicalPlan::ViewScan(Arc::new(scan))),
            predicate,
        }))
    }

    /// Check if input is a view scan
    pub fn has_view_scan(&self) -> bool {
        matches!(*self.input, LogicalPlan::ViewScan(_))
    }

    /// Get the view scan if it exists
    pub fn view_scan(&self) -> Option<&ViewScan> {
        match &*self.input {
            LogicalPlan::ViewScan(scan) => Some(scan),
            _ => None
        }
    }

    /// Create a new filter with updated predicate for view scan
    pub fn with_view_predicate(mut self, predicate: LogicalExpr) -> Self {
        if let Some(scan) = self.view_scan() {
            // Combine view filter with new predicate if exists
            if let Some(view_filter) = &scan.view_filter {
                self.predicate = LogicalExpr::Operator(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![view_filter.clone(), predicate],
                });
            } else {
                self.predicate = predicate;
            }
        }
        self
    }
}
