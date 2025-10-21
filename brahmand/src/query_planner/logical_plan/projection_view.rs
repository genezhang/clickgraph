use std::sync::Arc;

use super::{LogicalPlan, ViewScan, ProjectionItem, Projection};
use crate::query_planner::logical_expr::{LogicalExpr, ColumnAlias};

impl Projection {
    /// Create a new projection on a view scan
    pub fn with_view_scan(scan: ViewScan, items: Vec<ProjectionItem>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::ViewScan(Arc::new(scan))),
            items,
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

    /// Map view properties to source columns in projection
    pub fn map_view_properties(&mut self) {
        if let Some(scan) = self.view_scan().cloned() {
            for item in &mut self.items {
                if let LogicalExpr::ColumnAlias(col) = &mut item.expression {
                    // Try to map property to source column
                    if let Some(source_col) = scan.get_mapped_column(&col.0) {
                        col.0 = source_col.to_string();
                    }
                }
            }
        }
    }

    /// Add view-specific projections
    pub fn add_view_projections(&mut self) {
        if let Some(scan) = self.view_scan().cloned() {
            let mut new_items = Vec::new();
            
            // Add ID column projection if not present
            let has_id = self.items.iter().any(|item| {
                if let LogicalExpr::ColumnAlias(col) = &item.expression {
                    col.0 == scan.id_column
                } else {
                    false
                }
            });

            if !has_id {
                new_items.push(ProjectionItem {
                    expression: LogicalExpr::ColumnAlias(ColumnAlias(scan.id_column.clone())),
                    col_alias: Some(ColumnAlias(String::from("id"))),
                });
            }

            // Add any view-specific projections
            for proj in scan.projections {
                new_items.push(ProjectionItem {
                    expression: proj,
                    col_alias: None,
                });
            }

            // Add all new items
            self.items.extend(new_items);
        }
    }
}
