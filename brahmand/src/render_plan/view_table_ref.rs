use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::query_planner::logical_plan::{LogicalPlan, ViewScan};

// Import serde_arc module for serialization
#[path = "../utils/serde_arc.rs"]
mod serde_arc;

/// Represents a reference to a view or table in a render plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewTableRef {
    #[serde(with = "serde_arc")]
    pub source: Arc<LogicalPlan>,
    pub name: String,
}

impl ViewTableRef {
    /// Create a new table reference
    pub fn new_table(scan: ViewScan, name: String) -> Self {
        Self {
            source: Arc::new(LogicalPlan::ViewScan(Arc::new(scan))),
            name,
        }
    }

    /// Create a new view reference
    pub fn new_view(source: Arc<LogicalPlan>, name: String) -> Self {
        Self { source, name }
    }
}

use super::FromTable;

impl ViewTableRef {
    /// Convert to a FromTable instance
    pub fn into_from_table(self) -> FromTable {
        FromTable {
            table: Some(self),
            joins: Vec::new(),
        }
    }
}

/// Convert an Option<ViewTableRef> to Option<FromTable>
pub fn view_ref_to_from_table(view_ref: Option<ViewTableRef>) -> Option<FromTable> {
    view_ref.map(|v| v.into_from_table())
}

/// Convert an Option<FromTable> to Option<ViewTableRef>
pub fn from_table_to_view_ref(from_table: Option<FromTable>) -> Option<ViewTableRef> {
    from_table.and_then(|f| f.table)
}