use super::ViewTableRef;
use crate::query_planner::logical_plan::Join;

/// Contains information about the FROM clause tables
#[derive(Debug, Clone, PartialEq)]
pub struct FromTable {
    pub table: Option<ViewTableRef>,
    pub joins: Vec<Join>,
}

impl FromTable {
    /// Create a new FromTable instance
    pub fn new(table: Option<ViewTableRef>) -> Self {
        Self {
            table,
            joins: Vec::new(),
        }
    }
}
