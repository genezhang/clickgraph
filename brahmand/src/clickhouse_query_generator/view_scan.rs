//! ViewScan operations support
//!
//! This module handles converting ViewScan operations from the logical plan
//! into equivalent SQL queries.

use std::sync::Arc;

use crate::query_planner::{
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, ViewScan},
};

use super::ToSql;

/// Build a SQL query for a ViewScan operation
pub fn build_view_scan(scan: &ViewScan, plan: &LogicalPlan) -> String {
    let mut sql = String::new();
    sql.push_str(&format!("SELECT * FROM {}", scan.table_name));
    sql
}