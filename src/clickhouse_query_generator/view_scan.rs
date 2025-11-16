//! ViewScan operations support
//!
//! This module handles converting ViewScan operations from the logical plan
//! into equivalent SQL queries.

use std::sync::Arc;

use crate::query_planner::{
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, ViewScan},
};

/// Build a SQL query for a ViewScan operation
pub fn build_view_scan(scan: &ViewScan, plan: &LogicalPlan) -> String {
    let mut sql = String::new();
    
    // Build table reference with parameters if this is a parameterized view
    let table_ref = if let (Some(param_names), Some(param_values)) = 
        (&scan.view_parameter_names, &scan.view_parameter_values) 
    {
        // This is a parameterized view - generate view(param=value, ...) syntax
        let param_pairs: Vec<String> = param_names
            .iter()
            .filter_map(|name| {
                param_values.get(name).map(|value| {
                    // Escape single quotes in value for SQL safety
                    let escaped_value = value.replace('\'', "''");
                    format!("{} = '{}'", name, escaped_value)
                })
            })
            .collect();

        if param_pairs.is_empty() {
            // No matching parameters found - use plain table/view name
            log::warn!(
                "ViewScan: View '{}' expects parameters {:?} but none matched in provided values",
                scan.source_table,
                param_names
            );
            scan.source_table.clone()
        } else {
            // Generate parameterized view call: view_name(param1='value1', param2='value2')
            format!("{}({})", scan.source_table, param_pairs.join(", "))
        }
    } else {
        // Not a parameterized view - use plain table/view name
        scan.source_table.clone()
    };
    
    sql.push_str(&format!("SELECT * FROM {}", table_ref));
    sql
}
