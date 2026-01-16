//! Pure utility functions for plan building
//!
//! This module contains utility functions that have no dependencies on LogicalPlan
//! or complex state. These are safe to extract early in the refactoring process.
//!
//! Functions in this module should be:
//! - Pure (no side effects)
//! - Independent of LogicalPlan structure
//! - Reusable across different builder modules

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use std::collections::HashMap;
use crate::render_plan::SelectItem;

/// Build property mapping from select items for CTE column resolution.
/// Maps (alias, property) -> column_name for property access resolution.
///
/// This function handles three patterns:
/// 1. "alias.property" (dotted, used in VLP CTEs)
/// 2. "alias_property" (underscore, used in WITH CTEs)  
/// 3. No separator - aggregate column like "friends" from collect()
pub fn build_property_mapping_from_columns(
    select_items: &[SelectItem],
) -> HashMap<(String, String), String> {
    let mut property_mapping = HashMap::new();

    for item in select_items {
        if let Some(col_alias) = &item.col_alias {
            let col_name = &col_alias.0;

            // Pattern 1: "alias.property" (dotted, used in VLP CTEs)
            if let Some(dot_pos) = col_name.find('.') {
                let alias = col_name[..dot_pos].to_string();
                let property = col_name[dot_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping: ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );
            }
            // Pattern 2: "alias_property" (underscore, used in WITH CTEs)
            else if let Some(underscore_pos) = col_name.find('_') {
                let alias = col_name[..underscore_pos].to_string();
                let property = col_name[underscore_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping: ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );
            }
            // Pattern 3: No separator - aggregate column like "friends" from collect()
            // Store with empty alias so ARRAY JOIN can find it: ("", column_name) → column_name
            else {
                property_mapping.insert(("".to_string(), col_name.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (aggregate): (\"\", {}) → {}",
                    col_name,
                    col_name
                );
            }
        }
    }

    log::info!(
        "Built property mapping with {} entries",
        property_mapping.len()
    );
    property_mapping
}

/// Placeholder for strip_database_prefix function
/// Will be moved from plan_builder.rs lines 116-124
pub fn strip_database_prefix(_table_name: &str) -> String {
    // TODO: Implement when moving from plan_builder.rs
    String::new()
}

/// Placeholder for has_multi_type_vlp function
/// Will be moved from plan_builder.rs lines 125-156
pub fn has_multi_type_vlp(_plan: &LogicalPlan) -> bool {
    // TODO: Implement when moving from plan_builder.rs
    false
}

/// Placeholder for get_anchor_alias_from_plan function
/// Will be moved from plan_builder.rs lines 157-178
pub fn get_anchor_alias_from_plan(_plan: &LogicalPlan) -> Option<String> {
    // TODO: Implement when moving from plan_builder.rs
    None
}

/// Placeholder for extract_vlp_alias_mappings function
/// Will be moved from plan_builder.rs lines 651-789
pub fn extract_vlp_alias_mappings(_ctes: &crate::render_plan::CteItems) -> HashMap<String, String> {
    // TODO: Implement when moving from plan_builder.rs
    HashMap::new()
}

/// Placeholder for rewrite_render_expr_for_vlp function
/// Will be moved from plan_builder.rs lines 790-901
pub fn rewrite_render_expr_for_vlp(
    _expr: &mut crate::render_plan::render_expr::RenderExpr,
    _mappings: &HashMap<String, String>,
) {
    // TODO: Implement when moving from plan_builder.rs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_functions() {
        // Basic tests to ensure module compiles
        assert_eq!(strip_database_prefix("test"), "");
        assert!(!has_multi_type_vlp(&LogicalPlan::Empty));
        assert_eq!(get_anchor_alias_from_plan(&LogicalPlan::Empty), None);
    }
}