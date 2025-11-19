use crate::query_planner::logical_plan::{LogicalPlan, ViewScan};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Import serde_arc module for serialization
#[path = "../utils/serde_arc.rs"]
mod serde_arc;

/// Represents a reference to a view or table in a render plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewTableRef {
    #[serde(with = "serde_arc")]
    pub source: Arc<LogicalPlan>,
    pub name: String,
    /// The alias to use in SQL (e.g., the Cypher variable name like "u" or "n")
    pub alias: Option<String>,
    /// Whether to use FINAL keyword for this table (for ReplacingMergeTree, etc.)
    pub use_final: bool,
}

impl ViewTableRef {
    /// Build table reference with parameterized view syntax if applicable
    fn build_table_reference(scan: &ViewScan, base_name: &str) -> String {
        if let Some(param_names) = &scan.view_parameter_names {
            if !param_names.is_empty() {
                // Generate parameterized view call with $placeholder syntax
                // e.g., view_name(tenant_id = $tenant_id, region = $region)
                let param_pairs: Vec<String> = param_names
                    .iter()
                    .map(|name| format!("{} = ${}", name, name))
                    .collect();

                return format!("{}({})", base_name, param_pairs.join(", "));
            }
        }

        // Not a parameterized view - use plain table name
        base_name.to_string()
    }

    /// Create a new table reference
    pub fn new_table(scan: ViewScan, name: String) -> Self {
        let table_ref = Self::build_table_reference(&scan, &name);
        let use_final = scan.use_final; // Extract before moving scan
        Self {
            source: Arc::new(LogicalPlan::ViewScan(Arc::new(scan))),
            name: table_ref,
            alias: None,
            use_final,
        }
    }

    /// Create a new table reference with an explicit alias
    pub fn new_table_with_alias(scan: ViewScan, name: String, alias: String) -> Self {
        let table_ref = Self::build_table_reference(&scan, &name);
        let use_final = scan.use_final; // Extract before moving scan
        Self {
            source: Arc::new(LogicalPlan::ViewScan(Arc::new(scan))),
            name: table_ref,
            alias: Some(alias),
            use_final,
        }
    }

    /// Create a new view reference
    pub fn new_view(source: Arc<LogicalPlan>, name: String) -> Self {
        // Try to extract use_final from source if it's a ViewScan
        let use_final = if let LogicalPlan::ViewScan(scan) = source.as_ref() {
            scan.use_final
        } else {
            false
        };

        Self {
            source,
            name,
            alias: None,
            use_final,
        }
    }

    /// Create a new view reference with an explicit alias
    pub fn new_view_with_alias(source: Arc<LogicalPlan>, name: String, alias: String) -> Self {
        // Try to extract use_final from source if it's a ViewScan
        let use_final = if let LogicalPlan::ViewScan(scan) = source.as_ref() {
            scan.use_final
        } else {
            false
        };

        Self {
            source,
            name,
            alias: Some(alias),
            use_final,
        }
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
