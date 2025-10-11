//! View-specific logical plan nodes for graph views

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};

use super::LogicalPlan;
use crate::query_planner::logical_expr::LogicalExpr;

/// A scan operation on a view-based table
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ViewScan {
    /// The underlying source table name
    pub source_table: String,
    /// The view-specific filter conditions
    pub view_filter: Option<LogicalExpr>,
    /// Mapping from graph properties to source columns
    pub property_mapping: HashMap<String, String>,
    /// The column containing node/relationship IDs
    pub id_column: String,
    /// Output schema (property names)
    pub output_schema: Vec<String>,
    /// View-specific projections
    pub projections: Vec<LogicalExpr>,
    /// Child plan (if any)
    #[serde(skip)]
    pub input: Option<Arc<LogicalPlan>>,
}

impl ViewScan {
    /// Create a new view scan operation
    pub fn new(
        source_table: String,
        view_filter: Option<LogicalExpr>,
        property_mapping: HashMap<String, String>,
        id_column: String,
        output_schema: Vec<String>,
        projections: Vec<LogicalExpr>,
    ) -> Self {
        ViewScan {
            source_table,
            view_filter,
            property_mapping,
            id_column,
            output_schema,
            projections,
            input: None,
        }
    }

    /// Create a new view scan with an input plan
    pub fn with_input(
        source_table: String,
        view_filter: Option<LogicalExpr>,
        property_mapping: HashMap<String, String>,
        id_column: String,
        output_schema: Vec<String>,
        projections: Vec<LogicalExpr>,
        input: Arc<LogicalPlan>,
    ) -> Self {
        ViewScan {
            source_table,
            view_filter,
            property_mapping,
            id_column,
            output_schema,
            projections,
            input: Some(input),
        }
    }

    /// Get the mapped column name for a property
    pub fn get_mapped_column(&self, property: &str) -> Option<&str> {
        self.property_mapping.get(property).map(|s| s.as_str())
    }
}