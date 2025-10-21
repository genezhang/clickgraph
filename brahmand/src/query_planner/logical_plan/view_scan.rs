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
    /// For relationship scans: the column containing source node ID
    pub from_column: Option<String>,
    /// For relationship scans: the column containing target node ID
    pub to_column: Option<String>,
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
            from_column: None,
            to_column: None,
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
            from_column: None,
            to_column: None,
            input: Some(input),
        }
    }

    /// Create a new relationship view scan with source and target columns
    pub fn new_relationship(
        source_table: String,
        view_filter: Option<LogicalExpr>,
        property_mapping: HashMap<String, String>,
        id_column: String,
        output_schema: Vec<String>,
        projections: Vec<LogicalExpr>,
        from_column: String,
        to_column: String,
    ) -> Self {
        ViewScan {
            source_table,
            view_filter,
            property_mapping,
            id_column,
            output_schema,
            projections,
            from_column: Some(from_column),
            to_column: Some(to_column),
            input: None,
        }
    }

    /// Create a new relationship view scan with an input plan
    pub fn relationship_with_input(
        source_table: String,
        view_filter: Option<LogicalExpr>,
        property_mapping: HashMap<String, String>,
        id_column: String,
        output_schema: Vec<String>,
        projections: Vec<LogicalExpr>,
        from_column: String,
        to_column: String,
        input: Arc<LogicalPlan>,
    ) -> Self {
        ViewScan {
            source_table,
            view_filter,
            property_mapping,
            id_column,
            output_schema,
            projections,
            from_column: Some(from_column),
            to_column: Some(to_column),
            input: Some(input),
        }
    }

    /// Get the mapped column name for a property
    pub fn get_mapped_column(&self, property: &str) -> Option<&str> {
        self.property_mapping.get(property).map(|s| s.as_str())
    }

    /// Add a filter to this ViewScan, combining with existing filters
    pub fn with_additional_filter(&self, additional_filter: LogicalExpr) -> Self {
        use crate::query_planner::logical_expr::{Operator, OperatorApplication};
        
        let combined_filter = if let Some(existing_filter) = &self.view_filter {
            // Combine existing filter with additional filter using AND
            Some(LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![existing_filter.clone(), additional_filter],
            }))
        } else {
            // Use the additional filter as the only filter
            Some(additional_filter)
        };

        ViewScan {
            source_table: self.source_table.clone(),
            view_filter: combined_filter,
            property_mapping: self.property_mapping.clone(),
            id_column: self.id_column.clone(),
            output_schema: self.output_schema.clone(),
            projections: self.projections.clone(),
            from_column: self.from_column.clone(),
            to_column: self.to_column.clone(),
            input: self.input.clone(),
        }
    }

    /// Optimize property access by ensuring efficient column mapping
    pub fn optimize_property_access(&self) -> Self {
        // For now, return self unchanged
        // In the future, we can implement property access optimizations like:
        // - Reordering property mappings for better query performance
        // - Eliminating unused property mappings
        // - Optimizing complex property expressions
        self.clone()
    }

    /// Check if a filter condition can be pushed into this view scan
    pub fn can_push_filter(&self, filter: &LogicalExpr) -> bool {
        // For now, we'll conservatively allow most filters to be pushed
        // In the future, we can implement more sophisticated logic to check:
        // - Whether the filter references properties that exist in the view
        // - Whether the filter is compatible with existing view filters
        // - Whether pushing the filter would improve performance
        match filter {
            LogicalExpr::PropertyAccessExp(_) => true,
            LogicalExpr::Literal(_) => true,
            LogicalExpr::OperatorApplicationExp(op) => {
                op.operands.iter().all(|operand| self.can_push_filter(operand))
            },
            LogicalExpr::ScalarFnCall(_) => true,
            LogicalExpr::TableAlias(_) => true,
            LogicalExpr::Column(_) => true,
            _ => false,
        }
    }
}
