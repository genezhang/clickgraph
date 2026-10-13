//! View-specific logical plan nodes for graph views

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::LogicalPlan;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::filter_parser::SchemaFilter;
use crate::query_planner::logical_expr::LogicalExpr;

/// A scan operation on a view-based table
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ViewScan {
    /// The underlying source table name
    pub source_table: String,
    /// The view-specific filter conditions
    pub view_filter: Option<LogicalExpr>,
    /// Mapping from graph properties to source columns or expressions
    pub property_mapping: HashMap<String, PropertyValue>,
    /// The column containing node/relationship IDs
    pub id_column: String,
    /// Output schema (property names)
    pub output_schema: Vec<String>,
    /// View-specific projections
    pub projections: Vec<LogicalExpr>,
    /// For relationship scans: the column(s) containing source node ID
    pub from_id: Option<Identifier>,
    /// For relationship scans: the column(s) containing target node ID
    pub to_id: Option<Identifier>,
    /// Child plan (if any)
    #[serde(skip)]
    pub input: Option<Arc<LogicalPlan>>,
    /// Parameter names expected by this parameterized view (e.g., ["tenant_id", "region"])
    /// If None, this is not a parameterized view
    pub view_parameter_names: Option<Vec<String>>,
    /// Actual parameter values to pass to the view (e.g., {"tenant_id": "acme", "region": "US"})
    /// Must match view_parameter_names if the view is parameterized
    pub view_parameter_values: Option<HashMap<String, String>>,
    /// Whether to use FINAL keyword for this table scan
    /// - true: Use FINAL (for ReplacingMergeTree, CollapsingMergeTree, etc.)
    /// - false: Don't use FINAL
    pub use_final: bool,
    /// Whether this node is denormalized (stored on edge table)
    /// Set from schema during ViewScan creation
    pub is_denormalized: bool,

    // ===== Denormalized node support =====
    /// Property mappings when this node appears as FROM node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": Column("Origin"), "city": Column("OriginCityName")}
    pub from_node_properties: Option<HashMap<String, PropertyValue>>,

    /// Property mappings when this node appears as TO node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": Column("Dest"), "city": Column("DestCityName")}
    pub to_node_properties: Option<HashMap<String, PropertyValue>>,

    // ===== Polymorphic edge support =====
    /// Column containing edge type discriminator (for polymorphic edges)
    /// Example: Some("interaction_type")
    pub type_column: Option<String>,

    /// Valid type values for this edge (for polymorphic edges)
    /// Example: Some(vec!["FOLLOWS", "LIKES", "AUTHORED"])
    pub type_values: Option<Vec<String>>,

    /// Column containing from-node label (for polymorphic edges)
    /// Example: Some("from_type")
    pub from_label_column: Option<String>,

    /// Column containing to-node label (for polymorphic edges)
    /// Example: Some("to_type")
    pub to_label_column: Option<String>,

    // ===== Schema-level filtering =====
    /// Schema-level filter defined in YAML configuration
    /// Applied automatically to all scans of this node/relationship type
    /// Example: "ts >= now() - INTERVAL 7 DAY AND proto = 'tcp'"
    pub schema_filter: Option<SchemaFilter>,

    // ===== Node label for denormalized nodes =====
    /// The node label this ViewScan represents (e.g., "Airport", "User")
    /// Essential for denormalized nodes where label cannot be derived from table name
    pub node_label: Option<String>,
}

impl ViewScan {
    /// Create a new view scan operation
    pub fn new(
        source_table: String,
        view_filter: Option<LogicalExpr>,
        property_mapping: HashMap<String, PropertyValue>,
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
            from_id: None,
            to_id: None,
            input: None,
            view_parameter_names: None,
            view_parameter_values: None,
            use_final: false,       // Default: no FINAL
            is_denormalized: false, // Default: not denormalized
            from_node_properties: None,
            to_node_properties: None,
            type_column: None,
            type_values: None,
            from_label_column: None,
            to_label_column: None,
            schema_filter: None, // Default: no schema-level filter
            node_label: None,    // Default: no explicit label
        }
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
            LogicalExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .all(|operand| self.can_push_filter(operand)),
            LogicalExpr::ScalarFnCall(_) => true,
            LogicalExpr::TableAlias(_) => true,
            LogicalExpr::Column(_) => true,
            _ => false,
        }
    }
}
