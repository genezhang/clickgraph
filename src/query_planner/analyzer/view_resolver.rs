//! View resolution for Cypher query planning.
//!
//! This module handles resolving graph views to their underlying ClickHouse tables
//! during query planning. It maps node and relationship patterns to their
//! corresponding view definitions and resolves property references.

use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};

// Removed unused imports
use super::errors::AnalyzerError;

/// Resolves view references during query planning
#[derive(Debug, Clone)]
pub struct ViewResolver<'a> {
    /// Graph schema for direct lookups
    schema: &'a GraphSchema,
}

impl<'a> ViewResolver<'a> {
    /// Create a new ViewResolver that works directly with schema property mappings
    pub fn new(schema: &'a GraphSchema) -> Self {
        ViewResolver { schema }
    }

    /// Create a new ViewResolver that works directly with schema property mappings
    pub fn from_schema(schema: &'a GraphSchema) -> Self {
        ViewResolver { schema }
    }

    /// Get the schema for a node label
    pub fn resolve_node(&self, label: &str) -> Result<&'a NodeSchema, AnalyzerError> {
        // Handle $any wildcard for polymorphic edges
        if label == "$any" {
            return Err(AnalyzerError::NodeLabelNotFound(
                "$any (polymorphic wildcard - node type resolved at runtime)".to_string(),
            ));
        }

        self.schema
            .get_node_schema(label)
            .map_err(|_| AnalyzerError::NodeLabelNotFound(label.to_string()))
    }

    /// Get the schema for a relationship type
    /// For polymorphic relationships, provide from/to node labels for accurate resolution
    pub fn resolve_relationship(
        &self,
        type_name: &str,
        from_node: Option<&str>,
        to_node: Option<&str>,
    ) -> Result<&'a RelationshipSchema, AnalyzerError> {
        self.schema
            .get_rel_schema_with_nodes(type_name, from_node, to_node)
            .map_err(|_| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))
    }

    /// Resolve a node property to its underlying column
    pub fn resolve_node_property(
        &self,
        label: &str,
        property: &str,
    ) -> Result<crate::graph_catalog::expression_parser::PropertyValue, AnalyzerError> {
        self.resolve_node_property_with_role(label, property, None)
    }

    /// Resolve a node property with explicit role (From or To)
    /// This is needed for denormalized nodes where the same property maps to different columns
    /// depending on whether the node is the source or target of the relationship.
    pub fn resolve_node_property_with_role(
        &self,
        label: &str,
        property: &str,
        role: Option<crate::render_plan::cte_generation::NodeRole>,
    ) -> Result<crate::graph_catalog::expression_parser::PropertyValue, AnalyzerError> {
        use crate::render_plan::cte_generation::NodeRole;

        // Try to get the node schema and look up the property mapping
        let node_schema = self
            .schema
            .get_node_schema(label)
            .map_err(|_| AnalyzerError::NodeLabelNotFound(label.to_string()))?;

        // Try explicit property_mappings first
        if let Some(mapped) = node_schema.property_mappings.get(property) {
            return Ok(mapped.clone());
        }

        // For denormalized nodes, use the role to select the correct mapping
        if node_schema.is_denormalized {
            match role {
                Some(NodeRole::From) => {
                    // Explicitly From role - use from_properties only
                    if let Some(ref from_props) = node_schema.from_properties {
                        if let Some(mapped) = from_props.get(property) {
                            return Ok(
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    mapped.clone(),
                                ),
                            );
                        }
                    }
                }
                Some(NodeRole::To) => {
                    // Explicitly To role - use to_properties only
                    if let Some(ref to_props) = node_schema.to_properties {
                        if let Some(mapped) = to_props.get(property) {
                            return Ok(
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    mapped.clone(),
                                ),
                            );
                        }
                    }
                }
                None => {
                    // No role specified - try from_properties first (default for node-only queries)
                    // Note: UNION ALL for both positions is handled at a higher level
                    if let Some(ref from_props) = node_schema.from_properties {
                        if let Some(mapped) = from_props.get(property) {
                            return Ok(
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    mapped.clone(),
                                ),
                            );
                        }
                    }
                    // Fallback to to_properties (destination position)
                    if let Some(ref to_props) = node_schema.to_properties {
                        if let Some(mapped) = to_props.get(property) {
                            return Ok(
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    mapped.clone(),
                                ),
                            );
                        }
                    }
                }
            }
        }

        // Fallback to identity mapping (property name = column name)
        // This supports wide tables without requiring hundreds of explicit mappings
        Ok(crate::graph_catalog::expression_parser::PropertyValue::Column(property.to_string()))
    }

    /// Resolve a relationship property to its underlying column
    /// Resolve a relationship property with optional node label context
    /// For polymorphic relationships, provide from/to node labels for accurate resolution
    pub fn resolve_relationship_property(
        &self,
        type_name: &str,
        property: &str,
        from_node: Option<&str>,
        to_node: Option<&str>,
    ) -> Result<crate::graph_catalog::expression_parser::PropertyValue, AnalyzerError> {
        // Try to get the relationship schema and look up the property mapping
        let rel_schema = self
            .schema
            .get_rel_schema_with_nodes(type_name, from_node, to_node)
            .map_err(|_| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))?;

        // Try explicit mapping first, fallback to identity mapping (property name = column name)
        // This supports wide tables without requiring hundreds of explicit mappings
        Ok(rel_schema
            .property_mappings
            .get(property)
            .cloned()
            .unwrap_or_else(|| {
                crate::graph_catalog::expression_parser::PropertyValue::Column(property.to_string())
            }))
    }
}
