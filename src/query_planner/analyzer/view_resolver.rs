//! View resolution for Cypher query planning.
//! 
//! This module handles resolving graph views to their underlying ClickHouse tables
//! during query planning. It maps node and relationship patterns to their
//! corresponding view definitions and resolves property references.

use crate::graph_catalog::{
    graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
};

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
        ViewResolver {
            schema,
        }
    }

    /// Create a new ViewResolver that works directly with schema property mappings
    pub fn from_schema(schema: &'a GraphSchema) -> Self {
        ViewResolver {
            schema,
        }
    }

    /// Get the schema for a node label
    pub fn resolve_node(&self, label: &str) -> Result<&'a NodeSchema, AnalyzerError> {
        self.schema.get_node_schema(label)
            .map_err(|_| AnalyzerError::NodeLabelNotFound(label.to_string()))
    }

    /// Get the schema for a relationship type 
    pub fn resolve_relationship(&self, type_name: &str) -> Result<&'a RelationshipSchema, AnalyzerError> {
        self.schema.get_rel_schema(type_name)
            .map_err(|_| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))
    }

    /// Resolve a node property to its underlying column
    pub fn resolve_node_property(&self, label: &str, property: &str) -> Result<String, AnalyzerError> {
        // Try to get the node schema and look up the property mapping
        let node_schema = self.schema.get_node_schema(label)
            .map_err(|_| AnalyzerError::NodeLabelNotFound(label.to_string()))?;

        // Try explicit mapping first, fallback to identity mapping (property name = column name)
        // This supports wide tables without requiring hundreds of explicit mappings
        Ok(node_schema.property_mappings
            .get(property)
            .cloned()
            .unwrap_or_else(|| property.to_string()))
    }

    /// Resolve a relationship property to its underlying column
    pub fn resolve_relationship_property(&self, type_name: &str, property: &str) -> Result<String, AnalyzerError> {
        // Try to get the relationship schema and look up the property mapping
        let rel_schema = self.schema.get_rel_schema(type_name)
            .map_err(|_| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))?;

        // Try explicit mapping first, fallback to identity mapping (property name = column name)
        // This supports wide tables without requiring hundreds of explicit mappings
        Ok(rel_schema.property_mappings
            .get(property)
            .cloned()
            .unwrap_or_else(|| property.to_string()))
    }
}
