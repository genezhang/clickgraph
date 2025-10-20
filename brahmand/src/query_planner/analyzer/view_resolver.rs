//! View resolution for Cypher query planning.
//! 
//! This module handles resolving graph views to their underlying ClickHouse tables
//! during query planning. It maps node and relationship patterns to their
//! corresponding view definitions and resolves property references.

use std::collections::HashMap;

use crate::graph_catalog::{
    GraphViewDefinition,
    NodeViewMapping,
    RelationshipViewMapping,
    graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
};

// Removed unused imports
use super::errors::AnalyzerError;

/// Resolves view references during query planning
#[derive(Debug, Clone)]
pub struct ViewResolver<'a> {
    /// Graph schema for direct lookups
    schema: &'a GraphSchema,
    /// Node contexts for resolving property mappings
    left_node: Option<&'a NodeViewMapping>,
    rel: Option<&'a RelationshipViewMapping>,
    right_node: Option<&'a NodeViewMapping>,
    /// Active view definition
    view: &'a GraphViewDefinition,
    /// Cache of resolved node mappings
    node_mappings: HashMap<String, &'a NodeViewMapping>,
    /// Cache of resolved relationship mappings
    relationship_mappings: HashMap<String, &'a RelationshipViewMapping>,
}

impl<'a> ViewResolver<'a> {
    pub fn new(
        schema: &'a GraphSchema,
        view: &'a GraphViewDefinition
    ) -> Self {
        let mut resolver = ViewResolver {
            schema,
            left_node: None,
            rel: None,
            right_node: None,
            view,
            node_mappings: HashMap::new(),
            relationship_mappings: HashMap::new(),
        };
        resolver.initialize_mappings();
        resolver
    }

    /// Get schema for a node from the view definition
    pub fn get_node_schema(&self, table_name: &str) -> Option<&'a NodeSchema> {
        // Look up mapping from view definition
        self.view.nodes.get(table_name).map(|mapping| {
            // For now, return underlying table schema
            // TODO: Transform view mapping into proper NodeSchema
            self.schema.get_node_schema(&mapping.source_table)
                .ok()
        }).flatten()
    }

    /// Get schema for a relationship from the view definition  
    pub fn get_relationship_schema(&self, table_name: &str) -> Option<&'a RelationshipSchema> {
        // Look up mapping from view definition
        self.view.relationships.get(table_name).map(|mapping| {
            // For now, return underlying table schema
            // TODO: Transform view mapping into proper RelationshipSchema
            self.schema.get_rel_schema(&mapping.source_table)
                .ok()
        }).flatten()
    }

    /// Initialize mapping caches
    fn initialize_mappings(&mut self) {
        self.node_mappings = self.view.nodes
            .iter()
            .map(|(label, mapping)| (label.clone(), mapping))
            .collect();

        self.relationship_mappings = self.view.relationships
            .iter()
            .map(|(type_name, mapping)| (type_name.clone(), mapping))
            .collect();
    }

    /// Get the complete view mapping for a node label
    pub fn resolve_node(&self, label: &str) -> Result<(NodeViewMapping, &'a NodeSchema), AnalyzerError> {
        // Get the view mapping for this label
        let mapping = self.node_mappings.get(label)
            .ok_or_else(|| AnalyzerError::NodeLabelNotFound(label.to_string()))?;

        let schema = self.schema.get_node_schema(&mapping.source_table)
            .map_err(|_| AnalyzerError::TableNotFound(mapping.source_table.clone()))?;

        // Return cloned mapping and schema reference
        Ok(((*mapping).clone(), schema))
    }

    /// Get the complete view mapping for a relationship type 
    pub fn resolve_relationship(&self, type_name: &str) -> Result<(RelationshipViewMapping, &'a RelationshipSchema), AnalyzerError> {
        // Get the view mapping for this type
        let mapping = self.relationship_mappings.get(type_name)
            .ok_or_else(|| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))?;

        let schema = self.schema.get_rel_schema(&mapping.source_table)
            .map_err(|_| AnalyzerError::TableNotFound(mapping.source_table.clone()))?;

        // Return cloned mapping and schema reference
        Ok(((*mapping).clone(), schema))
    }

    /// Resolve a node property to its underlying column
    pub fn resolve_node_property(&self, label: &str, property: &str) -> Result<String, AnalyzerError> {
        let mapping = self.node_mappings.get(label)
            .ok_or_else(|| AnalyzerError::NodeLabelNotFound(label.to_string()))?;

        mapping.property_mappings.get(property)
            .cloned()
            .ok_or_else(|| AnalyzerError::PropertyNotFound {
                entity_type: "node".to_string(),
                entity_name: label.to_string(),
                property: property.to_string(),
            })
    }

    /// Resolve a relationship property to its underlying column
    pub fn resolve_relationship_property(&self, type_name: &str, property: &str) -> Result<String, AnalyzerError> {
        let mapping = self.relationship_mappings.get(type_name)
            .ok_or_else(|| AnalyzerError::RelationshipTypeNotFound(type_name.to_string()))?;

        mapping.property_mappings.get(property)
            .cloned()
            .ok_or_else(|| AnalyzerError::PropertyNotFound {
                entity_type: "relationship".to_string(),
                entity_name: type_name.to_string(),
                property: property.to_string(),
            })
    }

    /// Get any additional filtering conditions from the view definition
    pub fn get_view_filters(&self, label: &str) -> Option<String> {
        self.node_mappings.get(label)
            .and_then(|mapping| mapping.filter_condition.clone())
    }
}