//! Schema Property Filter
//!
//! Filters node and relationship schemas based on required properties from WHERE clauses.
//! 
//! Core principle: If a WHERE clause references a property (e.g., `n.bytes_sent > 100`),
//! then only node/relationship types that have that property in their schema should be
//! included in the UNION.
//!
//! Examples:
//! - Query: `MATCH (n) WHERE n.bytes_sent > 100`
//! - Schema has: User(name, email), NetworkConnection(bytes_sent, timestamp)
//! - Filter result: Only NetworkConnection (has bytes_sent property)

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashSet;

/// Filters schemas based on required properties
pub struct SchemaPropertyFilter<'a> {
    schema: &'a GraphSchema,
}

impl<'a> SchemaPropertyFilter<'a> {
    /// Create a new schema property filter
    pub fn new(schema: &'a GraphSchema) -> Self {
        SchemaPropertyFilter { schema }
    }

    /// Filter node schemas that have ALL required properties
    ///
    /// Returns: Vector of node label names that have all required properties
    ///
    /// # Arguments
    /// * `required_properties` - Set of property names that must exist
    ///
    /// # Returns
    /// Vector of node label names (e.g., ["User", "NetworkConnection"])
    pub fn filter_node_schemas(&self, required_properties: &HashSet<String>) -> Vec<String> {
        // If no properties required, return all node schemas
        if required_properties.is_empty() {
            return self.schema
                .all_node_schemas()
                .keys()
                .cloned()
                .collect();
        }

        let all_node_schemas = self.schema.all_node_schemas();
        let mut matching_labels = Vec::new();

        for (label, node_schema) in all_node_schemas {
            // Check if this node schema has ALL required properties
            let schema_properties: HashSet<String> = node_schema.property_mappings.keys()
                .map(|k| k.to_string())
                .collect();

            if required_properties.is_subset(&schema_properties) {
                matching_labels.push(label.clone());
            }
        }

        log::debug!(
            "SchemaPropertyFilter: Required properties {:?}, found {} matching node types: {:?}",
            required_properties,
            matching_labels.len(),
            matching_labels
        );

        matching_labels
    }

    /// Filter relationship schemas that have ALL required properties
    ///
    /// Returns: Vector of relationship type names that have all required properties
    ///
    /// # Arguments
    /// * `required_properties` - Set of property names that must exist
    ///
    /// # Returns
    /// Vector of relationship type names (e.g., ["FOLLOWS", "AUTHORED"])
    pub fn filter_relationship_schemas(&self, required_properties: &HashSet<String>) -> Vec<String> {
        // If no properties required, return all relationship schemas
        if required_properties.is_empty() {
            return self.schema
                .get_relationships_schemas()
                .keys()
                .cloned()
                .collect();
        }

        let all_rel_schemas = self.schema.get_relationships_schemas();
        let mut matching_types = Vec::new();

        for (rel_type, rel_schema) in all_rel_schemas {
            // Check if this relationship schema has ALL required properties
            let schema_properties: HashSet<String> = rel_schema.property_mappings.keys()
                .map(|k| k.to_string())
                .collect();

            if required_properties.is_subset(&schema_properties) {
                matching_types.push(rel_type.clone());
            }
        }

        log::debug!(
            "SchemaPropertyFilter: Required properties {:?}, found {} matching relationship types: {:?}",
            required_properties,
            matching_types.len(),
            matching_types
        );

        matching_types
    }
}

// Tests will be added as integration tests using real schema files
