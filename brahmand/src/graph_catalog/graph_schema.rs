use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::errors::GraphSchemaError;

/// Defines how a graph view maps to underlying ClickHouse tables
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphViewDefinition {
    /// Name of the graph view
    pub name: String,
    /// Mappings for each node label to source tables
    pub nodes: HashMap<String, NodeViewMapping>,
    /// Mappings for each relationship type to source tables
    pub relationships: HashMap<String, RelationshipViewMapping>,
}

/// Maps a node label to a source table in ClickHouse
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeViewMapping {
    /// Source table name in ClickHouse
    pub source_table: String,
    /// Column that contains the node ID
    pub id_column: String,
    /// Mapping of property names to column names
    pub property_mappings: HashMap<String, String>,
    /// Node label this mapping creates
    pub label: String,
    /// Optional WHERE clause filter
    pub filter_condition: Option<String>,
}

/// Maps a relationship type to a source table in ClickHouse
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipViewMapping {
    /// Source table name in ClickHouse
    pub source_table: String,
    /// Column containing the source node ID
    pub from_column: String,
    /// Column containing the target node ID 
    pub to_column: String,
    /// Mapping of property names to column names
    pub property_mappings: HashMap<String, String>,
    /// Relationship type this mapping creates
    pub type_name: String,
    /// Optional WHERE clause filter
    pub filter_condition: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSchema {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub primary_keys: String,
    pub node_id: NodeIdSchema,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub from_node: String,
    pub to_node: String,
    pub from_node_id_dtype: String,
    pub to_node_id_dtype: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipIndexSchema {
    pub base_rel_table_name: String,
    pub table_name: String,
    pub direction: Direction,
    pub index_type: IndexType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Direction {
    Outgoing,
    Incoming,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Incoming => f.write_str("incoming"),
            Direction::Outgoing => f.write_str("outgoing"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IndexType {
    Bitmap,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::Bitmap => f.write_str("Bitmap"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum GraphSchemaElement {
    Node(NodeSchema),
    Rel(RelationshipSchema),
    RelIndex(RelationshipIndexSchema),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeIdSchema {
    pub column: String,
    pub dtype: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphSchema {
    version: u32,
    nodes: HashMap<String, NodeSchema>,
    relationships: HashMap<String, RelationshipSchema>,
    relationships_indexes: HashMap<String, RelationshipIndexSchema>,
}

/// Trait for resolving view mappings to actual schemas
pub trait ViewSchemaResolver {
    /// Convert a view mapping to a concrete node schema
    fn resolve_node_view(&self, mapping: &NodeViewMapping) -> Result<NodeSchema, GraphSchemaError>;
    /// Convert a view mapping to a concrete relationship schema
    fn resolve_relationship_view(&self, mapping: &RelationshipViewMapping) -> Result<RelationshipSchema, GraphSchemaError>;
}

impl ViewSchemaResolver for GraphSchema {
    fn resolve_node_view(&self, mapping: &NodeViewMapping) -> Result<NodeSchema, GraphSchemaError> {
        // Validate source table and columns exist (in real implementation, check against ClickHouse)
        self.validate_table_exists(&mapping.source_table)?;
        self.validate_column_exists(&mapping.source_table, &mapping.id_column)?;
        
        for column in mapping.property_mappings.values() {
            self.validate_column_exists(&mapping.source_table, column)?;
        }

        // Convert view mapping to concrete schema
        let mut column_names = Vec::new();
        column_names.extend(mapping.property_mappings.values().cloned());
        column_names.push(mapping.id_column.clone());

        Ok(NodeSchema {
            table_name: mapping.source_table.clone(),
            column_names,
            primary_keys: mapping.id_column.clone(),
            node_id: NodeIdSchema {
                column: mapping.id_column.clone(),
                dtype: self.get_column_type(&mapping.source_table, &mapping.id_column)
                    .map_err(|_| GraphSchemaError::InvalidIdColumnType {
                        column: mapping.id_column.clone(),
                        table: mapping.source_table.clone(),
                    })?,
            },
        })
    }

    fn resolve_relationship_view(&self, mapping: &RelationshipViewMapping) -> Result<RelationshipSchema, GraphSchemaError> {
        // Validate source table and columns exist
        self.validate_table_exists(&mapping.source_table)?;
        self.validate_column_exists(&mapping.source_table, &mapping.from_column)?;
        self.validate_column_exists(&mapping.source_table, &mapping.to_column)?;

        for column in mapping.property_mappings.values() {
            self.validate_column_exists(&mapping.source_table, column)?;
        }

        // Get column types for the node ID columns
        let from_type = self.get_column_type(&mapping.source_table, &mapping.from_column)
            .map_err(|_| GraphSchemaError::InvalidIdColumnType {
                column: mapping.from_column.clone(),
                table: mapping.source_table.clone(),
            })?;
        
        let to_type = self.get_column_type(&mapping.source_table, &mapping.to_column)
            .map_err(|_| GraphSchemaError::InvalidIdColumnType {
                column: mapping.to_column.clone(),
                table: mapping.source_table.clone(),
            })?;

        let mut column_names = Vec::new();
        column_names.extend(mapping.property_mappings.values().cloned());
        column_names.push(mapping.from_column.clone());
        column_names.push(mapping.to_column.clone());

        Ok(RelationshipSchema {
            table_name: mapping.source_table.clone(),
            column_names,
            from_node: mapping.from_column.clone(),
            to_node: mapping.to_column.clone(),
            from_node_id_dtype: from_type,
            to_node_id_dtype: to_type,
        })
    }
}

impl GraphSchema {
    pub fn build(
        version: u32,
        nodes: HashMap<String, NodeSchema>,
        relationships: HashMap<String, RelationshipSchema>,
        relationships_indexes: HashMap<String, RelationshipIndexSchema>,
    ) -> GraphSchema {
        GraphSchema {
            version,
            nodes,
            relationships,
            relationships_indexes,
        }
    }

    pub fn insert_node_schema(&mut self, node_label: String, node_schema: NodeSchema) {
        self.nodes.insert(node_label, node_schema);
    }

    pub fn insert_relationship_schema(&mut self, type_name: String, rel_schema: RelationshipSchema) {
        self.relationships.insert(type_name, rel_schema);
    }

    /// Register a view mapping in the schema
    pub fn register_view(&mut self, view: GraphViewDefinition) -> Result<(), GraphSchemaError> {
        // First validate that all referenced tables exist
        for (label, node_mapping) in &view.nodes {
            let node_schema = self.resolve_node_view(node_mapping)?;
            self.insert_node_schema(label.clone(), node_schema);
        }

        for (type_name, rel_mapping) in &view.relationships {
            let rel_schema = self.resolve_relationship_view(rel_mapping)?;
            self.insert_relationship_schema(type_name.clone(), rel_schema);
        }

        Ok(())
    }

    // Helper methods for validation
    fn validate_table_exists(&self, _table: &str) -> Result<(), GraphSchemaError> {
        // TODO: Implement actual ClickHouse table existence check
        // For now, assume table exists
        Ok(())
    }

    fn validate_column_exists(&self, _table: &str, _column: &str) -> Result<(), GraphSchemaError> {
        // TODO: Implement actual ClickHouse column existence check
        // For now, assume column exists
        Ok(())
    }

    fn get_column_type(&self, _table: &str, _column: &str) -> Result<String, GraphSchemaError> {
        // TODO: Implement actual ClickHouse column type lookup
        // For now, return a default type
        Ok("UInt64".to_string())
    }

    pub fn insert_rel_schema(&mut self, rel_label: String, rel_schema: RelationshipSchema) {
        self.relationships.insert(rel_label, rel_schema);
    }

    pub fn insert_rel_index_schema(
        &mut self,
        rel_label: String,
        rel_index_schema: RelationshipIndexSchema,
    ) {
        self.relationships_indexes
            .insert(rel_label, rel_index_schema);
    }

    pub fn get_version(&self) -> u32 {
        self.version
    }

    pub fn increment_version(&mut self) {
        self.version += 1;
    }

    pub fn get_node_schema(&self, node_label: &str) -> Result<&NodeSchema, GraphSchemaError> {
        self.nodes.get(node_label).ok_or(GraphSchemaError::Node {
            node_label: node_label.to_string(),
        })
    }

    pub fn get_rel_schema(&self, rel_label: &str) -> Result<&RelationshipSchema, GraphSchemaError> {
        self.relationships
            .get(rel_label)
            .ok_or(GraphSchemaError::Relation {
                rel_label: rel_label.to_string(),
            })
    }

    pub fn get_rel_index_schema(
        &self,
        rel_label: &str,
    ) -> Result<&RelationshipIndexSchema, GraphSchemaError> {
        self.relationships_indexes
            .get(rel_label)
            .ok_or(GraphSchemaError::RelationIndex {
                rel_label: rel_label.to_string(),
            })
    }

    pub fn get_relationships_schemas(&self) -> &HashMap<String, RelationshipSchema> {
        &self.relationships
    }

    pub fn get_nodes_schemas(&self) -> &HashMap<String, NodeSchema> {
        &self.nodes
    }

    pub fn get_node_schema_opt(&self, node_label: &str) -> Option<&NodeSchema> {
        self.nodes.get(node_label)
    }

    pub fn get_relationships_schema_opt(&self, rel_label: &str) -> Option<&RelationshipSchema> {
        self.relationships.get(rel_label)
    }

    pub fn get_relationship_index_schema_opt(
        &self,
        rel_label: &str,
    ) -> Option<&RelationshipIndexSchema> {
        self.relationships_indexes.get(rel_label)
    }
}
