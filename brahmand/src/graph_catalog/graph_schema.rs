use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::errors::GraphSchemaError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub primary_keys: String,
    pub node_id: NodeIdSchema,
    pub property_mappings: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub from_node: String,  // Node type (e.g., "User")
    pub to_node: String,    // Node type (e.g., "User")
    pub from_column: String,  // Column name for source node ID (e.g., "user1_id")
    pub to_column: String,    // Column name for target node ID (e.g., "user2_id")
    pub from_node_id_dtype: String,
    pub to_node_id_dtype: String,
    pub property_mappings: HashMap<String, String>,
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
    database: String,
    nodes: HashMap<String, NodeSchema>,
    relationships: HashMap<String, RelationshipSchema>,
    relationships_indexes: HashMap<String, RelationshipIndexSchema>,
}

impl GraphSchema {
    pub fn build(
        version: u32,
        database: String,
        nodes: HashMap<String, NodeSchema>,
        relationships: HashMap<String, RelationshipSchema>,
        relationships_indexes: HashMap<String, RelationshipIndexSchema>,
    ) -> GraphSchema {
        GraphSchema {
            version,
            database,
            nodes,
            relationships,
            relationships_indexes,
        }
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn insert_node_schema(&mut self, node_label: String, node_schema: NodeSchema) {
        self.nodes.insert(node_label, node_schema);
    }

    pub fn insert_relationship_schema(&mut self, type_name: String, rel_schema: RelationshipSchema) {
        self.relationships.insert(type_name, rel_schema);
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
