use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::engine_detection::TableEngine;
use super::errors::GraphSchemaError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub primary_keys: String,
    pub node_id: NodeIdSchema,
    pub property_mappings: HashMap<String, String>,
    /// Optional: List of view parameters for parameterized views
    /// Example: Some(vec!["tenant_id".to_string(), "region".to_string()])
    pub view_parameters: Option<Vec<String>>,
    /// Table engine type (for FINAL keyword support)
    #[serde(skip)]
    pub engine: Option<TableEngine>,
    /// Optional: Whether to use FINAL keyword for this table
    /// - None: Auto-detect based on engine type
    /// - Some(true): Always use FINAL
    /// - Some(false): Never use FINAL
    pub use_final: Option<bool>,

    // ===== Denormalized node support =====

    /// If true, this node is denormalized on one or more edge tables
    /// (no physical node table exists)
    #[serde(skip)]
    pub is_denormalized: bool,

    /// Property mappings when this node appears as from_node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": "Origin", "city": "OriginCity"}
    #[serde(skip)]
    pub from_properties: Option<HashMap<String, String>>,

    /// Property mappings when this node appears as to_node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": "Dest", "city": "DestCity"}
    #[serde(skip)]
    pub to_properties: Option<HashMap<String, String>>,
}

impl NodeSchema {
    /// Determine if FINAL should be used for this node
    pub fn should_use_final(&self) -> bool {
        // 1. Check explicit override (user choice takes precedence)
        if let Some(use_final) = self.use_final {
            return use_final;
        }

        // 2. Auto-detect: Use FINAL for engines that need it for correctness
        // (Conservative: only deduplication/collapsing engines)
        if let Some(ref engine) = self.engine {
            engine.requires_final_for_correctness()
        } else {
            // No engine information - default to false
            false
        }
    }

    /// Check if this engine supports FINAL (regardless of whether we use it by default)
    pub fn can_use_final(&self) -> bool {
        if let Some(ref engine) = self.engine {
            engine.supports_final()
        } else {
            false
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub from_node: String, // Node type (e.g., "User")
    pub to_node: String,   // Node type (e.g., "User")
    pub from_id: String,   // Column name for source node ID (e.g., "user1_id")
    pub to_id: String,     // Column name for target node ID (e.g., "user2_id")
    pub from_node_id_dtype: String,
    pub to_node_id_dtype: String,
    pub property_mappings: HashMap<String, String>,
    /// Optional: List of view parameters for parameterized views
    pub view_parameters: Option<Vec<String>>,
    /// Table engine type (for FINAL keyword support)
    #[serde(skip)]
    pub engine: Option<TableEngine>,
    /// Optional: Whether to use FINAL keyword for this table
    /// - None: Auto-detect based on engine type
    /// - Some(true): Always use FINAL
    /// - Some(false): Never use FINAL
    pub use_final: Option<bool>,

    // ===== Denormalized node properties on edge tables =====

    /// Optional: Denormalized node properties (source node)
    /// Maps graph property names to table columns
    /// Example: {"city": "OriginCityName", "state": "OriginState"}
    #[serde(skip)]
    pub from_node_properties: Option<HashMap<String, String>>,

    /// Optional: Denormalized node properties (target node)
    /// Maps graph property names to table columns
    /// Example: {"city": "DestCityName", "state": "DestState"}
    #[serde(skip)]
    pub to_node_properties: Option<HashMap<String, String>>,
}

impl RelationshipSchema {
    /// Determine if FINAL should be used for this relationship
    pub fn should_use_final(&self) -> bool {
        // 1. Check explicit override (user choice takes precedence)
        if let Some(use_final) = self.use_final {
            return use_final;
        }

        // 2. Auto-detect: Use FINAL for engines that need it for correctness
        if let Some(ref engine) = self.engine {
            engine.requires_final_for_correctness()
        } else {
            false
        }
    }

    /// Check if this engine supports FINAL
    pub fn can_use_final(&self) -> bool {
        if let Some(ref engine) = self.engine {
            engine.supports_final()
        } else {
            false
        }
    }
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

#[derive(Debug, Clone)]
pub enum GraphSchemaElement {
    Node(NodeSchema),
    Rel(RelationshipSchema),
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
}

impl GraphSchema {
    pub fn build(
        version: u32,
        database: String,
        nodes: HashMap<String, NodeSchema>,
        relationships: HashMap<String, RelationshipSchema>,
    ) -> GraphSchema {
        GraphSchema {
            version,
            database,
            nodes,
            relationships,
        }
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn insert_node_schema(&mut self, node_label: String, node_schema: NodeSchema) {
        self.nodes.insert(node_label, node_schema);
    }

    pub fn insert_relationship_schema(
        &mut self,
        type_name: String,
        rel_schema: RelationshipSchema,
    ) {
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
}
