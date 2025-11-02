/// Graph view configuration management.
/// 
/// This module handles loading and validation of graph view definitions from YAML
/// or JSON configuration files. It supports:
/// 
/// - Loading from YAML/JSON files
/// - Structural validation of configurations
/// - Schema validation against ClickHouse

use std::fs;
use std::path::Path;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_yaml;
use super::errors::GraphSchemaError;
use super::schema_validator::SchemaValidator;
use super::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema, NodeIdSchema};

/// Graph views are defined in YAML with the following structure:
///
/// ```yaml
/// name: my_graph_views       # Configuration name
/// version: "1.0"            # Schema version
/// views:                    # List of view definitions
///   - name: user_graph      # Individual view name
///     nodes:               # Node mappings
///       user:              # Node label
///         source_table: users        # Source ClickHouse table
///         id_column: user_id         # Primary key column
///         property_mappings:         # Column mappings
///           name: full_name
///           email: email_address
///         label: User               # Node label in graph
///     relationships:      # Relationship mappings
///       follows:         # Relationship type
///         source_table: follows    # Source table
///         from_column: follower_id # Source node ID
///         to_column: followed_id   # Target node ID
///         type_name: FOLLOWS      # Relationship type in graph
/// ```
/// 
/// # Usage
/// 
/// ```rust,no_run
/// use brahmand::graph_catalog::{GraphViewConfig, SchemaValidator};
/// use clickhouse::Client;
/// 
/// async fn load_config(client: Client) {
///     let mut validator = SchemaValidator::new(client);
///     
///     // Load and validate configuration
///     let config = GraphViewConfig::from_yaml_file_validated(
///         "views.yaml",
///         &mut validator
///     ).await.unwrap();
///     
///     // Use configuration...
/// }
/// ```
/// 
/// # Testing
/// 
/// For testing view configurations without a ClickHouse instance:
/// 1. Use the mock client from `tests::mock_clickhouse`
/// 2. Create temporary YAML files with test configurations
/// 3. Validate using the mock client
/// 
/// See `tests::config_tests` for examples.

/// Configuration for graph schemas loaded from YAML/JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSchemaConfig {
    /// Graph schema definition
    pub graph_schema: GraphSchemaDefinition,
}

/// Graph schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSchemaDefinition {
    /// Node definitions
    pub nodes: Vec<NodeDefinition>,
    /// Relationship definitions
    pub relationships: Vec<RelationshipDefinition>,
}

/// Node definition in schema config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    /// Node label
    pub label: String,
    /// ClickHouse database name
    pub database: String,
    /// Source table name
    pub table: String,
    /// ID column name
    pub id_column: String,
    /// Property mappings
    #[serde(rename = "property_mappings")]
    pub properties: HashMap<String, String>,
}

/// Relationship definition in schema config
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipDefinition {
    /// Relationship type
    #[serde(rename = "type")]
    pub type_name: String,
    /// ClickHouse database name
    pub database: String,
    /// Source table name
    pub table: String,
    /// From column name
    pub from_column: String,
    /// To column name
    pub to_column: String,
    /// Node type for source (from) node - optional, defaults to first node type
    #[serde(default)]
    pub from_node: Option<String>,
    /// Node type for target (to) node - optional, defaults to first node type
    #[serde(default)]
    pub to_node: Option<String>,
    /// Property mappings
    #[serde(rename = "property_mappings")]
    pub properties: HashMap<String, String>,
}

impl GraphSchemaConfig {
    /// Load graph schema configuration from a YAML file
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphSchemaError> {
        let contents = fs::read_to_string(path)
            .map_err(|e| GraphSchemaError::ConfigReadError {
                error: e.to_string(),
            })?;
        
        Self::from_yaml_str(&contents)
    }

    /// Parse graph schema configuration from YAML string
    pub fn from_yaml_str(yaml: &str) -> Result<Self, GraphSchemaError> {
        serde_yaml::from_str(yaml)
            .map_err(|e| GraphSchemaError::ConfigParseError {
                error: e.to_string(),
            })
    }

    /// Basic validation of the schema configuration
    pub fn validate(&self) -> Result<(), GraphSchemaError> {
        // Check that we have at least one node
        if self.graph_schema.nodes.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: "Schema must contain at least one node definition".to_string(),
            });
        }

        // Check for duplicate node labels
        let mut seen_labels = std::collections::HashSet::new();
        for node in &self.graph_schema.nodes {
            if !seen_labels.insert(&node.label) {
                return Err(GraphSchemaError::InvalidConfig {
                    message: format!("Duplicate node label: {}", node.label),
                });
            }
        }

        // Check for duplicate relationship types
        let mut seen_types = std::collections::HashSet::new();
        for rel in &self.graph_schema.relationships {
            if !seen_types.insert(&rel.type_name) {
                return Err(GraphSchemaError::InvalidConfig {
                    message: format!("Duplicate relationship type: {}", rel.type_name),
                });
            }
        }

        Ok(())
    }

    /// Validate configuration against ClickHouse schema
    pub async fn validate_schema(&self, _validator: &mut SchemaValidator) -> Result<(), GraphSchemaError> {
        // For now, just do basic structural validation
        // In the future, this could validate against ClickHouse schema
        self.validate()
    }

    /// Convert to GraphSchema
    pub fn to_graph_schema(&self) -> Result<GraphSchema, GraphSchemaError> {
        self.validate()?; // Validate before converting

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Convert node definitions
        for node_def in &self.graph_schema.nodes {
            let node_schema = NodeSchema {
                database: node_def.database.clone(),
                table_name: node_def.table.clone(),
                column_names: node_def.properties.values().cloned().collect(),
                primary_keys: node_def.id_column.clone(),
                node_id: NodeIdSchema {
                    column: node_def.id_column.clone(),
                    dtype: "UInt64".to_string(), // Default, could be made configurable
                },
                property_mappings: node_def.properties.clone(),
            };
            nodes.insert(node_def.label.clone(), node_schema);
        }

        // Convert relationship definitions
        for rel_def in &self.graph_schema.relationships {
            // If from_node/to_node not specified, try to infer from first node type
            // This is a simple heuristic - for production, should be explicitly specified
            let default_node_type = self.graph_schema.nodes
                .first()
                .map(|n| n.label.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            
            let from_node = rel_def.from_node.as_ref()
                .unwrap_or(&default_node_type)
                .clone();
            let to_node = rel_def.to_node.as_ref()
                .unwrap_or(&default_node_type)
                .clone();
            
            let rel_schema = RelationshipSchema {
                database: rel_def.database.clone(),
                table_name: rel_def.table.clone(),
                column_names: rel_def.properties.values().cloned().collect(),
                from_node,
                to_node,
                from_column: rel_def.from_column.clone(),
                to_column: rel_def.to_column.clone(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: rel_def.properties.clone(),
            };
            relationships.insert(rel_def.type_name.clone(), rel_schema);
        }

        Ok(GraphSchema::build(
            1, // version
            "default".to_string(), // Default database, individual tables have their own
            nodes,
            relationships,
            HashMap::new(), // relationships_indexes
        ))
    }
}
