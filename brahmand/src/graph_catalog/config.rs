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
use super::graph_schema::{GraphViewDefinition, NodeViewMapping, RelationshipViewMapping, GraphSchema, NodeSchema, RelationshipSchema, NodeIdSchema};

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

/// Configuration for graph views loaded from YAML/JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphViewConfig {
    /// Name of the graph view configuration
    pub name: String,
    /// Version of the configuration format
    pub version: String,
    /// View definitions
    pub views: Vec<GraphViewDefinition>,
}

/// Configuration for graph schema loaded from YAML
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
    pub properties: HashMap<String, String>,
}

/// Relationship definition in schema config
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Property mappings
    pub properties: HashMap<String, String>,
}

impl GraphViewConfig {
    /// Load graph view configuration from a YAML file and validate against ClickHouse schema
    pub async fn from_yaml_file_validated<P: AsRef<Path>>(
        path: P,
        validator: &mut SchemaValidator,
    ) -> Result<Self, GraphSchemaError> {
        let config = Self::from_yaml_file(path)?;
        config.validate_schema(validator).await?;
        Ok(config)
    }

    /// Load graph view configuration from a YAML file
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphSchemaError> {
        let contents = fs::read_to_string(path)
            .map_err(|e| GraphSchemaError::ConfigReadError {
                error: e.to_string(),
            })?;
        
        let config = Self::from_yaml_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Parse graph view configuration from YAML string
    pub fn from_yaml_str(yaml: &str) -> Result<Self, GraphSchemaError> {
        serde_yaml::from_str(yaml)
            .map_err(|e| GraphSchemaError::ConfigParseError {
                error: e.to_string(),
            })
    }

    /// Load graph view configuration from a JSON file
    pub fn from_json_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphSchemaError> {
        let contents = fs::read_to_string(path)
            .map_err(|e| GraphSchemaError::ConfigReadError {
                error: e.to_string(),
            })?;
        
        let config = Self::from_json_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Parse graph view configuration from JSON string
    pub fn from_json_str(json: &str) -> Result<Self, GraphSchemaError> {
        serde_json::from_str(json)
            .map_err(|e| GraphSchemaError::ConfigParseError {
                error: e.to_string(),
            })
    }

    /// Validate the configuration structure and contents
    pub fn validate(&self) -> Result<(), GraphSchemaError> {
        // Check version format
        if !self.version.chars().all(|c| c.is_digit(10) || c == '.') {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("Invalid version format: {}", self.version),
            });
        }

        // Validate each view
        for view in &self.views {
            self.validate_view(view)?;
        }

        Ok(())
    }

    /// Validate a single view definition
    fn validate_view(&self, view: &GraphViewDefinition) -> Result<(), GraphSchemaError> {
        // Ensure view has at least one node mapping
        if view.nodes.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("View '{}' must have at least one node mapping", view.name),
            });
        }

        // Validate node mappings
        for (label, mapping) in &view.nodes {
            self.validate_node_mapping(label, mapping)?;
        }

        // Validate relationship mappings
        for (type_name, mapping) in &view.relationships {
            self.validate_relationship_mapping(type_name, mapping)?;
        }

        Ok(())
    }

    /// Validate a node view mapping
    fn validate_node_mapping(&self, label: &str, mapping: &NodeViewMapping) -> Result<(), GraphSchemaError> {
        // Check required fields are not empty
        if mapping.source_table.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("Node '{}': source_table cannot be empty", label),
            });
        }
        if mapping.id_column.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("Node '{}': id_column cannot be empty", label),
            });
        }

        Ok(())
    }

    /// Validate a relationship view mapping
    fn validate_relationship_mapping(&self, type_name: &str, mapping: &RelationshipViewMapping) -> Result<(), GraphSchemaError> {
        // Check required fields are not empty
        if mapping.source_table.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("Relationship '{}': source_table cannot be empty", type_name),
            });
        }
        if mapping.from_column.is_empty() || mapping.to_column.is_empty() {
            return Err(GraphSchemaError::InvalidConfig {
                message: format!("Relationship '{}': from_column and to_column must be specified", type_name),
            });
        }

        Ok(())
    }

    /// Validate configuration against ClickHouse schema
    pub async fn validate_schema(&self, validator: &mut SchemaValidator) -> Result<(), GraphSchemaError> {
        for view in &self.views {
            // Validate all node mappings
            for (_label, node_mapping) in &view.nodes {
                validator.validate_node_mapping(node_mapping).await?;
            }

            // Validate all relationship mappings
            for (_type, rel_mapping) in &view.relationships {
                validator.validate_relationship_mapping(rel_mapping).await?;
            }
        }

        Ok(())
    }
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
            let rel_schema = RelationshipSchema {
                database: rel_def.database.clone(),
                table_name: rel_def.table.clone(),
                column_names: rel_def.properties.values().cloned().collect(),
                from_node: "Unknown".to_string(), // Could be inferred or made configurable
                to_node: "Unknown".to_string(),   // Could be inferred or made configurable
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let yaml = r#"
name: test_view
version: "1.0"
views:
  - name: user_graph
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
        label: User
    relationships:
      follows:
        source_table: follows
        from_column: follower_id
        to_column: followed_id
        property_mappings: {}
        type_name: FOLLOWS
"#;
        let config = GraphViewConfig::from_yaml_str(yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_version() {
        let yaml = r#"
name: test_view
version: "invalid"
views: []
"#;
        let config = GraphViewConfig::from_yaml_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_node_mapping() {
        let yaml = r#"
name: test_view
version: "1.0"
views:
  - name: empty_graph
    nodes: {}
    relationships: {}
"#;
        let config = GraphViewConfig::from_yaml_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_node_mapping() {
        let yaml = r#"
name: test_view
version: "1.0"
views:
  - name: user_graph
    nodes:
      user:
        source_table: ""
        id_column: user_id
        property_mappings: {}
        label: User
    relationships: {}
"#;
        let config = GraphViewConfig::from_yaml_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }
}
