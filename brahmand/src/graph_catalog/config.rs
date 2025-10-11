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
use serde::{Deserialize, Serialize};
use serde_yaml;
use super::errors::GraphSchemaError;
use super::schema_validator::SchemaValidator;
use super::graph_schema::{GraphViewDefinition, NodeViewMapping, RelationshipViewMapping};

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
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphViewConfig {
    /// Name of the graph view configuration
    pub name: String,
    /// Version of the configuration format
    pub version: String,
    /// View definitions
    pub views: Vec<GraphViewDefinition>,
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