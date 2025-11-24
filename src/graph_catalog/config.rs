use super::errors::GraphSchemaError;
use super::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
use super::schema_validator::SchemaValidator;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;
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
///         from_id: follower_id # Source node ID
///         to_id: followed_id   # Target node ID
///         type_name: FOLLOWS      # Relationship type in graph
/// ```
///
/// # Usage
///
/// ```ignore
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
    /// Optional schema name (used for multi-schema registration)
    #[serde(default)]
    pub name: Option<String>,
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
    /// Optional: List of view parameters for parameterized views
    /// Example: ["tenant_id", "region"]
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,
    /// Optional: Whether to use FINAL keyword for this table
    /// - None (default): Auto-detect based on engine type
    /// - Some(true): Always use FINAL
    /// - Some(false): Never use FINAL
    #[serde(default)]
    pub use_final: Option<bool>,
    /// Optional: Auto-discover columns from ClickHouse table metadata
    /// When true, all table columns become properties with identity mappings
    /// (column_name → column_name), except those in exclude_columns.
    /// Manual property_mappings override auto-discovered mappings.
    #[serde(default)]
    pub auto_discover_columns: bool,
    /// Optional: Columns to exclude from auto-discovery
    /// Use for internal/system columns that shouldn't be exposed as properties
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    /// Optional: Naming convention for auto-discovered property names
    /// - "snake_case" (default): Keep original names (user_id → user_id)
    /// - "camelCase": Convert to camelCase (user_id → userId)
    #[serde(default = "default_naming_convention")]
    pub naming_convention: String,
}

fn default_naming_convention() -> String {
    "snake_case".to_string()
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
    /// From ID column name
    pub from_id: String,
    /// To ID column name
    pub to_id: String,
    /// Node label for source (from) node - optional, defaults to first node label
    #[serde(default)]
    pub from_node: Option<String>,
    /// Node label for target (to) node - optional, defaults to first node label
    #[serde(default)]
    pub to_node: Option<String>,
    /// Property mappings
    #[serde(rename = "property_mappings")]
    pub properties: HashMap<String, String>,
    /// Optional: List of view parameters for parameterized views
    /// Example: ["tenant_id", "region"]
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,
    /// Optional: Whether to use FINAL keyword for this table
    /// - None (default): Auto-detect based on engine type
    /// - Some(true): Always use FINAL
    /// - Some(false): Never use FINAL
    #[serde(default)]
    pub use_final: Option<bool>,
    /// Optional: Auto-discover columns from ClickHouse table metadata
    /// When true, all table columns become properties with identity mappings
    /// (column_name → column_name), except those in exclude_columns.
    /// Manual property_mappings override auto-discovered mappings.
    #[serde(default)]
    pub auto_discover_columns: bool,
    /// Optional: Columns to exclude from auto-discovery
    /// Use for internal/system columns that shouldn't be exposed as properties
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    /// Optional: Naming convention for auto-discovered property names
    /// - "snake_case" (default): Keep original names (user_id → user_id)
    /// - "camelCase": Convert to camelCase (user_id → userId)
    #[serde(default = "default_naming_convention")]
    pub naming_convention: String,
}

/// Convert snake_case to camelCase
fn snake_to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Apply naming convention to a column name
fn apply_naming_convention(column_name: &str, convention: &str) -> String {
    match convention {
        "camelCase" => snake_to_camel_case(column_name),
        _ => column_name.to_string(), // Default: keep as-is (snake_case)
    }
}

impl GraphSchemaConfig {
    /// Load graph schema configuration from a YAML file
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphSchemaError> {
        let contents = fs::read_to_string(path).map_err(|e| GraphSchemaError::ConfigReadError {
            error: e.to_string(),
        })?;

        Self::from_yaml_str(&contents)
    }

    /// Parse graph schema configuration from YAML string
    pub fn from_yaml_str(yaml: &str) -> Result<Self, GraphSchemaError> {
        serde_yaml::from_str(yaml).map_err(|e| GraphSchemaError::ConfigParseError {
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
    pub async fn validate_schema(
        &self,
        _validator: &mut SchemaValidator,
    ) -> Result<(), GraphSchemaError> {
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
                view_parameters: node_def.view_parameters.clone(),
                engine: None, // Will be populated during schema loading with ClickHouse client
                use_final: node_def.use_final,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
            };
            nodes.insert(node_def.label.clone(), node_schema);
        }

        // Convert relationship definitions
        for rel_def in &self.graph_schema.relationships {
            // If from_node/to_node not specified, try to infer from first node type
            // This is a simple heuristic - for production, should be explicitly specified
            let default_node_type = self
                .graph_schema
                .nodes
                .first()
                .map(|n| n.label.clone())
                .unwrap_or_else(|| "Unknown".to_string());

            let from_node = rel_def
                .from_node
                .as_ref()
                .unwrap_or(&default_node_type)
                .clone();
            let to_node = rel_def
                .to_node
                .as_ref()
                .unwrap_or(&default_node_type)
                .clone();

            let rel_schema = RelationshipSchema {
                database: rel_def.database.clone(),
                table_name: rel_def.table.clone(),
                column_names: rel_def.properties.values().cloned().collect(),
                from_node,
                to_node,
                from_id: rel_def.from_id.clone(),
                to_id: rel_def.to_id.clone(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: rel_def.properties.clone(),
                view_parameters: rel_def.view_parameters.clone(),
                engine: None, // Will be populated during schema loading with ClickHouse client
                use_final: rel_def.use_final,
                from_node_properties: None,
                to_node_properties: None,
            };
            relationships.insert(rel_def.type_name.clone(), rel_schema);
        }

        Ok(GraphSchema::build(
            1,                     // version
            "default".to_string(), // Default database, individual tables have their own
            nodes,
            relationships,
        ))
    }

    /// Convert to GraphSchema with auto-discovery and engine detection
    ///
    /// This method extends `to_graph_schema()` with:
    /// - Auto-discovery of table columns when `auto_discover_columns = true`
    /// - Automatic engine detection for FINAL keyword support
    ///
    /// # Arguments
    /// * `client` - ClickHouse client for querying metadata
    ///
    /// # Returns
    /// GraphSchema with auto-discovered properties and detected engines
    pub async fn to_graph_schema_with_client(
        &self,
        client: &clickhouse::Client,
    ) -> Result<GraphSchema, GraphSchemaError> {
        use super::column_info::query_table_columns;
        use super::engine_detection::detect_table_engine;

        self.validate()?;

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Convert node definitions with auto-discovery
        for node_def in &self.graph_schema.nodes {
            let property_mappings = if node_def.auto_discover_columns {
                // Auto-discover columns from ClickHouse
                let columns = query_table_columns(client, &node_def.database, &node_def.table)
                    .await
                    .map_err(|e| GraphSchemaError::ConfigReadError {
                        error: format!("Failed to query columns: {}", e),
                    })?;

                // Build identity mappings for all columns except excluded
                let mut mappings = HashMap::new();
                for col in columns {
                    if !node_def.exclude_columns.contains(&col) {
                        // Apply naming convention to property name
                        let property_name =
                            apply_naming_convention(&col, &node_def.naming_convention);
                        mappings.insert(property_name, col);
                    }
                }

                // Apply manual overrides from YAML (manual wins)
                mappings.extend(node_def.properties.clone());

                mappings
            } else {
                // Manual mode: use YAML as-is
                node_def.properties.clone()
            };

            // Auto-detect engine type
            let engine = detect_table_engine(client, &node_def.database, &node_def.table)
                .await
                .ok(); // Gracefully handle detection failures

            // Determine use_final: manual override > engine detection > false
            let use_final = node_def
                .use_final
                .unwrap_or_else(|| engine.as_ref().map(|e| e.supports_final()).unwrap_or(false));

            let node_schema = NodeSchema {
                database: node_def.database.clone(),
                table_name: node_def.table.clone(),
                column_names: property_mappings.values().cloned().collect(),
                primary_keys: node_def.id_column.clone(),
                node_id: NodeIdSchema {
                    column: node_def.id_column.clone(),
                    dtype: "UInt64".to_string(),
                },
                property_mappings,
                view_parameters: node_def.view_parameters.clone(),
                engine,
                use_final: Some(use_final),
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
            };
            nodes.insert(node_def.label.clone(), node_schema);
        }

        // Convert relationship definitions with auto-discovery
        for rel_def in &self.graph_schema.relationships {
            let property_mappings = if rel_def.auto_discover_columns {
                // Auto-discover columns from ClickHouse
                let columns = query_table_columns(client, &rel_def.database, &rel_def.table)
                    .await
                    .map_err(|e| GraphSchemaError::ConfigReadError {
                        error: format!("Failed to query columns: {}", e),
                    })?;

                // Build identity mappings for all columns except excluded
                let mut mappings = HashMap::new();
                for col in columns {
                    if !rel_def.exclude_columns.contains(&col) {
                        // Apply naming convention to property name
                        let property_name =
                            apply_naming_convention(&col, &rel_def.naming_convention);
                        mappings.insert(property_name, col);
                    }
                }

                // Apply manual overrides
                mappings.extend(rel_def.properties.clone());

                mappings
            } else {
                rel_def.properties.clone()
            };

            // Auto-detect engine type
            let engine = detect_table_engine(client, &rel_def.database, &rel_def.table)
                .await
                .ok();

            let use_final = rel_def
                .use_final
                .unwrap_or_else(|| engine.as_ref().map(|e| e.supports_final()).unwrap_or(false));

            let default_node_type = self
                .graph_schema
                .nodes
                .first()
                .map(|n| n.label.clone())
                .unwrap_or_else(|| "Unknown".to_string());

            let from_node = rel_def
                .from_node
                .as_ref()
                .unwrap_or(&default_node_type)
                .clone();
            let to_node = rel_def
                .to_node
                .as_ref()
                .unwrap_or(&default_node_type)
                .clone();

            let rel_schema = RelationshipSchema {
                database: rel_def.database.clone(),
                table_name: rel_def.table.clone(),
                column_names: property_mappings.values().cloned().collect(),
                from_node,
                to_node,
                from_id: rel_def.from_id.clone(),
                to_id: rel_def.to_id.clone(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings,
                view_parameters: rel_def.view_parameters.clone(),
                engine,
                use_final: Some(use_final),
                from_node_properties: None,
                to_node_properties: None,
            };
            relationships.insert(rel_def.type_name.clone(), rel_schema);
        }

        Ok(GraphSchema::build(
            1,
            "default".to_string(),
            nodes,
            relationships,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snake_to_camel_case() {
        assert_eq!(snake_to_camel_case("user_id"), "userId");
        assert_eq!(snake_to_camel_case("email_address"), "emailAddress");
        assert_eq!(snake_to_camel_case("first_name"), "firstName");
        assert_eq!(snake_to_camel_case("created_at"), "createdAt");
        assert_eq!(snake_to_camel_case("is_active"), "isActive");
        assert_eq!(
            snake_to_camel_case("full_name_with_title"),
            "fullNameWithTitle"
        );

        // Edge cases
        assert_eq!(snake_to_camel_case("id"), "id"); // No underscore
        assert_eq!(snake_to_camel_case("_internal"), "Internal"); // Leading underscore
        assert_eq!(snake_to_camel_case("user__id"), "userId"); // Double underscore
    }

    #[test]
    fn test_apply_naming_convention() {
        // camelCase conversion
        assert_eq!(apply_naming_convention("user_id", "camelCase"), "userId");
        assert_eq!(
            apply_naming_convention("email_address", "camelCase"),
            "emailAddress"
        );

        // snake_case (default - no conversion)
        assert_eq!(apply_naming_convention("user_id", "snake_case"), "user_id");
        assert_eq!(
            apply_naming_convention("email_address", "snake_case"),
            "email_address"
        );

        // Unknown convention (defaults to no conversion)
        assert_eq!(apply_naming_convention("user_id", "kebab-case"), "user_id");
    }
}
