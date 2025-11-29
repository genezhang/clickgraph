use super::errors::GraphSchemaError;
use super::expression_parser::{parse_property_value, PropertyValue};
use super::filter_parser::SchemaFilter;
use super::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
use super::schema_validator::SchemaValidator;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;

/// Identifier type supporting both single and composite IDs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Identifier {
    /// Single column identifier
    Single(String),
    /// Composite identifier (multiple columns)
    Composite(Vec<String>),
}

impl Identifier {
    /// Get all columns in the identifier
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Identifier::Single(col) => vec![col.as_str()],
            Identifier::Composite(cols) => cols.iter().map(|s| s.as_str()).collect(),
        }
    }
    
    /// Check if this is a composite identifier
    pub fn is_composite(&self) -> bool {
        matches!(self, Identifier::Composite(_))
    }
    
    /// Get the single column name (panics if composite)
    pub fn as_single(&self) -> &str {
        match self {
            Identifier::Single(col) => col.as_str(),
            Identifier::Composite(_) => panic!("Called as_single() on composite identifier"),
        }
    }
}

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Identifier::Single(s)
    }
}

impl From<Vec<String>> for Identifier {
    fn from(v: Vec<String>) -> Self {
        Identifier::Composite(v)
    }
}
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
    
    /// Relationship definitions (legacy, deprecated)
    /// Use `edges` field instead for new schemas
    #[serde(default)]
    pub relationships: Vec<RelationshipDefinition>,
    
    /// Edge definitions (new, preferred)
    /// Supports standard and polymorphic edges with composite IDs
    #[serde(default, alias = "relationships")]
    pub edges: Vec<EdgeDefinition>,
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
    
    // ===== Denormalized node support =====
    
    /// Optional: Property mappings when this node appears as from_node in relationships
    /// Used for denormalized nodes where properties exist in edge table
    /// Example: {"code": "origin_code", "city": "origin_city"}
    #[serde(default)]
    pub from_node_properties: Option<HashMap<String, String>>,
    
    /// Optional: Property mappings when this node appears as to_node in relationships
    /// Used for denormalized nodes where properties exist in edge table
    /// Example: {"code": "dest_code", "city": "dest_city"}
    #[serde(default)]
    pub to_node_properties: Option<HashMap<String, String>>,
    
    /// Optional: SQL predicate filter applied to all queries on this node
    /// Column references are prefixed with table alias at query time
    /// Example: "is_active = 1 AND created_at >= now() - INTERVAL 30 DAY"
    #[serde(default)]
    pub filter: Option<String>,
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
    /// Optional: Composite edge ID for cycle prevention in variable-length paths
    /// Examples: 
    ///   - Single: "relationship_id" or ["relationship_id"]
    ///   - Composite: ["from_id", "to_id", "timestamp"]
    /// Default: [from_id, to_id]
    #[serde(default)]
    pub edge_id: Option<Identifier>,
    /// Optional: SQL predicate filter applied to all queries on this relationship
    /// Column references are prefixed with table alias at query time
    /// Example: "is_active = 1 AND created_at >= now() - INTERVAL 30 DAY"
    #[serde(default)]
    pub filter: Option<String>,
}

/// Edge definition - supporting both standard and polymorphic patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeDefinition {
    /// Standard edge: explicit type, known nodes at config time
    Standard(StandardEdgeDefinition),
    /// Polymorphic edge: discover types and nodes from data at runtime
    Polymorphic(PolymorphicEdgeDefinition),
}

/// Standard edge definition (explicit, one config → one edge type)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandardEdgeDefinition {
    /// Edge type name
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
    /// Source node label (known at config time)
    pub from_node: String,
    /// Target node label (known at config time)
    pub to_node: String,
    
    /// Optional: Composite edge ID
    /// Examples: 
    ///   - Single: "relationship_id" or ["relationship_id"]
    ///   - Composite: ["from_id", "to_id", "timestamp"]
    /// Default: [from_id, to_id]
    #[serde(default)]
    pub edge_id: Option<Identifier>,
    
    // NOTE: from_node_properties and to_node_properties are defined on NODE definitions,
    // not on edge definitions. The edge gets these from the node definitions based on
    // from_node and to_node labels during schema loading.
    
    /// Property mappings for edge properties
    #[serde(rename = "property_mappings", default)]
    pub properties: HashMap<String, String>,
    
    /// Optional: View parameters for parameterized views
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,
    
    /// Optional: Whether to use FINAL keyword
    #[serde(default)]
    pub use_final: Option<bool>,
    
    /// Optional: Auto-discover columns
    #[serde(default)]
    pub auto_discover_columns: bool,
    
    /// Optional: Exclude columns from auto-discovery
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    
    /// Optional: Naming convention for auto-discovered properties
    #[serde(default = "default_naming_convention")]
    pub naming_convention: String,
    
    /// Optional: SQL predicate filter applied to all queries on this edge
    /// Column references are prefixed with table alias at query time
    /// Example: "is_active = 1 AND created_at >= now() - INTERVAL 30 DAY"
    #[serde(default)]
    pub filter: Option<String>,
}

/// Polymorphic edge definition (one config → many edge types from explicit list)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymorphicEdgeDefinition {
    /// Marker field (must be true)
    pub polymorphic: bool,
    /// ClickHouse database name
    pub database: String,
    /// Source table name
    pub table: String,
    /// From ID column name
    pub from_id: String,
    /// To ID column name
    pub to_id: String,
    
    /// Column containing edge type discriminator
    /// Example: "relation_type"
    pub type_column: String,
    
    /// Column containing source node label
    /// Example: "from_type"
    pub from_label_column: String,
    
    /// Column containing target node label
    /// Example: "to_type"
    pub to_label_column: String,
    
    /// List of edge types in this table (REQUIRED for production)
    /// Example: ["FOLLOWS", "LIKES", "AUTHORED"]
    /// One EdgeSchema will be generated per type value
    /// Node types (from_node/to_node) are matched at query time via label columns
    pub type_values: Vec<String>,
    
    /// Optional: Composite edge ID
    #[serde(default)]
    pub edge_id: Option<Identifier>,
    
    /// Property mappings (shared across all discovered edge types)
    #[serde(rename = "property_mappings", default)]
    pub properties: HashMap<String, String>,
    
    /// Optional: View parameters
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,
    
    /// Optional: Whether to use FINAL keyword
    #[serde(default)]
    pub use_final: Option<bool>,
    
    /// Optional: SQL predicate filter applied to all queries on this edge
    /// Column references are prefixed with table alias at query time
    /// Example: "is_active = 1 AND created_at >= now() - INTERVAL 30 DAY"
    #[serde(default)]
    pub filter: Option<String>,
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

/// Parse property mappings from HashMap<String, String> into HashMap<String, PropertyValue>
/// Detects whether each value is a simple column name or an expression and parses accordingly
fn parse_property_mappings(
    mappings: HashMap<String, String>,
) -> Result<HashMap<String, PropertyValue>, GraphSchemaError> {
    let mut parsed = HashMap::new();
    
    for (key, value) in mappings {
        let property_value = parse_property_value(&value).map_err(|e| {
            GraphSchemaError::InvalidConfig {
                message: format!("Failed to parse property '{}': {}", key, e),
            }
        })?;
        parsed.insert(key, property_value);
    }
    
    Ok(parsed)
}

// ============================================================================
// Schema Building Helpers
// ============================================================================
// These helper types and functions consolidate the shared logic between
// `to_graph_schema()` (sync, no discovery) and `to_graph_schema_with_client()` 
// (async, with auto-discovery and engine detection).

use super::engine_detection::TableEngine;

/// Optional discovery data for a table (populated by async ClickHouse queries)
#[derive(Debug, Clone, Default)]
struct TableDiscovery {
    /// Auto-discovered columns (if auto_discover_columns is enabled)
    columns: Option<Vec<String>>,
    /// Detected table engine type
    engine: Option<TableEngine>,
}

/// Build property mappings with optional auto-discovery
fn build_property_mappings(
    manual_mappings: HashMap<String, String>,
    discovery: &TableDiscovery,
    auto_discover: bool,
    exclude_columns: &[String],
    naming_convention: &str,
) -> HashMap<String, String> {
    if !auto_discover {
        return manual_mappings;
    }
    
    let mut mappings = HashMap::new();
    
    // Apply auto-discovered columns first (if available)
    if let Some(ref columns) = discovery.columns {
        for col in columns {
            if !exclude_columns.contains(col) {
                let property_name = apply_naming_convention(col, naming_convention);
                mappings.insert(property_name, col.clone());
            }
        }
    }
    
    // Manual mappings override auto-discovered (manual wins)
    mappings.extend(manual_mappings);
    
    mappings
}

/// Determine use_final value from config and detected engine
fn determine_use_final(config_use_final: Option<bool>, engine: &Option<TableEngine>) -> Option<bool> {
    // If config has explicit value, use it
    if config_use_final.is_some() {
        return config_use_final;
    }
    // Otherwise, auto-detect from engine
    Some(engine.as_ref().map(|e| e.supports_final()).unwrap_or(false))
}

/// Build a NodeSchema from a NodeDefinition with optional discovery data
fn build_node_schema(
    node_def: &NodeDefinition,
    discovery: &TableDiscovery,
) -> Result<NodeSchema, GraphSchemaError> {
    // Build property mappings (with optional auto-discovery)
    let raw_mappings = build_property_mappings(
        node_def.properties.clone(),
        discovery,
        node_def.auto_discover_columns,
        &node_def.exclude_columns,
        &node_def.naming_convention,
    );
    
    let property_mappings = parse_property_mappings(raw_mappings)?;
    
    // Determine use_final
    let use_final = determine_use_final(node_def.use_final, &discovery.engine);
    
    // Check if this is a denormalized node
    let is_denormalized = node_def.from_node_properties.is_some() 
        || node_def.to_node_properties.is_some();
    
    // Parse filter if provided
    let filter = if let Some(filter_str) = &node_def.filter {
        Some(SchemaFilter::new(filter_str).map_err(|e| {
            GraphSchemaError::ConfigReadError {
                error: format!("Invalid filter for node '{}': {}", node_def.label, e),
            }
        })?)
    } else {
        None
    };
    
    Ok(NodeSchema {
        database: node_def.database.clone(),
        table_name: node_def.table.clone(),
        column_names: property_mappings
            .values()
            .flat_map(|pv| pv.get_columns())
            .collect(),
        primary_keys: node_def.id_column.clone(),
        node_id: NodeIdSchema {
            column: node_def.id_column.clone(),
            dtype: "UInt64".to_string(),
        },
        property_mappings,
        view_parameters: node_def.view_parameters.clone(),
        engine: discovery.engine.clone(),
        use_final,
        filter,
        is_denormalized,
        from_properties: node_def.from_node_properties.clone(),
        to_properties: node_def.to_node_properties.clone(),
        denormalized_source_table: if is_denormalized {
            Some(format!("{}.{}", node_def.database, node_def.table))
        } else {
            None
        },
    })
}

/// Build a RelationshipSchema from a legacy RelationshipDefinition
fn build_relationship_schema(
    rel_def: &RelationshipDefinition,
    default_node_type: &str,
    discovery: &TableDiscovery,
) -> Result<RelationshipSchema, GraphSchemaError> {
    // Build property mappings (with optional auto-discovery)
    let raw_mappings = build_property_mappings(
        rel_def.properties.clone(),
        discovery,
        rel_def.auto_discover_columns,
        &rel_def.exclude_columns,
        &rel_def.naming_convention,
    );
    
    let property_mappings = parse_property_mappings(raw_mappings)?;
    
    // Determine use_final
    let use_final = determine_use_final(rel_def.use_final, &discovery.engine);
    
    let from_node = rel_def
        .from_node
        .as_ref()
        .unwrap_or(&default_node_type.to_string())
        .clone();
    let to_node = rel_def
        .to_node
        .as_ref()
        .unwrap_or(&default_node_type.to_string())
        .clone();
    
    // Parse filter if provided
    let filter = if let Some(filter_str) = &rel_def.filter {
        Some(SchemaFilter::new(filter_str).map_err(|e| {
            GraphSchemaError::ConfigReadError {
                error: format!("Invalid filter for relationship '{}': {}", rel_def.type_name, e),
            }
        })?)
    } else {
        None
    };
    
    Ok(RelationshipSchema {
        database: rel_def.database.clone(),
        table_name: rel_def.table.clone(),
        column_names: property_mappings
            .values()
            .flat_map(|pv| pv.get_columns())
            .collect(),
        from_node,
        to_node,
        from_id: rel_def.from_id.clone(),
        to_id: rel_def.to_id.clone(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings,
        view_parameters: rel_def.view_parameters.clone(),
        engine: discovery.engine.clone(),
        use_final,
        filter,
        edge_id: rel_def.edge_id.clone(),
        type_column: None,
        from_label_column: None,
        to_label_column: None,
        from_node_properties: None,
        to_node_properties: None,
    })
}

/// Build a RelationshipSchema from a StandardEdgeDefinition
fn build_standard_edge_schema(
    std_edge: &StandardEdgeDefinition,
    nodes: &HashMap<String, NodeSchema>,
    discovery: &TableDiscovery,
) -> Result<RelationshipSchema, GraphSchemaError> {
    // Build property mappings (with optional auto-discovery)
    let raw_mappings = build_property_mappings(
        std_edge.properties.clone(),
        discovery,
        std_edge.auto_discover_columns,
        &std_edge.exclude_columns,
        &std_edge.naming_convention,
    );
    
    let property_mappings = parse_property_mappings(raw_mappings)?;
    
    // Determine use_final
    let use_final = determine_use_final(std_edge.use_final, &discovery.engine);
    
    // Parse filter if provided
    let filter = if let Some(filter_str) = &std_edge.filter {
        Some(SchemaFilter::new(filter_str).map_err(|e| {
            GraphSchemaError::ConfigReadError {
                error: format!("Invalid filter for edge '{}': {}", std_edge.type_name, e),
            }
        })?)
    } else {
        None
    };
    
    // Look up denormalized node properties from NODE definitions (not edge)
    let from_node_props = nodes.get(&std_edge.from_node)
        .and_then(|n| n.from_properties.clone());
    let to_node_props = nodes.get(&std_edge.to_node)
        .and_then(|n| n.to_properties.clone());
    
    Ok(RelationshipSchema {
        database: std_edge.database.clone(),
        table_name: std_edge.table.clone(),
        column_names: property_mappings
            .values()
            .flat_map(|pv| pv.get_columns())
            .collect(),
        from_node: std_edge.from_node.clone(),
        to_node: std_edge.to_node.clone(),
        from_id: std_edge.from_id.clone(),
        to_id: std_edge.to_id.clone(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings,
        view_parameters: std_edge.view_parameters.clone(),
        engine: discovery.engine.clone(),
        use_final,
        filter,
        edge_id: std_edge.edge_id.clone(),
        type_column: None,
        from_label_column: None,
        to_label_column: None,
        from_node_properties: from_node_props,
        to_node_properties: to_node_props,
    })
}

/// Build RelationshipSchemas from a PolymorphicEdgeDefinition (one per type_value)
fn build_polymorphic_edge_schemas(
    poly_edge: &PolymorphicEdgeDefinition,
    discovery: &TableDiscovery,
) -> Result<Vec<(String, RelationshipSchema)>, GraphSchemaError> {
    let property_mappings = parse_property_mappings(poly_edge.properties.clone())?;
    
    // Determine use_final
    let use_final = determine_use_final(poly_edge.use_final, &discovery.engine);
    
    // Parse filter if provided
    let filter = if let Some(filter_str) = &poly_edge.filter {
        Some(SchemaFilter::new(filter_str).map_err(|e| {
            GraphSchemaError::ConfigReadError {
                error: format!("Invalid filter for polymorphic edge: {}", e),
            }
        })?)
    } else {
        None
    };
    
    let mut results = Vec::new();
    
    for type_val in &poly_edge.type_values {
        let rel_schema = RelationshipSchema {
            database: poly_edge.database.clone(),
            table_name: poly_edge.table.clone(),
            column_names: property_mappings
                .values()
                .flat_map(|pv| pv.get_columns())
                .collect(),
            // Node types are NOT known at schema load time
            // They will be matched at query time via from_label_column/to_label_column
            from_node: "$any".to_string(),
            to_node: "$any".to_string(),
            from_id: poly_edge.from_id.clone(),
            to_id: poly_edge.to_id.clone(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: property_mappings.clone(),
            view_parameters: poly_edge.view_parameters.clone(),
            engine: discovery.engine.clone(),
            use_final,
            filter: filter.clone(),
            edge_id: poly_edge.edge_id.clone(),
            type_column: Some(poly_edge.type_column.clone()),
            from_label_column: Some(poly_edge.from_label_column.clone()),
            to_label_column: Some(poly_edge.to_label_column.clone()),
            from_node_properties: None,
            to_node_properties: None,
        };
        results.push((type_val.clone(), rel_schema));
    }
    
    Ok(results)
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

        // Check for duplicate edge types (new format)
        for edge in &self.graph_schema.edges {
            let type_name = match edge {
                EdgeDefinition::Standard(std_edge) => &std_edge.type_name,
                EdgeDefinition::Polymorphic(_) => {
                    // Polymorphic edges generate multiple types at runtime
                    // Skip duplicate checking here
                    continue;
                }
            };
            
            if !seen_types.insert(type_name) {
                return Err(GraphSchemaError::InvalidConfig {
                    message: format!("Duplicate edge type: {}", type_name),
                });
            }
        }

        // Validate denormalized nodes (node.table == edge.table)
        self.validate_denormalized_nodes()?;

        // Validate polymorphic edges
        self.validate_polymorphic_edges()?;

        Ok(())
    }

    /// Validate denormalized node configurations
    fn validate_denormalized_nodes(&self) -> Result<(), GraphSchemaError> {
        // Build a map of (database, table) -> node definition for quick lookup
        let mut node_by_table: HashMap<(String, String), &NodeDefinition> = HashMap::new();
        for node in &self.graph_schema.nodes {
            let key = (node.database.clone(), node.table.clone());
            node_by_table.insert(key, node);
        }

        // Check standard edges - verify that denormalized nodes have properties defined on NODE
        for edge in &self.graph_schema.edges {
            if let EdgeDefinition::Standard(std_edge) = edge {
                let edge_table_key = (std_edge.database.clone(), std_edge.table.clone());

                // Check if from_node shares the same table (denormalized)
                if let Some(from_node_def) = node_by_table.get(&edge_table_key) {
                    if from_node_def.label == std_edge.from_node {
                        // Denormalized! Node definition must have from_node_properties
                        if from_node_def.from_node_properties.is_none() || 
                           from_node_def.from_node_properties.as_ref().unwrap().is_empty() {
                            return Err(GraphSchemaError::InvalidConfig {
                                message: format!(
                                    "Node '{}' is denormalized in edge '{}' (shares table '{}') but missing from_node_properties on node definition",
                                    std_edge.from_node, std_edge.type_name, std_edge.table
                                ),
                            });
                        }
                    }
                }

                // Check if to_node shares the same table (denormalized)
                if let Some(to_node_def) = node_by_table.get(&edge_table_key) {
                    if to_node_def.label == std_edge.to_node {
                        // Denormalized! Node definition must have to_node_properties
                        if to_node_def.to_node_properties.is_none() || 
                           to_node_def.to_node_properties.as_ref().unwrap().is_empty() {
                            return Err(GraphSchemaError::InvalidConfig {
                                message: format!(
                                    "Node '{}' is denormalized in edge '{}' (shares table '{}') but missing to_node_properties on node definition",
                                    std_edge.to_node, std_edge.type_name, std_edge.table
                                ),
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate polymorphic edge configurations
    fn validate_polymorphic_edges(&self) -> Result<(), GraphSchemaError> {
        for edge in &self.graph_schema.edges {
            if let EdgeDefinition::Polymorphic(poly_edge) = edge {
                // Check required discovery columns
                if poly_edge.type_column.is_empty() {
                    return Err(GraphSchemaError::InvalidConfig {
                        message: "Polymorphic edge requires non-empty type_column".to_string(),
                    });
                }
                if poly_edge.from_label_column.is_empty() {
                    return Err(GraphSchemaError::InvalidConfig {
                        message: "Polymorphic edge requires non-empty from_label_column".to_string(),
                    });
                }
                if poly_edge.to_label_column.is_empty() {
                    return Err(GraphSchemaError::InvalidConfig {
                        message: "Polymorphic edge requires non-empty to_label_column".to_string(),
                    });
                }

                // Require type_values (no auto-discovery for production)
                if poly_edge.type_values.is_empty() {
                    return Err(GraphSchemaError::InvalidConfig {
                        message: "Polymorphic edge requires non-empty type_values list (e.g., [\"FOLLOWS\", \"LIKES\"])".to_string(),
                    });
                }

                // Validate edge_id if present
                if let Some(ref edge_id) = poly_edge.edge_id {
                    if let Identifier::Composite(cols) = edge_id {
                        if cols.is_empty() {
                            return Err(GraphSchemaError::InvalidConfig {
                                message: "Composite edge_id cannot be empty array".to_string(),
                            });
                        }
                    }
                }
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

    /// Convert to GraphSchema (sync version, no auto-discovery)
    /// 
    /// This is the sync version that doesn't require a ClickHouse connection.
    /// For auto-discovery and engine detection, use `to_graph_schema_with_client()`.
    pub fn to_graph_schema(&self) -> Result<GraphSchema, GraphSchemaError> {
        self.validate()?;

        // No discovery data in sync mode
        let no_discovery = TableDiscovery::default();
        
        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Convert node definitions using shared builder
        for node_def in &self.graph_schema.nodes {
            let node_schema = build_node_schema(node_def, &no_discovery)?;
            nodes.insert(node_def.label.clone(), node_schema);
        }

        // Get default node type for legacy relationship definitions
        let default_node_type = self
            .graph_schema
            .nodes
            .first()
            .map(|n| n.label.clone())
            .unwrap_or_else(|| "Unknown".to_string());

        // Convert legacy relationship definitions using shared builder
        for rel_def in &self.graph_schema.relationships {
            let rel_schema = build_relationship_schema(rel_def, &default_node_type, &no_discovery)?;
            relationships.insert(rel_def.type_name.clone(), rel_schema);
        }

        // Convert edge definitions (new format) using shared builders
        for edge_def in &self.graph_schema.edges {
            match edge_def {
                EdgeDefinition::Standard(std_edge) => {
                    let rel_schema = build_standard_edge_schema(std_edge, &nodes, &no_discovery)?;
                    relationships.insert(std_edge.type_name.clone(), rel_schema);
                }
                EdgeDefinition::Polymorphic(poly_edge) => {
                    let poly_schemas = build_polymorphic_edge_schemas(poly_edge, &no_discovery)?;
                    for (type_name, rel_schema) in poly_schemas {
                        relationships.insert(type_name, rel_schema);
                    }
                }
            }
        }

        Ok(GraphSchema::build(
            1,
            "default".to_string(),
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
    /// Uses the same helper functions as `to_graph_schema()` but with populated
    /// `TableDiscovery` data from ClickHouse metadata queries.
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
            // Gather discovery data
            let columns = if node_def.auto_discover_columns {
                Some(
                    query_table_columns(client, &node_def.database, &node_def.table)
                        .await
                        .map_err(|e| GraphSchemaError::ConfigReadError {
                            error: format!("Failed to query columns for node '{}': {}", node_def.label, e),
                        })?
                )
            } else {
                None
            };
            
            let engine = detect_table_engine(client, &node_def.database, &node_def.table)
                .await
                .ok();
            
            let discovery = TableDiscovery { columns, engine };
            
            let node_schema = build_node_schema(node_def, &discovery)?;
            nodes.insert(node_def.label.clone(), node_schema);
        }

        // Get default node type for legacy relationship definitions
        let default_node_type = self
            .graph_schema
            .nodes
            .first()
            .map(|n| n.label.clone())
            .unwrap_or_else(|| "Unknown".to_string());

        // Convert relationship definitions with auto-discovery
        for rel_def in &self.graph_schema.relationships {
            let columns = if rel_def.auto_discover_columns {
                Some(
                    query_table_columns(client, &rel_def.database, &rel_def.table)
                        .await
                        .map_err(|e| GraphSchemaError::ConfigReadError {
                            error: format!("Failed to query columns for relationship '{}': {}", rel_def.type_name, e),
                        })?
                )
            } else {
                None
            };
            
            let engine = detect_table_engine(client, &rel_def.database, &rel_def.table)
                .await
                .ok();
            
            let discovery = TableDiscovery { columns, engine };
            
            let rel_schema = build_relationship_schema(rel_def, &default_node_type, &discovery)?;
            relationships.insert(rel_def.type_name.clone(), rel_schema);
        }

        // Convert edge definitions (new format) with auto-discovery
        for edge_def in &self.graph_schema.edges {
            match edge_def {
                EdgeDefinition::Standard(std_edge) => {
                    let columns = if std_edge.auto_discover_columns {
                        Some(
                            query_table_columns(client, &std_edge.database, &std_edge.table)
                                .await
                                .map_err(|e| GraphSchemaError::ConfigReadError {
                                    error: format!("Failed to query columns for edge '{}': {}", std_edge.type_name, e),
                                })?
                        )
                    } else {
                        None
                    };
                    
                    let engine = detect_table_engine(client, &std_edge.database, &std_edge.table)
                        .await
                        .ok();
                    
                    let discovery = TableDiscovery { columns, engine };
                    
                    let rel_schema = build_standard_edge_schema(std_edge, &nodes, &discovery)?;
                    relationships.insert(std_edge.type_name.clone(), rel_schema);
                }
                EdgeDefinition::Polymorphic(poly_edge) => {
                    // Polymorphic edges don't support auto_discover_columns,
                    // but we still detect the engine
                    let engine = detect_table_engine(client, &poly_edge.database, &poly_edge.table)
                        .await
                        .ok();
                    
                    let discovery = TableDiscovery { columns: None, engine };
                    
                    for (type_name, rel_schema) in build_polymorphic_edge_schemas(poly_edge, &discovery)? {
                        relationships.insert(type_name, rel_schema);
                    }
                }
            }
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
    fn test_relationship_definition_edge_id_parsing() {
        // Test that edge_id is correctly parsed from YAML relationship definition
        let yaml = r#"
name: test_edge_id
graph_schema:
  nodes:
    - label: Airport
      database: test
      table: flights
      id_column: code
      property_mappings: {}
  relationships:
    - type: FLIGHT
      database: test
      table: flights
      from_id: Origin
      to_id: Dest
      from_node: Airport
      to_node: Airport
      edge_id: [flight_id, flight_number]
      property_mappings:
        carrier: airline
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        
        // Check that RelationshipDefinition has the edge_id
        assert_eq!(config.graph_schema.relationships.len(), 1);
        let rel = &config.graph_schema.relationships[0];
        assert!(rel.edge_id.is_some(), "edge_id should be parsed from YAML");
        
        let edge_id = rel.edge_id.as_ref().unwrap();
        assert!(edge_id.is_composite(), "edge_id should be composite");
        assert_eq!(edge_id.columns(), vec!["flight_id", "flight_number"]);
    }

    #[test]
    fn test_relationship_definition_edge_id_in_graph_schema() {
        // Test that edge_id is preserved when converting to GraphSchema
        let yaml = r#"
name: test_edge_id
graph_schema:
  nodes:
    - label: Airport
      database: test
      table: flights
      id_column: code
      property_mappings: {}
  relationships:
    - type: FLIGHT
      database: test
      table: flights
      from_id: Origin
      to_id: Dest
      from_node: Airport
      to_node: Airport
      edge_id: [flight_id, flight_number]
      property_mappings:
        carrier: airline
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        
        // Convert to GraphSchema
        let graph_schema = config.to_graph_schema().expect("Failed to convert to GraphSchema");
        
        // Check that RelationshipSchema has the edge_id
        let rel_schema = graph_schema.get_rel_schema("FLIGHT").expect("Failed to get rel schema");
        assert!(rel_schema.edge_id.is_some(), "RelationshipSchema should have edge_id");
        
        let edge_id = rel_schema.edge_id.as_ref().unwrap();
        assert!(edge_id.is_composite(), "edge_id should be composite");
        assert_eq!(edge_id.columns(), vec!["flight_id", "flight_number"]);
    }

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

    #[test]
    fn test_denormalized_schema_validation_success() {
        // Valid denormalized schema (OnTime-style)
        // Node properties are defined on the NODE, not the edge
        let config = GraphSchemaConfig {
            name: Some("ontime".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "Airport".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    id_column: "airport_code".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                    // Denormalized node properties defined HERE (on node, not edge)
                    from_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "OriginCityName".to_string());
                        props.insert("state".to_string(), "OriginState".to_string());
                        props
                    }),
                    to_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "DestCityName".to_string());
                        props.insert("state".to_string(), "DestState".to_string());
                        props
                    }),
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Standard(StandardEdgeDefinition {
                    type_name: "FLIGHT".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    from_id: "Origin".to_string(),
                    to_id: "Dest".to_string(),
                    from_node: "Airport".to_string(),
                    to_node: "Airport".to_string(),
                    edge_id: Some(Identifier::Composite(vec![
                        "FlightDate".to_string(),
                        "FlightNum".to_string(),
                        "Origin".to_string(),
                        "Dest".to_string(),
                    ])),
                    // No from_node_properties/to_node_properties on edge - they come from node
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                })],
            },
        };

        // Should pass validation
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_denormalized_schema_validation_missing_from_properties() {
        // Invalid: denormalized node but missing from_node_properties on NODE definition
        let config = GraphSchemaConfig {
            name: Some("ontime_invalid".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "Airport".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    id_column: "airport_code".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                    from_node_properties: None,  // Missing! Node is used as from_node in edge
                    to_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "DestCityName".to_string());
                        props
                    }),
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Standard(StandardEdgeDefinition {
                    type_name: "FLIGHT".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    from_id: "Origin".to_string(),
                    to_id: "Dest".to_string(),
                    from_node: "Airport".to_string(),
                    to_node: "Airport".to_string(),
                    edge_id: None,
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                })],
            },
        };

        // Should fail validation
        let result = config.validate();
        assert!(result.is_err());
        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("missing from_node_properties"));
        }
    }

    #[test]
    fn test_polymorphic_schema_validation_success() {
        let config = GraphSchemaConfig {
            name: Some("social_poly".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "User".to_string(),
                    database: "brahmand".to_string(),
                    table: "users".to_string(),
                    id_column: "user_id".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
            filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                    from_node_properties: None,
                    to_node_properties: None,
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Polymorphic(PolymorphicEdgeDefinition {
                    polymorphic: true,
                    database: "brahmand".to_string(),
                    table: "interactions".to_string(),
                    from_id: "from_id".to_string(),
                    to_id: "to_id".to_string(),
                    type_column: "interaction_type".to_string(),
                    from_label_column: "from_type".to_string(),
                    to_label_column: "to_type".to_string(),
                    type_values: vec!["FOLLOWS".to_string(), "LIKES".to_string()],  // Required!
                    edge_id: Some(Identifier::Composite(vec![
                        "from_id".to_string(),
                        "to_id".to_string(),
                        "interaction_type".to_string(),
                        "timestamp".to_string(),
                    ])),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
            filter: None,
                })],
            },
        };

        // Should pass validation
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_polymorphic_schema_validation_missing_type_values() {
        let config = GraphSchemaConfig {
            name: Some("social_invalid".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "User".to_string(),
                    database: "brahmand".to_string(),
                    table: "users".to_string(),
                    id_column: "user_id".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
            filter: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                    from_node_properties: None,
                    to_node_properties: None,
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Polymorphic(PolymorphicEdgeDefinition {
                    polymorphic: true,
                    database: "brahmand".to_string(),
                    table: "interactions".to_string(),
                    from_id: "from_id".to_string(),
                    to_id: "to_id".to_string(),
                    type_column: "interaction_type".to_string(),
                    from_label_column: "from_type".to_string(),
                    to_label_column: "to_type".to_string(),
                    type_values: vec![],  // Empty!
                    edge_id: None,
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
            filter: None,
                })],
            },
        };

        // Should fail validation
        let result = config.validate();
        assert!(result.is_err());
        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("type_values"));
        }
    }

    #[test]
    fn test_composite_identifier() {
        let single = Identifier::Single("id".to_string());
        assert!(!single.is_composite());
        assert_eq!(single.columns(), vec!["id"]);

        let composite = Identifier::Composite(vec![
            "col1".to_string(),
            "col2".to_string(),
            "col3".to_string(),
        ]);
        assert!(composite.is_composite());
        assert_eq!(composite.columns(), vec!["col1", "col2", "col3"]);
    }
}

#[cfg(test)]
mod zeek_tests {
    use super::*;

    #[test]
    fn test_zeek_schema_parsing() {
        let yaml = std::fs::read_to_string("schemas/examples/zeek_dns_log.yaml")
            .expect("Failed to read zeek schema");
        
        let config = GraphSchemaConfig::from_yaml_str(&yaml)
            .expect("Failed to parse YAML");
        
        println!("Schema name: {:?}", config.name);
        println!("Nodes count: {}", config.graph_schema.nodes.len());
        println!("Edges count: {}", config.graph_schema.edges.len());
        println!("Relationships count: {}", config.graph_schema.relationships.len());
        
        // Convert to GraphSchema
        let schema = config.to_graph_schema().expect("Failed to convert to GraphSchema");
        
        println!("GraphSchema relationships: {}", schema.get_relationships_schemas().len());
        for (name, _rel) in schema.get_relationships_schemas() {
            println!("  - Relationship: {}", name);
        }
        
        assert!(schema.get_relationships_schemas().len() > 0, "Should have relationships!");
    }
}
