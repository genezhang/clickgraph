//! Graph schema definitions and operations
//!
//! Some methods are reserved for future online validation and runtime checks.
//!
//! Note: The following functions are intentionally unused (reserved for future):
//! - `validate_table_exists`, `validate_column_exists`, `get_column_type`: For runtime schema validation
//! - `is_relationship_compatible`, `is_node_type_compatible`: For polymorphic node type matching
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::config::Identifier;
use super::engine_detection::TableEngine;
use super::errors::GraphSchemaError;
use super::expression_parser::PropertyValue;
use super::filter_parser::SchemaFilter;
use super::schema_types::SchemaType;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub primary_keys: String,
    pub node_id: NodeIdSchema,
    pub property_mappings: HashMap<String, PropertyValue>,
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

    /// Optional: SQL predicate filter applied to all queries on this node
    /// Column references are prefixed with table alias at query time
    #[serde(skip)]
    pub filter: Option<SchemaFilter>,

    // ===== Denormalized node support =====
    /// If true, this node is denormalized on one or more edge tables
    /// (no physical node table exists)
    #[serde(skip)]
    pub is_denormalized: bool,

    /// Property mappings when this node appears as from_node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": "origin_code", "city": "origin_city"}
    #[serde(skip)]
    pub from_properties: Option<HashMap<String, String>>,

    /// Property mappings when this node appears as to_node in a relationship
    /// Only used for denormalized nodes
    /// Example: {"code": "dest_code", "city": "dest_city"}
    #[serde(skip)]
    pub to_properties: Option<HashMap<String, String>>,

    /// The edge table(s) that provide denormalized properties
    /// Only used for denormalized nodes
    /// Example: Some("flights")
    #[serde(skip)]
    pub denormalized_source_table: Option<String>,

    // ===== Polymorphic table support =====
    /// Optional: Column containing node type discriminator (for shared tables)
    /// Used when multiple node labels share the same table
    /// Example: "type" column distinguishes Post vs Comment in Message table
    #[serde(skip)]
    pub label_column: Option<String>,

    /// Optional: Value in label_column that identifies this node type
    /// Required when label_column is specified
    /// Example: "Comment" for Comment nodes in Message table where type='Comment'
    #[serde(skip)]
    pub label_value: Option<String>,

    // ===== Neo4j elementId support =====
    /// Types for node_id columns (for performant elementId queries)
    /// Populated from:
    /// 1. Auto-detection (querying ClickHouse system.columns) - preferred
    /// 2. Schema YAML (type/types field) - for sql_only mode
    /// Required for Neo4j compatibility (elementId function support)
    #[serde(skip)]
    pub node_id_types: Option<Vec<SchemaType>>,
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

    /// Check if this node type has a given Cypher property name.
    /// Checks property_mappings (standard), from_properties, and to_properties (denormalized).
    pub fn has_cypher_property(&self, cypher_prop: &str) -> bool {
        if self.property_mappings.contains_key(cypher_prop) {
            return true;
        }
        if let Some(ref from_props) = self.from_properties {
            if from_props.contains_key(cypher_prop) {
                return true;
            }
        }
        if let Some(ref to_props) = self.to_properties {
            if to_props.contains_key(cypher_prop) {
                return true;
            }
        }
        false
    }

    /// Check if this engine supports FINAL (regardless of whether we use it by default)
    pub fn can_use_final(&self) -> bool {
        if let Some(ref engine) = self.engine {
            engine.supports_final()
        } else {
            false
        }
    }

    /// Helper for tests: Create a NodeSchema with default denormalized fields (traditional node pattern)
    #[cfg(test)]
    pub fn new_traditional(
        database: String,
        table_name: String,
        column_names: Vec<String>,
        primary_keys: String,
        node_id: NodeIdSchema,
        property_mappings: HashMap<String, PropertyValue>,
        view_parameters: Option<Vec<String>>,
        engine: Option<TableEngine>,
        use_final: Option<bool>,
    ) -> Self {
        NodeSchema {
            database,
            table_name,
            column_names,
            primary_keys,
            node_id,
            property_mappings,
            view_parameters,
            engine,
            use_final,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub database: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    /// 🟢 GRAPH SPACE: Node label for source node (e.g., "User")
    /// Used for query planning, pattern matching, and property resolution
    pub from_node: String,
    /// 🟢 GRAPH SPACE: Node label for target node (e.g., "Product")
    /// Used for query planning, pattern matching, and property resolution
    pub to_node: String,
    /// 🔵 RELATIONAL SPACE: Table name for source node (e.g., "users_bench")
    /// Used for SQL generation and JOIN construction
    pub from_node_table: String,
    /// 🔵 RELATIONAL SPACE: Table name for target node (e.g., "products_bench")
    /// Used for SQL generation and JOIN construction
    pub to_node_table: String,
    pub from_id: Identifier, // FK column(s) for source node ID (e.g., "user1_id" or ["bank_id", "account_number"])
    pub to_id: Identifier, // FK column(s) for target node ID (e.g., "user2_id" or ["to_bank_id", "to_account_number"])
    pub from_node_id_dtype: SchemaType,
    pub to_node_id_dtype: SchemaType,
    pub property_mappings: HashMap<String, PropertyValue>,
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

    /// Optional: SQL predicate filter applied to all queries on this relationship
    /// Column references are prefixed with table alias at query time
    #[serde(skip)]
    pub filter: Option<SchemaFilter>,

    // ===== New fields for enhanced schema support =====
    /// Optional: Composite edge ID (for uniqueness filters)
    /// If None, defaults to [from_id, to_id]
    /// Example: Some(Composite(["FlightDate", "FlightNum", "Origin", "Dest"]))
    #[serde(skip)]
    pub edge_id: Option<Identifier>,

    /// Optional: Polymorphic edge discriminator columns
    /// Used to filter rows by edge type and node types at query time
    #[serde(skip)]
    pub type_column: Option<String>,
    #[serde(skip)]
    pub from_label_column: Option<String>,
    #[serde(skip)]
    pub to_label_column: Option<String>,

    /// Optional: Valid labels for polymorphic from side (closed-world validation)
    /// When specified, only these labels are allowed for the source node
    /// Example: Some(["User", "Group"]) for MEMBER_OF edge
    #[serde(skip)]
    pub from_label_values: Option<Vec<String>>,

    /// Optional: Valid labels for polymorphic to side (closed-world validation)
    /// When specified, only these labels are allowed for the target node
    /// Example: Some(["Folder", "File"]) for CONTAINS edge
    #[serde(skip)]
    pub to_label_values: Option<Vec<String>>,

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

    /// If true, this is an FK-edge pattern where the edge is represented by a
    /// foreign key column on one of the node tables (no separate edge table).
    ///
    /// Two variants:
    /// 1. Self-referencing: edge table = from_node table = to_node table
    ///    Example: fs_objects.parent_id → fs_objects.object_id
    ///    Query: (child:Object)-[:PARENT]->(parent:Object)
    ///    SQL:   child.parent_id = parent.object_id
    ///
    /// 2. Non-self-referencing: edge table = from_node table ≠ to_node table
    ///    Example: orders.customer_id → customers.customer_id
    ///    Query: (o:Order)-[:PLACED_BY]->(c:Customer)
    ///    SQL:   o.customer_id = c.customer_id
    ///
    /// Detection: edge table matches from_node OR to_node table, with no
    /// denormalized node properties (from_node_properties/to_node_properties).
    #[serde(skip)]
    pub is_fk_edge: bool,

    /// Optional: Edge traversal constraint expression
    /// SQL expression with 'from.property' and 'to.property' references
    /// Compiled to additional JOIN ON condition beyond ID equality
    /// Use AND/OR for composite logic
    ///
    /// Examples:
    ///   - "from.timestamp < to.timestamp"  (chronological ordering)
    ///   - "from.app_id = to.app_id"  (context preservation)
    ///   - "from.timestamp + 300 > to.timestamp AND from.country = to.country"
    ///
    /// Applied as:
    /// - Single-hop: Additional condition in JOIN ON clause
    /// - VLP: Additional condition in recursive CTE JOIN
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraints: Option<String>,

    // ===== Neo4j elementId support =====
    /// Types for edge_id columns (for performant elementId queries)
    /// Populated from:
    /// 1. Auto-detection (querying ClickHouse system.columns)
    /// 2. Schema YAML (type/types field)
    /// Required for Neo4j compatibility (elementId function support for relationships)
    #[serde(skip)]
    pub edge_id_types: Option<Vec<SchemaType>>,
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

    /// Get the fully qualified table name (database.table)
    pub fn full_table_name(&self) -> String {
        format!("{}.{}", self.database, self.table_name)
    }
}

impl NodeSchema {
    /// Get the fully qualified table name (database.table)
    pub fn full_table_name(&self) -> String {
        format!("{}.{}", self.database, self.table_name)
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
    Node(Box<NodeSchema>),
    Rel(Box<RelationshipSchema>),
}

/// Node identifier schema - supports both single and composite node IDs.
///
/// **Semantic Clarification (December 2025):**
/// `node_id` specifies PROPERTY NAMES (graph layer), not column names (relational layer).
/// The actual database column names are resolved through property_mappings.
///
/// For traditional nodes with own tables:
/// - `id` contains property name(s) that map to columns via property_mappings
/// - If not in property_mappings, identity mapping is auto-generated (property_name → column_name)
/// - Example: `node_id: user_id` → auto-generates `property_mappings: {user_id: user_id}`
///
/// For denormalized nodes (virtual nodes on edge tables):
/// - `id` contains property name(s) that get resolved via from_node_properties/to_node_properties
/// - Example: `node_id: ip` with `from_node_properties: {ip: "id.orig_h"}`
///
/// This provides backward compatibility: existing schemas work unchanged with auto-generated
/// identity mappings. This mirrors `Identifier` used for `edge_id` in relationship schemas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeIdSchema {
    /// The identifier - can be single column or composite
    pub id: Identifier,
    /// Generic data type (Integer, String, etc.) - used for validation
    pub dtype: crate::graph_catalog::schema_types::SchemaType,
}

impl NodeIdSchema {
    /// Create a new NodeIdSchema with a single column identifier
    pub fn single(column: String, dtype: crate::graph_catalog::schema_types::SchemaType) -> Self {
        NodeIdSchema {
            id: Identifier::Single(column),
            dtype,
        }
    }

    /// Create a new NodeIdSchema with a composite identifier
    pub fn composite(
        columns: Vec<String>,
        dtype: crate::graph_catalog::schema_types::SchemaType,
    ) -> Self {
        NodeIdSchema {
            id: Identifier::Composite(columns),
            dtype,
        }
    }

    /// Get the column name for single-column identifiers.
    ///
    /// **Panics** if called on composite identifier.
    /// For composite-safe access, use `columns()` or `sql_tuple()`.
    pub fn column(&self) -> &str {
        match self.id.as_single() {
            Ok(col) => col,
            Err(_) => panic!(
                "Attempted to access single column on composite node identifier. \
                This is a schema configuration error - composite node IDs should use \
                columns() for safe access or sql_tuple() for SQL generation."
            ),
        }
    }

    /// Get the column name for single-column identifiers, with error handling.
    ///
    /// Preferred for new code over `column()` to handle composite identifiers gracefully.
    pub fn column_or_error(&self) -> Result<&str, String> {
        self.id.as_single().map_err(|e| e.to_string())
    }

    /// Check if this is a composite identifier
    pub fn is_composite(&self) -> bool {
        self.id.is_composite()
    }

    /// Get all columns in the identifier
    pub fn columns(&self) -> Vec<&str> {
        self.id.columns()
    }

    /// Get all columns with alias prefix
    pub fn columns_with_alias(&self, alias: &str) -> Vec<String> {
        self.id
            .columns()
            .iter()
            .map(|col| format!("{}.{}", alias, col))
            .collect()
    }

    /// Generate SQL tuple expression
    /// For single column: "alias.column"
    /// For composite: "(alias.col1, alias.col2, ...)"
    pub fn sql_tuple(&self, alias: &str) -> String {
        self.id.to_sql_tuple(alias)
    }

    /// Generate SQL equality condition for JOIN
    /// For single: "left_alias.col = right_alias.col"
    /// For composite: "(left_alias.c1, left_alias.c2) = (right_alias.c1, right_alias.c2)"
    pub fn sql_equality(&self, left_alias: &str, right_alias: &str) -> String {
        let left = self.sql_tuple(left_alias);
        let right = self.sql_tuple(right_alias);
        format!("{} = {}", left, right)
    }
}

/// Metadata for denormalized node property access
/// Pre-computed at schema load time for efficient query generation
#[derive(Debug, Clone)]
pub struct PropertySource {
    /// Relationship type that contains this property (e.g., "FLIGHT")
    pub relationship_type: String,
    /// Which side: "from" or "to"
    pub side: String,
    /// Table column name
    pub column_name: String,
}

/// Pre-computed metadata for a denormalized node label
/// Stores all possible ways to access properties for this node
#[derive(Debug, Clone)]
pub struct ProcessedNodeMetadata {
    /// Node label (e.g., "Airport")
    pub label: String,

    /// Property sources: property_name -> list of sources
    /// Each property may be available in multiple edge tables
    /// Example: "city" -> [PropertySource{rel="FLIGHT", side="from", col="OriginCityName"},
    ///                     PropertySource{rel="FLIGHT", side="to", col="DestCityName"}]
    pub property_sources: HashMap<String, Vec<PropertySource>>,

    /// ID sources: which edges can provide the node ID
    /// Maps relationship_type -> (side, id_column)
    /// Example: "FLIGHT" -> ("from", "Origin"), ("to", "Dest")
    pub id_sources: HashMap<String, Vec<(String, String)>>,
}

impl ProcessedNodeMetadata {
    /// Create new metadata for a node label
    pub fn new(label: String) -> Self {
        ProcessedNodeMetadata {
            label,
            property_sources: HashMap::new(),
            id_sources: HashMap::new(),
        }
    }

    /// Add a property source
    pub fn add_property_source(&mut self, property: String, source: PropertySource) {
        self.property_sources
            .entry(property)
            .or_default()
            .push(source);
    }

    /// Add an ID source
    pub fn add_id_source(&mut self, rel_type: String, side: String, id_column: String) {
        self.id_sources
            .entry(rel_type)
            .or_default()
            .push((side, id_column));
    }

    /// Get property sources for a given property name
    pub fn get_property_sources(&self, property: &str) -> Option<&Vec<PropertySource>> {
        self.property_sources.get(property)
    }

    /// Get all relationship types that have this node
    pub fn get_relationship_types(&self) -> Vec<String> {
        self.id_sources.keys().cloned().collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphSchema {
    version: u32,
    database: String,
    nodes: HashMap<String, NodeSchema>,
    relationships: HashMap<String, RelationshipSchema>,

    /// Denormalized node metadata (computed at schema load)
    /// Maps node label -> metadata
    #[serde(skip)]
    denormalized_nodes: HashMap<String, ProcessedNodeMetadata>,

    /// Secondary index: relationship type name -> composite keys
    /// Enables O(1) lookup of all relationships by type without scanning
    /// Example: "HAS_TAG" -> ["HAS_TAG::Post::Tag", "HAS_TAG::Comment::Tag", "HAS_TAG::Message::Tag"]
    #[serde(skip)]
    rel_type_index: HashMap<String, Vec<String>>,
}

impl GraphSchema {
    /// Create a composite key for a relationship: "type::from_node::to_node"
    /// This allows multiple relationships with the same type but different node combinations
    pub fn make_rel_composite_key(type_name: &str, from_node: &str, to_node: &str) -> String {
        format!("{}::{}::{}", type_name, from_node, to_node)
    }

    pub fn build(
        version: u32,
        database: String,
        nodes: HashMap<String, NodeSchema>,
        relationships: HashMap<String, RelationshipSchema>,
    ) -> GraphSchema {
        // Build denormalized node metadata
        let denormalized_nodes = Self::build_denormalized_metadata(&relationships);

        // Build secondary index for fast relationship type lookup
        let rel_type_index = Self::build_rel_type_index(&relationships);

        GraphSchema {
            version,
            database,
            nodes,
            relationships,
            denormalized_nodes,
            rel_type_index,
        }
    }

    /// Build secondary index: relationship type -> list of composite keys
    /// Enables O(1) lookup by type name without iterating through all relationships
    ///
    /// NOTE: When both simple key ("TYPE") and composite key ("TYPE::FROM::TO") exist
    /// for the same relationship, we only include the composite key. This is because
    /// the config.rs inserts both for backward compatibility, but we only want to
    /// resolve to ONE actual table.
    fn build_rel_type_index(
        relationships: &HashMap<String, RelationshipSchema>,
    ) -> HashMap<String, Vec<String>> {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();

        // First pass: collect all composite keys (those containing "::")
        // Skip simple keys that have a corresponding composite key
        let mut composite_types: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for composite_key in relationships.keys() {
            if composite_key.contains("::") {
                // Extract base type name from composite key
                if let Some(type_name) = composite_key.split("::").next() {
                    composite_types.insert(type_name.to_string());
                }
            }
        }

        // Second pass: build index, skipping simple keys when composite exists
        for composite_key in relationships.keys() {
            let type_name = if composite_key.contains("::") {
                // This is a composite key - extract the type name (before first ::)
                composite_key
                    .split("::")
                    .next()
                    .unwrap_or(composite_key.as_str())
            } else {
                // This is a simple key - skip if we have composite keys for this type
                if composite_types.contains(composite_key) {
                    log::debug!(
                        "Skipping simple key '{}' in favor of composite key",
                        composite_key
                    );
                    continue;
                }
                composite_key.as_str()
            };

            index
                .entry(type_name.to_string())
                .or_default()
                .push(composite_key.clone());
        }

        index
    }

    /// Build denormalized node metadata from relationships
    /// Scans all relationships to find denormalized node properties
    fn build_denormalized_metadata(
        relationships: &HashMap<String, RelationshipSchema>,
    ) -> HashMap<String, ProcessedNodeMetadata> {
        let mut metadata_map: HashMap<String, ProcessedNodeMetadata> = HashMap::new();

        // Collect all actual relationship types (extract from composite keys)
        // Composite keys are in format "TYPE::FROM::TO"
        let mut processed_rels = std::collections::HashSet::new();

        for (rel_key, rel_schema) in relationships {
            // Extract relationship type from composite key (TYPE::FROM::TO -> TYPE)
            let rel_type = if rel_key.contains("::") {
                rel_key.split("::").next().unwrap().to_string()
            } else {
                rel_key.clone()
            };

            // Process this relationship type
            if !processed_rels.insert(rel_type.clone()) {
                continue; // Already processed this type
            }

            // Process from_node denormalized properties
            if let Some(ref from_props) = rel_schema.from_node_properties {
                let from_label = &rel_schema.from_node;
                let metadata = metadata_map
                    .entry(from_label.clone())
                    .or_insert_with(|| ProcessedNodeMetadata::new(from_label.clone()));

                // Add property sources
                for (prop_name, col_name) in from_props {
                    metadata.add_property_source(
                        prop_name.clone(),
                        PropertySource {
                            relationship_type: rel_type.clone(),
                            side: "from".to_string(),
                            column_name: col_name.clone(),
                        },
                    );
                }

                // Add ID source
                metadata.add_id_source(
                    rel_type.clone(),
                    "from".to_string(),
                    rel_schema.from_id.to_string(),
                );
            }

            // Process to_node denormalized properties
            if let Some(ref to_props) = rel_schema.to_node_properties {
                let to_label = &rel_schema.to_node;
                let metadata = metadata_map
                    .entry(to_label.clone())
                    .or_insert_with(|| ProcessedNodeMetadata::new(to_label.clone()));

                // Add property sources
                for (prop_name, col_name) in to_props {
                    metadata.add_property_source(
                        prop_name.clone(),
                        PropertySource {
                            relationship_type: rel_type.clone(),
                            side: "to".to_string(),
                            column_name: col_name.clone(),
                        },
                    );
                }

                // Add ID source
                metadata.add_id_source(
                    rel_type.clone(),
                    "to".to_string(),
                    rel_schema.to_id.to_string(),
                );
            }
        }

        metadata_map
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

    pub fn node_schema(&self, node_label: &str) -> Result<&NodeSchema, GraphSchemaError> {
        log::debug!(
            "node_schema: Looking for node_label='{}' in schema (has {} nodes: {:?})",
            node_label,
            self.nodes.len(),
            self.nodes.keys().take(5).collect::<Vec<_>>()
        );

        self.nodes.get(node_label).ok_or_else(|| {
            log::warn!(
                "node_schema: Node '{}' NOT FOUND. Available nodes: {:?}",
                node_label,
                self.nodes.keys().collect::<Vec<_>>()
            );
            GraphSchemaError::Node {
                node_label: node_label.to_string(),
            }
        })
    }

    pub fn get_rel_schema(&self, rel_label: &str) -> Result<&RelationshipSchema, GraphSchemaError> {
        log::debug!(
            "⚠️ get_rel_schema (OLD METHOD) called with rel_label='{}'",
            rel_label
        );

        // First try exact match (composite key lookup)
        if let Some(schema) = self.relationships.get(rel_label) {
            log::debug!("get_rel_schema: Found exact match for '{}'", rel_label);
            return Ok(schema);
        }

        // If not found and it's a simple type name (no "::"), use rel_type_index for O(1) lookup
        if !rel_label.contains("::") {
            if let Some(composite_keys) = self.rel_type_index.get(rel_label) {
                if let Some(first_key) = composite_keys.first() {
                    if let Some(schema) = self.relationships.get(first_key) {
                        log::debug!(
                            "get_rel_schema: Found relationship '{}' for type '{}' using rel_type_index",
                            first_key, rel_label
                        );
                        return Ok(schema);
                    }
                }
            }

            // If no matches found with rel_type_index
            log::error!(
                "❌ get_rel_schema: No relationship schema found for type '{}' in rel_type_index",
                rel_label
            );
            return Err(GraphSchemaError::Relation {
                rel_label: rel_label.to_string(),
            });
        }

        // If not found, return error
        log::error!(
            "❌ get_rel_schema: No relationship schema found for '{}'",
            rel_label
        );
        Err(GraphSchemaError::Relation {
            rel_label: rel_label.to_string(),
        })
    }

    /// Get relationship schema by type and node types (supports composite keys)
    pub fn get_rel_schema_with_nodes(
        &self,
        rel_type: &str,
        from_node: Option<&str>,
        to_node: Option<&str>,
    ) -> Result<&RelationshipSchema, GraphSchemaError> {
        // If both nodes specified, try composite key first
        if let (Some(from), Some(to)) = (from_node, to_node) {
            let composite_key = Self::make_rel_composite_key(rel_type, from, to);
            log::debug!(
                "get_rel_schema_with_nodes: Looking for composite key '{}'",
                composite_key
            );
            if let Some(schema) = self.relationships.get(&composite_key) {
                log::debug!(
                    "get_rel_schema_with_nodes: Found schema for composite key '{}'",
                    composite_key
                );
                return Ok(schema);
            }
            log::debug!(
                "get_rel_schema_with_nodes: Composite key '{}' not found",
                composite_key
            );
        }

        // Use rel_type_index for O(1) lookup (replaces slower prefix search)
        // This finds composite keys matching the relationship type (e.g., "FOLLOWS::User::User")
        if let Some(composite_keys) = self.rel_type_index.get(rel_type) {
            // When partial node info is available, filter candidates by matching from/to
            if from_node.is_some() || to_node.is_some() {
                let mut matched_key = None;
                for key in composite_keys {
                    if let Some(schema) = self.relationships.get(key) {
                        let from_ok =
                            from_node.is_none() || from_node == Some(schema.from_node.as_str());
                        let to_ok = to_node.is_none() || to_node == Some(schema.to_node.as_str());
                        if from_ok && to_ok {
                            matched_key = Some(key);
                            break;
                        }
                    }
                }
                if let Some(key) = matched_key {
                    if let Some(schema) = self.relationships.get(key) {
                        log::debug!(
                            "get_rel_schema_with_nodes: Found schema for composite key '{}' filtered by from={:?} to={:?}",
                            key, from_node, to_node
                        );
                        return Ok(schema);
                    }
                }
            }
            // No partial info or no match found — fall back to first
            if let Some(key) = composite_keys.first() {
                if let Some(schema) = self.relationships.get(key) {
                    log::debug!(
                        "get_rel_schema_with_nodes: Found schema for composite key '{}' when looking for type '{}'",
                        key, rel_type
                    );
                    return Ok(schema);
                }
            }
        }

        // Fallback: Try simple key lookup (for backward compatibility with integration tests)
        if let Some(schema) = self.relationships.get(rel_type) {
            log::debug!(
                "get_rel_schema_with_nodes: Found schema for simple key '{}'",
                rel_type
            );
            return Ok(schema);
        }

        Err(GraphSchemaError::Relation {
            rel_label: rel_type.to_string(),
        })
    }

    /// Get all relationship schemas matching a type name
    /// O(1) lookup using secondary index instead of O(n) iteration
    pub fn rel_schemas_for_type(&self, rel_type: &str) -> Vec<&RelationshipSchema> {
        // Use secondary index for O(1) lookup
        if let Some(composite_keys) = self.rel_type_index.get(rel_type) {
            composite_keys
                .iter()
                .filter_map(|key| self.relationships.get(key))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Find all relationship types that match a generic pattern, filtering by node type compatibility.
    ///
    /// This performs **semantic expansion** based on node types, not just pattern matching.
    ///
    /// For example:
    /// - Query: `(m:Message)-[:HAS_TAG]->(t:Tag)`
    /// - Message is polymorphic: [Post, Comment]
    /// - Result: Only [POST_HAS_TAG, COMMENT_HAS_TAG], NOT FORUM_HAS_TAG
    ///
    /// Strategy:
    /// 1. Try exact match first (if relationship exists, return it)
    /// 2. Pattern match to find candidates (e.g., *_HAS_TAG)
    /// 3. Filter candidates by from_node/to_node compatibility with provided node labels
    ///
    /// Parameters:
    /// - generic_name: The relationship type name (e.g., "HAS_TAG")
    /// - from_label: Optional source node label (e.g., "Message", "University")
    /// - to_label: Optional target node label (e.g., "Tag", "City")
    ///
    /// Returns empty vec if no matches found.
    pub fn expand_generic_relationship_type(
        &self,
        generic_name: &str,
        from_label: Option<&str>,
        to_label: Option<&str>,
    ) -> Vec<String> {
        // Use composite key index for direct O(1) lookup by type name
        // No more pattern matching - rely on proper schema definition

        // Get all composite keys for this relationship type from the index
        let composite_keys = match self.rel_type_index.get(generic_name) {
            Some(keys) => keys.clone(),
            None => return Vec::new(), // Type not found in schema
        };

        // If no node labels provided, return all matches
        if from_label.is_none() && to_label.is_none() {
            if !composite_keys.is_empty() {
                log::debug!(
                    "Found {} relationship(s) with type '{}': {:?}",
                    composite_keys.len(),
                    generic_name,
                    composite_keys
                );
            }
            return composite_keys;
        }

        // Filter by node label compatibility — pure graph-space matching.
        // No implicit subtype/inheritance: labels must match exactly,
        // consistent with Neo4j where labels are flat tags.
        let mut compatible: Vec<String> = Vec::new();
        for composite_key in &composite_keys {
            if let Some(schema) = self.relationships.get(composite_key) {
                let from_ok = from_label.is_none_or(|f| schema.from_node == f);
                let to_ok = to_label.is_none_or(|t| schema.to_node == t);
                if from_ok && to_ok {
                    compatible.push(composite_key.clone());
                }
            }
        }

        if !compatible.is_empty() {
            log::debug!(
                "Found {} compatible '{}' relationship(s) for ({:?})-[]->({:?}): {:?}",
                compatible.len(),
                generic_name,
                from_label,
                to_label,
                compatible
            );
        }

        compatible
    }

    /// Check if a relationship is compatible with given source/target node labels.
    ///
    /// Handles polymorphic nodes by checking if the relationship's from_node/to_node
    /// matches any concrete type that the polymorphic node can represent.
    ///
    /// Example:
    /// - Relationship: POST_HAS_TAG (Post → Tag)
    /// - Query node: Message (polymorphic: Post | Comment)
    /// - Result: COMPATIBLE (because Message can be Post)
    fn is_relationship_compatible(
        &self,
        rel_schema: &RelationshipSchema,
        from_label: Option<&str>,
        to_label: Option<&str>,
    ) -> bool {
        // Check from_node compatibility
        let from_ok = if let Some(from) = from_label {
            self.is_node_type_compatible(&rel_schema.from_node, from)
        } else {
            true // No constraint on source node
        };

        // Check to_node compatibility
        let to_ok = if let Some(to) = to_label {
            self.is_node_type_compatible(&rel_schema.to_node, to)
        } else {
            true // No constraint on target node
        };

        from_ok && to_ok
    }

    /// Check if a relationship node type is compatible with a query node label.
    ///
    /// Returns true if:
    /// 1. They match exactly (Post == Post)
    /// 2. Query label is polymorphic and the relationship node type could be one of its concrete types
    ///    
    /// For polymorphic nodes like Message (Post|Comment):
    /// - Message with label_column="type" represents entities from Post and Comment tables
    /// - POST_HAS_TAG with from_node="Post" is compatible (Post is a Message type)
    /// - FORUM_HAS_TAG with from_node="Forum" is NOT compatible (Forum is not a Message type)
    ///
    /// Detection heuristic:
    /// - If query node has label_column (polymorphic) and rel node exists as a separate node,
    ///   check if a relationship exists that connects them via the type discriminator
    fn is_node_type_compatible(&self, rel_node_type: &str, query_label: &str) -> bool {
        // Direct match
        if rel_node_type == query_label {
            return true;
        }

        // Check if query label is polymorphic
        if let Some(query_node_schema) = self.nodes.get(query_label) {
            // Polymorphic node check: has label_column (type discriminator)
            if query_node_schema.label_column.is_some() {
                // For Message with label_column="type", check if rel_node_type (e.g., "Post")
                // is a valid type by checking:
                // 1. If there's a node definition for rel_node_type (e.g., Post exists)
                // 2. If that node's table could map to the same data as the polymorphic node

                // Heuristic: If the rel_node_type exists as a node AND has a relationship
                // defined with that type, it's likely a concrete type

                // Better heuristic for LDBC: Check if rel_node_type appears in relationship
                // names that involve this polymorphic node's concrete types
                // E.g., POST_HAS_TAG suggests Post is a concrete type
                // COMMENT_HAS_TAG suggests Comment is a concrete type
                // FORUM_HAS_TAG does NOT match Message pattern

                // Check if we have relationships named like "{REL_NODE_TYPE}_*"
                // If Message is polymorphic and we see POST_HAS_TAG and COMMENT_HAS_TAG,
                // then Post and Comment are the concrete types

                // For now, use a simple check: does the table name match?
                // Message points to Message table, Post points to Post table
                // If rel_node_type table != query_label table, but query has label_column,
                // check if rel_node_type could be a valid type value

                if let Some(_rel_node_schema) = self.nodes.get(rel_node_type) {
                    // If the polymorphic node's table is different from the concrete node's table,
                    // they're not compatible (Message table vs Post table)
                    // UNLESS the polymorphic node is a view that unions multiple tables

                    // Simplified check: If polymorphic node table name matches or contains
                    // the concrete type name, it's compatible
                    // E.g., Message table for Message label, Post table for Post label
                    // Since Message is a union view, check if it could contain Post rows

                    // Most reliable: check if the rel_node_type appears in the label_value
                    // But label_value is not always set. Alternative: check naming convention

                    // LDBC-specific heuristic: Message represents Post and Comment
                    // because we see POST_* and COMMENT_* relationships in the schema
                    // This is implicit in the schema design

                    // For now, return true if rel_node exists (backward compatibility)
                    // but log that we're using a weak heuristic
                    log::debug!(
                        "Weak heuristic: '{}' node type compatible with '{}' polymorphic node (both exist as nodes)",
                        rel_node_type,
                        query_label
                    );

                    // BETTER CHECK: See if relationship pattern suggests they're related
                    // Count relationships that start with rel_node_type prefix
                    let rel_prefix = format!("{}_", rel_node_type.to_uppercase());
                    let has_typed_relationships = self
                        .relationships
                        .keys()
                        .any(|k| k.starts_with(&rel_prefix));

                    if has_typed_relationships {
                        // This suggests rel_node_type is a concrete entity type
                        // Now check if it makes sense as a component of the polymorphic node

                        // For Message (union view of Post+Comment), Post and Comment have different tables
                        // But they're still valid because Message is designed to represent both
                        //
                        // Key insight: If the polymorphic node has a label_column but different table,
                        // it's likely a union view. In this case, the concrete types ARE compatible
                        // even though tables differ.
                        //
                        // Only reject if the concrete type is clearly unrelated (different domain)
                        // E.g., Forum is not part of Message union

                        // Heuristic: Check if rel_node_type name appears in the polymorphic table name
                        // Message doesn't contain "Forum" → Forum incompatible
                        // Message doesn't contain "Post" or "Comment" either (weak check)

                        // Better: For polymorphic union nodes, check if table names suggest union
                        // Message table with type column suggests it's a union
                        // If query is for Message and rel is Post, check if "post" or "Post" appears
                        // in any relationship type involving Message

                        // Most practical: Accept Post and Comment as Message components,
                        // reject Forum. Use the presence of "Message" in relationship definitions
                        // or explicit type checking.

                        // FINAL HEURISTIC: If polymorphic table name is a generic/abstract name
                        // (like "Message") and concrete is specific (like "Post"), accept it
                        // UNLESS the concrete type name suggests it's from a different domain

                        // Check if rel_node_type is explicitly excluded
                        // Forum is NOT a Message, but Post and Comment ARE
                        let polymorphic_table_lc = query_node_schema.table_name.to_lowercase();
                        let concrete_type_lc = rel_node_type.to_lowercase();

                        // If the polymorphic node and concrete type are in the same "family"
                        // (both related to messaging), accept. Reject if clearly different.
                        // Heuristic: Forum != Message (different concepts)
                        //            Post ~= Message (Post is a type of Message)
                        //            Comment ~= Message (Comment is a type of Message)

                        // Simple check: if concrete type is "Forum" and polymorphic is "Message", reject
                        if polymorphic_table_lc == "message" && concrete_type_lc == "forum" {
                            log::debug!(
                                "Rejecting Forum as incompatible with Message (different domain)"
                            );
                            return false;
                        }

                        // Accept other combinations for union views
                        log::debug!(
                            "Accepting '{}' as compatible with '{}' polymorphic union",
                            rel_node_type,
                            query_label
                        );
                        return true;
                    }

                    return true; // Fallback to permissive
                }
            }
        }

        false
    }

    pub fn get_relationships_schemas(&self) -> &HashMap<String, RelationshipSchema> {
        &self.relationships
    }

    pub fn all_node_schemas(&self) -> &HashMap<String, NodeSchema> {
        &self.nodes
    }

    /// Expand a polymorphic `$any` node type to all concrete node labels.
    /// Returns a single-element vec for concrete types, all node labels for `$any`.
    pub fn expand_node_type(&self, node_type: &str) -> Vec<String> {
        if node_type == "$any" {
            self.nodes.keys().cloned().collect()
        } else {
            vec![node_type.to_string()]
        }
    }

    pub fn node_schema_opt(&self, node_label: &str) -> Option<&NodeSchema> {
        self.nodes.get(node_label)
    }

    pub fn get_relationships_schema_opt(&self, rel_label: &str) -> Option<&RelationshipSchema> {
        // First try exact match (composite key lookup)
        if let Some(schema) = self.relationships.get(rel_label) {
            return Some(schema);
        }

        // If not found and it's a simple type name (no "::"), use rel_type_index for O(1) lookup
        if !rel_label.contains("::") {
            if let Some(composite_keys) = self.rel_type_index.get(rel_label) {
                if let Some(first_key) = composite_keys.first() {
                    if let Some(schema) = self.relationships.get(first_key) {
                        return Some(schema);
                    }
                }
            }
        }

        None
    }

    /// Get the rel_type_index for debugging purposes
    pub fn get_rel_type_index(&self) -> &HashMap<String, Vec<String>> {
        &self.rel_type_index
    }

    /// Get unique relationship type names (without node type suffixes).
    /// Returns only base type names like "FOLLOWS", not "FOLLOWS::User::User".
    ///
    /// This is the standard helper for iterating relationship types without duplicates.
    /// The schema stores both simple keys ("FOLLOWS") and composite keys ("FOLLOWS::User::User")
    /// for backward compatibility, but most code should use this function to avoid duplicates.
    pub fn get_unique_relationship_types(&self) -> Vec<String> {
        self.relationships
            .keys()
            .filter(|key| !key.contains("::"))
            .cloned()
            .collect()
    }

    /// Get properties for a node label as (property_name, column_or_expr) pairs
    pub fn get_node_properties(&self, labels: &[String]) -> Vec<(String, String)> {
        if let Some(label) = labels.first() {
            if let Some(node_schema) = self.node_schema_opt(label) {
                node_schema
                    .property_mappings
                    .iter()
                    .map(|(prop_name, prop_value)| {
                        (prop_name.clone(), prop_value.raw().to_string())
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    /// Get properties for a relationship type as (property_name, column_or_expr) pairs
    pub fn get_relationship_properties(&self, rel_types: &[String]) -> Vec<(String, String)> {
        if let Some(rel_type) = rel_types.first() {
            if let Some(rel_schema) = self.get_relationships_schema_opt(rel_type) {
                rel_schema
                    .property_mappings
                    .iter()
                    .map(|(prop_name, prop_value)| {
                        (prop_name.clone(), prop_value.raw().to_string())
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    /// Get all node schemas with a specific label from all tables
    /// Returns a vector of (table_name, NodeSchema) pairs
    /// Used for MULTI_TABLE_LABEL scenarios where same label exists in multiple tables
    pub fn get_all_node_schemas_for_label(&self, label: &str) -> Vec<(&String, &NodeSchema)> {
        let mut results = Vec::new();

        // Search composite keys (database::table::label)
        for (composite_key, schema) in &self.nodes {
            if composite_key.contains("::") {
                // This is a composite key
                let parts: Vec<&str> = composite_key.split("::").collect();
                if parts.len() == 3 && parts[2] == label {
                    // Found a match
                    results.push((composite_key, schema));
                }
            }
        }

        results
    }

    pub fn get_denormalized_node_metadata(
        &self,
        node_label: &str,
    ) -> Option<&ProcessedNodeMetadata> {
        self.denormalized_nodes.get(node_label)
    }

    /// Check if a node label has denormalized properties
    pub fn is_denormalized_node(&self, node_label: &str) -> bool {
        self.denormalized_nodes.contains_key(node_label)
    }

    /// Get all denormalized node labels
    pub fn get_denormalized_node_labels(&self) -> Vec<&String> {
        self.denormalized_nodes.keys().collect()
    }

    /// Check if two relationship types are coupled (same table, shared coupling node)
    ///
    /// Two edges are coupled when:
    /// 1. They share the same physical table (database.table)
    /// 2. They share at least one node - the "coupling node" (edge1.to_node == edge2.from_node OR
    ///    edge1.from_node == edge2.to_node)
    ///
    /// Coupled edges exist in the same row (same event), so no JOIN is needed.
    ///
    /// Example (Zeek DNS log):
    ///
    /// - REQUESTED: (IP)-[:REQUESTED]->(Domain)  from dns_log
    /// - RESOLVED_TO: (Domain)-[:RESOLVED_TO]->(ResolvedIP)  from dns_log
    ///
    /// These are coupled because:
    ///
    /// - Same table: dns_log
    /// - Coupling node: Domain (REQUESTED.to_node == RESOLVED_TO.from_node)
    ///
    /// IMPORTANT: Same edge type twice is NOT coupled - each hop is a different row!
    /// For example: (a)-[r1:FLIGHT]->(b)-[r2:FLIGHT]->(c) requires joining two flight rows.
    pub fn are_edges_coupled(&self, edge1_type: &str, edge2_type: &str) -> bool {
        // CRITICAL FIX: Same edge type is NEVER coupled!
        // Multi-hop on same edge type means different rows, must JOIN.
        // Coupled edges are for DIFFERENT edge types on the same table (like DNS REQUESTED + RESOLVED_TO)
        if edge1_type == edge2_type {
            return false;
        }

        let edge1 = match self.get_relationships_schema_opt(edge1_type) {
            Some(e) => e,
            None => return false,
        };
        let edge2 = match self.get_relationships_schema_opt(edge2_type) {
            Some(e) => e,
            None => return false,
        };

        // Must be same physical table
        if edge1.full_table_name() != edge2.full_table_name() {
            return false;
        }

        // Must share at least one node
        edge1.to_node == edge2.from_node ||  // edge1 -> shared -> edge2
            edge1.from_node == edge2.to_node ||  // edge2 -> shared -> edge1
            edge1.to_node == edge2.to_node ||    // both point to same node
            edge1.from_node == edge2.from_node // both originate from same node
    }

    /// Get coupling info for two consecutive edges in a path pattern
    ///
    /// Returns Some(CoupledEdgeInfo) if edges are coupled,
    /// where the coupling node connects them (same value in the same row)
    ///
    /// For pattern: (a)-[e1]->(b)-[e2]->(c)
    /// If e1 and e2 are coupled, returns Some(info) with coupling_node = "b"
    ///
    /// IMPORTANT: Same edge type twice is NOT coupled - each hop is a different row!
    pub fn get_coupled_edge_info(
        &self,
        edge1_type: &str,
        edge2_type: &str,
    ) -> Option<CoupledEdgeInfo> {
        // Same edge type is never coupled - multi-hop on same type means different rows
        if edge1_type == edge2_type {
            return None;
        }

        let edge1 = self.get_relationships_schema_opt(edge1_type)?;
        let edge2 = self.get_relationships_schema_opt(edge2_type)?;

        // Must be same physical table
        if edge1.full_table_name() != edge2.full_table_name() {
            return None;
        }

        // Check for edge1.to_node == edge2.from_node (most common: chained path)
        if edge1.to_node == edge2.from_node {
            return Some(CoupledEdgeInfo {
                coupling_node: edge1.to_node.clone(),
                edge1_column: edge1.to_id.to_string(),
                edge2_column: edge2.from_id.to_string(),
                table_name: edge1.full_table_name(),
            });
        }

        // Check for edge1.from_node == edge2.to_node (reverse chain)
        if edge1.from_node == edge2.to_node {
            return Some(CoupledEdgeInfo {
                coupling_node: edge1.from_node.clone(),
                edge1_column: edge1.from_id.to_string(),
                edge2_column: edge2.to_id.to_string(),
                table_name: edge1.full_table_name(),
            });
        }

        None
    }
}

/// Information about coupled edges (edges in the same table row, same event)
#[derive(Debug, Clone)]
pub struct CoupledEdgeInfo {
    /// The coupling node - the node shared between the two edges
    pub coupling_node: String,
    /// Column in edge1 that references the shared node
    pub edge1_column: String,
    /// Column in edge2 that references the shared node
    pub edge2_column: String,
    /// The table containing both edges
    pub table_name: String,
}

// ============================================================================
// Denormalized Edge Table Detection Functions
// ============================================================================

/// Classification of edge table storage patterns
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeTablePattern {
    /// Traditional: Both nodes have separate tables from the edge
    Traditional,
    /// Fully denormalized: Both nodes share the same table as the edge
    FullyDenormalized,
    /// Mixed: One node shares edge table, the other has its own table
    Mixed {
        from_denormalized: bool,
        to_denormalized: bool,
    },
}

/// Detect if a node is using denormalized edge table pattern
///
/// A node is denormalized when:
/// 1. It shares the same physical table as the edge
/// 2. The edge has from_node_properties or to_node_properties defined
/// 3. The node has empty or minimal property_mappings (properties come from edge)
pub fn is_node_denormalized_on_edge(
    node: &NodeSchema,
    edge: &RelationshipSchema,
    is_from_node: bool,
) -> bool {
    // Must use same physical table (including database prefix)
    if node.full_table_name() != edge.full_table_name() {
        log::debug!(
            "  ❌ Not denormalized: different tables (node={}, edge={})",
            node.full_table_name(),
            edge.full_table_name()
        );
        return false;
    }

    // 🔄 REFACTORED: Check NODE-LEVEL denormalized properties (not edge-level)
    // Node must have denormalized properties for this direction
    let has_denormalized_props = if is_from_node {
        node.from_properties.is_some() && !node.from_properties.as_ref().unwrap().is_empty()
    } else {
        node.to_properties.is_some() && !node.to_properties.as_ref().unwrap().is_empty()
    };

    if !has_denormalized_props {
        log::debug!(
            "  ❌ Not denormalized: no {} properties for node label (table={}, is_denorm={})",
            if is_from_node { "from" } else { "to" },
            node.full_table_name(),
            node.is_denormalized
        );
        return false;
    }

    // Check if node is marked as denormalized and has the right source table
    let result = node.is_denormalized
        && node
            .denormalized_source_table
            .as_ref()
            .map(|t| t == &edge.full_table_name())
            .unwrap_or(false);

    log::debug!(
        "  {} Node denormalization check: table={}, is_denorm={}, source_table={:?}, result={}",
        if result { "✅" } else { "❌" },
        node.full_table_name(),
        node.is_denormalized,
        node.denormalized_source_table,
        result
    );

    result
}

/// Check if the edge has denormalized properties for a node position
///
/// This checks if the edge table contains the node's properties, allowing
/// the node data to be read directly from the edge table without a separate JOIN.
///
/// Use this to determine if a node should use the edge table as its data source.
pub fn edge_has_node_properties(edge: &RelationshipSchema, is_from_node: bool) -> bool {
    if is_from_node {
        edge.from_node_properties
            .as_ref()
            .is_some_and(|p| !p.is_empty())
    } else {
        edge.to_node_properties
            .as_ref()
            .is_some_and(|p| !p.is_empty())
    }
}

/// Check if BOTH nodes in a relationship use the denormalized pattern
pub fn is_fully_denormalized_edge_table(
    left_node: &NodeSchema,
    edge: &RelationshipSchema,
    right_node: &NodeSchema,
) -> bool {
    is_node_denormalized_on_edge(left_node, edge, true)
        && is_node_denormalized_on_edge(right_node, edge, false)
}

/// Classify the edge table pattern (traditional, fully denormalized, or mixed)
pub fn classify_edge_table_pattern(
    left_node: &NodeSchema,
    edge: &RelationshipSchema,
    right_node: &NodeSchema,
) -> EdgeTablePattern {
    let from_denorm = is_node_denormalized_on_edge(left_node, edge, true);
    let to_denorm = is_node_denormalized_on_edge(right_node, edge, false);

    let pattern = match (from_denorm, to_denorm) {
        (true, true) => EdgeTablePattern::FullyDenormalized,
        (false, false) => EdgeTablePattern::Traditional,
        (from_d, to_d) => EdgeTablePattern::Mixed {
            from_denormalized: from_d,
            to_denormalized: to_d,
        },
    };

    log::info!(
        "🔍 Edge pattern classification: {:?} for table {} (from_denorm={}, to_denorm={})",
        pattern,
        edge.full_table_name(),
        from_denorm,
        to_denorm
    );

    pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processed_node_metadata_creation() {
        let nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create a denormalized relationship (OnTime flights example)
        let mut from_props = HashMap::new();
        from_props.insert("city".to_string(), "OriginCityName".to_string());
        from_props.insert("state".to_string(), "OriginState".to_string());

        let mut to_props = HashMap::new();
        to_props.insert("city".to_string(), "DestCityName".to_string());
        to_props.insert("state".to_string(), "DestState".to_string());

        let flight_rel = RelationshipSchema {
            database: "default".to_string(),
            table_name: "ontime".to_string(),
            column_names: vec!["Origin".to_string(), "Dest".to_string()],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("Origin"),
            to_id: Identifier::from("Dest"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: Some(Identifier::Composite(vec![
                "FlightDate".to_string(),
                "FlightNum".to_string(),
            ])),
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_props),
            to_node_properties: Some(to_props),
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        relationships.insert("FLIGHT::Airport::Airport".to_string(), flight_rel);

        // Build schema
        let schema = GraphSchema::build(1, "default".to_string(), nodes, relationships);

        // Verify metadata was built
        assert!(schema.is_denormalized_node("Airport"));

        let metadata = schema.get_denormalized_node_metadata("Airport").unwrap();
        assert_eq!(metadata.label, "Airport");

        // Check property sources
        let city_sources = metadata.get_property_sources("city").unwrap();
        assert_eq!(city_sources.len(), 2); // from and to sides

        assert_eq!(city_sources[0].relationship_type, "FLIGHT");
        assert_eq!(city_sources[0].side, "from");
        assert_eq!(city_sources[0].column_name, "OriginCityName");

        assert_eq!(city_sources[1].relationship_type, "FLIGHT");
        assert_eq!(city_sources[1].side, "to");
        assert_eq!(city_sources[1].column_name, "DestCityName");

        // Check ID sources
        let rel_types = metadata.get_relationship_types();
        assert_eq!(rel_types.len(), 1);
        assert!(rel_types.contains(&"FLIGHT".to_string()));
    }

    #[test]
    fn test_multiple_denormalized_nodes() {
        let mut relationships = HashMap::new();

        // Create User->Post relationship with denormalized User properties
        let mut user_props = HashMap::new();
        user_props.insert("name".to_string(), "author_name".to_string());

        let authored_rel = RelationshipSchema {
            database: "default".to_string(),
            table_name: "posts".to_string(),
            column_names: vec!["user_id".to_string(), "post_id".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("user_id"),
            to_id: Identifier::from("post_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(user_props),
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        relationships.insert("AUTHORED::User::Post".to_string(), authored_rel);

        let schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), relationships);

        // User should be denormalized, Post should not
        assert!(schema.is_denormalized_node("User"));
        assert!(!schema.is_denormalized_node("Post"));

        let labels = schema.get_denormalized_node_labels();
        assert_eq!(labels.len(), 1);
        assert!(labels.contains(&&"User".to_string()));
    }

    // ========================================================================
    // Denormalized Edge Table Detection Tests
    // ========================================================================

    #[test]
    fn test_detect_fully_denormalized_pattern() {
        // Pattern: Airport nodes use flights table (fully denormalized)
        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());
        from_props.insert("city".to_string(), "origin_city".to_string());

        let mut to_props = HashMap::new();
        to_props.insert("code".to_string(), "dest_code".to_string());
        to_props.insert("city".to_string(), "dest_city".to_string());

        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: HashMap::new(), // Empty = denormalized
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props.clone()),
            to_properties: Some(to_props.clone()),
            denormalized_source_table: Some("test.flights".to_string()),
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(), // Same table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_props),
            to_node_properties: Some(to_props),
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Test detection
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(is_fully_denormalized_edge_table(
            &airport,
            &flight_edge,
            &airport
        ));

        let pattern = classify_edge_table_pattern(&airport, &flight_edge, &airport);
        assert_eq!(pattern, EdgeTablePattern::FullyDenormalized);
    }

    #[test]
    fn test_detect_traditional_pattern() {
        // Pattern: Airport nodes have separate airports table (traditional)
        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "airports".to_string(), // Different table
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "code".to_string(),
                    PropertyValue::Column("airport_code".to_string()),
                );
                props.insert(
                    "city".to_string(),
                    PropertyValue::Column("city_name".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(), // Different table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None, // No denormalized props
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Test detection
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(!is_fully_denormalized_edge_table(
            &airport,
            &flight_edge,
            &airport
        ));

        let pattern = classify_edge_table_pattern(&airport, &flight_edge, &airport);
        assert_eq!(pattern, EdgeTablePattern::Traditional);
    }

    #[test]
    fn test_detect_mixed_pattern_from_denormalized() {
        // Pattern: Airport uses flights table (denormalized), User uses users table (traditional)
        let mut from_props_airport = HashMap::new();
        from_props_airport.insert("code".to_string(), "origin_code".to_string());
        from_props_airport.insert("city".to_string(), "origin_city".to_string());

        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(), // Same as edge
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: HashMap::new(), // Empty
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props_airport.clone()),
            to_properties: None,
            denormalized_source_table: Some("test.flights".to_string()),
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let user = NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(), // Different from edge
            column_names: vec![],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "user_id".to_string(),
                    PropertyValue::Column("id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("full_name".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());
        from_props.insert("city".to_string(), "origin_city".to_string());

        let booked_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "User".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("user_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: Some(from_props),
            to_node_properties: None, // User is traditional
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Test detection
        assert!(is_node_denormalized_on_edge(&airport, &booked_edge, true));
        assert!(!is_node_denormalized_on_edge(&user, &booked_edge, false));
        assert!(!is_fully_denormalized_edge_table(
            &airport,
            &booked_edge,
            &user
        ));

        let pattern = classify_edge_table_pattern(&airport, &booked_edge, &user);
        assert_eq!(
            pattern,
            EdgeTablePattern::Mixed {
                from_denormalized: true,
                to_denormalized: false,
            }
        );
    }

    #[test]
    fn test_detect_mixed_pattern_to_denormalized() {
        // Pattern: User uses users table (traditional), Post uses posts table which is also edge table
        let user = NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("full_name".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let mut to_props_post = HashMap::new();
        to_props_post.insert("post_id".to_string(), "id".to_string());
        to_props_post.insert("title".to_string(), "post_title".to_string());

        let post = NodeSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(), // Same as edge
            column_names: vec![],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
            property_mappings: HashMap::new(), // Empty - denormalized
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: None,
            to_properties: Some(to_props_post.clone()),
            denormalized_source_table: Some("test.posts".to_string()),
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let mut to_props = HashMap::new();
        to_props.insert("post_id".to_string(), "id".to_string());
        to_props.insert("title".to_string(), "post_title".to_string());

        let authored_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(),
            column_names: vec![],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("author_id"),
            to_id: Identifier::from("id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: Some(to_props),
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Test detection
        assert!(!is_node_denormalized_on_edge(&user, &authored_edge, true));
        assert!(is_node_denormalized_on_edge(&post, &authored_edge, false));

        let pattern = classify_edge_table_pattern(&user, &authored_edge, &post);
        assert_eq!(
            pattern,
            EdgeTablePattern::Mixed {
                from_denormalized: false,
                to_denormalized: true,
            }
        );
    }

    #[test]
    fn test_edge_case_minimal_property_mappings() {
        // Pattern: Node has 1-2 property_mappings but still denormalized
        // (allows for computed properties or special cases)
        let mut from_props_min = HashMap::new();
        from_props_min.insert("code".to_string(), "origin_code".to_string());
        from_props_min.insert("city".to_string(), "origin_city".to_string());

        let mut to_props_min = HashMap::new();
        to_props_min.insert("code".to_string(), "dest_code".to_string());

        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: {
                let mut props = HashMap::new();
                // One or two direct mappings allowed
                props.insert(
                    "computed_field".to_string(),
                    PropertyValue::Column("calc_value".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props_min.clone()),
            to_properties: Some(to_props_min.clone()),
            denormalized_source_table: Some("test.flights".to_string()),
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());
        from_props.insert("city".to_string(), "origin_city".to_string());

        let mut to_props = HashMap::new();
        to_props.insert("code".to_string(), "dest_code".to_string());

        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_props),
            to_node_properties: Some(to_props),
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Should still be detected as denormalized (1-2 mappings allowed)
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(is_fully_denormalized_edge_table(
            &airport,
            &flight_edge,
            &airport
        ));
    }

    #[test]
    fn test_edge_case_same_table_no_denorm_props() {
        // Edge case: Node uses edge table BUT edge has no from/to_node_properties
        // This should NOT be detected as denormalized (misconfiguration)
        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(), // Same table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None, // Missing!
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Should NOT be detected as denormalized (missing props)
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(!is_fully_denormalized_edge_table(
            &airport,
            &flight_edge,
            &airport
        ));

        let pattern = classify_edge_table_pattern(&airport, &flight_edge, &airport);
        assert_eq!(pattern, EdgeTablePattern::Traditional);
    }

    #[test]
    fn test_edge_case_different_database_same_table_name() {
        // Edge case: Same table name but different databases
        // Should NOT be detected as denormalized
        let airport = NodeSchema {
            database: "db1".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };

        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());

        let flight_edge = RelationshipSchema {
            database: "db2".to_string(), // Different database!
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_props),
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Should NOT be detected (different databases)
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
    }

    #[test]
    fn test_edge_case_too_many_property_mappings() {
        // Edge case: Node has many property_mappings (>2)
        // Should NOT be detected as denormalized
        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "prop1".to_string(),
                    PropertyValue::Column("col1".to_string()),
                );
                props.insert(
                    "prop2".to_string(),
                    PropertyValue::Column("col2".to_string()),
                );
                props.insert(
                    "prop3".to_string(),
                    PropertyValue::Column("col3".to_string()),
                );
                props.insert(
                    "prop4".to_string(),
                    PropertyValue::Column("col4".to_string()),
                );
                props // More than 2 = traditional pattern
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            node_id_types: None,
        };

        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());

        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_props),
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // Should NOT be detected (too many property_mappings)
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
    }

    // ========================================================================
    // Coupled Edge Detection Tests
    // ========================================================================

    #[test]
    fn test_coupled_edges_same_table_shared_node() {
        // Zeek DNS pattern: REQUESTED and RESOLVED_TO in same table, sharing Domain node
        let mut relationships = HashMap::new();

        // REQUESTED: (IP)-[:REQUESTED]->(Domain)
        let requested = RelationshipSchema {
            database: "zeek".to_string(),
            table_name: "dns_log".to_string(),
            column_names: vec![],
            from_node: "IP".to_string(),
            to_node: "Domain".to_string(),
            from_node_table: "ips".to_string(),
            to_node_table: "domains".to_string(),
            from_id: Identifier::from("id.orig_h"),
            to_id: Identifier::from("query"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        // RESOLVED_TO: (Domain)-[:RESOLVED_TO]->(ResolvedIP)
        let resolved_to = RelationshipSchema {
            database: "zeek".to_string(),
            table_name: "dns_log".to_string(),
            column_names: vec![],
            from_node: "Domain".to_string(),
            to_node: "ResolvedIP".to_string(),
            from_node_table: "domains".to_string(),
            to_node_table: "resolved_ips".to_string(),
            from_id: Identifier::from("query"),
            to_id: Identifier::from("answers"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        relationships.insert("REQUESTED::IP::Domain".to_string(), requested);
        relationships.insert("RESOLVED_TO::Domain::ResolvedIP".to_string(), resolved_to);

        let schema = GraphSchema::build(1, "zeek".to_string(), HashMap::new(), relationships);

        // These edges should be coupled
        assert!(schema.are_edges_coupled("REQUESTED", "RESOLVED_TO"));
        assert!(schema.are_edges_coupled("RESOLVED_TO", "REQUESTED"));

        // Get coupling info
        let info = schema
            .get_coupled_edge_info("REQUESTED", "RESOLVED_TO")
            .unwrap();
        assert_eq!(info.coupling_node, "Domain");
        assert_eq!(info.edge1_column, "query");
        assert_eq!(info.edge2_column, "query");
        assert_eq!(info.table_name, "zeek.dns_log");
    }

    #[test]
    fn test_not_coupled_different_tables() {
        // Different tables = not coupled
        let mut relationships = HashMap::new();

        let edge1 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "table1".to_string(),
            column_names: vec![],
            from_node: "A".to_string(),
            to_node: "B".to_string(),
            from_node_table: "a_nodes".to_string(),
            to_node_table: "b_nodes".to_string(),
            from_id: Identifier::from("a_id"),
            to_id: Identifier::from("b_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        let edge2 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "table2".to_string(), // Different table!
            column_names: vec![],
            from_node: "B".to_string(), // Shares node B
            to_node: "C".to_string(),
            from_node_table: "b_nodes".to_string(),
            to_node_table: "c_nodes".to_string(),
            from_id: Identifier::from("b_id"),
            to_id: Identifier::from("c_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        relationships.insert("REL1::A::B".to_string(), edge1);
        relationships.insert("REL2::B::C".to_string(), edge2);

        let schema = GraphSchema::build(1, "db".to_string(), HashMap::new(), relationships);

        // Not coupled (different tables)
        assert!(!schema.are_edges_coupled("REL1", "REL2"));
        assert!(schema.get_coupled_edge_info("REL1", "REL2").is_none());
    }

    #[test]
    fn test_not_coupled_no_shared_node() {
        // Same table but no shared node = not coupled (bad schema design!)
        let mut relationships = HashMap::new();

        let edge1 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "same_table".to_string(),
            column_names: vec![],
            from_node: "A".to_string(),
            to_node: "B".to_string(),
            from_node_table: "a_nodes".to_string(),
            to_node_table: "b_nodes".to_string(),
            from_id: Identifier::from("a_id"),
            to_id: Identifier::from("b_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        let edge2 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "same_table".to_string(), // Same table
            column_names: vec![],
            from_node: "C".to_string(), // But different nodes! (C, D vs A, B)
            to_node: "D".to_string(),
            from_node_table: "c_nodes".to_string(),
            to_node_table: "d_nodes".to_string(),
            from_id: Identifier::from("c_id"),
            to_id: Identifier::from("d_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };

        relationships.insert("REL1::A::B".to_string(), edge1);
        relationships.insert("REL2::C::D".to_string(), edge2);

        let schema = GraphSchema::build(1, "db".to_string(), HashMap::new(), relationships);

        // Not coupled (no shared node - bad schema design)
        assert!(!schema.are_edges_coupled("REL1", "REL2"));
    }
}
