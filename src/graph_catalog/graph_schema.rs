use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::config::Identifier;
use super::engine_detection::TableEngine;
use super::errors::GraphSchemaError;
use super::expression_parser::PropertyValue;
use super::filter_parser::SchemaFilter;

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
    Node(NodeSchema),
    Rel(RelationshipSchema),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeIdSchema {
    pub column: String,
    pub dtype: String,
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
            .or_insert_with(Vec::new)
            .push(source);
    }
    
    /// Add an ID source
    pub fn add_id_source(&mut self, rel_type: String, side: String, id_column: String) {
        self.id_sources
            .entry(rel_type)
            .or_insert_with(Vec::new)
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
}

impl GraphSchema {
    pub fn build(
        version: u32,
        database: String,
        nodes: HashMap<String, NodeSchema>,
        relationships: HashMap<String, RelationshipSchema>,
    ) -> GraphSchema {
        // Build denormalized node metadata
        let denormalized_nodes = Self::build_denormalized_metadata(&relationships);
        
        GraphSchema {
            version,
            database,
            nodes,
            relationships,
            denormalized_nodes,
        }
    }
    
    /// Build denormalized node metadata from relationships
    /// Scans all relationships to find denormalized node properties
    fn build_denormalized_metadata(
        relationships: &HashMap<String, RelationshipSchema>,
    ) -> HashMap<String, ProcessedNodeMetadata> {
        let mut metadata_map: HashMap<String, ProcessedNodeMetadata> = HashMap::new();
        
        for (rel_type, rel_schema) in relationships {
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
                    rel_schema.from_id.clone(),
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
                    rel_schema.to_id.clone(),
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
    
    /// Get denormalized node metadata for a given node label
    pub fn get_denormalized_node_metadata(&self, node_label: &str) -> Option<&ProcessedNodeMetadata> {
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
    /// - REQUESTED: (IP)-[:REQUESTED]->(Domain)  from dns_log
    /// - RESOLVED_TO: (Domain)-[:RESOLVED_TO]->(ResolvedIP)  from dns_log
    /// These are coupled because:
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
        let shared_node = 
            edge1.to_node == edge2.from_node ||  // edge1 -> shared -> edge2
            edge1.from_node == edge2.to_node ||  // edge2 -> shared -> edge1
            edge1.to_node == edge2.to_node ||    // both point to same node
            edge1.from_node == edge2.from_node;  // both originate from same node
        
        shared_node
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
        edge2_type: &str
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
                edge1_column: edge1.to_id.clone(),
                edge2_column: edge2.from_id.clone(),
                table_name: edge1.full_table_name(),
            });
        }
        
        // Check for edge1.from_node == edge2.to_node (reverse chain)
        if edge1.from_node == edge2.to_node {
            return Some(CoupledEdgeInfo {
                coupling_node: edge1.from_node.clone(),
                edge1_column: edge1.from_id.clone(),
                edge2_column: edge2.to_id.clone(),
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
        return false;
    }
    
    // 🔄 REFACTORED: Check NODE-LEVEL denormalized properties (not edge-level)
    // Node must have denormalized properties for this direction
    let has_denormalized_props = if is_from_node {
        node.from_properties.is_some() 
            && !node.from_properties.as_ref().unwrap().is_empty()
    } else {
        node.to_properties.is_some() 
            && !node.to_properties.as_ref().unwrap().is_empty()
    };
    
    if !has_denormalized_props {
        return false;
    }
    
    // Check if node is marked as denormalized and has the right source table
    node.is_denormalized 
        && node.denormalized_source_table.as_ref().map(|t| t == &edge.full_table_name()).unwrap_or(false)
}

/// Check if the edge has denormalized properties for a node position
/// 
/// This checks if the edge table contains the node's properties, allowing 
/// the node data to be read directly from the edge table without a separate JOIN.
/// 
/// Use this to determine if a node should use the edge table as its data source.
pub fn edge_has_node_properties(edge: &RelationshipSchema, is_from_node: bool) -> bool {
    if is_from_node {
        edge.from_node_properties.as_ref().map_or(false, |p| !p.is_empty())
    } else {
        edge.to_node_properties.as_ref().map_or(false, |p| !p.is_empty())
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
    
    match (from_denorm, to_denorm) {
        (true, true) => EdgeTablePattern::FullyDenormalized,
        (false, false) => EdgeTablePattern::Traditional,
        (from_d, to_d) => EdgeTablePattern::Mixed {
            from_denormalized: from_d,
            to_denormalized: to_d,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_processed_node_metadata_creation() {
        let mut nodes = HashMap::new();
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
            from_id: "Origin".to_string(),
            to_id: "Dest".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        relationships.insert("FLIGHT".to_string(), flight_rel);
        
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
            from_id: "user_id".to_string(),
            to_id: "post_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
        };
        
        relationships.insert("AUTHORED".to_string(), authored_rel);
        
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
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: HashMap::new(),  // Empty = denormalized
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props.clone()),
            to_properties: Some(to_props.clone()),
            denormalized_source_table: Some("test.flights".to_string()),
        };
        
        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),  // Same table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        // Test detection
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(is_fully_denormalized_edge_table(&airport, &flight_edge, &airport));
        
        let pattern = classify_edge_table_pattern(&airport, &flight_edge, &airport);
        assert_eq!(pattern, EdgeTablePattern::FullyDenormalized);
    }
    
    #[test]
    fn test_detect_traditional_pattern() {
        // Pattern: Airport nodes have separate airports table (traditional)
        let airport = NodeSchema {
            database: "test".to_string(),
            table_name: "airports".to_string(),  // Different table
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("code".to_string(), PropertyValue::Column("airport_code".to_string()));
                props.insert("city".to_string(), PropertyValue::Column("city_name".to_string()));
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
        };
        
        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),  // Different table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,  // No denormalized props
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
        };
        
        // Test detection
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(!is_fully_denormalized_edge_table(&airport, &flight_edge, &airport));
        
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
            table_name: "flights".to_string(),  // Same as edge
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: HashMap::new(),  // Empty
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props_airport.clone()),
            to_properties: None,
            denormalized_source_table: Some("test.flights".to_string()),
        };
        
        let user = NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),  // Different from edge
            column_names: vec![],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema {
                column: "user_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("user_id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert("name".to_string(), PropertyValue::Column("full_name".to_string()));
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
            from_id: "origin_code".to_string(),
            to_id: "user_id".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
            to_node_properties: None,  // User is traditional
            is_fk_edge: false,
        };
        
        // Test detection
        assert!(is_node_denormalized_on_edge(&airport, &booked_edge, true));
        assert!(!is_node_denormalized_on_edge(&user, &booked_edge, false));
        assert!(!is_fully_denormalized_edge_table(&airport, &booked_edge, &user));
        
        let pattern = classify_edge_table_pattern(&airport, &booked_edge, &user);
        assert_eq!(pattern, EdgeTablePattern::Mixed {
            from_denormalized: true,
            to_denormalized: false,
        });
    }
    
    #[test]
    fn test_detect_mixed_pattern_to_denormalized() {
        // Pattern: User uses users table (traditional), Post uses posts table which is also edge table
        let user = NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema {
                column: "user_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("name".to_string(), PropertyValue::Column("full_name".to_string()));
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
        };
        
        let mut to_props_post = HashMap::new();
        to_props_post.insert("post_id".to_string(), "id".to_string());
        to_props_post.insert("title".to_string(), "post_title".to_string());
        
        let post = NodeSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(),  // Same as edge
            column_names: vec![],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema {
                column: "post_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: HashMap::new(),  // Empty - denormalized
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: None,
            to_properties: Some(to_props_post.clone()),
            denormalized_source_table: Some("test.posts".to_string()),
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
            from_id: "author_id".to_string(),
            to_id: "id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
        };
        
        // Test detection
        assert!(!is_node_denormalized_on_edge(&user, &authored_edge, true));
        assert!(is_node_denormalized_on_edge(&post, &authored_edge, false));
        
        let pattern = classify_edge_table_pattern(&user, &authored_edge, &post);
        assert_eq!(pattern, EdgeTablePattern::Mixed {
            from_denormalized: false,
            to_denormalized: true,
        });
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
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: {
                let mut props = HashMap::new();
                // One or two direct mappings allowed
                props.insert("computed_field".to_string(), PropertyValue::Column("calc_value".to_string()));
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
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        // Should still be detected as denormalized (1-2 mappings allowed)
        assert!(is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(is_fully_denormalized_edge_table(&airport, &flight_edge, &airport));
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
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        };
        
        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),  // Same table
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,  // Missing!
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
        };
        
        // Should NOT be detected as denormalized (missing props)
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, true));
        assert!(!is_node_denormalized_on_edge(&airport, &flight_edge, false));
        assert!(!is_fully_denormalized_edge_table(&airport, &flight_edge, &airport));
        
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
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        };
        
        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());
        
        let flight_edge = RelationshipSchema {
            database: "db2".to_string(),  // Different database!
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
            node_id: NodeIdSchema {
                column: "code".to_string(),
                dtype: "String".to_string(),
            },
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("prop1".to_string(), PropertyValue::Column("col1".to_string()));
                props.insert("prop2".to_string(), PropertyValue::Column("col2".to_string()));
                props.insert("prop3".to_string(), PropertyValue::Column("col3".to_string()));
                props.insert("prop4".to_string(), PropertyValue::Column("col4".to_string()));
                props  // More than 2 = traditional pattern
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
        };
        
        let mut from_props = HashMap::new();
        from_props.insert("code".to_string(), "origin_code".to_string());
        
        let flight_edge = RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
            from_id: "id.orig_h".to_string(),
            to_id: "query".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        // RESOLVED_TO: (Domain)-[:RESOLVED_TO]->(ResolvedIP)
        let resolved_to = RelationshipSchema {
            database: "zeek".to_string(),
            table_name: "dns_log".to_string(),
            column_names: vec![],
            from_node: "Domain".to_string(),
            to_node: "ResolvedIP".to_string(),
            from_id: "query".to_string(),
            to_id: "answers".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        relationships.insert("REQUESTED".to_string(), requested);
        relationships.insert("RESOLVED_TO".to_string(), resolved_to);
        
        let schema = GraphSchema::build(1, "zeek".to_string(), HashMap::new(), relationships);
        
        // These edges should be coupled
        assert!(schema.are_edges_coupled("REQUESTED", "RESOLVED_TO"));
        assert!(schema.are_edges_coupled("RESOLVED_TO", "REQUESTED"));
        
        // Get coupling info
        let info = schema.get_coupled_edge_info("REQUESTED", "RESOLVED_TO").unwrap();
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
            from_id: "a_id".to_string(),
            to_id: "b_id".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        let edge2 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "table2".to_string(),  // Different table!
            column_names: vec![],
            from_node: "B".to_string(),  // Shares node B
            to_node: "C".to_string(),
            from_id: "b_id".to_string(),
            to_id: "c_id".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        relationships.insert("REL1".to_string(), edge1);
        relationships.insert("REL2".to_string(), edge2);
        
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
            from_id: "a_id".to_string(),
            to_id: "b_id".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        let edge2 = RelationshipSchema {
            database: "db".to_string(),
            table_name: "same_table".to_string(),  // Same table
            column_names: vec![],
            from_node: "C".to_string(),  // But different nodes! (C, D vs A, B)
            to_node: "D".to_string(),
            from_id: "c_id".to_string(),
            to_id: "d_id".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
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
        };
        
        relationships.insert("REL1".to_string(), edge1);
        relationships.insert("REL2".to_string(), edge2);
        
        let schema = GraphSchema::build(1, "db".to_string(), HashMap::new(), relationships);
        
        // Not coupled (no shared node - bad schema design)
        assert!(!schema.are_edges_coupled("REL1", "REL2"));
    }
}
