//! Unified Pattern Schema Context
//!
//! This module provides a single abstraction (`PatternSchemaContext`) that captures
//! all schema-related decisions for a graph pattern ONCE, enabling exhaustive
//! pattern matching instead of scattered conditional checks.
//!
//! # Problem Solved
//!
//! Previously, schema detection was scattered across 4800+ lines in `graph_join_inference.rs`:
//! - `is_node_denormalized_on_edge()` - called multiple times
//!
//! Note: Some methods and fields are reserved for future pattern optimization features.
#![allow(dead_code)]
//! - `edge_has_node_properties()` - called at various points
//! - `classify_edge_table_pattern()` - computed repeatedly
//! - `are_edges_coupled()` - checked in nested conditions
//!
//! This led to:
//! - Complex nested conditionals
//! - Ping-pong bugs when fixing one schema type
//! - Difficulty reasoning about code behavior
//!
//! # Solution
//!
//! Compute `PatternSchemaContext` ONCE at pattern analysis time, then use
//! exhaustive `match` statements throughout the codebase.
//!
//! # Example
//!
//! ```ignore
//! let ctx = PatternSchemaContext::analyze(&left_node, &right_node, &edge, &graph_schema);
//!
//! match ctx.join_strategy {
//!     JoinStrategy::SingleTableScan { .. } => { /* denormalized path */ }
//!     JoinStrategy::Traditional { .. } => { /* standard JOINs */ }
//!     JoinStrategy::EdgeToEdge { .. } => { /* multi-hop denormalized */ }
//!     JoinStrategy::CoupledSameRow { .. } => { /* coupled optimization */ }
//! }
//! ```

use super::graph_schema::{
    classify_edge_table_pattern, EdgeTablePattern, GraphSchema, NodeSchema, RelationshipSchema,
};
use std::collections::HashMap;

// ============================================================================
// Core Types
// ============================================================================

/// Property mappings: Cypher property name → SQL column expression
pub type PropertyMappings = HashMap<String, String>;

/// Position of a node in a relationship pattern
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePosition {
    /// Left/source/from node
    Left,
    /// Right/target/to node
    Right,
}

impl std::fmt::Display for NodePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodePosition::Left => write!(f, "left"),
            NodePosition::Right => write!(f, "right"),
        }
    }
}

// ============================================================================
// Node Access Strategy
// ============================================================================

/// How to access data for a node in a graph pattern.
///
/// This determines:
/// - Where to get node properties from
/// - Whether a JOIN is needed
/// - What alias to use for property resolution
#[derive(Debug, Clone, PartialEq)]
pub enum NodeAccessStrategy {
    /// Node has its own dedicated table, JOIN required to access properties.
    ///
    /// Example: `users` table for `:User` nodes, separate from `follows` edge table.
    OwnTable {
        /// Fully qualified table name (e.g., "brahmand.users")
        table: String,
        /// Primary ID column for JOIN condition
        id_column: String,
        /// Property name → column mappings
        properties: PropertyMappings,
    },

    /// Node properties are embedded in the edge table, no JOIN needed.
    ///
    /// Example: OnTime flights where Airport code/city/state are columns in flights table.
    EmbeddedInEdge {
        /// The edge alias this node's data comes from
        edge_alias: String,
        /// Property name → column mappings (from edge's from_node_properties or to_node_properties)
        properties: PropertyMappings,
        /// True if this is the from_node, false if to_node
        is_from_node: bool,
    },

    /// Virtual node in polymorphic $any patterns.
    ///
    /// The actual node type is determined at runtime via from_label_column/to_label_column.
    Virtual {
        /// The label used in the query (may be filtered dynamically)
        label: String,
    },
}

impl NodeAccessStrategy {
    /// Returns the table/alias to use for property access
    pub fn property_source_alias(&self) -> Option<&str> {
        match self {
            NodeAccessStrategy::OwnTable { table, .. } => Some(table),
            NodeAccessStrategy::EmbeddedInEdge { edge_alias, .. } => Some(edge_alias),
            NodeAccessStrategy::Virtual { .. } => None,
        }
    }

    /// Returns true if this node requires a JOIN
    pub fn requires_join(&self) -> bool {
        matches!(self, NodeAccessStrategy::OwnTable { .. })
    }

    /// Returns true if node data comes from edge table
    pub fn is_embedded(&self) -> bool {
        matches!(self, NodeAccessStrategy::EmbeddedInEdge { .. })
    }

    /// Get property mapping for a given property name
    pub fn get_property_column(&self, prop_name: &str) -> Option<&str> {
        match self {
            NodeAccessStrategy::OwnTable { properties, .. }
            | NodeAccessStrategy::EmbeddedInEdge { properties, .. } => {
                properties.get(prop_name).map(|s| s.as_str())
            }
            NodeAccessStrategy::Virtual { .. } => None,
        }
    }
}

// ============================================================================
// Edge Access Strategy
// ============================================================================

/// How to access edge/relationship data in a graph pattern.
///
/// This determines:
/// - Which table contains edge data
/// - How to filter for specific edge types
/// - FK vs separate table patterns
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeAccessStrategy {
    /// Standard separate edge table (most common).
    ///
    /// Example: `user_follows` table with `follower_id`, `followed_id` columns.
    SeparateTable {
        /// Fully qualified table name
        table: String,
        /// Column for source node ID
        from_id: String,
        /// Column for target node ID
        to_id: String,
        /// Edge property mappings
        properties: PropertyMappings,
    },

    /// Polymorphic edge table with type discriminator column.
    ///
    /// Example: `interactions` table with `interaction_type` column.
    Polymorphic {
        /// Fully qualified table name
        table: String,
        /// Column for source node ID
        from_id: String,
        /// Column for target node ID
        to_id: String,
        /// Column containing edge type (e.g., "interaction_type") - optional for label-only polymorphism
        type_column: Option<String>,
        /// Valid type values for this relationship
        type_values: Vec<String>,
        /// Optional: column for source node label (for $any nodes)
        from_label_column: Option<String>,
        /// Optional: column for target node label (for $any nodes)
        to_label_column: Option<String>,
        /// Edge property mappings
        properties: PropertyMappings,
    },

    /// FK-edge pattern: edge is a foreign key column on node table.
    ///
    /// Example: `fs_objects` table with `parent_id` FK for hierarchical relationships.
    FkEdge {
        /// The node table that contains the FK column
        node_table: String,
        /// The FK column name
        fk_column: String,
    },
}

impl EdgeAccessStrategy {
    /// Get the table name for this edge
    pub fn table_name(&self) -> &str {
        match self {
            EdgeAccessStrategy::SeparateTable { table, .. }
            | EdgeAccessStrategy::Polymorphic { table, .. } => table,
            EdgeAccessStrategy::FkEdge { node_table, .. } => node_table,
        }
    }

    /// Get the from_id column
    pub fn from_id_column(&self) -> &str {
        match self {
            EdgeAccessStrategy::SeparateTable { from_id, .. }
            | EdgeAccessStrategy::Polymorphic { from_id, .. } => from_id,
            EdgeAccessStrategy::FkEdge { fk_column, .. } => fk_column,
        }
    }

    /// Get the to_id column
    pub fn to_id_column(&self) -> &str {
        match self {
            EdgeAccessStrategy::SeparateTable { to_id, .. }
            | EdgeAccessStrategy::Polymorphic { to_id, .. } => to_id,
            EdgeAccessStrategy::FkEdge { .. } => "id", // FK edges point to node's own ID
        }
    }

    /// Returns true if this is a polymorphic edge
    pub fn is_polymorphic(&self) -> bool {
        matches!(self, EdgeAccessStrategy::Polymorphic { .. })
    }

    /// Get type filter expression for polymorphic edges
    pub fn get_type_filter(&self, alias: &str) -> Option<String> {
        match self {
            EdgeAccessStrategy::Polymorphic {
                type_column,
                type_values,
                ..
            } => {
                // Only generate type filter if type_column exists
                let type_col = type_column.as_ref()?;

                if type_values.len() == 1 {
                    Some(format!("{}.{} = '{}'", alias, type_col, type_values[0]))
                } else if !type_values.is_empty() {
                    let types_str = type_values
                        .iter()
                        .map(|t| format!("'{}'", t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Some(format!("{}.{} IN ({})", alias, type_col, types_str))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Get label filter for polymorphic edges that filter by node type.
    ///
    /// For polymorphic edges with from_label_column/to_label_column, we need
    /// to filter by the actual node type when the query specifies concrete labels.
    ///
    /// # Arguments
    /// * `alias` - The edge table alias
    /// * `left_label` - The left node label from the query (e.g., "User")
    /// * `right_label` - The right node label from the query (e.g., "Group")
    ///
    /// # Returns
    /// A SQL filter string like "r.member_type = 'User'" or combined filters
    pub fn get_label_filter(
        &self,
        alias: &str,
        left_label: &str,
        right_label: &str,
    ) -> Option<String> {
        match self {
            EdgeAccessStrategy::Polymorphic {
                from_label_column,
                to_label_column,
                ..
            } => {
                let mut parts = Vec::new();

                // Add from_label filter if column exists and left_label is specified
                if let Some(ref col) = from_label_column {
                    if !left_label.is_empty() {
                        parts.push(format!("{}.{} = '{}'", alias, col, left_label));
                    }
                }

                // Add to_label filter if column exists and right_label is specified
                if let Some(ref col) = to_label_column {
                    if !right_label.is_empty() {
                        parts.push(format!("{}.{} = '{}'", alias, col, right_label));
                    }
                }

                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(" AND "))
                }
            }
            _ => None,
        }
    }
}
// ============================================================================

/// How to generate SQL JOINs for a graph pattern.
///
/// This is the key decision point - different strategies produce very different SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    /// Traditional pattern: separate node and edge tables requiring JOINs.
    ///
    /// ```sql
    /// SELECT ... FROM users u
    /// JOIN follows r ON r.follower_id = u.user_id
    /// JOIN users u2 ON u2.user_id = r.followed_id
    /// ```
    Traditional {
        /// Column on edge for left node join (e.g., "follower_id")
        left_join_col: String,
        /// Column on edge for right node join (e.g., "followed_id")
        right_join_col: String,
    },

    /// Fully denormalized: single table scan, no JOINs needed.
    ///
    /// ```sql
    /// SELECT Origin, Dest, ... FROM flights
    /// ```
    SingleTableScan {
        /// The table containing all data
        table: String,
    },

    /// Mixed pattern: one node requires JOIN, the other is embedded.
    ///
    /// ```sql
    /// SELECT ... FROM flights f
    /// JOIN users u ON u.user_id = f.pilot_id  -- only left node joined
    /// ```
    MixedAccess {
        /// Which node requires a JOIN
        joined_node: NodePosition,
        /// Column on edge for the joined node
        join_col: String,
    },

    /// Multi-hop denormalized: edge-to-edge JOIN for consecutive hops.
    ///
    /// ```sql
    /// SELECT ... FROM flights f1
    /// JOIN flights f2 ON f2.Origin = f1.Dest
    /// ```
    EdgeToEdge {
        /// Alias of the previous edge (e.g., "f1")
        prev_edge_alias: String,
        /// Column on previous edge (e.g., "Dest" from f1)
        prev_edge_col: String,
        /// Column on current edge (e.g., "Origin" of f2)
        curr_edge_col: String,
    },

    /// Coupled edges: same physical row, alias unification.
    ///
    /// No additional JOIN needed - both edges read from same row.
    CoupledSameRow {
        /// The unified alias for both edges
        unified_alias: String,
    },

    /// FK-edge pattern: edge is a foreign key column in the node table.
    ///
    /// Self-referencing example (parent_id on same table):
    /// ```sql
    /// SELECT child.*, parent.*
    /// FROM objects child
    /// JOIN objects parent ON parent.object_id = child.parent_id
    /// ```
    ///
    /// Non-self-referencing example (orders.user_id → users.id):
    /// Edge table IS the to_node table (orders), so we JOIN the from_node (users).
    /// ```sql
    /// SELECT u.*, o.*
    /// FROM users u
    /// JOIN orders o ON o.user_id = u.id
    /// ```
    FkEdgeJoin {
        /// The FK column in the edge table that references the from_node's ID
        from_id: String,
        /// The column in the edge table that identifies the to_node
        to_id: String,
        /// Which node table needs to be JOINed (the one that ISN'T the edge table)
        /// Left = edge_table == to_node_table, need to JOIN from_node (left)
        /// Right = edge_table == from_node_table, need to JOIN to_node (right)
        join_side: NodePosition,
        /// True if self-referencing (same table for both nodes)
        is_self_referencing: bool,
    },
}

impl JoinStrategy {
    /// Returns true if this strategy requires no JOINs
    pub fn is_joinless(&self) -> bool {
        matches!(
            self,
            JoinStrategy::SingleTableScan { .. } | JoinStrategy::CoupledSameRow { .. }
        )
    }

    /// Returns a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            JoinStrategy::Traditional { .. } => "Traditional (node-edge-node JOINs)",
            JoinStrategy::SingleTableScan { .. } => "Single table scan (denormalized)",
            JoinStrategy::MixedAccess { .. } => "Mixed (partial JOIN)",
            JoinStrategy::EdgeToEdge { .. } => "Edge-to-edge (multi-hop denormalized)",
            JoinStrategy::CoupledSameRow { .. } => "Coupled (same row, no JOIN)",
            JoinStrategy::FkEdgeJoin {
                is_self_referencing,
                ..
            } => {
                if *is_self_referencing {
                    "FK-edge self-join (same table)"
                } else {
                    "FK-edge (cross-table FK)"
                }
            }
        }
    }
}

// ============================================================================
// Coupled Edge Context
// ============================================================================

/// Context for coupled edge optimization.
///
/// Coupled edges share the same physical table and connect through a common
/// "coupling node", allowing alias unification and self-join elimination.
#[derive(Debug, Clone)]
pub struct CoupledEdgeContext {
    /// The previous edge alias this is coupled with
    pub prev_edge_alias: String,
    /// The coupling node alias
    pub coupling_node_alias: String,
    /// The shared table
    pub shared_table: String,
}

// ============================================================================
// Pattern Schema Context - The Main Abstraction
// ============================================================================

/// Complete schema context for a single graph pattern (edge + two nodes).
///
/// This is computed ONCE when analyzing a `GraphRel` and then used throughout
/// query planning and SQL generation via exhaustive pattern matching.
///
/// # Example
///
/// ```ignore
/// // For pattern: (a:Airport)-[r1:FLIGHT]->(b:Airport)
/// let ctx = PatternSchemaContext::analyze(&airport_schema, &airport_schema, &flight_schema, &graph_schema);
///
/// assert!(matches!(ctx.left_node, NodeAccessStrategy::EmbeddedInEdge { .. }));
/// assert!(matches!(ctx.right_node, NodeAccessStrategy::EmbeddedInEdge { .. }));
/// assert!(matches!(ctx.join_strategy, JoinStrategy::SingleTableScan { .. }));
/// ```
#[derive(Debug, Clone)]
pub struct PatternSchemaContext {
    /// Access strategy for the left (source/from) node
    pub left_node: NodeAccessStrategy,
    /// Access strategy for the right (target/to) node
    pub right_node: NodeAccessStrategy,
    /// Access strategy for the edge
    pub edge: EdgeAccessStrategy,
    /// How to generate JOINs for this pattern
    pub join_strategy: JoinStrategy,
    /// Coupled edge context (if this edge is coupled with a previous one)
    pub coupled_context: Option<CoupledEdgeContext>,
    /// The relationship type(s) for this pattern
    pub rel_types: Vec<String>,
    /// True if left node is polymorphic $any
    pub left_is_polymorphic: bool,
    /// True if right node is polymorphic $any
    pub right_is_polymorphic: bool,
    /// Edge constraints from schema (e.g. "from.age > to.age")
    pub constraints: Option<String>,
}

impl PatternSchemaContext {
    /// Analyze a graph pattern and produce unified schema context.
    ///
    /// This is the main entry point - call this once per GraphRel during analysis.
    ///
    /// # Arguments
    ///
    /// * `left_node_schema` - Schema for the left/from node
    /// * `right_node_schema` - Schema for the right/to node
    /// * `rel_schema` - Schema for the relationship/edge
    /// * `graph_schema` - Full graph schema for coupled edge detection
    /// * `rel_alias` - The alias assigned to this relationship in the query
    /// * `rel_types` - The relationship type(s) being matched
    /// * `prev_edge_info` - Previous edge info for multi-hop coupling detection
    pub fn analyze(
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
        graph_schema: &GraphSchema,
        rel_alias: &str,
        rel_types: Vec<String>,
        prev_edge_info: Option<(&str, &str, bool)>, // (prev_rel_alias, prev_rel_type, is_from_node)
    ) -> Result<Self, String> {
        // 1. Detect polymorphic $any patterns
        let left_is_polymorphic = rel_schema.from_node == "$any";
        let right_is_polymorphic = rel_schema.to_node == "$any";

        // 2. Classify edge table pattern using existing function
        let edge_pattern =
            classify_edge_table_pattern(left_node_schema, rel_schema, right_node_schema);

        // 3. Build edge access strategy
        let edge = Self::build_edge_strategy(rel_schema, &rel_types);

        // 4. Build node access strategies based on pattern
        let (left_node, right_node) = Self::build_node_strategies(
            left_node_schema,
            right_node_schema,
            rel_schema,
            rel_alias,
            left_is_polymorphic,
            right_is_polymorphic,
            &edge_pattern,
        )?;

        // 5. Determine join strategy and coupled context
        let (join_strategy, coupled_context) = Self::determine_join_strategy(
            &edge_pattern,
            rel_schema,
            left_node_schema,
            right_node_schema,
            graph_schema,
            rel_alias,
            &rel_types,
            prev_edge_info,
        );

        Ok(PatternSchemaContext {
            left_node,
            right_node,
            edge,
            join_strategy,
            coupled_context,
            rel_types,
            left_is_polymorphic,
            right_is_polymorphic,
            constraints: rel_schema.constraints.clone(),
        })
    }

    /// Resolve node ID column through property mappings.
    ///
    /// The `node_id` in the schema is a Cypher property name (e.g., "ip"),
    /// but JOIN conditions need the actual database column name (e.g., "orig_h").
    /// 
    /// For denormalized edge schemas, the mapping is in from_properties/to_properties
    /// of the node schema (which correspond to the edge's table). For standalone nodes,
    /// the mapping is in property_mappings.
    fn resolve_id_column(node_schema: &NodeSchema, is_from_node: bool) -> Result<String, String> {
        // Get the node ID property name from schema (Cypher name)
        let id_property = node_schema
            .node_id
            .columns()
            .first()
            .ok_or_else(|| format!("Node schema has no ID columns defined"))?
            .to_string();

        // Try to resolve through from_properties or to_properties (for denormalized edges)
        let node_props_opt = if is_from_node {
            &node_schema.from_properties
        } else {
            &node_schema.to_properties
        };
        
        if let Some(node_props) = node_props_opt {
            if let Some(column_name) = node_props.get(&id_property) {
                log::info!("🔧 resolve_id_column: '{}' (Cypher) → '{}' (DB column) via {} for table {}", 
                    id_property, column_name,
                    if is_from_node { "from_properties" } else { "to_properties" },
                    node_schema.table_name);
                return Ok(column_name.clone());
            }
        }

        // Fallback: Try property_mappings (for standalone node tables)
        if let Some(property_value) = node_schema.property_mappings.get(&id_property) {
            let resolved = property_value.to_sql_column_only();
            log::info!("🔧 resolve_id_column: '{}' (Cypher) → '{}' (DB column) via property_mappings for table {}", 
                id_property, resolved, node_schema.table_name);
            return Ok(resolved);
        }

        // No mapping found - use the property name directly
        // (this is OK for simple schemas where Cypher name = DB column name)
        log::info!("🔧 resolve_id_column: '{}' used as-is (no mapping) for table {}", 
            id_property, node_schema.table_name);
        Ok(id_property)
    }

    /// Build edge access strategy from relationship schema
    fn build_edge_strategy(
        rel_schema: &RelationshipSchema,
        rel_types: &[String],
    ) -> EdgeAccessStrategy {
        // Check if polymorphic: either has type_column, from_label_column, or to_label_column
        let has_type_column = rel_schema.type_column.is_some();
        let has_label_columns =
            rel_schema.from_label_column.is_some() || rel_schema.to_label_column.is_some();

        if has_type_column || has_label_columns {
            EdgeAccessStrategy::Polymorphic {
                table: rel_schema.full_table_name(),
                from_id: rel_schema.from_id.clone(),
                to_id: rel_schema.to_id.clone(),
                type_column: rel_schema.type_column.clone(),
                type_values: rel_types.to_vec(),
                from_label_column: rel_schema.from_label_column.clone(),
                to_label_column: rel_schema.to_label_column.clone(),
                properties: rel_schema
                    .property_mappings
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                    .collect(),
            }
        } else {
            // Standard separate table
            EdgeAccessStrategy::SeparateTable {
                table: rel_schema.full_table_name(),
                from_id: rel_schema.from_id.clone(),
                to_id: rel_schema.to_id.clone(),
                properties: rel_schema
                    .property_mappings
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                    .collect(),
            }
        }
    }

    /// Build node access strategies based on edge table pattern
    fn build_node_strategies(
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
        rel_alias: &str,
        _left_is_polymorphic: bool,
        _right_is_polymorphic: bool,
        edge_pattern: &EdgeTablePattern,
    ) -> Result<(NodeAccessStrategy, NodeAccessStrategy), String> {
        // IMPORTANT: Even when the relationship schema defines polymorphic endpoints ($any),
        // the actual query provides concrete node labels. The caller resolves these labels
        // to actual NodeSchema objects (left_node_schema, right_node_schema).
        //
        // Therefore, we should NOT create Virtual nodes when we have concrete schemas.
        // Virtual nodes were previously created based on rel_schema.from_node == "$any",
        // but this ignores the fact that the query specifies concrete types like User, Group.
        //
        // The fix: Always use node_strategy_for_position() which builds OwnTable/Embedded
        // strategies based on the actual node schema, not the edge's polymorphic endpoint.
        //
        // Note: _left_is_polymorphic and _right_is_polymorphic are kept for API compatibility
        // and may be used for edge filtering (type_column filters), but not for node strategies.

        // Non-polymorphic: use edge pattern classification
        let node_strategies = match edge_pattern {
            EdgeTablePattern::FullyDenormalized => {
                let left = NodeAccessStrategy::EmbeddedInEdge {
                    edge_alias: rel_alias.to_string(),
                    properties: Self::extract_denorm_props(rel_schema, true),
                    is_from_node: true,
                };
                let right = NodeAccessStrategy::EmbeddedInEdge {
                    edge_alias: rel_alias.to_string(),
                    properties: Self::extract_denorm_props(rel_schema, false),
                    is_from_node: false,
                };
                (left, right)
            }
            EdgeTablePattern::Traditional => {
                let left = NodeAccessStrategy::OwnTable {
                    table: left_node_schema.full_table_name(),
                    id_column: Self::resolve_id_column(left_node_schema, true)?,
                    properties: left_node_schema
                        .property_mappings
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                        .collect(),
                };
                let right = NodeAccessStrategy::OwnTable {
                    table: right_node_schema.full_table_name(),
                    id_column: Self::resolve_id_column(right_node_schema, false)?,
                    properties: right_node_schema
                        .property_mappings
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                        .collect(),
                };
                (left, right)
            }
            EdgeTablePattern::Mixed {
                from_denormalized,
                to_denormalized,
            } => {
                let left = if *from_denormalized {
                    NodeAccessStrategy::EmbeddedInEdge {
                        edge_alias: rel_alias.to_string(),
                        properties: Self::extract_denorm_props(rel_schema, true),
                        is_from_node: true,
                    }
                } else {
                    NodeAccessStrategy::OwnTable {
                        table: left_node_schema.full_table_name(),
                        id_column: Self::resolve_id_column(left_node_schema, true)?,
                        properties: left_node_schema
                            .property_mappings
                            .iter()
                            .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                            .collect(),
                    }
                };
                let right = if *to_denormalized {
                    NodeAccessStrategy::EmbeddedInEdge {
                        edge_alias: rel_alias.to_string(),
                        properties: Self::extract_denorm_props(rel_schema, false),
                        is_from_node: false,
                    }
                } else {
                    NodeAccessStrategy::OwnTable {
                        table: right_node_schema.full_table_name(),
                        id_column: Self::resolve_id_column(right_node_schema, false)?,
                        properties: right_node_schema
                            .property_mappings
                            .iter()
                            .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                            .collect(),
                    }
                };
                (left, right)
            }
        };
        Ok(node_strategies)
    }

    /// Helper to build node strategy for a specific position
    fn node_strategy_for_position(
        node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
        rel_alias: &str,
        is_from_node: bool,
        edge_pattern: &EdgeTablePattern,
    ) -> Result<NodeAccessStrategy, String> {
        let is_denormalized = match edge_pattern {
            EdgeTablePattern::FullyDenormalized => true,
            EdgeTablePattern::Mixed {
                from_denormalized,
                to_denormalized,
            } => {
                if is_from_node {
                    *from_denormalized
                } else {
                    *to_denormalized
                }
            }
            EdgeTablePattern::Traditional => false,
        };

        if is_denormalized {
            Ok(NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: rel_alias.to_string(),
                properties: Self::extract_denorm_props(rel_schema, is_from_node),
                is_from_node,
            })
        } else {
            Ok(NodeAccessStrategy::OwnTable {
                table: node_schema.full_table_name(),
                id_column: node_schema
                    .node_id
                    .columns()
                    .first()
                    .ok_or_else(|| format!("Node schema for '{}' has no ID columns defined", node_schema.table_name))?
                    .to_string(),
                properties: node_schema
                    .property_mappings
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_sql_column_only()))
                    .collect(),
            })
        }
    }

    /// Extract denormalized properties from relationship schema
    /// Note: from_node_properties and to_node_properties are already String values (column names)
    fn extract_denorm_props(
        rel_schema: &RelationshipSchema,
        is_from_node: bool,
    ) -> PropertyMappings {
        let props = if is_from_node {
            &rel_schema.from_node_properties
        } else {
            &rel_schema.to_node_properties
        };

        props
            .as_ref()
            .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Determine join strategy based on edge pattern and coupling
    fn determine_join_strategy(
        edge_pattern: &EdgeTablePattern,
        rel_schema: &RelationshipSchema,
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        graph_schema: &GraphSchema,
        _rel_alias: &str,
        rel_types: &[String],
        prev_edge_info: Option<(&str, &str, bool)>,
    ) -> (JoinStrategy, Option<CoupledEdgeContext>) {
        // Check for coupled edges (same table, previous hop)
        if let Some((prev_alias, prev_type, is_from_node)) = prev_edge_info {
            if let Ok(prev_rel_schema) = graph_schema.get_rel_schema(prev_type) {
                // Check if same table
                if prev_rel_schema.full_table_name() == rel_schema.full_table_name() {
                    // Check if coupled via graph_schema
                    // Use first rel_type from the pattern, or default to "unknown"
                    let rel_type = rel_types.first().map(|s| s.as_str()).unwrap_or("unknown");
                    if graph_schema.are_edges_coupled(prev_type, rel_type) {
                        let coupled_ctx = CoupledEdgeContext {
                            prev_edge_alias: prev_alias.to_string(),
                            coupling_node_alias: prev_alias.to_string(), // simplified
                            shared_table: rel_schema.full_table_name(),
                        };
                        return (
                            JoinStrategy::CoupledSameRow {
                                unified_alias: prev_alias.to_string(),
                            },
                            Some(coupled_ctx),
                        );
                    } else {
                        // Multi-hop denormalized but NOT coupled - edge-to-edge JOIN
                        let prev_col = if is_from_node {
                            prev_rel_schema.from_id.clone()
                        } else {
                            prev_rel_schema.to_id.clone()
                        };
                        return (
                            JoinStrategy::EdgeToEdge {
                                prev_edge_alias: prev_alias.to_string(),
                                prev_edge_col: prev_col,
                                curr_edge_col: rel_schema.from_id.clone(),
                            },
                            None,
                        );
                    }
                }
            }
        }

        // Check for FK-edge pattern first (edge table IS a node table with FK column)
        if rel_schema.is_fk_edge {
            let is_self_referencing = rel_schema.from_node == rel_schema.to_node;

            // Determine which node needs to be JOINed (the one that ISN'T the edge table)
            // - If edge_table == to_node_table: edge IS right node, need to JOIN left (from_node)
            // - If edge_table == from_node_table: edge IS left node, need to JOIN right (to_node)
            let edge_table = rel_schema.full_table_name();
            let left_table = left_node_schema.full_table_name();
            let right_table = right_node_schema.full_table_name();

            let join_side = if edge_table == right_table {
                // Edge IS the to_node table, need to JOIN the from_node (left)
                NodePosition::Left
            } else if edge_table == left_table {
                // Edge IS the from_node table, need to JOIN the to_node (right)
                NodePosition::Right
            } else {
                // Shouldn't happen if is_fk_edge is set correctly
                NodePosition::Right // Default fallback
            };

            return (
                JoinStrategy::FkEdgeJoin {
                    from_id: rel_schema.from_id.clone(),
                    to_id: rel_schema.to_id.clone(),
                    join_side,
                    is_self_referencing,
                },
                None,
            );
        }

        // No coupling - determine by edge pattern
        match edge_pattern {
            EdgeTablePattern::FullyDenormalized => (
                JoinStrategy::SingleTableScan {
                    table: rel_schema.full_table_name(),
                },
                None,
            ),
            EdgeTablePattern::Traditional => (
                JoinStrategy::Traditional {
                    left_join_col: rel_schema.from_id.clone(),
                    right_join_col: rel_schema.to_id.clone(),
                },
                None,
            ),
            EdgeTablePattern::Mixed {
                from_denormalized,
                to_denormalized: _,
            } => {
                if *from_denormalized {
                    (
                        JoinStrategy::MixedAccess {
                            joined_node: NodePosition::Right,
                            join_col: rel_schema.to_id.clone(),
                        },
                        None,
                    )
                } else {
                    (
                        JoinStrategy::MixedAccess {
                            joined_node: NodePosition::Left,
                            join_col: rel_schema.from_id.clone(),
                        },
                        None,
                    )
                }
            }
        }
    }

    // ========================================================================
    // Convenience Methods
    // ========================================================================

    /// Returns true if this pattern is fully denormalized (no JOINs needed)
    pub fn is_fully_denormalized(&self) -> bool {
        matches!(self.join_strategy, JoinStrategy::SingleTableScan { .. })
    }

    /// Returns true if either node is polymorphic ($any)
    pub fn has_polymorphic_node(&self) -> bool {
        self.left_is_polymorphic || self.right_is_polymorphic
    }

    /// Returns true if this is a coupled edge pattern
    pub fn is_coupled(&self) -> bool {
        self.coupled_context.is_some()
    }

    /// Get a summary string for debugging
    pub fn debug_summary(&self) -> String {
        format!(
            "PatternSchema {{ left: {}, right: {}, edge: {}, strategy: {} }}",
            match &self.left_node {
                NodeAccessStrategy::OwnTable { .. } => "OwnTable",
                NodeAccessStrategy::EmbeddedInEdge { .. } => "Embedded",
                NodeAccessStrategy::Virtual { .. } => "Virtual",
            },
            match &self.right_node {
                NodeAccessStrategy::OwnTable { .. } => "OwnTable",
                NodeAccessStrategy::EmbeddedInEdge { .. } => "Embedded",
                NodeAccessStrategy::Virtual { .. } => "Virtual",
            },
            match &self.edge {
                EdgeAccessStrategy::SeparateTable { .. } => "SeparateTable",
                EdgeAccessStrategy::Polymorphic { .. } => "Polymorphic",
                EdgeAccessStrategy::FkEdge { .. } => "FkEdge",
            },
            self.join_strategy.description(),
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::graph_catalog::graph_schema::NodeIdSchema;

    fn make_test_node(_label: &str, table: &str, id_col: &str) -> NodeSchema {
        NodeSchema {
            database: "test_db".to_string(),
            table_name: table.to_string(),
            column_names: vec![id_col.to_string(), "name".to_string()],
            primary_keys: id_col.to_string(),
            node_id: NodeIdSchema::single(id_col.to_string(), "Int64".to_string()),
            property_mappings: HashMap::from([
                ("id".to_string(), PropertyValue::Column(id_col.to_string())),
                (
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
            ]),
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
        }
    }

    fn make_denormalized_node(_label: &str, table: &str, id_col: &str) -> NodeSchema {
        NodeSchema {
            database: "test_db".to_string(),
            table_name: table.to_string(),
            column_names: vec![id_col.to_string()],
            primary_keys: id_col.to_string(),
            node_id: NodeIdSchema::single(id_col.to_string(), "String".to_string()),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            denormalized_source_table: Some(format!("test_db.{}", table)),
            label_column: None,
            label_value: None,
            from_properties: Some(HashMap::from([("code".to_string(), "Origin".to_string())])),
            to_properties: Some(HashMap::from([("code".to_string(), "Dest".to_string())])),
        }
    }

    fn make_test_edge(_type_name: &str, table: &str) -> RelationshipSchema {
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: table.to_string(),
            column_names: vec!["from_id".to_string(), "to_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            from_node_id_dtype: "Int64".to_string(),
            to_node_id_dtype: "Int64".to_string(),
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
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
        }
    }

    fn make_denormalized_edge(_type_name: &str, table: &str) -> RelationshipSchema {
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: table.to_string(),
            column_names: vec![
                "Origin".to_string(),
                "Dest".to_string(),
                "OriginCity".to_string(),
                "DestCity".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: "Origin".to_string(),
            to_id: "Dest".to_string(),
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
            from_label_values: None,
            to_label_values: None,
            from_node_properties: Some(HashMap::from([
                ("code".to_string(), "Origin".to_string()),
                ("city".to_string(), "OriginCity".to_string()),
            ])),
            to_node_properties: Some(HashMap::from([
                ("code".to_string(), "Dest".to_string()),
                ("city".to_string(), "DestCity".to_string()),
            ])),
            is_fk_edge: false,
        }
    }

    #[test]
    fn test_node_access_strategy_requires_join() {
        let own_table = NodeAccessStrategy::OwnTable {
            table: "users".to_string(),
            id_column: "id".to_string(),
            properties: HashMap::new(),
        };
        assert!(own_table.requires_join());

        let embedded = NodeAccessStrategy::EmbeddedInEdge {
            edge_alias: "r".to_string(),
            properties: HashMap::new(),
            is_from_node: true,
        };
        assert!(!embedded.requires_join());

        let virtual_node = NodeAccessStrategy::Virtual {
            label: "User".to_string(),
        };
        assert!(!virtual_node.requires_join());
    }

    #[test]
    fn test_edge_access_strategy_type_filter() {
        let single_type = EdgeAccessStrategy::Polymorphic {
            table: "interactions".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            type_column: Some("interaction_type".to_string()),
            type_values: vec!["FOLLOWS".to_string()],
            from_label_column: None,
            to_label_column: None,
            properties: HashMap::new(),
        };
        assert_eq!(
            single_type.get_type_filter("r"),
            Some("r.interaction_type = 'FOLLOWS'".to_string())
        );

        let multi_type = EdgeAccessStrategy::Polymorphic {
            table: "interactions".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            type_column: Some("interaction_type".to_string()),
            type_values: vec!["FOLLOWS".to_string(), "LIKES".to_string()],
            from_label_column: None,
            to_label_column: None,
            properties: HashMap::new(),
        };
        assert_eq!(
            multi_type.get_type_filter("r"),
            Some("r.interaction_type IN ('FOLLOWS', 'LIKES')".to_string())
        );

        let separate = EdgeAccessStrategy::SeparateTable {
            table: "follows".to_string(),
            from_id: "follower_id".to_string(),
            to_id: "followed_id".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(separate.get_type_filter("r"), None);
    }

    #[test]
    fn test_edge_access_strategy_label_filter() {
        // Test from_label_column only (MEMBER_OF with User|Group on left)
        let from_only = EdgeAccessStrategy::Polymorphic {
            table: "memberships".to_string(),
            from_id: "member_id".to_string(),
            to_id: "group_id".to_string(),
            type_column: None, // No type discriminator
            type_values: vec![],
            from_label_column: Some("member_type".to_string()),
            to_label_column: None,
            properties: HashMap::new(),
        };
        // User on left side
        assert_eq!(
            from_only.get_label_filter("r", "User", "Group"),
            Some("r.member_type = 'User'".to_string())
        );
        // Group on left side
        assert_eq!(
            from_only.get_label_filter("r", "Group", "Group"),
            Some("r.member_type = 'Group'".to_string())
        );

        // Test to_label_column only (CONTAINS with Folder|File on right)
        let to_only = EdgeAccessStrategy::Polymorphic {
            table: "fs_contents".to_string(),
            from_id: "parent_id".to_string(),
            to_id: "child_id".to_string(),
            type_column: None,
            type_values: vec![],
            from_label_column: None,
            to_label_column: Some("child_type".to_string()),
            properties: HashMap::new(),
        };
        // Folder on right
        assert_eq!(
            to_only.get_label_filter("r", "Folder", "Folder"),
            Some("r.child_type = 'Folder'".to_string())
        );
        // File on right
        assert_eq!(
            to_only.get_label_filter("r", "Folder", "File"),
            Some("r.child_type = 'File'".to_string())
        );

        // Test both label columns (HAS_ACCESS: User|Group -> Folder|File)
        let both = EdgeAccessStrategy::Polymorphic {
            table: "permissions".to_string(),
            from_id: "subject_id".to_string(),
            to_id: "object_id".to_string(),
            type_column: None,
            type_values: vec![],
            from_label_column: Some("subject_type".to_string()),
            to_label_column: Some("object_type".to_string()),
            properties: HashMap::new(),
        };
        // User -> Folder
        assert_eq!(
            both.get_label_filter("r", "User", "Folder"),
            Some("r.subject_type = 'User' AND r.object_type = 'Folder'".to_string())
        );
        // Group -> File
        assert_eq!(
            both.get_label_filter("r", "Group", "File"),
            Some("r.subject_type = 'Group' AND r.object_type = 'File'".to_string())
        );

        // Test with type_column + label columns (full polymorphic)
        let full = EdgeAccessStrategy::Polymorphic {
            table: "interactions".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            type_column: Some("interaction_type".to_string()),
            type_values: vec!["FOLLOWS".to_string()],
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            properties: HashMap::new(),
        };
        // Label filter should still work (type filter is separate)
        assert_eq!(
            full.get_label_filter("r", "User", "User"),
            Some("r.from_type = 'User' AND r.to_type = 'User'".to_string())
        );
        // And type filter should also work
        assert_eq!(
            full.get_type_filter("r"),
            Some("r.interaction_type = 'FOLLOWS'".to_string())
        );

        // Test no label columns (separate table - no label filtering needed)
        let separate = EdgeAccessStrategy::SeparateTable {
            table: "follows".to_string(),
            from_id: "follower_id".to_string(),
            to_id: "followed_id".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(separate.get_label_filter("r", "User", "User"), None);

        // Test Polymorphic with no label columns (only type_column)
        let type_only = EdgeAccessStrategy::Polymorphic {
            table: "interactions".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            type_column: Some("interaction_type".to_string()),
            type_values: vec!["FOLLOWS".to_string()],
            from_label_column: None,
            to_label_column: None,
            properties: HashMap::new(),
        };
        assert_eq!(type_only.get_label_filter("r", "User", "User"), None);
    }

    #[test]
    fn test_edge_access_strategy_type_filter_with_none_type_column() {
        // Regression test: Polymorphic with type_column = None should return None for type filter
        // This is the case for edges like MEMBER_OF that only have from_label_column
        let label_only = EdgeAccessStrategy::Polymorphic {
            table: "memberships".to_string(),
            from_id: "member_id".to_string(),
            to_id: "group_id".to_string(),
            type_column: None, // <-- This was the bug: type_column can be None
            type_values: vec![],
            from_label_column: Some("member_type".to_string()),
            to_label_column: None,
            properties: HashMap::new(),
        };
        // Should NOT panic, should return None
        assert_eq!(label_only.get_type_filter("r"), None);
    }

    #[test]
    fn test_join_strategy_is_joinless() {
        let single_table = JoinStrategy::SingleTableScan {
            table: "flights".to_string(),
        };
        assert!(single_table.is_joinless());

        let coupled = JoinStrategy::CoupledSameRow {
            unified_alias: "r1".to_string(),
        };
        assert!(coupled.is_joinless());

        let traditional = JoinStrategy::Traditional {
            left_join_col: "from_id".to_string(),
            right_join_col: "to_id".to_string(),
        };
        assert!(!traditional.is_joinless());
    }
}
