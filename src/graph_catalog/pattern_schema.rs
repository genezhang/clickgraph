//! Pattern Schema Context
//!
//! The layout of one relationship pattern's schema (`PatternSchemaContext`),
//! computed once: how each node and the edge are stored and how they join.
//! `bound_plan::lower::path` walks the standard layout (each in its own
//! table) and refuses the others by this classification, not by raw schema
//! flags (CLAUDE.md rule 7).

use super::config::Identifier;
use super::graph_schema::{
    classify_edge_table_pattern, EdgeTablePattern, NodeSchema, RelationshipSchema,
};

/// How to access a node's data in a graph pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeAccessStrategy {
    /// Node has its own dedicated table, JOIN required to access properties.
    ///
    /// Example: `users` table for `:User` nodes, separate from `follows` edge table.
    OwnTable {
        /// Fully qualified table name (e.g., "brahmand.users")
        table: String,
        /// Primary ID column(s) for JOIN condition — single or composite
        id_column: Identifier,
    },

    /// Node properties are embedded in the edge table, no JOIN needed.
    ///
    /// Example: OnTime flights where Airport code/city/state are columns in flights table.
    EmbeddedInEdge,
}

/// How to access an edge's data.
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
    },

    /// Polymorphic edge table with type or label discriminator columns.
    ///
    /// Example: `interactions` table with `interaction_type` column.
    Polymorphic,
}

/// How a pattern's node and edge tables join.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    /// Traditional pattern: separate node and edge tables requiring JOINs.
    ///
    /// ```sql
    /// SELECT ... FROM users u
    /// JOIN follows r ON r.follower_id = u.user_id
    /// JOIN users u2 ON u2.user_id = r.followed_id
    /// ```
    Traditional,

    /// Fully denormalized: single table scan, no JOINs needed.
    ///
    /// ```sql
    /// SELECT Origin, Dest, ... FROM flights
    /// ```
    SingleTableScan,

    /// One node embedded in the edge table, the other on its own table.
    MixedAccess,

    /// FK-edge: the edge table IS a node table, its FK column naming the
    /// other node.
    FkEdgeJoin,
}

/// Complete schema context for a single graph pattern (edge + two nodes).
#[derive(Debug, Clone)]
pub struct PatternSchemaContext {
    /// Query alias for the left (source/from) node (e.g., "a", "user1")
    pub left_node_alias: String,
    /// Query alias for the right (target/to) node (e.g., "b", "user2")
    pub right_node_alias: String,
    /// Access strategy for the left (source/from) node
    pub left_node: NodeAccessStrategy,
    /// Access strategy for the right (target/to) node
    pub right_node: NodeAccessStrategy,
    /// Access strategy for the edge
    pub edge: EdgeAccessStrategy,
    /// How to generate JOINs for this pattern
    pub join_strategy: JoinStrategy,
}

impl PatternSchemaContext {
    /// Analyze a graph pattern and produce its schema context.
    ///
    /// # Arguments
    ///
    /// * `left_node_alias` - Query alias for the left/from node (e.g., "a")
    /// * `right_node_alias` - Query alias for the right/to node (e.g., "b")
    /// * `left_node_schema` - Schema for the left/from node
    /// * `right_node_schema` - Schema for the right/to node
    /// * `rel_schema` - Schema for the relationship/edge
    pub fn analyze(
        left_node_alias: &str,
        right_node_alias: &str,
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
    ) -> Result<Self, String> {
        let edge_pattern =
            classify_edge_table_pattern(left_node_schema, rel_schema, right_node_schema);
        let edge = Self::build_edge_strategy(rel_schema);
        let (left_node, right_node) =
            Self::build_node_strategies(left_node_schema, right_node_schema, &edge_pattern)?;
        let join_strategy = Self::determine_join_strategy(&edge_pattern, rel_schema);
        Ok(PatternSchemaContext {
            left_node_alias: left_node_alias.to_string(),
            right_node_alias: right_node_alias.to_string(),
            left_node,
            right_node,
            edge,
            join_strategy,
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
    fn resolve_id_column(
        node_schema: &NodeSchema,
        is_from_node: bool,
    ) -> Result<Identifier, String> {
        let id_columns = node_schema.node_id.columns();
        if id_columns.is_empty() {
            return Err("Node schema has no ID columns defined".to_string());
        }

        // Try to resolve through from_properties or to_properties (for denormalized edges)
        let node_props_opt = if is_from_node {
            &node_schema.from_properties
        } else {
            &node_schema.to_properties
        };

        if let Some(node_props) = node_props_opt {
            // For denormalized edges, resolve each ID column through property mapping
            let first_prop = id_columns[0];
            if let Some(column_name) = node_props.get(first_prop) {
                log::info!(
                    "🔧 resolve_id_column: '{}' (Cypher) → '{}' (DB column) via {} for table {}",
                    first_prop,
                    column_name,
                    if is_from_node {
                        "from_properties"
                    } else {
                        "to_properties"
                    },
                    node_schema.table_name
                );
                // For denormalized edges, composite node IDs are not expected
                return Ok(Identifier::Single(column_name.clone()));
            }
        }

        // Standalone node table: resolve each ID column through property_mappings
        let resolved: Vec<String> = id_columns
            .iter()
            .map(|col| {
                if let Some(property_value) = node_schema.property_mappings.get(*col) {
                    property_value.raw().to_string()
                } else {
                    col.to_string()
                }
            })
            .collect();

        log::info!(
            "🔧 resolve_id_column: {:?} → {:?} for table {}",
            id_columns,
            resolved,
            node_schema.table_name
        );

        if resolved.len() == 1 {
            Ok(Identifier::Single(resolved.into_iter().next().unwrap()))
        } else {
            Ok(Identifier::Composite(resolved))
        }
    }

    /// Build edge access strategy from relationship schema
    fn build_edge_strategy(rel_schema: &RelationshipSchema) -> EdgeAccessStrategy {
        // Polymorphic: has a type_column, from_label_column or to_label_column
        let has_type_column = rel_schema.type_column.is_some();
        let has_label_columns =
            rel_schema.from_label_column.is_some() || rel_schema.to_label_column.is_some();

        if has_type_column || has_label_columns {
            EdgeAccessStrategy::Polymorphic
        } else {
            EdgeAccessStrategy::SeparateTable {
                table: rel_schema.full_table_name(),
                from_id: rel_schema.from_id.to_string(),
                to_id: rel_schema.to_id.to_string(),
            }
        }
    }

    /// Build node access strategies based on edge table pattern (from the
    /// actual node schemas, not the edge's polymorphic `$any` endpoints).
    fn build_node_strategies(
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        edge_pattern: &EdgeTablePattern,
    ) -> Result<(NodeAccessStrategy, NodeAccessStrategy), String> {
        let own_table = |node_schema: &NodeSchema, is_from_node: bool| {
            Ok::<_, String>(NodeAccessStrategy::OwnTable {
                table: node_schema.full_table_name(),
                id_column: Self::resolve_id_column(node_schema, is_from_node)?,
            })
        };
        Ok(match edge_pattern {
            EdgeTablePattern::FullyDenormalized => (
                NodeAccessStrategy::EmbeddedInEdge,
                NodeAccessStrategy::EmbeddedInEdge,
            ),
            EdgeTablePattern::Traditional => (
                own_table(left_node_schema, true)?,
                own_table(right_node_schema, false)?,
            ),
            EdgeTablePattern::Mixed {
                from_denormalized,
                to_denormalized,
            } => {
                let left = if *from_denormalized {
                    NodeAccessStrategy::EmbeddedInEdge
                } else {
                    own_table(left_node_schema, true)?
                };
                let right = if *to_denormalized {
                    NodeAccessStrategy::EmbeddedInEdge
                } else {
                    own_table(right_node_schema, false)?
                };
                (left, right)
            }
        })
    }

    /// Determine join strategy based on edge pattern
    fn determine_join_strategy(
        edge_pattern: &EdgeTablePattern,
        rel_schema: &RelationshipSchema,
    ) -> JoinStrategy {
        // FK-edge pattern first (edge table IS a node table with FK column)
        if rel_schema.is_fk_edge {
            return JoinStrategy::FkEdgeJoin;
        }
        match edge_pattern {
            EdgeTablePattern::FullyDenormalized => JoinStrategy::SingleTableScan,
            EdgeTablePattern::Traditional => JoinStrategy::Traditional,
            EdgeTablePattern::Mixed { .. } => JoinStrategy::MixedAccess,
        }
    }
}
