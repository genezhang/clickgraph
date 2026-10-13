//! CTE generation utilities for variable-length path queries

use std::collections::HashMap;

use crate::clickhouse_query_generator::variable_length_cte::WeightCteConfig;
use crate::clickhouse_query_generator::NodeProperty;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_plan::ShortestPathMode;
use crate::query_planner::logical_plan::VariableLengthSpec;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
pub struct CteGenerationContext {
    /// Graph schema for this query (enables multi-schema support)
    schema: Option<GraphSchema>,
    /// Variable length specification for the path pattern
    pub spec: VariableLengthSpec,
    /// Path variable name (e.g., "p" in `MATCH p = (a)-[*]->(b)`)
    pub path_variable: Option<String>,
    /// Shortest path mode for shortestPath() and allShortestPaths()
    pub shortest_path_mode: Option<ShortestPathMode>,
    /// Relationship types for the VLP pattern (for polymorphic edges)
    pub relationship_types: Option<Vec<String>>,
    /// Edge ID identifier from schema (for RETURN relationships(p))
    pub edge_id: Option<Identifier>,
    /// Relationship Cypher alias (e.g., "r" in `(a)-[r*]->(b)`)
    pub relationship_cypher_alias: Option<String>,
    /// Start node label (for polymorphic heterogeneous paths)
    pub start_node_label: Option<String>,
    /// End node label (for polymorphic heterogeneous paths)
    pub end_node_label: Option<String>,
    /// Whether this VLP is optional (affects start node filter handling)
    pub is_optional: bool,

    /// **NEW (Feb 2026)**: Multi-type pattern combinations for UNION generation
    /// Map: `(from_alias, to_alias) → Vec<TypeCombination>`
    /// When set, CTE generation creates UNION of all pattern combinations
    pub pattern_combinations:
        Option<HashMap<(String, String), Vec<crate::query_planner::logical_plan::TypeCombination>>>,

    /// Weighted shortest path: pre-computed edge weight CTE configuration
    pub weight_cte: Option<WeightCteConfig>,
    /// Whether the query uses `relationships(path)`. When false, VLP CTE skips
    /// growing path_relationships arrays, saving significant memory.
    pub needs_path_relationships: bool,
    /// Lightweight BFS mode for shortestPath queries that only need length(path).
    /// Generates a global-visited-set BFS instead of per-path tracking.
    pub use_bfs_mode: bool,
    /// True when the original edge direction is Either (undirected).
    /// BFS mode generates two UNION ALL branches for both traversal directions.
    pub is_undirected: bool,
    /// #617: true when this undirected VLP was normalized by the analyzer to a
    /// SINGLE directed walk over a doubled-edge set (instead of the legacy
    /// two-monotone-arm Union split). Distinct from `is_undirected`, which is
    /// also true for the individual arms of a legacy split.
    pub undirected_single_walk: bool,
    /// #1077: per-path-node properties requested via `[n IN nodes(p) | n.<prop>]`.
    /// Threaded to the VLP generator, which accumulates a parallel `path_<prop>`
    /// array per entry. Empty unless the query reads path-node properties.
    pub path_node_properties: Vec<NodeProperty>,
    /// The bound plan's paths as values (`PathValues`): each path's nodes
    /// and relationships accumulated as `path_node_values` /
    /// `path_rel_values`. Empty for the legacy pipeline.
    pub path_values: crate::clickhouse_query_generator::variable_length_cte::PathValues,
    /// The relation the walk reads in place of the edge table (the bound
    /// plan's undirected relationship: its table in both directions, with
    /// the walked columns named in the pattern context). `None` for the
    /// legacy pipeline.
    pub walk_relation: Option<String>,
    /// The bound plan's walks between composite ids: each step joins the
    /// relationship to the path's last node column by column (the path's
    /// `end_<col>` components), not by the ids' pipe-joined texts, which two
    /// ids can share (`('B|1', '2')`, `('B', '1|2')`). `false` for the legacy
    /// pipeline.
    pub composite_by_columns: bool,
    /// Root plan reference for checking path variable usage across the entire query.
    /// Set at the top-level to_render_plan call so VLP extraction can check if path
    /// variables are used bare (preventing BFS optimization).
    pub root_plan: Option<std::sync::Arc<LogicalPlan>>,
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_spec(mut self, spec: VariableLengthSpec) -> Self {
        self.spec = spec;
        self
    }

    /// Set relationship types (for polymorphic edges)
    pub(crate) fn with_relationship_types(mut self, types: Option<Vec<String>>) -> Self {
        self.relationship_types = types;
        self
    }

    /// Set edge ID from schema
    pub(crate) fn with_edge_id(mut self, edge_id: Option<Identifier>) -> Self {
        self.edge_id = edge_id;
        self
    }

    /// Set relationship Cypher alias
    pub(crate) fn with_relationship_cypher_alias(mut self, alias: Option<String>) -> Self {
        self.relationship_cypher_alias = alias;
        self
    }

    /// Set node labels for polymorphic heterogeneous paths
    pub(crate) fn with_node_labels(
        mut self,
        start_label: Option<String>,
        end_label: Option<String>,
    ) -> Self {
        self.start_node_label = start_label;
        self.end_node_label = end_label;
        self
    }

    /// Set the graph schema (builder pattern)
    pub(crate) fn with_schema_owned(mut self, schema: GraphSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub(crate) fn schema(&self) -> Option<&GraphSchema> {
        self.schema.as_ref()
    }
}

/// Schema-aware property mapping with relationship context
/// Checks denormalized properties first, then falls back to node properties
/// Indicates whether a node is on the FROM (left) or TO (right) side of a relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Node is on the FROM side (left_connection in GraphRel)
    From,
    /// Node is on the TO side (right_connection in GraphRel)
    To,
}

pub fn map_property_to_column_with_relationship_context(
    property: &str,
    node_label: &str,
    relationship_type: Option<&str>,
    node_role: Option<NodeRole>,
    _schema_name: Option<&str>,
) -> Result<String, String> {
    use std::fs::OpenOptions;
    use std::io::Write;

    // Get schema from task-local context (set at query entry),
    // falling back to GLOBAL_SCHEMAS for backward compatibility (tests).
    let current_schema = crate::server::query_context::get_current_schema_with_fallback();
    let schema = if let Some(ref s) = current_schema {
        s.as_ref()
    } else {
        return Err(format!(
            "No schema available for property '{}' on node '{}'",
            property, node_label
        ));
    };

    log::info!(
        "🔍 map_property_to_column_with_relationship_context: property='{}', node_label='{}'",
        property,
        node_label,
    );

    // Get the node schema first
    let node_schema = schema.all_node_schemas().get(node_label).ok_or_else(|| {
        let available: Vec<String> = schema.all_node_schemas().keys().cloned().collect();
        let msg = format!(
            "Node label '{}' not found in schema. Available labels: {}",
            node_label,
            available.join(", ")
        );
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    // 🆕 DENORMALIZED NODE: Check node-level denormalized properties FIRST
    // ✅ PHASE 2 APPROVED: Queries schema configuration, not plan flags
    if node_schema.is_denormalized {
        if let Some(rel_type) = relationship_type {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                // Foreign-edge (id-via-FK) guard: when the node is denormalized on
                // a DIFFERENT table than this edge, its node-level from/to_properties
                // point at columns of its own source table (e.g. flights.Origin),
                // which do NOT exist in the edge table. Skip the node-level mapping
                // and fall through to the edge-level branch below, which carries the
                // correct {node_id → edge FK column} mapping for the foreign edge.
                let coupled_on_this_edge = node_schema.denormalized_source_table.as_deref()
                    == Some(rel_schema.full_table_name().as_str());
                if coupled_on_this_edge {
                    // Use the caller-provided role to determine which property map to use
                    match node_role {
                        Some(NodeRole::From) => {
                            // Node is on the FROM side - use from_properties
                            if let Some(from_props) = &node_schema.from_properties {
                                if let Some(column) = from_props.get(property) {
                                    return Ok(column.clone());
                                }
                            }
                        }
                        Some(NodeRole::To) => {
                            // Node is on the TO side - use to_properties
                            if let Some(to_props) = &node_schema.to_properties {
                                if let Some(column) = to_props.get(property) {
                                    return Ok(column.clone());
                                }
                            }
                        }
                        None => {
                            // Fallback: try to infer from schema (works when labels differ)
                            if rel_schema.from_node == node_label {
                                if let Some(from_props) = &node_schema.from_properties {
                                    if let Some(column) = from_props.get(property) {
                                        return Ok(column.clone());
                                    }
                                }
                            }
                            if rel_schema.to_node == node_label {
                                if let Some(to_props) = &node_schema.to_properties {
                                    if let Some(column) = to_props.get(property) {
                                        return Ok(column.clone());
                                    }
                                }
                            }
                        }
                    }
                } // end if coupled_on_this_edge
            }
        } else {
            // No relationship context (standalone node scan).
            // Try from_properties first, then to_properties as fallback.
            // This handles coupled schemas where nodes have no property_mappings
            // but store column mappings in from/to_node_properties.
            if let Some(from_props) = &node_schema.from_properties {
                if let Some(column) = from_props.get(property) {
                    log::info!(
                        "✓ Denormalized standalone node: {}.{} → {} (from_properties)",
                        node_label,
                        property,
                        column
                    );
                    return Ok(column.clone());
                }
            }
            if let Some(to_props) = &node_schema.to_properties {
                if let Some(column) = to_props.get(property) {
                    log::info!(
                        "✓ Denormalized standalone node: {}.{} → {} (to_properties)",
                        node_label,
                        property,
                        column
                    );
                    return Ok(column.clone());
                }
            }
        }
    }

    // 🆕 DENORMALIZED EDGE: Check edge-level denormalized properties (for backward compatibility)
    if let Some(rel_type) = relationship_type {
        if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
            // Use the caller-provided role to determine which property map to use
            match node_role {
                Some(NodeRole::From) => {
                    // Node is on the FROM side - use from_node_properties
                    if let Some(from_props) = &rel_schema.from_node_properties {
                        if let Some(column) = from_props.get(property) {
                            return Ok(column.clone());
                        }
                    }
                }
                Some(NodeRole::To) => {
                    // Node is on the TO side - use to_node_properties
                    if let Some(to_props) = &rel_schema.to_node_properties {
                        if let Some(column) = to_props.get(property) {
                            return Ok(column.clone());
                        }
                    }
                }
                None => {
                    // Fallback: try to infer from schema (works when labels differ)
                    if rel_schema.from_node == node_label {
                        if let Some(from_props) = &rel_schema.from_node_properties {
                            if let Some(column) = from_props.get(property) {
                                return Ok(column.clone());
                            }
                        }
                    }
                    if rel_schema.to_node == node_label {
                        if let Some(to_props) = &rel_schema.to_node_properties {
                            if let Some(column) = to_props.get(property) {
                                return Ok(column.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    // Fall back to traditional node property mapping

    let column = node_schema.property_mappings.get(property).ok_or_else(|| {
        // Sorted so the error text is stable across processes (HashMap keys).
        let mut available: Vec<String> = node_schema.property_mappings.keys().cloned().collect();
        available.sort();
        let msg = format!(
            "Property '{}' not found for node label '{}'. Available properties: {}",
            property,
            node_label,
            available.join(", ")
        );
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    Ok(column.raw().to_string())
}
