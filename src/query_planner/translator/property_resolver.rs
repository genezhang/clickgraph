//! Unified property resolution for graph-to-SQL translation
//!
//! This module handles the translation of graph properties to SQL columns,
//! supporting three schema patterns:
//! 1. **Standard**: Separate node and edge tables with property_mappings
//! 2. **Denormalized**: Virtual nodes stored on edge tables with from/to_node_properties
//! 3. **Polymorphic**: Type discriminators with type_column and type filtering
//!
//! Key Design Principle: Single unified component for all property resolution,
//! eliminating ad-hoc property mapping scattered across different code locations.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::errors::QueryPlannerError;
use crate::query_planner::logical_plan::ViewScan;
use std::collections::HashMap;

/// Position of a node in a relationship (for denormalized patterns)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodePosition {
    /// Node is the source (FROM) in a relationship
    From,
    /// Node is the target (TO) in a relationship
    To,
    /// Node is standalone (not in relationship context)
    /// Used for standard and polymorphic patterns
    Standalone,
}

/// Maps a graph alias to SQL alias and metadata
///
/// CRITICAL: For denormalized patterns, the same node alias can appear in
/// multiple edges with different roles! We track the edge_alias to distinguish:
///
/// Example: MATCH (a)-[f]->(b)-[g]->(c)
///   Node 'b' has TWO mappings:
///   - (b, f) → TO position, maps b.city → f.DestCityName
///   - (b, g) → FROM position, maps b.city → g.OriginCityName
#[derive(Debug, Clone)]
pub struct AliasMapping {
    /// SQL table alias (e.g., "f", "u1", "flights_cte")
    pub sql_alias: String,

    /// Position in relationship (for denormalized nodes)
    pub position: NodePosition,

    /// Is this a denormalized node?
    pub is_denormalized: bool,

    /// Is this a polymorphic edge?
    pub is_polymorphic: bool,

    /// Edge context (for denormalized multi-hop):
    /// Which edge is this node part of?
    /// Required to distinguish roles when same node appears in multiple edges
    pub edge_alias: Option<String>,

    /// Type filters for polymorphic edges
    /// Example: ["interaction_type = 'FOLLOWS'", "from_type = 'User'"]
    pub type_filters: Vec<String>,
}

/// Result of property resolution
#[derive(Debug, Clone)]
pub struct PropertyResolution {
    /// SQL table alias to use
    pub table_alias: String,

    /// Resolved property value (column or expression)
    pub property_value: PropertyValue,

    /// Additional WHERE conditions (for polymorphic patterns)
    pub type_filters: Vec<String>,

    /// Original graph alias (for debugging)
    pub graph_alias: String,

    /// Original property name (for debugging)
    pub property_name: String,
}

/// Resolves graph properties to SQL columns with unified alias resolution
///
/// This is the SINGLE UNIFIED COMPONENT for graph→SQL translation.
/// All property mapping and alias resolution flows through here.
///
/// Supports three schema patterns:
/// 1. Standard: property_mappings (e.g., name → username)
/// 2. Denormalized: from/to_node_properties (e.g., city → OriginCity vs DestCity)
/// 3. Polymorphic: property_mappings + type_column filters
pub struct PropertyResolver {
    /// ViewScan metadata for each graph alias
    /// Key: graph alias (e.g., "a", "u", "f")
    view_scans: HashMap<String, ViewScan>,

    /// Graph-to-SQL alias mapping
    ///
    /// Key: (node_alias, edge_alias_opt) - composite key for denormalized multi-hop
    /// Value: Vec<AliasMapping> to support multiple roles for same node
    /// Examples:
    ///
    /// - Standard: "u" → [AliasMapping{sql_alias: "u1", ...}]
    /// - Denormalized single-hop: "a" → [AliasMapping{sql_alias: "f", edge: Some("f"), ...}]
    /// - Denormalized multi-hop: "b" → `[AliasMapping{sql_alias: "f", edge: Some("f"), ...}, ...]`
    alias_mappings: HashMap<String, Vec<AliasMapping>>,
}

impl PropertyResolver {
    /// Create a new PropertyResolver
    pub fn new() -> Self {
        PropertyResolver {
            view_scans: HashMap::new(),
            alias_mappings: HashMap::new(),
        }
    }

    /// Register a ViewScan for a graph alias
    ///
    /// This associates graph concepts (node/edge aliases) with SQL metadata
    /// (table names, property mappings, denormalized flags, etc.)
    pub fn register_view_scan(&mut self, graph_alias: String, view_scan: ViewScan) {
        self.view_scans.insert(graph_alias, view_scan);
    }

    /// Register an alias mapping (graph alias → SQL alias)
    ///
    /// For denormalized patterns, call this MULTIPLE TIMES for the same node
    /// if it appears in multiple edges with different roles.
    ///
    /// # Arguments
    /// * `graph_alias` - Graph alias (e.g., "a", "u", "b")
    /// * `mapping` - AliasMapping with SQL alias, position, edge context
    ///
    /// # Example (Denormalized Multi-Hop)
    /// ```text
    /// // MATCH (a)-[f]->(b)-[g]->(c)
    /// resolver.register_alias("a", AliasMapping {
    ///     sql_alias: "f", position: From, edge_alias: Some("f"), ...
    /// });
    /// resolver.register_alias("b", AliasMapping {
    ///     sql_alias: "f", position: To, edge_alias: Some("f"), ...
    /// });
    /// resolver.register_alias("b", AliasMapping {
    ///     sql_alias: "g", position: From, edge_alias: Some("g"), ...
    /// });
    /// resolver.register_alias("c", AliasMapping {
    ///     sql_alias: "g", position: To, edge_alias: Some("g"), ...
    /// });
    /// ```
    pub fn register_alias(&mut self, graph_alias: String, mapping: AliasMapping) {
        self.alias_mappings
            .entry(graph_alias)
            .or_default()
            .push(mapping);
    }

    /// Resolve a graph property to SQL column AND alias in one step
    ///
    /// This is the PRIMARY METHOD for property resolution throughout the codebase.
    ///
    /// # Arguments
    /// * `graph_alias` - Graph alias (e.g., "a", "u", "f")
    /// * `property` - Property name (e.g., "city", "name", "distance")
    /// * `edge_context` - Optional edge alias for denormalized multi-hop resolution
    ///   Required when same node appears in multiple edges
    ///
    /// # Returns
    /// PropertyResolution with:
    /// - table_alias: SQL alias to use (resolved from graph alias + edge context)
    /// - property_value: PropertyValue::Column or PropertyValue::Expression
    /// - type_filters: Additional WHERE conditions (for polymorphic)
    ///
    /// # Example (Standard Pattern)
    /// ```text
    /// // MATCH (u:User) WHERE u.name = 'Alice'
    /// resolve_property("u", "name", None)
    /// → PropertyResolution {
    ///     table_alias: "u1",
    ///     property_value: Column("username"),  // from property_mappings
    ///     type_filters: [],
    ///   }
    /// ```
    ///
    /// # Example (Denormalized Single-Hop)
    /// ```text
    /// // MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) WHERE a.city = 'LAX'
    /// resolve_property("a", "city", Some("f"))
    /// → PropertyResolution {
    ///     table_alias: "f",
    ///     property_value: Column("OriginCityName"),  // from from_node_properties
    ///     type_filters: [],
    ///   }
    /// ```
    ///
    /// # Example (Denormalized Multi-Hop - Role Matters!)
    /// ```text
    /// // MATCH (a)-[f]->(b)-[g]->(c) WHERE b.city = 'NYC'
    ///
    /// resolve_property("b", "city", Some("f"))  // b as TO in edge f
    /// → PropertyResolution {
    ///     table_alias: "f",
    ///     property_value: Column("DestCityName"),
    ///     ...
    ///   }
    ///
    /// resolve_property("b", "city", Some("g"))  // b as FROM in edge g
    /// → PropertyResolution {
    ///     table_alias: "g",
    ///     property_value: Column("OriginCityName"),
    ///     ...
    ///   }
    /// ```
    ///
    /// # Example (Polymorphic)
    /// ```text
    /// // MATCH (u:User)-[i:FOLLOWS]->(v:User)
    /// resolve_property("i", "follow_date", None)
    /// → PropertyResolution {
    ///     table_alias: "i",
    ///     property_value: Column("interaction_date"),
    ///     type_filters: ["interaction_type = 'FOLLOWS'"],
    ///   }
    /// ```
    pub fn resolve_property(
        &self,
        graph_alias: &str,
        property: &str,
        edge_context: Option<&str>,
    ) -> Result<PropertyResolution, QueryPlannerError> {
        // 1. Look up ViewScan for this graph alias
        let view_scan = self.view_scans.get(graph_alias).ok_or_else(|| {
            QueryPlannerError::InvalidQuery(format!(
                "No ViewScan registered for graph alias '{}'",
                graph_alias
            ))
        })?;

        // 2. Find the appropriate alias mapping
        let mappings = self.alias_mappings.get(graph_alias).ok_or_else(|| {
            QueryPlannerError::InvalidQuery(format!(
                "No alias mapping found for graph alias '{}'",
                graph_alias
            ))
        })?;

        // 3. Select the correct mapping based on edge context (for denormalized multi-hop)
        let mapping = if view_scan.is_denormalized && edge_context.is_some() {
            // Denormalized multi-hop: Find mapping with matching edge_alias
            mappings
                .iter()
                .find(|m| m.edge_alias.as_deref() == edge_context)
                .ok_or_else(|| {
                    QueryPlannerError::InvalidQuery(format!(
                        "No alias mapping found for node '{}' in edge context '{:?}'",
                        graph_alias, edge_context
                    ))
                })?
        } else {
            // Standard/polymorphic or denormalized single-hop: Use first (only) mapping
            mappings.first().ok_or_else(|| {
                QueryPlannerError::InvalidQuery(format!(
                    "Empty alias mapping list for graph alias '{}'",
                    graph_alias
                ))
            })?
        };

        // 4. Resolve property based on schema pattern
        let property_value = if view_scan.is_denormalized {
            // Denormalized pattern: Use from/to_node_properties based on position
            self.resolve_denormalized_property(view_scan, property, mapping.position)?
        } else if mapping.is_polymorphic {
            // Polymorphic pattern: Use standard property_mappings
            self.resolve_standard_property(view_scan, property)?
        } else {
            // Standard pattern: Use property_mappings
            self.resolve_standard_property(view_scan, property)?
        };

        Ok(PropertyResolution {
            table_alias: mapping.sql_alias.clone(),
            property_value,
            type_filters: mapping.type_filters.clone(),
            graph_alias: graph_alias.to_string(),
            property_name: property.to_string(),
        })
    }

    /// Resolve property for standard/polymorphic patterns
    /// Uses ViewScan.property_mapping
    fn resolve_standard_property(
        &self,
        view_scan: &ViewScan,
        property: &str,
    ) -> Result<PropertyValue, QueryPlannerError> {
        view_scan
            .property_mapping
            .get(property)
            .cloned()
            .ok_or_else(|| {
                QueryPlannerError::InvalidQuery(format!(
                    "Property '{}' not found in property_mapping for table '{}'",
                    property, view_scan.source_table
                ))
            })
    }

    /// Resolve property for denormalized patterns
    /// Uses from_node_properties or to_node_properties based on position
    fn resolve_denormalized_property(
        &self,
        view_scan: &ViewScan,
        property: &str,
        position: NodePosition,
    ) -> Result<PropertyValue, QueryPlannerError> {
        match position {
            NodePosition::From => {
                // Node is FROM in relationship, use from_node_properties
                view_scan
                    .from_node_properties
                    .as_ref()
                    .and_then(|props| props.get(property).cloned())
                    .ok_or_else(|| {
                        QueryPlannerError::InvalidQuery(format!(
                            "Property '{}' not found in from_node_properties for denormalized table '{}'",
                            property, view_scan.source_table
                        ))
                    })
            }
            NodePosition::To => {
                // Node is TO in relationship, use to_node_properties
                view_scan
                    .to_node_properties
                    .as_ref()
                    .and_then(|props| props.get(property).cloned())
                    .ok_or_else(|| {
                        QueryPlannerError::InvalidQuery(format!(
                            "Property '{}' not found in to_node_properties for denormalized table '{}'",
                            property, view_scan.source_table
                        ))
                    })
            }
            NodePosition::Standalone => {
                // Shouldn't happen for denormalized nodes
                Err(QueryPlannerError::InvalidQuery(format!(
                    "Denormalized node '{}' cannot have Standalone position. Must be From or To.",
                    view_scan.source_table
                )))
            }
        }
    }

    /// Get the SQL alias for a graph alias
    ///
    /// For standard/polymorphic: Returns the single SQL alias
    /// For denormalized: Requires edge_context to disambiguate
    ///
    /// This is a convenience method for code that only needs the alias,
    /// not the full property resolution.
    pub fn get_sql_alias(
        &self,
        graph_alias: &str,
        edge_context: Option<&str>,
    ) -> Result<String, QueryPlannerError> {
        let mappings = self.alias_mappings.get(graph_alias).ok_or_else(|| {
            QueryPlannerError::InvalidQuery(format!(
                "No alias mapping found for graph alias '{}'",
                graph_alias
            ))
        })?;

        if let Some(edge) = edge_context {
            // Find mapping with matching edge_alias
            mappings
                .iter()
                .find(|m| m.edge_alias.as_deref() == Some(edge))
                .map(|m| m.sql_alias.clone())
                .ok_or_else(|| {
                    QueryPlannerError::InvalidQuery(format!(
                        "No alias mapping found for '{}' in edge context '{}'",
                        graph_alias, edge
                    ))
                })
        } else {
            // Use first mapping
            mappings
                .first()
                .map(|m| m.sql_alias.clone())
                .ok_or_else(|| {
                    QueryPlannerError::InvalidQuery(format!(
                        "Empty alias mapping list for graph alias '{}'",
                        graph_alias
                    ))
                })
        }
    }

    /// Get ViewScan for a graph alias (for advanced use cases)
    pub fn get_view_scan(&self, graph_alias: &str) -> Option<&ViewScan> {
        self.view_scans.get(graph_alias)
    }

    /// Check if a graph alias is denormalized
    pub fn is_denormalized(&self, graph_alias: &str) -> bool {
        self.view_scans
            .get(graph_alias)
            .map(|vs| vs.is_denormalized)
            .unwrap_or(false)
    }

    /// Check if a graph alias is polymorphic
    pub fn is_polymorphic(&self, graph_alias: &str) -> bool {
        self.alias_mappings
            .get(graph_alias)
            .and_then(|mappings| mappings.first())
            .map(|m| m.is_polymorphic)
            .unwrap_or(false)
    }
}

impl Default for PropertyResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: Create a standard ViewScan
    fn create_standard_view_scan(table: &str) -> ViewScan {
        let mut property_mapping = HashMap::new();
        property_mapping.insert(
            "name".to_string(),
            PropertyValue::Column("username".to_string()),
        );
        property_mapping.insert(
            "email".to_string(),
            PropertyValue::Column("email_address".to_string()),
        );

        ViewScan::new(
            table.to_string(),
            None,
            property_mapping,
            "user_id".to_string(),
            vec!["name".to_string(), "email".to_string()],
            vec![],
        )
    }

    /// Helper: Create a denormalized ViewScan with from/to properties
    fn create_denormalized_view_scan(table: &str) -> ViewScan {
        let mut property_mapping = HashMap::new();
        property_mapping.insert(
            "distance".to_string(),
            PropertyValue::Column("Distance".to_string()),
        );

        let mut from_props = HashMap::new();
        from_props.insert(
            "code".to_string(),
            PropertyValue::Column("Origin".to_string()),
        );
        from_props.insert(
            "city".to_string(),
            PropertyValue::Column("OriginCityName".to_string()),
        );

        let mut to_props = HashMap::new();
        to_props.insert(
            "code".to_string(),
            PropertyValue::Column("Dest".to_string()),
        );
        to_props.insert(
            "city".to_string(),
            PropertyValue::Column("DestCityName".to_string()),
        );

        let mut view_scan = ViewScan::new(
            table.to_string(),
            None,
            property_mapping,
            "flight_id".to_string(),
            vec!["distance".to_string()],
            vec![],
        );
        view_scan.is_denormalized = true;
        view_scan.from_node_properties = Some(from_props);
        view_scan.to_node_properties = Some(to_props);
        view_scan
    }

    #[test]
    fn test_standard_property_resolution() {
        let mut resolver = PropertyResolver::new();

        // Register ViewScan
        resolver.register_view_scan("u".to_string(), create_standard_view_scan("users"));

        // Register alias mapping
        resolver.register_alias(
            "u".to_string(),
            AliasMapping {
                sql_alias: "u1".to_string(),
                position: NodePosition::Standalone,
                is_denormalized: false,
                is_polymorphic: false,
                edge_alias: None,
                type_filters: vec![],
            },
        );

        // Resolve property
        let result = resolver.resolve_property("u", "name", None).unwrap();

        assert_eq!(result.table_alias, "u1");
        assert_eq!(
            result.property_value,
            PropertyValue::Column("username".to_string())
        );
        assert_eq!(result.type_filters.len(), 0);
    }

    #[test]
    fn test_alias_resolution_without_edge_context() {
        let mut resolver = PropertyResolver::new();

        resolver.register_view_scan("u".to_string(), create_standard_view_scan("users"));
        resolver.register_alias(
            "u".to_string(),
            AliasMapping {
                sql_alias: "u1".to_string(),
                position: NodePosition::Standalone,
                is_denormalized: false,
                is_polymorphic: false,
                edge_alias: None,
                type_filters: vec![],
            },
        );

        let alias = resolver.get_sql_alias("u", None).unwrap();
        assert_eq!(alias, "u1");
    }

    #[test]
    fn test_denormalized_multi_hop_alias_disambiguation() {
        let mut resolver = PropertyResolver::new();

        // Register ViewScan for node 'b'
        resolver.register_view_scan("b".to_string(), create_denormalized_view_scan("flights"));

        // Register TWO mappings for 'b' (appears in edges 'f' and 'g')
        resolver.register_alias(
            "b".to_string(),
            AliasMapping {
                sql_alias: "f".to_string(),
                position: NodePosition::To,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("f".to_string()),
                type_filters: vec![],
            },
        );
        resolver.register_alias(
            "b".to_string(),
            AliasMapping {
                sql_alias: "g".to_string(),
                position: NodePosition::From,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("g".to_string()),
                type_filters: vec![],
            },
        );

        // Resolve with edge context 'f'
        let alias_f = resolver.get_sql_alias("b", Some("f")).unwrap();
        assert_eq!(alias_f, "f");

        // Resolve with edge context 'g'
        let alias_g = resolver.get_sql_alias("b", Some("g")).unwrap();
        assert_eq!(alias_g, "g");
    }

    #[test]
    fn test_polymorphic_with_type_filters() {
        let mut resolver = PropertyResolver::new();

        let mut property_mapping = HashMap::new();
        property_mapping.insert(
            "date".to_string(),
            PropertyValue::Column("interaction_date".to_string()),
        );

        let view_scan = ViewScan::new(
            "interactions".to_string(),
            None,
            property_mapping,
            "interaction_id".to_string(),
            vec!["date".to_string()],
            vec![],
        );

        resolver.register_view_scan("i".to_string(), view_scan);
        resolver.register_alias(
            "i".to_string(),
            AliasMapping {
                sql_alias: "i".to_string(),
                position: NodePosition::Standalone,
                is_denormalized: false,
                is_polymorphic: true,
                edge_alias: None,
                type_filters: vec!["interaction_type = 'FOLLOWS'".to_string()],
            },
        );

        let result = resolver.resolve_property("i", "date", None).unwrap();

        assert_eq!(result.table_alias, "i");
        assert_eq!(
            result.property_value,
            PropertyValue::Column("interaction_date".to_string())
        );
        assert_eq!(result.type_filters, vec!["interaction_type = 'FOLLOWS'"]);
    }

    #[test]
    fn test_missing_property_returns_error() {
        let mut resolver = PropertyResolver::new();

        resolver.register_view_scan("u".to_string(), create_standard_view_scan("users"));
        resolver.register_alias(
            "u".to_string(),
            AliasMapping {
                sql_alias: "u1".to_string(),
                position: NodePosition::Standalone,
                is_denormalized: false,
                is_polymorphic: false,
                edge_alias: None,
                type_filters: vec![],
            },
        );

        let result = resolver.resolve_property("u", "nonexistent", None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in property_mapping"));
    }

    #[test]
    fn test_denormalized_from_property_resolution() {
        let mut resolver = PropertyResolver::new();

        // Register denormalized ViewScan with from/to properties
        resolver.register_view_scan("a".to_string(), create_denormalized_view_scan("flights"));

        // Register alias: node 'a' is FROM in edge 'f'
        resolver.register_alias(
            "a".to_string(),
            AliasMapping {
                sql_alias: "f".to_string(),
                position: NodePosition::From,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("f".to_string()),
                type_filters: vec![],
            },
        );

        // Resolve property: a.city (FROM position)
        let result = resolver.resolve_property("a", "city", Some("f")).unwrap();

        assert_eq!(result.table_alias, "f");
        assert_eq!(
            result.property_value,
            PropertyValue::Column("OriginCityName".to_string())
        );
        assert_eq!(result.type_filters.len(), 0);
    }

    #[test]
    fn test_denormalized_to_property_resolution() {
        let mut resolver = PropertyResolver::new();

        // Register denormalized ViewScan
        resolver.register_view_scan("b".to_string(), create_denormalized_view_scan("flights"));

        // Register alias: node 'b' is TO in edge 'f'
        resolver.register_alias(
            "b".to_string(),
            AliasMapping {
                sql_alias: "f".to_string(),
                position: NodePosition::To,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("f".to_string()),
                type_filters: vec![],
            },
        );

        // Resolve property: b.city (TO position)
        let result = resolver.resolve_property("b", "city", Some("f")).unwrap();

        assert_eq!(result.table_alias, "f");
        assert_eq!(
            result.property_value,
            PropertyValue::Column("DestCityName".to_string())
        );
    }

    #[test]
    fn test_denormalized_multi_hop_different_properties() {
        let mut resolver = PropertyResolver::new();

        // Node 'b' appears in TWO edges with different roles
        resolver.register_view_scan("b".to_string(), create_denormalized_view_scan("flights"));

        // b is TO in edge 'f'
        resolver.register_alias(
            "b".to_string(),
            AliasMapping {
                sql_alias: "f".to_string(),
                position: NodePosition::To,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("f".to_string()),
                type_filters: vec![],
            },
        );

        // b is FROM in edge 'g'
        resolver.register_alias(
            "b".to_string(),
            AliasMapping {
                sql_alias: "g".to_string(),
                position: NodePosition::From,
                is_denormalized: true,
                is_polymorphic: false,
                edge_alias: Some("g".to_string()),
                type_filters: vec![],
            },
        );

        // Resolve b.city in edge 'f' (TO position)
        let result_f = resolver.resolve_property("b", "city", Some("f")).unwrap();
        assert_eq!(result_f.table_alias, "f");
        assert_eq!(
            result_f.property_value,
            PropertyValue::Column("DestCityName".to_string())
        );

        // Resolve b.city in edge 'g' (FROM position)
        let result_g = resolver.resolve_property("b", "city", Some("g")).unwrap();
        assert_eq!(result_g.table_alias, "g");
        assert_eq!(
            result_g.property_value,
            PropertyValue::Column("OriginCityName".to_string())
        );

        // Same node, same property, but DIFFERENT columns based on edge context!
    }
}
