//! Multi-Type Variable-Length Path Expansion
//!
//! This module handles expansion of variable-length paths with multiple relationship types
//! and/or multiple end node types. The key challenge is type safety: different node types
//! have different ID domains (e.g., User.user_id=3 ≠ Post.post_id=3).
//!
//! Instead of using recursive CTEs (which are unsafe for polymorphic types), we enumerate
//! all valid path combinations and generate explicit type-safe JOINs for each, then UNION
//! the results.
//!
//! Example:
//! ```cypher
//! MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
//! ```
//!
//! Valid 1-hop paths:
//! - User-[FOLLOWS]->User
//! - User-[AUTHORED]->Post
//!
//! Valid 2-hop paths:
//! - User-[FOLLOWS]->User-[FOLLOWS]->User
//! - User-[FOLLOWS]->User-[AUTHORED]->Post
//! - User-[AUTHORED]->Post-[?]->??? (check schema for edges from Post)
//!
//! Invalid paths are filtered out based on schema constraints.

use crate::graph_catalog::graph_schema::{GraphSchema, RelationshipSchema};
use std::collections::HashSet;

/// Represents a single hop in a path with type information
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathHop {
    /// Relationship type (e.g., "FOLLOWS", "AUTHORED")
    pub rel_type: String,
    /// Starting node type (e.g., "User")
    pub from_node_type: String,
    /// Ending node type (e.g., "User", "Post")
    pub to_node_type: String,
    /// Whether this hop traverses the edge in reverse direction
    /// (i.e., we're going from to_node to from_node in the schema)
    pub reversed: bool,
}

/// Represents a complete path enumeration (sequence of hops)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathEnumeration {
    pub hops: Vec<PathHop>,
}

impl PathEnumeration {
    /// Get the starting node type of this path
    pub fn start_type(&self) -> Option<&str> {
        self.hops.first().map(|h| h.from_node_type.as_str())
    }

    /// Get the ending node type of this path
    pub fn end_type(&self) -> Option<&str> {
        self.hops.last().map(|h| h.to_node_type.as_str())
    }

    /// Get the length (number of hops) of this path
    pub fn length(&self) -> usize {
        self.hops.len()
    }
}

/// Enumerate all valid path combinations for a multi-type VLP
///
/// # Arguments
/// * `start_labels` - Possible labels for the start node (e.g., ["User"])
/// * `rel_types` - Possible relationship types (e.g., ["FOLLOWS", "AUTHORED"])
/// * `end_labels` - Possible labels for the end node (e.g., ["User", "Post"])
/// * `min_hops` - Minimum path length (e.g., 1)
/// * `max_hops` - Maximum path length (e.g., 2)
/// * `schema` - Graph schema for validation
///
/// # Returns
/// Vector of valid path enumerations, filtered by schema constraints
///
/// # Example
/// ```rust
/// use clickgraph::query_planner::analyzer::multi_type_vlp_expansion::enumerate_vlp_paths;
/// use clickgraph::graph_catalog::graph_schema::GraphSchema;
/// use std::collections::HashMap;
///
/// // Create a minimal schema for demonstration
/// let schema = GraphSchema::build(
///     1,
///     "test_db".to_string(),
///     HashMap::new(), // nodes
///     HashMap::new(), // relationships
/// );
///
/// let paths = enumerate_vlp_paths(
///     &["User".to_string()],
///     &["FOLLOWS".to_string(), "AUTHORED".to_string()],
///     &["User".to_string(), "Post".to_string()],
///     1,
///     2,
///     &schema
/// );
/// // Returns paths like:
/// // - [User-FOLLOWS->User]
/// // - [User-AUTHORED->Post]
/// // - [User-FOLLOWS->User-FOLLOWS->User]
/// // - [User-FOLLOWS->User-AUTHORED->Post]
/// ```
pub fn enumerate_vlp_paths(
    start_labels: &[String],
    rel_types: &[String],
    end_labels: &[String],
    min_hops: usize,
    max_hops: usize,
    schema: &GraphSchema,
) -> Vec<PathEnumeration> {
    enumerate_vlp_paths_with_direction(
        start_labels,
        rel_types,
        end_labels,
        min_hops,
        max_hops,
        schema,
        false,
    )
}

/// Enumerate paths including both outgoing and incoming edges (for undirected patterns)
pub fn enumerate_vlp_paths_undirected(
    start_labels: &[String],
    rel_types: &[String],
    end_labels: &[String],
    min_hops: usize,
    max_hops: usize,
    schema: &GraphSchema,
) -> Vec<PathEnumeration> {
    enumerate_vlp_paths_with_direction(
        start_labels,
        rel_types,
        end_labels,
        min_hops,
        max_hops,
        schema,
        true,
    )
}

fn enumerate_vlp_paths_with_direction(
    start_labels: &[String],
    rel_types: &[String],
    end_labels: &[String],
    min_hops: usize,
    max_hops: usize,
    schema: &GraphSchema,
    include_incoming: bool,
) -> Vec<PathEnumeration> {
    let mut all_paths = Vec::new();

    // Generate paths for each length from min_hops to max_hops
    for length in min_hops..=max_hops {
        let paths_of_length = generate_paths_of_length(
            start_labels,
            rel_types,
            end_labels,
            length,
            schema,
            include_incoming,
        );
        all_paths.extend(paths_of_length);
    }

    // Deduplicate paths (same sequence of hops may be generated multiple ways)
    let unique_paths: HashSet<PathEnumeration> = all_paths.into_iter().collect();
    unique_paths.into_iter().collect()
}

/// Generate all valid paths of a specific length
fn generate_paths_of_length(
    start_labels: &[String],
    rel_types: &[String],
    end_labels: &[String],
    length: usize,
    schema: &GraphSchema,
    include_incoming: bool,
) -> Vec<PathEnumeration> {
    let mut result = Vec::new();

    // For each starting label
    for start_label in start_labels {
        // Generate all paths starting from this label
        let paths = generate_paths_recursive(
            start_label,
            rel_types,
            end_labels,
            length,
            Vec::new(),
            schema,
            include_incoming,
        );
        result.extend(paths);
    }

    result
}

/// Recursively generate paths using depth-first search
///
/// # Arguments
/// * `current_node_type` - Current node type we're at
/// * `rel_types` - Available relationship types
/// * `end_labels` - Target end node labels
/// * `remaining_hops` - How many more hops we need
/// * `path_so_far` - Path built up so far
/// * `schema` - Graph schema for validation
fn generate_paths_recursive(
    current_node_type: &str,
    rel_types: &[String],
    end_labels: &[String],
    remaining_hops: usize,
    path_so_far: Vec<PathHop>,
    schema: &GraphSchema,
    include_incoming: bool,
) -> Vec<PathEnumeration> {
    // Base case: no more hops needed
    if remaining_hops == 0 {
        // Check if current node matches any of the target end labels
        // Empty end_labels or "UnknownEndType" means any node type is acceptable
        let end_matches = end_labels.is_empty()
            || end_labels
                .iter()
                .any(|l| l == current_node_type || l == "UnknownEndType" || l == "$any");
        if end_matches {
            return vec![PathEnumeration { hops: path_so_far }];
        } else {
            return vec![]; // Path doesn't end at valid type
        }
    }

    let mut result = Vec::new();

    // Try each relationship type
    for rel_type in rel_types {
        // Find outgoing edges (from current node)
        let valid_edges = find_edges_from_node(schema, rel_type, current_node_type);

        for edge in &valid_edges {
            // For polymorphic edges with $any node types, expand to concrete types.
            // Note: N node types → N×N combinations per edge, but filtered by
            // current_node_type match below, limiting actual branches to N per hop.
            let from_types = schema.expand_node_type(&edge.from_node);
            let to_types = schema.expand_node_type(&edge.to_node);

            for from_t in &from_types {
                // Filter: from_type must match current_node_type
                if from_t != current_node_type {
                    continue;
                }
                for to_t in &to_types {
                    let hop = PathHop {
                        rel_type: rel_type.clone(),
                        from_node_type: from_t.clone(),
                        to_node_type: to_t.clone(),
                        reversed: false,
                    };

                    let mut new_path = path_so_far.clone();
                    new_path.push(hop);

                    let sub_paths = generate_paths_recursive(
                        to_t,
                        rel_types,
                        end_labels,
                        remaining_hops - 1,
                        new_path,
                        schema,
                        include_incoming,
                    );
                    result.extend(sub_paths);
                }
            }
        }

        // Also find incoming edges (to current node) for undirected patterns
        if include_incoming {
            let incoming_edges = find_edges_to_node(schema, rel_type, current_node_type);

            for edge in incoming_edges {
                let edge_to_types = schema.expand_node_type(&edge.to_node);
                let edge_from_types = schema.expand_node_type(&edge.from_node);

                for to_t in &edge_to_types {
                    // Filter: to_node must match current_node_type (reversed hop)
                    if to_t != current_node_type {
                        continue;
                    }
                    for from_t in &edge_from_types {
                        // Create a reversed hop: we traverse FROM current TO edge.from_node
                        // but through the edge table where current is the to_node
                        let hop = PathHop {
                            rel_type: rel_type.clone(),
                            from_node_type: to_t.clone(),
                            to_node_type: from_t.clone(),
                            reversed: true,
                        };

                        let mut new_path = path_so_far.clone();
                        new_path.push(hop);

                        let sub_paths = generate_paths_recursive(
                            from_t,
                            rel_types,
                            end_labels,
                            remaining_hops - 1,
                            new_path,
                            schema,
                            include_incoming,
                        );
                        result.extend(sub_paths);
                    }
                }
            }
        }
    }

    result
}

/// Find all edges of a given type that start from a specific node type
///
/// Handles both simple and composite relationship keys.
fn find_edges_from_node<'a>(
    schema: &'a GraphSchema,
    rel_type: &str,
    from_node_type: &str,
) -> Vec<&'a RelationshipSchema> {
    let mut edges = Vec::new();

    log::debug!(
        "🔍 find_edges_from_node: looking for rel_type='{}' from_node_type='{}'",
        rel_type,
        from_node_type
    );

    // Check all relationships in the schema
    for (key, rel_schema) in schema.get_relationships_schemas() {
        log::debug!(
            "  Checking relationship key='{}': {} -> {}",
            key,
            rel_schema.from_node,
            rel_schema.to_node
        );

        // Match by relationship type (handle both simple and composite keys)
        let matches_type = if key.contains("::") {
            // Composite key: "TYPE::FROM::TO"
            key.split("::").next() == Some(rel_type)
        } else {
            // Simple key: "TYPE"
            key == rel_type
        };

        log::debug!(
            "    matches_type={} (key.contains('::')={}, key.split('::').next()={:?})",
            matches_type,
            key.contains("::"),
            key.split("::").next()
        );

        // Check if this edge starts from the specified node type
        // $any means polymorphic: actual type determined at runtime via from_label_column
        if matches_type
            && (rel_schema.from_node == from_node_type || rel_schema.from_node == "$any")
        {
            log::debug!("    ✅ Found matching edge!");
            edges.push(rel_schema);
        } else {
            log::debug!(
                "    ❌ No match: matches_type={}, from_node matches={}",
                matches_type,
                rel_schema.from_node == from_node_type
            );
        }
    }

    log::debug!("  Returning {} edges", edges.len());
    edges
}

/// Find all edges of a given type where the TO node matches (for incoming/reverse traversal)
fn find_edges_to_node<'a>(
    schema: &'a GraphSchema,
    rel_type: &str,
    to_node_type: &str,
) -> Vec<&'a RelationshipSchema> {
    let mut edges = Vec::new();

    for (key, rel_schema) in schema.get_relationships_schemas() {
        let matches_type = if key.contains("::") {
            key.split("::").next() == Some(rel_type)
        } else {
            key == rel_type
        };

        if matches_type && (rel_schema.to_node == to_node_type || rel_schema.to_node == "$any") {
            log::debug!(
                "🔍 find_edges_to_node: found rel '{}' where {} -> {} (incoming to {})",
                key,
                rel_schema.from_node,
                rel_schema.to_node,
                to_node_type
            );
            edges.push(rel_schema);
        }
    }

    edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    fn create_test_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create User node
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "users".to_string(),
                column_names: vec![],
                primary_keys: "user_id".to_string(),
                node_id: NodeIdSchema::single("user_id".to_string(), "UInt64".to_string()),
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
            },
        );

        // Create Post node
        nodes.insert(
            "Post".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "posts".to_string(),
                column_names: vec![],
                primary_keys: "post_id".to_string(),
                node_id: NodeIdSchema::single("post_id".to_string(), "UInt64".to_string()),
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
            },
        );

        // Create FOLLOWS relationship: User -> User
        relationships.insert(
            "FOLLOWS::User::User".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "follows".to_string(),
                column_names: vec![],
                from_node: "User".to_string(),
                to_node: "User".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "users".to_string(),
                from_id: "follower_id".to_string(),
                to_id: "followed_id".to_string(),
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
                from_label_values: None,
                to_label_values: None,
                from_node_properties: None,
                to_node_properties: None,
                is_fk_edge: false,
                constraints: None,
                edge_id_types: None,
            },
        );

        // Create AUTHORED relationship: User -> Post
        relationships.insert(
            "AUTHORED::User::Post".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "authored".to_string(),
                column_names: vec![],
                from_node: "User".to_string(),
                to_node: "Post".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "posts".to_string(),
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
                from_label_values: None,
                to_label_values: None,
                from_node_properties: None,
                to_node_properties: None,
                is_fk_edge: false,
                constraints: None,
                edge_id_types: None,
            },
        );

        GraphSchema::build(1, "test".to_string(), nodes, relationships)
    }

    #[test]
    fn test_enumerate_single_hop_single_type() {
        let schema = create_test_schema();

        // Simple case: User -[FOLLOWS]-> User
        let paths = enumerate_vlp_paths(
            &[String::from("User")],
            &[String::from("FOLLOWS")],
            &[String::from("User")],
            1,
            1,
            &schema,
        );

        assert_eq!(paths.len(), 1, "Should have exactly 1 path");
        assert_eq!(paths[0].length(), 1, "Path should have 1 hop");
        assert_eq!(paths[0].hops[0].rel_type, "FOLLOWS");
        assert_eq!(paths[0].hops[0].from_node_type, "User");
        assert_eq!(paths[0].hops[0].to_node_type, "User");
    }

    #[test]
    fn test_enumerate_multi_type_single_hop() {
        let schema = create_test_schema();

        // Multi-type: User -[FOLLOWS|AUTHORED]-> (User|Post)
        let paths = enumerate_vlp_paths(
            &[String::from("User")],
            &[String::from("FOLLOWS"), String::from("AUTHORED")],
            &[String::from("User"), String::from("Post")],
            1,
            1,
            &schema,
        );

        // Should have 2 valid paths:
        // 1. User -[FOLLOWS]-> User
        // 2. User -[AUTHORED]-> Post
        assert_eq!(paths.len(), 2, "Should have 2 valid 1-hop paths");

        let follows_path = paths.iter().find(|p| p.hops[0].rel_type == "FOLLOWS");
        let authored_path = paths.iter().find(|p| p.hops[0].rel_type == "AUTHORED");

        assert!(follows_path.is_some(), "Should have FOLLOWS path");
        assert!(authored_path.is_some(), "Should have AUTHORED path");

        let follows = follows_path.unwrap();
        assert_eq!(follows.hops[0].from_node_type, "User");
        assert_eq!(follows.hops[0].to_node_type, "User");

        let authored = authored_path.unwrap();
        assert_eq!(authored.hops[0].from_node_type, "User");
        assert_eq!(authored.hops[0].to_node_type, "Post");
    }

    #[test]
    fn test_enumerate_two_hop_multi_type() {
        let schema = create_test_schema();

        // 2-hop: User -[FOLLOWS|AUTHORED*2]-> (User|Post)
        let paths = enumerate_vlp_paths(
            &[String::from("User")],
            &[String::from("FOLLOWS"), String::from("AUTHORED")],
            &[String::from("User"), String::from("Post")],
            2,
            2,
            &schema,
        );

        // Valid 2-hop paths:
        // 1. User -[FOLLOWS]-> User -[FOLLOWS]-> User
        // 2. User -[FOLLOWS]-> User -[AUTHORED]-> Post
        // Note: User -[AUTHORED]-> Post -[???]-> ??? has no outgoing edges from Post

        assert!(paths.len() >= 2, "Should have at least 2 valid 2-hop paths");

        // Check for User-FOLLOWS-User-FOLLOWS-User
        let double_follows = paths.iter().find(|p| {
            p.length() == 2
                && p.hops[0].rel_type == "FOLLOWS"
                && p.hops[1].rel_type == "FOLLOWS"
                && p.end_type() == Some("User")
        });
        assert!(
            double_follows.is_some(),
            "Should have User-FOLLOWS-User-FOLLOWS-User path"
        );

        // Check for User-FOLLOWS-User-AUTHORED-Post
        let follows_authored = paths.iter().find(|p| {
            p.length() == 2
                && p.hops[0].rel_type == "FOLLOWS"
                && p.hops[1].rel_type == "AUTHORED"
                && p.end_type() == Some("Post")
        });
        assert!(
            follows_authored.is_some(),
            "Should have User-FOLLOWS-User-AUTHORED-Post path"
        );
    }

    #[test]
    fn test_no_valid_paths() {
        let schema = create_test_schema();

        // Invalid: Post has no outgoing edges in our test schema
        let paths = enumerate_vlp_paths(
            &[String::from("Post")],
            &[String::from("FOLLOWS")],
            &[String::from("User")],
            1,
            1,
            &schema,
        );

        assert_eq!(paths.len(), 0, "Should have no valid paths");
    }

    #[test]
    fn test_path_enumeration_with_min_max_range() {
        let schema = create_test_schema();

        // Range: User -[FOLLOWS*1..2]-> User
        let paths = enumerate_vlp_paths(
            &[String::from("User")],
            &[String::from("FOLLOWS")],
            &[String::from("User")],
            1,
            2,
            &schema,
        );

        // Should have paths of length 1 and 2
        let one_hop = paths.iter().filter(|p| p.length() == 1).count();
        let two_hop = paths.iter().filter(|p| p.length() == 2).count();

        assert_eq!(one_hop, 1, "Should have 1 one-hop path");
        assert_eq!(two_hop, 1, "Should have 1 two-hop path");
    }
}
