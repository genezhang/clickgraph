//! Node classification utilities for graph catalog
//!
//! This module consolidates logic for determining node properties and patterns,
//! eliminating duplication across the codebase.
//!
//! # Purpose
//!
//! Node classification involves determining:
//! - Whether a node is denormalized (properties in edge table)
//! - Whether a node appears on a specific side (from/to) of a relationship
//! - Property access patterns for different node types
//!
//! Previously, this logic was scattered across:
//! - `graph_schema.rs` - Node schema checks
//! - `pattern_schema.rs` - Pattern-specific classification
//! - `graph_join_inference.rs` - Join-specific classification
//!
//! # Example
//!
//! ```ignore
//! use crate::graph_catalog::node_classification::{is_node_denormalized, has_denormalized_properties};
//! use crate::graph_catalog::graph_schema::{NodeSchema, RelationshipSchema};
//!
//! if is_node_denormalized(node_schema) {
//!     // Handle denormalized properties
//! }
//!
//! if has_denormalized_properties(rel_schema, "from") {
//!     // Source node is denormalized in this relationship
//! }
//! ```

use crate::graph_catalog::graph_schema::{NodeSchema, RelationshipSchema};
use crate::graph_catalog::schema_types::SchemaType;

/// Check if a node is denormalized (has properties defined in relationships)
///
/// A denormalized node has:
/// 1. `is_denormalized` flag set to true, OR
/// 2. `from_node_properties` defined, OR  
/// 3. `to_node_properties` defined
///
/// # Arguments
/// * `node` - Node schema to check
///
/// # Returns
/// `true` if node has any denormalized properties
///
/// # Example
/// ```ignore
/// if is_node_denormalized(&node_schema) {
///     // This node may have properties in edge tables
/// }
/// ```
pub fn is_node_denormalized(node: &NodeSchema) -> bool {
    node.is_denormalized || node.from_properties.is_some() || node.to_properties.is_some()
}

/// Check if a node has denormalized properties when it appears as a specific side
///
/// # Arguments
/// * `node` - Node schema to check
/// * `side` - Either "from" or "to"
///
/// # Returns
/// `true` if node has properties defined for the given side
pub fn has_denormalized_properties_on_side(node: &NodeSchema, side: &str) -> bool {
    match side {
        "from" => node
            .from_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty()),
        "to" => node
            .to_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty()),
        _ => false,
    }
}

/// Check if a relationship contains denormalized properties for a specific side
///
/// # Arguments
/// * `rel` - Relationship schema to check
/// * `side` - Either "from" or "to"
///
/// # Returns
/// `true` if relationship has denormalized properties for that node side
pub fn rel_has_denormalized_properties(rel: &RelationshipSchema, side: &str) -> bool {
    match side {
        "from" => rel
            .from_node_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty()),
        "to" => rel
            .to_node_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty()),
        _ => false,
    }
}

/// Check if both nodes in a relationship are denormalized
///
/// This occurs when both from_node_properties and to_node_properties are defined.
///
/// # Arguments
/// * `rel` - Relationship schema to check
///
/// # Returns
/// `true` if relationship has denormalized properties on both sides
pub fn rel_has_both_nodes_denormalized(rel: &RelationshipSchema) -> bool {
    rel.from_node_properties
        .as_ref()
        .is_some_and(|props| !props.is_empty())
        && rel
            .to_node_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty())
}

/// Check if a relationship has denormalized properties on either side
///
/// # Arguments
/// * `rel` - Relationship schema to check
///
/// # Returns
/// `true` if relationship has denormalized properties on from or to side
pub fn rel_has_any_denormalized(rel: &RelationshipSchema) -> bool {
    rel.from_node_properties
        .as_ref()
        .is_some_and(|props| !props.is_empty())
        || rel
            .to_node_properties
            .as_ref()
            .is_some_and(|props| !props.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::NodeIdSchema;
    use std::collections::HashMap;

    #[test]
    fn test_is_node_denormalized_with_flag() {
        let mut node = NodeSchema::new_traditional(
            "test_db".to_string(),
            "test_table".to_string(),
            vec!["id".to_string()],
            "id".to_string(),
            NodeIdSchema::single(
                "id".to_string(),
                crate::graph_catalog::schema_types::SchemaType::Integer,
            ),
            HashMap::new(),
            None,
            None,
            None,
        );
        node.is_denormalized = true;

        assert!(is_node_denormalized(&node));
    }

    #[test]
    fn test_is_node_denormalized_with_from_properties() {
        let mut node = NodeSchema::new_traditional(
            "test_db".to_string(),
            "test_table".to_string(),
            vec!["id".to_string()],
            "id".to_string(),
            NodeIdSchema::single(
                "id".to_string(),
                crate::graph_catalog::schema_types::SchemaType::Integer,
            ),
            HashMap::new(),
            None,
            None,
            None,
        );
        node.from_properties = Some(HashMap::from([(
            "code".to_string(),
            "origin_code".to_string(),
        )]));

        assert!(is_node_denormalized(&node));
    }

    #[test]
    fn test_is_node_denormalized_false() {
        let node = NodeSchema::new_traditional(
            "test_db".to_string(),
            "test_table".to_string(),
            vec!["id".to_string()],
            "id".to_string(),
            NodeIdSchema::single(
                "id".to_string(),
                crate::graph_catalog::schema_types::SchemaType::Integer,
            ),
            HashMap::new(),
            None,
            None,
            None,
        );

        assert!(!is_node_denormalized(&node));
    }

    #[test]
    fn test_has_denormalized_properties_on_side() {
        let mut node = NodeSchema::new_traditional(
            "test_db".to_string(),
            "test_table".to_string(),
            vec!["id".to_string()],
            "id".to_string(),
            NodeIdSchema::single(
                "id".to_string(),
                crate::graph_catalog::schema_types::SchemaType::Integer,
            ),
            HashMap::new(),
            None,
            None,
            None,
        );
        node.from_properties = Some(HashMap::from([(
            "city".to_string(),
            "origin_city".to_string(),
        )]));

        assert!(has_denormalized_properties_on_side(&node, "from"));
        assert!(!has_denormalized_properties_on_side(&node, "to"));
        assert!(!has_denormalized_properties_on_side(&node, "invalid"));
    }
}
