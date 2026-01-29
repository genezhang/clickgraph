//! MATCH clause processing for Cypher queries.
//!
//! This module handles the translation of Cypher MATCH patterns into logical plans.
//! It supports:
//! - Node patterns: `(n:Label)`
//! - Relationship patterns: `(a)-[r:TYPE]->(b)`
//! - Variable-length paths: `(a)-[*1..3]->(b)`
//! - Shortest path: `shortestPath((a)-[*]->(b))`
//!
//! # Architecture
//!
//! The module is organized into focused submodules:
//! - `traversal.rs` - Core MATCH clause evaluation and pattern traversal
//! - `view_scan.rs` - ViewScan generation for nodes and relationships
//! - `helpers.rs` - Utility functions (property conversion, scan helpers, etc.)
//! - `errors.rs` - Error types for match clause processing
//! - `tests.rs` - Unit tests for match clause processing
//!
//! **Note**: Type inference has been moved to `query_planner::analyzer::match_type_inference`
//! to maintain proper separation between logical plan construction (here) and analysis (analyzer).

mod errors;
mod helpers;
mod traversal;
mod view_scan;

#[cfg(test)]
mod tests;

// Re-export all public items from traversal module
pub use traversal::*;

// Re-export ViewScan generation functions
pub use view_scan::{
    generate_relationship_center, try_generate_relationship_view_scan, try_generate_view_scan,
};

// Re-export helper functions
pub use helpers::{
    compute_connection_aliases, compute_rel_node_labels, compute_variable_length,
    convert_properties, convert_properties_to_operator_application, determine_optional_anchor,
    generate_denormalization_aware_scan, generate_scan, is_denormalized_scan,
    is_label_denormalized, register_node_in_context, register_path_variable,
    register_relationship_in_context,
};
