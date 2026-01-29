//! Graph Join Inference Analyzer
//!
//! This module handles inferring SQL JOINs from Cypher graph patterns.
//! It converts MATCH patterns like `(a)-[r:FOLLOWS]->(b)` into appropriate
//! SQL JOIN conditions for ClickHouse.
//!
//! ## Architecture Overview
//!
//! The analyzer uses a phased approach:
//!
//! 1. **Pattern Metadata Construction** - Build a lightweight index over GraphRel trees
//! 2. **PatternSchemaContext Integration** - Map patterns to schema-aware join strategies
//! 3. **Join Generation** - Create SQL JOINs based on pattern type (standard, FK-edge, denormalized)
//! 4. **Cross-Branch Detection** - Handle branching patterns like `(a)-[:R1]->(b), (a)-[:R2]->(c)`
//!
//! ## Module Structure
//!
//! - `cross_branch` - Cross-branch join detection and relationship uniqueness constraints
//! - `helpers` - Utility functions for join inference
//! - `metadata` - Pattern graph metadata types and builder
//! - `inference` - Core join inference implementation
//! - `tests` - Comprehensive unit tests
//!
//! ## Key Types
//!
//! - [`GraphJoinInference`] - Main analyzer pass implementing [`AnalyzerPass`]
//! - [`JoinContext`] - Tracks joined tables during traversal (re-exported from join_context)
//! - [`VlpEndpointInfo`] - Variable-length path endpoint information
//! - [`PatternGraphMetadata`] - Complete metadata for a MATCH clause
//! - [`PatternNodeInfo`] - Cached information about a node variable
//! - [`PatternEdgeInfo`] - Cached information about a relationship variable

// Submodules extracted from legacy
pub mod cross_branch;
pub mod helpers;
pub mod metadata;

// Core implementation (renamed from legacy)
#[allow(clippy::module_inception)]
mod inference;

// Unit tests
#[cfg(test)]
mod tests;

// Re-export the public API from inference
pub use inference::GraphJoinInference;

// Re-export metadata types
pub use metadata::{
    expr_references_alias, is_node_referenced, plan_references_alias, PatternEdgeInfo,
    PatternGraphMetadata, PatternMetadataBuilder, PatternNodeInfo,
};

// Re-export JoinContext types from the shared module
pub use crate::query_planner::join_context::{JoinContext, VlpEndpointInfo, VlpPosition};
