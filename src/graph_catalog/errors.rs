//! # Graph Schema Error Types
//!
//! Comprehensive error handling for graph schema validation, configuration parsing,
//! and view-based graph model mapping.
//!
//! ## Error Categories
//!
//! - **Schema Errors**: Missing or invalid node/relationship schemas
//! - **View Mapping Errors**: Invalid configuration for table-to-graph mapping
//! - **Configuration Errors**: File I/O and parsing issues during schema loading
//!
//! ## Usage Patterns
//!
//! When returning schema errors, use context helpers to provide operational information:
//!
//! ```ignore
//! // ✅ GOOD: Provides what and where
//! GraphSchemaError::node_error_with_context(
//!     "User",
//!     "When validating graph pattern: MATCH (n:User)"
//! )
//!
//! // ✅ GOOD: Operational context
//! GraphSchemaError::config_error_with_context(
//!     "schema.yaml",
//!     "While loading node definitions at startup"
//! )
//! ```

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum GraphSchemaError {
    #[error("No relationship schema found for `{rel_label}`.")]
    Relation { rel_label: String },
    #[error("No node schema found for `{node_label}`")]
    Node { node_label: String },
    #[error("No relationship index schema found for `{rel_label}`.")]
    RelationIndex { rel_label: String },
    #[error("Invalid view mapping: source table '{table}' does not exist")]
    InvalidSourceTable { table: String },
    #[error("Invalid view mapping: column '{column}' not found in table '{table}'")]
    InvalidColumn { column: String, table: String },
    #[error("Invalid view mapping: ID column '{column}' has incompatible type in table '{table}'")]
    InvalidIdColumnType { column: String, table: String },
    #[error("Invalid view mapping: cannot resolve node reference '{node}' for relationship")]
    InvalidNodeReference { node: String },
    #[error("Failed to read configuration file: {error}")]
    ConfigReadError { error: String },
    #[error("Failed to parse configuration: {error}")]
    ConfigParseError { error: String },
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },
}

/// Helper methods for creating errors with context information
impl GraphSchemaError {
    /// Create a Node error with context information
    ///
    /// # Example
    /// ```ignore
    /// GraphSchemaError::node_error_with_context(
    ///     "User",
    ///     "In MATCH pattern: (n:User)-[:FOLLOWS]->(m:User)"
    /// )
    /// ```
    pub fn node_error_with_context(
        node_label: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let label = node_label.into();
        let ctx = context.into();
        GraphSchemaError::Node {
            node_label: format!("{}\n  Context: {}", label, ctx),
        }
    }

    /// Create a Relation error with context information
    ///
    /// # Example
    /// ```ignore
    /// GraphSchemaError::relation_error_with_context(
    ///     "FOLLOWS",
    ///     "Traversing relationship: (a)-[:FOLLOWS]->(b)"
    /// )
    /// ```
    pub fn relation_error_with_context(
        rel_label: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let label = rel_label.into();
        let ctx = context.into();
        GraphSchemaError::Relation {
            rel_label: format!("{}\n  Context: {}", label, ctx),
        }
    }

    /// Create a configuration error with context information
    ///
    /// # Example
    /// ```ignore
    /// GraphSchemaError::config_error_with_context(
    ///     "schema.yaml",
    ///     "While parsing node definitions"
    /// )
    /// ```
    pub fn config_error_with_context(
        config_path: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let path = config_path.into();
        let ctx = context.into();
        GraphSchemaError::InvalidConfig {
            message: format!(
                "Configuration error in '{}': {}\n  Context: {}",
                path, "failed to load", ctx
            ),
        }
    }
}
