//! Procedure execution module for Neo4j-compatible schema metadata procedures.
//!
//! This module provides execution infrastructure for standalone CALL procedures
//! that bypass the query planner and return metadata about the graph schema.
//!
//! Supported procedures:
//! - `db.labels()` - Returns all node labels in the current schema
//! - `db.relationshipTypes()` - Returns all relationship types in the current schema
//! - `dbms.components()` - Returns ClickGraph version and edition information
//! - `db.propertyKeys()` - Returns all unique property keys across nodes and relationships
//! - `db.schema.nodeTypeProperties()` - Returns property metadata for each node type
//! - `db.schema.relTypeProperties()` - Returns property metadata for each relationship type
//! - `apoc.meta.schema()` - Returns APOC-format schema metadata for MCP server compatibility
//!
//! # Architecture
//!
//! Procedures are executed directly without SQL generation:
//! 1. Parser recognizes CALL statement → StandaloneProcedureCall AST
//! 2. Handler routes to procedures::executor (BYPASSES query planner)
//! 3. Executor looks up procedure in registry
//! 4. Procedure executes against GraphSchema
//! 5. Results formatted as Bolt/JSON records
//!
//! # Schema Selection
//!
//! Procedures operate on a single logical schema.
//! The schema is selected by the HTTP request's `schema_name` parameter,
//! falling back to the `"default"` schema when the parameter is omitted.
//!
//! Note: Schema selection via `USE` clauses in CALL statements or via Bolt
//! connection database parameters is not currently supported; all procedure
//! calls are evaluated against the HTTP-selected (or default) schema.

pub mod apoc_meta_schema;
pub mod db_labels;
pub mod db_property_keys;
pub mod db_relationship_types;
pub mod db_schema_node_type_properties;
pub mod db_schema_rel_type_properties;
pub mod dbms_components;
pub mod dbms_stubs;
pub mod executor;
pub mod return_evaluator;
pub mod show_databases;

// Re-export key functions for easier access
pub use executor::{
    execute_procedure_query, execute_procedure_union, execute_procedure_union_with_return,
    extract_procedure_names_from_union, is_procedure_only_query, is_procedure_only_statement,
    is_procedure_union_query,
};
pub use return_evaluator::apply_return_clause;

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;
use std::sync::Arc;

/// Result type for procedure execution
pub type ProcedureResult = Result<Vec<HashMap<String, serde_json::Value>>, String>;

/// Function signature for procedure implementation
pub type ProcedureFn = Arc<dyn Fn(&GraphSchema) -> ProcedureResult + Send + Sync + 'static>;

/// Registry of available procedures
pub struct ProcedureRegistry {
    procedures: HashMap<String, ProcedureFn>,
}

impl ProcedureRegistry {
    /// Create a new procedure registry with all built-in procedures registered
    pub fn new() -> Self {
        let mut registry = Self {
            procedures: HashMap::new(),
        };

        // Register built-in procedures
        registry.register("db.labels", Arc::new(db_labels::execute));
        registry.register(
            "db.relationshipTypes",
            Arc::new(db_relationship_types::execute),
        );
        registry.register("dbms.components", Arc::new(dbms_components::execute));
        registry.register("db.propertyKeys", Arc::new(db_property_keys::execute));
        registry.register(
            "db.schema.nodeTypeProperties",
            Arc::new(db_schema_node_type_properties::execute),
        );
        registry.register(
            "db.schema.relTypeProperties",
            Arc::new(db_schema_rel_type_properties::execute),
        );

        // Register APOC procedures for MCP server compatibility
        registry.register("apoc.meta.schema", Arc::new(apoc_meta_schema::execute));

        // Register dbms.* stubs for Neo4j Browser compatibility
        registry.register("dbms.clientConfig", Arc::new(dbms_stubs::client_config));
        registry.register(
            "dbms.security.showCurrentUser",
            Arc::new(dbms_stubs::show_current_user),
        );
        registry.register("dbms.procedures", Arc::new(dbms_stubs::list_procedures));
        registry.register("dbms.functions", Arc::new(dbms_stubs::list_functions));

        registry
    }

    /// Register a procedure with the given name
    pub fn register(&mut self, name: &str, func: ProcedureFn) {
        self.procedures.insert(name.to_string(), func);
    }

    /// Look up a procedure by name
    pub fn get(&self, name: &str) -> Option<&ProcedureFn> {
        self.procedures.get(name)
    }

    /// Check if a procedure exists
    pub fn contains(&self, name: &str) -> bool {
        self.procedures.contains_key(name)
    }

    /// Get all registered procedure names
    pub fn names(&self) -> Vec<&str> {
        self.procedures.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ProcedureRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = ProcedureRegistry::new();
        // Now we have 11 procedures registered (6 core + 1 apoc + 4 dbms stubs)
        assert_eq!(registry.names().len(), 11);

        // Verify all expected procedures are registered
        assert!(registry.contains("db.labels"));
        assert!(registry.contains("db.relationshipTypes"));
        assert!(registry.contains("dbms.components"));
        assert!(registry.contains("db.propertyKeys"));
        assert!(registry.contains("db.schema.nodeTypeProperties"));
        assert!(registry.contains("db.schema.relTypeProperties"));
        assert!(registry.contains("dbms.clientConfig"));
        assert!(registry.contains("dbms.security.showCurrentUser"));
        assert!(registry.contains("dbms.procedures"));
        assert!(registry.contains("dbms.functions"));
        assert!(registry.contains("apoc.meta.schema"));
    }

    #[test]
    fn test_registry_register_and_lookup() {
        let mut registry = ProcedureRegistry::new();

        // Should already have 11 built-in procedures (6 core + 1 apoc + 4 dbms stubs)
        assert_eq!(registry.names().len(), 11);

        // Register a dummy procedure
        let dummy_proc: ProcedureFn = Arc::new(|_schema| {
            Ok(vec![HashMap::from([(
                "result".to_string(),
                serde_json::json!("test"),
            )])])
        });

        registry.register("test.procedure", dummy_proc);

        assert!(registry.contains("test.procedure"));
        assert!(registry.get("test.procedure").is_some());
        assert_eq!(registry.names().len(), 12); // 11 built-in + 1 test
    }

    #[test]
    fn test_registry_lookup_missing() {
        let registry = ProcedureRegistry::new();
        assert!(!registry.contains("missing.procedure"));
        assert!(registry.get("missing.procedure").is_none());
    }
}
