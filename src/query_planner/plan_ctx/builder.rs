//! PlanCtx Builder Pattern
//!
//! This module provides a fluent builder API for constructing PlanCtx instances.
//! It replaces the multiple constructors (new, with_tenant, with_parameters, etc.)
//! with a single flexible builder pattern.
//!
//! # Example
//!
//! ```ignore
//! let plan_ctx = PlanCtxBuilder::new(schema)
//!     .tenant_id("tenant-123".to_string())
//!     .view_parameters(params)
//!     .max_inferred_types(10)
//!     .build();
//! ```

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    graph_catalog::{graph_schema::GraphSchema, pattern_schema::PatternSchemaContext},
    query_planner::{
        analyzer::property_requirements::PropertyRequirements, join_context::VlpEndpointInfo,
        typed_variable::VariableRegistry,
    },
};

use super::{PlanCtx, TableCtx};

/// Builder for PlanCtx with fluent API.
///
/// All fields have sensible defaults; only `schema` is required.
pub struct PlanCtxBuilder {
    // Required
    schema: Arc<GraphSchema>,

    // Optional with defaults
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
    max_inferred_types: usize,
    parent_scope: Option<Box<PlanCtx>>,
    is_with_scope: bool,

    // Internal state (typically defaults)
    alias_table_ctx_map: HashMap<String, TableCtx>,
    optional_aliases: HashSet<String>,
    cte_counter: usize,
    cte_columns: HashMap<String, HashMap<String, String>>,
    cte_entity_types: HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>>,
    property_requirements: Option<PropertyRequirements>,
    pattern_contexts: HashMap<String, Arc<PatternSchemaContext>>,
    vlp_endpoints: HashMap<String, VlpEndpointInfo>,
    variables: VariableRegistry,
    cte_alias_sources: HashMap<String, (String, String)>,
}

impl PlanCtxBuilder {
    /// Create a new builder with the required schema.
    pub fn new(schema: Arc<GraphSchema>) -> Self {
        PlanCtxBuilder {
            schema,
            tenant_id: None,
            view_parameter_values: None,
            max_inferred_types: 5, // Default value
            parent_scope: None,
            is_with_scope: false,
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            pattern_contexts: HashMap::new(),
            vlp_endpoints: HashMap::new(),
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
        }
    }

    /// Create a builder from a parent scope (for WITH clause scoping).
    /// Inherits schema, tenant_id, view_parameters, and vlp_endpoints.
    pub fn from_parent(parent: &PlanCtx, is_with_scope: bool) -> Self {
        PlanCtxBuilder {
            schema: parent.schema.clone(),
            tenant_id: parent.tenant_id.clone(),
            view_parameter_values: parent.view_parameter_values.clone(),
            max_inferred_types: parent.max_inferred_types,
            parent_scope: Some(Box::new(parent.clone())),
            is_with_scope,
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            pattern_contexts: HashMap::new(),
            vlp_endpoints: parent.vlp_endpoints.clone(),
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
        }
    }

    /// Set the tenant ID for multi-tenant deployments.
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set the tenant ID optionally.
    pub fn tenant_id_opt(mut self, tenant_id: Option<String>) -> Self {
        self.tenant_id = tenant_id;
        self
    }

    /// Set view parameter values for parameterized views.
    pub fn view_parameters(mut self, params: HashMap<String, String>) -> Self {
        self.view_parameter_values = Some(params);
        self
    }

    /// Set view parameter values optionally.
    pub fn view_parameters_opt(mut self, params: Option<HashMap<String, String>>) -> Self {
        self.view_parameter_values = params;
        self
    }

    /// Set maximum number of inferred edge types.
    pub fn max_inferred_types(mut self, max: usize) -> Self {
        self.max_inferred_types = max;
        self
    }

    /// Mark this scope as a WITH scope (barrier for variable lookup).
    pub fn as_with_scope(mut self) -> Self {
        self.is_with_scope = true;
        self
    }

    /// Set the parent scope explicitly.
    pub fn parent_scope(mut self, parent: PlanCtx) -> Self {
        self.parent_scope = Some(Box::new(parent));
        self
    }

    /// Initialize with existing table contexts.
    pub fn with_table_contexts(mut self, contexts: HashMap<String, TableCtx>) -> Self {
        self.alias_table_ctx_map = contexts;
        self
    }

    /// Initialize with existing optional aliases.
    pub fn with_optional_aliases(mut self, aliases: HashSet<String>) -> Self {
        self.optional_aliases = aliases;
        self
    }

    /// Set the CTE counter (for generating unique CTE names).
    pub fn cte_counter(mut self, counter: usize) -> Self {
        self.cte_counter = counter;
        self
    }

    /// Set property requirements.
    pub fn property_requirements(mut self, requirements: PropertyRequirements) -> Self {
        self.property_requirements = Some(requirements);
        self
    }

    /// Set VLP endpoints.
    pub fn vlp_endpoints(mut self, endpoints: HashMap<String, VlpEndpointInfo>) -> Self {
        self.vlp_endpoints = endpoints;
        self
    }

    /// Set the variable registry.
    pub fn variables(mut self, registry: VariableRegistry) -> Self {
        self.variables = registry;
        self
    }

    /// Build the PlanCtx instance.
    pub fn build(self) -> PlanCtx {
        PlanCtx {
            alias_table_ctx_map: self.alias_table_ctx_map,
            optional_aliases: self.optional_aliases,
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema: self.schema,
            tenant_id: self.tenant_id,
            view_parameter_values: self.view_parameter_values,
            denormalized_node_edges: HashMap::new(),
            parent_scope: self.parent_scope,
            is_with_scope: self.is_with_scope,
            cte_counter: self.cte_counter,
            cte_columns: self.cte_columns,
            cte_entity_types: self.cte_entity_types,
            property_requirements: self.property_requirements,
            max_inferred_types: self.max_inferred_types,
            pattern_contexts: self.pattern_contexts,
            vlp_endpoints: self.vlp_endpoints,
            variables: self.variables,
            cte_alias_sources: self.cte_alias_sources,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_schema() -> Arc<GraphSchema> {
        Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    #[test]
    fn test_builder_minimal() {
        let schema = make_test_schema();
        let ctx = PlanCtxBuilder::new(schema.clone()).build();

        assert_eq!(ctx.schema().database(), "test");
        assert!(ctx.tenant_id.is_none());
        assert_eq!(ctx.max_inferred_types, 5);
    }

    #[test]
    fn test_builder_with_tenant() {
        let schema = make_test_schema();
        let ctx = PlanCtxBuilder::new(schema).tenant_id("tenant-123").build();

        assert_eq!(ctx.tenant_id, Some("tenant-123".to_string()));
    }

    #[test]
    fn test_builder_with_all_params() {
        let schema = make_test_schema();
        let mut params = HashMap::new();
        params.insert("region".to_string(), "US".to_string());

        let ctx = PlanCtxBuilder::new(schema)
            .tenant_id("tenant-123")
            .view_parameters(params.clone())
            .max_inferred_types(10)
            .build();

        assert_eq!(ctx.tenant_id, Some("tenant-123".to_string()));
        assert_eq!(ctx.view_parameter_values, Some(params));
        assert_eq!(ctx.max_inferred_types, 10);
    }

    #[test]
    fn test_builder_from_parent() {
        let schema = make_test_schema();
        let parent = PlanCtxBuilder::new(schema.clone())
            .tenant_id("parent-tenant")
            .max_inferred_types(15)
            .build();

        let child = PlanCtxBuilder::from_parent(&parent, true).build();

        assert_eq!(child.tenant_id, Some("parent-tenant".to_string()));
        assert_eq!(child.max_inferred_types, 15);
        assert!(child.is_with_scope);
        assert!(child.parent_scope.is_some());
    }

    #[test]
    fn test_builder_as_with_scope() {
        let schema = make_test_schema();
        let ctx = PlanCtxBuilder::new(schema).as_with_scope().build();

        assert!(ctx.is_with_scope);
    }
}
