//! The context a variable-length walk's CTE is generated from
//! (`bound_plan::lower::path` → `cte_manager`).

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::render_plan::variable_length::VariableLengthSpec;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
pub struct CteGenerationContext {
    /// Graph schema for this query (enables multi-schema support)
    schema: Option<GraphSchema>,
    /// Variable length specification for the path pattern
    pub spec: VariableLengthSpec,
    /// Relationship types for the VLP pattern (its edge constraints)
    pub relationship_types: Option<Vec<String>>,
    /// Edge ID identifier from schema (for RETURN relationships(p))
    pub edge_id: Option<Identifier>,
    /// The bound plan's paths as values (`PathValues`): each path's nodes
    /// and relationships accumulated as `path_node_values` /
    /// `path_rel_values`.
    pub path_values: crate::clickhouse_query_generator::variable_length_cte::PathValues,
    /// The relation the walk reads in place of the edge table (the bound
    /// plan's undirected relationship: its table in both directions, with
    /// the walked columns named in the pattern context).
    pub walk_relation: Option<String>,
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_spec(mut self, spec: VariableLengthSpec) -> Self {
        self.spec = spec;
        self
    }

    /// Set relationship types
    pub(crate) fn with_relationship_types(mut self, types: Option<Vec<String>>) -> Self {
        self.relationship_types = types;
        self
    }

    /// Set edge ID from schema
    pub(crate) fn with_edge_id(mut self, edge_id: Option<Identifier>) -> Self {
        self.edge_id = edge_id;
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
