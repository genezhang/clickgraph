//! The variable-length walk's CTE: the standard layout's tables and columns
//! handed to the generator (`sql_generator/emitters/clickhouse/
//! variable_length_cte.rs`), for `bound_plan::lower::path`.

use crate::clickhouse_query_generator::variable_length_cte::{
    NodeProperty, VariableLengthCteGenerator,
};
use crate::graph_catalog::{EdgeAccessStrategy, NodeAccessStrategy, PatternSchemaContext};
use crate::render_plan::cte_generation::CteGenerationContext;
use crate::render_plan::filter_pipeline::CategorizedFilters;

/// Unified error type for CTE operations
#[derive(Debug, thiserror::Error)]
pub enum CteError {
    #[error("Invalid strategy for pattern: {0}")]
    InvalidStrategy(String),

    #[error("Schema validation failed: {0}")]
    SchemaValidationError(String),
}

/// Result of CTE SQL generation
#[derive(Debug, Clone)]
pub struct CteGenerationResult {
    pub sql: String,
    pub cte_name: String,
}

/// Main entry point for CTE generation
pub struct CteManager {
    context: CteGenerationContext,
}

impl CteManager {
    /// A CTE manager for `context`'s walk (its schema: the context's).
    pub fn with_context(context: CteGenerationContext) -> Self {
        Self { context }
    }

    /// The walk's CTE (`pattern_ctx`: the standard layout's nodes and edge;
    /// `properties`: the node properties the CTE carries; `filters`: the
    /// start node's and the relationship's pre-rendered SQL).
    pub fn generate_vlp_cte(
        &self,
        pattern_ctx: &PatternSchemaContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        log::debug!(
            "CteManager::generate_vlp_cte for {} -[*]-> {}",
            pattern_ctx.left_node_alias,
            pattern_ctx.right_node_alias
        );
        VariableLengthCteStrategy::new(pattern_ctx)?.generate_sql(
            &self.context,
            properties,
            filters,
        )
    }
}

/// The standard layout's walk (the nodes and the edge each in their own
/// table): the one shape `bound_plan::lower::path` hands the generator.
pub struct VariableLengthCteStrategy {
    pattern_ctx: PatternSchemaContext,
    start_table: String,
    start_id_col: String,
    end_table: String,
    end_id_col: String,
    rel_table: String,
    rel_from_col: String,
    rel_to_col: String,
}

impl VariableLengthCteStrategy {
    /// The tables and columns of `pattern_ctx`'s nodes and edge; an error
    /// for any layout but the standard one (the caller gates it first).
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        let own_table = |node: &NodeAccessStrategy| match node {
            NodeAccessStrategy::OwnTable {
                table, id_column, ..
            } => Ok((table.clone(), id_column.to_string())),
            _ => Err(CteError::InvalidStrategy(
                "internal: a walk node outside its own table".into(),
            )),
        };
        let (start_table, start_id_col) = own_table(&pattern_ctx.left_node)?;
        let (end_table, end_id_col) = own_table(&pattern_ctx.right_node)?;
        let EdgeAccessStrategy::SeparateTable {
            table,
            from_id,
            to_id,
            ..
        } = &pattern_ctx.edge
        else {
            return Err(CteError::InvalidStrategy(
                "internal: a walk edge outside its own table".into(),
            ));
        };
        Ok(Self {
            pattern_ctx: pattern_ctx.clone(),
            start_table,
            start_id_col,
            end_table,
            end_id_col,
            rel_table: table.clone(),
            rel_from_col: from_id.clone(),
            rel_to_col: to_id.clone(),
        })
    }

    /// Generate SQL using the wrapped VariableLengthCteGenerator
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // We need the schema to create the generator
        let schema = context.schema().ok_or_else(|| {
            CteError::SchemaValidationError("Schema required for VLP generation".into())
        })?;

        let mut generator = VariableLengthCteGenerator::new(
            schema,
            context.spec.clone(),
            &self.start_table,
            &self.start_id_col,
            &self.rel_table,
            &self.rel_from_col,
            &self.rel_to_col,
            &self.end_table,
            &self.end_id_col,
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties.to_vec(),
            filters.start_sql.clone(),
            filters.relationship_sql.clone(),
            context.relationship_types.clone(),
            context.edge_id.clone(),
        );

        // The bound plan's paths as values: set only by its standard-layout
        // call (`bound_plan::lower::path`), emitted by the standard arms.
        generator.path_values = context.path_values.clone();
        generator.walk_relation = context.walk_relation.clone();

        // Generate the CTE using the comprehensive generator
        let cte = generator.generate_cte();

        // Convert to CteGenerationResult
        let cte_name = cte.cte_name.clone();
        // Extract SQL from CteContent - VLP CTEs always use RawSql
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s.clone(),
            crate::render_plan::CteContent::Structured(_) => {
                return Err(CteError::InvalidStrategy(
                    "VLP CTE should use RawSql, not Structured content".into(),
                ));
            }
        };

        Ok(CteGenerationResult { sql, cte_name })
    }
}
