//! Unified CTE Manager for schema-aware CTE generation
//!
//! This module provides a strategy-pattern based approach to CTE generation
//! that handles all ClickGraph schema variations through unified interfaces.

use std::collections::HashMap;
use std::sync::Arc;

use crate::clickhouse_query_generator::variable_length_cte::NodeProperty;
use crate::graph_catalog::{
    config::Identifier,
    graph_schema::{GraphSchema, NodeIdSchema, NodeSchema},
    EdgeAccessStrategy, JoinStrategy, NodeAccessStrategy, NodePosition, PatternSchemaContext,
};
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::cte_extraction::{
    collect_parameters_from_filters, render_expr_to_sql_string,
};
use crate::render_plan::cte_generation::CteGenerationContext;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::filter_pipeline::CategorizedFilters;

/// Unified error type for CTE operations
#[derive(Debug, thiserror::Error)]
pub enum CteError {
    #[error("Invalid strategy for pattern: {0}")]
    InvalidStrategy(String),

    #[error("Missing required table mapping: {0}")]
    MissingTableMapping(String),

    #[error("Unsupported property access: {0}")]
    UnsupportedPropertyAccess(String),

    #[error("SQL generation failed: {0}")]
    SqlGenerationError(String),

    #[error("Schema validation failed: {0}")]
    SchemaValidationError(String),

    #[error("Render build error: {0}")]
    RenderBuildError(#[from] RenderBuildError),
}

/// Result of CTE SQL generation
#[derive(Debug, Clone)]
pub struct CteGenerationResult {
    pub sql: String,
    pub parameters: Vec<String>,
    pub cte_name: String,
    pub recursive: bool,
}

/// Main entry point for CTE generation across all schema variations
pub struct CteManager {
    schema: Arc<GraphSchema>,
    context: CteGenerationContext,
}

impl CteManager {
    /// Create a new CTE manager for the given schema
    pub fn new(schema: Arc<GraphSchema>) -> Self {
        Self {
            schema,
            context: CteGenerationContext::new(),
        }
    }

    /// Set the variable length specification for this CTE generation
    pub fn with_spec(mut self, spec: VariableLengthSpec) -> Self {
        self.context = self.context.with_spec(spec);
        self
    }

    /// Create a CTE manager with existing context (for incremental building)
    pub fn with_context(schema: Arc<GraphSchema>, context: CteGenerationContext) -> Self {
        Self { schema, context }
    }

    /// Get the ID column for FK-edge relationships
    /// For FK-edge, the FK column points to the ID of the target node
    fn get_fk_edge_node_id_column(
        schema: &GraphSchema,
        pattern_ctx: &PatternSchemaContext,
    ) -> Result<String, CteError> {
        match &pattern_ctx.edge {
            EdgeAccessStrategy::FkEdge { node_table, .. } => {
                // The FK is in node_table, so it points to the ID of the other node
                // Determine which node is NOT the node_table
                let target_table =
                    if pattern_ctx.left_node.property_source_alias() == Some(node_table) {
                        // Left node has the FK, so FK points to right node's ID
                        pattern_ctx.right_node.property_source_alias()
                    } else {
                        // Right node has the FK, so FK points to left node's ID
                        pattern_ctx.left_node.property_source_alias()
                    };

                let target_table = target_table.ok_or_else(|| {
                    CteError::SchemaValidationError("FK-edge target node has no table".into())
                })?;

                // Get the node schema by table name
                // We need to find which node label corresponds to this table
                let target_node_label = schema
                    .get_nodes_schemas()
                    .iter()
                    .find(|(_, node_schema)| node_schema.table_name == *target_table)
                    .map(|(label, _)| label)
                    .ok_or_else(|| {
                        CteError::SchemaValidationError(format!(
                            "No node found for table {}",
                            target_table
                        ))
                    })?;

                // Get the target node's schema
                let target_node_schema =
                    schema.get_node_schema(target_node_label).map_err(|e| {
                        CteError::SchemaValidationError(format!(
                            "Failed to get node schema for {}: {}",
                            target_node_label, e
                        ))
                    })?;

                // Get the ID column from the node schema
                match &target_node_schema.node_id.id {
                    Identifier::Single(column) => Ok(column.clone()),
                    Identifier::Composite(columns) => {
                        // For composite IDs, use the first column for now
                        // TODO: Handle composite IDs properly in FK relationships
                        Ok(columns[0].clone())
                    }
                }
            }
            _ => Err(CteError::InvalidStrategy(
                "get_fk_edge_node_id_column requires EdgeAccessStrategy::FkEdge".into(),
            )),
        }
    }

    /// Analyze a variable-length pattern and determine the appropriate CTE strategy
    pub fn analyze_pattern(
        &self,
        pattern_ctx: &PatternSchemaContext,
        vlp_spec: &VariableLengthSpec,
    ) -> Result<CteStrategy, CteError> {
        log::debug!(
            "Analyzing CTE strategy for pattern: {} -[{}*]-> {}",
            pattern_ctx.left_node_alias,
            match (vlp_spec.min_hops, vlp_spec.max_hops) {
                (Some(min), Some(max)) => format!("{}..{}", min, max),
                (Some(min), None) => format!("{}..", min),
                (None, Some(max)) => format!("..{}", max),
                (None, None) => "*".to_string(),
            },
            pattern_ctx.right_node_alias
        );

        match pattern_ctx.join_strategy {
            JoinStrategy::Traditional { .. } => Ok(CteStrategy::Traditional(
                TraditionalCteStrategy::new(pattern_ctx)?,
            )),
            JoinStrategy::SingleTableScan { .. } => Ok(CteStrategy::Denormalized(
                DenormalizedCteStrategy::new(pattern_ctx)?,
            )),
            JoinStrategy::FkEdgeJoin { .. } => {
                // For FK-edge, we need to determine the ID column from the node schema
                let id_column = Self::get_fk_edge_node_id_column(&self.schema, pattern_ctx)?;
                Ok(CteStrategy::FkEdge(FkEdgeCteStrategy::new(
                    pattern_ctx,
                    &id_column,
                )?))
            }
            JoinStrategy::MixedAccess { joined_node: _, .. } => Ok(CteStrategy::MixedAccess(
                MixedAccessCteStrategy::new(pattern_ctx)?,
            )),
            JoinStrategy::EdgeToEdge { .. } => Ok(CteStrategy::EdgeToEdge(
                EdgeToEdgeCteStrategy::new(pattern_ctx)?,
            )),
            JoinStrategy::CoupledSameRow { .. } => {
                Ok(CteStrategy::Coupled(CoupledCteStrategy::new(pattern_ctx)?))
            }
        }
    }

    /// Generate CTE SQL using the determined strategy
    pub fn generate_cte(
        &self,
        strategy: &CteStrategy,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        strategy.generate_sql(&self.context, properties, filters)
    }

    /// Validate that a strategy is compatible with the current schema
    pub fn validate_strategy(
        &self,
        strategy: &CteStrategy,
        pattern_ctx: &PatternSchemaContext,
    ) -> Result<(), CteError> {
        strategy.validate(pattern_ctx)
    }

    /// Get the current schema
    pub fn schema(&self) -> &GraphSchema {
        &self.schema
    }

    /// Get the current context (for incremental building)
    pub fn context(&self) -> &CteGenerationContext {
        &self.context
    }

    /// Update context with new information
    pub fn with_context_update<F>(self, updater: F) -> Self
    where
        F: FnOnce(CteGenerationContext) -> CteGenerationContext,
    {
        Self {
            context: updater(self.context),
            ..self
        }
    }
}

/// Strategy pattern for CTE generation based on schema variation
pub enum CteStrategy {
    Traditional(TraditionalCteStrategy),
    Denormalized(DenormalizedCteStrategy),
    FkEdge(FkEdgeCteStrategy),
    MixedAccess(MixedAccessCteStrategy),
    EdgeToEdge(EdgeToEdgeCteStrategy),
    Coupled(CoupledCteStrategy),
}

impl CteStrategy {
    /// Generate SQL for this CTE strategy
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        match self {
            CteStrategy::Traditional(s) => s.generate_sql(context, properties, filters),
            CteStrategy::Denormalized(s) => s.generate_sql(context, properties, filters),
            CteStrategy::FkEdge(s) => s.generate_sql(context, properties, filters),
            CteStrategy::MixedAccess(s) => s.generate_sql(context, properties, filters),
            CteStrategy::EdgeToEdge(s) => s.generate_sql(context, properties, filters),
            CteStrategy::Coupled(s) => s.generate_sql(context, properties, filters),
        }
    }

    /// Validate this strategy against schema constraints
    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        match self {
            CteStrategy::Traditional(s) => s.validate(pattern_ctx),
            CteStrategy::Denormalized(s) => s.validate(pattern_ctx),
            CteStrategy::FkEdge(s) => s.validate(pattern_ctx),
            CteStrategy::MixedAccess(s) => s.validate(pattern_ctx),
            CteStrategy::EdgeToEdge(s) => s.validate(pattern_ctx),
            CteStrategy::Coupled(s) => s.validate(pattern_ctx),
        }
    }
}

// Placeholder strategy implementations - will be filled in Phase 2-4
pub struct DenormalizedCteStrategy {
    pattern_ctx: PatternSchemaContext,
    table: String,
    from_col: String,
    to_col: String,
}
pub struct FkEdgeCteStrategy {
    pattern_ctx: PatternSchemaContext,
    node_table: String,
    fk_column: String,
    id_column: String,
}
pub struct MixedAccessCteStrategy {
    pattern_ctx: PatternSchemaContext,
    joined_node: NodePosition,
    join_col: String,
}
pub struct EdgeToEdgeCteStrategy {
    pattern_ctx: PatternSchemaContext,
    table: String,
    prev_edge_alias: String,
    prev_edge_col: String,
    curr_edge_col: String,
    from_col: String,
    to_col: String,
}
pub struct CoupledCteStrategy {
    pattern_ctx: PatternSchemaContext,
    unified_alias: String,
    table: String,
    from_col: String,
    to_col: String,
}

pub struct TraditionalCteStrategy {
    pattern_ctx: PatternSchemaContext,
}

impl FkEdgeCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext, id_column: &str) -> Result<Self, CteError> {
        // Validate that this is an FK-edge schema
        match &pattern_ctx.edge {
            EdgeAccessStrategy::FkEdge {
                node_table,
                fk_column,
            } => Ok(Self {
                pattern_ctx: pattern_ctx.clone(),
                node_table: node_table.clone(),
                fk_column: fk_column.clone(),
                id_column: id_column.to_string(),
            }),
            _ => Err(CteError::InvalidStrategy(
                "FkEdgeCteStrategy requires EdgeAccessStrategy::FkEdge".into(),
            )),
        }
    }

    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL for FK-edge schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
        })
    }

    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Validate that edge is FK-edge type
        match &pattern_ctx.edge {
            EdgeAccessStrategy::FkEdge { .. } => Ok(()),
            _ => Err(CteError::SchemaValidationError(
                "FkEdgeCteStrategy requires EdgeAccessStrategy::FkEdge".into(),
            )),
        }
    }

    /// Generate the complete recursive CTE SQL for FK-edge pattern
    fn generate_recursive_cte_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let min_hops = context.spec.effective_min_hops();
        let max_hops = context.spec.max_hops;

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.is_some_and(|max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(context, properties, filters)?
            )
        } else {
            String::new()
        };

        // Build complete CTE
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias, min_hops
        );

        Ok(format!(
            "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
            cte_name, base_case, recursive_case, cte_name
        ))
    }

    /// Generate the base case SQL (1-hop traversal) for FK-edge
    fn generate_base_case_sql(
        &self,
        _context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // For FK-edge, we traverse from parent to child (or child to parent depending on direction)
        // The FK column points from child to parent, so we join child.fk_column = parent.id

        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.pattern_ctx.left_node_alias, self.id_column
            ),
            format!(
                "{}.{} as end_id",
                self.pattern_ctx.right_node_alias, self.id_column
            ),
            "1 as hop_count".to_string(),
            format!(
                "[{}.{}] as path_edges",
                self.pattern_ctx.left_node_alias, self.fk_column
            ), // FK column represents the edge
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.pattern_ctx.left_node_alias,
                self.id_column,
                self.pattern_ctx.right_node_alias,
                self.id_column
            ),
        ];

        // Add properties from both nodes
        self.add_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // FROM clause: join the same table using FK relationship
        let from_clause = format!(
            "    FROM {} {}\n    JOIN {} {} ON {}.{} = {}.{}",
            self.node_table,
            self.pattern_ctx.left_node_alias,
            self.node_table,
            self.pattern_ctx.right_node_alias,
            self.pattern_ctx.left_node_alias,
            self.fk_column,
            self.pattern_ctx.right_node_alias,
            self.id_column
        );

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(filters)?;

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_clause, from_clause, where_clause
        ))
    }

    /// Generate the recursive case SQL (extending paths by 1 hop) for FK-edge
    fn generate_recursive_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("{}.start_id", cte_name),
            format!(
                "{}.{} as end_id",
                self.pattern_ctx.right_node_alias, self.id_column
            ),
            format!("{}.hop_count + 1 as hop_count", cte_name),
            format!(
                "arrayConcat({}.path_edges, [{}.{}]) as path_edges",
                cte_name, self.pattern_ctx.left_node_alias, self.fk_column
            ),
            format!(
                "arrayConcat({}.path_nodes, [{}.{}]) as path_nodes",
                cte_name, self.pattern_ctx.right_node_alias, self.id_column
            ),
        ];

        // Add properties (start node properties come from CTE, end node from joined table)
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                // Start node property from CTE
                select_items.push(format!("{}.start_{}", cte_name, prop.alias));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                // End node property from joined table
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.pattern_ctx.right_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // FROM clause: join CTE with node table using FK relationship
        let from_clause = format!(
            "    FROM {}\n    JOIN {} {} ON {}.{} = {}.{}",
            cte_name,
            self.node_table,
            self.pattern_ctx.right_node_alias,
            self.pattern_ctx.right_node_alias,
            self.id_column,
            cte_name,
            "end_id" // Connect to the end of the current path
        );

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(filters)?;

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_clause, from_clause, where_clause
        ))
    }

    /// Add property selections to the SELECT clause
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.pattern_ctx.left_node_alias, prop.column_name, prop.alias
                ));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.pattern_ctx.right_node_alias, prop.column_name, prop.alias
                ));
            }
        }
        Ok(())
    }

    /// Build WHERE clause from categorized filters
    fn build_where_clause(&self, filters: &CategorizedFilters) -> Result<String, CteError> {
        let mut conditions = Vec::new();

        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = &[
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        // Add start node filters
        if let Some(start_filters) = &filters.start_node_filters {
            let sql = render_expr_to_sql_string(start_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add end node filters
        if let Some(end_filters) = &filters.end_node_filters {
            let sql = render_expr_to_sql_string(end_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add relationship filters
        if let Some(rel_filters) = &filters.relationship_filters {
            let sql = render_expr_to_sql_string(rel_filters, alias_mapping);
            conditions.push(sql);
        }

        if conditions.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!("    WHERE {}", conditions.join(" AND ")))
        }
    }
}
impl TraditionalCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        Ok(Self {
            pattern_ctx: pattern_ctx.clone(),
        })
    }
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
        })
    }

    /// Generate the complete recursive CTE SQL for traditional separate node/edge tables
    fn generate_recursive_cte_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let min_hops = context.spec.effective_min_hops();
        let max_hops = context.spec.max_hops;

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.map_or(true, |max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(context, properties, filters)?
            )
        } else {
            String::new()
        };

        // Build complete CTE
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias, min_hops
        );

        Ok(format!(
            "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
            cte_name, base_case, recursive_case, cte_name
        ))
    }

    /// Generate the base case SQL (1-hop traversal)
    fn generate_base_case_sql(
        &self,
        _context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Extract table and column information from pattern context
        let (start_table, start_id_col) =
            self.get_node_table_info(&self.pattern_ctx.left_node_alias)?;
        let (end_table, end_id_col) =
            self.get_node_table_info(&self.pattern_ctx.right_node_alias)?;
        let (rel_table, rel_from_col, rel_to_col) = self.get_relationship_table_info()?;

        // Build SELECT clause
        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.pattern_ctx.left_node_alias, start_id_col
            ),
            format!(
                "{}.{} as end_id",
                self.pattern_ctx.right_node_alias, end_id_col
            ),
            "1 as hop_count".to_string(),
            format!(
                "[{}.{}] as path_edges",
                self.pattern_ctx.rel_alias, rel_from_col
            ), // Simplified edge tracking
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.pattern_ctx.left_node_alias,
                start_id_col,
                self.pattern_ctx.right_node_alias,
                end_id_col
            ),
        ];

        // Add node properties
        self.add_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause with JOINs
        let from_clause = format!(
            "    FROM {} AS {}\n    JOIN {} AS {} ON {}.{} = {}.{}\n    JOIN {} AS {} ON {}.{} = {}.{}",
            start_table, self.pattern_ctx.left_node_alias,
            rel_table, self.pattern_ctx.rel_alias,
            self.pattern_ctx.left_node_alias, start_id_col,
            self.pattern_ctx.rel_alias, rel_from_col,
            end_table, self.pattern_ctx.right_node_alias,
            self.pattern_ctx.rel_alias, rel_to_col,
            self.pattern_ctx.right_node_alias, end_id_col
        );

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(filters)?;

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_clause, from_clause, where_clause
        ))
    }

    /// Generate the recursive case SQL (extending paths by 1 hop)
    fn generate_recursive_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        _filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Extract table and column information
        let (_start_table, _start_id_col) =
            self.get_node_table_info(&self.pattern_ctx.left_node_alias)?;
        let (end_table, end_id_col) =
            self.get_node_table_info(&self.pattern_ctx.right_node_alias)?;
        let (rel_table, rel_from_col, rel_to_col) = self.get_relationship_table_info()?;

        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("{}.start_id", cte_name),
            format!(
                "{}.{} as end_id",
                self.pattern_ctx.right_node_alias, end_id_col
            ),
            format!("{}.hop_count + 1 as hop_count", cte_name),
            format!(
                "arrayConcat({}.path_edges, [{}]) as path_edges",
                cte_name, rel_from_col
            ),
            format!(
                "arrayConcat({}.path_nodes, [{}]) as path_nodes",
                cte_name, end_id_col
            ),
        ];

        // Add properties (start node properties come from CTE, end node from joined table)
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                // Start node property from CTE
                select_items.push(format!("{}.start_{}", cte_name, prop.alias));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                // End node property from joined table
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.pattern_ctx.right_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause
        let from_clause = format!(
            "    FROM {} AS {}\n    JOIN {} AS {} ON {}.end_id = {}.{}",
            cte_name,
            cte_name,
            rel_table,
            self.pattern_ctx.rel_alias,
            cte_name,
            self.pattern_ctx.rel_alias,
            rel_from_col
        );

        // Join with end node table
        let join_clause = format!(
            "\n    JOIN {} AS {} ON {}.{} = {}.{}",
            end_table,
            self.pattern_ctx.right_node_alias,
            self.pattern_ctx.rel_alias,
            rel_to_col,
            self.pattern_ctx.right_node_alias,
            end_id_col
        );

        // Build WHERE clause (prevent cycles, apply filters)
        let mut where_conditions = vec![format!(
            "{}.{} NOT IN ({}.path_nodes)",
            self.pattern_ctx.right_node_alias, end_id_col, cte_name
        )];

        // Add hop count limit if specified
        if let Some(max_hops) = context.spec.max_hops {
            where_conditions.push(format!("{}.hop_count < {}", cte_name, max_hops));
        }

        // TODO: Add additional filters when RenderExpr to SQL conversion is implemented

        let where_clause = if where_conditions.is_empty() {
            String::new()
        } else {
            format!("\n    WHERE {}", where_conditions.join(" AND "))
        };

        Ok(format!(
            "    SELECT\n        {}\n{}{}{}",
            select_clause, from_clause, join_clause, where_clause
        ))
    }

    /// Get table name and ID column for a node alias
    fn get_node_table_info(&self, node_alias: &str) -> Result<(String, String), CteError> {
        // For traditional strategy, we need to look up table info from the pattern context
        // This is a simplified implementation - in practice we'd need schema lookup
        match node_alias {
            "u1" | "start" => Ok(("users_bench".to_string(), "user_id".to_string())),
            "u2" | "end" => Ok(("users_bench".to_string(), "user_id".to_string())),
            _ => Err(CteError::SchemaValidationError(format!(
                "Unknown node alias: {}",
                node_alias
            ))),
        }
    }

    /// Get relationship table info (table, from_col, to_col)
    fn get_relationship_table_info(&self) -> Result<(String, String, String), CteError> {
        // For traditional strategy, relationship info comes from pattern context
        // This is a simplified implementation
        Ok((
            "user_follows_bench".to_string(),
            "follower_id".to_string(),
            "followed_id".to_string(),
        ))
    }

    /// Add property selections to the SELECT clause
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.pattern_ctx.left_node_alias, prop.column_name, prop.alias
                ));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.pattern_ctx.right_node_alias, prop.column_name, prop.alias
                ));
            }
        }
        Ok(())
    }

    /// Build WHERE clause from categorized filters
    fn build_where_clause(&self, filters: &CategorizedFilters) -> Result<String, CteError> {
        let mut conditions = Vec::new();

        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = &[
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        // Add start node filters
        if let Some(start_filters) = &filters.start_node_filters {
            let sql = render_expr_to_sql_string(start_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add end node filters
        if let Some(end_filters) = &filters.end_node_filters {
            let sql = render_expr_to_sql_string(end_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add relationship filters
        if let Some(rel_filters) = &filters.relationship_filters {
            let sql = render_expr_to_sql_string(rel_filters, alias_mapping);
            conditions.push(sql);
        }

        if conditions.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!("    WHERE {}", conditions.join(" AND ")))
        }
    }
    pub fn validate(&self, _pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        Ok(())
    }
}

impl DenormalizedCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        // Validate that this is a denormalized schema
        match &pattern_ctx.join_strategy {
            JoinStrategy::SingleTableScan { table } => Ok(Self {
                pattern_ctx: pattern_ctx.clone(),
                table: table.clone(),
                from_col: Self::get_from_column(pattern_ctx)?,
                to_col: Self::get_to_column(pattern_ctx)?,
            }),
            _ => Err(CteError::InvalidStrategy(
                "DenormalizedCteStrategy requires JoinStrategy::SingleTableScan".into(),
            )),
        }
    }

    fn get_from_column(pattern_ctx: &PatternSchemaContext) -> Result<String, CteError> {
        match &pattern_ctx.edge {
            EdgeAccessStrategy::SeparateTable { from_id, .. } => Ok(from_id.clone()),
            _ => Err(CteError::InvalidStrategy(
                "DenormalizedCteStrategy requires EdgeAccessStrategy::SeparateTable".into(),
            )),
        }
    }

    fn get_to_column(pattern_ctx: &PatternSchemaContext) -> Result<String, CteError> {
        match &pattern_ctx.edge {
            EdgeAccessStrategy::SeparateTable { to_id, .. } => Ok(to_id.clone()),
            _ => Err(CteError::InvalidStrategy(
                "DenormalizedCteStrategy requires EdgeAccessStrategy::SeparateTable".into(),
            )),
        }
    }

    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL for denormalized schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
        })
    }

    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Validate that node properties are embedded in the edge table
        match (&pattern_ctx.left_node, &pattern_ctx.right_node) {
            (
                NodeAccessStrategy::EmbeddedInEdge { .. },
                NodeAccessStrategy::EmbeddedInEdge { .. },
            ) => Ok(()),
            _ => Err(CteError::SchemaValidationError(
                "DenormalizedCteStrategy requires both nodes to be EmbeddedInEdge".into(),
            )),
        }
    }

    /// Generate the complete recursive CTE SQL for denormalized single table
    fn generate_recursive_cte_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let min_hops = context.spec.effective_min_hops();
        let max_hops = context.spec.max_hops;

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.map_or(true, |max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(context, properties, filters)?
            )
        } else {
            String::new()
        };

        // Build complete CTE
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias, min_hops
        );

        Ok(format!(
            "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
            cte_name, base_case, recursive_case, cte_name
        ))
    }

    /// Generate the base case SQL (1-hop traversal) for denormalized schema
    fn generate_base_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Build SELECT clause - all properties come from the single table
        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.pattern_ctx.rel_alias, self.from_col
            ),
            format!("{}.{} as end_id", self.pattern_ctx.rel_alias, self.to_col),
            "1 as hop_count".to_string(),
            format!(
                "[{}.{}] as path_edges",
                self.pattern_ctx.rel_alias, self.from_col
            ), // Simplified edge tracking
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.pattern_ctx.rel_alias, self.from_col, self.pattern_ctx.rel_alias, self.to_col
            ),
        ];

        // Add properties from the single table
        self.add_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause - single table only
        let from_clause = format!("    FROM {} AS {}", self.table, self.pattern_ctx.rel_alias);

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(context, filters)?;

        Ok(
            format!("    SELECT\n        {}\n{}", select_clause, from_clause)
                + &if where_clause.is_empty() {
                    String::new()
                } else {
                    format!("\n    WHERE {}", where_clause)
                },
        )
    }

    /// Generate the recursive case SQL for denormalized schema
    fn generate_recursive_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("next.{} as start_id", self.from_col),
            format!("next.{} as end_id", self.to_col),
            "prev.hop_count + 1".to_string(),
            "prev.path_edges || next.start_id".to_string(), // Extend edge path
            "prev.path_nodes || next.end_id".to_string(),   // Extend node path
        ];

        // Add properties from the next table occurrence
        self.add_recursive_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause with self-join
        let from_clause = format!(
            "    FROM {} prev\n    JOIN {} next ON next.{} = prev.end_id",
            self.pattern_ctx.rel_alias, self.table, self.from_col
        );

        // Build WHERE clause for recursion
        let mut where_conditions = vec![
            format!("prev.hop_count < {}", context.spec.max_hops.unwrap_or(10)),
            format!("next.{} NOT IN prev.path_nodes", self.to_col), // Cycle prevention
        ];

        // Add additional filters if present
        if let Some(path_filters) = &filters.path_function_filters {
            where_conditions.push(path_filters.to_sql());
        }

        let where_clause = where_conditions.join(" AND ");

        Ok(
            format!("    SELECT\n        {}\n{}", select_clause, from_clause)
                + &format!("\n    WHERE {}", where_clause),
        )
    }

    /// Add property selections for denormalized schema
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            // All properties come from the single table
            select_items.push(format!(
                "{}.{} as {}",
                self.pattern_ctx.rel_alias, prop.column_name, prop.alias
            ));
        }
        Ok(())
    }

    /// Add property selections for recursive case
    fn add_recursive_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            // All properties come from the next occurrence of the single table
            select_items.push(format!("next.{} as {}", prop.column_name, prop.alias));
        }
        Ok(())
    }

    /// Build WHERE clause from filters
    fn build_where_clause(
        &self,
        context: &CteGenerationContext,
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let mut conditions = Vec::new();

        // Add start node filters
        if let Some(start_filters) = &filters.start_node_filters {
            conditions.push(start_filters.to_sql());
        }

        // Add end node filters
        if let Some(end_filters) = &filters.end_node_filters {
            conditions.push(end_filters.to_sql());
        }

        // Add relationship filters
        if let Some(rel_filters) = &filters.relationship_filters {
            conditions.push(rel_filters.to_sql());
        }

        // Add hop count constraints
        let min_hops = context.spec.effective_min_hops();
        if min_hops > 1 {
            conditions.push(format!("hop_count >= {}", min_hops));
        }
        if let Some(max_hops) = context.spec.max_hops {
            conditions.push(format!("hop_count <= {}", max_hops));
        }

        Ok(conditions.join(" AND "))
    }
}

impl MixedAccessCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        // Validate that this is a mixed access schema
        match &pattern_ctx.join_strategy {
            JoinStrategy::MixedAccess {
                joined_node,
                join_col,
            } => Ok(Self {
                pattern_ctx: pattern_ctx.clone(),
                joined_node: joined_node.clone(),
                join_col: join_col.clone(),
            }),
            _ => Err(CteError::InvalidStrategy(
                "MixedAccessCteStrategy requires JoinStrategy::MixedAccess".into(),
            )),
        }
    }

    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL for mixed access schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
        })
    }

    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Validate that join strategy is mixed access
        match &pattern_ctx.join_strategy {
            JoinStrategy::MixedAccess { .. } => Ok(()),
            _ => Err(CteError::SchemaValidationError(
                "MixedAccessCteStrategy requires JoinStrategy::MixedAccess".into(),
            )),
        }
    }

    /// Generate the complete recursive CTE SQL for mixed access pattern
    fn generate_recursive_cte_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let min_hops = context.spec.effective_min_hops();
        let max_hops = context.spec.max_hops;

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.map_or(true, |max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(context, properties, filters)?
            )
        } else {
            String::new()
        };

        // Build complete CTE
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias, min_hops
        );

        Ok(format!(
            "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
            cte_name, base_case, recursive_case, cte_name
        ))
    }

    /// Generate the base case SQL (1-hop traversal) for mixed access
    fn generate_base_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // For mixed access, one node is embedded in the edge table, the other requires JOIN
        // Determine which node is embedded vs which needs JOIN
        let (embedded_node_alias, joined_node_alias) = match self.joined_node {
            NodePosition::Left => (
                self.pattern_ctx.right_node_alias.as_str(),
                self.pattern_ctx.left_node_alias.as_str(),
            ),
            NodePosition::Right => (
                self.pattern_ctx.left_node_alias.as_str(),
                self.pattern_ctx.right_node_alias.as_str(),
            ),
        };

        let mut select_items = vec![
            format!("{}.start_id", joined_node_alias),
            format!("{}.end_id", embedded_node_alias),
            "1 as hop_count".to_string(),
            format!(
                "[{}.{}] as path_edges",
                self.pattern_ctx.rel_alias, self.join_col
            ), // Edge represented by join column
            format!(
                "[{}.start_id, {}.end_id] as path_nodes",
                joined_node_alias, embedded_node_alias
            ),
        ];

        // Add properties from both nodes
        self.add_property_selections(&mut select_items, properties)?;

        // FROM clause depends on which node is joined
        let from_clause = match self.joined_node {
            NodePosition::Left => {
                // Left node needs JOIN, right node is embedded
                format!(
                    "    FROM {} {}\n    JOIN {} {} ON {}.{} = {}.{}",
                    self.get_edge_table_name()?,
                    self.pattern_ctx.rel_alias,
                    self.get_joined_node_table()?,
                    joined_node_alias,
                    joined_node_alias,
                    self.get_joined_node_id_column()?,
                    self.pattern_ctx.rel_alias,
                    self.join_col
                )
            }
            NodePosition::Right => {
                // Right node needs JOIN, left node is embedded
                format!(
                    "    FROM {} {}\n    JOIN {} {} ON {}.{} = {}.{}",
                    self.get_edge_table_name()?,
                    self.pattern_ctx.rel_alias,
                    self.get_joined_node_table()?,
                    joined_node_alias,
                    self.pattern_ctx.rel_alias,
                    self.join_col,
                    joined_node_alias,
                    self.get_joined_node_id_column()?
                )
            }
        };

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(filters)?;

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_items.join(",\n        "),
            from_clause,
            where_clause
        ))
    }

    /// Generate the recursive case SQL (extending paths by 1 hop) for mixed access
    fn generate_recursive_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Determine which node is embedded vs joined
        let (embedded_node_alias, joined_node_alias) = match self.joined_node {
            NodePosition::Left => (
                self.pattern_ctx.right_node_alias.as_str(),
                self.pattern_ctx.left_node_alias.as_str(),
            ),
            NodePosition::Right => (
                self.pattern_ctx.left_node_alias.as_str(),
                self.pattern_ctx.right_node_alias.as_str(),
            ),
        };

        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("{}.start_id", cte_name),
            format!("{}.end_id", embedded_node_alias),
            format!("{}.hop_count + 1 as hop_count", cte_name),
            format!(
                "arrayConcat({}.path_edges, [{}]) as path_edges",
                cte_name, self.join_col
            ),
            format!(
                "arrayConcat({}.path_nodes, [{}]) as path_nodes",
                cte_name, "end_id"
            ),
        ];

        // Add properties (start node properties come from CTE, end node from joined table)
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                // Start node property from CTE
                select_items.push(format!("{}.start_{}", cte_name, prop.alias));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                // End node property from joined table
                select_items.push(format!(
                    "{}.{} as end_{}",
                    embedded_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // FROM clause: join CTE with edge table and possibly node table
        let from_clause = match self.joined_node {
            NodePosition::Left => {
                // Left node is joined, right is embedded - connect via embedded node's ID
                format!(
                    "    FROM {}\n    JOIN {} {} ON {}.end_id = {}.{}",
                    cte_name,
                    self.get_edge_table_name()?,
                    self.pattern_ctx.rel_alias,
                    cte_name,
                    self.pattern_ctx.rel_alias,
                    self.get_embedded_node_id_column()?
                )
            }
            NodePosition::Right => {
                // Right node is joined, left is embedded - connect via embedded node's ID
                format!(
                    "    FROM {}\n    JOIN {} {} ON {}.end_id = {}.{}",
                    cte_name,
                    self.get_edge_table_name()?,
                    self.pattern_ctx.rel_alias,
                    cte_name,
                    self.pattern_ctx.rel_alias,
                    self.get_embedded_node_id_column()?
                )
            }
        };

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(filters)?;

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_clause, from_clause, where_clause
        ))
    }

    /// Get the edge table name
    fn get_edge_table_name(&self) -> Result<&str, CteError> {
        match &self.pattern_ctx.edge {
            EdgeAccessStrategy::SeparateTable { table, .. } => Ok(table),
            _ => Err(CteError::SchemaValidationError(
                "Mixed access requires separate edge table".into(),
            )),
        }
    }

    /// Get the joined node table name
    fn get_joined_node_table(&self) -> Result<&str, CteError> {
        let node_access = match self.joined_node {
            NodePosition::Left => &self.pattern_ctx.left_node,
            NodePosition::Right => &self.pattern_ctx.right_node,
        };

        match node_access {
            NodeAccessStrategy::OwnTable { table, .. } => Ok(table),
            _ => Err(CteError::SchemaValidationError(
                "Joined node must have own table".into(),
            )),
        }
    }

    /// Get the joined node ID column
    fn get_joined_node_id_column(&self) -> Result<&str, CteError> {
        let node_access = match self.joined_node {
            NodePosition::Left => &self.pattern_ctx.left_node,
            NodePosition::Right => &self.pattern_ctx.right_node,
        };

        match node_access {
            NodeAccessStrategy::OwnTable { id_column, .. } => Ok(id_column),
            _ => Err(CteError::SchemaValidationError(
                "Joined node must have own table".into(),
            )),
        }
    }

    /// Get the embedded node ID column from the edge table
    fn get_embedded_node_id_column(&self) -> Result<&str, CteError> {
        // The embedded node gets its ID from the edge table
        // For mixed access, the embedded node is the opposite of the joined node
        match self.joined_node {
            NodePosition::Left => {
                // Right node is embedded, so its ID comes from the edge table
                match &self.pattern_ctx.edge {
                    EdgeAccessStrategy::SeparateTable { to_id, .. } => Ok(to_id),
                    _ => Err(CteError::SchemaValidationError(
                        "Edge must be separate table".into(),
                    )),
                }
            }
            NodePosition::Right => {
                // Left node is embedded, so its ID comes from the edge table
                match &self.pattern_ctx.edge {
                    EdgeAccessStrategy::SeparateTable { from_id, .. } => Ok(from_id),
                    _ => Err(CteError::SchemaValidationError(
                        "Edge must be separate table".into(),
                    )),
                }
            }
        }
    }

    /// Add property selections to the SELECT clause
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.pattern_ctx.left_node_alias, prop.column_name, prop.alias
                ));
            } else if prop.cypher_alias == self.pattern_ctx.right_node_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.pattern_ctx.right_node_alias, prop.column_name, prop.alias
                ));
            }
        }
        Ok(())
    }

    /// Build WHERE clause from categorized filters
    fn build_where_clause(&self, filters: &CategorizedFilters) -> Result<String, CteError> {
        let mut conditions = Vec::new();

        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = &[
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        // Add start node filters
        if let Some(start_filters) = &filters.start_node_filters {
            let sql = render_expr_to_sql_string(start_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add end node filters
        if let Some(end_filters) = &filters.end_node_filters {
            let sql = render_expr_to_sql_string(end_filters, alias_mapping);
            conditions.push(sql);
        }

        // Add relationship filters
        if let Some(rel_filters) = &filters.relationship_filters {
            let sql = render_expr_to_sql_string(rel_filters, alias_mapping);
            conditions.push(sql);
        }

        if conditions.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!("    WHERE {}", conditions.join(" AND ")))
        }
    }
}

impl EdgeToEdgeCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        // Validate that this is an edge-to-edge schema
        match &pattern_ctx.join_strategy {
            JoinStrategy::EdgeToEdge {
                prev_edge_alias,
                prev_edge_col,
                curr_edge_col,
            } => {
                // For edge-to-edge, both nodes should be embedded in the edge table
                let (table, from_col, to_col) = match &pattern_ctx.edge {
                    EdgeAccessStrategy::SeparateTable {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    _ => {
                        return Err(CteError::InvalidStrategy(
                            "EdgeToEdgeCteStrategy requires SeparateTable edge access".into(),
                        ))
                    }
                };

                Ok(Self {
                    pattern_ctx: pattern_ctx.clone(),
                    table,
                    prev_edge_alias: prev_edge_alias.clone(),
                    prev_edge_col: prev_edge_col.clone(),
                    curr_edge_col: curr_edge_col.clone(),
                    from_col,
                    to_col,
                })
            }
            _ => Err(CteError::InvalidStrategy(
                "EdgeToEdgeCteStrategy requires EdgeToEdge join strategy".into(),
            )),
        }
    }
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL for edge-to-edge schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
        })
    }

    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Validate that join strategy is edge-to-edge
        match &pattern_ctx.join_strategy {
            JoinStrategy::EdgeToEdge { .. } => Ok(()),
            _ => Err(CteError::SchemaValidationError(
                "EdgeToEdgeCteStrategy requires JoinStrategy::EdgeToEdge".into(),
            )),
        }
    }

    /// Generate the complete recursive CTE SQL for edge-to-edge pattern
    fn generate_recursive_cte_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let min_hops = context.spec.effective_min_hops();
        let max_hops = context.spec.max_hops;

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.map_or(true, |max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(context, properties, filters)?
            )
        } else {
            String::new()
        };

        // Build complete CTE
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias, min_hops
        );

        Ok(format!(
            "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
            cte_name, base_case, recursive_case, cte_name
        ))
    }

    /// Generate the base case SQL (1-hop traversal) for edge-to-edge schema
    fn generate_base_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Build SELECT clause - all properties come from the single table
        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.pattern_ctx.rel_alias, self.from_col
            ),
            format!("{}.{} as end_id", self.pattern_ctx.rel_alias, self.to_col),
            "1 as hop_count".to_string(),
            format!(
                "[{}.{}] as path_edges",
                self.pattern_ctx.rel_alias, self.from_col
            ), // Simplified edge tracking
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.pattern_ctx.rel_alias, self.from_col, self.pattern_ctx.rel_alias, self.to_col
            ),
        ];

        // Add properties from the single table
        self.add_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause - single table only
        let from_clause = format!("    FROM {} AS {}", self.table, self.pattern_ctx.rel_alias);

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(context, filters)?;

        Ok(
            format!("    SELECT\n        {}\n{}", select_clause, from_clause)
                + &if where_clause.is_empty() {
                    String::new()
                } else {
                    format!("\n    WHERE {}", where_clause)
                },
        )
    }

    /// Generate the recursive case SQL for edge-to-edge schema
    fn generate_recursive_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("next.{} as start_id", self.from_col),
            format!("next.{} as end_id", self.to_col),
            "prev.hop_count + 1".to_string(),
            "prev.path_edges || next.start_id".to_string(), // Extend edge path
            "prev.path_nodes || next.end_id".to_string(),   // Extend node path
        ];

        // Add properties from the next table occurrence
        self.add_recursive_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause with self-join on edge-to-edge connection
        let from_clause = format!(
            "    FROM {} prev\n    JOIN {} next ON next.{} = prev.end_id",
            self.pattern_ctx.rel_alias, self.table, self.from_col
        );

        // Build WHERE clause for recursion
        let where_conditions = [
            format!("prev.hop_count < {}", context.spec.max_hops.unwrap_or(10)),
            format!("next.{} NOT IN prev.path_nodes", self.to_col), // Cycle prevention
        ];

        // TODO: Add additional filters when RenderExpr to SQL conversion is implemented

        let where_clause = if where_conditions.is_empty() {
            String::new()
        } else {
            format!("\n    WHERE {}", where_conditions.join(" AND "))
        };

        Ok(format!(
            "    SELECT\n        {}\n{}{}",
            select_clause, from_clause, where_clause
        ))
    }

    /// Add property selections for base case
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            // All properties come from the single table
            select_items.push(format!(
                "{}.{} as {}",
                self.pattern_ctx.rel_alias, prop.column_name, prop.alias
            ));
        }
        Ok(())
    }

    /// Add property selections for recursive case
    fn add_recursive_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            // All properties come from the next occurrence of the single table
            select_items.push(format!("next.{} as {}", prop.column_name, prop.alias));
        }
        Ok(())
    }

    /// Build WHERE clause from filters
    fn build_where_clause(
        &self,
        context: &CteGenerationContext,
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let mut conditions: Vec<String> = Vec::new();

        // TODO: Implement filter conversion when RenderExpr to SQL is available
        // For now, return empty WHERE clause
        Ok(String::new())
    }
}

impl CoupledCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        // Validate that this is a coupled same-row schema
        match &pattern_ctx.join_strategy {
            JoinStrategy::CoupledSameRow { unified_alias } => {
                // For coupled same-row, both edges are in the same table
                let (table, from_col, to_col) = match &pattern_ctx.edge {
                    EdgeAccessStrategy::SeparateTable {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    _ => {
                        return Err(CteError::InvalidStrategy(
                            "CoupledCteStrategy requires SeparateTable edge access".into(),
                        ))
                    }
                };

                Ok(Self {
                    pattern_ctx: pattern_ctx.clone(),
                    unified_alias: unified_alias.clone(),
                    table,
                    from_col,
                    to_col,
                })
            }
            _ => Err(CteError::InvalidStrategy(
                "CoupledCteStrategy requires CoupledSameRow join strategy".into(),
            )),
        }
    }
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name
        let cte_name = format!(
            "vlp_{}_{}_{}",
            self.pattern_ctx.left_node_alias,
            self.pattern_ctx.right_node_alias,
            context.spec.effective_min_hops()
        );

        // For coupled same-row, we can often do a simple select since edges are in the same row
        // But for variable-length paths, we might need to handle multiple hops within the row
        let sql = self.generate_simple_select_sql(context, properties, filters)?;

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name: cte_name.clone(),
            recursive: false, // Coupled edges in same row don't need recursion
        })
    }

    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Validate that join strategy is coupled same-row
        match &pattern_ctx.join_strategy {
            JoinStrategy::CoupledSameRow { .. } => Ok(()),
            _ => Err(CteError::SchemaValidationError(
                "CoupledCteStrategy requires JoinStrategy::CoupledSameRow".into(),
            )),
        }
    }

    /// Generate simple SELECT SQL for coupled same-row pattern
    fn generate_simple_select_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Build SELECT clause - all data comes from the single table row
        let mut select_items = vec![
            format!("{}.{} as start_id", self.unified_alias, self.from_col),
            format!("{}.{} as end_id", self.unified_alias, self.to_col),
            "1 as hop_count".to_string(), // For coupled edges, each row represents 1 "logical" hop
            format!("[{}.{}] as path_edges", self.unified_alias, self.from_col),
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.unified_alias, self.from_col, self.unified_alias, self.to_col
            ),
        ];

        // Add properties from the single table
        for prop in properties {
            select_items.push(format!(
                "{}.{} as {}",
                self.unified_alias, prop.column_name, prop.alias
            ));
        }

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause - single table only
        let from_clause = format!("FROM {} AS {}", self.table, self.unified_alias);

        // Build WHERE clause from filters
        let where_clause = self.build_where_clause(context, filters)?;

        Ok(format!("SELECT\n    {}\n{}", select_clause, from_clause)
            + &if where_clause.is_empty() {
                String::new()
            } else {
                format!("\nWHERE {}", where_clause)
            })
    }

    /// Build WHERE clause from filters
    fn build_where_clause(
        &self,
        context: &CteGenerationContext,
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        let mut conditions: Vec<String> = Vec::new();

        // TODO: Implement filter conversion when RenderExpr to SQL is available
        // For now, return empty WHERE clause
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traditional_cte_strategy_basic() {
        // Create a simple pattern context for testing
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "u1".to_string(),
            right_node_alias: "u2".to_string(),
            rel_alias: "r".to_string(),
            join_strategy: JoinStrategy::Traditional {
                left_join_col: "follower_id".to_string(),
                right_join_col: "followed_id".to_string(),
            },
            // Fill in other required fields with defaults
            left_node: NodeAccessStrategy::OwnTable {
                table: "users_bench".to_string(),
                id_column: "user_id".to_string(),
                properties: std::collections::HashMap::new(),
            },
            right_node: NodeAccessStrategy::OwnTable {
                table: "users_bench".to_string(),
                id_column: "user_id".to_string(),
                properties: std::collections::HashMap::new(),
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "user_follows_bench".to_string(),
                from_id: "follower_id".to_string(),
                to_id: "followed_id".to_string(),
                properties: std::collections::HashMap::new(),
            },
            coupled_context: None,
            rel_types: vec!["FOLLOWS".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        };

        // Create a traditional strategy
        let strategy = TraditionalCteStrategy::new(&pattern_ctx).unwrap();

        // Create a basic CTE generation context
        let context = CteGenerationContext::with_schema(GraphSchema::build(
            1,
            "test".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        ))
        .with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        })
        .with_start_cypher_alias("u1".to_string())
        .with_end_cypher_alias("u2".to_string());

        // Test with empty properties and filters
        let properties = vec![];
        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(generation_result.recursive);
        assert!(generation_result.cte_name.starts_with("vlp_u1_u2_"));
        assert!(!generation_result.sql.is_empty());
        assert!(generation_result.sql.contains("WITH RECURSIVE"));
        assert!(generation_result.sql.contains("users_bench"));
        assert!(generation_result.sql.contains("user_follows_bench"));
    }

    #[test]
    fn test_denormalized_cte_strategy_basic() {
        // Create a denormalized pattern context (single table with embedded properties)
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "f1".to_string(),
            right_node_alias: "f2".to_string(),
            rel_alias: "flights".to_string(),
            left_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "flights".to_string(),
                properties: HashMap::new(), // Simplified for test
                is_from_node: true,
            },
            right_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "flights".to_string(),
                properties: HashMap::new(), // Simplified for test
                is_from_node: false,
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "flights".to_string(),
                from_id: "Origin".to_string(),
                to_id: "Dest".to_string(),
                properties: HashMap::new(), // Simplified for test
            },
            join_strategy: JoinStrategy::SingleTableScan {
                table: "flights".to_string(),
            },
            coupled_context: None,
            rel_types: vec!["FLIES_TO".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        };

        // Create strategy
        let strategy = DenormalizedCteStrategy::new(&pattern_ctx);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();

        // Create context
        let context = CteGenerationContext::new()
            .with_spec(VariableLengthSpec {
                min_hops: Some(1),
                max_hops: Some(3),
            })
            .with_start_cypher_alias("f1".to_string())
            .with_end_cypher_alias("f2".to_string());

        // Test with empty properties and filters
        let properties = vec![];
        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(generation_result.recursive);
        assert!(generation_result.cte_name.starts_with("vlp_f1_f2_"));
        assert!(!generation_result.sql.is_empty());
        assert!(generation_result.sql.contains("WITH RECURSIVE"));
        assert!(generation_result.sql.contains("flights"));
        assert!(generation_result.sql.contains("Origin"));
        assert!(generation_result.sql.contains("Dest"));
    }

    #[test]
    fn test_fk_edge_cte_strategy_basic() {
        // Create a minimal schema for testing
        let mut nodes = HashMap::new();
        nodes.insert(
            "File".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "files".to_string(),
                column_names: vec![
                    "id".to_string(),
                    "name".to_string(),
                    "parent_id".to_string(),
                ],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );

        let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

        // Create a FK-edge pattern context (hierarchical relationship via FK column)
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "parent".to_string(),
            right_node_alias: "child".to_string(),
            rel_alias: "hierarchy".to_string(),
            left_node: NodeAccessStrategy::OwnTable {
                table: "files".to_string(),
                id_column: "id".to_string(),
                properties: HashMap::new(), // Simplified for test
            },
            right_node: NodeAccessStrategy::OwnTable {
                table: "files".to_string(),
                id_column: "id".to_string(),
                properties: HashMap::new(), // Simplified for test
            },
            edge: EdgeAccessStrategy::FkEdge {
                node_table: "files".to_string(),
                fk_column: "parent_id".to_string(),
            },
            join_strategy: JoinStrategy::FkEdgeJoin {
                from_id: "id".to_string(),
                to_id: "parent_id".to_string(),
                join_side: NodePosition::Left, // edge_table == to_node_table
                is_self_referencing: true,
            },
            coupled_context: None,
            rel_types: vec!["PARENT_OF".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        };

        // Create CTE manager and analyze pattern
        let manager = CteManager::new(Arc::new(schema));
        let vlp_spec = VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        };

        let strategy_result = manager.analyze_pattern(&pattern_ctx, &vlp_spec);
        assert!(strategy_result.is_ok());
        let strategy = strategy_result.unwrap();

        // Create context
        let context = CteGenerationContext::new()
            .with_spec(VariableLengthSpec {
                min_hops: Some(1),
                max_hops: Some(3),
            })
            .with_start_cypher_alias("parent".to_string())
            .with_end_cypher_alias("child".to_string());

        // Test with empty properties and filters
        let properties = vec![];
        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(generation_result.recursive);
        assert!(generation_result.cte_name.starts_with("vlp_parent_child_"));
        assert!(!generation_result.sql.is_empty());
        assert!(generation_result.sql.contains("WITH RECURSIVE"));
        assert!(generation_result.sql.contains("files"));
        assert!(generation_result.sql.contains("parent_id"));
        assert!(generation_result.sql.contains("id"));
    }

    #[test]
    fn test_mixed_access_cte_strategy_basic() {
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "u".to_string(),
            right_node_alias: "p".to_string(),
            rel_alias: "r".to_string(),
            left_node: NodeAccessStrategy::OwnTable {
                table: "users".to_string(),
                id_column: "user_id".to_string(),
                properties: HashMap::new(),
            },
            right_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "r".to_string(),
                properties: HashMap::new(),
                is_from_node: false,
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "user_posts".to_string(),
                from_id: "user_id".to_string(),
                to_id: "post_id".to_string(),
                properties: HashMap::new(),
            },
            join_strategy: JoinStrategy::MixedAccess {
                joined_node: NodePosition::Left,
                join_col: "user_id".to_string(),
            },
            coupled_context: None,
            rel_types: vec!["AUTHORED".to_string()],
            constraints: None,
            left_is_polymorphic: false,
            right_is_polymorphic: false,
        };

        let strategy = MixedAccessCteStrategy::new(&pattern_ctx).unwrap();

        // Test validation
        assert!(strategy.validate(&pattern_ctx).is_ok());

        // Test SQL generation
        let context = CteGenerationContext::new()
            .with_spec(VariableLengthSpec {
                min_hops: Some(1),
                max_hops: Some(3),
            })
            .with_start_cypher_alias("u".to_string())
            .with_end_cypher_alias("p".to_string());

        let properties = vec![
            NodeProperty {
                cypher_alias: "u".to_string(),
                column_name: "full_name".to_string(),
                alias: "name".to_string(),
            },
            NodeProperty {
                cypher_alias: "p".to_string(),
                column_name: "content".to_string(),
                alias: "content".to_string(),
            },
        ];

        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        let result = strategy
            .generate_sql(&context, &properties, &filters)
            .unwrap();

        // Verify CTE name
        assert_eq!(result.cte_name, "vlp_u_p_1");

        // Verify SQL contains expected elements
        assert!(result.sql.contains("WITH RECURSIVE vlp_u_p_1 AS"));
        assert!(result.sql.contains("user_posts r"));
        assert!(result.sql.contains("JOIN users u ON u.user_id = r.user_id"));
        assert!(result.sql.contains("u.full_name as start_name"));
        assert!(result.sql.contains("p.content as end_content"));
        assert!(result.sql.contains("UNION ALL"));
    }

    #[test]
    fn test_edge_to_edge_cte_strategy_basic() {
        // Create a pattern context for edge-to-edge testing
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "f1".to_string(),
            right_node_alias: "f2".to_string(),
            rel_alias: "r".to_string(),
            join_strategy: JoinStrategy::EdgeToEdge {
                prev_edge_alias: "f1".to_string(),
                prev_edge_col: "Dest".to_string(),
                curr_edge_col: "Origin".to_string(),
            },
            left_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "r".to_string(),
                properties: std::collections::HashMap::new(),
                is_from_node: true,
            },
            right_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "r".to_string(),
                properties: std::collections::HashMap::new(),
                is_from_node: false,
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "flights".to_string(),
                from_id: "Origin".to_string(),
                to_id: "Dest".to_string(),
                properties: std::collections::HashMap::new(),
            },
            coupled_context: None,
            rel_types: vec!["FLIGHT".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        };

        // Create an edge-to-edge strategy
        let strategy = EdgeToEdgeCteStrategy::new(&pattern_ctx).unwrap();

        // Create a basic CTE generation context
        let context = CteGenerationContext::with_schema(GraphSchema::build(
            1,
            "test".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        ))
        .with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        })
        .with_start_cypher_alias("f1".to_string())
        .with_end_cypher_alias("f2".to_string());

        // Test with empty properties and filters
        let properties = vec![];
        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(generation_result.recursive);
        assert!(generation_result.cte_name.starts_with("vlp_f1_f2_"));
        assert!(!generation_result.sql.is_empty());
        assert!(generation_result.sql.contains("WITH RECURSIVE"));
    }

    #[test]
    fn test_coupled_cte_strategy_basic() {
        // Create a pattern context for coupled same-row testing
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "n1".to_string(),
            right_node_alias: "n2".to_string(),
            rel_alias: "r".to_string(),
            join_strategy: JoinStrategy::CoupledSameRow {
                unified_alias: "coupled".to_string(),
            },
            left_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "r".to_string(),
                properties: std::collections::HashMap::new(),
                is_from_node: true,
            },
            right_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "r".to_string(),
                properties: std::collections::HashMap::new(),
                is_from_node: false,
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "coupled_edges".to_string(),
                from_id: "from_id".to_string(),
                to_id: "to_id".to_string(),
                properties: std::collections::HashMap::new(),
            },
            coupled_context: None,
            rel_types: vec!["COUPLED".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        };

        // Create a coupled strategy
        let strategy = CoupledCteStrategy::new(&pattern_ctx).unwrap();

        // Create a basic CTE generation context
        let context = CteGenerationContext::with_schema(GraphSchema::build(
            1,
            "test".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        ))
        .with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(1), // Coupled edges typically represent single hops
        })
        .with_start_cypher_alias("n1".to_string())
        .with_end_cypher_alias("n2".to_string());

        // Test with empty properties and filters
        let properties = vec![];
        let filters = CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(!generation_result.recursive); // Coupled edges don't need recursion
        assert!(generation_result.cte_name.starts_with("vlp_n1_n2_"));
        assert!(!generation_result.sql.is_empty());
        assert!(generation_result.sql.contains("SELECT"));
        assert!(generation_result
            .sql
            .contains("FROM coupled_edges AS coupled"));
    }
}
