//! Unified CTE Manager for schema-aware CTE generation
//!
//! This module provides a strategy-pattern based approach to CTE generation
//! that handles all ClickGraph schema variations through unified interfaces.

use std::sync::Arc;

use crate::clickhouse_query_generator::variable_length_cte::{
    NodeProperty, VariableLengthCteGenerator,
};
use crate::graph_catalog::{
    config::Identifier, graph_schema::GraphSchema, EdgeAccessStrategy, JoinStrategy,
    NodeAccessStrategy, NodePosition, PatternSchemaContext,
};
use crate::query_planner::join_context::{
    VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
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

/// Metadata for a column in a generated CTE
///
/// This provides complete information for mapping Cypher property accesses to CTE columns
/// WITHOUT heuristics or underscore splitting.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CteColumnMetadata {
    /// The column name in the CTE (e.g., "end_id", "end_city")
    pub cte_column_name: String,
    /// The Cypher alias this column belongs to (e.g., "u2")
    pub cypher_alias: String,
    /// The Cypher property name from schema (e.g., "city", "email", "name")
    pub cypher_property: String,
    /// The actual DB column name from schema (e.g., "city", "email_address", "full_name")
    pub db_column: String,
    /// Whether this is an ID column (used for GROUP BY)
    pub is_id_column: bool,
    /// The VLP position (Start or End) for VLP CTEs
    pub vlp_position: Option<VlpColumnPosition>,
}

/// Position indicator for VLP CTE columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VlpColumnPosition {
    Start,
    End,
}

/// VLP endpoint metadata for converting CteGenerationResult to Cte
#[derive(Debug, Clone, Default)]
pub struct VlpEndpointInfo {
    /// Internal start node alias used in CTE (e.g., "start_node")
    pub start_alias: String,
    /// Internal end node alias used in CTE (e.g., "end_node")
    pub end_alias: String,
    /// Start node table name
    pub start_table: String,
    /// End node table name
    pub end_table: String,
    /// Original Cypher alias for start node
    pub cypher_start_alias: String,
    /// Original Cypher alias for end node
    pub cypher_end_alias: String,
    /// Start node ID column name
    pub start_id_col: String,
    /// End node ID column name
    pub end_id_col: String,
    /// Path variable name (e.g., "p" in MATCH p = (a)-[*]->(b))
    pub path_variable: Option<String>,
}

/// Result of CTE SQL generation
#[derive(Debug, Clone)]
pub struct CteGenerationResult {
    pub sql: String,
    pub parameters: Vec<String>,
    pub cte_name: String,
    pub recursive: bool,
    /// The table alias used in FROM clause (e.g., "t" for VLP CTEs)
    pub from_alias: String,
    /// Metadata for all columns in the CTE
    pub columns: Vec<CteColumnMetadata>,
    /// VLP endpoint info (for conversion to Cte)
    pub vlp_endpoint: Option<VlpEndpointInfo>,
    /// Filters that should be applied to outer SELECT (e.g., end_node_filters for denormalized VLP)
    pub outer_where_filters: Option<String>,
}

impl CteGenerationResult {
    /// Get the ID column for a given Cypher alias
    pub fn get_id_column_for_alias(&self, alias: &str) -> Option<&CteColumnMetadata> {
        self.columns
            .iter()
            .find(|c| c.cypher_alias == alias && c.is_id_column)
    }

    /// Get all columns for a given Cypher alias
    pub fn get_columns_for_alias(&self, alias: &str) -> Vec<&CteColumnMetadata> {
        self.columns
            .iter()
            .filter(|c| c.cypher_alias == alias)
            .collect()
    }

    /// Get the FROM alias to use when referencing this CTE's columns
    pub fn get_from_alias(&self) -> &str {
        &self.from_alias
    }

    /// Build column metadata for VLP CTE columns
    ///
    /// VLP CTEs generate columns like: start_id, end_id, start_city, end_city, etc.
    /// This creates the metadata mapping those back to Cypher aliases.
    pub fn build_vlp_column_metadata(
        left_alias: &str,
        right_alias: &str,
        properties: &[NodeProperty],
        id_column: &str,
    ) -> Vec<CteColumnMetadata> {
        let mut columns = Vec::new();

        // Add start node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_START_ID_COLUMN.to_string(),
            cypher_alias: left_alias.to_string(),
            cypher_property: id_column.to_string(),
            db_column: id_column.to_string(),
            is_id_column: true,
            vlp_position: Some(VlpColumnPosition::Start),
        });

        // Add end node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_END_ID_COLUMN.to_string(),
            cypher_alias: right_alias.to_string(),
            cypher_property: id_column.to_string(),
            db_column: id_column.to_string(),
            is_id_column: true,
            vlp_position: Some(VlpColumnPosition::End),
        });

        // Add property columns for both start and end nodes
        for prop in properties {
            // Skip ID column as it's already added
            if prop.alias == id_column {
                continue;
            }

            // Use property names directly from schema - NO SPLITTING!
            // prop.alias: Cypher property name (e.g., "email", "full_name")
            // prop.column_name: DB column name (e.g., "email_address", "full_name")
            let cypher_property_name = &prop.alias;
            let db_column_name = &prop.column_name;

            // Start node property (only for properties belonging to start node)
            if prop.cypher_alias == left_alias {
                columns.push(CteColumnMetadata {
                    cte_column_name: format!("start_{}", cypher_property_name),
                    cypher_alias: left_alias.to_string(),
                    cypher_property: cypher_property_name.clone(),
                    db_column: db_column_name.clone(),
                    is_id_column: false,
                    vlp_position: Some(VlpColumnPosition::Start),
                });
            }

            // End node property (only for properties belonging to end node)
            if prop.cypher_alias == right_alias {
                columns.push(CteColumnMetadata {
                    cte_column_name: format!("end_{}", cypher_property_name),
                    cypher_alias: right_alias.to_string(),
                    cypher_property: cypher_property_name.clone(),
                    db_column: db_column_name.clone(),
                    is_id_column: false,
                    vlp_position: Some(VlpColumnPosition::End),
                });
            }
        }

        columns
    }

    /// Convert CteGenerationResult to Cte struct for downstream use
    pub fn to_cte(&self) -> crate::render_plan::Cte {
        use crate::render_plan::{Cte, CteContent};

        if let Some(ref endpoint) = self.vlp_endpoint {
            Cte::new_vlp(
                self.cte_name.clone(),
                CteContent::RawSql(self.sql.clone()),
                self.recursive,
                endpoint.start_alias.clone(),
                endpoint.end_alias.clone(),
                endpoint.start_table.clone(),
                endpoint.end_table.clone(),
                endpoint.cypher_start_alias.clone(),
                endpoint.cypher_end_alias.clone(),
                endpoint.start_id_col.clone(),
                endpoint.end_id_col.clone(),
                endpoint.path_variable.clone(),
            )
        } else {
            Cte::new(
                self.cte_name.clone(),
                CteContent::RawSql(self.sql.clone()),
                self.recursive,
            )
        }
    }
}

/// Helper function to build WHERE clause from categorized filters.
///
/// Prefers pre-rendered SQL strings (start_sql, end_sql, relationship_sql) when available
/// for backward compatibility with existing filter rendering in cte_extraction.rs.
/// Falls back to rendering from RenderExpr if pre-rendered SQL is not available.
fn build_where_clause_from_filters(
    filters: &CategorizedFilters,
    alias_mapping: &[(String, String)],
) -> String {
    let mut conditions = Vec::new();

    // Add start node filters - prefer pre-rendered SQL
    if let Some(start_sql) = &filters.start_sql {
        conditions.push(start_sql.clone());
    } else if let Some(start_filters) = &filters.start_node_filters {
        let sql = render_expr_to_sql_string(start_filters, alias_mapping);
        conditions.push(sql);
    }

    // Add end node filters - prefer pre-rendered SQL
    if let Some(end_sql) = &filters.end_sql {
        conditions.push(end_sql.clone());
    } else if let Some(end_filters) = &filters.end_node_filters {
        let sql = render_expr_to_sql_string(end_filters, alias_mapping);
        conditions.push(sql);
    }

    // Add relationship filters - prefer pre-rendered SQL
    if let Some(rel_sql) = &filters.relationship_sql {
        conditions.push(rel_sql.clone());
    } else if let Some(rel_filters) = &filters.relationship_filters {
        let sql = render_expr_to_sql_string(rel_filters, alias_mapping);
        conditions.push(sql);
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("    WHERE {}", conditions.join(" AND "))
    }
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
                    .all_node_schemas()
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
                let target_node_schema = schema.node_schema(target_node_label).map_err(|e| {
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
                DenormalizedCteStrategy::new(pattern_ctx, self.schema.clone())?,
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

    /// Generate a Variable-Length Path CTE using the unified strategy
    ///
    /// This is the main entry point for generating VLP CTEs. It:
    /// 1. Creates a VariableLengthCteStrategy from the PatternSchemaContext
    /// 2. Sets up the CteGenerationContext with VLP-specific fields
    /// 3. Generates the CTE SQL using the appropriate generator variant
    ///
    /// # Arguments
    /// * `pattern_ctx` - The graph pattern schema context
    /// * `properties` - Node properties to include in CTE projection
    /// * `filters` - Pre-rendered SQL filters for start/end nodes and relationships
    ///
    /// # Returns
    /// A CteGenerationResult containing the SQL and metadata, or an error
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

        // Create the VLP strategy from pattern context
        let strategy = VariableLengthCteStrategy::new(pattern_ctx, &self.schema)?;

        // Generate using the strategy
        strategy.generate_sql(&self.context, properties, filters)
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
    /// Variable-length path strategy - wraps the comprehensive VariableLengthCteGenerator
    VariableLength(VariableLengthCteStrategy),
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
            CteStrategy::Traditional(s) => {
                log::warn!("🔍 Using TraditionalCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::Denormalized(s) => {
                log::warn!("🔍 Using DenormalizedCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::FkEdge(s) => {
                log::warn!("🔍 Using FkEdgeCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::MixedAccess(s) => {
                log::warn!("🔍 Using MixedAccessCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::EdgeToEdge(s) => {
                log::warn!("🔍 Using EdgeToEdgeCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::Coupled(s) => {
                log::warn!("🔍 Using CoupledCteStrategy");
                s.generate_sql(context, properties, filters)
            }
            CteStrategy::VariableLength(s) => {
                log::warn!("🔍 Using VariableLengthCteStrategy");
                s.generate_sql(context, properties, filters)
            }
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
            CteStrategy::VariableLength(s) => s.validate(pattern_ctx),
        }
    }
}

// Placeholder strategy implementations - will be filled in Phase 2-4
pub struct DenormalizedCteStrategy {
    pattern_ctx: PatternSchemaContext,
    table: String,
    from_col: String,
    to_col: String,
    schema: Arc<GraphSchema>,
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
    _prev_edge_alias: String,
    _prev_edge_col: String,
    _curr_edge_col: String,
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

        // Build column metadata for downstream code
        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            &self.id_column,
        );

        // Build VLP endpoint info for FK-edge
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: self.node_table.clone(),
            end_table: self.node_table.clone(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.id_column.clone(),
            end_id_col: self.id_column.clone(),
            path_variable: None,
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
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
            VLP_END_ID_COLUMN // Connect to the end of the current path
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
            log::warn!(
                "🔍 VLP Property: alias=\'{}\', cypher_alias=\'{}\', column=\'{}\'",
                prop.alias,
                prop.cypher_alias,
                prop.column_name
            );
            // Skip ID column as it's already explicitly added as start_id/end_id
            if prop.alias == "id" {
                continue;
            }
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
        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = vec![
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        Ok(build_where_clause_from_filters(filters, &alias_mapping))
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

        // Build column metadata - get ID column from pattern context
        let (start_table, start_id_col) = self
            .get_node_table_info(&self.pattern_ctx.left_node_alias)
            .unwrap_or_else(|_| ("unknown".to_string(), "id".to_string()));
        let (end_table, end_id_col) = self
            .get_node_table_info(&self.pattern_ctx.right_node_alias)
            .unwrap_or_else(|_| ("unknown".to_string(), "id".to_string()));

        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            &start_id_col,
        );

        // Build VLP endpoint info for conversion to Cte
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table,
            end_table,
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col,
            end_id_col,
            path_variable: None, // Will be set from GraphRel.path_variable later
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
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
        let needs_recursion = max_hops.is_none_or(|max| max > min_hops);
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
        // Determine which node based on alias
        let node_strategy = if node_alias == self.pattern_ctx.left_node_alias {
            &self.pattern_ctx.left_node
        } else if node_alias == self.pattern_ctx.right_node_alias {
            &self.pattern_ctx.right_node
        } else {
            return Err(CteError::SchemaValidationError(format!(
                "Unknown node alias '{}': expected '{}' or '{}'",
                node_alias, self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias
            )));
        };

        // Extract table and ID column from NodeAccessStrategy
        match node_strategy {
            NodeAccessStrategy::OwnTable {
                table, id_column, ..
            } => Ok((table.clone(), id_column.to_string())),
            NodeAccessStrategy::EmbeddedInEdge { edge_alias, .. } => {
                // For embedded nodes in traditional strategy, this is unexpected
                // but we can use the edge alias as the table reference
                Err(CteError::InvalidStrategy(format!(
                    "TraditionalCteStrategy expects nodes with OwnTable, got EmbeddedInEdge for alias '{}' (edge: {})",
                    node_alias, edge_alias
                )))
            }
            NodeAccessStrategy::Virtual { label } => Err(CteError::InvalidStrategy(format!(
                "TraditionalCteStrategy does not support Virtual nodes (label: {})",
                label
            ))),
        }
    }

    /// Get relationship table info (table, from_col, to_col)
    fn get_relationship_table_info(&self) -> Result<(String, String, String), CteError> {
        // Extract from EdgeAccessStrategy in PatternSchemaContext
        match &self.pattern_ctx.edge {
            EdgeAccessStrategy::SeparateTable {
                table,
                from_id,
                to_id,
                ..
            } => Ok((table.clone(), from_id.clone(), to_id.clone())),
            EdgeAccessStrategy::Polymorphic {
                table,
                from_id,
                to_id,
                ..
            } => {
                // Polymorphic edges can also work with traditional strategy
                Ok((table.clone(), from_id.clone(), to_id.clone()))
            }
            EdgeAccessStrategy::FkEdge {
                node_table,
                fk_column,
            } => Err(CteError::InvalidStrategy(format!(
                "TraditionalCteStrategy expects SeparateTable edge, got FkEdge (table: {}, fk: {})",
                node_table, fk_column
            ))),
        }
    }

    /// Add property selections to the SELECT clause
    fn add_property_selections(
        &self,
        select_items: &mut Vec<String>,
        properties: &[NodeProperty],
    ) -> Result<(), CteError> {
        for prop in properties {
            log::warn!(
                "🔍 VLP Property: alias=\'{}\', cypher_alias=\'{}\', column=\'{}\'",
                prop.alias,
                prop.cypher_alias,
                prop.column_name
            );
            // Skip ID column as it's already explicitly added as start_id/end_id
            if prop.alias == "id" {
                continue;
            }
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
        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = vec![
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        Ok(build_where_clause_from_filters(filters, &alias_mapping))
    }
    pub fn validate(&self, _pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        Ok(())
    }
}

// ===== DenormalizedCteStrategy =====
/// Strategy for denormalized schemas where node properties are embedded in edge table
///
/// **ARCHITECTURE NOTE**: This implementation follows the exact pattern from the OLD
/// VariableLengthCteGenerator (lines 908-1095 in variable_length_cte.rs).
///
/// **CRITICAL PATTERN**: For denormalized VLP with end_node_filters:
/// - Inner CTE: Apply start_node_filters only, traverse all paths
/// - Outer CTE: Wrap inner and apply end_node_filters after traversal
///
/// **WHY**: Denormalized schemas have both nodes in the SAME ROW. Applying both start
/// and end filters in the base case would only find direct paths (e.g., LAX→ATL).
/// Multi-hop paths (e.g., LAX→ORD→ATL) require traversing from LAX first, then
/// filtering for ATL in the final results.
///
/// **REFACTORING LESSON**: When refactoring, copy the logic EXACTLY from the working
/// implementation. Don't reimplement patterns from scratch - you'll miss critical details
/// like the wrapper CTE pattern that took 2+ hours to rediscover.
impl DenormalizedCteStrategy {
    pub fn new(
        pattern_ctx: &PatternSchemaContext,
        schema: Arc<GraphSchema>,
    ) -> Result<Self, CteError> {
        // Validate that this is a denormalized schema
        match &pattern_ctx.join_strategy {
            JoinStrategy::SingleTableScan { table } => Ok(Self {
                pattern_ctx: pattern_ctx.clone(),
                table: table.clone(),
                from_col: Self::get_from_column(pattern_ctx)?,
                to_col: Self::get_to_column(pattern_ctx)?,
                schema,
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
            "vlp_{}_{}",
            self.pattern_ctx.left_node_alias,
            context.spec.effective_min_hops()
        );

        // Build the recursive CTE SQL for denormalized schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        // Build column metadata for denormalized schema
        let mut columns = Vec::new();

        // Add start node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_START_ID_COLUMN.to_string(),
            cypher_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_property: "id".to_string(),
            db_column: self.from_col.clone(),
            is_id_column: true,
            vlp_position: Some(VlpColumnPosition::Start),
        });

        // Add end node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_END_ID_COLUMN.to_string(),
            cypher_alias: self.pattern_ctx.right_node_alias.clone(),
            cypher_property: "id".to_string(),
            db_column: self.to_col.clone(),
            is_id_column: true,
            vlp_position: Some(VlpColumnPosition::End),
        });

        // Add property columns - for denormalized, properties need start_/end_ prefixes
        for prop in properties {
            // Determine prefix based on which endpoint this property belongs to
            let position = if prop.cypher_alias == self.pattern_ctx.left_node_alias {
                VlpColumnPosition::Start
            } else {
                VlpColumnPosition::End
            };
            let prefix = match position {
                VlpColumnPosition::Start => "start_",
                VlpColumnPosition::End => "end_",
                _ => "",
            };

            // 🔧 FIX: For denormalized VLP, CTE columns must have start_/end_ prefixes
            // This allows the VLP rewrite to correctly map origin.city → t.start_OriginCityName
            let cte_column_name = format!("{}{}", prefix, prop.column_name);

            columns.push(CteColumnMetadata {
                cte_column_name,                         // e.g., "start_OriginCityName"
                cypher_alias: prop.cypher_alias.clone(), // e.g., "origin"
                cypher_property: prop.alias.clone(),     // e.g., "city"
                db_column: prop.column_name.clone(),     // e.g., "OriginCityName"
                is_id_column: false,
                vlp_position: Some(position),
            });
        }

        // Build VLP endpoint info for denormalized (edge table is the only table)
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: self.table.clone(),
            end_table: self.table.clone(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.from_col.clone(),
            end_id_col: self.to_col.clone(),
            path_variable: None,
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            // ⚠️ CRITICAL: For denormalized VLP, end_node_filters must be applied in outer SELECT
            outer_where_filters: filters.end_sql.clone(),
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

    /// Map logical property name to physical column name in edge table
    /// For denormalized nodes, properties are stored in from_node_properties and to_node_properties
    fn map_denormalized_property(
        &self,
        logical_prop: &str,
        is_from_node: bool,
    ) -> Result<String, CteError> {
        // Find the node schema that points to our relationship table
        let node_schemas = self.schema.all_node_schemas();
        let rel_table_name = self.table.rsplit('.').next().unwrap_or(&self.table);

        let node_schema = node_schemas
            .values()
            .find(|n| {
                let schema_table = n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
                schema_table == rel_table_name
            })
            .ok_or_else(|| {
                CteError::SchemaValidationError(format!(
                    "No node schema found for table '{}'",
                    rel_table_name
                ))
            })?;

        // Get the appropriate property mapping (from_properties or to_properties)
        let property_map = if is_from_node {
            node_schema.from_properties.as_ref()
        } else {
            node_schema.to_properties.as_ref()
        };

        if let Some(map) = property_map {
            // Find the physical column for the logical property
            if let Some(physical_col) = map.get(logical_prop) {
                return Ok(physical_col.clone());
            }
        }

        // Property not found in mapping
        Err(CteError::SchemaValidationError(format!(
            "Property '{}' not found in {} mappings for table '{}'",
            logical_prop,
            if is_from_node { "from_node" } else { "to_node" },
            rel_table_name
        )))
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

        let cte_name = format!(
            "vlp_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias
        );

        // ⚠️ CRITICAL FIX: For denormalized VLP with end_node_filters, wrap with outer CTE
        // Following OLD generator pattern (line 908-1070 in variable_length_cte.rs)
        //
        // WHY: Denormalized schemas have start/end nodes in SAME ROW. Applying both filters
        // in base case would only find direct connections. Instead:
        // 1. Inner CTE: Apply start filter, traverse all paths
        // 2. Outer CTE: Filter by end node after traversal completes
        let (recursive_cte_name, needs_wrapper) = if filters.end_sql.is_some() {
            (format!("{}_inner", cte_name), true)
        } else {
            (cte_name.clone(), false)
        };

        // Generate base case (1-hop)
        let base_case = self.generate_base_case_sql(context, properties, filters)?;

        // Generate recursive case if needed
        let needs_recursion = max_hops.is_none_or(|max| max > min_hops);
        let recursive_case = if needs_recursion {
            format!(
                "\n    UNION ALL\n{}",
                self.generate_recursive_case_sql(
                    context,
                    properties,
                    filters,
                    &recursive_cte_name
                )?
            )
        } else {
            String::new()
        };

        if needs_wrapper {
            let end_filter = filters.end_sql.as_ref().unwrap();

            // ⚠️ FIX: Rewrite filter for CTE columns
            // filters.end_sql uses the relationship table alias (e.g., "f.Dest = 'ATL'")
            // But in the outer CTE, we select from the inner CTE which has columns like "end_Dest"
            // Replace: "f.COLUMN" → "end_COLUMN" (where f is the rel_alias)
            let rewritten_filter =
                end_filter.replace(&format!("{}.", self.pattern_ctx.rel_alias), "end_");

            let inner_cte = format!(
                "{} AS (\n{}{}\n)",
                recursive_cte_name, base_case, recursive_case
            );

            // Build WHERE clause for outer CTE
            let mut where_conditions = vec![rewritten_filter];
            if min_hops > 1 {
                where_conditions.push(format!("hop_count >= {}", min_hops));
            }
            let where_clause = where_conditions.join(" AND ");

            // Outer CTE selects from inner and applies end filter
            // Match format of other strategies: WITH RECURSIVE ... SELECT *
            Ok(format!(
                "WITH RECURSIVE {},\n{} AS (\n    SELECT * FROM {} WHERE {}\n) SELECT * FROM {}",
                inner_cte, cte_name, recursive_cte_name, where_clause, cte_name
            ))
        } else {
            // No end filter: simple single CTE
            // Match format of other strategies: WITH RECURSIVE ... SELECT *
            Ok(format!(
                "WITH RECURSIVE {} AS (\n{}{}\n) SELECT * FROM {}",
                recursive_cte_name, base_case, recursive_case, recursive_cte_name
            ))
        }
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
        recursive_cte_name: &str,
    ) -> Result<String, CteError> {
        // Build SELECT clause for recursive case
        let mut select_items = vec![
            format!("next.{} as start_id", self.from_col),
            format!("next.{} as end_id", self.to_col),
            "vp.hop_count + 1".to_string(),
            format!("arrayConcat(vp.path_edges, [next.{}])", self.from_col), // Extend edge path array
            format!("arrayConcat(vp.path_nodes, [next.{}])", self.to_col), // Extend node path array
        ];

        // Add properties from the next table occurrence
        self.add_recursive_property_selections(&mut select_items, properties)?;

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause with self-join - use the passed recursive_cte_name
        let from_clause = format!(
            "    FROM {} vp\n    JOIN {} next ON next.{} = vp.end_id",
            recursive_cte_name, self.table, self.from_col
        );

        // Build WHERE clause for recursion
        let mut where_conditions = vec![
            format!("vp.hop_count < {}", context.spec.max_hops.unwrap_or(10)),
            // ⚠️ ClickHouse limitation: NOT IN with array doesn't work in recursive CTEs
            // Use NOT has(array, element) instead for cycle detection
            format!("NOT has(vp.path_nodes, next.{})", self.to_col), // Cycle prevention
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
            log::warn!(
                "🔍 VLP Property: alias=\'{}\', cypher_alias=\'{}\', column=\'{}\'",
                prop.alias,
                prop.cypher_alias,
                prop.column_name
            );
            // Skip ID column as it's already explicitly added as start_id/end_id
            if prop.alias == "id" {
                continue;
            }
            // Determine if this property belongs to start (from) or end (to) node
            let is_from_node = prop.cypher_alias == self.pattern_ctx.left_node_alias;
            let prefix = if is_from_node { "start_" } else { "end_" };

            // Map logical property to physical column in edge table
            let physical_col = self.map_denormalized_property(&prop.column_name, is_from_node)?;

            let sql = format!(
                "{}.{} as {}{}",
                self.pattern_ctx.rel_alias, physical_col, prefix, physical_col
            );
            select_items.push(sql);
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
            // Determine if this property belongs to start (from) or end (to) node
            let is_from_node = prop.cypher_alias == self.pattern_ctx.left_node_alias;
            let _prefix = if is_from_node { "start_" } else { "end_" };

            // Map logical property to physical column in edge table
            let physical_col = self.map_denormalized_property(&prop.column_name, is_from_node)?;

            if is_from_node {
                // Start node property comes from previous iteration (already has prefix)
                select_items.push(format!(
                    "vp.start_{} as start_{}",
                    physical_col, physical_col
                ));
            } else {
                // End node property comes from the new edge being joined
                select_items.push(format!("next.{} as end_{}", physical_col, physical_col));
            }
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

        // ⚠️ CRITICAL FIX: For VLP in denormalized schemas, end_node_filters must NOT be applied in base case!
        //
        // WHY: In denormalized schema, start and end nodes are in SAME ROW (same table).
        // Base case with both filters: WHERE f.Origin = 'LAX' AND f.Dest = 'ATL'
        //   → Only finds direct LAX→ATL flights (wrong!)
        //
        // Correct approach:
        //   Base case: WHERE f.Origin = 'LAX' (start from LAX)
        //   Outer query: WHERE t.end_Dest = 'ATL' (filter final destination)
        //
        // This matches Neo4j behavior: VLP first explores from start, then filters end.

        // Add start node filters - prefer pre-rendered SQL
        if let Some(start_sql) = &filters.start_sql {
            conditions.push(start_sql.clone());
        } else if let Some(start_filters) = &filters.start_node_filters {
            conditions.push(start_filters.to_sql());
        }

        // ❌ DO NOT add end_node_filters here for denormalized schemas!
        // They will be applied in the outer SELECT after path traversal completes.

        // Add relationship filters - prefer pre-rendered SQL
        if let Some(rel_sql) = &filters.relationship_sql {
            conditions.push(rel_sql.clone());
        } else if let Some(rel_filters) = &filters.relationship_filters {
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
                joined_node: *joined_node,
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

        // Build column metadata
        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            "id", // Mixed access uses generic ID
        );

        // Build VLP endpoint info for mixed access
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: "mixed_table".to_string(), // TODO: extract from pattern_ctx
            end_table: "mixed_table".to_string(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.join_col.clone(),
            end_id_col: self.join_col.clone(),
            path_variable: None,
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
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
        let needs_recursion = max_hops.is_none_or(|max| max > min_hops);
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
        _context: &CteGenerationContext,
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
        let (embedded_node_alias, _joined_node_alias) = match self.joined_node {
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
            format!("{}.{}", cte_name, VLP_START_ID_COLUMN),
            format!("{}.{}", embedded_node_alias, VLP_END_ID_COLUMN),
            format!("{}.hop_count + 1 as hop_count", cte_name),
            format!(
                "arrayConcat({}.path_edges, [{}]) as path_edges",
                cte_name, self.join_col
            ),
            format!(
                "arrayConcat({}.path_nodes, [{}]) as path_nodes",
                cte_name, VLP_END_ID_COLUMN
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
    fn get_joined_node_id_column(&self) -> Result<String, CteError> {
        let node_access = match self.joined_node {
            NodePosition::Left => &self.pattern_ctx.left_node,
            NodePosition::Right => &self.pattern_ctx.right_node,
        };

        match node_access {
            NodeAccessStrategy::OwnTable { id_column, .. } => Ok(id_column.to_string()),
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
            log::warn!(
                "🔍 VLP Property: alias=\'{}\', cypher_alias=\'{}\', column=\'{}\'",
                prop.alias,
                prop.cypher_alias,
                prop.column_name
            );
            // Skip ID column as it's already explicitly added as start_id/end_id
            if prop.alias == "id" {
                continue;
            }
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
        // Create alias mapping: Cypher aliases map to themselves in CTE context
        let alias_mapping = vec![
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        Ok(build_where_clause_from_filters(filters, &alias_mapping))
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
                    _prev_edge_alias: prev_edge_alias.clone(),
                    _prev_edge_col: prev_edge_col.clone(),
                    _curr_edge_col: curr_edge_col.clone(),
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

        // Build column metadata
        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            "id", // Edge-to-edge uses generic ID
        );

        // Build VLP endpoint info for edge-to-edge
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: self.table.clone(),
            end_table: self.table.clone(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.from_col.clone(),
            end_id_col: self.to_col.clone(),
            path_variable: None,
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
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
        let needs_recursion = max_hops.is_none_or(|max| max > min_hops);
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
        _filters: &CategorizedFilters,
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
            log::warn!(
                "🔍 VLP Property: alias=\'{}\', cypher_alias=\'{}\', column=\'{}\'",
                prop.alias,
                prop.cypher_alias,
                prop.column_name
            );
            // Skip ID column as it's already explicitly added as start_id/end_id
            if prop.alias == "id" {
                continue;
            }
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
        _context: &CteGenerationContext,
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Use pre-rendered SQL if available
        let alias_mapping = vec![
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        Ok(build_where_clause_from_filters(filters, &alias_mapping))
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

        // Build column metadata
        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            "id", // Coupled uses generic ID
        );

        // Build VLP endpoint info for coupled same-row
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: self.table.clone(),
            end_table: self.table.clone(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.from_col.clone(),
            end_id_col: self.to_col.clone(),
            path_variable: None,
        };

        Ok(CteGenerationResult {
            sql,
            parameters: collect_parameters_from_filters(filters),
            cte_name: cte_name.clone(),
            recursive: false, // Coupled edges in same row don't need recursion
            from_alias: self.unified_alias.clone(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
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
        _context: &CteGenerationContext,
        filters: &CategorizedFilters,
    ) -> Result<String, CteError> {
        // Use pre-rendered SQL if available
        let alias_mapping = vec![
            (
                self.pattern_ctx.left_node_alias.clone(),
                self.pattern_ctx.left_node_alias.clone(),
            ),
            (
                self.pattern_ctx.right_node_alias.clone(),
                self.pattern_ctx.right_node_alias.clone(),
            ),
        ];

        Ok(build_where_clause_from_filters(filters, &alias_mapping))
    }
}

// ============================================================================
// VariableLengthCteStrategy - Wraps the comprehensive VariableLengthCteGenerator
// ============================================================================

/// Strategy for variable-length path CTE generation.
///
/// This strategy wraps the existing `VariableLengthCteGenerator` to provide
/// a unified interface within the CteManager strategy pattern while preserving
/// all the comprehensive SQL generation capabilities including:
/// - Shortest path modes (ROW_NUMBER partitioning)
/// - Heterogeneous polymorphic paths (two-CTE structure)
/// - Zero-hop base cases
/// - Complex filter rewriting
/// - Edge constraint compilation
/// - Denormalized and mixed access patterns
pub struct VariableLengthCteStrategy {
    pattern_ctx: PatternSchemaContext,
    /// Start node table name
    start_table: String,
    /// Start node ID column
    start_id_col: String,
    /// End node table name (may be same as start for self-joins)
    end_table: String,
    /// End node ID column
    end_id_col: String,
    /// Relationship/edge table name
    rel_table: String,
    /// Relationship from column
    rel_from_col: String,
    /// Relationship to column
    rel_to_col: String,
    /// Whether this is a denormalized pattern (both nodes embedded in edge)
    is_denormalized: bool,
    /// Whether start node is denormalized
    start_is_denormalized: bool,
    /// Whether end node is denormalized
    end_is_denormalized: bool,
    /// Whether this is an FK-edge pattern
    is_fk_edge: bool,
    /// Polymorphic edge type column
    type_column: Option<String>,
    /// Polymorphic from label column
    from_label_column: Option<String>,
    /// Polymorphic to label column
    to_label_column: Option<String>,
    /// Expected from node label (for polymorphic filtering)
    from_node_label: Option<String>,
    /// Expected to node label (for polymorphic filtering)
    to_node_label: Option<String>,
}

impl VariableLengthCteStrategy {
    /// Create a new VariableLengthCteStrategy from a PatternSchemaContext
    pub fn new(pattern_ctx: &PatternSchemaContext, schema: &GraphSchema) -> Result<Self, CteError> {
        // Extract table/column info based on join strategy and node access patterns
        let (start_table, start_id_col, start_is_denorm) =
            Self::extract_node_info(&pattern_ctx.left_node, &pattern_ctx.edge, true)?;
        let (end_table, end_id_col, end_is_denorm) =
            Self::extract_node_info(&pattern_ctx.right_node, &pattern_ctx.edge, false)?;
        let (rel_table, rel_from_col, rel_to_col) =
            Self::extract_edge_info(&pattern_ctx.edge, schema)?;

        // Determine denormalized/FK-edge patterns
        let is_denormalized = start_is_denorm && end_is_denorm;
        let is_fk_edge = matches!(pattern_ctx.join_strategy, JoinStrategy::FkEdgeJoin { .. });

        // Extract polymorphic edge columns
        let (type_column, from_label_column, to_label_column) =
            Self::extract_polymorphic_info(&pattern_ctx.edge);

        Ok(Self {
            pattern_ctx: pattern_ctx.clone(),
            start_table,
            start_id_col,
            end_table,
            end_id_col,
            rel_table,
            rel_from_col,
            rel_to_col,
            is_denormalized,
            start_is_denormalized: start_is_denorm,
            end_is_denormalized: end_is_denorm,
            is_fk_edge,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label: None, // Set during generate_sql based on context
            to_node_label: None,
        })
    }

    /// Extract node table and ID column info from NodeAccessStrategy
    fn extract_node_info(
        node: &NodeAccessStrategy,
        edge: &EdgeAccessStrategy,
        is_start: bool,
    ) -> Result<(String, String, bool), CteError> {
        match node {
            NodeAccessStrategy::OwnTable {
                table, id_column, ..
            } => Ok((table.clone(), id_column.to_string(), false)),
            NodeAccessStrategy::EmbeddedInEdge { edge_alias: _, .. } => {
                // For embedded nodes, get the edge table and use from_id/to_id based on position
                let (edge_table, from_col, to_col) = match edge {
                    EdgeAccessStrategy::SeparateTable {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    EdgeAccessStrategy::Polymorphic {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    EdgeAccessStrategy::FkEdge {
                        node_table,
                        fk_column,
                    } => (node_table.clone(), fk_column.clone(), "id".to_string()),
                };
                let id_col = if is_start { from_col } else { to_col };
                Ok((edge_table, id_col, true))
            }
            NodeAccessStrategy::Virtual { label } => {
                // Virtual nodes use the edge table
                let (edge_table, from_col, to_col) = match edge {
                    EdgeAccessStrategy::SeparateTable {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    EdgeAccessStrategy::Polymorphic {
                        table,
                        from_id,
                        to_id,
                        ..
                    } => (table.clone(), from_id.clone(), to_id.clone()),
                    EdgeAccessStrategy::FkEdge { .. } => {
                        return Err(CteError::InvalidStrategy(format!(
                            "Virtual node '{}' not compatible with FK-edge",
                            label
                        )));
                    }
                };
                let id_col = if is_start { from_col } else { to_col };
                Ok((edge_table, id_col, true))
            }
        }
    }

    /// Extract edge table and column info from EdgeAccessStrategy
    fn extract_edge_info(
        edge: &EdgeAccessStrategy,
        _schema: &GraphSchema,
    ) -> Result<(String, String, String), CteError> {
        match edge {
            EdgeAccessStrategy::SeparateTable {
                table,
                from_id,
                to_id,
                ..
            } => Ok((table.clone(), from_id.clone(), to_id.clone())),
            EdgeAccessStrategy::Polymorphic {
                table,
                from_id,
                to_id,
                ..
            } => Ok((table.clone(), from_id.clone(), to_id.clone())),
            EdgeAccessStrategy::FkEdge {
                node_table,
                fk_column,
            } => {
                // FK-edge: the "relationship" is the FK column on the node table
                // from_id is the FK column, to_id is the target node's ID
                Ok((node_table.clone(), fk_column.clone(), "id".to_string()))
            }
        }
    }

    /// Extract polymorphic edge info (type discriminator columns)
    fn extract_polymorphic_info(
        edge: &EdgeAccessStrategy,
    ) -> (Option<String>, Option<String>, Option<String>) {
        match edge {
            EdgeAccessStrategy::Polymorphic {
                type_column,
                from_label_column,
                to_label_column,
                ..
            } => (
                type_column.clone(),
                from_label_column.clone(),
                to_label_column.clone(),
            ),
            _ => (None, None, None),
        }
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

        // Convert ShortestPathMode from logical plan type if present
        let shortest_path_mode = context
            .shortest_path_mode
            .as_ref()
            .map(|m| m.clone().into());

        // ✅ REFACTORING COMPLETE: Use refactored DenormalizedCteStrategy directly
        if self.is_denormalized {
            let strategy = DenormalizedCteStrategy {
                pattern_ctx: self.pattern_ctx.clone(),
                table: self.rel_table.clone(),
                from_col: self.rel_from_col.clone(),
                to_col: self.rel_to_col.clone(),
                schema: Arc::new(schema.clone()),
            };

            log::debug!(
                "🔧 Using NEW DenormalizedCteStrategy for {}-[*]->{}",
                self.pattern_ctx.left_node_alias,
                self.pattern_ctx.right_node_alias
            );

            return strategy.generate_sql(context, properties, filters);
        }

        // Build the generator based on pattern type (Traditional/FK-Edge schemas)
        let mut generator = if self.is_denormalized {
            // Dead code - we return above for denormalized
            unreachable!("Denormalized case handled above");
            VariableLengthCteGenerator::new_denormalized(
                schema,
                context.spec.clone(),
                &self.rel_table,
                &self.rel_from_col,
                &self.rel_to_col,
                &self.pattern_ctx.left_node_alias,
                &self.pattern_ctx.right_node_alias,
                context.relationship_cypher_alias.as_deref().unwrap_or(""),
                properties.to_vec(),
                shortest_path_mode,
                filters.start_sql.clone(),
                filters.end_sql.clone(),
                filters.relationship_sql.clone(),
                context.path_variable.clone(),
                context.relationship_types.clone(),
                context.edge_id.clone(),
            )
        } else if self.start_is_denormalized != self.end_is_denormalized {
            // Mixed access pattern
            VariableLengthCteGenerator::new_mixed(
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
                context.relationship_cypher_alias.as_deref().unwrap_or(""),
                properties.to_vec(),
                shortest_path_mode,
                filters.start_sql.clone(),
                filters.end_sql.clone(),
                filters.relationship_sql.clone(),
                context.path_variable.clone(),
                context.relationship_types.clone(),
                context.edge_id.clone(),
                self.start_is_denormalized,
                self.end_is_denormalized,
            )
        } else {
            // Traditional or FK-edge pattern
            VariableLengthCteGenerator::new_with_fk_edge(
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
                context.relationship_cypher_alias.as_deref().unwrap_or(""),
                properties.to_vec(),
                shortest_path_mode,
                filters.start_sql.clone(),
                filters.end_sql.clone(),
                filters.relationship_sql.clone(),
                context.path_variable.clone(),
                context.relationship_types.clone(),
                context.edge_id.clone(),
                self.type_column.clone(),
                self.from_label_column.clone(),
                self.to_label_column.clone(),
                context
                    .start_node_label
                    .clone()
                    .or_else(|| self.from_node_label.clone()),
                context
                    .end_node_label
                    .clone()
                    .or_else(|| self.to_node_label.clone()),
                self.is_fk_edge,
            )
        };

        // For heterogeneous polymorphic paths (different start/end labels with to_label_column),
        // set intermediate node info to enable proper recursive traversal.
        // The intermediate type is the same as start type (e.g., Group→Group recursion).
        // Use context labels if available, otherwise fall back to strategy fields
        let effective_start_label = context
            .start_node_label
            .as_ref()
            .or(self.from_node_label.as_ref());
        let effective_end_label = context
            .end_node_label
            .as_ref()
            .or(self.to_node_label.as_ref());

        if self.to_label_column.is_some() {
            if let (Some(from_label), Some(to_label)) = (effective_start_label, effective_end_label)
            {
                if from_label != to_label {
                    log::info!(
                        "CteManager: Setting intermediate node for heterogeneous polymorphic path"
                    );
                    log::info!("  - start_label: {}, end_label: {}", from_label, to_label);
                    log::info!(
                        "  - intermediate: table={}, id_col={}, label={}",
                        self.start_table,
                        self.start_id_col,
                        from_label
                    );
                    generator.set_intermediate_node(
                        &self.start_table,
                        &self.start_id_col,
                        from_label,
                    );
                }
            }
        }

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

        // Build column metadata
        let columns = CteGenerationResult::build_vlp_column_metadata(
            &self.pattern_ctx.left_node_alias,
            &self.pattern_ctx.right_node_alias,
            properties,
            &self.start_id_col,
        );

        // Build VLP endpoint info
        let vlp_endpoint = VlpEndpointInfo {
            start_alias: "start_node".to_string(),
            end_alias: "end_node".to_string(),
            start_table: self.start_table.clone(),
            end_table: self.end_table.clone(),
            cypher_start_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_end_alias: self.pattern_ctx.right_node_alias.clone(),
            start_id_col: self.start_id_col.clone(),
            end_id_col: self.end_id_col.clone(),
            path_variable: context.path_variable.clone(),
        };

        Ok(CteGenerationResult {
            sql,
            parameters: vec![],
            cte_name,
            recursive: true,
            from_alias: VLP_CTE_FROM_ALIAS.to_string(),
            columns,
            vlp_endpoint: Some(vlp_endpoint),
            outer_where_filters: None,
        })
    }

    /// Validate the strategy against pattern constraints
    pub fn validate(&self, _pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        // Basic validation - ensure we have necessary table info
        if self.rel_table.is_empty() {
            return Err(CteError::SchemaValidationError(
                "Relationship table name is required".into(),
            ));
        }
        if self.start_id_col.is_empty() || self.end_id_col.is_empty() {
            return Err(CteError::SchemaValidationError(
                "Node ID columns are required".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use crate::graph_catalog::schema_types::SchemaType;
    use std::collections::HashMap;

    #[test]
    fn test_traditional_cte_strategy_basic() {
        // Create a simple pattern context for testing
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "u1".to_string(),
            right_node_alias: "u2".to_string(),
            rel_alias: "r".to_string(),
            join_strategy: JoinStrategy::Traditional {
                left_join_col: Identifier::from("follower_id"),
                right_join_col: Identifier::from("followed_id"),
            },
            // Fill in other required fields with defaults
            left_node: NodeAccessStrategy::OwnTable {
                table: "users_bench".to_string(),
                id_column: Identifier::from("user_id"),
                properties: std::collections::HashMap::new(),
            },
            right_node: NodeAccessStrategy::OwnTable {
                table: "users_bench".to_string(),
                id_column: Identifier::from("user_id"),
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
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

        // Create an empty schema for the test
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        ));

        // Create strategy
        let strategy = DenormalizedCteStrategy::new(&pattern_ctx, schema);
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
        };

        // Generate SQL
        let result = strategy.generate_sql(&context, &properties, &filters);

        // Should succeed
        assert!(result.is_ok());

        let generation_result = result.unwrap();

        // Check basic properties
        assert!(generation_result.recursive);
        assert!(generation_result.cte_name.starts_with("vlp_f1_"));
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
                node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
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

        let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

        // Create a FK-edge pattern context (hierarchical relationship via FK column)
        let pattern_ctx = PatternSchemaContext {
            left_node_alias: "parent".to_string(),
            right_node_alias: "child".to_string(),
            rel_alias: "hierarchy".to_string(),
            left_node: NodeAccessStrategy::OwnTable {
                table: "files".to_string(),
                id_column: Identifier::from("id"),
                properties: HashMap::new(), // Simplified for test
            },
            right_node: NodeAccessStrategy::OwnTable {
                table: "files".to_string(),
                id_column: Identifier::from("id"),
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
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
                id_column: Identifier::from("user_id"),
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
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
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
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
