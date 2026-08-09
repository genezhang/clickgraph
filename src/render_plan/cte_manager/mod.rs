//! Unified CTE Manager for schema-aware CTE generation
//!
//! This module provides a strategy-pattern based approach to CTE generation
//! that handles all ClickGraph schema variations through unified interfaces.

use std::sync::Arc;

use crate::clickhouse_query_generator::variable_length_cte::{
    EdgeIdentity, EdgeUniquenessPolicy, NodeProperty, VariableLengthCteGenerator,
};
use crate::graph_catalog::{
    config::Identifier, graph_schema::GraphSchema, EdgeAccessStrategy, JoinStrategy,
    NodeAccessStrategy, PatternSchemaContext,
};
use crate::query_planner::join_context::{
    VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
};
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::cte_extraction::collect_parameters_from_filters;
use crate::render_plan::cte_generation::CteGenerationContext;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::filter_pipeline::CategorizedFilters;

/// Dialect-correct array literal. CH: `[a, b]`. Spark: `array(a, b)`.
/// Thin shorthand over `FunctionMapper::array_literal` — CTE generation
/// builds these in many places (path_edges, path_nodes).
fn arr(elems: &str) -> String {
    crate::sql_generator::function_mapper::current_function_mapper().array_literal(elems)
}

/// `arrayConcat(arr_expr, [scalar])` — appends a single scalar onto an
/// array. Goes through `FunctionMapper` for both the function name
/// (`arrayConcat` / `concat`) and the array literal shape (`[x]` /
/// `array(x)`).
fn arr_append(arr_expr: &str, scalar: &str) -> String {
    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
    format!(
        "{}({}, {})",
        mapper.array_concat(),
        arr_expr,
        mapper.array_literal(scalar)
    )
}

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
    /// Path variable name (e.g., "p" in `MATCH p = (a)-[*]->(b)`)
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

/// Denormalized-schema strategy: node properties are embedded in the edge table.
pub struct DenormalizedCteStrategy {
    pattern_ctx: PatternSchemaContext,
    table: String,
    from_col: String,
    to_col: String,
    schema: Arc<GraphSchema>,
    /// The policy value spelling this walk's edge identity (the #887
    /// [`EdgeIdentity`]). A denormalized VLP is a single directional recursive
    /// self-join over the edge table (no #617 doubled-edge CTE), so the
    /// identity is always the plain [`EdgeIdentity::EdgeIdColumns`]: the
    /// schema-declared `edge_id` (resolved once at construction) when present
    /// — distinguishing parallel edges that share one `(from, to)` node pair,
    /// e.g. two flights on one route keyed by `flight_id` (#710) — else the
    /// `(from, to)` pair. Consumed by [`Self::edge_tuple`] to spell the
    /// trail-uniqueness key (the denormalized counterpart of #806/#606's
    /// flat/recursive paths).
    identity: EdgeIdentity,
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
            JoinStrategy::SingleTableScan { table } => {
                let from_col = Self::get_from_column(pattern_ctx)?;
                let to_col = Self::get_to_column(pattern_ctx)?;
                Ok(Self {
                    pattern_ctx: pattern_ctx.clone(),
                    table: table.clone(),
                    from_col: from_col.clone(),
                    to_col: to_col.clone(),
                    identity: EdgeIdentity::EdgeIdColumns {
                        edge_id: Self::resolve_edge_id(pattern_ctx, &schema),
                        from_col,
                        to_col,
                    },
                    schema,
                })
            }
            _ => Err(CteError::InvalidStrategy(
                "DenormalizedCteStrategy requires JoinStrategy::SingleTableScan".into(),
            )),
        }
    }

    /// Resolve the relationship's schema-declared composite `edge_id`, if any,
    /// from its first relationship type. Mirrors the emitter's resolution at
    /// `cte_extraction.rs` (`schema.get_rel_schema(label).edge_id`), so the
    /// denormalized path spells edge identity the same way the flat/recursive
    /// paths do (#710/#806/#606). Returns `None` when there is no rel type or
    /// the schema declares no `edge_id` — the historical `(from, to)` behavior.
    fn resolve_edge_id(
        pattern_ctx: &PatternSchemaContext,
        schema: &GraphSchema,
    ) -> Option<Identifier> {
        pattern_ctx
            .rel_types
            .first()
            .and_then(|label| schema.get_rel_schema(label).ok())
            .and_then(|rel_schema| rel_schema.edge_id.clone())
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

    /// Whether this denormalized VLP should enforce Cypher's default
    /// relationship-uniqueness (an edge may not be reused, but a node MAY be
    /// revisited) via a `path_edges` array of `(from, to)` edge tuples, instead
    /// of the stronger node-uniqueness via `path_nodes`.
    ///
    /// This is the denormalized-strategy counterpart of the standard emitter's
    /// [`VariableLengthCteGenerator::uses_edge_uniqueness`] (#598 part 2). It
    /// mirrors that helper's two shape guards, both of which prevent a
    /// base/recursive column-shape mismatch (unbound-identifier, cf. #469) and
    /// preserve intended semantics:
    ///
    /// - **shortestPath** stays node-unique: revisiting a node can never
    ///   shorten a path, and the shortest-path arms have their own base shape.
    /// - **open zero-hop `*0..N`** (`effective_min_hops() == 0` and NOT a
    ///   closed pattern) stays node-unique: its base row is a node paired
    ///   with itself and carries no edge, so it cannot seed a 1-hop
    ///   `path_edges` tuple; seeding it only in the ordinary base would
    ///   diverge from the zero-hop base's column shape.
    /// - **closed zero-hop `*0..N`** (`(a)-[*0..N]->(a)`) IS edge-unique
    ///   (#628 mirror): counting real cycles requires edge-uniqueness —
    ///   node-uniqueness structurally forbids returning to the start, so
    ///   every cycle is dropped and only the zero-length self rows survive
    ///   (the #605/#625/#980 fail-loud, now lifted). The zero-hop base has no
    ///   edge, but it CAN seed an empty `path_edges` array — a TYPED-empty
    ///   one via [`typed_empty_edges_seed`]
    ///   (`(SELECT arraySlice([tuple(__seed_edge.f1, …)], 1, 0) FROM <edge
    ///   table> AS __seed_edge LIMIT 1)`; a bare `[]` = `Array(Nothing)`
    ///   fails ClickHouse recursive-CTE column unification against the
    ///   recursive arm's `Array(Tuple(...))`, proven live on #887 — see the
    ///   zero-hop base for the full story); hops >= 1 accumulate and dedupe
    ///   normally. Scoped to the closed case so an open `*0..N` (whose
    ///   node-uniqueness is a separate, non-cyclic concern) is unchanged.
    ///
    /// A denormalized edge's identity is its schema-declared `edge_id` when one
    /// is present, else the `(from_col, to_col)` node pair — see
    /// [`Self::edge_tuple`] (#710).
    ///
    /// #887 Phase 1–2: the decision now lives in the shared
    /// [`EdgeUniquenessPolicy`] ([`Self::edge_uniqueness_policy`]); this
    /// delegates so the base-case `path_edges` seed sites stay byte-identical.
    fn uses_edge_uniqueness(&self, context: &CteGenerationContext) -> bool {
        self.edge_uniqueness_policy(context).uses_edge_uniqueness()
    }

    /// Whether this VLP is a CLOSED pattern — the same Cypher variable on both
    /// endpoints (`(a)-[*..]->(a)`). The planner leaves the two connection
    /// aliases equal for a same-variable pattern (never renaming one), so the
    /// strategy sees `left_node_alias == right_node_alias` — the same
    /// alias-equality invariant the standard emitter's
    /// [`VariableLengthCteGenerator::is_closed_pattern`] relies on (#625/#628).
    /// A closed pattern counts cycles (the outer query adds `start_id =
    /// end_id`); see #625/#628/#980.
    fn is_closed_pattern(&self) -> bool {
        self.pattern_ctx.left_node_alias == self.pattern_ctx.right_node_alias
    }

    /// The edge-identity value for one hop, read off `rel_alias` (base case) or
    /// `next` (recursive case), seeded into / matched against `path_edges` for
    /// trail-uniqueness.
    ///
    /// Spelled via the shared policy value [`Self::identity`] (the #887
    /// [`EdgeIdentity`]) — the same spelling the standard emitter uses in
    /// [`VariableLengthCteGenerator::build_edge_tuple_recursive`] (#710):
    ///
    /// - `Single(col)` → bare `rel_alias.col` (scalar element),
    /// - `Composite(cols)` → `tuple(rel_alias.c1, rel_alias.c2, …)`,
    /// - `None` → the historical `tuple(rel_alias.from, rel_alias.to)` pair
    ///   (byte-identical to the pre-#710 behavior).
    ///
    /// A denormalized VLP is a single directional recursive self-join over the
    /// edge table (no #617 doubled-edge CTE), so no from/to orientation swap is
    /// needed — the identity is the plain [`EdgeIdentity::EdgeIdColumns`].
    fn edge_tuple(&self, rel_alias: &str) -> String {
        self.identity.spell(
            crate::sql_generator::function_mapper::current_function_mapper().tuple_constructor(),
            rel_alias,
        )
    }

    /// Build the [`EdgeUniquenessPolicy`] for this denormalized VLP (#887
    /// Phase 1–2), from the same terms `uses_edge_uniqueness` reads plus the
    /// [`Self::identity`] value `edge_tuple` already spells through. A denorm
    /// VLP is never heterogeneous-polymorphic (both endpoints are embedded in
    /// the edge), so `is_hetero_poly` is `false` — byte-identical to the inline
    /// CM predicate. During the spike phase the resulting decision is asserted
    /// equal to the inline `uses_edge_uniqueness(context)` at the gate site.
    fn edge_uniqueness_policy(&self, context: &CteGenerationContext) -> EdgeUniquenessPolicy {
        EdgeUniquenessPolicy::new(
            context.shortest_path_mode.is_some(),
            false,
            context.spec.effective_min_hops(),
            self.is_closed_pattern(),
            self.identity.clone(),
        )
    }

    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters,
    ) -> Result<CteGenerationResult, CteError> {
        // Generate CTE name using vlp_{start}_{end} to match from_builder.rs expectation
        let cte_name = format!(
            "vlp_{}_{}",
            self.pattern_ctx.left_node_alias, self.pattern_ctx.right_node_alias
        );

        // Build the recursive CTE SQL for denormalized schema
        let sql = self.generate_recursive_cte_sql(context, properties, filters)?;

        // Build column metadata for denormalized schema
        let mut columns = Vec::new();

        // #678: resolve the endpoint's logical node_id property name (e.g. `code`)
        // rather than hardcoding the placeholder `"id"`. The shared
        // `build_vlp_column_metadata` (used by every OTHER strategy) sets the id
        // column's `cypher_property` to the real logical id name; only the denorm
        // builder hand-rolled it to `"id"`. That mismatch made the #620/#677 VLP
        // WITH-item id pre-pass (which fires when `prop_access.column ==
        // cypher_property`) miss `WITH a.code AS x` over a denorm VLP, so it fell
        // through to the blind prefix rewriter → `t.start_code` (a column the CTE
        // never projects; it projects `start_id`/`end_id`) → Code 47.
        // Composite ids stay `"id"` (loud), per #683-r2 / #605 composite discipline.
        let start_id_prop = self
            .denorm_node_id_property_name()
            .unwrap_or_else(|| "id".to_string());
        let end_id_prop = start_id_prop.clone();

        // Add start node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_START_ID_COLUMN.to_string(),
            cypher_alias: self.pattern_ctx.left_node_alias.clone(),
            cypher_property: start_id_prop,
            db_column: self.from_col.clone(),
            is_id_column: true,
            vlp_position: Some(VlpColumnPosition::Start),
        });

        // Add end node ID column
        columns.push(CteColumnMetadata {
            cte_column_name: VLP_END_ID_COLUMN.to_string(),
            cypher_alias: self.pattern_ctx.right_node_alias.clone(),
            cypher_property: end_id_prop,
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

    /// Resolve the endpoint node's **logical** node_id property name (e.g. `code`)
    /// for a denormalized VLP, mirroring `map_denormalized_property`'s node-schema
    /// resolution (match the node schema whose table is this edge/denorm table).
    ///
    /// This name is used ONLY as a match-key for the #620/#677 WITH-item id
    /// pre-pass: when the accessed column equals it, the pre-pass rewrites the
    /// access to the CTE's **position-based** id column (`start_id` = from-role,
    /// `end_id` = to-role), the position taken from the alias→start/end mapping,
    /// NOT from this name. So the name only decides *whether* the pre-pass fires,
    /// never *which* column it targets; a wrong or missing resolution can at worst
    /// cause a loud Code-47 miss, never a wrong-position value.
    ///
    /// Returns `None` (→ caller falls back to the `"id"` placeholder, keeping the
    /// pre-existing loud behavior) when:
    /// - the id is composite (#683-r2 / #605 composite discipline), or
    /// - no node schema matches the table, or
    /// - **more than one** node schema is hosted on the table with a distinct
    ///   node_id (e.g. zeek's `dns_log` hosts IP/`ip_address`, Domain/`domain_name`,
    ///   ResolvedIP/`answers`). Picking one arbitrarily would let a *malformed*
    ///   query (`WITH a.domain_name` where `a` is an `:IP`) silently match and drop
    ///   to `start_id` instead of erroring — a loud→silent regression. Staying
    ///   `None` keeps those loud. Single-label denorm tables (the common case:
    ///   `flights_denorm` hosts only `Airport`) resolve normally.
    fn denorm_node_id_property_name(&self) -> Option<String> {
        let node_schemas = self.schema.all_node_schemas();
        let rel_table_name = self.table.rsplit('.').next().unwrap_or(&self.table);

        let mut matches = node_schemas.values().filter(|n| {
            let schema_table = n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
            schema_table == rel_table_name
        });

        let node_schema = matches.next()?;

        if node_schema.node_id.is_composite() {
            return None;
        }
        let id_prop = node_schema.node_id.column_or_error().ok()?;

        // If another node schema on the SAME table has a DIFFERENT logical id,
        // the table is multi-label (heterogeneous endpoints) and we cannot safely
        // pick one id for both VLP endpoints — stay `None`/loud rather than risk
        // a loud→silent match on a mislabeled property access.
        for other in matches {
            if other.node_id.is_composite() || other.node_id.column_or_error().ok() != Some(id_prop)
            {
                return None;
            }
        }
        Some(id_prop.to_string())
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
            // Try logical property name first (e.g., "code" → "origin_code")
            if let Some(physical_col) = map.get(logical_prop) {
                return Ok(physical_col.clone());
            }
            // Also try reverse lookup: caller may have passed the physical column name
            // (e.g., "origin_code" → find that it maps to "origin_code" via key "code")
            if let Some(physical_col) = map.values().find(|v| v.as_str() == logical_prop) {
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
        // Wrapper is needed for end-node filters OR when min_hops > 1 (to filter out
        // shorter paths from the recursive CTE output).
        let (recursive_cte_name, needs_wrapper) = if filters.end_sql.is_some() || min_hops > 1 {
            (format!("{}_inner", cte_name), true)
        } else {
            (cte_name.clone(), false)
        };

        // #489: `*0..N` needs a genuine zero-hop base case (the start node
        // paired with itself, `hop_count = 0`) — mirrors the standard
        // (non-denormalized) VLP CTE, which scans the real node table and
        // seeds the recursion from there. Denormalized/virtual-node schemas
        // have no separate node table, so the zero-hop "node universe" is
        // synthesized from the edge table itself (see
        // `generate_zero_hop_base_case_sql`). This REPLACES the ordinary
        // 1-hop base case (rather than adding alongside it): hop_count=1
        // rows are produced by the recursive term extending from the
        // zero-hop rows, exactly reproducing what the direct 1-hop scan
        // would have produced — adding both would double-count every
        // 1-hop row.
        let is_zero_hop = min_hops == 0;
        let base_case = if is_zero_hop {
            self.generate_zero_hop_base_case_sql(context, properties)?
        } else {
            self.generate_base_case_sql(context, properties, filters)?
        };

        // Generate recursive case if needed.
        // Note: needs_recursion depends on max_hops > 1 (not > min_hops) because the
        // ordinary base case always starts at hop_count=1, so we need recursion
        // whenever max > 1. For the zero-hop base case (hop_count=0), recursion
        // is needed whenever max_hops allows reaching hop_count>=1 at all.
        let needs_recursion = if is_zero_hop {
            max_hops.is_none_or(|max| max >= 1)
        } else {
            max_hops.is_none_or(|max| max > 1)
        };
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
            let inner_cte = format!(
                "{} AS (\n{}{}\n)",
                recursive_cte_name, base_case, recursive_case
            );

            // Build WHERE clause for outer CTE
            let mut where_conditions = Vec::new();

            // End-node filter (e.g., b.code = 'ATL') rewritten for CTE columns
            if let Some(ref end_filter) = filters.end_sql {
                // filters.end_sql uses the relationship table alias (e.g., "f.Dest = 'ATL'")
                // In the outer CTE, replace "f.COLUMN" → "end_COLUMN"
                let rewritten =
                    end_filter.replace(&format!("{}.", self.pattern_ctx.rel_alias), "end_");
                where_conditions.push(rewritten);
            }

            // Min-hops filter: exclude paths shorter than requested minimum
            if min_hops > 1 {
                where_conditions.push(format!("hop_count >= {}", min_hops));
            }

            let where_clause = where_conditions.join(" AND ");

            // Return TWO CTEs without WITH RECURSIVE prefix (added by Ctes::to_sql())
            // Format: inner_cte AS (...), outer_cte AS (SELECT ... WHERE ...)
            Ok(format!(
                "{},\n{} AS (\n    SELECT * FROM {} WHERE {}\n)",
                inner_cte, cte_name, recursive_cte_name, where_clause
            ))
        } else {
            // No end filter: simple single CTE without WITH RECURSIVE prefix
            Ok(format!(
                "{} AS (\n{}{}\n)",
                recursive_cte_name, base_case, recursive_case
            ))
        }
    }

    /// Quoted relationship-type literal for the path_relationships array
    /// (e.g. `'FLIGHT'`), or `""` when no path variable is bound (no growth
    /// needed — mirrors the standard emitter's needs_path_data gating) or the
    /// type is unknown. Relationship types may arrive as composite schema keys
    /// (`TYPE::FromLabel::ToLabel`); only the Cypher-visible type name is
    /// emitted.
    fn relationship_type_literal(&self, context: &CteGenerationContext) -> String {
        if context.path_variable.is_none() {
            return String::new();
        }
        context
            .relationship_types
            .as_ref()
            .and_then(|types| types.first())
            .map(|t| {
                let type_name = crate::graph_catalog::composite_key_utils::extract_type_name(t);
                format!("'{}'", type_name)
            })
            .unwrap_or_default()
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
            // path_edges seeds the trail-uniqueness array. Under edge-uniqueness
            // (#606) it holds the `(from, to)` edge-identity tuple so a physical
            // edge is deduplicated while nodes may still be revisited; otherwise
            // it keeps the legacy single-`from_col` form (node-uniqueness paths
            // never consult it — they filter on `path_nodes`).
            if self.uses_edge_uniqueness(context) {
                format!(
                    "{} as path_edges",
                    arr(&self.edge_tuple(&self.pattern_ctx.rel_alias))
                )
            } else {
                format!(
                    "{} as path_edges",
                    arr(&format!("{}.{}", self.pattern_ctx.rel_alias, self.from_col))
                )
            },
            format!(
                "{} as path_nodes",
                arr(&format!(
                    "{}.{}, {}.{}",
                    self.pattern_ctx.rel_alias,
                    self.from_col,
                    self.pattern_ctx.rel_alias,
                    self.to_col
                ))
            ),
            // path_relationships: part of the cross-strategy VLP CTE contract —
            // the standard emitter (variable_length_cte.rs) always projects it,
            // and the path materializer consumes it for `RETURN p`:
            // tuple(path_nodes, path_relationships, hop_count) (#469).
            // Populated only when a path variable is bound (mirrors the
            // standard emitter's needs_path_data gating); `[]` otherwise so
            // the column always exists.
            format!(
                "{} as path_relationships",
                arr(&self.relationship_type_literal(context))
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

    /// Generate the zero-hop base case SQL for `*0..N` VLP on a denormalized
    /// (single-table / virtual-node) schema (#489).
    ///
    /// The standard (non-denormalized) VLP CTE's zero-hop base case scans
    /// the real node table and pairs each node with itself (`start_id =
    /// end_id = node.id`, `hop_count = 0`) — the start node standing in as
    /// its own trivial path. Denormalized/virtual-node schemas have no
    /// separate node table: a node's identity and properties only exist
    /// embedded in the edge table, in two different roles (see
    /// `map_denormalized_property`'s `is_from_node` parameter — the origin
    /// role vs. the destination role each have their own property-name
    /// mapping in the schema). The zero-hop "node universe" is therefore
    /// synthesized as the UNION of distinct node ids appearing in either
    /// role, so a node that only ever appears on one side (e.g. an airport
    /// with no outbound flights in the sample data) still gets a zero-hop
    /// row.
    ///
    /// This assumes property values are consistent for the same node id
    /// regardless of which role (origin/destination) it was scanned under —
    /// the same assumption every other denormalized-VLP code path in this
    /// module already relies on (see `map_denormalized_property`). If a
    /// requested property is only defined for one role (an asymmetric
    /// schema mapping), this fails loudly (`CteError`) rather than silently
    /// emitting a NULL-filled or mistyped column.
    fn generate_zero_hop_base_case_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
    ) -> Result<String, CteError> {
        let mut from_role_cols = vec![format!("{} AS __node_id", self.from_col)];
        let mut to_role_cols = vec![format!("{} AS __node_id", self.to_col)];
        // (canonical alias, source property) pairs, in the SAME order as
        // `properties`, matching `add_property_selections`'s column
        // ordering so this branch's column list lines up positionally with
        // the ordinary 1-hop base case's for the UNION ALL.
        let mut canon_props: Vec<(String, &NodeProperty)> = Vec::new();

        for (i, prop) in properties.iter().enumerate() {
            // Skip ID column, mirroring `add_property_selections` (already
            // covered by start_id/end_id above).
            if prop.alias == "id" {
                continue;
            }
            // `prop.column_name` is already resolved to prop's OWN role's
            // physical column (e.g. "OriginCityName" when the property was
            // requested for the from-node side) — `map_denormalized_property`
            // only round-trips it for THAT role via its reverse-lookup
            // fallback, not the opposite one. `prop.alias` is the portable
            // logical name (e.g. "city") that exists as a key in each role's
            // property-name mapping, so it's what we need to resolve the
            // OPPOSITE role's physical column too.
            let from_physical = self.map_denormalized_property(&prop.alias, true)?;
            let to_physical = self.map_denormalized_property(&prop.alias, false)?;
            let canon_alias = format!("__prop_{}", i);
            from_role_cols.push(format!("{} AS {}", from_physical, canon_alias));
            to_role_cols.push(format!("{} AS {}", to_physical, canon_alias));
            canon_props.push((canon_alias, prop));
        }

        let node_universe = format!(
            "(\n            SELECT DISTINCT {}\n            FROM {}\n            UNION DISTINCT\n            SELECT DISTINCT {}\n            FROM {}\n        ) AS node_universe",
            from_role_cols.join(", "),
            self.table,
            to_role_cols.join(", "),
            self.table,
        );

        // Empty arrays need an explicit type cast here (unlike the ordinary
        // 1-hop base case, whose `path_edges`/`path_relationships` always
        // carry at least one real String element): a bare `[]` infers as
        // `Array(Nothing)`, which the recursive CTE engine rejects when the
        // recursive term's `arrayConcat(path_edges, [...String...])`
        // produces `Array(String)` for the same column ("Conversion from
        // String to Nothing is not supported"). Goes through FunctionMapper
        // per the dialect-dispatch rule rather than an inline literal, so
        // every supported SQL dialect gets its own correctly-typed cast.
        //
        // #628 (denorm mirror, #887 Phase 2b): a CLOSED `*0..N` walk is
        // edge-unique (see `uses_edge_uniqueness`), whose recursive arm
        // appends a real `(from, to)` EDGE TUPLE to `path_edges` — the
        // string-typed cast above would NOT unify with that. A bare `[]`
        // (`Array(Nothing)`) fails LIVE too: ClickHouse's recursive CTE
        // engine rejects the base arm's `Array(Nothing)` against the
        // recursive arm's `Array(Tuple(...))` with CANNOT_CONVERT_TYPE
        // (proven on #887 — the old "bottom type unifies" premise is false
        // for recursive-CTE column unification). The seed must instead be an
        // EMPTY but correctly-typed array, which is only knowable from the
        // EDGE table: `typed_empty_edges_seed` pulls a one-element identity
        // tuple out of the edge source and takes an empty slice of it
        // (`(SELECT arraySlice([tuple(__seed_edge.f1, …)], 1, 0) FROM
        // <edge table> AS __seed_edge LIMIT 1)`) — a scalar subquery
        // ClickHouse hoists, evaluated once. The recursive arm's
        // `NOT has(path_edges, …)` then dedupes edges from hop 1 onward.
        // Gated identically to the base/recursive arms via
        // `uses_edge_uniqueness(context)`, so a pattern that stays
        // node-unique (open `*0..N`, shortestPath) is byte-unchanged.
        let empty_string_array = crate::sql_generator::function_mapper::current_function_mapper()
            .empty_string_array_cast();
        let path_edges_seed = if self.uses_edge_uniqueness(context) {
            format!(
                "{} as path_edges",
                crate::clickhouse_query_generator::variable_length_cte::typed_empty_edges_seed(
                    &self.table,
                    &self.edge_tuple("__seed_edge")
                )
            )
        } else {
            format!("{} as path_edges", empty_string_array)
        };
        let mut select_items = vec![
            "node_universe.__node_id as start_id".to_string(),
            "node_universe.__node_id as end_id".to_string(),
            "0 as hop_count".to_string(),
            path_edges_seed,
            format!("{} as path_nodes", arr("node_universe.__node_id")),
            format!("{} as path_relationships", empty_string_array),
        ];

        for (canon_alias, prop) in &canon_props {
            let is_from_node = prop.cypher_alias == self.pattern_ctx.left_node_alias;
            let prefix = if is_from_node { "start_" } else { "end_" };
            let physical_col = self.map_denormalized_property(&prop.column_name, is_from_node)?;
            select_items.push(format!(
                "node_universe.{} as {}{}",
                canon_alias, prefix, physical_col
            ));
        }

        let select_clause = select_items.join(",\n        ");

        Ok(format!(
            "    SELECT\n        {}\n    FROM {}",
            select_clause, node_universe
        ))
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
        //
        // `start_id` must carry the TRUE start of the whole VLP path forward
        // unchanged on every recursive hop — it's the generic join-key column
        // consumers correlate against (e.g. a fixed hop preceding this VLP,
        // via `PlanCtx::get_vlp_join_reference` / #524). It must NOT be
        // reassigned to `next.{from_col}` (the current hop's own origin,
        // which is just `vp.end_id` restated) — that drifts start_id to the
        // latest intermediate node on every hop beyond the first, silently
        // corrupting any downstream correlation for hop_count > 1 rows.
        // Mirrors the already-correct pattern in `add_recursive_property_selections`
        // below, which carries forward `vp.start_{property}` for the same reason.
        let mut select_items = vec![
            "vp.start_id as start_id".to_string(),
            format!("next.{} as end_id", self.to_col),
            "vp.hop_count + 1".to_string(),
            // Extend path_edges. Under edge-uniqueness (#606) append the new
            // hop's `(from, to)` edge tuple so the shape matches the base seed
            // and the recursive cycle check below; otherwise keep the legacy
            // single-`from_col` append (unused by node-uniqueness filtering).
            if self.uses_edge_uniqueness(context) {
                arr_append("vp.path_edges", &self.edge_tuple("next"))
            } else {
                arr_append("vp.path_edges", &format!("next.{}", self.from_col))
            }, // Extend edge path array
            arr_append("vp.path_nodes", &format!("next.{}", self.to_col)), // Extend node path array
            // Extend the relationship-type path array (see base case for why
            // this column is part of the VLP CTE contract). Grows only when a
            // path variable is bound; stays `[]` otherwise.
            {
                let rel_type = self.relationship_type_literal(context);
                if rel_type.is_empty() {
                    format!("{} as path_relationships", arr(""))
                } else {
                    format!(
                        "{} as path_relationships",
                        arr_append("vp.path_relationships", &rel_type)
                    )
                }
            },
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
            // ⚠️ ClickHouse limitation: NOT IN with array doesn't work in recursive CTEs.
            // Use NOT <array_contains>(array, element) for cycle detection. The membership
            // function is dialect-specific: CH `has`, Spark `array_contains`.
            //
            // Under edge-uniqueness (#606) test membership of the new hop's
            // `(from, to)` edge tuple against path_edges — a physical edge may
            // not repeat, but a node MAY be revisited (Cypher's default
            // relationship-uniqueness). Otherwise fall back to node-uniqueness
            // (reject any revisited node) via path_nodes.
            // #887 Phase 1–2: routed through the shared EdgeUniquenessPolicy
            // (identity spelled off "next", node fallback on `next.<to_col>`) —
            // byte-identical to the old inline gate, proven by the spike.
            self.edge_uniqueness_policy(context)
                .recursive_cycle_predicate("next", &format!("next.{}", self.to_col)), // Cycle prevention
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
        let mapper = crate::sql_generator::function_mapper::current_function_mapper();
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

            // #558: `physical_col` may itself contain a literal dot (e.g.
            // zeek's Tuple/Nested-style `id.orig_h`). ClickHouse accepts the
            // unquoted dotted form on READ (`t.id.orig_h`, a compound
            // identifier), but the same text is invalid when it appears as
            // an output alias (`AS start_id.orig_h` is a syntax error,
            // ClickHouse Code 62 — aliases don't get the compound-identifier
            // grammar). Quote both sides through the dialect-dispatched
            // `FunctionMapper::quote_alias` (same helper `quote_qualified_col`
            // uses for the outer-query reference to this very column, so the
            // CTE-defined name and the reference to it agree byte-for-byte).
            let quoted_read_col = mapper.quote_alias(&physical_col);
            let cte_col_name = format!("{}{}", prefix, physical_col);
            let quoted_alias = mapper.quote_alias(&cte_col_name);

            let sql = format!(
                "{}.{} as {}",
                self.pattern_ctx.rel_alias, quoted_read_col, quoted_alias
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
        let mapper = crate::sql_generator::function_mapper::current_function_mapper();
        for prop in properties {
            // Determine if this property belongs to start (from) or end (to) node
            let is_from_node = prop.cypher_alias == self.pattern_ctx.left_node_alias;
            let prefix = if is_from_node { "start_" } else { "end_" };

            // Map logical property to physical column in edge table
            let physical_col = self.map_denormalized_property(&prop.column_name, is_from_node)?;
            let cte_col_name = format!("{}{}", prefix, physical_col);
            let quoted_alias = mapper.quote_alias(&cte_col_name);

            if is_from_node {
                // Start node property comes from previous iteration (already
                // has prefix) — `vp` is the recursive CTE's own alias, so its
                // column really is named `start_<physical_col>` (dot and
                // all); quote both the reference and the re-asserted alias.
                select_items.push(format!("vp.{} as {}", quoted_alias, quoted_alias));
            } else {
                // End node property comes from the new edge being joined —
                // `next` is the real physical table, so quote the read side
                // like the base case does, and quote the alias for the same
                // Code-62 reason.
                let quoted_read_col = mapper.quote_alias(&physical_col);
                select_items.push(format!("next.{} as {}", quoted_read_col, quoted_alias));
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
        // Only max_hops limit in base case — the base case always produces hop_count=1 rows.
        // The min_hops >= N filter is applied in the outer wrapper CTE, not here.
        //
        // #525: this used to push `hop_count <= {max_hops}` — but `hop_count`
        // is a SELECT-list alias (`1 as hop_count`) in the SAME SELECT, not a
        // real column, and standard SQL forbids WHERE from referencing a
        // SELECT-list alias. It only "worked" because ClickHouse
        // non-standardly substitutes the alias's expression, evaluating
        // `1 <= {max_hops}` — a tautology for every max_hops >= 1, never a
        // real bound (the base case is hardcoded to hop_count = 1; the real
        // recursion bound lives in the recursive term's `vp.hop_count < N`).
        // Emit that literal comparison directly: standard-SQL-portable and
        // provably identical to what ClickHouse evaluated before. (Only this
        // strategy's base case had the alias reference; the zero-hop base
        // case of #489 builds its own SQL without this WHERE clause.)
        if let Some(max_hops) = context.spec.max_hops {
            conditions.push(format!("1 <= {}", max_hops));
        }

        Ok(conditions.join(" AND "))
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
            let (rel_from_col, rel_to_col) = (self.rel_from_col.clone(), self.rel_to_col.clone());
            let strategy = DenormalizedCteStrategy {
                pattern_ctx: self.pattern_ctx.clone(),
                table: self.rel_table.clone(),
                from_col: rel_from_col.clone(),
                to_col: rel_to_col.clone(),
                identity: EdgeIdentity::EdgeIdColumns {
                    edge_id: DenormalizedCteStrategy::resolve_edge_id(&self.pattern_ctx, schema),
                    from_col: rel_from_col,
                    to_col: rel_to_col,
                },
                schema: Arc::new(schema.clone()),
            };

            log::debug!(
                "🔧 Using NEW DenormalizedCteStrategy for {}-[*]->{}",
                self.pattern_ctx.left_node_alias,
                self.pattern_ctx.right_node_alias
            );

            return strategy.generate_sql(context, properties, filters);
        }

        // Build the generator for everything except the fully-denormalized
        // pattern (which returned above via `DenormalizedCteStrategy`).
        // Three remaining cases:
        //   * mixed access (one endpoint denormalized, the other not)
        //     → `new_mixed`
        //   * traditional / FK-edge / polymorphic edge schemas
        //     → `new_with_fk_edge` (the polymorphic-capable constructor —
        //       `polymorphic_info` is extracted in `Self::new` and threaded
        //       in via the generator's polymorphic fields below)
        let mut generator = if self.start_is_denormalized != self.end_is_denormalized {
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

        // Set weight CTE if configured for weighted shortest path
        if let Some(ref weight_config) = context.weight_cte {
            generator.set_weight_cte(weight_config.clone());
        }

        // Skip path_relationships growth when relationships(path) isn't used
        generator.needs_path_relationships = context.needs_path_relationships;
        // Lightweight BFS mode for shortestPath + length(path)-only queries
        generator.use_bfs_mode = context.use_bfs_mode;
        generator.is_undirected = context.is_undirected;
        generator.undirected_single_walk = context.undirected_single_walk;

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
    use std::collections::HashMap;

    /// Build the denormalized pattern context used by the cycle-check tests.
    fn denormalized_flights_pattern_ctx() -> PatternSchemaContext {
        PatternSchemaContext {
            left_node_alias: "f1".to_string(),
            right_node_alias: "f2".to_string(),
            rel_alias: "flights".to_string(),
            left_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "flights".to_string(),
                properties: HashMap::new(),
                is_from_node: true,
            },
            right_node: NodeAccessStrategy::EmbeddedInEdge {
                edge_alias: "flights".to_string(),
                properties: HashMap::new(),
                is_from_node: false,
            },
            edge: EdgeAccessStrategy::SeparateTable {
                table: "flights".to_string(),
                from_id: "Origin".to_string(),
                to_id: "Dest".to_string(),
                properties: HashMap::new(),
            },
            join_strategy: JoinStrategy::SingleTableScan {
                table: "flights".to_string(),
            },
            coupled_context: None,
            rel_types: vec!["FLIES_TO".to_string()],
            left_is_polymorphic: false,
            right_is_polymorphic: false,
            constraints: None,
        }
    }

    fn denormalized_cycle_check_sql() -> (DenormalizedCteStrategy, CteGenerationContext) {
        let pattern_ctx = denormalized_flights_pattern_ctx();
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        ));
        let strategy = DenormalizedCteStrategy::new(&pattern_ctx, schema).unwrap();
        let context = CteGenerationContext::new().with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        });
        (strategy, context)
    }

    fn empty_filters() -> CategorizedFilters {
        CategorizedFilters {
            start_node_filters: None,
            end_node_filters: None,
            relationship_filters: None,
            path_function_filters: None,
            start_sql: None,
            end_sql: None,
            relationship_sql: None,
        }
    }

    /// Regression: the recursive VLP cycle check must be dialect-aware. Under
    /// ClickHouse (default) it uses `has`; under Databricks it must use Spark's
    /// `array_contains` (Spark has no `has`). Covers the DenormalizedCteStrategy
    /// recursive builder — the path the server/cg VLP queries route through.
    ///
    /// This range VLP (min_hops=1, not shortestPath) enforces #606
    /// relationship-uniqueness, so the membership test is over `path_edges`
    /// (the `(from,to)` edge tuple), not `path_nodes`.
    #[test]
    fn test_denormalized_cte_cycle_check_clickhouse_default() {
        let (strategy, context) = denormalized_cycle_check_sql();
        let sql = strategy
            .generate_sql(&context, &[], &empty_filters())
            .unwrap()
            .sql;
        assert!(
            sql.contains("NOT has(vp.path_edges"),
            "CH cycle check should use `has` on path_edges (#606); got:\n{sql}"
        );
        assert!(
            !sql.contains("array_contains"),
            "CH SQL leaked Spark `array_contains`; got:\n{sql}"
        );
    }

    #[tokio::test]
    async fn test_denormalized_cte_cycle_check_databricks_dialect() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let (strategy, context) = denormalized_cycle_check_sql();
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async {
            strategy
                .generate_sql(&context, &[], &empty_filters())
                .unwrap()
                .sql
        })
        .await;
        assert!(
            sql.contains("array_contains(vp.path_edges"),
            "Databricks cycle check should use `array_contains` on path_edges \
             (#606); got:\n{sql}"
        );
        assert!(
            !sql.contains("has(vp.path_edges"),
            "Databricks SQL leaked ClickHouse `has(...)` cycle check; got:\n{sql}"
        );
    }

    /// Build a denormalized strategy with an explicit `edge_id`, bypassing the
    /// schema round-trip so `edge_tuple`'s three arms can be exercised directly.
    fn denorm_strategy_with_edge_id(edge_id: Option<Identifier>) -> DenormalizedCteStrategy {
        let pattern_ctx = denormalized_flights_pattern_ctx();
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        ));
        DenormalizedCteStrategy {
            pattern_ctx,
            table: "flights".to_string(),
            from_col: "Origin".to_string(),
            to_col: "Dest".to_string(),
            identity: EdgeIdentity::EdgeIdColumns {
                edge_id,
                from_col: "Origin".to_string(),
                to_col: "Dest".to_string(),
            },
            schema,
        }
    }

    /// #710: a denormalized VLP's edge identity must consult the schema
    /// `edge_id` so parallel edges sharing one `(from, to)` node pair are not
    /// collapsed (silent under-count). `edge_tuple` mirrors the standard
    /// emitter's `build_edge_tuple_recursive`:
    ///   - `Composite` → `tuple(alias.c1, alias.c2, …)`,
    ///   - `Single`    → bare scalar `alias.col`,
    ///   - `None`      → the historical `(from, to)` pair (byte-identical).
    #[test]
    fn denorm_edge_tuple_uses_schema_edge_id_710() {
        // Composite edge_id — the parallel-edge case (e.g. flight_id + number).
        let composite = denorm_strategy_with_edge_id(Some(Identifier::Composite(vec![
            "flight_id".to_string(),
            "flight_number".to_string(),
        ])));
        assert_eq!(
            composite.edge_tuple("next"),
            "tuple(next.flight_id, next.flight_number)",
            "composite edge_id must spell the full ordered tuple (#710)"
        );

        // Single-column edge_id — bare scalar element (matches the emitter's
        // Single arm; `arr(...)` wraps it into a scalar `path_edges` array).
        // Live-reachable via single-column denorm `edge_id` schemas such as
        // `schemas/test/denorm_selfloop_multitype.yaml` (`edge_id: evt_id`),
        // though no golden-corpus VLP currently exercises it.
        let single = denorm_strategy_with_edge_id(Some(Identifier::Single("evt_id".to_string())));
        assert_eq!(
            single.edge_tuple("next"),
            "next.evt_id",
            "single-column edge_id must render as a bare scalar (#710)"
        );

        // No edge_id — byte-identical to the pre-#710 `(from, to)` behavior.
        let none = denorm_strategy_with_edge_id(None);
        assert_eq!(
            none.edge_tuple("next"),
            "tuple(next.Origin, next.Dest)",
            "absent edge_id must preserve the historical node-pair identity"
        );
        // ...and on the base-case alias too.
        assert_eq!(none.edge_tuple("f"), "tuple(f.Origin, f.Dest)");
    }

    /// #710: end-to-end — a composite `edge_id` must appear on ALL THREE
    /// edge-identity sites of the generated recursive CTE (base seed, recursive
    /// append, cycle check), and the collapsing `(Origin, Dest)` node pair must
    /// be gone from `path_edges`.
    #[test]
    fn denorm_vlp_composite_edge_id_in_generated_sql_710() {
        let composite =
            Identifier::Composite(vec!["flight_id".to_string(), "flight_number".to_string()]);
        let strategy = denorm_strategy_with_edge_id(Some(composite));
        let context = CteGenerationContext::new().with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        });
        let sql = strategy
            .generate_sql(&context, &[], &empty_filters())
            .unwrap()
            .sql;
        // Base seed.
        assert!(
            sql.contains("[tuple(flights.flight_id, flights.flight_number)] as path_edges"),
            "base seed must use the composite edge_id; got:\n{sql}"
        );
        // Recursive append.
        assert!(
            sql.contains("arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)])"),
            "recursive append must use the composite edge_id; got:\n{sql}"
        );
        // Cycle check.
        assert!(
            sql.contains("NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))"),
            "cycle check must use the composite edge_id; got:\n{sql}"
        );
        // The collapsing node-pair identity must no longer key path_edges.
        assert!(
            !sql.contains("tuple(next.Origin, next.Dest)"),
            "node-pair (Origin, Dest) must not key path_edges once edge_id exists; got:\n{sql}"
        );
    }

    /// #887 Phase 2b: the denorm mirror of the standard #628 fix — a CLOSED
    /// `*0..N` VLP on a denormalized schema must enforce EDGE-uniqueness (a
    /// path MAY revisit a node but must not reuse an edge), so real cycles
    /// survive the outer `start_id = end_id` closed constraint. This is what
    /// the lifted #605/#625/#980 (render-side) and #978 (analyzer-side)
    /// fail-louds were protecting: under node-uniqueness a walk can never
    /// return to its start, so every cycle is dropped and the count silently
    /// collapses to the zero-length self rows. The zero-hop base has no edge,
    /// so it seeds an EMPTY-but-typed `path_edges` array via
    /// [`typed_empty_edges_seed`] — a bare `[]` (ClickHouse `Array(Nothing)`)
    /// FAILS the recursive-CTE column unification against the recursive arm's
    /// concrete `Array(Tuple(...))` (CANNOT_CONVERT_TYPE, proven live on
    /// #887), so the seed is instead a typed-empty slice of a real
    /// edge-identity tuple pulled from the edge table; the recursive arm's
    /// `NOT has(path_edges, …)` then dedupes edges from hop 1 onward.
    /// An OPEN `*0..N` stays node-unique and byte-unchanged.
    #[test]
    fn denorm_closed_zero_hop_vlp_uses_edge_uniqueness_628_mirror() {
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        ));

        // CLOSED: same alias on both endpoints → edge-uniqueness.
        let mut closed_ctx = denormalized_flights_pattern_ctx();
        closed_ctx.left_node_alias = "a".to_string();
        closed_ctx.right_node_alias = "a".to_string();
        let strategy = DenormalizedCteStrategy::new(&closed_ctx, schema.clone()).unwrap();
        let context = CteGenerationContext::new().with_spec(VariableLengthSpec {
            min_hops: Some(0),
            max_hops: Some(3),
        });
        assert!(strategy.is_closed_pattern());
        assert!(
            strategy.uses_edge_uniqueness(&context),
            "closed *0..N must use edge-uniqueness (#628 mirror)"
        );
        let closed_sql = strategy
            .generate_sql(&context, &[], &empty_filters())
            .unwrap()
            .sql;
        // Zero-hop base seeds an empty-but-typed path_edges array: a
        // typed-empty slice of a real edge-identity tuple pulled from the
        // edge table via a scalar subquery (a bare `[]` = Array(Nothing)
        // fails ClickHouse recursive-CTE unification — #887 live proof).
        assert!(
            closed_sql.contains("as path_edges")
                && closed_sql.contains("__seed_edge")
                && closed_sql.contains("LIMIT 1"),
            "closed *0..N zero-hop base must seed typed-empty path_edges; got:\n{closed_sql}"
        );
        assert!(
            !closed_sql.contains("[] as path_edges"),
            "closed *0..N zero-hop base must not seed bare `[]`; got:\n{closed_sql}"
        );
        // ...and the recursive arm enforces EDGE-uniqueness (not node).
        assert!(
            closed_sql.contains("NOT has(vp.path_edges,"),
            "closed *0..N recursive arm must dedupe on path_edges; got:\n{closed_sql}"
        );
        assert!(
            !closed_sql.contains("NOT has(vp.path_nodes,"),
            "closed *0..N must NOT use node-uniqueness; got:\n{closed_sql}"
        );

        // OPEN: distinct aliases → stays node-unique, typed empty-string seed.
        let open_strategy =
            DenormalizedCteStrategy::new(&denormalized_flights_pattern_ctx(), schema).unwrap();
        assert!(
            !open_strategy.uses_edge_uniqueness(&context),
            "open *0..N must stay node-unique"
        );
        let open_sql = open_strategy
            .generate_sql(&context, &[], &empty_filters())
            .unwrap()
            .sql;
        assert!(
            !open_sql.contains("[] as path_edges"),
            "open *0..N must not seed `[] as path_edges`; got:\n{open_sql}"
        );
        assert!(
            open_sql.contains("NOT has(vp.path_nodes,"),
            "open *0..N must use node-uniqueness; got:\n{open_sql}"
        );
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
        let context = CteGenerationContext::new().with_spec(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(3),
        });

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
        // WITH RECURSIVE is added by Ctes::to_sql() at the top level, not in generator output
        assert!(generation_result.sql.contains(" AS (\n")); // CTE structure with newline
        assert!(generation_result.sql.contains("flights"));
        assert!(generation_result.sql.contains("Origin"));
        assert!(generation_result.sql.contains("Dest"));
    }
}
