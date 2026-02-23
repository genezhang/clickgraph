//! Query planning context.
//!
//! [`PlanCtx`] maintains state during logical plan construction, tracking:
//! - Variable-to-table mappings
//! - OPTIONAL MATCH handling
//! - Projection aliases
//! - Schema configuration
//! - Multi-tenant parameters
//!
//! # Key Components
//!
//! - [`PlanCtx`] - Main planning context with scope support
//! - [`TableCtx`] - Table/alias metadata for SQL generation
//! - [`VariableRegistry`] - TypedVariable tracking for nodes, relationships, paths
//!
//! # Scope Chain
//!
//! WITH clauses create nested scopes:
//! ```text
//! MATCH (a) WITH a MATCH (b) WITH a, b MATCH (c)
//! └─ scope3 ─────────────────────────────────────┘
//!    └─ scope2 ───────────────────────┘
//!       └─ scope1 ────────┘
//! ```
//!
//! Variable lookup traverses the scope chain from current to root.

pub mod builder;
pub mod errors;
mod table_ctx;

// Re-export TableCtx for backward compatibility
pub use table_ctx::TableCtx;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::{
    graph_catalog::{graph_schema::GraphSchema, pattern_schema::PatternSchemaContext},
    query_planner::{
        analyzer::property_requirements::PropertyRequirements,
        join_context::VlpEndpointInfo,
        logical_expr::LogicalExpr,
        logical_plan::ProjectionItem,
        plan_ctx::errors::PlanCtxError,
        typed_variable::{CollectionElementType, TypedVariable, VariableRegistry, VariableSource},
    },
};

#[derive(Debug, Clone)]
pub struct PlanCtx {
    alias_table_ctx_map: HashMap<String, TableCtx>,
    /// Track which table aliases came from OPTIONAL MATCH for LEFT JOIN generation
    optional_aliases: HashSet<String>,
    /// Track projection aliases from WITH/aggregation clauses (alias_name -> original_expression)
    /// Used to identify when filters reference projection results (HAVING clause)
    projection_aliases: HashMap<String, LogicalExpr>,
    /// Flag to indicate we're currently processing an OPTIONAL MATCH clause
    /// All new aliases created during this mode should be marked as optional
    in_optional_match_mode: bool,
    /// Graph schema for this query (enables multi-schema support)
    schema: Arc<GraphSchema>,
    /// Tenant ID for multi-tenant deployments (passed to parameterized views)
    tenant_id: Option<String>,
    /// View parameter values for parameterized views (e.g., {"region": "US", "tier": "premium"})
    /// These are passed to table functions: table(region = 'US', tier = 'premium')
    view_parameter_values: Option<HashMap<String, String>>,
    /// Track denormalized node-to-edge mappings: node_alias -> (edge_alias, is_from_node, node_label, rel_type)
    /// Used for multi-hop denormalized patterns to create edge-to-edge JOINs
    denormalized_node_edges: HashMap<String, (String, bool, String, String)>,
    /// Parent scope for WITH clause nesting (enables proper variable scoping)
    /// Lookup chain: current scope → parent scope → ... → root scope (global schema)
    /// Example: MATCH (a) WITH a MATCH (b) → second MATCH has parent scope containing 'a'
    parent_scope: Option<Box<PlanCtx>>,
    /// Flag indicating this scope was created by WITH clause (acts as scope barrier)
    /// When true, variable lookup stops here and doesn't search parent scope
    /// Example: MATCH (a)-[]->(b) WITH a MATCH (a)-[]->(b)  // second b is different!
    is_with_scope: bool,
    /// Counter for generating unique CTE names (ensures with_a_b_cte_0, with_a_b_cte_1, etc.)
    /// Incremented each time a WITH clause is processed to prevent duplicate CTE names
    pub(crate) cte_counter: usize,
    /// Track exported columns for each CTE
    /// Map: CTE name → (graph_property → cte_column_name)
    /// Example: "with_p_cte_1" → {"firstName" → "p_firstName", "age" → "p_age"}
    /// Note: CTE column names use underscore (variablename_alias),
    /// while final SELECT uses dot notation (variablename.alias)
    cte_columns: HashMap<String, HashMap<String, String>>,
    /// Track entity types (node/relationship labels) for each CTE alias
    /// Map: CTE name → (alias → (is_rel, labels))
    /// Example: "with_tag_cte_1" → {"tag" → (false, ["Tag"])}
    /// This preserves node/relationship type information across WITH boundaries,
    /// enabling property resolution after WITH (e.g., `WITH tag ... RETURN tag.name`)
    cte_entity_types: HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>>,
    /// Property requirements tracking for optimization
    /// Populated by PropertyRequirementsAnalyzer pass (root-to-leaf traversal)
    /// Consumed by property expansion in renderer to prune unnecessary columns
    /// Example: If RETURN only uses friend.firstName, don't collect friend.* (200 columns)
    property_requirements: Option<PropertyRequirements>,
    /// Maximum number of inferred edge types for generic patterns like [*1] (default: 5)
    /// Can be overridden per-query via QueryRequest.max_inferred_types
    /// Reasonable values for GraphRAG: 10-20 edge types
    pub(crate) max_inferred_types: usize,
    /// Pattern schema contexts for each relationship in the query
    /// Map: relationship alias (e.g., "r", "follows") → PatternSchemaContext
    /// Populated during graph pattern analysis, consumed by property resolution
    /// Enables property resolver to determine node access strategies (OwnTable vs EmbeddedInEdge)
    /// and make role (from/to) explicit via NodeAccessStrategy::is_from_node field
    pattern_contexts: HashMap<String, Arc<PatternSchemaContext>>,
    /// **NEW (Jan 2026)**: VLP endpoint tracking for correct JOIN generation
    /// Map: Cypher alias (e.g., "u2") → VlpEndpointInfo
    /// When a node is a VLP endpoint, subsequent JOINs must reference the CTE
    /// (e.g., `t.end_id` instead of `u2.user_id`)
    /// Populated by graph_join_inference.rs when VLP patterns are detected
    vlp_endpoints: HashMap<String, VlpEndpointInfo>,
    /// Counter for generating unique per-VLP CTE outer aliases (vt0, vt1, vt2, ...).
    /// Scoped to this PlanCtx (per-query), avoiding global static race conditions.
    vlp_alias_counter: usize,
    /// **NEW (Jan 2026)**: Typed variable registry for unified variable tracking
    /// This is the single source of truth for variable types across the query.
    /// Replaces fragmented type tracking in TableCtx and ScopeContext.
    /// See docs/development/variable-type-system-design.md for architecture.
    variables: VariableRegistry,
    /// **NEW (Jan 2026)**: Track CTE alias source mappings for renamed variables
    /// Map: new_alias → (original_alias, cte_name)
    /// Example: "person" → ("u", "with_person_cte_1") for `WITH u AS person`
    /// Used to resolve property access on renamed CTE aliases:
    /// When accessing person.name, map to u.name, then to CTE column u_name
    cte_alias_sources: HashMap<String, (String, String)>,
    /// WHERE clause property requirements (for pattern combination pruning)
    /// Extracted from WHERE clause before pattern traversal for property-based optimization
    /// Map: alias (e.g., "n", "r") → set of required property names (e.g., {"bytes_sent", "timestamp"})
    /// Enables pruning of UNION branches that don't have required properties
    /// Example: `WHERE n.bytes_sent IS NOT NULL` → {"n": {"bytes_sent"}}
    where_property_requirements: HashMap<String, HashSet<String>>,

    /// Extracted from WHERE clause `id(var) IN [...]` patterns for UNION pruning optimization
    /// Map: alias (e.g., "a", "b") → set of node labels extracted from encoded IDs
    /// Enables pruning of UNION branches to only include relevant relationship types
    /// Example: `WHERE id(a) IN [<user-ids>]` → {"a": {"User"}}
    where_label_constraints: HashMap<String, HashSet<String>>,

    /// Status messages collected during planning (PatternResolver diagnostics)
    /// Vector of (level, message) tuples for warnings, info, and errors during analysis
    /// Used by PatternResolver to report combination limits, missing types, etc.
    /// Example: (Warning, "Type combinations limited to 38")
    status_messages: Vec<(StatusLevel, String)>,

    /// Node type combinations for simple untyped node queries
    /// Map: node_alias → Vec<label>
    /// Example: {"n": ["User", "Post", "ZeekLog"]}
    /// Used when a simple node query like `MATCH (n) RETURN n` can match multiple node types
    /// CTE generation will create UNION of all node tables
    node_combinations: HashMap<String, Vec<String>>,

    /// Pattern type combinations for untyped relationship patterns
    /// Map: (from_alias, to_alias) → Vec<TypeCombination>
    /// Example: {("a", "b"): [(User, FOLLOWS, User), (User, AUTHORED, Post), ...]}
    /// Used when pattern inference finds ambiguous nodes that could match multiple types
    /// CTE generation will create UNION of all valid pattern combinations
    pattern_combinations: HashMap<(String, String), Vec<TypeCombination>>,

    /// Connected pattern group combinations (optimization - WIP)
    /// Map: group_id → Vec<GroupCombination>
    /// Example: {"a_b_c": [GroupCombination{...}, ...]}
    /// Used when multiple patterns share variables (e.g., `(a)-[r1]->(b)-[r2]->(c)` shares `b`)
    /// Each GroupCombination assigns types to all patterns in the group consistently
    /// Status: Under development - not yet used in CTE generation
    group_combinations: HashMap<String, Vec<GroupCombination>>,
}

/// Status message severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusLevel {
    /// Informational message (e.g., "No valid types found")
    Info,
    /// Warning message (e.g., "Combination limit reached")
    Warning,
    /// Error message (should not prevent query execution, just notify)
    Error,
}

/// Type combination for multi-type pattern resolution
/// Represents one valid (from_label, rel_type, to_label) tuple from schema
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypeCombination {
    /// From node label (e.g., "User")
    pub from_label: String,
    /// Relationship type (e.g., "FOLLOWS")
    pub rel_type: String,
    /// To node label (e.g., "User")
    pub to_label: String,
}

/// Group combination for connected patterns (optimization - WIP)
/// Represents one valid type assignment across multiple connected patterns.
/// Used when patterns share variables (e.g., `(a)-[r1]->(b)-[r2]->(c)` shares `b`).
/// Status: Data structure complete, optimization logic under development
#[derive(Debug, Clone)]
pub struct GroupCombination {
    /// Type assignment for each pattern in the connected group
    /// Key: (left_alias, right_alias) identifies the pattern (e.g., ("a", "b"))
    /// Value: Type combination for that pattern
    /// Example: {("a","b"): (User, FOLLOWS, User), ("b","c"): (User, WORKS_AT, Company)}
    pub pattern_types: HashMap<(String, String), TypeCombination>,
}

impl PlanCtx {
    pub fn insert_table_ctx(&mut self, alias: String, table_ctx: TableCtx) {
        crate::debug_print!(
            "DEBUG PlanCtx::insert_table_ctx: alias='{}', in_optional_match_mode={}",
            alias,
            self.in_optional_match_mode
        );

        // NEW (Jan 2026): Also register in typed variable system
        // This keeps both systems in sync during migration period
        // SKIP if variable is already defined (e.g., path variables may have been
        // registered with full metadata via define_path() before this call)
        if !self.variables.contains(&alias) {
            let labels = table_ctx.get_labels().cloned().unwrap_or_default();
            let is_rel = table_ctx.is_relation();
            let is_path = table_ctx.is_path_variable();

            log::debug!(
                "🔍 insert_table_ctx: alias='{}', is_rel={}, is_path={}, labels={:?}",
                alias,
                is_rel,
                is_path,
                table_ctx.get_labels()
            );

            if is_rel {
                // It's a relationship variable
                self.variables.define_relationship(
                    alias.clone(),
                    labels,
                    table_ctx.get_from_node_label().cloned(),
                    table_ctx.get_to_node_label().cloned(),
                    VariableSource::Match,
                );
            } else if is_path {
                // It's a path variable (no labels, not a relationship)
                // Note: We don't have full path info here (start/end nodes, bounds),
                // so we register a basic path. The full info would need to be passed
                // from the caller or set via define_path() directly.
                log::info!(
                    "⚠️  Registering '{}' as Path variable via heuristic (no labels)",
                    alias
                );
                self.variables.define_path(
                    alias.clone(),
                    None,  // start_node - not available from TableCtx
                    None,  // end_node - not available from TableCtx
                    None,  // relationship - not available from TableCtx
                    None,  // length_bounds - not available from TableCtx
                    false, // is_shortest_path - would need explicit flag
                );
            } else {
                // It's a node variable
                log::debug!(
                    "✓ Registering '{}' as Node variable with labels={:?}",
                    alias,
                    labels
                );
                self.variables
                    .define_node(alias.clone(), labels, VariableSource::Match);
            }
        } else if let Some(typed_var) = self.variables.lookup(&alias) {
            let variant_name = if typed_var.is_node() {
                "Node"
            } else if typed_var.is_relationship() {
                "Relationship"
            } else if typed_var.as_path().is_some() {
                "Path"
            } else {
                "Unknown"
            };
            log::info!(
                "⚠️  Skipping insert_table_ctx for '{}' - already registered as {} variant",
                alias,
                variant_name
            );
        }

        self.alias_table_ctx_map.insert(alias.clone(), table_ctx);

        // Auto-mark as optional if we're in OPTIONAL MATCH mode
        if self.in_optional_match_mode {
            crate::debug_println!("DEBUG PlanCtx: Auto-marking '{}' as optional", alias);
            self.optional_aliases.insert(alias);
        }
    }

    /// Mark a table alias as coming from an OPTIONAL MATCH clause
    pub fn mark_as_optional(&mut self, alias: String) {
        self.optional_aliases.insert(alias);
    }

    /// Set the OPTIONAL MATCH processing mode
    /// When true, all new aliases will be automatically marked as optional
    pub fn set_optional_match_mode(&mut self, enabled: bool) {
        self.in_optional_match_mode = enabled;
    }

    /// Check if we're currently processing an OPTIONAL MATCH clause
    pub fn is_optional_match_mode(&self) -> bool {
        self.in_optional_match_mode
    }

    /// Register a projection alias (e.g., `follows` from `COUNT(b) as follows`)
    pub fn register_projection_alias(&mut self, alias: String, expression: LogicalExpr) {
        self.projection_aliases.insert(alias, expression);
    }

    /// Check if an alias is a projection alias
    pub fn is_projection_alias(&self, alias: &str) -> bool {
        self.projection_aliases.contains_key(alias)
    }

    /// Get the original expression for a projection alias
    pub fn get_projection_alias_expr(&self, alias: &str) -> Option<&LogicalExpr> {
        self.projection_aliases.get(alias)
    }

    /// Check if a table alias came from an OPTIONAL MATCH clause
    pub fn is_optional(&self, alias: &str) -> bool {
        self.optional_aliases.contains(alias)
    }

    /// Get a reference to the set of optional aliases
    pub fn get_optional_aliases(&self) -> &HashSet<String> {
        &self.optional_aliases
    }

    pub fn get_alias_table_ctx_map(&self) -> &HashMap<String, TableCtx> {
        &self.alias_table_ctx_map
    }

    pub fn get_mut_alias_table_ctx_map(&mut self) -> &mut HashMap<String, TableCtx> {
        &mut self.alias_table_ctx_map
    }

    /// Iterate over all table contexts (alias, TableCtx pairs)
    pub fn iter_table_contexts(&self) -> impl Iterator<Item = (&String, &TableCtx)> {
        self.alias_table_ctx_map.iter()
    }

    /// Get the graph schema for this query
    pub fn schema(&self) -> &GraphSchema {
        &self.schema
    }

    pub fn get_table_ctx(&self, alias: &str) -> Result<&TableCtx, PlanCtxError> {
        // Try current scope first
        if let Some(ctx) = self.alias_table_ctx_map.get(alias) {
            return Ok(ctx);
        }

        // WITH scope acts as a barrier - don't look beyond it
        // This implements WITH's shielding semantics: only exported variables are visible
        if self.is_with_scope {
            return Err(PlanCtxError::TableCtx {
                alias: alias.to_string(),
            });
        }

        // Search parent scope recursively (scope chain)
        if let Some(parent) = &self.parent_scope {
            return parent.get_table_ctx(alias);
        }

        // Not found in any scope
        Err(PlanCtxError::TableCtx {
            alias: alias.to_string(),
        })
    }

    pub fn get_table_ctx_from_alias_opt(
        &self,
        alias: &Option<String>,
    ) -> Result<&TableCtx, PlanCtxError> {
        let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
            alias: "".to_string(),
        })?;
        self.alias_table_ctx_map
            .get(&alias)
            .ok_or(PlanCtxError::TableCtx {
                alias: alias.clone(),
            })
    }

    pub fn get_node_table_ctx(&self, node_alias: &str) -> Result<&TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get(node_alias)
            .ok_or(PlanCtxError::NodeTableCtx {
                alias: node_alias.to_string(),
            })
    }

    pub fn get_rel_table_ctx(&self, rel_alias: &str) -> Result<&TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get(rel_alias)
            .ok_or(PlanCtxError::RelTableCtx {
                alias: rel_alias.to_string(),
            })
    }

    /// Get mutable reference to table context in CURRENT SCOPE ONLY.
    ///
    /// NOTE: This does NOT search parent scopes. Mutable access is restricted to
    /// the current scope to maintain proper scope isolation. If you need to mutate
    /// a variable from a parent scope (e.g., from WITH), it should already be in
    /// the current scope (copied during WITH processing).
    pub fn get_mut_table_ctx(&mut self, alias: &str) -> Result<&mut TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get_mut(alias)
            .ok_or(PlanCtxError::TableCtx {
                alias: alias.to_string(),
            })
    }

    // pub fn get_mut_table_ctx_from_alias_opt(
    //     &mut self,
    //     alias: &Option<String>,
    // ) -> Result<&mut TableCtx, PlanCtxError> {
    //     let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
    //         alias: "".to_string(),
    //     })?;
    //     self.alias_table_ctx_map
    //         .get_mut(&alias)
    //         .ok_or(PlanCtxError::TableCtx {
    //             alias: alias.clone(),
    //         })
    // }

    /// Get optional mutable reference to table context in CURRENT SCOPE ONLY.
    ///
    /// NOTE: This does NOT search parent scopes. See get_mut_table_ctx() for rationale.
    pub fn get_mut_table_ctx_opt(&mut self, alias: &str) -> Option<&mut TableCtx> {
        self.alias_table_ctx_map.get_mut(alias)
    }

    pub fn get_mut_table_ctx_opt_from_alias_opt(
        &mut self,
        alias: &Option<String>,
    ) -> Result<Option<&mut TableCtx>, PlanCtxError> {
        let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
            alias: "".to_string(),
        })?;
        Ok(self.alias_table_ctx_map.get_mut(&alias))
    }

    /// Register a denormalized node alias with its associated edge
    /// Used for multi-hop denormalized patterns to create edge-to-edge JOINs
    pub fn register_denormalized_alias(
        &mut self,
        alias: String,
        rel_alias: String,
        is_from_node: bool,
        node_label: String,
        rel_type: String,
    ) {
        self.denormalized_node_edges
            .insert(alias, (rel_alias, is_from_node, node_label, rel_type));
    }

    /// Get denormalized alias info: returns (edge_alias, is_from_node, node_label, rel_type) if node is denormalized
    pub fn get_denormalized_alias_info(
        &self,
        node_alias: &str,
    ) -> Option<(String, bool, String, String)> {
        self.denormalized_node_edges.get(node_alias).cloned()
    }

    /// Get all denormalized aliases for transfer to rendering phase
    /// Returns an iterator over (node_alias, (edge_alias, is_from_node, node_label, rel_type))
    /// Used by render_plan to transfer PlanCtx state to task-local storage
    pub fn iter_denormalized_aliases(
        &self,
    ) -> impl Iterator<Item = (&String, &(String, bool, String, String))> {
        self.denormalized_node_edges.iter()
    }

    /// Get an iterator over all aliases and their TableCtx in the CURRENT scope only.
    /// Used for copying child scope state back to parent scope.
    pub fn iter_aliases(&self) -> impl Iterator<Item = (&String, &TableCtx)> {
        self.alias_table_ctx_map.iter()
    }
}

impl PlanCtx {
    /// Create a new PlanCtx with the given schema
    pub fn new(schema: Arc<GraphSchema>) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id: None,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            max_inferred_types: 20, // Increased for Neo4j Browser node expansion
            pattern_contexts: HashMap::new(),
            vlp_endpoints: HashMap::new(),
            vlp_alias_counter: 0,
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
            where_property_requirements: HashMap::new(),
            where_label_constraints: HashMap::new(),
            status_messages: Vec::new(),
            node_combinations: HashMap::new(),
            group_combinations: HashMap::new(),
            pattern_combinations: HashMap::new(),
        }
    }

    /// Create a new PlanCtx with the given schema and tenant ID
    pub fn with_tenant(schema: Arc<GraphSchema>, tenant_id: Option<String>) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            max_inferred_types: 20, // Increased for Neo4j Browser node expansion
            pattern_contexts: HashMap::new(),
            vlp_endpoints: HashMap::new(),
            vlp_alias_counter: 0,
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
            where_property_requirements: HashMap::new(),
            where_label_constraints: HashMap::new(),
            status_messages: Vec::new(),
            node_combinations: HashMap::new(),
            group_combinations: HashMap::new(),
            pattern_combinations: HashMap::new(),
        }
    }

    /// Create a new PlanCtx with schema, tenant_id, and view_parameters
    pub fn with_parameters(
        schema: Arc<GraphSchema>,
        tenant_id: Option<String>,
        view_parameter_values: Option<HashMap<String, String>>,
    ) -> Self {
        Self::with_all_parameters(schema, tenant_id, view_parameter_values, 4)
    }

    /// Create a new PlanCtx with all parameters including max_inferred_types
    pub fn with_all_parameters(
        schema: Arc<GraphSchema>,
        tenant_id: Option<String>,
        view_parameter_values: Option<HashMap<String, String>>,
        max_inferred_types: usize,
    ) -> Self {
        // Merge tenant_id into view_parameter_values so ViewScan can use it
        let merged_view_params = match (&tenant_id, &view_parameter_values) {
            (Some(tid), Some(params)) => {
                let mut merged = params.clone();
                merged.entry("tenant_id".to_string()).or_insert(tid.clone());
                Some(merged)
            }
            (Some(tid), None) => {
                let mut params = HashMap::new();
                params.insert("tenant_id".to_string(), tid.clone());
                Some(params)
            }
            (None, Some(params)) => Some(params.clone()),
            (None, None) => None,
        };

        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id,
            view_parameter_values: merged_view_params,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            max_inferred_types,
            pattern_contexts: HashMap::new(),
            vlp_endpoints: HashMap::new(),
            vlp_alias_counter: 0,
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
            where_property_requirements: HashMap::new(),
            where_label_constraints: HashMap::new(),
            status_messages: Vec::new(),
            node_combinations: HashMap::new(),
            group_combinations: HashMap::new(),
            pattern_combinations: HashMap::new(),
        }
    }

    /// Create a child scope with parent context (for WITH clause scoping)
    /// The child scope inherits schema, tenant_id, and view_parameters from parent
    /// but has its own alias_table_ctx_map for local variables
    ///
    /// **CRITICAL**: Set `is_with_scope=true` when creating scope for WITH clause!
    /// This makes the scope act as a barrier preventing lookup of parent variables.
    ///
    /// Example: MATCH (a)-[]->(b) WITH a MATCH (a)-[]->(b)
    ///   - Scope1 (before WITH): {a: User, b: User}
    ///   - Scope2 (WITH, is_with_scope=true): {a: User} - shields b from Scope1!
    ///   - Second MATCH creates NEW b in Scope2, different from Scope1's b
    pub fn with_parent_scope(parent: &PlanCtx, is_with_scope: bool) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema: parent.schema.clone(),
            tenant_id: parent.tenant_id.clone(),
            view_parameter_values: parent.view_parameter_values.clone(),
            denormalized_node_edges: HashMap::new(),
            parent_scope: Some(Box::new(parent.clone())),
            is_with_scope,
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            max_inferred_types: parent.max_inferred_types,
            pattern_contexts: HashMap::new(), // New scope - patterns computed fresh
            vlp_endpoints: parent.vlp_endpoints.clone(), // Inherit VLP endpoint info from parent
            vlp_alias_counter: parent.vlp_alias_counter, // Continue counter from parent scope
            variables: VariableRegistry::new(), // Fresh variable registry for new scope
            cte_alias_sources: HashMap::new(),
            where_property_requirements: HashMap::new(),
            where_label_constraints: HashMap::new(),
            status_messages: Vec::new(),
            node_combinations: HashMap::new(),
            group_combinations: HashMap::new(),
            pattern_combinations: HashMap::new(),
        }
    }

    /// Create an empty PlanCtx with an empty schema (for tests only)
    pub fn new_empty() -> Self {
        use crate::graph_catalog::graph_schema::GraphSchema;
        let empty_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema: Arc::new(empty_schema),
            tenant_id: None,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
            max_inferred_types: 4,
            pattern_contexts: HashMap::new(),
            vlp_endpoints: HashMap::new(),
            vlp_alias_counter: 0,
            variables: VariableRegistry::new(),
            cte_alias_sources: HashMap::new(),
            where_property_requirements: HashMap::new(),
            where_label_constraints: HashMap::new(),
            status_messages: Vec::new(),
            node_combinations: HashMap::new(),
            group_combinations: HashMap::new(),
            pattern_combinations: HashMap::new(),
        }
    }

    // ========================================================================
    // Status Message Methods (for PatternResolver and other analyzer passes)
    // ========================================================================

    /// Add an informational status message
    pub fn add_info(&mut self, message: impl Into<String>) {
        self.status_messages
            .push((StatusLevel::Info, message.into()));
    }

    /// Add a warning status message
    pub fn add_warning(&mut self, message: impl Into<String>) {
        self.status_messages
            .push((StatusLevel::Warning, message.into()));
    }

    /// Add an error status message
    pub fn add_error(&mut self, message: impl Into<String>) {
        self.status_messages
            .push((StatusLevel::Error, message.into()));
    }

    /// Get all status messages
    pub fn get_messages(&self) -> &[(StatusLevel, String)] {
        &self.status_messages
    }

    /// Clear all status messages
    pub fn clear_messages(&mut self) {
        self.status_messages.clear();
    }

    // ========================================================================
    // CTE Management
    // ========================================================================

    /// Generate next unique CTE ID
    pub fn next_cte_id(&mut self) -> usize {
        let id = self.cte_counter;
        self.cte_counter += 1;
        id
    }

    // ========================================================================
    // Existing Methods Continue Below
    // ========================================================================

    /// Get the tenant ID for this query context
    pub fn tenant_id(&self) -> Option<&String> {
        self.tenant_id.as_ref()
    }

    /// Get the view parameter values for parameterized views
    pub fn view_parameter_values(&self) -> Option<&HashMap<String, String>> {
        self.view_parameter_values.as_ref()
    }

    /// Merge another PlanCtx into this one
    /// Used for UNION queries where each branch has its own context
    /// Note: This is a simple merge that may have alias conflicts if not careful
    pub fn merge(&mut self, other: PlanCtx) {
        // Merge alias-to-table mappings
        for (alias, table_ctx) in other.alias_table_ctx_map {
            // Only insert if not already present to avoid conflicts
            self.alias_table_ctx_map.entry(alias).or_insert(table_ctx);
        }

        // Merge optional aliases
        for alias in other.optional_aliases {
            self.optional_aliases.insert(alias);
        }

        // Merge projection aliases
        for (alias, expr) in other.projection_aliases {
            self.projection_aliases.entry(alias).or_insert(expr);
        }

        // Merge denormalized node edges
        for (alias, info) in other.denormalized_node_edges {
            self.denormalized_node_edges.entry(alias).or_insert(info);
        }

        // Merge status messages from other context
        self.status_messages.extend(other.status_messages);
    }

    /// Register columns exported by a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name (e.g., "with_p_cte_1")
    /// * `items` - The projection items from WITH clause
    ///
    /// This extracts property names from ProjectionItems and their aliases,
    /// using the naming convention: variablename_propertyname (e.g., "p_firstName")
    pub fn register_cte_columns(&mut self, cte_name: &str, items: &[ProjectionItem]) {
        let mut columns = HashMap::new();

        for item in items {
            // Extract property name from the expression
            if let LogicalExpr::PropertyAccessExp(prop_access) = &item.expression {
                let table_alias = prop_access.table_alias.0.as_str();

                // Extract property name from PropertyValue enum
                let property_name = match &prop_access.column {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                        col.clone()
                    }
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(expr) => {
                        expr.clone()
                    }
                };

                // CTE column name follows convention: variablename_propertyname
                // e.g., p.firstName → p_firstName
                let cte_column = if let Some(alias) = &item.col_alias {
                    // If user provided alias, use it
                    alias.0.clone()
                } else {
                    // Otherwise, generate: variablename_propertyname
                    format!("{}_{}", table_alias, property_name)
                };

                columns.insert(property_name, cte_column);
            }
        }

        log::info!(
            "📊 Registered CTE '{}' with {} columns: {:?}",
            cte_name,
            columns.len(),
            columns
        );
        self.cte_columns.insert(cte_name.to_string(), columns);
    }

    /// Register a single column mapping for a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name
    /// * `schema_column` - The schema-specific column name (e.g., "PersonId", "CommentId")
    /// * `cte_column` - The standardized CTE column name (e.g., "from_node_id", "to_node_id")
    ///
    /// Used for multi-variant relationship CTEs that need to map multiple schema columns
    /// to standardized names.
    pub fn register_cte_column(&mut self, cte_name: &str, schema_column: &str, cte_column: &str) {
        self.cte_columns
            .entry(cte_name.to_string())
            .or_default()
            .insert(schema_column.to_string(), cte_column.to_string());
    }

    /// Get the CTE column name for a property
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name or alias (e.g., "with_o_cte_0" or "o")
    /// * `property` - The graph property name (e.g., "firstName")
    ///
    /// # Returns
    /// The CTE column name (e.g., "p_firstName") or None if not found
    pub fn get_cte_column(&self, cte_name: &str, property: &str) -> Option<&str> {
        // Try direct lookup first
        if let Some(columns) = self.cte_columns.get(cte_name) {
            if let Some(col) = columns.get(property) {
                return Some(col.as_str());
            }
        }
        // Try alias lookup
        if let Some((_original_alias, actual_cte_name)) = self.cte_alias_sources.get(cte_name) {
            if let Some(columns) = self.cte_columns.get(actual_cte_name) {
                return columns.get(property).map(|s| s.as_str());
            }
        }
        None
    }

    /// Check if a table name is a CTE reference
    /// This works with both CTE names (e.g., "with_o_cte_0") and aliases (e.g., "o")
    pub fn is_cte(&self, name: &str) -> bool {
        // Check if it's directly a CTE name
        if self.cte_columns.contains_key(name) {
            return true;
        }
        // Check if it's an alias that maps to a CTE
        if let Some((_original_alias, cte_name)) = self.cte_alias_sources.get(name) {
            return self.cte_columns.contains_key(cte_name);
        }
        false
    }

    /// Register entity types for aliases exported by a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name (e.g., "with_tag_cte_1")
    /// * `exported_aliases` - The aliases exported by WITH (e.g., ["tag", "post"])
    ///
    /// This preserves node/relationship type information across WITH boundaries.
    /// Example: WITH tag, post → stores tag: (false, ["Tag"]), post: (false, ["Post"])
    ///
    /// This enables property resolution after WITH: `WITH tag ... RETURN tag.name`
    pub fn register_cte_entity_types(&mut self, cte_name: &str, exported_aliases: &[String]) {
        let mut entity_types = HashMap::new();

        for alias in exported_aliases {
            // Look up the TableCtx for this alias in current scope
            if let Ok(table_ctx) = self.get_table_ctx(alias) {
                let is_rel = table_ctx.is_relation();
                let labels = table_ctx.get_labels().cloned();

                log::info!(
                    "📊 Registering entity type for CTE '{}' alias '{}': is_rel={}, labels={:?}",
                    cte_name,
                    alias,
                    is_rel,
                    labels
                );

                entity_types.insert(alias.clone(), (is_rel, labels.clone()));

                // NEW (Jan 2026): Also update typed variable system with CTE source
                // This keeps track of which CTE the variable came from
                if is_rel {
                    self.variables.define_relationship(
                        alias.clone(),
                        labels.unwrap_or_default(),
                        table_ctx.get_from_node_label().cloned(),
                        table_ctx.get_to_node_label().cloned(),
                        VariableSource::Cte {
                            cte_name: cte_name.to_string(),
                            property_mapping: Box::new(HashMap::new()),
                        },
                    );
                } else if table_ctx.is_path_variable() {
                    // Path variable exported through CTE
                    self.variables
                        .define_path(alias.clone(), None, None, None, None, false);
                    // Note: Path info is limited when passing through CTE
                } else if labels.as_ref().is_none_or(|l| l.is_empty())
                    && table_ctx.is_explicit_alias()
                {
                    // Computed alias with no labels (e.g., `WITH count(*) AS cnt`)
                    // → scalar, not a node
                    self.variables.define_scalar(
                        alias.clone(),
                        VariableSource::Cte {
                            cte_name: cte_name.to_string(),
                            property_mapping: Box::new(HashMap::new()),
                        },
                    );
                } else {
                    self.variables.define_node(
                        alias.clone(),
                        labels.unwrap_or_default(),
                        VariableSource::Cte {
                            cte_name: cte_name.to_string(),
                            property_mapping: Box::new(HashMap::new()),
                        },
                    );
                }
            } else {
                // Alias not found in current scope - might be from parent scope or error
                // Could be a scalar/aggregation from the WITH clause
                log::warn!(
                    "⚠️  CTE '{}' exports alias '{}' but no TableCtx found in scope (may be scalar)",
                    cte_name,
                    alias
                );

                // NEW (Jan 2026): Register as scalar since it's not in TableCtx
                // This handles aggregation results like COUNT(x) AS cnt
                self.variables.define_scalar(
                    alias.clone(),
                    VariableSource::Cte {
                        cte_name: cte_name.to_string(),
                        property_mapping: Box::new(HashMap::new()),
                    },
                );
            }
        }

        self.cte_entity_types
            .insert(cte_name.to_string(), entity_types);
    }

    /// Get entity type information for a CTE alias
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name
    /// * `alias` - The exported alias
    ///
    /// # Returns
    /// Some((is_rel, labels)) if found, None otherwise
    pub fn get_cte_entity_type(
        &self,
        cte_name: &str,
        alias: &str,
    ) -> Option<&(bool, Option<Vec<String>>)> {
        self.cte_entity_types.get(cte_name)?.get(alias)
    }

    /// Get property requirements for optimization
    ///
    /// Returns None if not yet populated by PropertyRequirementsAnalyzer pass
    pub fn get_property_requirements(&self) -> Option<&PropertyRequirements> {
        self.property_requirements.as_ref()
    }

    /// Set property requirements (called by PropertyRequirementsAnalyzer pass)
    ///
    /// This should be called once after analyzing the query plan to determine
    /// which properties are actually needed for each alias.
    pub fn set_property_requirements(&mut self, requirements: PropertyRequirements) {
        self.property_requirements = Some(requirements);
    }

    /// Check if property requirements have been populated
    ///
    /// Returns true if PropertyRequirementsAnalyzer pass has run and set requirements
    pub fn has_property_requirements(&self) -> bool {
        self.property_requirements.is_some()
    }

    // ========================================================================
    // WHERE Property Requirements (Track C - Property-Based Optimization)
    // ========================================================================

    /// Set property requirements extracted from WHERE clause
    /// Used for property-based UNION branch pruning
    pub fn set_where_property_requirements(
        &mut self,
        requirements: HashMap<String, HashSet<String>>,
    ) {
        log::debug!("Setting WHERE property requirements: {:?}", requirements);
        self.where_property_requirements = requirements;
    }

    /// Get property requirements for a specific alias from WHERE clause
    /// Returns None if no requirements for this alias
    pub fn get_where_property_requirements(&self, alias: &str) -> Option<&HashSet<String>> {
        self.where_property_requirements.get(alias)
    }

    /// Check if alias has any WHERE property requirements
    pub fn has_where_property_requirements(&self, alias: &str) -> bool {
        self.where_property_requirements.contains_key(alias)
    }

    // ========================================================================
    // WHERE Label Constraints (UNION Pruning Optimization)
    // ========================================================================

    /// Set label constraints extracted from WHERE clause id() patterns
    /// Used for UNION branch pruning optimization
    pub fn set_where_label_constraints(&mut self, constraints: HashMap<String, HashSet<String>>) {
        log::debug!("Setting WHERE label constraints: {:?}", constraints);
        self.where_label_constraints = constraints;
    }

    /// Get label constraints for a specific alias from WHERE clause
    /// Returns None if no constraints for this alias
    pub fn get_where_label_constraints(&self, alias: &str) -> Option<&HashSet<String>> {
        self.where_label_constraints.get(alias)
    }

    /// Check if alias has any WHERE label constraints
    pub fn has_where_label_constraints(&self, alias: &str) -> bool {
        self.where_label_constraints.contains_key(alias)
    }
}

impl fmt::Display for PlanCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n---- PlanCtx Starts Here ----")?;
        for (alias, table_ctx) in &self.alias_table_ctx_map {
            writeln!(f, "\n [{}]:", alias)?;
            table_ctx.fmt_with_indent(f, 2)?;
        }
        writeln!(f, "\n---- PlanCtx Ends Here ----")?;
        Ok(())
    }
}

impl PlanCtx {
    // ========================================================================
    // Pattern Schema Context Management (Phase 1A-2)
    // ========================================================================

    /// Register a PatternSchemaContext for a relationship pattern
    ///
    /// This is called during graph pattern analysis (graph_join_inference.rs)
    /// after computing the PatternSchemaContext for each relationship.
    ///
    /// # Arguments
    /// * `rel_alias` - The relationship alias (e.g., "r", "follows")
    /// * `ctx` - The analyzed pattern schema context
    pub fn register_pattern_context(&mut self, rel_alias: String, ctx: PatternSchemaContext) {
        crate::debug_print!(
            "🔧 PlanCtx::register_pattern_context: rel='{}', left_node='{}', right_node='{}'",
            rel_alias,
            ctx.left_node_alias,
            ctx.right_node_alias
        );
        self.pattern_contexts.insert(rel_alias, Arc::new(ctx));
    }

    /// Get the PatternSchemaContext for a relationship
    ///
    /// # Arguments
    /// * `rel_alias` - The relationship alias (e.g., "r", "follows")
    ///
    /// # Returns
    /// - `Some(&PatternSchemaContext)` if the relationship has been analyzed
    /// - `None` if the relationship pattern hasn't been registered yet
    pub fn get_pattern_context(&self, rel_alias: &str) -> Option<&PatternSchemaContext> {
        self.pattern_contexts.get(rel_alias).map(|arc| arc.as_ref())
    }

    /// Get the NodeAccessStrategy for a specific node variable
    ///
    /// This is the key method for property resolution - given a node alias,
    /// it finds which relationship pattern(s) the node appears in and returns
    /// the appropriate access strategy.
    ///
    /// # Arguments
    /// * `node_alias` - The node variable alias (e.g., "a", "user")
    /// * `edge_alias` - Optional relationship context for multi-hop disambiguation
    ///
    /// # Returns
    /// - `Some(&NodeAccessStrategy)` if the node is part of a registered pattern
    /// - `None` if the node isn't part of any relationship pattern (standalone node)
    ///
    /// # Notes
    /// For multi-hop queries where same node appears in multiple edges (e.g., `(a)-[r1]->(b)-[r2]->(c)`),
    /// the node `b` has different strategies in context of r1 (right/to_node) vs r2 (left/from_node).
    /// If `edge_alias` is provided, it disambiguates which pattern to use.
    pub fn get_node_strategy(
        &self,
        node_alias: &str,
        edge_alias: Option<&str>,
    ) -> Option<&crate::graph_catalog::pattern_schema::NodeAccessStrategy> {
        // If edge_alias is provided, use it directly
        if let Some(rel_alias) = edge_alias {
            if let Some(ctx) = self.get_pattern_context(rel_alias) {
                return ctx.get_node_strategy(node_alias);
            }
        }

        // Otherwise, search all patterns for this node
        // (Returns first match - caller should provide edge_alias for disambiguation)
        for ctx in self.pattern_contexts.values() {
            if let Some(strategy) = ctx.get_node_strategy(node_alias) {
                return Some(strategy);
            }
        }

        None
    }

    /// Get all pattern contexts (for debugging)
    pub fn get_all_pattern_contexts(&self) -> &HashMap<String, Arc<PatternSchemaContext>> {
        &self.pattern_contexts
    }

    // ========================================================================
    // VLP Endpoint Tracking (for VLP+chained pattern JOIN generation)
    // ========================================================================

    /// Register a VLP endpoint (node that's part of a variable-length path).
    /// When generating JOINs for subsequent patterns, these endpoints should
    /// reference the VLP CTE (t.start_id/t.end_id) instead of the original table.
    pub fn register_vlp_endpoint(&mut self, alias: String, info: VlpEndpointInfo) {
        log::debug!(
            "🔧 PlanCtx::register_vlp_endpoint: alias='{}', position={:?}, other='{}', rel='{}'",
            alias,
            info.position,
            info.other_endpoint_alias,
            info.rel_alias
        );
        self.vlp_endpoints.insert(alias, info);
    }

    /// Register multiple VLP endpoints at once (convenience method).
    pub fn register_vlp_endpoints(&mut self, endpoints: HashMap<String, VlpEndpointInfo>) {
        for (alias, info) in endpoints {
            self.register_vlp_endpoint(alias, info);
        }
    }

    /// Check if an alias is a VLP endpoint (needs CTE reference translation).
    pub fn is_vlp_endpoint(&self, alias: &str) -> bool {
        self.vlp_endpoints.contains_key(alias)
    }

    /// Get VLP endpoint info for an alias.
    pub fn get_vlp_endpoint(&self, alias: &str) -> Option<&VlpEndpointInfo> {
        self.vlp_endpoints.get(alias)
    }

    /// Get all VLP endpoints (for debugging or bulk operations).
    pub fn get_vlp_endpoints(&self) -> &HashMap<String, VlpEndpointInfo> {
        &self.vlp_endpoints
    }

    /// Generate the next unique VLP CTE alias (vt0, vt1, vt2, ...).
    /// Called once per VLP (not per endpoint) — both endpoints of the same VLP share the alias.
    /// Scoped to this PlanCtx, so concurrent queries get independent counters.
    pub fn next_vlp_alias(&mut self) -> String {
        let idx = self.vlp_alias_counter;
        self.vlp_alias_counter += 1;
        format!("vt{}", idx)
    }

    /// Get the proper (table_alias, column) for a JOIN condition, accounting for VLP endpoints.
    ///
    /// This is the key method for fixing VLP+chained patterns:
    /// - For regular nodes: returns (alias, column) unchanged
    /// - For VLP endpoints: returns ("t", "start_id"/"end_id")
    pub fn get_vlp_join_reference(&self, alias: &str, default_column: &str) -> (String, String) {
        if let Some(vlp_info) = self.vlp_endpoints.get(alias) {
            (
                vlp_info.vlp_alias.clone(),
                vlp_info.cte_column().to_string(),
            )
        } else {
            (alias.to_string(), default_column.to_string())
        }
    }

    /// Register a CTE alias source mapping for variable renaming
    /// When a variable is renamed through a WITH clause (u AS person),
    /// tracks that "person" comes from "u"
    ///
    /// # Arguments
    /// * `new_alias` - The renamed alias (e.g., "person")
    /// * `original_alias` - The original alias (e.g., "u")
    /// * `cte_name` - The CTE name (e.g., "with_person_cte_1")
    pub fn register_cte_alias_source(
        &mut self,
        new_alias: String,
        original_alias: String,
        cte_name: String,
    ) {
        log::info!(
            "📝 Registered CTE alias source: {} → ({}, {})",
            new_alias,
            original_alias,
            cte_name
        );
        self.cte_alias_sources
            .insert(new_alias, (original_alias, cte_name));
    }

    /// Get the source (original alias, cte_name) for a CTE-backed alias
    /// Returns None if alias is not a renamed CTE reference
    pub fn get_cte_alias_source(&self, alias: &str) -> Option<&(String, String)> {
        self.cte_alias_sources.get(alias)
    }
}

// ============================================================================
// Typed Variable API (NEW Jan 2026)
// See docs/development/variable-type-system-design.md
// ============================================================================

impl PlanCtx {
    // ========================================================================
    // Variable Definition Methods (populate during MATCH/WITH processing)
    // ========================================================================

    /// Define a node variable in the current scope
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "a", "user")
    /// * `labels` - Node labels (e.g., ["User"])
    ///
    /// # Example
    /// ```text
    /// MATCH (a:User) → plan_ctx.define_node("a", vec!["User"])
    /// ```
    pub fn define_node(&mut self, name: impl Into<String>, labels: Vec<String>) {
        self.variables
            .define_node(name, labels, VariableSource::Match);
    }

    /// Update labels on an existing node variable (e.g., after type inference)
    pub fn update_node_labels(&mut self, name: &str, labels: Vec<String>) {
        self.variables.update_node_labels(name, labels);
    }

    /// Define a node variable from a CTE export
    ///
    /// # Arguments
    /// * `name` - Variable name
    /// * `labels` - Node labels (preserved from original)
    /// * `cte_name` - The CTE name (e.g., "with_a_cte_1")
    pub fn define_node_from_cte(
        &mut self,
        name: impl Into<String>,
        labels: Vec<String>,
        cte_name: String,
    ) {
        self.variables.define_node(
            name,
            labels,
            VariableSource::Cte {
                cte_name,
                property_mapping: Box::new(HashMap::new()),
            },
        );
    }

    /// Define a relationship variable in the current scope
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "r", "follows")
    /// * `rel_types` - Relationship types (e.g., ["FOLLOWS"])
    /// * `from_label` - Label of source node (for polymorphic resolution)
    /// * `to_label` - Label of target node (for polymorphic resolution)
    ///
    /// # Example
    /// ```text
    /// MATCH (a)-[r:FOLLOWS]->(b) → plan_ctx.define_relationship("r", vec!["FOLLOWS"], Some("User"), Some("User"), Some("Outgoing"))
    /// ```
    pub fn define_relationship(
        &mut self,
        name: impl Into<String>,
        rel_types: Vec<String>,
        from_label: Option<String>,
        to_label: Option<String>,
        direction: Option<String>,
    ) {
        self.variables.define_relationship_with_direction(
            name,
            rel_types,
            from_label,
            to_label,
            VariableSource::Match,
            direction,
        );
    }

    /// Define a relationship variable from a CTE export
    pub fn define_relationship_from_cte(
        &mut self,
        name: impl Into<String>,
        rel_types: Vec<String>,
        from_label: Option<String>,
        to_label: Option<String>,
        cte_name: String,
        direction: Option<String>,
    ) {
        self.variables.define_relationship_with_direction(
            name,
            rel_types,
            from_label,
            to_label,
            VariableSource::Cte {
                cte_name,
                property_mapping: Box::new(HashMap::new()),
            },
            direction,
        );
    }

    /// Define a scalar variable (from aggregation, expression, etc.)
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "count", "total")
    /// * `cte_name` - The CTE name where this scalar was computed
    ///
    /// # Example
    /// ```text
    /// WITH count(b) as follower_count → plan_ctx.define_scalar("follower_count", "with_cte_1")
    /// ```
    pub fn define_scalar(&mut self, name: impl Into<String>, cte_name: String) {
        self.variables.define_scalar(
            name,
            VariableSource::Cte {
                cte_name,
                property_mapping: Box::new(HashMap::new()),
            },
        );
    }

    /// Define a scalar from UNWIND
    pub fn define_scalar_from_unwind(&mut self, name: impl Into<String>, source_array: String) {
        self.variables
            .define_scalar(name, VariableSource::Unwind { source_array });
    }

    /// Define a path variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "p", "path")
    /// * `start_node` - Alias of start node
    /// * `end_node` - Alias of end node
    /// * `relationship` - Alias of relationship pattern
    /// * `length_bounds` - (min, max) hops for variable-length patterns
    /// * `is_shortest_path` - Whether this is a shortest path pattern
    ///
    /// # Example
    /// ```text
    /// MATCH p = (a)-[*1..3]->(b) → plan_ctx.define_path("p", Some("a"), Some("b"), None, Some((Some(1), Some(3))), false)
    /// ```
    pub fn define_path(
        &mut self,
        name: impl Into<String>,
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
        is_shortest_path: bool,
    ) {
        self.variables.define_path(
            name,
            start_node,
            end_node,
            relationship,
            length_bounds,
            is_shortest_path,
        );
    }

    /// Define a collection variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "nodes", "items")
    /// * `element_type` - What type of elements the collection contains
    /// * `cte_name` - The CTE name where this collection was computed
    ///
    /// # Example
    /// ```text
    /// WITH nodes(p) as path_nodes → plan_ctx.define_collection("path_nodes", CollectionElementType::Nodes, "with_cte_1")
    /// ```
    pub fn define_collection(
        &mut self,
        name: impl Into<String>,
        element_type: CollectionElementType,
        cte_name: String,
    ) {
        self.variables.define_collection(
            name,
            element_type,
            VariableSource::Cte {
                cte_name,
                property_mapping: Box::new(HashMap::new()),
            },
        );
    }

    // ========================================================================
    // Variable Lookup Methods
    // ========================================================================

    /// Look up a typed variable by name
    ///
    /// This is THE unified lookup method - single source of truth for variable types.
    /// Replaces fragmented lookup across TableCtx, ScopeContext, etc.
    ///
    /// # Returns
    /// - `Some(&TypedVariable)` if variable exists in current scope
    /// - `None` if variable not found
    ///
    /// # Note
    /// This method does NOT search parent scopes (yet). Parent scope search
    /// will be added in Phase 2 when we integrate with existing scope chain.
    pub fn lookup_variable(&self, name: &str) -> Option<&TypedVariable> {
        self.variables.lookup(name)
    }

    /// Check if a typed variable exists
    pub fn has_variable(&self, name: &str) -> bool {
        self.variables.contains(name)
    }

    /// Get all typed variable names in current scope
    pub fn variable_names(&self) -> impl Iterator<Item = &String> {
        self.variables.names()
    }

    /// Get the variable registry (for advanced operations)
    pub fn variables(&self) -> &VariableRegistry {
        &self.variables
    }

    /// Get mutable access to variable registry
    pub fn variables_mut(&mut self) -> &mut VariableRegistry {
        &mut self.variables
    }

    // ========================================================================
    // CTE Export Methods
    // ========================================================================

    /// Export variables through a WITH clause to a new CTE
    ///
    /// Creates CTE-sourced versions of the specified variables.
    /// This is called during WITH clause processing.
    ///
    /// # Arguments
    /// * `exported_names` - Names of variables being exported
    /// * `cte_name` - The CTE name for the WITH clause
    ///
    /// # Returns
    /// A new VariableRegistry containing only exported variables with CTE source
    ///
    /// # Example
    /// ```text
    /// MATCH (a:User)-[r]->(b) WITH a, count(b) as cnt
    /// → plan_ctx.export_variables_to_cte(&["a"], "with_a_cnt_cte_1")
    /// ```
    pub fn export_variables_to_cte(
        &self,
        exported_names: &[&str],
        cte_name: &str,
    ) -> VariableRegistry {
        self.variables.export_to_cte(exported_names, cte_name)
    }

    /// Import variables from a CTE export into the current scope
    ///
    /// Used after WITH clause processing to make exported variables available.
    pub fn import_variables_from_cte(&mut self, exported: &VariableRegistry) {
        self.variables.merge_overwrite(exported);
    }

    // ========================================================================
    // Multi-Type Support Methods (Feb 2026)
    // ========================================================================

    /// Store node type combinations for multi-type simple nodes
    ///
    /// Used when a simple node query like `MATCH (n) RETURN n` can match multiple types.
    /// CTE generation will create UNION of all node tables.
    ///
    /// # Example
    /// ```ignore
    /// plan_ctx.store_node_combinations("n", vec!["User", "Post", "ZeekLog"]);
    /// ```
    pub fn store_node_combinations(&mut self, alias: &str, labels: Vec<String>) {
        log::info!("🎯 Storing node combinations for '{}': {:?}", alias, labels);
        self.node_combinations.insert(alias.to_string(), labels);
    }

    /// Get node type combinations for an alias
    pub fn get_node_combinations(&self, alias: &str) -> Option<&Vec<String>> {
        self.node_combinations.get(alias)
    }

    /// Store pattern type combinations for multi-type patterns
    ///
    /// Used when pattern inference finds ambiguous nodes that could match multiple types.
    /// CTE generation will create UNION of all valid pattern combinations.
    ///
    /// # Example
    /// ```ignore
    /// let combinations = vec![
    ///     TypeCombination {
    ///         from_label: "User".to_string(),
    ///         rel_type: "FOLLOWS".to_string(),
    ///         to_label: "User".to_string(),
    ///     },
    ///     TypeCombination {
    ///         from_label: "User".to_string(),
    ///         rel_type: "AUTHORED".to_string(),
    ///         to_label: "Post".to_string(),
    ///     },
    /// ];
    /// plan_ctx.store_pattern_combinations("a", "b", combinations);
    /// ```
    pub fn store_pattern_combinations(
        &mut self,
        from_alias: &str,
        to_alias: &str,
        combinations: Vec<TypeCombination>,
    ) {
        log::info!(
            "🎯 Storing {} pattern combinations for '{}' -> '{}'",
            combinations.len(),
            from_alias,
            to_alias
        );
        self.pattern_combinations
            .insert((from_alias.to_string(), to_alias.to_string()), combinations);
    }

    /// Get pattern type combinations for a pair of aliases
    pub fn get_pattern_combinations(
        &self,
        from_alias: &str,
        to_alias: &str,
    ) -> Option<&Vec<TypeCombination>> {
        self.pattern_combinations
            .get(&(from_alias.to_string(), to_alias.to_string()))
    }

    /// Generate pattern combinations from schema matches
    ///
    /// This is a helper method that generates cartesian product of node types and relationship types,
    /// validates them against the schema, and stores the results.
    ///
    /// # Arguments
    /// * `from_alias` - From node alias
    /// * `to_alias` - To node alias
    /// * `from_candidates` - Candidate labels for from node
    /// * `rel_candidates` - Candidate relationship types
    /// * `to_candidates` - Candidate labels for to node
    /// * `schema` - Graph schema for validation
    /// * `max_combinations` - Maximum number of combinations (default 38)
    ///
    /// # Returns
    /// Number of valid combinations generated
    pub fn generate_and_store_pattern_combinations(
        &mut self,
        from_alias: &str,
        to_alias: &str,
        from_candidates: Vec<String>,
        rel_candidates: Vec<String>,
        to_candidates: Vec<String>,
        schema: &crate::graph_catalog::graph_schema::GraphSchema,
        max_combinations: usize,
    ) -> usize {
        let mut combinations = Vec::new();
        let mut count = 0;

        // Generate cartesian product and validate against schema
        for from_label in &from_candidates {
            for rel_type in &rel_candidates {
                for to_label in &to_candidates {
                    // Validate this combination against schema
                    if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                        let from_matches = from_label == &rel_schema.from_node;
                        let to_matches = to_label == &rel_schema.to_node;

                        if from_matches && to_matches {
                            combinations.push(TypeCombination {
                                from_label: from_label.clone(),
                                rel_type: rel_type.clone(),
                                to_label: to_label.clone(),
                            });
                            count += 1;

                            if count >= max_combinations {
                                log::warn!(
                                    "⚠️ Hit {} combination limit for '{}' -> '{}'",
                                    max_combinations,
                                    from_alias,
                                    to_alias
                                );
                                self.store_pattern_combinations(from_alias, to_alias, combinations);
                                return count;
                            }
                        }
                    }
                }
            }
        }

        if !combinations.is_empty() {
            self.store_pattern_combinations(from_alias, to_alias, combinations);
        }

        count
    }

    // ========================================================================
    // Group Combination Methods (Feb 2026 - Cross-Pattern Optimization)
    // ========================================================================

    /// Store group combinations for connected patterns
    ///
    /// Used when multiple patterns share variables (e.g., `(a)-[r1]->(b)-[r2]->(c)` shares `b`).
    /// Group combinations ensure consistent type assignment across all patterns.
    ///
    /// # Arguments
    /// * `group_id` - Unique identifier for the connected group (e.g., "a_b_c")
    /// * `combos` - Valid type combinations for the entire group
    ///
    /// # Example
    /// ```ignore
    /// let group_combos = vec![
    ///     GroupCombination {
    ///         pattern_types: [
    ///             (("a","b"), TypeCombination { from: "User", rel: "FOLLOWS", to: "User" }),
    ///             (("b","c"), TypeCombination { from: "User", rel: "WORKS_AT", to: "Company" }),
    ///         ].into(),
    ///     },
    /// ];
    /// plan_ctx.store_group_combinations("a_b_c", group_combos);
    /// ```
    pub fn store_group_combinations(&mut self, group_id: String, combos: Vec<GroupCombination>) {
        log::info!(
            "🎯 Storing {} group combinations for connected group '{}'",
            combos.len(),
            group_id
        );
        self.group_combinations.insert(group_id, combos);
    }

    /// Get group combinations for a connected group
    pub fn get_group_combinations(&self, group_id: &str) -> Option<&Vec<GroupCombination>> {
        self.group_combinations.get(group_id)
    }

    /// Extract pattern-specific combinations from group combinations
    ///
    /// This is the key interface for downstream code: given a specific pattern
    /// (identified by from/to aliases), extract just its combinations from the group.
    ///
    /// # Arguments
    /// * `group_id` - The connected group identifier
    /// * `left_alias` - From node alias (e.g., "a")
    /// * `right_alias` - To node alias (e.g., "b")
    ///
    /// # Returns
    /// Vector of type combinations for this specific pattern, or None if group not found
    ///
    /// # Example
    /// ```ignore
    /// // After storing group combos for "a_b_c"
    /// let r1_combos = plan_ctx.get_pattern_combinations_from_group("a_b_c", "a", "b");
    /// // Returns: [(User, FOLLOWS, User), (User, AUTHORED, Post), ...]
    /// ```
    pub fn get_pattern_combinations_from_group(
        &self,
        group_id: &str,
        left_alias: &str,
        right_alias: &str,
    ) -> Option<Vec<TypeCombination>> {
        self.group_combinations.get(group_id).map(|group_combos| {
            group_combos
                .iter()
                .filter_map(|gc| {
                    gc.pattern_types
                        .get(&(left_alias.to_string(), right_alias.to_string()))
                        .cloned()
                })
                .collect()
        })
    }
}
