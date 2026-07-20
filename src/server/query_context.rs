//! Query Context - Task-local storage for per-query state
//!
//! This module provides isolated per-query context using `tokio::task_local!`.
//! Each HTTP request/query gets its own context that is:
//! - Isolated from other concurrent queries (even on the same OS thread)
//! - Automatically cleaned up when the query completes
//! - Accessible from any code path during query processing
//!
//! ## Usage Pattern
//!
//! The query handler MUST wrap all query processing in `with_query_context()`:
//!
//! ```ignore
//! pub async fn query_handler(...) -> Result<...> {
//!     let context = QueryContext::new(schema_name);
//!
//!     with_query_context(context, async {
//!         // ALL query processing happens here
//!         // Context is automatically available via get_query_context()
//!         process_query().await
//!     }).await
//! }
//! ```
//!
//! ## Why task_local! instead of thread_local!
//!
//! In an async server like Axum/Tokio:
//! - `thread_local!`: Shared by ALL async tasks on the same OS thread - UNSAFE for concurrent queries
//! - `task_local!`: Each async task gets isolated storage - SAFE
//!
//! The `.scope()` wrapper is REQUIRED for task_local to work. Without it, `try_with()` returns None.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::sql_generator::SqlDialect;

/// Per-query context holding all query-scoped state
/// This replaces multiple scattered task_local!/thread_local! declarations
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
    /// SQL dialect to target for this query. Drives both function-name
    /// mapping ([`crate::sql_generator::function_mapper::for_dialect`]) and
    /// the few structural rewrites that can't be expressed as a name swap
    /// (e.g. `arrayCount` → `size(filter(...))` for Databricks/Spark).
    ///
    /// Defaults to ClickHouse so call sites outside a task-local scope —
    /// notably unit tests — keep emitting CH SQL unchanged.
    pub dialect: SqlDialect,

    /// Schema name for this query (from USE clause or request parameter)
    pub schema_name: Option<String>,

    /// The resolved GraphSchema for this query, set once at query entry.
    /// All downstream code should use `get_current_schema()` instead of
    /// accessing GLOBAL_SCHEMAS directly.
    pub schema: Option<Arc<GraphSchema>>,

    /// Denormalized edge alias mapping: target_node_alias → edge_alias
    /// For denormalized edges where the edge table serves as both edge and target node
    pub denormalized_aliases: HashMap<String, String>,

    /// Relationship columns: alias → (from_id_column, to_id_column)
    /// Used for IS NULL checks on relationship aliases
    pub relationship_columns: HashMap<String, (String, String)>,

    /// CTE property mappings: cte_alias → { property → column_name }
    /// For resolving properties on CTE-exported variables
    pub cte_property_mappings: HashMap<String, HashMap<String, String>>,

    /// Multi-type VLP aliases: alias → cte_name
    /// For aliases that are multi-type VLP endpoints requiring JSON extraction
    pub multi_type_vlp_aliases: HashMap<String, String>,

    /// #466: `pattern_union_*` CTE names actually referenced by the current
    /// render plan's FROM/JOIN (registered by from_builder/join_builder the
    /// moment they emit the CTE reference). The outer-WHERE rewriter
    /// (`filter_builder`) may only SKIP a node-property conjunct when the
    /// CTE that absorbs it per-branch is registered here — an unconditional
    /// skip silently dropped predicates on plan shapes where the CTE never
    /// renders (e.g. some multi-MATCH cartesians whose FROM/JOINs stay on
    /// plain tables and whose built-but-unreferenced CTE is dead-eliminated).
    /// FROM/JOIN extraction runs before filter extraction in every render
    /// arm, so registrations are visible to the filter builder.
    ///
    /// NOTE (reviewer, round 4): entries are never cleared between subplans
    /// of one query, so a CTE registered while rendering one subplan stays
    /// visible while rendering later subplans of the same task. Since CTE
    /// names are `pattern_union_{rel_alias}` and a rel alias maps to the
    /// same pattern within a query, no incorrect skip has been reproduced;
    /// if per-subplan isolation is ever needed, scope this set per render
    /// pass instead.
    pub pattern_union_scope_ctes: HashSet<String>,

    /// #623: relationship aliases of exact-bound (min==max) VLPs that the
    /// analyzer decided to REROUTE from the flat r1..rN self-join chain to the
    /// recursive-CTE path, because the exact VLP is adjacent to another hop
    /// (a sibling GraphRel shares one of its endpoint aliases). The flat
    /// expander cannot compose with a neighboring hop — it silently collapses
    /// `*N..N` to `*1..1` (trailing neighbor) or drops the leading hop — so
    /// such an exact VLP must go through the same recursive CTE the RANGE path
    /// uses (which composes on both sides off `t.start_id`/`t.end_id`). The
    /// analyzer's `should_skip_for_vlp` populates this; the four render gates
    /// (`is_fixed_length_vlp`, the filter gate, `use_chained_join`, the
    /// join-expansion gate) read it via `is_adjacent_exact_vlp_reroute` to
    /// treat the VLP as non-fixed-length. A pure lookup channel, mirroring
    /// `multi_type_vlp_aliases` — no `GraphRel` field, no struct-literal churn.
    pub adjacent_exact_vlp_reroute_aliases: HashSet<String>,

    /// #623: path variable → exact hop count for a fixed exact-bound VLP
    /// (`MATCH p = (a)-[:R*N..N]->(b)`). The flat expander renders `N`
    /// relationship joins (not `2N`), so the `length(p)` fallback's
    /// `joins/2` heuristic yields `N/2` — wrong. `build_vlp_context` records
    /// the true `N` here (keyed by the VLP's `path_variable`); the `length(p)`
    /// resolver reads it. Empty when the query has no path-variable VLP.
    pub vlp_exact_path_hops: HashMap<String, u32>,

    /// VLP CTE outer-query aliases: cte_name → vlp_alias (e.g., "vlp_u1_u2" → "vt0")
    /// Used by FROM/JOIN builders to assign unique aliases per VLP CTE.
    /// NOTE: Currently not populated — see TODO(multi-vlp) in cte_extraction.rs.
    pub vlp_cte_outer_aliases: HashMap<String, String>,

    /// Current variable registry for SQL rendering.
    /// Set from the CTE or RenderPlan being rendered; used by PropertyAccessExp::to_sql()
    /// to resolve cypher_alias.property → correct SQL column.
    /// Wrapped in Arc to match RenderPlan/Cte fields and avoid cloning overhead.
    pub current_variable_registry:
        Option<std::sync::Arc<crate::query_planner::typed_variable::VariableRegistry>>,

    /// Cypher alias → (generation, CTE FROM alias, {Cypher property → CTE
    /// column}) for a WITH-CTE-exported variable, published *while*
    /// `build_chained_with_match_cte_plan` is still building the plan (not
    /// only at the very end, unlike `current_variable_registry`/
    /// `cte_property_mappings`, both of which are populated only after the
    /// full render plan — including any pre-rendered EXISTS SQL baked from it
    /// — already exists).
    ///
    /// This is a narrow, purpose-built channel for
    /// `render_expr::generate_exists_sql`'s `GraphRel` branch: it needs to
    /// resolve a WITH-barrier-crossing `EXISTS { ... }` pattern's correlation
    /// variable id through the CTE scope active *at that exact point in the
    /// build*, before the variable moves on to a later WITH clause's own CTE.
    /// `current_variable_registry` cannot serve this purpose today —
    /// `VariableRegistry::define_node`/`define_scalar` unconditionally
    /// construct a fresh, empty `property_mapping` for `VariableSource::Cte`,
    /// so it never actually carries CTE column info in production (ordinary
    /// property access instead resolves via the separate legacy
    /// `cte_property_mappings` path, populated later from the final render
    /// plan). Written only in `build_chained_with_match_cte_plan`; read only
    /// by `generate_exists_sql` — cannot affect any other resolution path.
    ///
    /// The `generation` tag (see `cte_scope_generation` /
    /// `enter_cte_scope_generation`) is what keeps this safe across
    /// independent subplans (UNION arms, cartesian-product sides, etc.) that
    /// reuse the same Cypher alias name within a single query/task: a query
    /// like `MATCH (a) WITH a, count(*) AS c ... RETURN a.name UNION MATCH
    /// (a) WHERE EXISTS {(a)-[:LIKED]->(y)} RETURN 'x' AS name` renders each
    /// UNION arm independently, and the second arm's fresh, non-CTE-scoped
    /// `a` must NOT resolve through the first arm's now-stale entry for `a`
    /// just because they share a name — see the entry point's doc for how
    /// generation scoping prevents that without needing to enumerate every
    /// independent-subplan boundary (UNION arms, cartesian sides, OPTIONAL
    /// MATCH branches, ...) by hand.
    pub cte_scope_for_correlation: HashMap<String, (u64, String, HashMap<String, String>)>,

    /// Current "CTE scope generation" — see `enter_cte_scope_generation`.
    /// `0` is the sentinel meaning "no `build_chained_with_match_cte_plan`
    /// invocation is currently active" (i.e. we're between independent
    /// subplans, or haven't started rendering any WITH-CTE plan yet).
    pub cte_scope_generation: u64,

    /// Weight CTE config for weighted shortest path (Dijkstra).
    /// Set in build_chained_with_match_cte_plan when a weight CTE is detected.
    /// Read by VLP CTE generation to use weighted mode.
    pub weight_cte_config: Option<crate::clickhouse_query_generator::WeightCteConfig>,

    /// CTE alias → CTE name mapping for the CURRENT rendering scope (CTE body or main plan).
    /// Set per-scope during to_sql() rendering so `IN alias.column` can resolve to
    /// `IN (SELECT column FROM cte_name)` when the alias refers to a CTE (scalar column).
    /// Updated before each CTE body render and restored after.
    pub cte_alias_to_cte_name: HashMap<String, String>,

    /// All known CTE names in the current query plan.
    /// Set once during render_plan_to_sql() so Cte::to_sql() can build scope-specific
    /// alias mappings without needing access to the full plan.
    pub all_cte_names: HashSet<String>,

    /// Alias → node label mapping built from the render plan's FROM/JOIN tables.
    /// Used by `n.id` pseudo-property resolution to find the correct schema node_id
    /// column for a given table alias when the variable registry is not populated
    /// (simple queries without WITH clauses).
    pub alias_label_map: HashMap<String, String>,

    /// Names of CTE output columns that hold an array/collection value (produced
    /// by a `collect`/`groupArray` aggregate or a list literal). Set once during
    /// render_plan_to_sql(). Lets the Databricks `size()` render dispatch pick
    /// Spark `size` (collection) vs `length` (string) when the argument is a
    /// carried-forward collection column whose type the variable registry does
    /// not track (e.g. `WITH collect(post) AS posts`).
    pub array_cte_columns: HashSet<String>,

    /// S1 stats-informed planning: per-table row-count snapshot for this query
    /// (`docs/design/STATS_PLANNING.md`). Attached at query entry by the server
    /// when `CLICKGRAPH_STATS_ENABLED=true` and a fetch has succeeded; `None`
    /// everywhere else — sql_only, embedded, tests, flag off — which keeps the
    /// planner byte-identical to the stats-less engine (the ONLY consumer is
    /// `select_anchor`'s ordering-only ranking; guardrail: stats never change
    /// row membership, PRIORITIES.md §1.7).
    pub table_stats: Option<Arc<crate::graph_catalog::table_stats::TableStatsSnapshot>>,

    /// #596: Cypher aliases bound in the OUTER (enclosing) query scope at the
    /// point an `EXISTS { ... }` pattern predicate is rendered. Populated from
    /// the outer plan's live node/relationship aliases (see
    /// `collect_live_table_aliases`) at the top of `to_render_plan` /
    /// `build_chained_with_match_cte_plan`, BEFORE the WHERE predicate — and
    /// therefore any pre-rendered EXISTS SQL — is built. `generate_exists_sql`
    /// reads it to decide, for each endpoint of a correlated `EXISTS`
    /// relationship pattern, whether that endpoint is an outer anchor (bound
    /// here) or a fresh existentially-quantified inner variable. This is the
    /// only signal that distinguishes structurally-identical subplans like
    /// `EXISTS { (b)-[:R]->(a) }` where BOTH `a,b` are outer (correlate both
    /// endpoints) vs. only `a` is outer (correlate the right endpoint alone).
    /// Union/merge semantics: an alias bound in ANY enclosing scope is "outer"
    /// to a more-deeply-nested EXISTS, so entries are only ever added.
    pub exists_outer_aliases: HashSet<String>,
}

/// Process-wide default SQL dialect for server-handled queries. Set once at
/// server startup (`run_with_config`) from the `--databricks` flag. The
/// embedded/`cg` paths set the dialect per-query (via `set_current_dialect`)
/// and never touch this, so it stays at the ClickHouse default for them.
static SERVER_DIALECT: std::sync::OnceLock<SqlDialect> = std::sync::OnceLock::new();

/// Set the process-wide default dialect for server-handled queries. Idempotent
/// (first write wins); call once during server init before serving requests.
pub fn set_server_dialect(dialect: SqlDialect) {
    let _ = SERVER_DIALECT.set(dialect);
}

/// The process-wide server dialect, or [`SqlDialect::ClickHouse`] if unset.
fn server_dialect() -> SqlDialect {
    SERVER_DIALECT.get().copied().unwrap_or_default()
}

/// Process-wide Neo4j-compatibility flag for server-handled queries. Set once at
/// server startup from `--neo4j-compat-mode`. When on, property resolution treats
/// a property that the schema does not declare as NULL (Neo4j schemaless
/// semantics) instead of an identity-mapped column that would error at the
/// database. Off (the default) preserves the identity-mapping wide-table path.
/// Embedded/`cg` never set this, so they keep the default behavior.
static SERVER_NEO4J_COMPAT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Enable process-wide Neo4j-compat property resolution. Idempotent (first write
/// wins); call once during server init before serving requests.
pub fn set_server_neo4j_compat(enabled: bool) {
    let _ = SERVER_NEO4J_COMPAT.set(enabled);
}

/// Whether the server is running in Neo4j-compat mode (default false).
pub fn server_neo4j_compat() -> bool {
    SERVER_NEO4J_COMPAT.get().copied().unwrap_or(false)
}

impl QueryContext {
    /// Create a new query context with schema name.
    ///
    /// Seeds the dialect from the process-wide server default
    /// ([`set_server_dialect`]) so every server-handled query (HTTP, Bolt,
    /// export) targets the right SQL dialect. Without this, server queries
    /// always rendered ClickHouse SQL even in `--databricks` mode. Callers that
    /// need a different dialect (embedded) override it via `set_current_dialect`
    /// inside the `with_query_context` scope.
    pub fn new(schema_name: Option<String>) -> Self {
        Self {
            schema_name,
            dialect: server_dialect(),
            ..Default::default()
        }
    }

    /// Create an empty query context (for testing or when schema is determined later)
    pub fn empty() -> Self {
        Self::default()
    }
}

// The single task-local storage for query context
tokio::task_local! {
    /// Task-local query context - isolated per async task
    /// MUST be accessed within a `.scope()` wrapper
    static QUERY_CONTEXT: RefCell<QueryContext>;
}

/// Execute an async operation with query context
/// This wraps the operation in a task_local `.scope()` so the context is available
///
/// # Example
/// ```ignore
/// with_query_context(QueryContext::new(Some("myschema".to_string())), async {
///     // Context is available via get_* functions
///     let schema = get_current_schema_name();
///     process_query().await
/// }).await
/// ```
pub async fn with_query_context<F, R>(context: QueryContext, f: F) -> R
where
    F: Future<Output = R>,
{
    QUERY_CONTEXT.scope(RefCell::new(context), f).await
}

// ============================================================================
// DIALECT ACCESSORS
// ============================================================================

/// Get the SQL dialect for the current query.
/// Returns [`SqlDialect::ClickHouse`] when called outside a task-local
/// scope (e.g. unit tests), matching the historical hard-coded behavior.
pub fn get_current_dialect() -> SqlDialect {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().dialect)
        .unwrap_or_default()
}

/// Set the SQL dialect for the current query (typically once at entry).
pub fn set_current_dialect(dialect: SqlDialect) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().dialect = dialect;
    });
}

// ============================================================================
// SCHEMA NAME ACCESSORS
// ============================================================================

/// Get the current query's schema name
pub fn get_current_schema_name() -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().schema_name.clone())
        .ok()
        .flatten()
}

/// Set the schema name for the current query
/// (Usually set once at context creation, but can be updated if needed)
pub fn set_current_schema_name(name: Option<String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().schema_name = name;
    });
}

/// Clear the schema name (for cleanup at query exit)
pub fn clear_current_schema_name() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().schema_name = None;
    });
}

/// Get the resolved GraphSchema for the current query.
/// Returns None if no schema was set (e.g., outside query context).
pub fn get_current_schema() -> Option<Arc<GraphSchema>> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().schema.clone())
        .ok()
        .flatten()
}

/// Get the current schema, falling back to GLOBAL_SCHEMAS if not in a task-local context.
/// This is needed for unit tests that set up GLOBAL_SCHEMAS directly without task-local scope.
pub fn get_current_schema_with_fallback() -> Option<Arc<GraphSchema>> {
    // Try task-local first
    if let Some(schema) = get_current_schema() {
        return Some(schema);
    }
    // Fallback to GLOBAL_SCHEMAS for backward compatibility (tests)
    let schema_name = get_current_schema_name().unwrap_or_else(|| "default".to_string());
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get(&schema_name) {
                return Some(Arc::new(schema.clone()));
            }
            if let Some(schema) = schemas.values().next() {
                return Some(Arc::new(schema.clone()));
            }
        }
    }
    None
}

/// Set the resolved GraphSchema for the current query.
/// Should be called once at query entry after resolving the schema name.
pub fn set_current_schema(schema: Arc<GraphSchema>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().schema = Some(schema);
    });
}

// ============================================================================
// TABLE STATS ACCESSORS (S1 stats-informed planning)
// ============================================================================

/// Attach a per-table row-count snapshot for the current query. Called once at
/// query entry by the server, ONLY when `CLICKGRAPH_STATS_ENABLED=true` and a
/// stats fetch has succeeded. No-op outside a task-local scope.
pub fn set_current_table_stats(stats: Arc<crate::graph_catalog::table_stats::TableStatsSnapshot>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().table_stats = Some(stats);
    });
}

/// The current query's table-stats snapshot, or `None` when stats-informed
/// planning is disabled/unavailable (the default). Consumers must treat `None`
/// (and any per-table miss) as "fall back to the stats-less heuristic" —
/// stats influence ordering only, never row membership (PRIORITIES.md §1.7).
pub fn get_current_table_stats(
) -> Option<Arc<crate::graph_catalog::table_stats::TableStatsSnapshot>> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().table_stats.clone())
        .ok()
        .flatten()
}

/// Attach the process-wide stats cache's current snapshot (covering `schema`'s
/// databases) to the task-local context, refreshing the cache first if its TTL
/// elapsed. No-ops — leaving the planner stats-less — when the cache was never
/// installed (`CLICKGRAPH_STATS_ENABLED` off, embedded/warehouse modes) or no
/// fetch has ever succeeded. Call once per query, after `set_current_schema`.
pub async fn attach_current_table_stats(schema: &GraphSchema) {
    if let Some(cache) = crate::server::GLOBAL_TABLE_STATS.get() {
        let dbs = crate::graph_catalog::table_stats::schema_databases(schema);
        if let Some(snapshot) = cache.snapshot(&dbs).await {
            set_current_table_stats(snapshot);
        }
    }
}

// ============================================================================
// CTE COLUMN REGISTRY ACCESSORS
// ============================================================================

// ============================================================================
// DENORMALIZED ALIAS ACCESSORS
// ============================================================================

/// Register an alias mapping for denormalized edges
/// Maps target_node_alias → edge_alias (e.g., "d" → "r2")
pub fn register_denormalized_alias(target_node_alias: &str, edge_alias: &str) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .denormalized_aliases
            .insert(target_node_alias.to_string(), edge_alias.to_string());
    });
}

/// Look up the edge alias for a target node alias (if denormalized)
pub fn get_denormalized_alias_mapping(target_node_alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            ctx.borrow()
                .denormalized_aliases
                .get(target_node_alias)
                .cloned()
        })
        .ok()
        .flatten()
}

/// Clear all denormalized alias mappings
pub fn clear_denormalized_aliases() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().denormalized_aliases.clear();
    });
}

// ============================================================================
// RELATIONSHIP COLUMNS ACCESSORS
// ============================================================================

/// Set relationship columns for the current query
pub fn set_relationship_columns(columns: HashMap<String, (String, String)>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().relationship_columns = columns;
    });
}

/// Get relationship columns for an alias
pub fn get_relationship_columns(alias: &str) -> Option<(String, String)> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().relationship_columns.get(alias).cloned())
        .ok()
        .flatten()
}

/// Clear relationship columns
pub fn clear_relationship_columns() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().relationship_columns.clear();
    });
}

// ============================================================================
// CTE PROPERTY MAPPINGS ACCESSORS
// ============================================================================

/// Set CTE property mappings for the current query
/// Deep-merges new mappings with existing ones to preserve prior entries
pub fn set_cte_property_mappings(mappings: HashMap<String, HashMap<String, String>>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        let existing = &mut ctx.cte_property_mappings;

        // Deep-merge: for each CTE alias, merge its property mappings
        for (cte_alias, new_props) in mappings {
            let entry = existing.entry(cte_alias).or_default();
            // New or updated properties overwrite existing ones; unrelated ones are preserved
            for (prop, column) in new_props {
                entry.insert(prop, column);
            }
        }
    });
}

/// Get a CTE property mapping
pub fn get_cte_property_mapping(cte_alias: &str, property: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            ctx.borrow()
                .cte_property_mappings
                .get(cte_alias)
                .and_then(|props| props.get(property).cloned())
        })
        .ok()
        .flatten()
}

/// Get all CTE property mappings for a given alias
/// Returns Vec<(property_name, cte_column_name)> sorted by property name for deterministic order
pub fn get_all_cte_properties(cte_alias: &str) -> Vec<(String, String)> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            ctx.borrow()
                .cte_property_mappings
                .get(cte_alias)
                .map(|props| {
                    let mut entries: Vec<(String, String)> =
                        props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// Clear CTE property mappings
pub fn clear_cte_property_mappings() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().cte_property_mappings.clear();
    });
}

// ============================================================================
// MULTI-TYPE VLP ALIASES ACCESSORS
// ============================================================================

/// Get the current multi-type VLP aliases map.
pub fn get_multi_type_vlp_aliases() -> HashMap<String, String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().multi_type_vlp_aliases.clone())
        .unwrap_or_default()
}

/// Set multi-type VLP aliases for the current query
pub fn set_multi_type_vlp_aliases(aliases: HashMap<String, String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().multi_type_vlp_aliases = aliases;
    });
}

/// Register a relationship CTE name immediately when created
/// This allows nested rendering to look up CTE names deterministically
/// Maps alias (e.g., "r") → cte_name (e.g., "vlp_multi_type_a_t1")
pub fn register_relationship_cte_name(alias: &str, cte_name: &str) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .multi_type_vlp_aliases
            .insert(alias.to_string(), cte_name.to_string());
    });
}

/// #466: record that the current render plan's FROM/JOIN references a
/// `pattern_union_*` CTE. Called by from_builder/join_builder at the exact
/// points where they emit the CTE reference (before filter extraction runs).
pub fn register_pattern_union_in_scope(cte_name: &str) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .pattern_union_scope_ctes
            .insert(cte_name.to_string());
    });
}

/// #466: is this `pattern_union_*` CTE referenced by the current plan's
/// FROM/JOIN? The outer-WHERE builder may only skip a node-property conjunct
/// when this returns true — the CTE genuinely applies the conjunct
/// per-branch; otherwise the conjunct must stay in the outer WHERE (never
/// skip-without-apply). Returns false outside a task-local scope — the
/// conservative direction (keep the filter).
pub fn is_pattern_union_in_scope(cte_name: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().pattern_union_scope_ctes.contains(cte_name))
        .unwrap_or(false)
}

/// Get a relationship CTE name by alias
/// Returns None if alias not registered (use for lookup-only, no recomputation)
pub fn get_relationship_cte_name(alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().multi_type_vlp_aliases.get(alias).cloned())
        .ok()
        .flatten()
}

/// #623: mark an exact-bound VLP (by its relationship alias) as rerouted from
/// the flat self-join chain to the recursive-CTE path because it is adjacent to
/// another hop. Called by the analyzer's `should_skip_for_vlp` decision. A
/// no-op outside a task-local scope (all query processing is wrapped in
/// `with_query_context`, including the embedded/`cg` path).
pub fn register_adjacent_exact_vlp_reroute(rel_alias: &str) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .adjacent_exact_vlp_reroute_aliases
            .insert(rel_alias.to_string());
    });
}

/// #623: is this exact VLP (by relationship alias) rerouted to the recursive
/// CTE (adjacent to another hop)? Read by the render gates to treat it as
/// non-fixed-length. Returns false outside a task-local scope — the
/// conservative direction (keep the historical flat path for the standalone
/// case, which is byte-identical and correct).
pub fn is_adjacent_exact_vlp_reroute(rel_alias: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| {
            ctx.borrow()
                .adjacent_exact_vlp_reroute_aliases
                .contains(rel_alias)
        })
        .unwrap_or(false)
}

/// #623: record the exact hop count `N` of a fixed `*N..N` VLP that declares a
/// path variable, keyed by that path variable. Called from `build_vlp_context`.
/// No-op outside a task-local scope.
pub fn register_vlp_exact_path_hops(path_var: &str, hops: u32) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .vlp_exact_path_hops
            .insert(path_var.to_string(), hops);
    });
}

/// #623: the recorded exact hop count for a path variable's fixed VLP, if any.
/// Read by the `length(p)` resolver so a flat `*N..N` VLP reports `N`, not the
/// `joins/2` heuristic. None outside a task-local scope or for non-VLP paths.
pub fn get_vlp_exact_path_hops(path_var: &str) -> Option<u32> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().vlp_exact_path_hops.get(path_var).copied())
        .ok()
        .flatten()
}

/// Register a VLP CTE's outer-query alias (e.g., "vlp_u1_u2" → "vt0")
///
/// NOTE: Currently intentionally not called. The render phase (select_builder,
/// to_sql_query, VLPExprRewriter) still uses hardcoded VLP_CTE_FROM_ALIAS ("t")
/// for expression rendering. Until render-phase code is updated for per-VLP aliases,
/// calling this would cause FROM alias / expression reference mismatches.
/// See TODO(multi-vlp) in cte_extraction.rs.
#[allow(dead_code)]
pub fn register_vlp_cte_outer_alias(cte_name: &str, vlp_alias: &str) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut()
            .vlp_cte_outer_aliases
            .insert(cte_name.to_string(), vlp_alias.to_string());
    });
}

/// Get the outer-query alias for a VLP CTE by CTE name
/// Returns None if not registered, falling back to VLP_CTE_FROM_ALIAS
pub fn get_vlp_cte_outer_alias(cte_name: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().vlp_cte_outer_aliases.get(cte_name).cloned())
        .ok()
        .flatten()
}

/// Check if an alias is a multi-type VLP endpoint
pub fn is_multi_type_vlp_alias(alias: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().multi_type_vlp_aliases.contains_key(alias))
        .unwrap_or(false)
}

/// Clear multi-type VLP aliases
pub fn clear_multi_type_vlp_aliases() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().multi_type_vlp_aliases.clear();
    });
}

// ============================================================================
// VARIABLE REGISTRY ACCESSORS
// ============================================================================

/// Set the current variable registry for SQL rendering
pub fn set_current_variable_registry(
    registry: std::sync::Arc<crate::query_planner::typed_variable::VariableRegistry>,
) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().current_variable_registry = Some(registry);
    });
}

/// Record the set of CTE columns that hold array/collection values.
pub fn set_array_cte_columns(columns: std::collections::HashSet<String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().array_cte_columns = columns;
    });
}

/// True if `column` is a known array/collection-valued CTE column.
pub fn is_array_cte_column(column: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().array_cte_columns.contains(column))
        .unwrap_or(false)
}

/// Snapshot the current array-CTE-column set (for save/restore around re-entrant
/// `render_plan_to_sql` calls so a nested sub-plan doesn't clobber the parent's).
pub fn get_array_cte_columns() -> std::collections::HashSet<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().array_cte_columns.clone())
        .unwrap_or_default()
}

/// Get the current variable registry (for property resolution during SQL rendering)
pub fn get_current_variable_registry(
) -> Option<std::sync::Arc<crate::query_planner::typed_variable::VariableRegistry>> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().current_variable_registry.clone())
        .ok()
        .flatten()
}

/// Resolve a property using the current variable registry (if available).
/// This avoids cloning the entire registry; resolution happens inside the task-local borrow.
pub fn resolve_with_current_registry(
    alias: &str,
    property: &str,
) -> Option<crate::query_planner::typed_variable::ResolvedProperty> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let ctx = ctx.borrow();
            let registry = ctx.current_variable_registry.as_ref()?;
            let schema = ctx.schema.as_ref()?;
            Some(registry.resolve(alias, property, schema))
        })
        .ok()
        .flatten()
}

/// Global source of fresh, never-repeating `cte_scope_for_correlation`
/// generation ids. Process-wide (not per-task) is fine: uniqueness across
/// concurrently-active generations is all that's required, and using a
/// simple atomic avoids needing per-task counter state. Starts at 1 — 0 is
/// reserved as the "no generation active" sentinel (see
/// `cte_scope_generation` field doc).
static CTE_SCOPE_GENERATION_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// Enter a new CTE-scope-correlation "generation". Call at the very top of
/// `build_chained_with_match_cte_plan`, for BOTH top-level invocations (one
/// per independent subplan: a UNION arm, a cartesian-product side, the whole
/// query if it has no such branching, ...) and its own recursive re-entry
/// (nested WITH-clause bodies within the same subplan).
///
/// Returns the previous generation, which the caller MUST restore via
/// `restore_cte_scope_generation` on every exit path — use a `Drop` guard,
/// since the caller has many early `?` returns.
///
/// This is what makes `cte_scope_for_correlation` safe to consult across
/// independent subplans without having to find and instrument every single
/// boundary where one might start (UNION arms, cartesian sides, OPTIONAL
/// MATCH branches, and whatever else might be added later): an entry is only
/// ever considered live while the exact `build_chained_with_match_cte_plan`
/// invocation that wrote it is still the innermost one on the stack. The
/// moment that invocation returns (for ANY reason — its subplan finished
/// rendering, an independent sibling subplan starts next, doesn't matter),
/// its generation is retired: no future write can produce that generation
/// value again (the counter is monotonic), so no future read can match it.
///
/// A fresh top-level entry (previous generation was the `0` sentinel — i.e.
/// no invocation is currently active, so this can't be a nested recursive
/// call) also garbage-collects every entry left over from previously
/// finished generations: they can never be resolved again, so there's no
/// reason to let the map grow across a query with many independent subplans.
///
/// This is a deliberate design choice, not an oversight: `cte_property_mappings`,
/// `relationship_columns`, and `denormalized_aliases` each have a matching
/// `clear_*` function with zero callers anywhere in the codebase (a
/// separate, pre-existing gap — those maps are simply never cleared today).
/// `cte_scope_for_correlation` avoids joining that same pattern by folding
/// its cleanup into generation entry above, rather than adding another
/// `clear_cte_scope_for_correlation()` that would need a caller to remember
/// to invoke (and could easily end up with zero callers too).
pub fn enter_cte_scope_generation() -> u64 {
    let new_gen = CTE_SCOPE_GENERATION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    QUERY_CONTEXT
        .try_with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let prev = ctx.cte_scope_generation;
            if prev == 0 {
                ctx.cte_scope_for_correlation.clear();
            }
            ctx.cte_scope_generation = new_gen;
            prev
        })
        .unwrap_or(0)
}

/// Restore the CTE-scope-correlation generation to `prev` (the value
/// `enter_cte_scope_generation` returned). See that function's doc.
pub fn restore_cte_scope_generation(prev: u64) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().cte_scope_generation = prev;
    });
}

/// RAII guard for `cte_scope_for_correlation`'s generation scoping (see
/// `enter_cte_scope_generation`'s doc). Construct one at the start of any
/// independent subplan's rendering — `build_chained_with_match_cte_plan`'s
/// own entry (top-level AND its own recursive re-entry), and every place a
/// *sibling* subplan gets rendered on its own (a UNION branch, a
/// cartesian-product side, ...): those siblings run to completion one after
/// another within the SAME task/query, so without an explicit fresh
/// generation per sibling, a later one reusing an earlier one's Cypher alias
/// name could otherwise still see that earlier sibling's now-stale
/// `cte_scope_for_correlation` entries (the outer generation they'd both
/// inherit by default hasn't changed just because a sibling finished).
///
/// Dropping the guard restores the previous generation — use it for the
/// narrowest scope that renders just that one subplan/branch, not a whole
/// multi-branch loop, so each sibling truly gets an independent generation.
#[must_use]
pub struct CteScopeGenerationGuard {
    prev_generation: u64,
    /// #596: snapshot of `exists_outer_aliases` at guard construction, restored
    /// on drop so each independent sibling subplan (UNION arm, cartesian side)
    /// starts EXISTS anchor-classification from the enclosing scope's aliases
    /// only — never accumulating a prior sibling's bound aliases.
    prev_exists_outer_aliases: HashSet<String>,
}

impl CteScopeGenerationGuard {
    pub fn enter() -> Self {
        let prev_exists_outer_aliases = snapshot_exists_outer_aliases();
        Self {
            prev_generation: enter_cte_scope_generation(),
            prev_exists_outer_aliases,
        }
    }
}

impl Drop for CteScopeGenerationGuard {
    fn drop(&mut self) {
        restore_cte_scope_generation(self.prev_generation);
        restore_exists_outer_aliases(std::mem::take(&mut self.prev_exists_outer_aliases));
    }
}

/// Publish a WITH-CTE-exported alias's current CTE scope (FROM alias +
/// Cypher-property → CTE-column mapping) for EXISTS correlation-variable
/// resolution, tagged with the CURRENTLY ACTIVE generation (see
/// `enter_cte_scope_generation`). See the `cte_scope_for_correlation` field
/// doc for why this exists as a separate channel from
/// `current_variable_registry`, and for why the generation tag is needed.
pub fn set_cte_scope_for_correlation(
    alias: String,
    sql_alias: String,
    property_mapping: HashMap<String, String>,
) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        let generation = ctx.cte_scope_generation;
        ctx.cte_scope_for_correlation
            .insert(alias, (generation, sql_alias, property_mapping));
    });
}

/// Resolve `alias`'s CTE-scoped SQL reference for `property` (a Cypher
/// property name), if `alias` is currently a WITH-CTE-exported variable with
/// that property present AND the entry's generation matches the currently
/// active one (i.e. it was published by the `build_chained_with_match_cte_plan`
/// invocation that's still in progress right now, not by an earlier,
/// already-finished, independent subplan that happened to reuse the same
/// alias name — see `enter_cte_scope_generation`). Returns `(sql_alias,
/// cte_column)`, e.g. `("a_cnt", "p1_a_user_id")`. Returns `None` if `alias`
/// isn't CTE-scoped in the current subplan (fresh MATCH — the common case),
/// the property isn't in its mapping, or the entry is stale.
pub fn resolve_correlation_cte_column(alias: &str, property: &str) -> Option<(String, String)> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let ctx = ctx.borrow();
            let (generation, sql_alias, mapping) = ctx.cte_scope_for_correlation.get(alias)?;
            if *generation != ctx.cte_scope_generation {
                return None;
            }
            let column = mapping.get(property)?;
            Some((sql_alias.clone(), column.clone()))
        })
        .ok()
        .flatten()
}

/// Get the primary node label for a given Cypher alias.
///
/// Checks, in priority order:
/// 1. The current variable registry (populated for WITH-clause queries)
/// 2. The alias-label map built from FROM/JOIN tables at render time
///
/// Returns None if alias is not known or not a node variable.
pub fn get_node_label_for_alias(alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let ctx = ctx.borrow();
            // First: alias→label map built from actual FROM/JOIN table names at render time.
            // This is ground truth for the current SQL branch — the alias IS this table.
            // Checked first because the variable registry may carry stale labels from a
            // different branch (e.g., VLP context has `b → Post`, but FOLLOWS branch has
            // `b → social.users → User`).
            if let Some(label) = ctx.alias_label_map.get(alias) {
                return Some(label.clone());
            }
            // Second: variable registry (populated for queries with WITH clauses)
            if let Some(registry) = ctx.current_variable_registry.as_ref() {
                if let Some(var) = registry.lookup(alias) {
                    if let Some(label) = var.primary_label_or_type() {
                        return Some(label.to_string());
                    }
                }
            }
            None
        })
        .ok()
        .flatten()
}

/// #596: Merge (union) a set of outer-scope-bound Cypher aliases into
/// `exists_outer_aliases`. Called at the top of `to_render_plan` /
/// `build_chained_with_match_cte_plan` with the current plan's live
/// node/relationship aliases, before the WHERE predicate (and any EXISTS SQL
/// baked from it) is rendered. Union semantics — never removes — because an
/// alias bound in an enclosing scope stays "outer" to any deeper nested EXISTS.
pub fn merge_exists_outer_aliases<I: IntoIterator<Item = String>>(aliases: I) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().exists_outer_aliases.extend(aliases);
    });
}

/// #596: Is `alias` bound in the outer (enclosing) query scope? Used by
/// `generate_exists_sql` to classify each EXISTS relationship-pattern endpoint
/// as an outer anchor vs. a fresh inner variable.
pub fn is_exists_outer_alias(alias: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().exists_outer_aliases.contains(alias))
        .unwrap_or(false)
}

/// #596: Snapshot the current `exists_outer_aliases` set. Paired with
/// [`restore_exists_outer_aliases`] by `CteScopeGenerationGuard` to isolate the
/// set across independent sibling subplans (UNION arms, cartesian sides): merge
/// semantics correctly propagate an enclosing scope's aliases INTO a nested
/// EXISTS, but a *sibling* arm that binds the same alias name must NOT leak it
/// into the next arm's EXISTS correlation (that treats a sibling's fresh inner
/// var as a bogus outer anchor → out-of-scope column → Code 47). Same
/// cross-subplan leak class #593/#594 fixed for `cte_scope_for_correlation`.
pub fn snapshot_exists_outer_aliases() -> HashSet<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().exists_outer_aliases.clone())
        .unwrap_or_default()
}

/// #596: Restore `exists_outer_aliases` to a prior snapshot (see
/// [`snapshot_exists_outer_aliases`]).
pub fn restore_exists_outer_aliases(snapshot: HashSet<String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().exists_outer_aliases = snapshot;
    });
}

/// #596: Is `alias` a WITH-barrier-crossing CTE-scoped correlation variable in
/// the current generation? Complements `is_exists_outer_alias`: after a WITH
/// barrier the outer anchor is exported through a CTE (and skipped by
/// `collect_live_table_aliases`, which ignores `ViewScan`), so its outer-ness
/// is recorded here instead. Mirrors `resolve_correlation_cte_column`'s
/// generation-guarded lookup but is property-agnostic (endpoint classification
/// only, not id rendering).
pub fn is_correlation_cte_alias(alias: &str) -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let ctx = ctx.borrow();
            match ctx.cte_scope_for_correlation.get(alias) {
                Some((generation, _, _)) => *generation == ctx.cte_scope_generation,
                None => false,
            }
        })
        .unwrap_or(false)
}

/// Set the alias→label mapping derived from the render plan's FROM/JOIN tables.
pub fn set_alias_label_map(map: HashMap<String, String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().alias_label_map = map;
    });
}

/// Clear the current variable registry
pub fn clear_current_variable_registry() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().current_variable_registry = None;
    });
}

// ============================================================================
// BULK OPERATIONS
// ============================================================================

/// Set all render contexts at once (for render phase entry)
pub fn set_all_render_contexts(
    relationship_columns: HashMap<String, (String, String)>,
    cte_mappings: HashMap<String, HashMap<String, String>>,
    multi_type_aliases: HashMap<String, String>,
    cte_alias_to_cte_name: HashMap<String, String>,
) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        ctx.relationship_columns = relationship_columns;
        ctx.cte_property_mappings = cte_mappings;
        ctx.multi_type_vlp_aliases = multi_type_aliases;
        ctx.cte_alias_to_cte_name = cte_alias_to_cte_name;
    });
}

/// Clear all render contexts (for render phase exit)
pub fn clear_all_render_contexts() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        ctx.relationship_columns.clear();
        ctx.cte_property_mappings.clear();
        ctx.multi_type_vlp_aliases.clear();
        ctx.cte_alias_to_cte_name.clear();
        ctx.all_cte_names.clear();
        ctx.weight_cte_config = None;
    });
}

/// Get the set of all known CTE names in the current query
pub fn get_all_cte_names() -> HashSet<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().all_cte_names.clone())
        .unwrap_or_default()
}

/// Set the set of all CTE names for the current query
pub fn set_all_cte_names(names: HashSet<String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().all_cte_names = names;
    });
}

/// Look up the CTE name for a given SQL alias (FROM/JOIN alias that references a CTE)
pub fn get_cte_name_for_alias(alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().cte_alias_to_cte_name.get(alias).cloned())
        .ok()
        .flatten()
}

/// Set the CTE alias mapping for the current rendering scope.
/// Returns the previous mapping so it can be restored after rendering.
pub fn set_cte_alias_scope(mapping: HashMap<String, String>) -> HashMap<String, String> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            std::mem::replace(&mut ctx.cte_alias_to_cte_name, mapping)
        })
        .unwrap_or_default()
}

/// Set weight CTE config for weighted shortest path
pub fn set_weight_cte_config(config: crate::clickhouse_query_generator::WeightCteConfig) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().weight_cte_config = Some(config);
    });
}

/// Get weight CTE config for weighted shortest path
pub fn get_weight_cte_config() -> Option<crate::clickhouse_query_generator::WeightCteConfig> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().weight_cte_config.clone())
        .ok()
        .flatten()
}

// ============================================================================
// BRANCH CONTEXT SNAPSHOT — for per-SQL-scope isolation
// ============================================================================

/// Snapshot of branch-scoped rendering context — the two fields that vary per SQL scope.
/// Use snapshot_branch_context() / restore_branch_context() at every SQL branch boundary.
#[derive(Clone, Default)]
pub struct BranchContextSnapshot {
    pub multi_type_vlp_aliases: HashMap<String, String>,
    pub alias_label_map: HashMap<String, String>,
}

/// Save the current branch-scoped rendering context.
/// Call this before entering a new SQL scope (UNION branch, CTE body, outer SELECT).
pub fn snapshot_branch_context() -> BranchContextSnapshot {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let ctx = ctx.borrow();
            BranchContextSnapshot {
                multi_type_vlp_aliases: ctx.multi_type_vlp_aliases.clone(),
                alias_label_map: ctx.alias_label_map.clone(),
            }
        })
        .unwrap_or_default()
}

/// Restore a previously saved branch-scoped rendering context.
/// Call this after a SQL scope finishes rendering.
pub fn restore_branch_context(snapshot: BranchContextSnapshot) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        ctx.multi_type_vlp_aliases = snapshot.multi_type_vlp_aliases;
        ctx.alias_label_map = snapshot.alias_label_map;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_context_new_defaults_to_clickhouse_when_server_dialect_unset() {
        // The embedded/`cg` paths never call `set_server_dialect`, so a fresh
        // context must default to ClickHouse (the historical behavior). NOTE:
        // we deliberately do NOT call `set_server_dialect` here — it writes a
        // process-wide OnceLock that would leak into sibling tests in this
        // binary. The Databricks path (server sets the global → `new()` reads
        // it) is covered end-to-end by the DeltaGraph/zeta transport tests.
        assert_eq!(QueryContext::new(None).dialect, SqlDialect::ClickHouse);
    }

    #[tokio::test]
    async fn test_query_context_isolation() {
        // Context 1
        let result1 = with_query_context(QueryContext::new(Some("schema1".to_string())), async {
            get_current_schema_name()
        })
        .await;
        assert_eq!(result1, Some("schema1".to_string()));

        // Context 2 - should be isolated
        let result2 = with_query_context(QueryContext::new(Some("schema2".to_string())), async {
            get_current_schema_name()
        })
        .await;
        assert_eq!(result2, Some("schema2".to_string()));
    }

    #[tokio::test]
    async fn test_denormalized_aliases() {
        with_query_context(QueryContext::empty(), async {
            register_denormalized_alias("d", "r2");
            assert_eq!(get_denormalized_alias_mapping("d"), Some("r2".to_string()));
            assert_eq!(get_denormalized_alias_mapping("unknown"), None);
        })
        .await;
    }
}
