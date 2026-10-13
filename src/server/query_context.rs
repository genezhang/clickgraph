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
use std::sync::atomic::{AtomicU32, Ordering};
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

    /// #969: types of `reduce()` lambda binders in scope while the CURRENT
    /// reduce body is being rendered — `accumulator → type of initial_value`,
    /// `variable → element type of list`. `infer_render_type` consults this so a
    /// bare binder reference (`s`/`x`, which otherwise types as unknown) resolves
    /// to its real type. That lets the string-`+`→`concat` detector (#871) fire
    /// for `reduce(s = '', x IN ['a','b','c'] | s + x)` — otherwise the body
    /// renders a numeric `+` on two strings → ClickHouse Code 43. Pushed (saving
    /// the prior map) around the body render at each `ReduceExpr` site and
    /// restored after, so nested reduces scope correctly. Empty outside any
    /// reduce → unknown binder stays `None` (conservative, unchanged behavior).
    pub reduce_binder_types:
        HashMap<String, crate::sql_generator::emitters::clickhouse::type_inference::RenderType>,

    /// Alias → node label mapping built from the render plan's FROM/JOIN tables
    /// (type inference reads a column's declared type through it).
    pub alias_label_map: HashMap<String, String>,

    /// Names of CTE output columns that hold an array/collection value (produced
    /// by a `collect`/`groupArray` aggregate or a list literal). Set once during
    /// render_plan_to_sql(). Lets the Databricks `size()` render dispatch pick
    /// Spark `size` (collection) vs `length` (string) when the argument is a
    /// carried-forward collection column (e.g. `WITH collect(post) AS posts`).
    pub array_cte_columns: HashSet<String>,

    /// S1 stats-informed planning: per-table row-count snapshot for this query
    /// (`docs/design/STATS_PLANNING.md`). Attached at query entry by the server
    /// when `CLICKGRAPH_STATS_ENABLED=true` and a fetch has succeeded; `None`
    /// everywhere else — sql_only, embedded, tests, flag off — which keeps the
    /// planner byte-identical to the stats-less engine (the ONLY consumer is
    /// `select_anchor`'s ordering-only ranking; guardrail: stats never change
    /// row membership, PRIORITIES.md §1.7).
    pub table_stats: Option<Arc<crate::graph_catalog::table_stats::TableStatsSnapshot>>,

    /// Last generated anonymous alias number (`t{N}`) and CTE number
    /// (`cte{N}`) for THIS query. Per query, so that one request resetting its
    /// counters can never rewind another concurrent request's counters
    /// mid-translation (which re-issues an alias already used in that query).
    pub alias_counter: u32,
    pub cte_counter: u32,
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

/// Whether the caller is inside a query context ([`with_query_context`] or
/// [`with_query_context_sync`]).
pub fn has_query_context() -> bool {
    QUERY_CONTEXT.try_with(|_| ()).is_ok()
}

/// Synchronous counterpart of [`with_query_context`]: run `f` with `context`
/// as the task-local query context (for synchronous entry points such as
/// `translate::translate_read` called by library code without one).
pub fn with_query_context_sync<R>(context: QueryContext, f: impl FnOnce() -> R) -> R {
    QUERY_CONTEXT.sync_scope(RefCell::new(context), f)
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

/// Process-global fallback for generated aliases, used only outside a query
/// context (unit tests). Inside one, the counter is per query
/// (`QueryContext::alias_counter`).
static ALIAS_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Generate a simple, human-readable alias for anonymous nodes/edges.
/// Returns "t1", "t2", "t3", etc. Much easier to read than UUID hex strings!
pub fn generate_id() -> String {
    let n = next_query_counter(QueryCounter::Alias)
        .unwrap_or_else(|| ALIAS_COUNTER.fetch_add(1, Ordering::SeqCst));
    format!("t{}", n)
}

/// Restart the generated-alias and CTE counters for deterministic SQL.
/// Called at the start of each translation (`translate::translate_read`).
///
/// Inside a query context this resets only THAT query's counters. The
/// process-global fallbacks are reset only when there is no context: a global
/// reset used to rewind the counters of other requests being translated
/// concurrently, so the same `t{N}` could be issued twice in one query.
pub fn reset_all_counters() {
    if !crate::server::query_context::reset_query_counters() {
        ALIAS_COUNTER.store(1, Ordering::SeqCst);
        CTE_COUNTER.store(1, Ordering::SeqCst);
    }
}

/// Process-global fallback for CTE names outside a query context.
static CTE_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Generate a simple, human-readable CTE name.
/// Returns "cte1", "cte2", "cte3", etc. Much shorter than UUID strings!
pub fn generate_cte_id() -> String {
    let n = next_query_counter(QueryCounter::Cte)
        .unwrap_or_else(|| CTE_COUNTER.fetch_add(1, Ordering::SeqCst));
    format!("cte{}", n)
}

/// Which per-query counter to advance (see [`next_query_counter`]).
#[derive(Debug, Clone, Copy)]
pub enum QueryCounter {
    Alias,
    Cte,
}

/// Advance this query's counter and return the new value (1, 2, ...), or
/// `None` outside a query context (callers then use their process-global
/// fallback, as unit tests without a context always did).
pub fn next_query_counter(counter: QueryCounter) -> Option<u32> {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let slot = match counter {
                QueryCounter::Alias => &mut ctx.alias_counter,
                QueryCounter::Cte => &mut ctx.cte_counter,
            };
            *slot += 1;
            *slot
        })
        .ok()
}

/// Restart this query's counters. Returns false outside a query context.
pub fn reset_query_counters() -> bool {
    QUERY_CONTEXT
        .try_with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            ctx.alias_counter = 0;
            ctx.cte_counter = 0;
        })
        .is_ok()
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

// ============================================================================
// OWN-TABLE JOIN REQUEST ACCESSORS (#1006)
// ============================================================================

// ============================================================================
// RELATIONSHIP COLUMNS ACCESSORS
// ============================================================================

/// #969: install the reduce-lambda binder-type map for the CURRENT reduce body
/// render, returning the PRIOR map so the caller can restore it after (so nested
/// reduces scope correctly). See [`QueryContext::reduce_binder_types`].
pub fn push_reduce_binder_types(
    types: HashMap<String, crate::sql_generator::emitters::clickhouse::type_inference::RenderType>,
) -> HashMap<String, crate::sql_generator::emitters::clickhouse::type_inference::RenderType> {
    QUERY_CONTEXT
        .try_with(|ctx| std::mem::replace(&mut ctx.borrow_mut().reduce_binder_types, types))
        .unwrap_or_default()
}

/// #969: restore a reduce binder-type map saved by [`push_reduce_binder_types`].
pub fn restore_reduce_binder_types(
    prev: HashMap<String, crate::sql_generator::emitters::clickhouse::type_inference::RenderType>,
) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().reduce_binder_types = prev;
    });
}

/// #969: the declared type of a `reduce()` lambda binder currently in scope, if
/// any. `None` outside a reduce body (map empty) → conservative, unchanged.
pub fn get_reduce_binder_type(
    name: &str,
) -> Option<crate::sql_generator::emitters::clickhouse::type_inference::RenderType> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().reduce_binder_types.get(name).copied())
        .ok()
        .flatten()
}

// ============================================================================
// MULTI-TYPE VLP ALIASES ACCESSORS
// ============================================================================

// ============================================================================
// VARIABLE REGISTRY ACCESSORS
// ============================================================================

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

/// Get the primary node label for a given Cypher alias.
///
/// It reads the alias-label map built from the printed plan's FROM/JOIN
/// tables (`set_alias_label_map`); `None` when the alias is not a node's.
pub fn get_node_label_for_alias(alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().alias_label_map.get(alias).cloned())
        .ok()
        .flatten()
}

/// Set the alias→label mapping derived from the render plan's FROM/JOIN tables.
pub fn set_alias_label_map(map: HashMap<String, String>) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        ctx.borrow_mut().alias_label_map = map;
    });
}

// ============================================================================
// BULK OPERATIONS
// ============================================================================

// ============================================================================
// BRANCH CONTEXT SNAPSHOT — for per-SQL-scope isolation
// ============================================================================

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
}
