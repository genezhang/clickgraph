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
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::graph_catalog::graph_schema::GraphSchema;

/// Per-query context holding all query-scoped state
/// This replaces multiple scattered task_local!/thread_local! declarations
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
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

    /// Current variable registry for SQL rendering.
    /// Set from the CTE or RenderPlan being rendered; used by PropertyAccessExp::to_sql()
    /// to resolve cypher_alias.property → correct SQL column.
    /// Wrapped in Arc to match RenderPlan/Cte fields and avoid cloning overhead.
    pub current_variable_registry: Option<std::sync::Arc<crate::query_planner::typed_variable::VariableRegistry>>,
}

impl QueryContext {
    /// Create a new query context with schema name
    pub fn new(schema_name: Option<String>) -> Self {
        Self {
            schema_name,
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

/// Get a relationship CTE name by alias
/// Returns None if alias not registered (use for lookup-only, no recomputation)
pub fn get_relationship_cte_name(alias: &str) -> Option<String> {
    QUERY_CONTEXT
        .try_with(|ctx| ctx.borrow().multi_type_vlp_aliases.get(alias).cloned())
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
) {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        ctx.relationship_columns = relationship_columns;
        ctx.cte_property_mappings = cte_mappings;
        ctx.multi_type_vlp_aliases = multi_type_aliases;
    });
}

/// Clear all render contexts (for render phase exit)
pub fn clear_all_render_contexts() {
    let _ = QUERY_CONTEXT.try_with(|ctx| {
        let mut ctx = ctx.borrow_mut();
        ctx.relationship_columns.clear();
        ctx.cte_property_mappings.clear();
        ctx.multi_type_vlp_aliases.clear();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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
