//! Filter pipeline utilities for categorizing and processing filters

use super::render_expr::RenderExpr;

/// Represents categorized filters for different parts of a query
///
/// This struct supports two modes:
/// 1. RenderExpr-based: Filters as AST (start_node_filters, end_node_filters, etc.)
/// 2. Pre-rendered SQL: Filters already rendered to SQL strings (start_sql, end_sql, etc.)
///
/// Pre-rendered SQL is used for backward compatibility with VariableLengthCteGenerator
/// during the transition to CteManager. Once migration is complete, we can remove
/// the pre-rendered SQL fields.
#[derive(Debug, Clone, Default)]
pub struct CategorizedFilters {
    // RenderExpr-based filters (preferred - supports re-rendering with different alias mappings)
    pub start_node_filters: Option<RenderExpr>,
    pub end_node_filters: Option<RenderExpr>,
    pub relationship_filters: Option<RenderExpr>,
    pub path_function_filters: Option<RenderExpr>,
    /// #1103: predicates referencing BOTH VLP endpoints (e.g. `friend <> root`,
    /// `friend.id <> root.id`). These are neither start-only nor end-only: they
    /// constrain the (start, FINAL endpoint) pair of a whole path, so they must
    /// be applied on the post-recursion wrapper — never in the base case (which
    /// sees only hop-1 rows) and never per-hop on the recursive arm (whose
    /// `end_node` is an INTERMEDIATE node, and which Cypher's TRAIL semantics
    /// allow to revisit any node). Kept separate from `start_node_filters` so
    /// the base case cannot pick them up.
    pub both_endpoint_filters: Option<RenderExpr>,

    // Pre-rendered SQL strings (for backward compatibility during CteManager transition)
    // These take precedence over RenderExpr when present
    pub start_sql: Option<String>,
    pub end_sql: Option<String>,
    pub relationship_sql: Option<String>,
    /// Pre-rendered SQL for [`Self::both_endpoint_filters`] (#1103).
    pub both_endpoint_sql: Option<String>,
}
