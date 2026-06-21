//! Common utilities for ClickHouse query generation

// ⚠️ NOTE: Literal rendering code appears in multiple places due to different Literal types:
//
// - crate::query_planner::logical_expr::Literal (used in to_sql.rs)
// - crate::render_plan::render_expr::Literal (used in to_sql_query.rs)
//
// These are structurally similar but different types, making consolidation complex.
// The rendering logic is duplicated across:
// 1. to_sql.rs (lines ~65-70): Handles LogicalExpr::Literal
// 2. to_sql_query.rs (lines ~1620-1630): Handles RenderExpr::Literal
//
// Future Improvement: Create a unified Literal trait that both types implement,
// enabling a single render_literal() function in this module.

/// Quote a ClickHouse identifier (column name, table name) if it contains special characters.
///
/// ClickHouse requires backtick quoting for identifiers that contain:
/// - Dots (.)
/// - Spaces
/// - Hyphens (-)
/// - Other special characters
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::quote_identifier;
/// assert_eq!(quote_identifier("user_id"), "user_id");
/// assert_eq!(quote_identifier("id.orig_h"), "`id.orig_h`");
/// assert_eq!(quote_identifier("user-name"), "`user-name`");
/// ```
pub fn quote_identifier(name: &str) -> String {
    if name.contains('.')
        || name.contains(' ')
        || name.contains('-')
        || name.contains('(')
        || name.contains(')')
    {
        format!("`{}`", name)
    } else {
        name.to_string()
    }
}

/// Format a qualified column reference: table_alias.column_name
///
/// This function properly quotes the column name if it contains special characters.
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::qualified_column;
/// assert_eq!(qualified_column("t1", "user_id"), "t1.user_id");
/// assert_eq!(qualified_column("t1", "id.orig_h"), "t1.`id.orig_h`");
/// ```
pub fn qualified_column(table_alias: &str, column_name: &str) -> String {
    format!("{}.{}", table_alias, quote_identifier(column_name))
}

/// Emit a substring-containment predicate for Cypher `haystack CONTAINS needle`,
/// dialect-aware.
///
/// ClickHouse `position(haystack, needle)` and Spark/Databricks
/// `position(substr, str)` take their two arguments in OPPOSITE order, so the
/// operands are swapped when rendering for Databricks. Both return a 1-based
/// index (0 = not found), so the `> 0` test is identical.
pub fn contains_predicate(haystack: &str, needle: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        // Spark: position(substr, str) — substring first.
        SqlDialect::Databricks => format!("(position({}, {}) > 0)", needle, haystack),
        // ClickHouse: position(haystack, needle) — haystack first.
        _ => format!("(position({}, {}) > 0)", haystack, needle),
    }
}

/// Resolve a SQL function name to its active-dialect spelling via the function
/// registry, falling back to `name` unchanged when it has no registry entry.
///
/// Lets the duplicate generic `ScalarFnCall` renderers (in `render_plan`) map
/// dialect-divergent names (e.g. `tuple` -> Spark `struct`) without each one
/// re-implementing the lookup. The canonical `RenderExpr::to_sql` arm already
/// routes through the registry directly; this is the shared shim for the others.
pub fn dialect_function_name(name: &str) -> String {
    use super::function_registry::get_function_mapping;
    match get_function_mapping(&name.to_lowercase()) {
        Some(mapping) => mapping
            .name_for(crate::server::query_context::get_current_dialect())
            .to_string(),
        None => name.to_string(),
    }
}
