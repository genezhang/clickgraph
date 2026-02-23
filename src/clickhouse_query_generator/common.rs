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
/// use clickhouse_query_generator::common::quote_identifier;
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
/// use clickhouse_query_generator::common::qualified_column;
/// assert_eq!(qualified_column("t1", "user_id"), "t1.user_id");
/// assert_eq!(qualified_column("t1", "id.orig_h"), "t1.`id.orig_h`");
/// ```
pub fn qualified_column(table_alias: &str, column_name: &str) -> String {
    format!("{}.{}", table_alias, quote_identifier(column_name))
}
