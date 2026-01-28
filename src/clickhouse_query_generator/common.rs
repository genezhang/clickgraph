//! Common utilities for ClickHouse query generation

use crate::clickhouse_query_generator::errors::ClickhouseQueryGeneratorError;

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

/// Helper for creating errors with context
///
/// # Example
/// ```ignore
/// use crate::clickhouse_query_generator::common::error_with_context;
///
/// return Err(ClickhouseQueryGeneratorError::schema_error_with_context(
///     "Node table not found for label: User",
///     "while expanding relationship 'FOLLOWS' at hop 2"
/// ));
/// ```
pub fn error_with_context(
    error: ClickhouseQueryGeneratorError,
    context: impl Into<String>,
) -> ClickhouseQueryGeneratorError {
    let ctx = context.into();
    match error {
        ClickhouseQueryGeneratorError::SchemaError(msg) => {
            ClickhouseQueryGeneratorError::SchemaError(format!("{}\n  Context: {}", msg, ctx))
        }
        ClickhouseQueryGeneratorError::ColumnNotFound(col) => {
            ClickhouseQueryGeneratorError::ColumnNotFound(format!("{} ({})", col, ctx))
        }
        other => other,
    }
}

/// Macro for adding context to error results
///
/// # Example
/// ```ignore
/// validate_column(col_name).map_err(|e| {
///     map_err_context!(e, "while validating column in table 'Users'")
/// })?;
/// ```
#[macro_export]
macro_rules! map_err_context {
    ($err:expr, $ctx:expr) => {
        $crate::clickhouse_query_generator::common::error_with_context($err, $ctx)
    };
}
