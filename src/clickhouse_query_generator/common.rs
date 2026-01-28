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
