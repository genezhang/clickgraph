//! Canonical WITH-clause key generation.
//!
//! A "WITH key" is the stable string identity of a `WithClause`, used to
//! distinguish e.g. `WITH friend` from `WITH friend, post` when grouping and
//! matching WITH barriers during WITH→CTE lowering.
//!
//! ## The Problem (D1)
//! This logic previously existed as **three** near-duplicate local helpers
//! inside `render_plan/with_to_cte/mod.rs`:
//! - `generate_with_key_from_with_clause` — the *rich* variant: uses
//!   `exported_aliases` when present, else falls back to extracting aliases
//!   from the projection items, else `"with_var"`. This is the variant the
//!   authoritative `find_all_with_clauses_grouped` relies on.
//! - `get_with_key` / `get_with_clause_key` — two *simple* copies that used
//!   `exported_aliases` when present and otherwise jumped straight to
//!   `"with_var"`, skipping the item-extraction fallback. Both carried a
//!   comment claiming to "match `find_all_with_clauses_grouped`".
//!
//! ## The Solution
//! One canonical `with_clause_key()` — the rich variant — as the single source
//! of truth, so grouping and matching can never disagree about a barrier's
//! identity.

use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{ProjectionItem, WithClause};

/// Extract the alias from a WITH projection item.
/// Priority: explicit col_alias > inferred from expression (variable name, table alias)
/// Note: Strips ".*" suffix from col_alias (e.g., "friend.*" -> "friend")
fn extract_with_alias(item: &ProjectionItem) -> Option<String> {
    // First check for explicit alias
    if let Some(ref alias) = item.col_alias {
        // Strip ".*" suffix if present (added by projection_tagging.rs for node expansions)
        let clean_alias = alias.0.strip_suffix(".*").unwrap_or(&alias.0).to_string();
        log::info!(
            "🔍 extract_with_alias: Found explicit col_alias: {} -> {}",
            alias.0,
            clean_alias
        );
        return Some(clean_alias);
    }

    // Helper to extract alias from nested expression
    fn extract_alias_from_expr(expr: &LogicalExpr) -> Option<String> {
        match expr {
            LogicalExpr::ColumnAlias(ca) => {
                log::debug!("🔍 extract_with_alias: ColumnAlias: {}", ca.0);
                Some(ca.0.clone())
            }
            LogicalExpr::TableAlias(ta) => {
                log::debug!("🔍 extract_with_alias: TableAlias: {}", ta.0);
                Some(ta.0.clone())
            }
            LogicalExpr::Column(col) => {
                // A bare column name - this is often the variable name in WITH
                // e.g., WITH friend -> Column("friend")
                // Skip "*" since it's not a real variable name
                if col.0 == "*" {
                    log::debug!("🔍 extract_with_alias: Skipping Column('*')");
                    None
                } else {
                    log::debug!("🔍 extract_with_alias: Column: {}", col.0);
                    Some(col.0.clone())
                }
            }
            LogicalExpr::PropertyAccessExp(pa) => {
                // For property access like `friend.name`, use the table alias
                log::info!(
                    "🔍 extract_with_alias: PropertyAccessExp: {}.{:?}",
                    pa.table_alias.0,
                    pa.column
                );
                Some(pa.table_alias.0.clone())
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                // Handle operators like DISTINCT that wrap other expressions
                // Try to extract alias from the first operand
                log::debug!(
                    "🔍 extract_with_alias: OperatorApplicationExp with {:?}, checking operands",
                    op_app.operator
                );
                for operand in &op_app.operands {
                    if let Some(alias) = extract_alias_from_expr(operand) {
                        return Some(alias);
                    }
                }
                None
            }
            other => {
                log::info!(
                    "🔍 extract_with_alias: Unhandled expression type in nested: {:?}",
                    std::mem::discriminant(other)
                );
                None
            }
        }
    }

    // Try to infer from expression
    log::info!(
        "🔍 extract_with_alias: Expression type: {:?}",
        std::mem::discriminant(&item.expression)
    );
    extract_alias_from_expr(&item.expression)
}

/// Generate a unique key for a `WithClause` based on its exported aliases or projection items.
///
/// This is the canonical WITH-key generator (D1 dedup): it prefers the
/// already-computed `exported_aliases` (sorted, underscore-joined), falls back
/// to extracting aliases from the projection items, and otherwise returns
/// `"with_var"`. The key distinguishes e.g. `WITH friend` from
/// `WITH friend, post`.
pub fn with_clause_key(wc: &WithClause) -> String {
    // First try exported_aliases (preferred, already computed)
    if !wc.exported_aliases.is_empty() {
        let mut aliases = wc.exported_aliases.clone();
        aliases.sort();
        return aliases.join("_");
    }
    // Fall back to extracting from items
    let mut aliases: Vec<String> = wc
        .items
        .iter()
        .filter_map(extract_with_alias)
        .filter(|a| a != "*")
        .collect();
    aliases.sort();
    if aliases.is_empty() {
        "with_var".to_string()
    } else {
        aliases.join("_")
    }
}
