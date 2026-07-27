//! CTE-reference rewriting — transforms `RenderExpr`/`OperatorApplication`
//! property accesses to CTE column naming when the referenced alias is a CTE.
//!
//! Extracted verbatim from `plan_builder_utils.rs` in P2.5 (first sub-slice,
//! `REFACTORING_SAFETY_PLAN.md` §5.1). No logic edits — the one externally-called
//! function (`rewrite_operator_application_for_cte`) is re-exported `pub(crate)`
//! from `plan_builder_utils` during the transition so `join_builder`'s
//! `super::plan_builder_utils::rewrite_operator_application_for_cte` call site
//! keeps resolving.
//!
//! Scope note: P2.5's §5.1 home covers the whole CTE-ref extraction + rewriting
//! group (`remap_cte_names_*`, `rewrite_logical_expr_cte_refs`,
//! `update_graph_joins_cte_refs`, the alias walkers deferred from P2.4, and the
//! D2 dedup). Those are landing incrementally; this first sub-slice is the
//! self-contained 4-function expression-rewriting cluster.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::render_plan::render_expr::{
    OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::utils::cte_column_naming::cte_column_name;
use std::collections::HashMap;

/// Rewrite a RenderExpr to use CTE column names where applicable.
/// Converts property access expressions to use CTE column naming convention.
/// E.g., a.user_id becomes a_b.a_user_id (where a_b is the CTE alias)
// P2.5: widened `fn` → `pub(crate) fn` (the sole change vs the original) so the
// two remaining call sites in plan_builder_utils resolve via the transition
// re-export; body is verbatim.
pub(crate) fn rewrite_operator_application_for_cte_join(
    op_app: &OperatorApplication,
    cte_alias: &str,
    cte_references: &HashMap<String, String>,
) -> OperatorApplication {
    // Rewrite operands to use CTE column names
    let rewritten_operands: Vec<RenderExpr> = op_app
        .operands
        .iter()
        .map(|operand| rewrite_render_expr_for_cte_operand(operand, cte_alias, cte_references))
        .collect();

    OperatorApplication {
        operator: op_app.operator,
        operands: rewritten_operands,
    }
}

/// Public version for use by join_builder
/// Rewrites operator application to use CTE column names.
/// The table alias is kept (e.g., "o" stays "o") but column becomes "o_user_id".
pub fn rewrite_operator_application_for_cte(
    op_app: &OperatorApplication,
    cte_references: &HashMap<String, String>,
) -> OperatorApplication {
    // Rewrite operands to use CTE column names
    let rewritten_operands: Vec<RenderExpr> = op_app
        .operands
        .iter()
        .map(|operand| rewrite_render_expr_for_cte_simple(operand, cte_references))
        .collect();

    OperatorApplication {
        operator: op_app.operator,
        operands: rewritten_operands,
    }
}

/// Simple CTE expression rewriting - just prefixes column names, keeps table alias the same.
/// E.g., o.user_id -> o.o_user_id (when "o" is in cte_references)
fn rewrite_render_expr_for_cte_simple(
    expr: &RenderExpr,
    cte_references: &HashMap<String, String>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            // Check if this alias is from a CTE
            if cte_references.contains_key(&pa.table_alias.0) {
                // Rewrite column to use CTE naming: alias_column
                // Keep the same table alias (e.g., "o" stays "o")
                let cte_column = cte_column_name(&pa.table_alias.0, pa.column.raw());
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: pa.table_alias.clone(), // Keep same table alias
                    column: PropertyValue::Column(cte_column),
                })
            } else {
                // Not a CTE reference, keep as-is
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application_for_cte(inner_op, cte_references),
        ),
        _ => expr.clone(),
    }
}

/// Rewrite a RenderExpr operand to use CTE column names where applicable.
/// Helper function that avoids needing cte_schemas parameter.
fn rewrite_render_expr_for_cte_operand(
    expr: &RenderExpr,
    cte_alias: &str,
    cte_references: &HashMap<String, String>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            // Check if this alias is from a CTE
            if cte_references.contains_key(&pa.table_alias.0) {
                // Rewrite to use CTE alias and column naming.
                // Skip re-encoding if the column is already a CTE-encoded name (p{N}_...)
                // to avoid double-encoding like p20_a_allNeighboursCount_p1_a_user_id
                let raw_col = pa.column.raw();
                let cte_column = if crate::utils::cte_column_naming::is_cte_column(raw_col) {
                    raw_col.to_string()
                } else {
                    cte_column_name(&pa.table_alias.0, raw_col)
                };
                log::info!(
                    "🔧 Rewriting property access: {}.{} -> {}.{}",
                    pa.table_alias.0,
                    pa.column.raw(),
                    cte_alias,
                    cte_column
                );
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_alias.to_string()),
                    column: PropertyValue::Column(cte_column),
                })
            } else {
                // Not a CTE reference, keep as-is
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op) => RenderExpr::OperatorApplicationExp(
            rewrite_operator_application_for_cte_join(inner_op, cte_alias, cte_references),
        ),
        _ => expr.clone(),
    }
}
