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
//! group. Landing incrementally — done so far: the expression-rewriting cluster
//! (`rewrite_operator_application_for_cte`/`_join`, `rewrite_render_expr_for_cte_*`),
//! the CTE-name remap pair (`remap_cte_names_in_expr`/`_in_render_plan`), and the
//! join-condition rewrite group (`collect_with_cte_table_aliases`,
//! `strip_table_alias_from_resolved`, `rewrite_join_conditions_for_cte_aliases`).
//! Remaining: `rewrite_logical_expr_cte_refs`, `update_graph_joins_cte_refs`, the
//! alias walkers deferred from P2.4, and the D2 dedup.

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

/// Apply CTE name remapping to RenderExpr recursively
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `cte_name_mapping` - Maps analyzer CTE names to actual CTE names
pub fn remap_cte_names_in_expr(
    expr: crate::render_plan::render_expr::RenderExpr,
    cte_name_mapping: &std::collections::HashMap<String, String>,
) -> crate::render_plan::render_expr::RenderExpr {
    use crate::render_plan::render_expr::*;

    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            let table_alias = &pa.table_alias.0;

            // Check if this table_alias is a CTE name that needs remapping
            if let Some(actual_cte_name) = cte_name_mapping.get(table_alias) {
                log::debug!("🔧 remap_cte_names: {} → {}", table_alias, actual_cte_name);
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(actual_cte_name.clone()),
                    column: pa.column,
                })
            } else {
                RenderExpr::PropertyAccessExp(pa)
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args = agg
                .args
                .into_iter()
                .map(|arg| remap_cte_names_in_expr(arg, cte_name_mapping))
                .collect();
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name,
                args: new_args,
            })
        }
        RenderExpr::ScalarFnCall(func) => {
            let new_args = func
                .args
                .into_iter()
                .map(|arg| remap_cte_names_in_expr(arg, cte_name_mapping))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name,
                args: new_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let new_operands = op
                .operands
                .into_iter()
                .map(|operand| remap_cte_names_in_expr(operand, cte_name_mapping))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: new_operands,
            })
        }
        RenderExpr::Case(case) => {
            let new_when_then = case
                .when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        remap_cte_names_in_expr(when, cte_name_mapping),
                        remap_cte_names_in_expr(then, cte_name_mapping),
                    )
                })
                .collect();
            let new_expr = case
                .expr
                .map(|e| Box::new(remap_cte_names_in_expr(*e, cte_name_mapping)));
            let new_else = case
                .else_expr
                .map(|e| Box::new(remap_cte_names_in_expr(*e, cte_name_mapping)));
            RenderExpr::Case(RenderCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }
        other => other,
    }
}

/// Apply CTE name remapping to all expressions in a RenderPlan
pub fn remap_cte_names_in_render_plan(
    plan: &mut crate::render_plan::RenderPlan,
    cte_name_mapping: &std::collections::HashMap<String, String>,
) {
    use crate::render_plan::render_expr::RenderExpr;

    if cte_name_mapping.is_empty() {
        return;
    }

    log::info!(
        "🔧 remap_cte_names_in_render_plan: Applying {} CTE name mappings",
        cte_name_mapping.len()
    );
    for (from, to) in cte_name_mapping {
        log::debug!("🔧   {} → {}", from, to);
    }

    // Rewrite SELECT items
    for item in &mut plan.select.items {
        item.expression = remap_cte_names_in_expr(item.expression.clone(), cte_name_mapping);
    }

    // Rewrite JOIN conditions
    for join in &mut plan.joins.0 {
        for op in &mut join.joining_on {
            // Recursively rewrite the OperatorApplication
            if let RenderExpr::OperatorApplicationExp(new_op) = remap_cte_names_in_expr(
                RenderExpr::OperatorApplicationExp(op.clone()),
                cte_name_mapping,
            ) {
                *op = new_op;
            }
        }
    }

    // Rewrite WHERE clause
    if let Some(filter) = &plan.filters.0 {
        plan.filters.0 = Some(remap_cte_names_in_expr(filter.clone(), cte_name_mapping));
    }

    // Rewrite GROUP BY
    plan.group_by.0 = plan
        .group_by
        .0
        .iter()
        .map(|expr| remap_cte_names_in_expr(expr.clone(), cte_name_mapping))
        .collect();

    // Rewrite ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = remap_cte_names_in_expr(item.expression.clone(), cte_name_mapping);
    }
}

/// Collect all `with_*_cte_*` table aliases referenced in a RenderPlan's expressions.
// P2.5 sub-slice C: `fn` → `pub(crate) fn` so the transition re-export reaches it.
pub(crate) fn collect_with_cte_table_aliases(
    plan: &crate::render_plan::RenderPlan,
) -> std::collections::HashSet<String> {
    use crate::render_plan::render_expr::RenderExpr;

    fn collect_from_expr(expr: &RenderExpr, result: &mut std::collections::HashSet<String>) {
        match expr {
            RenderExpr::PropertyAccessExp(pa) => {
                let alias = &pa.table_alias.0;
                if alias.starts_with("with_") && alias.contains("_cte_") {
                    result.insert(alias.clone());
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                for operand in &op.operands {
                    collect_from_expr(operand, result);
                }
            }
            RenderExpr::AggregateFnCall(fc) => {
                for arg in &fc.args {
                    collect_from_expr(arg, result);
                }
            }
            RenderExpr::ScalarFnCall(fc) => {
                for arg in &fc.args {
                    collect_from_expr(arg, result);
                }
            }
            RenderExpr::Case(ce) => {
                if let Some(ref expr) = ce.expr {
                    collect_from_expr(expr, result);
                }
                for (cond, val) in &ce.when_then {
                    collect_from_expr(cond, result);
                    collect_from_expr(val, result);
                }
                if let Some(ref else_expr) = ce.else_expr {
                    collect_from_expr(else_expr, result);
                }
            }
            RenderExpr::ExistsSubquery(_) => {
                // ExistsSubquery contains pre-rendered SQL, no expressions to scan
            }
            _ => {}
        }
    }

    let mut result = std::collections::HashSet::new();
    for item in &plan.select.items {
        collect_from_expr(&item.expression, &mut result);
    }
    for join in &plan.joins.0 {
        for op in &join.joining_on {
            collect_from_expr(&RenderExpr::OperatorApplicationExp(op.clone()), &mut result);
        }
    }
    if let Some(ref filter) = plan.filters.0 {
        collect_from_expr(filter, &mut result);
    }
    for expr in &plan.group_by.0 {
        collect_from_expr(expr, &mut result);
    }
    for item in &plan.order_by.0 {
        collect_from_expr(&item.expression, &mut result);
    }
    result
}

/// Strip table alias from resolved CTE property accesses, recursively.
/// Converts `PropertyAccessExp(table_alias, column)` → `Column(column)` so ORDER BY
/// references output column aliases (visible after GROUP BY) instead of internal table references.
// P2.5 sub-slice C: `fn` → `pub(crate) fn` so the transition re-export reaches it.
pub(crate) fn strip_table_alias_from_resolved(expr: &RenderExpr) -> RenderExpr {
    use super::render_expr::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    // Exhaustive combinator: drop the table alias off each resolved property
    // access (PropertyAccess → bare Column), recurse structurally into every
    // value-wrapper. The former hand-rolled walk handled
    // PropertyAccess/Operator/ScalarFn/Aggregate and fell through
    // `_ => expr.clone()` for List/Case/ArraySubscript/…, silently leaving a
    // qualified column inside those wrappers (e.g. `ORDER BY [x.a][0]` over a
    // CTE-scoped WITH). Latent (no corpus query reached it; byte-identical on
    // migration), now structurally impossible.
    map_render_expr(expr, &mut |node| match node {
        RenderExpr::PropertyAccessExp(pa) => {
            if let PropertyValue::Column(col) = &pa.column {
                RenderRewrite::Replace(RenderExpr::Column(Column(PropertyValue::Column(
                    col.clone(),
                ))))
            } else {
                RenderRewrite::Replace(node.clone())
            }
        }
        _ => RenderRewrite::Recurse,
    })
}

/// Rewrite join conditions in a rendered plan that reference CTE aliases.
/// When a join condition uses `cte_alias.base_column` (e.g., `friend.id`),
/// replace it with the CTE's prefixed column (e.g., `friend.p6_friend_id`).
// P2.5 sub-slice C: `fn` → `pub(crate) fn` so the transition re-export reaches it.
pub(crate) fn rewrite_join_conditions_for_cte_aliases(
    plan: &mut crate::render_plan::RenderPlan,
    cte_references: &std::collections::HashMap<String, String>,
    cte_schemas: &super::CteSchemas,
) {
    use crate::render_plan::render_expr::RenderExpr;

    fn rewrite_expr_for_cte(
        expr: RenderExpr,
        cte_references: &std::collections::HashMap<String, String>,
        cte_schemas: &super::CteSchemas,
    ) -> RenderExpr {
        match expr {
            RenderExpr::PropertyAccessExp(mut pa) => {
                let alias = &pa.table_alias.0;
                if let Some(cte_name) = cte_references.get(alias) {
                    if let Some(meta) = cte_schemas.get(cte_name) {
                        let col_name = match &pa.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(c) => {
                                c.clone()
                            }
                            crate::graph_catalog::expression_parser::PropertyValue::Expression(
                                e,
                            ) => e.clone(),
                        };
                        // Look up (alias, column) → CTE column name
                        if let Some(cte_col) = meta
                            .property_mapping
                            .get(&(alias.clone(), col_name.clone()))
                        {
                            log::info!(
                                "🔧 rewrite_join_cte: {}.{} → {}.{}",
                                alias,
                                col_name,
                                alias,
                                cte_col
                            );
                            pa.column =
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    cte_col.clone(),
                                );
                        }
                    }
                }
                RenderExpr::PropertyAccessExp(pa)
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|o| rewrite_expr_for_cte(o, cte_references, cte_schemas))
                    .collect();
                RenderExpr::OperatorApplicationExp(op)
            }
            other => other,
        }
    }

    for join in &mut plan.joins.0 {
        for op in &mut join.joining_on {
            let rewritten = rewrite_expr_for_cte(
                RenderExpr::OperatorApplicationExp(op.clone()),
                cte_references,
                cte_schemas,
            );
            if let RenderExpr::OperatorApplicationExp(new_op) = rewritten {
                *op = new_op;
            }
        }
    }

    // Also rewrite FROM table if it references a CTE
    if let crate::render_plan::FromTableItem(Some(ref mut from_ref)) = plan.from {
        if let Some(alias) = &from_ref.alias {
            if let Some(cte_name) = cte_references.get(alias) {
                if from_ref.name != *cte_name {
                    log::info!(
                        "🔧 rewrite_join_cte: Updating FROM table '{}' → '{}' for alias '{}'",
                        from_ref.name,
                        cte_name,
                        alias
                    );
                    from_ref.name = cte_name.clone();
                }
            }
        }
    }

    // CRITICAL: Also rewrite UNION branches' joins and FROM
    // BidirectionalUnion creates UNION branches that are rendered independently.
    // Their join conditions reference base table columns (e.g., person.id) which
    // need CTE column rewriting just like the main plan's joins.
    if let super::UnionItems(Some(ref mut union)) = plan.union {
        for branch in &mut union.input {
            rewrite_join_conditions_for_cte_aliases(branch, cte_references, cte_schemas);
        }
    }
}
