//! Variable-length-path (VLP) expression-rewriting transforms.
//!
//! Pure `&mut RenderExpr` / `&mut RenderPlan` rewriters that fix up how VLP
//! endpoint aliases and path-function columns are referenced after a VLP CTE
//! has been generated. They are the lowest-risk group split out of the
//! `plan_builder_utils.rs` god-file (Phase 2 / P2.1 of
//! `docs/design/REFACTORING_SAFETY_PLAN.md` §5.1): none touch the WITH→CTE
//! builders, they take everything they need as parameters, and they hold no
//! module state.
//!
//! Extracted verbatim from `plan_builder_utils.rs` (no logic edits). The old
//! path re-exports these via `pub(crate) use` during the transition so the
//! narrow set of existing callers is unaffected.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::render_expr::{Column, PropertyAccess, RenderExpr, TableAlias};
use crate::render_plan::RenderPlan;
use std::collections::HashMap;

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

/// Rewrite aggregate function arguments to use VLP CTE aliases for end nodes.
///
/// **Problem**: When `COUNT(DISTINCT b)` is used where `b` is a VLP end node:
/// - The aggregate normalizer converts `TableAlias("b")` to `PropertyAccessExp{table_alias: "b", column: "end_id"}`
/// - But in SQL, `b` doesn't exist as a table - the VLP CTE is joined as `vlp_a_b AS t`
/// - Result: `SELECT COUNT(DISTINCT b.end_id)` fails with "Identifier cannot be resolved"
///
/// **Solution**: Check if any PropertyAccessExp references a VLP end node Cypher alias,
/// and if so, replace it with the VLP CTE JOIN alias.
///
/// # Example
/// ```sql
/// -- Before rewrite:
/// SELECT COUNT(DISTINCT b.end_id)  -- ❌ b doesn't exist
/// FROM users AS a
/// LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
///
/// -- After rewrite:
/// SELECT COUNT(DISTINCT t.end_id)  -- ✅ t is the VLP CTE alias
/// FROM users AS a
/// LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
/// ```
pub fn rewrite_vlp_aggregate_aliases(plan: &mut RenderPlan) -> RenderPlanBuilderResult<()> {
    // Build mapping: VLP end node Cypher alias -> VLP CTE JOIN alias
    // Example: {"b": "t"} for `vlp_a_b AS t`
    let mut vlp_end_to_cte_alias: HashMap<String, String> = HashMap::new();

    // #647: For an END-anchored OPTIONAL VLP (`(a)<-[*]-(b)`, anchor `a` is the
    // pattern END node) the FROM clause binds the END node directly — its
    // properties come from the base table, NOT the VLP CTE. The VLP CTE's
    // `vlp_cypher_end_alias` then equals the FROM-bound anchor, so mapping it to
    // the CTE join alias would rewrite `a.name` → `vt0.name` (a column the CTE
    // does not expose). Skip the FROM-bound anchor here; the OTHER endpoint (the
    // one actually joined via the CTE) is projected from `vt0.start_*` by
    // `rewrite_vlp_union_branch_aliases`. For the common anchor-at-start layout
    // the FROM alias is the anchor (start), never the end alias, so this
    // exclusion is a no-op there → byte-identical.
    let optional_vlp_from_anchor: Option<&str> = plan
        .from
        .0
        .as_ref()
        .and_then(|from_ref| from_ref.alias.as_deref())
        .filter(|alias| !alias.starts_with("vlp_"));

    // #647/#643: only apply the end-anchored skip for a SINGLE VLP join. With
    // TWO OR MORE VLP joins (chained `OPTIONAL MATCH (a)<-[*]-(b) OPTIONAL MATCH
    // (b)<-[*]-(c)`) the endpoint→CTE-alias resolution is a separate, unsolved
    // defect (#643): the far endpoint would resolve to the WRONG VLP's column.
    // That chained shape is LOUD (Code 47) on main; keep it loud by NOT skipping
    // (the historical mapping then dangles exactly as before) rather than letting
    // it execute a silently-wrong aggregate (ground rule 1). Mirrors the
    // `vlp_join_count > 1` guard #630 uses in `rewrite_vlp_union_branch_aliases`.
    let single_vlp_join = plan
        .joins
        .0
        .iter()
        .filter(|j| j.table_name.starts_with("vlp_"))
        .count()
        == 1;

    // Extract VLP metadata from CTEs
    for cte in &plan.ctes.0 {
        if let Some(ref cypher_end_alias) = cte.vlp_cypher_end_alias {
            // #647: skip only for a GENUINE end-anchored inversion — the FROM
            // binds the end alias AND the pattern's two endpoints are distinct
            // (`start_alias != end_alias`). A CLOSED VLP (`(a)<-[*]-(a)`) has
            // start_alias == end_alias == the FROM anchor; it is a separate,
            // loud-on-main shape (#625/#631) left on the original layout by the
            // analyzer, so it must NOT be skipped here (skipping would silently
            // change its projection). Guarding on distinct endpoints keeps it
            // byte-identical.
            let endpoints_distinct =
                cte.vlp_cypher_start_alias.as_deref() != Some(cypher_end_alias.as_str());
            if single_vlp_join
                && endpoints_distinct
                && Some(cypher_end_alias.as_str()) == optional_vlp_from_anchor
            {
                log::debug!(
                    "🔧 VLP aggregate rewrite: skipping FROM-bound anchor end alias '{}' (#647 end-anchored OPTIONAL VLP)",
                    cypher_end_alias
                );
                continue;
            }
            // Find the corresponding JOIN to get the CTE alias
            for join in &plan.joins.0 {
                if join.table_name == cte.cte_name {
                    log::info!(
                        "🔧 VLP aggregate rewrite: Mapping Cypher alias '{}' -> CTE alias '{}' (from CTE '{}')",
                        cypher_end_alias,
                        join.table_alias,
                        cte.cte_name
                    );
                    vlp_end_to_cte_alias.insert(cypher_end_alias.clone(), join.table_alias.clone());
                    break;
                }
            }
        }
    }

    // If no VLP end nodes found, nothing to rewrite
    if vlp_end_to_cte_alias.is_empty() {
        return Ok(());
    }

    log::debug!(
        "VLP aggregate rewrite: Found {} VLP end node(s) to rewrite",
        vlp_end_to_cte_alias.len()
    );

    // Rewrite SELECT items
    for item in &mut plan.select.items {
        rewrite_expr_for_vlp_end_nodes(&mut item.expression, &vlp_end_to_cte_alias);
    }

    // Rewrite GROUP BY expressions
    for expr in &mut plan.group_by.0 {
        rewrite_expr_for_vlp_end_nodes(expr, &vlp_end_to_cte_alias);
    }

    // Rewrite HAVING clause
    if let Some(ref mut having) = plan.having_clause {
        rewrite_expr_for_vlp_end_nodes(having, &vlp_end_to_cte_alias);
    }

    // Rewrite ORDER BY expressions
    for item in &mut plan.order_by.0 {
        rewrite_expr_for_vlp_end_nodes(&mut item.expression, &vlp_end_to_cte_alias);
    }

    Ok(())
}

/// Recursively rewrite a RenderExpr to replace VLP end node aliases with CTE aliases.
///
/// This function handles the conversion:
/// - `b.end_id` (where b is VLP end node) → `t.end_id` (where t is VLP CTE alias)
fn rewrite_expr_for_vlp_end_nodes(
    expr: &mut RenderExpr,
    vlp_end_to_cte_alias: &HashMap<String, String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Check if this property references a VLP end node
            if let Some(cte_alias) = vlp_end_to_cte_alias.get(&prop.table_alias.0) {
                log::info!(
                    "🔧 VLP aggregate rewrite: Replacing {}.{} with {}.{}",
                    prop.table_alias.0,
                    prop.column.raw(),
                    cte_alias,
                    prop.column.raw()
                );
                prop.table_alias = TableAlias(cte_alias.clone());
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite aggregate function arguments
            for arg in &mut agg.args {
                rewrite_expr_for_vlp_end_nodes(arg, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operator operands (handles DISTINCT)
            for operand in &mut op.operands {
                rewrite_expr_for_vlp_end_nodes(operand, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            // Recursively rewrite function arguments
            for arg in &mut func.args {
                rewrite_expr_for_vlp_end_nodes(arg, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::Case(case) => {
            // Rewrite CASE expression
            if let Some(ref mut e) = case.expr {
                rewrite_expr_for_vlp_end_nodes(e, vlp_end_to_cte_alias);
            }
            for (when, then) in &mut case.when_then {
                rewrite_expr_for_vlp_end_nodes(when, vlp_end_to_cte_alias);
                rewrite_expr_for_vlp_end_nodes(then, vlp_end_to_cte_alias);
            }
            if let Some(ref mut else_expr) = case.else_expr {
                rewrite_expr_for_vlp_end_nodes(else_expr, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            rewrite_expr_for_vlp_end_nodes(array, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(index, vlp_end_to_cte_alias);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            rewrite_expr_for_vlp_end_nodes(array, vlp_end_to_cte_alias);
            if let Some(ref mut f) = from {
                rewrite_expr_for_vlp_end_nodes(f, vlp_end_to_cte_alias);
            }
            if let Some(ref mut t) = to {
                rewrite_expr_for_vlp_end_nodes(t, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::InSubquery(subq) => {
            rewrite_expr_for_vlp_end_nodes(&mut subq.expr, vlp_end_to_cte_alias);
        }
        RenderExpr::List(items) => {
            // Recursively rewrite each element of the list
            for item in items {
                rewrite_expr_for_vlp_end_nodes(item, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            // Recursively rewrite each value expression in the map literal
            for (_key, value) in entries {
                rewrite_expr_for_vlp_end_nodes(value, vlp_end_to_cte_alias);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            // Recursively rewrite all subexpressions of the reduce expression
            rewrite_expr_for_vlp_end_nodes(&mut reduce.initial_value, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(&mut reduce.list, vlp_end_to_cte_alias);
            rewrite_expr_for_vlp_end_nodes(&mut reduce.expression, vlp_end_to_cte_alias);
        }
        // Remaining expression types are leaves and don't contain nested aliases
        _ => {}
    }
}

/// Enhanced version that takes the FROM alias into account.
/// For VLP CTEs, the FROM clause looks like: FROM vlp_a_b AS t
/// So we need to use the alias (t) when rendering, and also add property prefixes (start_/end_).
///
/// This function assumes that any necessary translation from DB column names to
/// Cypher property names has already been performed (e.g., via
/// `translate_db_columns_to_cypher_properties`) before it is called. Its primary
/// responsibility is to rewrite expressions to use the correct VLP FROM alias and
/// the appropriate `start_`/`end_` prefixes for VLP CTE columns.
pub fn rewrite_render_expr_for_vlp_with_from_alias(
    expr: &mut RenderExpr,
    mappings: &HashMap<String, String>,
    vlp_from_alias: &str,
) {
    match expr {
        RenderExpr::Column(column) => {
            // Path functions use bare Column("path_nodes") that get qualified as t.path_nodes during SQL generation
            // We need to convert them to PropertyAccessExp so they can be rewritten
            // Check if this is a path function column (path_nodes, hop_count, path_relationships)
            let col_name_str = column.0.raw().to_string(); // Clone to avoid borrow issues
            if matches!(
                col_name_str.as_str(),
                "path_nodes" | "hop_count" | "path_relationships" | "path_edges"
            ) {
                log::info!(
                    "🔄 VLP: Converting Column({}) to PropertyAccessExp({}.{})",
                    col_name_str,
                    VLP_CTE_FROM_ALIAS,
                    col_name_str
                );
                // Replace Column with PropertyAccessExp using VLP FROM alias
                let _new_prop_access = PropertyAccess {
                    table_alias: TableAlias(VLP_CTE_FROM_ALIAS.to_string()),
                    column: PropertyValue::Column(col_name_str.clone()),
                };

                // Rewrite the table alias if it's in the mappings
                let rewritten_alias = mappings
                    .get(VLP_CTE_FROM_ALIAS)
                    .cloned()
                    .unwrap_or_else(|| vlp_from_alias.to_string());
                log::info!(
                    "🔄 Rewriting {}.{} → {}.{}",
                    VLP_CTE_FROM_ALIAS,
                    col_name_str,
                    rewritten_alias,
                    col_name_str
                );

                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rewritten_alias),
                    column: PropertyValue::Column(col_name_str),
                });
            }
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Check if this table alias needs rewriting
            if let Some(vlp_internal_alias) = mappings.get(&prop_access.table_alias.0) {
                // CRITICAL: Handle VLP property name rewriting with FROM alias
                // For normal VLP, the CTE has columns like:
                //   start_email, start_name, start_city, end_email, end_name, end_city
                // NOT just: email, name, city
                //
                // So when rewriting a.city → use the FROM alias + PREFIX the column:
                // 1. Keep the FROM alias (t): FROM vlp_a_b AS t
                // 2. PREFIX the column: city → start_city (for start node) or end_city (for end node)
                // 3. Final: t.start_city
                //
                // The mapping tells us the internal alias (start_node or end_node), which we use
                // to determine the prefix (start_ or end_).

                let col_name = prop_access.column.raw();

                // Determine if this is a start or end node based on the mapping
                let prefix = if vlp_internal_alias.starts_with("start_") {
                    "start_"
                } else if vlp_internal_alias.starts_with("end_") {
                    "end_"
                } else {
                    // Not a node alias, use as-is
                    ""
                };

                let prefixed_col = if !prefix.is_empty() {
                    format!("{}{}", prefix, col_name)
                } else {
                    col_name.to_string()
                };

                log::info!(
                    "🔄 VLP: Rewriting {}.{} → {}.{} (vlp_internal_alias={}, prefix={})",
                    prop_access.table_alias.0,
                    col_name,
                    vlp_from_alias,
                    prefixed_col,
                    vlp_internal_alias,
                    prefix
                );

                // Update both the alias (to FROM alias) and the column name (with prefix)
                prop_access.table_alias.0 = vlp_from_alias.to_string();
                if !prefix.is_empty() {
                    // Replace the column with the prefixed version
                    prop_access.column = PropertyValue::Column(prefixed_col);
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            for operand in &mut op_app.operands {
                rewrite_render_expr_for_vlp_with_from_alias(operand, mappings, vlp_from_alias);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                rewrite_render_expr_for_vlp_with_from_alias(arg, mappings, vlp_from_alias);
            }
        }
        RenderExpr::AggregateFnCall(func) => {
            for arg in &mut func.args {
                rewrite_render_expr_for_vlp_with_from_alias(arg, mappings, vlp_from_alias);
            }
        }
        RenderExpr::InSubquery(in_exp) => {
            rewrite_render_expr_for_vlp_with_from_alias(&mut in_exp.expr, mappings, vlp_from_alias);
        }
        RenderExpr::Case(case_exp) => {
            // #576: also rewrite the simple-CASE scrutinee.
            if let Some(scrutinee) = &mut case_exp.expr {
                rewrite_render_expr_for_vlp_with_from_alias(scrutinee, mappings, vlp_from_alias);
            }
            for (when_expr, then_expr) in &mut case_exp.when_then {
                rewrite_render_expr_for_vlp_with_from_alias(when_expr, mappings, vlp_from_alias);
                rewrite_render_expr_for_vlp_with_from_alias(then_expr, mappings, vlp_from_alias);
            }
            if let Some(else_expr) = &mut case_exp.else_expr {
                rewrite_render_expr_for_vlp_with_from_alias(else_expr, mappings, vlp_from_alias);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                rewrite_render_expr_for_vlp_with_from_alias(item, mappings, vlp_from_alias);
            }
        }
        // Other expression types don't contain table aliases
        _ => {}
    }
}

/// Translate DB column names to Cypher property names in PropertyAccessExp expressions.
/// VLP CTE columns use Cypher property names (e.g., `start_name` for `name`), but
/// after schema resolution, PropertyAccessExp may contain DB column names (e.g., `full_name`).
/// This pre-processes expressions so the VLP rewriter generates correct column references.
pub(crate) fn translate_db_columns_to_cypher_properties(
    expr: &mut RenderExpr,
    db_to_cypher: &HashMap<(String, String), String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop_access) => {
            let key = (
                prop_access.table_alias.0.clone(),
                prop_access.column.raw().to_string(),
            );
            if let Some(cypher_prop) = db_to_cypher.get(&key) {
                log::debug!(
                    "🔄 VLP DB→Cypher: {}.{} → {}.{}",
                    key.0,
                    key.1,
                    key.0,
                    cypher_prop
                );
                prop_access.column = PropertyValue::Column(cypher_prop.clone());
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                translate_db_columns_to_cypher_properties(operand, db_to_cypher);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                translate_db_columns_to_cypher_properties(arg, db_to_cypher);
            }
        }
        RenderExpr::AggregateFnCall(func) => {
            for arg in &mut func.args {
                translate_db_columns_to_cypher_properties(arg, db_to_cypher);
            }
        }
        RenderExpr::Case(case_exp) => {
            // #576: also translate the simple-CASE scrutinee.
            if let Some(scrutinee) = &mut case_exp.expr {
                translate_db_columns_to_cypher_properties(scrutinee, db_to_cypher);
            }
            for (when_expr, then_expr) in &mut case_exp.when_then {
                translate_db_columns_to_cypher_properties(when_expr, db_to_cypher);
                translate_db_columns_to_cypher_properties(then_expr, db_to_cypher);
            }
            if let Some(else_expr) = &mut case_exp.else_expr {
                translate_db_columns_to_cypher_properties(else_expr, db_to_cypher);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                translate_db_columns_to_cypher_properties(item, db_to_cypher);
            }
        }
        RenderExpr::InSubquery(in_sub) => {
            translate_db_columns_to_cypher_properties(&mut in_sub.expr, db_to_cypher);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, val) in entries.iter_mut() {
                translate_db_columns_to_cypher_properties(val, db_to_cypher);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            translate_db_columns_to_cypher_properties(array, db_to_cypher);
            translate_db_columns_to_cypher_properties(index, db_to_cypher);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            translate_db_columns_to_cypher_properties(array, db_to_cypher);
            if let Some(ref mut from_expr) = from {
                translate_db_columns_to_cypher_properties(from_expr, db_to_cypher);
            }
            if let Some(ref mut to_expr) = to {
                translate_db_columns_to_cypher_properties(to_expr, db_to_cypher);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            translate_db_columns_to_cypher_properties(&mut reduce.initial_value, db_to_cypher);
            translate_db_columns_to_cypher_properties(&mut reduce.list, db_to_cypher);
            translate_db_columns_to_cypher_properties(&mut reduce.expression, db_to_cypher);
        }
        _ => {}
    }
}

/// Enhanced version with NEW lookup-based mapping using complete metadata.
/// Maps (cypher_alias, db_column) → (cte_column_name, vlp_position)
/// NO HEURISTICS - all matching is direct and exact.
pub fn rewrite_render_expr_for_vlp_with_endpoint_info(
    expr: &mut RenderExpr,
    mappings: &HashMap<String, String>,
    vlp_from_alias: &str,
    endpoint_position: &HashMap<String, &str>,
    cte_column_mapping: &HashMap<
        (String, String),
        (String, crate::render_plan::cte_manager::VlpColumnPosition),
    >,
) {
    log::debug!("🔍 REWRITE: Processing expr with new lookup-based mapping (no splitting)");
    match expr {
        RenderExpr::TableAlias(alias) => {
            let alias_str = alias.0.clone();
            log::debug!(
                "🔍 REWRITE TableAlias: alias='{}', in_mappings={}",
                alias_str,
                mappings.contains_key(&alias_str)
            );
            // Check if this is a Cypher alias (mapping exists)
            if mappings.contains_key(&alias_str) {
                // For VLP endpoints, TableAlias should be rewritten to the CTE column
                // E.g., TableAlias("b") → Column("t.end_id")
                if let Some((cte_column_name, _position)) =
                    cte_column_mapping.get(&(alias_str.clone(), "id".to_string()))
                {
                    log::debug!(
                        "✅ REWRITE: TableAlias '{}' → Column('{}')",
                        alias_str,
                        cte_column_name
                    );
                    *expr =
                        RenderExpr::Column(Column(PropertyValue::Column(cte_column_name.clone())));
                } else {
                    log::debug!(
                        "❌ REWRITE: TableAlias '{}' not in cte_column_mapping for 'id'",
                        alias_str
                    );
                }
            }
            // No change needed if not in mappings
        }

        RenderExpr::PropertyAccessExp(prop_access) => {
            let alias = prop_access.table_alias.0.clone();
            let col_name = prop_access.column.raw();

            log::debug!(
                "🔍 REWRITE PropertyAccessExp: alias='{}', col_name='{}', in_mappings={}",
                alias,
                col_name,
                mappings.contains_key(&alias)
            );

            // Check if this is a Cypher alias (mapping exists)
            if mappings.contains_key(&alias) {
                log::debug!(
                    "✅ REWRITE: Found table_alias '{}' in mappings",
                    prop_access.table_alias.0
                );

                let col_name = prop_access.column.raw();
                let alias = prop_access.table_alias.0.clone();

                // NEW ALGORITHM: Direct lookup using DB column name
                // No splitting, no guessing - just look up the exact DB column name
                if let Some((cte_column_name, _position)) =
                    cte_column_mapping.get(&(alias.clone(), col_name.to_string()))
                {
                    log::debug!(
                        "✅ REWRITE: Direct lookup SUCCESS: ({}, {}) → {}",
                        alias,
                        col_name,
                        cte_column_name
                    );

                    // Rewrite to use the CTE column
                    prop_access.table_alias.0 = vlp_from_alias.to_string();
                    prop_access.column = PropertyValue::Column(cte_column_name.clone());
                } else {
                    // Fallback: construct from endpoint_position
                    // This handles cases where metadata wasn't fully populated
                    let prefix = match endpoint_position.get(alias.as_str()) {
                        Some(&"start") => "start_",
                        Some(&"end") => "end_",
                        _ => "",
                    };

                    let fallback_col = format!("{}{}", prefix, col_name);
                    log::debug!(
                        "⚠️ REWRITE: Lookup FAILED for ({}, {}), falling back to: {}",
                        alias,
                        col_name,
                        fallback_col
                    );

                    prop_access.table_alias.0 = vlp_from_alias.to_string();
                    prop_access.column = PropertyValue::Column(fallback_col);
                }
            }
        }
        RenderExpr::Case(case_expr) => {
            // Recursively rewrite expressions in the CASE
            // #576: also rewrite the simple-CASE scrutinee.
            if let Some(scrutinee) = &mut case_expr.expr {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    scrutinee,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
            for (when_expr, then_expr) in &mut case_expr.when_then {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    when_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    then_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
            if let Some(else_expr) = &mut case_expr.else_expr {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    else_expr,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            for operand in &mut op_app.operands {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    operand,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            for arg in &mut fn_call.args {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    arg,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::AggregateFnCall(fn_call) => {
            for arg in &mut fn_call.args {
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    arg,
                    mappings,
                    vlp_from_alias,
                    endpoint_position,
                    cte_column_mapping,
                );
            }
        }
        RenderExpr::InSubquery(in_exp) => {
            rewrite_render_expr_for_vlp_with_endpoint_info(
                &mut in_exp.expr,
                mappings,
                vlp_from_alias,
                endpoint_position,
                cte_column_mapping,
            );
        }
        _ => {
            // Other expression types don't need rewriting
        }
    }
}

/// Extract join condition from equality, for CTE correlation predicates
/// - Maps Cypher node aliases (u, x) to VLP internal aliases (start_node, end_node)
/// - Handles denormalized VLP patterns where both nodes are in the same table
/// - Maps path function aliases ("t") to actual VLP CTE aliases
/// - Skips multi-type VLP CTEs which use Cypher aliases directly
pub fn extract_vlp_alias_mappings(ctes: &crate::render_plan::CteItems) -> HashMap<String, String> {
    let mut mappings = HashMap::new();

    for (idx, cte) in ctes.0.iter().enumerate() {
        log::info!(
            "🔍 CTE[{}]: name={}, vlp_start={:?}, vlp_cypher_start={:?}",
            idx,
            cte.cte_name,
            cte.vlp_start_alias,
            cte.vlp_cypher_start_alias
        );

        // Skip alias mappings for multi-type VLP CTEs - they use Cypher aliases directly
        // and properties are extracted via JSON_VALUE() using the Cypher alias
        if cte.cte_name.starts_with("vlp_multi_type_") {
            log::debug!("🔄 VLP: Skipping alias mapping for multi-type VLP CTE (uses Cypher alias directly)");
            continue;
        }

        // Check if this is a VLP CTE with metadata
        if let Some(cypher_start) = &cte.vlp_cypher_start_alias {
            // Get the VLP internal alias, defaulting to "start_node" if not set
            let vlp_start = cte
                .vlp_start_alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "start_node".to_string());

            // Check if this is a denormalized VLP (both nodes in same table)
            // ✅ PHASE 2 APPROVED: Derives denormalization from schema structure, not flag
            let is_denormalized =
                cte.vlp_start_table == cte.vlp_end_table && cte.vlp_start_table.is_some();

            if is_denormalized {
                // For denormalized VLP, map Cypher alias directly to VLP CTE alias
                // (not to internal VLP aliases like "start_node")
                let vlp_cte_alias = cte
                    .cte_name
                    .replace("vlp_cte", "vlp")
                    .replace("chained_path_", "vlp");
                log::info!(
                    "🔄 VLP mapping (denormalized): {} → {}",
                    cypher_start,
                    vlp_cte_alias
                );
                mappings.insert(cypher_start.clone(), vlp_cte_alias.clone());
            } else {
                log::debug!("🔄 VLP mapping: {} → {}", cypher_start, vlp_start);
                mappings.insert(cypher_start.clone(), vlp_start.clone());
            }
        }

        if let Some(cypher_end) = &cte.vlp_cypher_end_alias {
            // Get the VLP internal alias, defaulting to "end_node" if not set
            let vlp_end = cte
                .vlp_end_alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "end_node".to_string());

            // Check if this is a denormalized VLP (both nodes in same table)
            // ✅ PHASE 2 APPROVED: Same structural check as above
            let is_denormalized =
                cte.vlp_start_table == cte.vlp_end_table && cte.vlp_start_table.is_some();

            if is_denormalized {
                // For denormalized VLP, map Cypher alias directly to VLP CTE alias
                let vlp_cte_alias = cte
                    .cte_name
                    .replace("vlp_cte", "vlp")
                    .replace("chained_path_", "vlp");
                log::info!(
                    "🔄 VLP mapping (denormalized): {} → {}",
                    cypher_end,
                    vlp_cte_alias
                );
                mappings.insert(cypher_end.clone(), vlp_cte_alias.clone());
            } else {
                log::debug!("🔄 VLP mapping: {} → {}", cypher_end, vlp_end);
                mappings.insert(cypher_end.clone(), vlp_end.clone());
            }
        }

        // 🔧 FIX: Map VLP FROM alias to the actual VLP CTE alias
        // When rewrite_logical_path_functions converts length(path) → t.hop_count,
        // we need to rewrite "t" to the actual VLP alias (e.g., "vlp1", "vlp2")
        if cte.cte_name.starts_with("vlp_cte") || cte.cte_name.starts_with("chained_path_") {
            // Extract VLP alias from CTE name: vlp_cte1 → vlp1, vlp_cte2 → vlp2
            let vlp_alias = cte
                .cte_name
                .replace("vlp_cte", "vlp")
                .replace("chained_path_", "vlp");
            log::debug!(
                "🔄 VLP path function mapping: {} → {}",
                VLP_CTE_FROM_ALIAS,
                vlp_alias
            );
            mappings.insert(VLP_CTE_FROM_ALIAS.to_string(), vlp_alias.clone());

            // ⚠️ TODO: REMOVE THIS FALLBACK - PROPER FIX REQUIRED
            // See notes/HOLISTIC_FIX_METHODOLOGY.md for details
            //
            // This fallback blindly maps relationship aliases (f, r, e, t1-t99) to VLP CTE aliases.
            // This is INCORRECT because:
            // 1. Relationship property filters (e.g., f.flight_number = 123) should be applied
            // ✅ HOLISTIC FIX (Dec 26, 2025): Relationship filters now properly handled in CTE generation
            // - FK-edge patterns: Map to start_node/new_start/current_node in cte_extraction.rs
            // - Standard patterns: Map to rel alias in cte_extraction.rs
            // - Denormalized patterns: Map to rel alias in cte_extraction.rs
            // No fallback mapping needed - filters are applied inside the CTE where they belong.
            log::debug!(
                "VLP relationship filters handled in CTE generation - no fallback mapping needed"
            );
        }
    }

    mappings
}
