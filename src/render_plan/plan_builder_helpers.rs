//! Helper functions for plan building
//!
//! This module contains utility functions used by the RenderPlanBuilder trait implementation.
//! These functions assist with:
//! - Plan tree traversal and table/column extraction
//! - Expression rendering and SQL string generation
//! - Relationship and node information lookup
//! - Path function rewriting
//! - Schema lookups
//! - Polymorphic edge filter generation

use super::render_expr::{
    AggregateFnCall, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr,
    ScalarFnCall, TableAlias,
};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
use crate::render_plan::cte_extraction::{
    get_node_label_for_alias, get_relationship_type_for_alias,
};
// Note: Direction import commented out until Issue #1 (Undirected Multi-Hop SQL) is fixed
// use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::sql_generator::function_mapper::current_function_mapper;
use std::collections::HashSet;
#[cfg(test)] // test-only-live: exercised solely by unit tests (P2.10 dead_code sweep)
/// Recursively rewrite TableAlias references that are in `with_aliases` to reference the CTE.
/// This handles the case where `AVG(follows)` needs to become `AVG(grouped_data.follows)`.
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `with_aliases` - Set of WITH alias names that should be rewritten
/// * `cte_name` - The name of the CTE to reference (e.g., "grouped_data")
///
/// # Returns
/// A tuple of (rewritten_expression, all_from_with) where `all_from_with` is true
/// if all leaf references in the expression came from WITH aliases.
pub(super) fn rewrite_with_aliases_to_cte(
    expr: RenderExpr,
    with_aliases: &HashSet<String>,
    cte_name: &str,
) -> (RenderExpr, bool) {
    match expr {
        RenderExpr::TableAlias(alias) => {
            if with_aliases.contains(&alias.0) {
                // Rewrite to CTE reference: grouped_data.follows
                let rewritten = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_name.to_string()),
                    column: PropertyValue::Column(alias.0.clone()),
                });
                (rewritten, true)
            } else {
                (RenderExpr::TableAlias(alias), false)
            }
        }
        RenderExpr::ColumnAlias(alias) => {
            if with_aliases.contains(&alias.0) {
                // Rewrite to CTE reference
                let rewritten = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(cte_name.to_string()),
                    column: PropertyValue::Column(alias.0.clone()),
                });
                (rewritten, true)
            } else {
                (RenderExpr::ColumnAlias(alias), false)
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments
            let mut all_from_with = true;
            let new_args: Vec<RenderExpr> = agg
                .args
                .into_iter()
                .map(|arg| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(arg, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name,
                    args: new_args,
                }),
                all_from_with,
            )
        }
        RenderExpr::ScalarFnCall(func) => {
            // Recursively rewrite arguments
            let mut all_from_with = true;
            let new_args: Vec<RenderExpr> = func
                .args
                .into_iter()
                .map(|arg| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(arg, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: func.name,
                    args: new_args,
                }),
                all_from_with,
            )
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let mut all_from_with = true;
            let new_operands: Vec<RenderExpr> = op
                .operands
                .into_iter()
                .map(|operand| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(operand, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();

            (
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                }),
                all_from_with,
            )
        }
        RenderExpr::Case(case) => {
            // Recursively rewrite CASE expression
            use super::render_expr::RenderCase;
            let mut all_from_with = true;

            // Rewrite the optional CASE expression (for simple CASE syntax)
            let new_expr = case.expr.map(|e| {
                let (rewritten, from_with) =
                    rewrite_with_aliases_to_cte(*e, with_aliases, cte_name);
                if !from_with {
                    all_from_with = false;
                }
                Box::new(rewritten)
            });

            let new_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .into_iter()
                .map(|(cond, result)| {
                    let (new_cond, cond_from_with) =
                        rewrite_with_aliases_to_cte(cond, with_aliases, cte_name);
                    let (new_result, result_from_with) =
                        rewrite_with_aliases_to_cte(result, with_aliases, cte_name);
                    if !cond_from_with || !result_from_with {
                        all_from_with = false;
                    }
                    (new_cond, new_result)
                })
                .collect();

            let new_else = case.else_expr.map(|e| {
                let (new_else, else_from_with) =
                    rewrite_with_aliases_to_cte(*e, with_aliases, cte_name);
                if !else_from_with {
                    all_from_with = false;
                }
                Box::new(new_else)
            });

            (
                RenderExpr::Case(RenderCase {
                    expr: new_expr,
                    when_then: new_when_then,
                    else_expr: new_else,
                }),
                all_from_with,
            )
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // Property access doesn't come from WITH directly,
            // but we pass through (handled by rewrite_table_aliases_to_cte if needed)
            (RenderExpr::PropertyAccessExp(prop), false)
        }
        RenderExpr::List(items) => {
            let mut all_from_with = true;
            let new_items: Vec<RenderExpr> = items
                .into_iter()
                .map(|item| {
                    let (rewritten, from_with) =
                        rewrite_with_aliases_to_cte(item, with_aliases, cte_name);
                    if !from_with {
                        all_from_with = false;
                    }
                    rewritten
                })
                .collect();
            (RenderExpr::List(new_items), all_from_with)
        }
        // Literals, Star, Column, Parameter, Raw don't need rewriting and don't come from WITH
        other => (other, false),
    }
}

/// Helper function to check if a LogicalPlan node represents a denormalized node
///
/// ✅ PHASE 2 APPROVED: This is a structural query helper, not property resolution logic.
/// It reads flags set by analyzer passes to determine JOIN requirements.
/// For denormalized nodes, the node data lives on the edge table, not a separate node table
/// For nested GraphRels, we recursively check the leaf nodes
pub(super) fn is_node_denormalized(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // Check the GraphNode's own is_denormalized flag first
            if crate::graph_catalog::pattern_schema::node_denormalized_flag(node) {
                return true;
            }
            // Fall back to checking ViewScan input
            if let LogicalPlan::ViewScan(view_scan) = node.input.as_ref() {
                crate::graph_catalog::pattern_schema::scan_denormalized_flag(view_scan)
            } else {
                false
            }
        }
        // For nested GraphRel, check if the innermost node is denormalized
        LogicalPlan::GraphRel(graph_rel) => {
            // Recursively check the left side to find the leftmost GraphNode
            is_node_denormalized(&graph_rel.left)
        }
        // Union of denormalized GraphNodes (standalone denormalized node scan)
        LogicalPlan::Union(union) => {
            !union.inputs.is_empty() && union.inputs.iter().all(|input| is_node_denormalized(input))
        }
        _ => false,
    }
}

/// Check if a Union plan consists entirely of denormalized GraphNodes.
/// Returns true only for standalone denormalized node scans (from/to branches).
pub(super) fn is_denormalized_union(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Union(union) = plan {
        !union.inputs.is_empty() && union.inputs.iter().all(|input| is_node_denormalized(input))
    } else {
        false
    }
}

/// #583 (standard stage): build a doubled-EDGE subquery for a STANDARD
/// single-hop undirected OPTIONAL pattern, aliased AS the EDGE (relationship
/// alias) so it drops in as a replacement for the raw edge table in the
/// existing `FROM anchor LEFT JOIN <edge> LEFT JOIN neighbor` chain — mirroring
/// the merged denorm stage (`build_denorm_doubled_edge_subquery`), the only
/// difference being that a standard schema keeps the neighbor node in a SEPARATE
/// table (so the neighbor `LEFT JOIN` is left untouched, not embedded here).
///
/// The undirected split renders each direction as an independent chain
/// (`users AS a LEFT JOIN follows LEFT JOIN users b`, then a second arm driven
/// by `users AS b`) stapled with UNION ALL; each arm NULL-extends blind to the
/// other, so a one-role-only anchor gets a spurious `(anchor, NULL)` AND the
/// neighbor-driven second arm manufactures a phantom `(NULL, neighbor)` row
/// (#583). Instead the analyzer keeps ONE directed `was_undirected` GraphRel and
/// this helper replaces the raw edge table with:
///
/// ```sql
/// (SELECT e.<from_id>, e.<to_id>, e.<edge cols…> FROM <edge_table> AS e
///  UNION ALL
///  SELECT e.<to_id> AS <from_id>, e.<from_id> AS <to_id>, e.<edge cols…>
///         FROM <edge_table> AS e)
/// ```
///
/// aliased `AS <rel_alias>`. The reverse arm swaps `from_id`/`to_id` under their
/// ORIGINAL names, so the surrounding join conditions written for the raw edge
/// table (`<rel>.from_id = anchor.id` and `neighbor.id = <rel>.to_id`) work
/// UNCHANGED and now match BOTH orientations — a single anchor LEFT JOIN then
/// NULL-extends exactly once, only for genuinely zero-neighbor anchors. Because
/// the edge alias and ALL its physical columns (incl. the edge-id column
/// `count(r)` lowers to, sourced from `all_valid_physical_columns()`) are
/// exposed, `count(r)` / `r.<prop>` / `WHERE r.<prop>` / `RETURN r` all resolve
/// (unlike an AS-neighbor subquery, which hid the edge — the #1039 review's
/// BLOCKING-1). Proven live: 23 rows / one `(anchor, NULL)` for an isolated
/// node; `count(r)` correct (with the engine's `join_use_nulls=1`).
///
/// Every reference is table-qualified (`e.<col>`) so the reverse arm's
/// `e.<to_id> AS <from_id>` binds the RAW column, never the just-created alias
/// (ClickHouse would otherwise silently flip the value — mirrors the denorm
/// doubled-edge qualification discipline).
///
/// Returns `None` when the edge from/to id columns are missing — the caller
/// fails LOUD rather than emit a silently-single-direction join.
pub(super) fn build_standard_doubled_edge_subquery(
    edge_vs: &crate::query_planner::logical_plan::ViewScan,
    rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema,
) -> Option<String> {
    let q = crate::clickhouse_query_generator::quote_identifier;

    // Edge role id columns (single-column only; composite standard ids keep the
    // legacy two-arm path per the analyzer gate).
    let from_id = edge_vs.from_id.as_ref()?.first_column().to_string();
    let to_id = edge_vs.to_id.as_ref()?.first_column().to_string();
    let edge_table = &edge_vs.source_table;

    // Edge-own pass-through columns: EVERY physical column the relationship row
    // can carry (property_mappings + the edge identity column(s) — `edge_id`,
    // which `count(r)` lowers to and which need not be in `property_mappings`),
    // MINUS the from/to id columns handled by the explicit swap. Sourced from the
    // schema-catalog `all_valid_physical_columns()` so an edge-id-only column is
    // never dropped (dropping it would make `count(r)` reference a column the
    // subquery doesn't project → ClickHouse Code 47). Deterministic order.
    let role_cols: std::collections::HashSet<String> =
        [from_id.clone(), to_id.clone()].into_iter().collect();
    let mut passthrough: Vec<String> = rel_schema
        .all_valid_physical_columns()
        .into_iter()
        .filter(|c| !role_cols.contains(c))
        .collect();
    passthrough.sort();
    passthrough.dedup();

    // Forward arm: from/to verbatim, then every edge-own column.
    let mut forward_cols: Vec<String> = vec![
        format!("{e}.{}", q(&from_id), e = q(EDGE_ARM_ALIAS)),
        format!("{e}.{}", q(&to_id), e = q(EDGE_ARM_ALIAS)),
    ];
    for c in &passthrough {
        forward_cols.push(format!("{}.{}", q(EDGE_ARM_ALIAS), q(c)));
    }

    // Reverse arm: swap from/to under their ORIGINAL names; edge-own columns
    // unchanged. Table-qualified so the AS target never shadows the raw source.
    let mut reverse_cols: Vec<String> = vec![
        format!(
            "{e}.{} AS {}",
            q(&to_id),
            q(&from_id),
            e = q(EDGE_ARM_ALIAS)
        ),
        format!(
            "{e}.{} AS {}",
            q(&from_id),
            q(&to_id),
            e = q(EDGE_ARM_ALIAS)
        ),
    ];
    for c in &passthrough {
        reverse_cols.push(format!("{}.{}", q(EDGE_ARM_ALIAS), q(c)));
    }

    Some(format!(
        "(SELECT {fwd} FROM {table} AS {e} UNION ALL SELECT {rev} FROM {table} AS {e})",
        fwd = forward_cols.join(", "),
        rev = reverse_cols.join(", "),
        table = edge_table,
        e = q(EDGE_ARM_ALIAS),
    ))
}

/// Inner edge alias used inside [`build_standard_doubled_edge_subquery`]. Local
/// to the subquery; `e` matches the denorm helper's convention.
const EDGE_ARM_ALIAS: &str = "e";

/// #583 (polymorphic stage): build a doubled-edge subquery for a POLYMORPHIC
/// single-hop undirected OPTIONAL pattern. Same shape as
/// [`build_standard_doubled_edge_subquery`] (each edge row in both
/// orientations, from/to swapped under their original names, aliased AS the
/// edge), with TWO poly-specific differences:
///
///  1. **The type/label discriminator is folded into BOTH arms' WHERE.** A
///     polymorphic edge stores all types in one shared table
///     (`interactions`) discriminated by a type column plus per-endpoint label
///     columns. In the two-arm split path that predicate lives in the edge
///     join's `pre_filter`, which the SQL generator wraps as
///     `(SELECT * FROM <table> WHERE <pre_filter>)`. Here we fold it directly
///     into each arm and the caller CLEARS the join's `pre_filter` (otherwise
///     `Join::to_sql` would double-wrap). The filter string is the already-
///     dispatch-built `pre_filter` rendered without a table alias (bare column
///     names resolve inside the subquery), so no filter is reconstructed here
///     (CLAUDE.md rule 7 — the predicate came through the schema-catalog
///     `EdgeAccessStrategy` type/label filter APIs). The analyzer gate scopes
///     this to same-QUERY-LABEL endpoints, so the same symmetric filter is
///     correct on both arms (a cross-label poly edge would need different
///     endpoint-label predicates per arm and is excluded).
///  2. **Passthrough columns come from `doubled_edge_passthrough_columns()`**
///     (not `all_valid_physical_columns()`), because the former includes the
///     poly discriminator/label columns — needed so `RETURN r` / any
///     discriminator reference resolves, and harmless otherwise.
pub(super) fn build_polymorphic_doubled_edge_subquery(
    edge_vs: &crate::query_planner::logical_plan::ViewScan,
    rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema,
    discriminator_filter: &str,
) -> Option<String> {
    let q = crate::clickhouse_query_generator::quote_identifier;

    let from_id = edge_vs.from_id.as_ref()?.first_column().to_string();
    let to_id = edge_vs.to_id.as_ref()?.first_column().to_string();
    let edge_table = &edge_vs.source_table;

    // Poly discriminator columns MUST be in the passthrough set (unlike the
    // standard helper) so downstream references and the arms stay column-aligned.
    let role_cols: std::collections::HashSet<String> =
        [from_id.clone(), to_id.clone()].into_iter().collect();
    let mut passthrough: Vec<String> = rel_schema
        .doubled_edge_passthrough_columns()
        .into_iter()
        .filter(|c| !role_cols.contains(c))
        .collect();
    passthrough.sort();
    passthrough.dedup();

    let mut forward_cols: Vec<String> = vec![
        format!("{e}.{}", q(&from_id), e = q(EDGE_ARM_ALIAS)),
        format!("{e}.{}", q(&to_id), e = q(EDGE_ARM_ALIAS)),
    ];
    for c in &passthrough {
        forward_cols.push(format!("{}.{}", q(EDGE_ARM_ALIAS), q(c)));
    }

    let mut reverse_cols: Vec<String> = vec![
        format!(
            "{e}.{} AS {}",
            q(&to_id),
            q(&from_id),
            e = q(EDGE_ARM_ALIAS)
        ),
        format!(
            "{e}.{} AS {}",
            q(&from_id),
            q(&to_id),
            e = q(EDGE_ARM_ALIAS)
        ),
    ];
    for c in &passthrough {
        reverse_cols.push(format!("{}.{}", q(EDGE_ARM_ALIAS), q(c)));
    }

    // The discriminator filter is rendered without a table alias, so it applies
    // to the `e`-aliased scan inside each arm via bare column names.
    Some(format!(
        "(SELECT {fwd} FROM {table} AS {e} WHERE {filter} \
         UNION ALL SELECT {rev} FROM {table} AS {e} WHERE {filter})",
        fwd = forward_cols.join(", "),
        rev = reverse_cols.join(", "),
        table = edge_table,
        e = q(EDGE_ARM_ALIAS),
        filter = discriminator_filter,
    ))
}

/// #583: build a doubled-edge subquery for a denormalized single-hop undirected
/// OPTIONAL pattern, so the existing single anchor LEFT JOIN sees every physical
/// edge in BOTH orientations.
///
/// The undirected split renders each direction as an independent
/// `__denorm_scan_a LEFT JOIN <edge> ON a.code = <edge>.origin_code` (and the
/// reverse arm on `dest_code`) under UNION ALL; each arm NULL-extends blind to
/// the other, so a one-role-only anchor gets a spurious `(anchor, NULL)` (#583).
/// Instead, the analyzer keeps ONE directed `was_undirected` GraphRel and this
/// helper replaces the raw edge table with:
///
/// ```sql
/// (SELECT e.<all cols> FROM <edge_table> AS e
///  UNION ALL
///  SELECT e.<to-role cols AS from-role names>, e.<from-role cols AS to-role names>,
///         e.<edge-own cols> FROM <edge_table> AS e)
/// ```
///
/// so the outer join (`a.code = t1.origin_code`) and the neighbor projection
/// (`t1.dest_*`) are BYTE-IDENTICAL to the directed shape — only the join target
/// changes. NULL-extension then happens once, for genuinely zero-neighbor
/// anchors only (proven: 16 rows vs the buggy 18).
///
/// Column set is derived entirely from the edge `ViewScan` via the
/// schema-catalog `edge_side_node_properties` dispatch point (the from/to
/// role-property maps give the role-swap pairs by Cypher key; from_id/to_id
/// are the id swap pair; `property_mapping` values are edge-own pass-throughs).
/// Every reference is table-qualified (`e.<col>`) so the reverse arm's
/// `e.dest_code AS origin_code` binds the RAW column, never the just-created
/// alias (ClickHouse would otherwise silently flip the value — mirrors #617's
/// doubled-edge qualification discipline).
pub(super) fn build_denorm_doubled_edge_subquery(
    edge_vs: &crate::query_planner::logical_plan::ViewScan,
    rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema,
) -> Option<String> {
    let q = crate::clickhouse_query_generator::quote_identifier;

    // Role id columns (single-column only — composite denorm ids are out of
    // scope for #583's denorm stage; the analyzer gate resolves same-label
    // endpoints but composite ids ride a different family).
    let from_id = edge_vs.from_id.as_ref()?.first_column().to_string();
    let to_id = edge_vs.to_id.as_ref()?.first_column().to_string();

    // Role-property swap pairs, matched by Cypher property key present on BOTH
    // sides (e.g. `code` → (origin_code, dest_code), `city` → (origin_city,
    // dest_city)). Deterministic order. Routed through the schema-catalog
    // dispatch point (CLAUDE.md rule 7) rather than reading the raw ViewScan
    // role-property fields directly.
    let from_props =
        crate::graph_catalog::pattern_schema::edge_side_node_properties(edge_vs, true)?;
    let to_props = crate::graph_catalog::pattern_schema::edge_side_node_properties(edge_vs, false)?;
    let mut swap_pairs: Vec<(String, String)> = Vec::new(); // (from_col, to_col)
    let mut keys: Vec<&String> = from_props.keys().collect();
    keys.sort();
    for key in keys {
        let (Some(PropertyValue::Column(from_col)), Some(PropertyValue::Column(to_col))) =
            (from_props.get(key), to_props.get(key))
        else {
            continue;
        };
        swap_pairs.push((from_col.clone(), to_col.clone()));
    }
    // The id columns are role-swap columns too (they may or may not also appear
    // as a mapped property). Ensure they are present exactly once.
    if !swap_pairs.iter().any(|(f, _)| f == &from_id) {
        swap_pairs.push((from_id.clone(), to_id.clone()));
    }

    // Edge-own pass-through columns: EVERY physical column the relationship's
    // own row can carry (property_mappings, the edge identity column(s) —
    // `edge_id`, which `count(r)` lowers to and which is NOT necessarily in
    // `property_mappings` — and any denormalized role-property columns), MINUS
    // the role-swap columns handled above. Sourced from the schema-catalog
    // `all_valid_physical_columns()` so an edge-id-only column (e.g. a FLIGHT
    // edge's `flight_id` listed under `edge_id:` but not `property_mappings:`)
    // is never dropped — dropping it would make `count(r)` reference a column
    // the subquery doesn't project (ClickHouse Code 47). Deterministic order.
    let role_cols: HashSet<String> = swap_pairs
        .iter()
        .flat_map(|(f, t)| [f.clone(), t.clone()])
        .collect();
    let mut passthrough: Vec<String> = rel_schema
        .all_valid_physical_columns()
        .into_iter()
        .filter(|c| !role_cols.contains(c))
        .collect();
    passthrough.sort();
    passthrough.dedup();

    // Forward arm: every column verbatim.
    let mut forward_cols: Vec<String> = Vec::new();
    for (f, t) in &swap_pairs {
        forward_cols.push(format!("e.{}", q(f)));
        forward_cols.push(format!("e.{}", q(t)));
    }
    for c in &passthrough {
        forward_cols.push(format!("e.{}", q(c)));
    }

    // Reverse arm: swap each role pair (to-col AS from-name, from-col AS
    // to-name); edge-own columns unchanged. Table-qualified so the AS target
    // never shadows the raw source column.
    let mut reverse_cols: Vec<String> = Vec::new();
    for (f, t) in &swap_pairs {
        reverse_cols.push(format!("e.{} AS {}", q(t), q(f)));
        reverse_cols.push(format!("e.{} AS {}", q(f), q(t)));
    }
    for c in &passthrough {
        reverse_cols.push(format!("e.{}", q(c)));
    }

    let table = &edge_vs.source_table;
    Some(format!(
        "(SELECT {fwd} FROM {table} AS e UNION ALL SELECT {rev} FROM {table} AS e)",
        fwd = forward_cols.join(", "),
        rev = reverse_cols.join(", "),
    ))
}

/// #611: true when a denormalized standalone-scan Union appears ANYWHERE in
/// the subtree — not just as a direct child. A CHAINED optional clause over
/// a denormalized schema (`MATCH (a) OPTIONAL MATCH (a)-[f]->(b) OPTIONAL
/// MATCH (b)-[g]->(c) WHERE a.x`) has the `__denorm_scan` Union buried under
/// its left leg, so `is_optional_denorm_union_graphrel` (direct children
/// only) misses it; that render path builds joins per-fragment and swallows
/// the fold's loud error, silently dropping the fragment's JOIN. The gate
/// helper uses this to treat the whole denorm family as out of scope (old,
/// main-identical outer placement — tracked residual on #615).
pub(super) fn subtree_contains_denormalized_union(plan: &LogicalPlan) -> bool {
    if is_denormalized_union(plan) {
        return true;
    }
    match plan {
        LogicalPlan::GraphRel(gr) => {
            subtree_contains_denormalized_union(&gr.left)
                || subtree_contains_denormalized_union(&gr.center)
                || subtree_contains_denormalized_union(&gr.right)
        }
        LogicalPlan::GraphNode(gn) => subtree_contains_denormalized_union(&gn.input),
        LogicalPlan::Filter(f) => subtree_contains_denormalized_union(&f.input),
        LogicalPlan::Projection(p) => subtree_contains_denormalized_union(&p.input),
        LogicalPlan::CartesianProduct(cp) => {
            subtree_contains_denormalized_union(&cp.left)
                || subtree_contains_denormalized_union(&cp.right)
        }
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|i| subtree_contains_denormalized_union(i)),
        _ => false,
    }
}

/// Check if a GraphRel is an OPTIONAL denormalized pattern with a Union on
/// either side (standalone anchor node scan). This pattern requires special
/// CTE + LEFT JOIN rendering.
pub(super) fn is_optional_denorm_union_graphrel(
    gr: &crate::query_planner::logical_plan::GraphRel,
) -> bool {
    optional_denorm_union_anchor_is_left(gr).is_some()
}

/// Determine which side of a GraphRel carries the denormalized
/// standalone-scan anchor Union, for the special OPTIONAL CTE + LEFT JOIN
/// rendering path.
///
/// Returns `Some(true)` when the anchor Union is on the left (the common
/// outgoing-direction shape, e.g. `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`),
/// `Some(false)` when it's on the right — reached for shapes where
/// CLAUDE.md rule 4's anchor-aware FROM/JOIN reversal puts the pre-existing
/// anchor on the right connection (e.g. incoming-direction OPTIONAL MATCH,
/// `MATCH (a) OPTIONAL MATCH (a)<-[:R]-(b)`, #506) — or `None` if this
/// GraphRel isn't this special pattern at all.
pub(super) fn optional_denorm_union_anchor_is_left(
    gr: &crate::query_planner::logical_plan::GraphRel,
) -> Option<bool> {
    if gr.is_optional.unwrap_or(false) && gr.variable_length.is_none() {
        if is_denormalized_union(&gr.left) {
            return Some(true);
        }
        if is_denormalized_union(&gr.right) {
            return Some(false);
        }
    }
    None
}

/// #644: A denormalized OPTIONAL variable-length path renders its anchor as
/// `FROM <denorm_edge_table> AS a LEFT JOIN vlp_a_b AS vt0 ON a.<id> = vt0.start_id`.
/// Two problems:
///  1. The anchor JOIN key is the LOGICAL node id (`a.code`), but the denorm
///     edge table stores role-specific physical columns (`origin_code`/
///     `dest_code`) — there is no `code` column → ClickHouse Code 47.
///  2. Even if the JOIN key were remapped to `origin_code`, scanning the raw
///     edge table for the anchor enumerates only airports that appear as an
///     ORIGIN, silently dropping destination-only airports, and the
///     unaggregated edge rows fan out the LEFT-JOIN count (edge grain, not node
///     grain) — a SILENT wrong result.
///
/// The correct anchor is the same node-grain `__denorm_scan_{alias}` CTE the
/// single-hop denorm OPTIONAL path already builds (see `plan_builder.rs`'s
/// `optional_denorm_union_anchor_is_left` branch): a `UNION DISTINCT` of the
/// from-role and to-role physical columns, collapsed to one row per node id.
/// This helper post-processes a rendered GraphJoins plan for exactly the VLP
/// anchor shape, injecting that CTE, pointing FROM at it, and rewriting the
/// anchor-side VLP JOIN key to the CTE's physical id column.
///
/// Deliberately narrow — returns without modifying the plan (leaving the
/// existing LOUD Code 47 in place, per ground rule 1) for any shape it cannot
/// prove correct:
///  - non-denormalized anchor (standard / FK-edge / polymorphic) — untouched;
///  - more than one VLP LEFT JOIN (chained denorm VLP, #643-class) — LOUD;
///  - composite node id (`#646`-adjacent) — LOUD;
///  - FROM not bound to the raw denorm edge table under the anchor alias
///    (already a CTE, or a different plan shape) — untouched.
///
/// Consumes denorm classification via the schema-catalog `NodeSchema`
/// (`NodeSchema::denorm_role_properties` returns `Some` only for an
/// edge-hosted node, CLAUDE.md rule 7), never a raw plan-level flag or
/// table-name comparison.
pub(super) fn rewrite_denorm_optional_vlp_anchor_scan(
    render_plan: &mut super::RenderPlan,
    gj_input: &LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) {
    // The anchor is whatever alias the FROM clause binds. Bail unless the FROM
    // is a bare base-table reference under some alias (a `vlp_`/`__denorm_scan_`
    // CTE FROM means this isn't the raw-edge-table VLP anchor shape).
    let Some(from_ref) = render_plan.from.0.as_ref() else {
        return;
    };
    let Some(anchor_alias) = from_ref.alias.clone() else {
        return;
    };
    if from_ref.name.starts_with("vlp_") || from_ref.name.contains("__denorm_scan_") {
        return;
    }

    // Find the (single) optional VLP GraphRel whose anchor is this FROM alias.
    let Some(vlp_rel) = find_optional_vlp_graphrel_for_anchor(gj_input, &anchor_alias) else {
        return;
    };

    // An anchor-gate WHERE (`OPTIONAL MATCH (a)-[*]->(b) WHERE a.<prop> …`)
    // must fold into the LEFT JOIN's ON (NULL-extending anchors that fail the
    // gate). For denormalized schemas that fold is currently NOT performed
    // (#621's gate machinery is separately gated out for the denorm shape), so
    // the conjunct is dropped. On main that drop is masked by the anchor's own
    // Code 47 (the query fails loud). Rerouting the anchor to the scan CTE would
    // make it EXECUTE while still silently ignoring the gate — a loud→silent
    // regression. Keep this subshape LOUD by refusing to rewrite it (ground
    // rule 1); handling the denorm anchor gate is a separate follow-up.
    if vlp_rel.optional_anchor_where.is_some() {
        return;
    }

    // Resolve the anchor node label from whichever connection the FROM binds.
    // Restrict to the anchor-at-START layout (`(a)-[*]->(b)`, anchor is the
    // pattern's `left_connection`). The #647 end-anchored layout resolves the
    // anchor's own properties via the TO-role property map (`dest_code`), which
    // would not match this CTE's FROM-role physical column names — a separate,
    // unproven shape kept LOUD here (ground rule 1).
    if vlp_rel.left_connection != anchor_alias {
        return;
    }
    let Some(anchor_label) = get_node_label_for_alias(&anchor_alias, gj_input) else {
        return;
    };

    let Some(node_schema) = schema.node_schema_opt(&anchor_label) else {
        return;
    };

    // Composite node ids are out of scope (#646-adjacent) — keep LOUD.
    let id_cols = node_schema.node_id.columns();
    if id_cols.len() != 1 {
        return;
    }
    let logical_id = id_cols[0].to_string();

    // Schema-catalog denorm classification (CLAUDE.md rule 7): a node hosted on
    // an edge table exposes role-specific physical columns via
    // `denorm_role_properties`; a standard/FK-edge/polymorphic node returns
    // `None` for both roles → this returns without touching the plan (the shape
    // stays on its existing, correct render path). Both roles must be present to
    // build the from/to UNION scan.
    let (Some(from_props), Some(to_props)) = (
        node_schema.denorm_role_properties(true),
        node_schema.denorm_role_properties(false),
    ) else {
        return;
    };
    // Physical id column on each role (e.g. origin_code / dest_code).
    let (Some(from_id_col), Some(to_id_col)) =
        (from_props.get(&logical_id), to_props.get(&logical_id))
    else {
        return;
    };

    // Only rewrite when there is exactly ONE VLP LEFT JOIN and it references the
    // anchor alias with the logical id column. More than one VLP join is a
    // chained denorm VLP (#643-class) — keep it LOUD.
    let vlp_join_indices: Vec<usize> = render_plan
        .joins
        .0
        .iter()
        .enumerate()
        .filter(|(_, j)| j.table_name.starts_with("vlp_"))
        .map(|(i, _)| i)
        .collect();
    if vlp_join_indices.len() != 1 {
        return;
    }
    let vlp_join_idx = vlp_join_indices[0];

    // Confirm the anchor JOIN operand references the anchor alias + logical id,
    // and rewrite it to the CTE's physical id column. If it doesn't match the
    // expected shape, leave the plan untouched (stay LOUD).
    let mut rewrote_join = false;
    for cond in render_plan.joins.0[vlp_join_idx].joining_on.iter_mut() {
        for operand in cond.operands.iter_mut() {
            if let RenderExpr::PropertyAccessExp(pa) = operand {
                if pa.table_alias.0 == anchor_alias
                    && matches!(&pa.column, PropertyValue::Column(c) if c == &logical_id)
                {
                    pa.column = PropertyValue::Column(from_id_col.clone());
                    rewrote_join = true;
                }
            }
        }
    }
    if !rewrote_join {
        return;
    }

    // Build the node-grain `__denorm_scan_{alias}` CTE. Column set = the union
    // of both roles' physical column names; the id column is the GROUP BY key,
    // every other column takes a deterministic `min()` representative (node
    // grain, mirroring `wrap_denorm_scan_cte_at_node_grain`). Emitted physical
    // column names so the anchor's SELECT/GROUP BY (which already project
    // `a.origin_code` for this shape) resolve against the CTE unchanged.
    let full_table = node_schema.full_table_name();
    let cte_name = format!("__denorm_scan_{}", anchor_alias);

    // Deterministic ordering of the non-id physical columns.
    let mut other_cols: Vec<(String, String)> = Vec::new(); // (from_col, to_col) aligned by cypher prop
    let mut cypher_props: Vec<&String> = from_props.keys().collect();
    cypher_props.sort();
    for prop in cypher_props {
        if prop == &logical_id {
            continue;
        }
        if let (Some(fc), Some(tc)) = (from_props.get(prop), to_props.get(prop)) {
            other_cols.push((fc.clone(), tc.clone()));
        }
    }

    // Inner UNION DISTINCT: from-role branch then to-role branch. Each branch
    // projects the role's physical column under the FROM-role physical name so
    // both branches share one column schema.
    let mut from_select = format!("      s.{fc} AS \"{fc}\"", fc = from_id_col);
    let mut to_select = format!("      s.{tc} AS \"{fc}\"", tc = to_id_col, fc = from_id_col);
    for (fc, tc) in &other_cols {
        from_select.push_str(&format!(",\n      s.{fc} AS \"{fc}\"", fc = fc));
        to_select.push_str(&format!(",\n      s.{tc} AS \"{fc}\"", tc = tc, fc = fc));
    }
    let inner_sql = format!(
        "SELECT \n{from_select}\nFROM {tbl} AS s\nUNION DISTINCT \nSELECT \n{to_select}\nFROM {tbl} AS s",
        from_select = from_select,
        to_select = to_select,
        tbl = full_table,
    );

    // Wrap at node grain: GROUP BY the id column, min() every other column.
    let cte_sql = if other_cols.is_empty() {
        inner_sql
    } else {
        let mut grain_select = format!("      \"{id}\" AS \"{id}\"", id = from_id_col);
        for (fc, _) in &other_cols {
            grain_select.push_str(&format!(",\n      min(\"{fc}\") AS \"{fc}\"", fc = fc));
        }
        format!(
            "SELECT \n{grain_select}\nFROM (\n{inner}\n)\nGROUP BY \"{id}\"",
            grain_select = grain_select,
            inner = inner_sql,
            id = from_id_col,
        )
    };

    let cte = super::Cte::new(cte_name.clone(), super::CteContent::RawSql(cte_sql), false);
    render_plan.ctes.0.insert(0, cte);

    // Point the anchor FROM at the scan CTE (alias unchanged).
    if let Some(from_ref) = render_plan.from.0.as_mut() {
        from_ref.name = cte_name;
        from_ref.source = std::sync::Arc::new(LogicalPlan::Empty);
        from_ref.use_final = false;
    }

    // #683: the anchor's own aggregate/projection may still reference the LOGICAL
    // node id (`a.code`) — `count(a)` normalises (projection_tagging) to
    // `count(a.<node_id.columns().first()>)` = `count(a.code)`, which the
    // `__denorm_scan_{alias}` CTE never projects (it exposes the FROM-role
    // physical `origin_code`). The JOIN-key rewrite above already mapped the join
    // operand; mirror it across the SELECT / GROUP BY / HAVING / ORDER BY so the
    // anchor's logical-id references resolve to the same physical column. Scoped
    // to `(anchor_alias, logical_id)` only, so already-physical refs
    // (`a.origin_code`) and non-id properties (`a.city` → `a.origin_city`, already
    // resolved upstream) are untouched.
    for item in render_plan.select.items.iter_mut() {
        rewrite_anchor_logical_id_to_physical(
            &mut item.expression,
            &anchor_alias,
            &logical_id,
            from_id_col,
        );
    }
    for expr in render_plan.group_by.0.iter_mut() {
        rewrite_anchor_logical_id_to_physical(expr, &anchor_alias, &logical_id, from_id_col);
    }
    if let Some(having) = render_plan.having_clause.as_mut() {
        rewrite_anchor_logical_id_to_physical(having, &anchor_alias, &logical_id, from_id_col);
    }
    for item in render_plan.order_by.0.iter_mut() {
        rewrite_anchor_logical_id_to_physical(
            &mut item.expression,
            &anchor_alias,
            &logical_id,
            from_id_col,
        );
    }
}

/// #683: rewrite every `<anchor_alias>.<logical_id>` PropertyAccess in `expr` to
/// `<anchor_alias>.<physical_id>`. Used by `rewrite_denorm_optional_vlp_anchor_scan`
/// to align the anchor's logical-id references (e.g. the `count(a.code)` a bare
/// `count(a)` normalises to) with the `__denorm_scan_{alias}` CTE's physical id
/// column. Routed through the EXHAUSTIVE `map_render_expr` (§5 walker discipline)
/// so every wrapper variant (incl. ArraySubscript/Reduce/Map) is covered — a new
/// `RenderExpr` variant fails to compile rather than silently skipping a rewrite.
fn rewrite_anchor_logical_id_to_physical(
    expr: &mut RenderExpr,
    anchor_alias: &str,
    logical_id: &str,
    physical_id: &str,
) {
    let mut f = |e: &RenderExpr| -> crate::render_plan::render_expr::RenderRewrite {
        use crate::render_plan::render_expr::RenderRewrite;
        if let RenderExpr::PropertyAccessExp(pa) = e {
            if pa.table_alias.0 == anchor_alias
                && matches!(&pa.column, PropertyValue::Column(c) if c == logical_id)
            {
                return RenderRewrite::Replace(RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: pa.table_alias.clone(),
                    column: PropertyValue::Column(physical_id.to_string()),
                }));
            }
        }
        RenderRewrite::Recurse
    };
    *expr = crate::render_plan::render_expr::map_render_expr(expr, &mut f);
}

/// #644 helper: find the single OPTIONAL variable-length GraphRel in `plan`
/// whose start OR end connection is `anchor_alias`. Returns `None` if there is
/// no such VLP, or if more than one VLP GraphRel is present (chained VLP — the
/// caller keeps that shape LOUD).
fn find_optional_vlp_graphrel_for_anchor<'a>(
    plan: &'a LogicalPlan,
    anchor_alias: &str,
) -> Option<&'a crate::query_planner::logical_plan::GraphRel> {
    let mut found: Option<&crate::query_planner::logical_plan::GraphRel> = None;
    let mut vlp_count = 0usize;
    collect_optional_vlp_graphrels(plan, anchor_alias, &mut found, &mut vlp_count);
    if vlp_count == 1 {
        found
    } else {
        None
    }
}

fn collect_optional_vlp_graphrels<'a>(
    plan: &'a LogicalPlan,
    anchor_alias: &str,
    found: &mut Option<&'a crate::query_planner::logical_plan::GraphRel>,
    vlp_count: &mut usize,
) {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            if gr.variable_length.is_some() {
                *vlp_count += 1;
                if gr.is_optional.unwrap_or(false)
                    && (gr.left_connection == anchor_alias || gr.right_connection == anchor_alias)
                {
                    *found = Some(gr);
                }
            }
            collect_optional_vlp_graphrels(&gr.left, anchor_alias, found, vlp_count);
            collect_optional_vlp_graphrels(&gr.center, anchor_alias, found, vlp_count);
            collect_optional_vlp_graphrels(&gr.right, anchor_alias, found, vlp_count);
        }
        LogicalPlan::GraphJoins(gj) => {
            collect_optional_vlp_graphrels(&gj.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::GraphNode(gn) => {
            collect_optional_vlp_graphrels(&gn.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::Projection(p) => {
            collect_optional_vlp_graphrels(&p.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::GroupBy(gb) => {
            collect_optional_vlp_graphrels(&gb.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::Filter(f) => {
            collect_optional_vlp_graphrels(&f.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::OrderBy(o) => {
            collect_optional_vlp_graphrels(&o.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::Skip(s) => {
            collect_optional_vlp_graphrels(&s.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::Limit(l) => {
            collect_optional_vlp_graphrels(&l.input, anchor_alias, found, vlp_count)
        }
        LogicalPlan::Unwind(u) => {
            collect_optional_vlp_graphrels(&u.input, anchor_alias, found, vlp_count)
        }
        _ => {}
    }
}

/// #508: determine whether the OPTIONAL denorm CTE + LEFT JOIN anchor is
/// genuinely on the edge's FROM side or TO side — independent of
/// `anchor_is_left`, which only records a STRUCTURAL fact (which side of
/// THIS `GraphRel`'s plan tree the anchor's standalone scan Union sits on)
/// that can disagree with the edge's actual from/to role for the anchor's
/// label. Live-verified mismatch: a textually-reversed OPTIONAL MATCH
/// (`(rip)<-[:RESOLVED_TO]-(d)` instead of `(d)-[:RESOLVED_TO]->(rip)`)
/// flips `anchor_is_left` to `false` while `d` is STILL `RESOLVED_TO`'s
/// `from`-node — using `anchor_is_left` directly (as both the JOIN-key
/// derivation and the SELECT-list anchor-property rewrite previously did)
/// picked the edge's `to_id` column (`answers`, ResolvedIP's own column) for
/// the anchor, which either produced an impossible `1 = 0` JOIN (no anchor
/// property maps to it) or, worse, silently attributed the OTHER node's own
/// to-side property map onto the anchor's alias (label conflation: `rip.ip`
/// rendering as `d.ip`).
///
/// Tries the `anchor_is_left`-implied side first (preserves the existing
/// resolution for every already-working shape unchanged); falls back to the
/// opposite side only when the primary side's edge id column has no
/// property mapping anywhere in the anchor's own scan Union. Returns the
/// original `anchor_is_left` guess when NEITHER side resolves (a genuine
/// label-impossible hop, e.g. an `IP` anchor for `RESOLVED_TO` which expects
/// `Domain`) — unchanged from prior behavior, correctly falls through to the
/// impossible-join / no-mapping paths downstream.
pub(super) fn resolve_anchor_is_from_side(
    anchor_plan: &LogicalPlan,
    edge_vs: &crate::query_planner::logical_plan::ViewScan,
    anchor_is_left: bool,
) -> bool {
    let has_mapping_for = |is_from_side: bool, edge_id_col: &str| -> bool {
        let LogicalPlan::Union(union) = anchor_plan else {
            return false;
        };
        union.inputs.iter().any(|input| {
            let LogicalPlan::GraphNode(gn) = input.as_ref() else {
                return false;
            };
            let LogicalPlan::ViewScan(vs) = gn.input.as_ref() else {
                return false;
            };
            crate::graph_catalog::pattern_schema::edge_side_node_properties(vs, is_from_side)
                .is_some_and(|props| props.values().any(|v| v.raw() == edge_id_col))
        })
    };
    let from_id_col = edge_vs
        .from_id
        .as_ref()
        .map(|id| id.first_column().to_string());
    let to_id_col = edge_vs
        .to_id
        .as_ref()
        .map(|id| id.first_column().to_string());
    let (primary_is_from, primary_col, fallback_is_from, fallback_col) = if anchor_is_left {
        (true, from_id_col, false, to_id_col)
    } else {
        (false, to_id_col, true, from_id_col)
    };
    if primary_col.is_some_and(|col| has_mapping_for(primary_is_from, &col)) {
        return primary_is_from;
    }
    if fallback_col.is_some_and(|col| has_mapping_for(fallback_is_from, &col)) {
        return fallback_is_from;
    }
    anchor_is_left
}

/// Traverse through wrapper nodes (GraphJoins, Projection, GroupBy, etc.) to find
/// an OPTIONAL denormalized GraphRel with Union left. Returns the inner GraphRel
/// plan node if found.
pub(super) fn find_inner_optional_denorm_graphrel(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::GraphRel(gr) if is_optional_denorm_union_graphrel(gr) => Some(plan),
        // Not itself the special pattern — the anchor Union may be buried deeper
        // in a chain of nested optional hops (#505: `MATCH (a) OPTIONAL MATCH
        // (a)-[:R]->(b) OPTIONAL MATCH (b)-[:R]->(c)` nests as
        // `GraphRel(t2){ left: GraphRel(t1){ left: Union(a), ... }, ... }`, with
        // `a` having no required binding anywhere). Search both children for a
        // qualifying inner GraphRel — the caller is responsible for stitching
        // the outer hop(s)' already-correct JOINs back in (see #505 fix site).
        LogicalPlan::GraphRel(gr) => find_inner_optional_denorm_graphrel(&gr.left)
            .or_else(|| find_inner_optional_denorm_graphrel(&gr.right)),
        // NOTE: deliberately NO generic `LogicalPlan::Union` arm here. This
        // function has a second caller (`to_render_plan_with_ctx`'s top-level
        // OPTIONAL-denorm-Union detection, `plan_builder.rs` ~3596) that
        // relies on it returning `None` when `plan` ITSELF is the bidirectional
        // pattern's outer `Union` of two direction-permutation GraphRel
        // branches (built by `bidirectional_union.rs` for an undirected
        // pattern) — that caller's OWN union-of-two-branches rendering must
        // run instead (each branch gets its own correctly-anchored LEFT JOIN,
        // #507/B1). A blanket Union arm here was tried and reverted: it made
        // this top-level call SUCCEED for that outer Union too, incorrectly
        // delegating to a single inner branch's render and silently dropping
        // the other direction branch — confirmed via `MATCH (a:IP) OPTIONAL
        // MATCH (a)-[r:ACCESSED]-(b:IP) RETURN a.ip, a.port, count(r) AS c`
        // (the B1 golden test's own repro) losing its outer SELECT/second
        // UNION branch entirely. #529 shape 2 (an undirected OPTIONAL pattern
        // feeding a `WITH`-aggregate) is instead fixed at its OWN narrower
        // caller, `denorm_scan_cte_anchor_names_and_id_col` below, which
        // unwraps a `Union` of GraphRel branches itself before delegating
        // to this function per-branch — see that function's own comment.
        LogicalPlan::GraphJoins(gj) => find_inner_optional_denorm_graphrel(&gj.input),
        LogicalPlan::Projection(p) => find_inner_optional_denorm_graphrel(&p.input),
        LogicalPlan::GroupBy(gb) => find_inner_optional_denorm_graphrel(&gb.input),
        LogicalPlan::Filter(f) => find_inner_optional_denorm_graphrel(&f.input),
        LogicalPlan::OrderBy(o) => find_inner_optional_denorm_graphrel(&o.input),
        LogicalPlan::Limit(l) => find_inner_optional_denorm_graphrel(&l.input),
        LogicalPlan::Skip(s) => find_inner_optional_denorm_graphrel(&s.input),
        _ => None,
    }
}

/// #590: locate a `CartesianProduct` (through the same query-modifier wrappers as
/// `find_inner_optional_denorm_graphrel`) whose BOTH arms are independent
/// denormalized OPTIONAL subtrees — the product of the
/// `OptionalCartesianDistribution` analyzer pass on a disconnected multi-anchor
/// query like `MATCH (a:Airport),(x:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b)
/// OPTIONAL MATCH (x)-[:FLIGHT]-(y)`. Returns the `CartesianProduct` plan node so
/// the render layer can render each arm through the single-anchor
/// `__denorm_scan_{alias}` machinery and merge (CTEs + CROSS JOIN).
///
/// An "independent denormalized OPTIONAL subtree" is a subtree that itself
/// contains a denorm-optional GraphRel (`find_inner_optional_denorm_graphrel`
/// succeeds) — this covers both a single optional hop (`GraphRel(opt){Union}`)
/// and a chained one (`GraphRel(opt){GraphRel(opt){Union}}`). Deliberately
/// requires the pattern on BOTH arms so the single-anchor path (which has no
/// CartesianProduct) is never diverted here.
pub(super) fn find_cartesian_of_denorm_optionals(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        // Require each arm to be a DIRECTED single-anchor optional-denorm subtree
        // (`find_inner_optional_denorm_graphrel` — which deliberately has NO Union
        // arm, so a direction-split UNDIRECTED arm never matches). The undirected
        // multi-anchor shape (`CP(Union[GR_out,GR_in], …)`) is intentionally left to
        // the generic path rather than composed here — composing two per-arm UNION
        // renders under a CROSS JOIN needs machinery this arm doesn't have, and a
        // partial attempt emits invalid SQL. See #590 notes.
        LogicalPlan::CartesianProduct(cp)
            if find_inner_optional_denorm_graphrel(&cp.left).is_some()
                && find_inner_optional_denorm_graphrel(&cp.right).is_some() =>
        {
            Some(plan)
        }
        LogicalPlan::GraphJoins(gj) => find_cartesian_of_denorm_optionals(&gj.input),
        LogicalPlan::Projection(p) => find_cartesian_of_denorm_optionals(&p.input),
        LogicalPlan::GroupBy(gb) => find_cartesian_of_denorm_optionals(&gb.input),
        LogicalPlan::Filter(f) => find_cartesian_of_denorm_optionals(&f.input),
        LogicalPlan::OrderBy(o) => find_cartesian_of_denorm_optionals(&o.input),
        LogicalPlan::Limit(l) => find_cartesian_of_denorm_optionals(&l.input),
        LogicalPlan::Skip(s) => find_cartesian_of_denorm_optionals(&s.input),
        _ => None,
    }
}

/// #590 SAFETY GUARD: true when the plan contains a `CartesianProduct` at least
/// one of whose arms is (or wraps, as its anchor) a DENORMALIZED standalone
/// node-scan Union — i.e. a *disconnected multi-anchor denormalized* pattern, the
/// exact shape `combine_node_with_existing_plan` now materializes so BOTH anchors
/// survive into the plan (#590 planner fix).
///
/// Only ONE such shape — a directed CartesianProduct whose BOTH arms are
/// single-anchor optional-denorm subtrees — has a correct render path
/// (`find_cartesian_of_denorm_optionals` + the #590 render arm, which returns
/// BEFORE this guard is consulted). EVERY other disconnected-multi-anchor
/// denormalized shape (one anchor optional, undirected, 3+ anchors, chained
/// through the optional endpoint, base non-optional) currently falls to the
/// generic render path, which either silently mis-attributes edge columns onto
/// anchor aliases with `ON 1 = 1` (silent-wrong — ground-rule-1 violation) or
/// emits dangling anchor references (`Code 47` from ClickHouse). Before the
/// planner fix these all failed loudly (the second anchor was dropped, so the
/// generic path produced an honest `UNKNOWN_IDENTIFIER`); the fix must not
/// convert an honest error into silent-wrong output.
///
/// The render layer consults this AFTER the one handled shape has returned, and
/// raises a loud `InvalidRenderPlan` when it fires — preserving the
/// honest-failure contract for every not-yet-supported denormalized disconnected
/// shape. NON-denormalized disconnected patterns (`MATCH (a:User),(x:User)`,
/// #601) never carry a denorm node-scan Union, so this never fires for them.
pub(super) fn contains_disconnected_denorm_cartesian(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CartesianProduct(cp) => {
            // A disconnected denorm anchor pattern: at least one arm's subtree
            // carries a denormalized standalone node-scan Union.
            subtree_contains_denormalized_union(&cp.left)
                || subtree_contains_denormalized_union(&cp.right)
                // …or a denorm cartesian nested deeper in either arm (3+ anchors).
                || contains_disconnected_denorm_cartesian(&cp.left)
                || contains_disconnected_denorm_cartesian(&cp.right)
        }
        LogicalPlan::GraphRel(gr) => {
            contains_disconnected_denorm_cartesian(&gr.left)
                || contains_disconnected_denorm_cartesian(&gr.center)
                || contains_disconnected_denorm_cartesian(&gr.right)
        }
        LogicalPlan::GraphNode(gn) => contains_disconnected_denorm_cartesian(&gn.input),
        LogicalPlan::GraphJoins(gj) => contains_disconnected_denorm_cartesian(&gj.input),
        LogicalPlan::Projection(p) => contains_disconnected_denorm_cartesian(&p.input),
        LogicalPlan::GroupBy(gb) => contains_disconnected_denorm_cartesian(&gb.input),
        LogicalPlan::Filter(f) => contains_disconnected_denorm_cartesian(&f.input),
        LogicalPlan::OrderBy(o) => contains_disconnected_denorm_cartesian(&o.input),
        LogicalPlan::Limit(l) => contains_disconnected_denorm_cartesian(&l.input),
        LogicalPlan::Skip(s) => contains_disconnected_denorm_cartesian(&s.input),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|i| contains_disconnected_denorm_cartesian(i)),
        _ => false,
    }
}
/// pattern (the `__denorm_scan_{alias}` CTE built in `plan_builder.rs`,
/// shared machinery with #502/#505/#506/#507), return the Cypher property
/// name that CTE exposes for the node's identity column — the SAME
/// forward-resolution #475 already does for the SELECT list, needed here for
/// the WITH-clause's GROUP BY key construction
/// (`expand_table_alias_to_group_by_id_only` in `plan_builder_utils.rs`).
///
/// That function's generic ID lookups (CTE-schema registry, then
/// `find_id_column_for_alias` reading the raw `ViewScan.id_column`) don't
/// know about this special CTE at all — it isn't registered in the general
/// `cte_schemas`/`cte_references` maps the WITH-CTE compiler tracks — so
/// they fall through to the RAW physical column name (e.g. `"id.orig_h"`),
/// producing `GROUP BY a."id.orig_h"` against a CTE that only exposes the
/// Cypher property name (`"ip"`) — invalid SQL. This function is checked
/// FIRST, before those generic fallbacks, whenever the alias might be this
/// special anchor.
///
/// Returns `None` (caller falls through to the generic lookups, unaffected)
/// when `alias` isn't this anchor pattern, or when the anchor's identity
/// property can't be forward-resolved (mirrors #507's same-shaped
/// resolution, which also returns `None` conservatively in that case).
pub(super) fn denorm_scan_cte_anchor_id_property(
    plan: &LogicalPlan,
    alias: &str,
) -> Option<String> {
    let (id_candidates, _all_names, id_col) = denorm_scan_cte_anchor_names_and_id_col(plan, alias)?;

    // A node's role-specific property map can hold BOTH a genuine Cypher
    // alias for the id column (e.g. `ip -> id.orig_h`) AND a raw
    // self-mapping entry (`id.orig_h -> id.orig_h`, present when the schema
    // also declares the physical column name as its own Cypher property).
    // The CTE only ever exposes the FORMER as a real column — the
    // self-mapped entry's "property name" IS the raw db column text, so
    // preferring it would reproduce exactly the bug this function fixes.
    // Exclude self-mapping candidates first; only fall back to one if that
    // leaves nothing (still better than the raw `ViewScan.id_column` the
    // caller would otherwise use).
    let mut candidates: Vec<&String> = id_candidates.iter().filter(|k| *k != &id_col).collect();
    if candidates.is_empty() {
        candidates = id_candidates.iter().collect();
    }
    candidates.sort();
    candidates.into_iter().next().cloned()
}

/// #510 (SELECT-list sibling): the same anchor-detection this module already
/// does for the GROUP BY key (`denorm_scan_cte_anchor_id_property`), but
/// returning EVERY Cypher property the `__denorm_scan_{alias}` CTE exposes
/// — `(cypher_name, cte_column_name)` pairs, where the CTE column name is
/// identical to the Cypher name (the anchor scan CTE projects each
/// role-specific property entry under its own Cypher property name, never a
/// raw db column). Self-mapping entries (property name literally equal to
/// the raw db column text — see `denorm_scan_cte_anchor_id_property`'s doc)
/// are excluded so callers never emit a reference to a column the CTE
/// doesn't actually have.
///
/// Used by `expand_table_alias_to_select_items` (`plan_builder_utils.rs`) to
/// build the WITH-clause's auto-expanded SELECT items for a pass-through
/// anchor alias (`WITH a, count(r) AS c`): that function's generic lookup
/// (`LogicalPlan::get_properties_with_table_alias`) resolves alias binding
/// STRUCTURALLY against the ORIGINAL (pre-render) LogicalPlan tree, where
/// the anchor node is still nested inside the OPTIONAL edge's GraphRel — so
/// it (correctly, for the ordinary embedded-denorm case #493/#475 target)
/// returns the EDGE alias as the "actual" table alias. For THIS special
/// CTE + LEFT JOIN pattern that's wrong: post-render, the anchor's own
/// properties come from the `__denorm_scan_{alias}` CTE, not the
/// LEFT-JOINed (and NULL-extended on an OPTIONAL-miss row) edge alias.
pub(super) fn denorm_scan_cte_anchor_properties(
    plan: &LogicalPlan,
    alias: &str,
) -> Option<Vec<(String, String)>> {
    let (_id_candidates, all_names, id_col) = denorm_scan_cte_anchor_names_and_id_col(plan, alias)?;
    let exposed: Vec<(String, String)> = all_names
        .iter()
        .filter(|k| k.as_str() != id_col.as_str())
        .map(|k| (k.clone(), k.clone()))
        .collect();
    if exposed.is_empty() {
        None
    } else {
        Some(exposed)
    }
}

/// Shared lookup behind `denorm_scan_cte_anchor_id_property` and
/// `denorm_scan_cte_anchor_properties`: find the anchor Union node for
/// `alias` (if `alias` is the anchor of an OPTIONAL denorm CTE + LEFT JOIN
/// pattern) and return (id property name candidates, every Cypher property
/// name the CTE exposes, the matched branch's physical `id_column`).
///
/// #520/B1: an EARLIER version of this function used
/// `edge_side_node_properties(vs, anchor_is_left)` — the property map for
/// ONLY the role matching the CURRENT rendering context's `anchor_is_left`.
/// That silently failed whenever the schema's canonical `id_column` happens
/// to equal only the OTHER role's physical column (i.e. whenever the
/// role-specific property maps' `ip -> id.orig_h` / `ip -> id.resp_h` mappings
/// use DIFFERENT physical columns for the same conceptual identity — the
/// norm for a coupled cross-table schema like zeek's `IP`). Confirmed via a
/// live repro: an UNDIRECTED pattern's second UnionDistribution branch (the
/// `anchor_is_left=false` invocation) hit exactly this — its role's `ip`
/// mapping is `id.resp_h`, which never matches `id_column="id.orig_h"`, so
/// `node_id_prop_name` silently resolved to `None` and the #507 node-grain
/// wrap was skipped for that branch's CTE — a SILENT wrong-result bug
/// (per-node counts fragmenting across grain rows), not a loud failure.
///
/// Fix: search EVERY Union branch's BOTH from- and to-node-properties maps
/// (not just the one role `anchor_is_left` currently cares about) for a
/// value matching `id_column`. The anchor scan CTE always exposes the union
/// of every role's Cypher property NAMES identically (one scan branch per
/// role, all aliased to the same shared property names) — only the
/// candidate-column VALUE differs per role — so it is always safe to accept
/// a match found via any role, regardless of which role the current
/// rendering context happens to need.
fn denorm_scan_cte_anchor_names_and_id_col(
    plan: &LogicalPlan,
    alias: &str,
) -> Option<(
    std::collections::HashSet<String>,
    std::collections::HashSet<String>,
    String,
)> {
    // #529 shape 2: an UNDIRECTED OPTIONAL MATCH is expanded (by
    // `bidirectional_union.rs`) into a `Union` of two direction-permutation
    // GraphRel branches BEFORE this traversal ever runs — e.g. `MATCH (a)
    // OPTIONAL MATCH (a)-[r]-(b) WITH a, count(r) AS c ...` on a coupled
    // schema. `find_inner_optional_denorm_graphrel` deliberately has NO
    // generic `Union` arm (a DIFFERENT caller, `to_render_plan_with_ctx`'s
    // top-level detection, needs it to return `None` for exactly this shape
    // of Union — see that function's own comment), so unwrap it HERE
    // instead, scoped to only this helper's two callers
    // (`denorm_scan_cte_anchor_id_property`/`_properties`, used by the
    // WITH-clause GROUP BY / SELECT expansion in `plan_builder_utils.rs`).
    // Without this, the anchor was invisible to this helper for an undirected
    // OPTIONAL pattern, so callers fell back to the raw physical db column —
    // invalid SQL (`GROUP BY a."id.orig_h"` against a CTE that only exposes
    // `"ip"`).
    if let LogicalPlan::Union(u) = plan {
        return u
            .inputs
            .iter()
            .find_map(|branch| denorm_scan_cte_anchor_names_and_id_col(branch, alias));
    }
    let inner = find_inner_optional_denorm_graphrel(plan)?;
    let LogicalPlan::GraphRel(gr) = inner else {
        return None;
    };
    let anchor_is_left = optional_denorm_union_anchor_is_left(gr)?;
    let anchor_plan: &LogicalPlan = if anchor_is_left {
        gr.left.as_ref()
    } else {
        gr.right.as_ref()
    };
    let LogicalPlan::Union(union) = anchor_plan else {
        return None;
    };

    // Confirm this Union's own node alias is the one we're looking for —
    // `find_inner_optional_denorm_graphrel` searches the WHOLE plan tree
    // (including nested/chained OPTIONAL hops, #505), so it may return a
    // DIFFERENT hop's anchor than the alias we were asked about.
    let node_alias = union.inputs.first().and_then(|input| {
        if let LogicalPlan::GraphNode(gn) = input.as_ref() {
            Some(gn.alias.clone())
        } else {
            None
        }
    })?;
    if node_alias != alias {
        return None;
    }

    let mut id_col: Option<String> = None;
    let mut all_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut id_candidates: std::collections::HashSet<String> = std::collections::HashSet::new();

    for input in &union.inputs {
        let LogicalPlan::GraphNode(gn) = input.as_ref() else {
            continue;
        };
        let LogicalPlan::ViewScan(vs) = gn.input.as_ref() else {
            continue;
        };
        if id_col.is_none() {
            id_col = Some(vs.id_column.clone());
        }
        let this_id_col = id_col.as_deref().unwrap_or_default();
        // Route through the schema-catalog dispatch API for BOTH roles
        // (CLAUDE.md rule 7) rather than reading the role-specific property
        // fields on `vs` directly — same API `edge_side_node_properties`
        // already used elsewhere in this module, just called for both sides
        // instead of only the caller's current `anchor_is_left`.
        for props in [
            crate::graph_catalog::pattern_schema::edge_side_node_properties(vs, true),
            crate::graph_catalog::pattern_schema::edge_side_node_properties(vs, false),
        ]
        .into_iter()
        .flatten()
        {
            for (name, value) in props {
                all_names.insert(name.clone());
                if value.raw() == this_id_col {
                    id_candidates.insert(name.clone());
                }
            }
        }
    }

    let id_col = id_col?;
    if all_names.is_empty() {
        return None;
    }
    Some((id_candidates, all_names, id_col))
}

/// #582: reverse of `denorm_scan_cte_anchor_properties` — given a PHYSICAL
/// db-column name (e.g. `id.orig_h`) that an earlier analysis pass
/// structurally rewrote a Cypher property to, find the Cypher property
/// name `alias`'s `__denorm_scan_{alias}` CTE actually exposes for it.
///
/// Shares `denorm_scan_cte_anchor_names_and_id_col`'s anchor-detection
/// traversal (`find_inner_optional_denorm_graphrel` +
/// `optional_denorm_union_anchor_is_left`) but walks the ANCHOR'S OWN scan
/// Union's `edge_side_node_properties` (both roles, same #520/B1 reasoning)
/// directly, rather than reusing whichever edge happens to be the CURRENT
/// GraphRel's `center` — those can disagree. A label-impossible OPTIONAL
/// MATCH (e.g. `MATCH (a:IP) OPTIONAL MATCH (a)-[:RESOLVED_TO]->(b)` on the
/// zeek coupled schema, where `RESOLVED_TO` connects Domain/ResolvedIP, not
/// IP, and renders as a `1 = 0` fallback join) still builds `a`'s anchor CTE
/// from IP's own node-property definition — a lookup keyed off the CURRENT
/// (label-mismatched) edge's own property maps finds nothing there, leaving
/// the physical column unresolved.
///
/// Returns `None` (caller leaves the column untouched) when `alias` isn't
/// this anchor pattern, or no property's value matches `physical_col`.
fn denorm_scan_cte_anchor_reverse_property(
    plan: &LogicalPlan,
    alias: &str,
    physical_col: &str,
) -> Option<String> {
    if let LogicalPlan::Union(u) = plan {
        return u.inputs.iter().find_map(|branch| {
            denorm_scan_cte_anchor_reverse_property(branch, alias, physical_col)
        });
    }
    let inner = find_inner_optional_denorm_graphrel(plan)?;
    let LogicalPlan::GraphRel(gr) = inner else {
        return None;
    };
    let anchor_is_left = optional_denorm_union_anchor_is_left(gr)?;
    let anchor_plan: &LogicalPlan = if anchor_is_left {
        gr.left.as_ref()
    } else {
        gr.right.as_ref()
    };
    let LogicalPlan::Union(union) = anchor_plan else {
        return None;
    };

    // Confirm this Union's own node alias is the one we were asked about —
    // `find_inner_optional_denorm_graphrel` searches the WHOLE plan tree
    // (chained OPTIONAL hops, #505), so it may return a DIFFERENT hop's
    // anchor than `alias`.
    let node_alias = union.inputs.first().and_then(|input| {
        if let LogicalPlan::GraphNode(gn) = input.as_ref() {
            Some(gn.alias.clone())
        } else {
            None
        }
    })?;
    if node_alias != alias {
        return None;
    }

    // Collect EVERY Cypher name whose value matches `physical_col` across
    // both roles, rather than returning on the first match found while
    // iterating a `HashMap` (`edge_side_node_properties`'s property maps
    // aren't insertion-ordered — per-process-random iteration order, the
    // #480 class of nondeterminism). A node's role-specific property map
    // can hold BOTH a genuine Cypher alias for a column (e.g. `ip ->
    // id.orig_h`) AND a raw self-mapping entry (`id.orig_h -> id.orig_h`,
    // present when the schema also declares the physical column name as its
    // own Cypher property) — the CTE only ever exposes the FORMER as a real
    // column (see `denorm_scan_cte_anchor_id_property`'s identical guard),
    // so self-mapping candidates are excluded first and only used as a
    // fallback if nothing else matches. Sorting the remaining candidates
    // makes the pick fully deterministic even when the schema genuinely
    // maps two different Cypher names to the same physical column.
    let mut candidates: Vec<String> = Vec::new();
    for input in &union.inputs {
        let LogicalPlan::GraphNode(gn) = input.as_ref() else {
            continue;
        };
        let LogicalPlan::ViewScan(vs) = gn.input.as_ref() else {
            continue;
        };
        for props in [
            crate::graph_catalog::pattern_schema::edge_side_node_properties(vs, true),
            crate::graph_catalog::pattern_schema::edge_side_node_properties(vs, false),
        ]
        .into_iter()
        .flatten()
        {
            for (name, value) in props {
                if value.raw() == physical_col {
                    candidates.push(name.clone());
                }
            }
        }
    }
    if candidates.is_empty() {
        return None;
    }
    let non_self_mapped: Vec<&String> = candidates.iter().filter(|n| *n != physical_col).collect();
    if !non_self_mapped.is_empty() {
        let mut sorted = non_self_mapped;
        sorted.sort();
        return sorted.first().map(|s| s.to_string());
    }
    candidates.sort();
    candidates.into_iter().next()
}

/// Clone `plan`, clearing `anchor_connection` on every `GraphRel` node
/// encountered.
///
/// Used ONLY when re-extracting the outer WHERE clause for the special
/// denormalized CTE + LEFT JOIN rendering path (see the
/// `optional_denorm_union_anchor_is_left` fix site in `plan_builder.rs`).
/// `collect_graphrel_predicates` deliberately drops a predicate that
/// references ONLY the non-anchor ("optional") alias when `anchor_connection`
/// is set, on the assumption that some downstream mechanism (a JOIN
/// `pre_filter`) picks it up instead. The denorm CTE + LEFT JOIN path has no
/// such downstream mechanism, so that predicate would simply vanish — a
/// regression exposed once #506 started setting `anchor_connection` for
/// incoming-direction OPTIONAL MATCH (outgoing-direction queries never hit
/// this drop because their `anchor_connection` is `None` by construction,
/// CLAUDE.md rule 4, which routes `collect_graphrel_predicates` through its
/// "no anchor determined — keep all predicates" fallback instead).
///
/// Clearing `anchor_connection` here reproduces that same "keep all"
/// fallback for filter-extraction purposes only. This clone is discarded
/// immediately after use — FROM/JOIN construction (which legitimately needs
/// `anchor_connection` for the anchor-aware reversal) is built from the
/// original, unmodified plan and is completely unaffected.
pub(super) fn clear_anchor_connection_for_filters(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            let mut gr = gr.clone();
            gr.anchor_connection = None;
            gr.left = std::sync::Arc::new(clear_anchor_connection_for_filters(&gr.left));
            gr.center = std::sync::Arc::new(clear_anchor_connection_for_filters(&gr.center));
            gr.right = std::sync::Arc::new(clear_anchor_connection_for_filters(&gr.right));
            LogicalPlan::GraphRel(gr)
        }
        LogicalPlan::GraphJoins(gj) => {
            let mut gj = gj.clone();
            gj.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&gj.input));
            LogicalPlan::GraphJoins(gj)
        }
        LogicalPlan::Projection(p) => {
            let mut p = p.clone();
            p.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&p.input));
            LogicalPlan::Projection(p)
        }
        LogicalPlan::Filter(f) => {
            let mut f = f.clone();
            f.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&f.input));
            LogicalPlan::Filter(f)
        }
        LogicalPlan::GroupBy(gb) => {
            let mut gb = gb.clone();
            gb.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&gb.input));
            LogicalPlan::GroupBy(gb)
        }
        LogicalPlan::OrderBy(o) => {
            let mut o = o.clone();
            o.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&o.input));
            LogicalPlan::OrderBy(o)
        }
        LogicalPlan::Limit(l) => {
            let mut l = l.clone();
            l.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&l.input));
            LogicalPlan::Limit(l)
        }
        LogicalPlan::Skip(s) => {
            let mut s = s.clone();
            s.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&s.input));
            LogicalPlan::Skip(s)
        }
        LogicalPlan::GraphNode(gn) => {
            let mut gn = gn.clone();
            gn.input = std::sync::Arc::new(clear_anchor_connection_for_filters(&gn.input));
            LogicalPlan::GraphNode(gn)
        }
        LogicalPlan::Union(u) => {
            let mut u = u.clone();
            u.inputs = u
                .inputs
                .iter()
                .map(|i| std::sync::Arc::new(clear_anchor_connection_for_filters(i)))
                .collect();
            LogicalPlan::Union(u)
        }
        other => other.clone(),
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
///
/// NOTE: For GraphRel, this returns the relationship table (center), which is correct
/// for most use cases. If you need the END NODE table from a nested GraphRel,
/// use `extract_end_node_table_name` instead.
pub(super) fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        // For CTEs, return the CTE name directly (don't recurse into input)
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_table_name(&node.input),
        LogicalPlan::GraphRel(rel) => extract_table_name(&rel.center),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        // For WithClause, return the CTE name (always set by analysis phase)
        LogicalPlan::WithClause(wc) => wc.cte_name.clone(),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_table_name(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to extract the END NODE table name from a LogicalPlan node.
///
/// CRITICAL: For nested GraphRel patterns (multi-hop traversals), this extracts
/// the rightmost/terminal node's table, NOT the relationship table.
///
/// Example: For `(a)-[:REL1]-(b)-[:REL2]-(c)` represented as:
///   GraphRel { left: GraphNode(a), center: REL1, right: GraphRel { left: b, center: REL2, right: c } }
///
/// - `extract_table_name` on the outer GraphRel would return REL1's table (WRONG for end node)
/// - `extract_end_node_table_name` on the outer GraphRel.right would return c's table (CORRECT)
pub(super) fn extract_end_node_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_end_node_table_name(&node.input),
        // CRITICAL: For GraphRel, extract from the RIGHT side (end node), not CENTER (relationship)
        LogicalPlan::GraphRel(rel) => extract_end_node_table_name(&rel.right),
        LogicalPlan::Filter(filter) => extract_end_node_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_end_node_table_name(&proj.input),
        // For WithClause, return the CTE name (always set by analysis phase)
        LogicalPlan::WithClause(wc) => wc.cte_name.clone(),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_end_node_table_name(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the ID column of the END NODE in a potentially nested GraphRel pattern.
///
/// Similar to `extract_end_node_table_name`, but for ID columns.
/// For nested patterns like (a)-[r1]->(b)-[r2]->(c), when called on the outer GraphRel.right,
/// this traverses through inner GraphRels to find the actual end node's ID column.
///
/// The difference from `extract_id_column` is:
/// - `extract_id_column(&GraphRel)` returns rel.center's ID (relationship table's ID)
/// - `extract_end_node_id_column(&GraphRel)` returns the actual end node's ID (via rel.right)
pub(super) fn extract_end_node_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_end_node_id_column(&node.input),
        // CRITICAL: For GraphRel, extract from the RIGHT side (end node), not CENTER (relationship)
        LogicalPlan::GraphRel(rel) => extract_end_node_id_column(&rel.right),
        LogicalPlan::Filter(filter) => extract_end_node_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_end_node_id_column(&proj.input),
        // For Union (denormalized nodes), extract from first branch
        LogicalPlan::Union(union) => {
            if !union.inputs.is_empty() {
                extract_end_node_id_column(&union.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to extract the table reference with parameterized view syntax if applicable.
/// For a ViewScan with view_parameter_names, returns `table_name(param1='value1', param2='value2')`.
/// For other cases, returns just the table name.
///
/// This is used for JOINs where parameterized views need to be called with parameters.
/// Example: `JOIN friendships_by_tenant(tenant_id='acme') AS f ON ...`
pub(super) fn extract_parameterized_table_ref(plan: &LogicalPlan) -> Option<String> {
    match plan {
        // For CTEs, return the CTE name directly (no parameters)
        LogicalPlan::Cte(cte) => Some(cte.name.clone()),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = (
                &view_scan.view_parameter_names,
                &view_scan.view_parameter_values,
            ) {
                if !param_names.is_empty() {
                    // Generate parameterized view call with actual values: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                // Escape single quotes in value for SQL safety
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();

                    if param_pairs.is_empty() {
                        log::debug!(
                            "extract_parameterized_table_ref: ViewScan '{}' expects parameters {:?} but none matched in values",
                            view_scan.source_table, param_names
                        );
                        return Some(view_scan.source_table.clone());
                    }

                    log::debug!(
                        "extract_parameterized_table_ref: ViewScan '{}' generating: {}({})",
                        view_scan.source_table,
                        view_scan.source_table,
                        param_pairs.join(", ")
                    );
                    return Some(format!(
                        "{}({})",
                        view_scan.source_table,
                        param_pairs.join(", ")
                    ));
                }
            }
            // No parameters - return plain table name
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => extract_parameterized_table_ref(&node.input),
        LogicalPlan::GraphRel(rel) => extract_parameterized_table_ref(&rel.center),
        LogicalPlan::Filter(filter) => extract_parameterized_table_ref(&filter.input),
        LogicalPlan::Projection(proj) => extract_parameterized_table_ref(&proj.input),
        _ => None,
    }
}

/// Extract a mapping of alias → parameterized table reference from a LogicalPlan tree.
///
/// This traverses the plan and builds a HashMap where:
/// - Keys are aliases (from GraphNode.alias or GraphRel.alias)
/// - Values are table references with parameterized view syntax if applicable
///
/// For parameterized views, the value will be `table(param = $param)` format.
/// For regular tables, the value is just the table name.
///
/// This is used to fix JOINs generated from GraphJoins, ensuring that
/// parameterized views are called correctly in all JOIN clauses.
pub(super) fn extract_rel_and_node_tables(
    plan: &LogicalPlan,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();

    match plan {
        LogicalPlan::GraphRel(gr) => {
            // Use the centralized helper to get parameterized table reference
            if let Some(parameterized_ref) = extract_parameterized_table_ref(&gr.center) {
                log::debug!(
                    "extract_rel_and_node_tables: GraphRel alias='{}' → '{}'",
                    gr.alias,
                    parameterized_ref
                );
                map.insert(gr.alias.clone(), parameterized_ref);
            }

            // Recursively check left and right nodes
            map.extend(extract_rel_and_node_tables(&gr.left));
            map.extend(extract_rel_and_node_tables(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            // Use the centralized helper to get parameterized table reference
            if let Some(parameterized_ref) = extract_parameterized_table_ref(&gn.input) {
                log::debug!(
                    "extract_rel_and_node_tables: GraphNode alias='{}' → '{}'",
                    gn.alias,
                    parameterized_ref
                );
                map.insert(gn.alias.clone(), parameterized_ref);
            }
        }
        LogicalPlan::Projection(p) => {
            map.extend(extract_rel_and_node_tables(&p.input));
        }
        LogicalPlan::Filter(f) => {
            map.extend(extract_rel_and_node_tables(&f.input));
        }
        LogicalPlan::CartesianProduct(cp) => {
            map.extend(extract_rel_and_node_tables(&cp.left));
            map.extend(extract_rel_and_node_tables(&cp.right));
        }
        LogicalPlan::GraphJoins(gj) => {
            map.extend(extract_rel_and_node_tables(&gj.input));
        }
        _ => {}
    }

    map
}

/// Helper function to find the table name for a given alias by recursively searching the plan tree
/// Used to find the anchor node's table in multi-hop queries
/// Find the table name for a given alias by traversing the LogicalPlan tree.
/// This is used to determine the correct FROM table in CTE patterns where
/// the grouping key alias (e.g., "g" in "WITH g, COUNT(u)") needs to be
/// resolved to its underlying table (e.g., "sec_groups").
///
/// IMPORTANT: This function is EXHAUSTIVE - all LogicalPlan variants must be
/// handled explicitly. This ensures we don't silently miss new plan types.
pub(super) fn find_table_name_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        // === Terminal nodes that can match ===
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                // Found the matching GraphNode, extract table name from its input
                match &*node.input {
                    LogicalPlan::ViewScan(scan) => Some(scan.source_table.clone()),
                    _ => None,
                }
            } else {
                // Not a match, recurse into input
                find_table_name_for_alias(&node.input, target_alias)
            }
        }
        LogicalPlan::GraphRel(rel) => {
            // Check if the target is a relationship alias (e.g., "f1" for denormalized edges)
            if rel.alias == target_alias {
                // The relationship alias matches - get table from its center ViewScan
                if let LogicalPlan::ViewScan(scan) = &*rel.center {
                    return Some(scan.source_table.clone());
                }
            }
            // Search in both left and right branches
            find_table_name_for_alias(&rel.left, target_alias)
                .or_else(|| find_table_name_for_alias(&rel.right, target_alias))
        }

        // === Wrapper nodes - recurse into input ===
        LogicalPlan::Cte(cte) => find_table_name_for_alias(&cte.input, target_alias),
        LogicalPlan::Projection(proj) => find_table_name_for_alias(&proj.input, target_alias),
        LogicalPlan::GroupBy(group_by) => find_table_name_for_alias(&group_by.input, target_alias),
        LogicalPlan::Filter(filter) => find_table_name_for_alias(&filter.input, target_alias),
        LogicalPlan::OrderBy(order) => find_table_name_for_alias(&order.input, target_alias),
        LogicalPlan::GraphJoins(joins) => find_table_name_for_alias(&joins.input, target_alias),
        LogicalPlan::Skip(skip) => find_table_name_for_alias(&skip.input, target_alias),
        LogicalPlan::Limit(limit) => find_table_name_for_alias(&limit.input, target_alias),
        LogicalPlan::Unwind(unwind) => find_table_name_for_alias(&unwind.input, target_alias),

        // === Union - search all branches ===
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(table) = find_table_name_for_alias(input, target_alias) {
                    return Some(table);
                }
            }
            None
        }

        // === Terminal nodes that cannot contain aliases ===
        LogicalPlan::Empty => None,
        LogicalPlan::ViewScan(_) => None, // ViewScan itself doesn't have alias, GraphNode wraps it
        LogicalPlan::PageRank(_) => None, // PageRank is a computed result, no direct table alias

        // === CartesianProduct - search both branches ===
        LogicalPlan::CartesianProduct(cp) => find_table_name_for_alias(&cp.left, target_alias)
            .or_else(|| find_table_name_for_alias(&cp.right, target_alias)),

        // === WithClause - search input ===
        LogicalPlan::WithClause(wc) => find_table_name_for_alias(&wc.input, target_alias),

        // === Write variants - recurse into preceding read pipeline ===
        LogicalPlan::Create(c) => find_table_name_for_alias(&c.input, target_alias),
        LogicalPlan::SetProperties(sp) => find_table_name_for_alias(&sp.input, target_alias),
        LogicalPlan::Delete(d) => find_table_name_for_alias(&d.input, target_alias),
        LogicalPlan::Remove(r) => find_table_name_for_alias(&r.input, target_alias),
    }
}

/// Helper to extract ID column name from ViewScan
///
/// For GraphRel, follows rel.right (end node) rather than rel.center (relationship table).
/// This is correct because callers always pass graph_rel.left or graph_rel.right
/// expecting a NODE's ID column, not a relationship table's FK column.
/// For nested patterns like (a)-[:R1]->(b)-[:R2]->(c), extract_id_column(&outer.left)
/// follows the inner GraphRel's right to get b's ID (the connection point).
pub(super) fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::GraphRel(rel) => extract_id_column(&rel.right),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        // For WithClause, recurse into input to get ID column from underlying node
        LogicalPlan::WithClause(wc) => extract_id_column(&wc.input),
        // For Union (polymorphic nodes), only return id_column if all branches agree.
        // If branches disagree (e.g. User→user_id vs Post→post_id), return None so
        // callers fall back to schema lookup using the concrete relationship table context.
        LogicalPlan::Union(union) => {
            if union.inputs.is_empty() {
                return None;
            }
            let cols: Vec<Option<String>> = union
                .inputs
                .iter()
                .map(|input| extract_id_column(input))
                .collect();
            if cols.windows(2).all(|w| w[0] == w[1]) {
                cols.into_iter().next().flatten()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper function to extract all relationship connections from a plan tree
/// Returns a vector of (left_connection, right_connection, relationship_alias) tuples
pub(super) fn get_all_relationship_connections(
    plan: &LogicalPlan,
) -> Vec<(String, String, String)> {
    let mut connections = vec![];

    fn collect_connections(plan: &LogicalPlan, connections: &mut Vec<(String, String, String)>) {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                connections.push((
                    graph_rel.left_connection.clone(),
                    graph_rel.right_connection.clone(),
                    graph_rel.alias.clone(),
                ));
                // Recurse into nested GraphRels (multi-hop chains)
                collect_connections(&graph_rel.left, connections);
                collect_connections(&graph_rel.right, connections);
            }
            LogicalPlan::Projection(proj) => collect_connections(&proj.input, connections),
            LogicalPlan::Filter(filter) => collect_connections(&filter.input, connections),
            LogicalPlan::GraphJoins(graph_joins) => {
                collect_connections(&graph_joins.input, connections)
            }
            LogicalPlan::GraphNode(graph_node) => {
                collect_connections(&graph_node.input, connections)
            }
            _ => {}
        }
    }

    collect_connections(plan, &mut connections);
    connections
}

/// Helper function to find the anchor/first node in a multi-hop pattern
/// The anchor is the node that should be in the FROM clause
/// Strategy: Prefer required (non-optional) nodes over optional nodes
/// When mixing MATCH and OPTIONAL MATCH, the required node should be the anchor (FROM table)
///
/// Algorithm:
/// 1. PRIORITY: Find ANY required node (handles MATCH (n) + OPTIONAL MATCH patterns around n)
/// 2. Find true leftmost node (left-only) among required nodes
/// 3. Fall back to any required node if no leftmost required found
/// 4. Fall back to traditional anchor pattern for all-optional cases
/// 5. CRITICAL: Skip denormalized aliases (extracted from GraphNode.is_denormalized in plan tree)
pub(super) fn find_anchor_node(
    connections: &[(String, String, String)],
    optional_aliases: &std::collections::HashSet<String>,
    denormalized_aliases: &std::collections::HashSet<String>,
) -> Option<String> {
    if connections.is_empty() {
        return None;
    }

    // CRITICAL FIX FOR OPTIONAL MATCH BUG:
    // When we have MATCH (n:User) OPTIONAL MATCH (n)-[:FOLLOWS]->(out) OPTIONAL MATCH (in)-[:FOLLOWS]->(n)
    // The connections are: [(n, out, FOLLOWS), (in, n, FOLLOWS)]
    // Traditional leftmost logic would choose 'in' (left-only), but 'in' is optional!
    // We must prioritize 'n' (required) even though it appears on both sides.

    // Strategy 0: Collect all unique nodes (left and right)
    let mut all_nodes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (left, right, _) in connections {
        all_nodes.insert(left.clone());
        all_nodes.insert(right.clone());
    }

    // Strategy 1: Find ANY required node - this handles the OPTIONAL MATCH around required node case
    // If there's a required node anywhere in the pattern, use it as anchor
    // CRITICAL: Filter out denormalized aliases (virtual nodes on edge tables)
    let required_nodes: Vec<String> = all_nodes
        .iter()
        .filter(|node| {
            let is_optional = optional_aliases.contains(*node);
            let is_denormalized = denormalized_aliases.contains(*node);
            log::debug!(
                "🔍 find_anchor_node: node='{}' optional={} denormalized={}",
                node,
                is_optional,
                is_denormalized
            );
            !is_optional && !is_denormalized
        })
        .cloned()
        .collect();

    log::info!(
        "🔍 find_anchor_node: required_nodes after filtering: {:?}",
        required_nodes
    );

    if !required_nodes.is_empty() {
        // We have required nodes - prefer one that's truly leftmost (left-only)
        let right_nodes: std::collections::BTreeSet<_> = connections
            .iter()
            .map(|(_, right, _)| right.clone())
            .collect();

        // Check if any required node is leftmost (left-only)
        // CRITICAL: Also skip denormalized aliases
        for (left, _, _) in connections {
            if !right_nodes.contains(left)
                && !optional_aliases.contains(left)
                && !denormalized_aliases.contains(left)
            {
                log::info!(
                    "✓ Found REQUIRED leftmost anchor: {} (required + left-only)",
                    left
                );
                return Some(left.clone());
            }
        }

        // No required node is leftmost, just use the first required node we find
        let anchor = required_nodes[0].clone();
        log::info!(
            "✓ Found REQUIRED anchor (not leftmost): {} (required node in mixed pattern)",
            anchor
        );
        return Some(anchor);
    }

    // CRITICAL: If required_nodes is EMPTY (all nodes are denormalized or optional),
    // return None to signal that the relationship table should be used as anchor!
    log::debug!(
        "🔍 find_anchor_node: All nodes filtered out (denormalized/optional), returning None"
    );
    if all_nodes.iter().all(|n| denormalized_aliases.contains(n)) {
        log::debug!(
            "🔍 find_anchor_node: All nodes are denormalized - use relationship table as FROM!"
        );
        return None;
    }

    // Strategy 2: No required nodes found - all optional. Use traditional leftmost logic.
    let right_nodes: std::collections::BTreeSet<_> = connections
        .iter()
        .map(|(_, right, _)| right.clone())
        .collect();

    for (left, _, _) in connections {
        if !right_nodes.contains(left) && !denormalized_aliases.contains(left) {
            log::info!(
                "✓ Found leftmost anchor (all optional): {} (left-only)",
                left
            );
            return Some(left.clone());
        }
    }

    // Strategy 3: Fallback to first left_connection (circular or complex pattern)
    let fallback = connections.first().map(|(left, _, _)| left.clone());
    if let Some(ref alias) = fallback {
        log::warn!("⚠️ No clear anchor, using fallback: {}", alias);
    }
    fallback
}

use super::cte_extraction::FixedPathInfo;

/// Build a composite-aware SQL expression for a node's id, given the alias
/// it's actually bound under. Single-column ids render as `alias.col` (or
/// `toString(alias.col)` when `force_string_cast` is set); composite ids
/// always collapse into a pipe-joined string
/// (`concat(toString(alias.c1), '|', toString(alias.c2), ...)`), mirroring
/// the VLP recursive CTE's own composite-ID convention (`emit_id_expr` in
/// `sql_generator/emitters/clickhouse/variable_length_cte.rs`) so a
/// composite-key node's `nodes(p)` entry carries its FULL identity instead of
/// silently dropping every column past the first.
///
/// `force_string_cast` exists because `nodes(p)` builds a single SQL
/// `array(...)`, which requires every element to share a common type.
/// A path mixing a single-column node (e.g. an integer `Customer.customer_id`)
/// with a composite-key node (whose pipe-join is always a string, e.g.
/// `Account(bank_id, account_number)`) would otherwise produce an
/// array of INCOMPATIBLE types (ClickHouse: `NO_COMMON_TYPE`) — the caller
/// sets this whenever any node id on the path is composite, so every element
/// (including plain single-column ones) casts to string uniformly.
fn id_expr_for_alias(
    alias: &str,
    id: &crate::graph_catalog::config::Identifier,
    force_string_cast: bool,
) -> RenderExpr {
    use crate::graph_catalog::config::Identifier;
    match id {
        Identifier::Single(col) => {
            let prop = RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: PropertyValue::Column(col.clone()),
            });
            if force_string_cast {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: current_function_mapper().cast_string().to_string(),
                    args: vec![prop],
                })
            } else {
                prop
            }
        }
        Identifier::Composite(cols) => {
            let cast_name = current_function_mapper().cast_string().to_string();
            let cast_col = |col: &String| {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: cast_name.clone(),
                    args: vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(col.clone()),
                    })],
                })
            };
            let mut args = Vec::with_capacity(cols.len() * 2 - 1);
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    args.push(RenderExpr::Literal(super::render_expr::Literal::String(
                        "|".to_string(),
                    )));
                }
                args.push(cast_col(col));
            }
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "concat".to_string(),
                args,
            })
        }
    }
}

/// Rewrite path function calls for FIXED multi-hop patterns (no variable length)
/// For fixed patterns, we know the hop count and aliases at compile time
/// Converts:
/// - length(p) → literal hop_count value
/// - nodes(p) → [r1.from_id, r1.to_id, r2.to_id, ...] array of node IDs
/// - relationships(p) → [r1, r2, ...] tuple of relationship aliases
pub fn rewrite_fixed_path_functions_with_info(
    expr: &RenderExpr,
    path_info: &FixedPathInfo,
) -> RenderExpr {
    match expr {
        RenderExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function call with the path variable as argument
            if fn_call.args.len() == 1 {
                if let RenderExpr::TableAlias(TableAlias(alias)) = &fn_call.args[0] {
                    if alias == &path_info.path_var_name {
                        match fn_call.name.as_str() {
                            "length" => {
                                // Convert length(p) to literal hop count
                                return RenderExpr::Literal(super::render_expr::Literal::Integer(
                                    path_info.hop_count as i64,
                                ));
                            }
                            "nodes" => {
                                // #497: if we don't actually have any node
                                // aliases (e.g. `path_info` came from the
                                // best-effort fallback in to_sql_query.rs,
                                // which can detect a path function reference
                                // — including a NESTED one like
                                // `length(nodes(p))`, since it recurses into
                                // function args — without being able to
                                // populate real alias/column metadata for
                                // arbitrary VLP-syntax fixed hop counts like
                                // `*2`), leave the call UNCHANGED rather than
                                // emit a valid-looking-but-wrong empty
                                // `array()`/`tuple()`. An unresolved `nodes(p)`
                                // failing loudly at execution (unbound `p`) is
                                // strictly better than silently evaluating to
                                // an empty collection (ground rule 1).
                                if path_info.node_aliases.is_empty() {
                                    return expr.clone();
                                }

                                // Build array of node ID references: [r1.Origin, r1.Dest, r2.Dest].
                                // #497 composite-ID fix: a node's `Identifier`
                                // may be `Composite` (e.g. Account keyed on
                                // (bank_id, account_number)) — a single
                                // `PropertyAccessExp` would silently drop
                                // every column past the first. Pipe-join
                                // composite columns via `id_expr_for_alias`,
                                // mirroring the VLP recursive CTE's own
                                // composite-ID convention (`emit_id_expr` in
                                // variable_length_cte.rs). If the path mixes a
                                // composite-key node with a plain single-column
                                // one, cast every element to string so the
                                // resulting `array(...)` has one common type
                                // (unmixed single-column paths keep the
                                // existing untouched — no behavior change).
                                let any_composite = path_info.node_aliases.iter().any(|a| {
                                    matches!(
                                        path_info.node_id_columns.get(a),
                                        Some((
                                            _,
                                            crate::graph_catalog::config::Identifier::Composite(_)
                                        ))
                                    )
                                });
                                let node_args: Vec<RenderExpr> = path_info
                                    .node_aliases
                                    .iter()
                                    .filter_map(|node_alias| {
                                        path_info.node_id_columns.get(node_alias).map(
                                            |(rel_alias, id)| {
                                                id_expr_for_alias(rel_alias, id, any_composite)
                                            },
                                        )
                                    })
                                    .collect();

                                // Use array() function for ClickHouse arrays
                                if node_args.is_empty() {
                                    // Fallback: aliases are known but no ID columns
                                    // resolved for any of them — return tuple of
                                    // aliases (still better than silently empty;
                                    // downstream will surface an unbound-alias
                                    // error rather than a wrong empty result).
                                    let fallback_args: Vec<RenderExpr> = path_info
                                        .node_aliases
                                        .iter()
                                        .map(|a| RenderExpr::TableAlias(TableAlias(a.clone())))
                                        .collect();
                                    return RenderExpr::ScalarFnCall(ScalarFnCall {
                                        name: "tuple".to_string(),
                                        args: fallback_args,
                                    });
                                }

                                return RenderExpr::ScalarFnCall(ScalarFnCall {
                                    name: "array".to_string(),
                                    args: node_args,
                                });
                            }
                            "relationships" => {
                                // #497: same "don't fabricate an empty
                                // collection from unresolved metadata" guard
                                // as the `nodes` arm above.
                                if path_info.rel_aliases.is_empty() {
                                    return expr.clone();
                                }

                                // mirror the VLP recursive CTE's `path_relationships`
                                // column, which is an array of relationship TYPE-NAME
                                // string literals (see get_relationship_type_array in
                                // variable_length_cte.rs) — not a tuple of raw table
                                // aliases, which isn't a resolvable SQL value on its own.
                                // Keeping fixed-path and VLP-path `relationships(p)`
                                // return the same shape means downstream consumers
                                // don't need to special-case which route a path took.
                                let rel_args: Vec<RenderExpr> = path_info
                                    .rel_aliases
                                    .iter()
                                    .filter_map(|alias| path_info.rel_types.get(alias))
                                    .map(|type_name| {
                                        RenderExpr::Literal(super::render_expr::Literal::String(
                                            type_name.clone(),
                                        ))
                                    })
                                    .collect();
                                return RenderExpr::ScalarFnCall(ScalarFnCall {
                                    name: "array".to_string(),
                                    args: rel_args,
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Recursively rewrite arguments for nested calls
            let rewritten_args: Vec<RenderExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_fixed_path_functions_with_info(arg, path_info))
                .collect();

            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands: Vec<RenderExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_fixed_path_functions_with_info(operand, path_info))
                .collect();

            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: rewritten_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            // count(p) → count(*): each row already represents exactly one
            // path, so counting the (otherwise-unbound) path variable itself
            // is equivalent to counting rows.
            if agg.args.len() == 1 && agg.name.to_lowercase() == "count" {
                if let RenderExpr::TableAlias(TableAlias(alias)) = &agg.args[0] {
                    if alias == &path_info.path_var_name {
                        return RenderExpr::AggregateFnCall(AggregateFnCall {
                            name: agg.name.clone(),
                            args: vec![RenderExpr::Star],
                        });
                    }
                }
            }

            // Recursively rewrite arguments for aggregate functions
            let rewritten_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_fixed_path_functions_with_info(arg, path_info))
                .collect();

            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(), // For other expression types, return as-is
    }
}

/// Rewrite path function calls on LogicalExpr (before conversion to RenderExpr)
/// This is used for WITH clause expressions that need path function rewriting
/// Converts: length(p) → PropertyAccess(t, hop_count), nodes(p) → PropertyAccess(t, path_nodes)
pub(super) fn rewrite_logical_path_functions(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    path_var_name: &str,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{
        LogicalExpr, PropertyAccess, ScalarFnCall, TableAlias,
    };

    match expr {
        LogicalExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function call with the path variable as argument
            if fn_call.args.len() == 1 {
                if let LogicalExpr::TableAlias(TableAlias(alias)) = &fn_call.args[0] {
                    if alias == path_var_name {
                        // Convert path functions to CTE column references
                        // 🔧 FIX (Jan 23, 2026): Generate bare Column, not PropertyAccess with "t"
                        // In WITH clause contexts, the VLP CTE may be aliased differently (e.g., "path" instead of "t")
                        // Using bare columns lets the SQL renderer add the correct table alias later
                        let column_name = match fn_call.name.as_str() {
                            "length" => Some("hop_count"),
                            "nodes" => Some("path_nodes"),
                            "relationships" => Some("path_relationships"),
                            _ => None,
                        };

                        if let Some(col_name) = column_name {
                            // Generate a bare PropertyAccess without table alias
                            // This will be converted to RenderExpr::Column later,
                            // which the SQL renderer recognizes as a VLP column
                            return LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias("__vlp_bare_col".to_string()), // Special marker for bare column
                                column: PropertyValue::Column(col_name.to_string()),
                            });
                        }
                    }
                }
            }

            // Recursively rewrite function arguments
            let rewritten_args: Vec<LogicalExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_logical_path_functions(arg, path_var_name))
                .collect();

            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        LogicalExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments for aggregate functions
            let rewritten_args: Vec<LogicalExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_logical_path_functions(arg, path_var_name))
                .collect();

            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands: Vec<LogicalExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_logical_path_functions(operand, path_var_name))
                .collect();

            LogicalExpr::OperatorApplicationExp(
                crate::query_planner::logical_expr::OperatorApplication {
                    operator: op.operator,
                    operands: rewritten_operands,
                },
            )
        }
        _ => expr.clone(), // For other expression types, return as-is
    }
}

// =============================================================================
// PROPER SCHEMA-PARAMETERIZED VERSIONS
// These functions take schema as a parameter and should be used instead of the
// deprecated versions above that access GLOBAL_SCHEMAS directly.
// =============================================================================

// =============================================================================
// TODO: Relationship Uniqueness Filtering for Undirected Multi-Hop Patterns
// =============================================================================
// The following structs and functions are prepared for implementing Issue #2
// (Undirected Patterns - Relationship Uniqueness) from KNOWN_ISSUES.md.
//
// However, they cannot be used yet because Issue #1 (Undirected Multi-Hop
// Patterns Generate Broken SQL) must be fixed first. The BidirectionalUnion
// optimizer transforms Direction::Either patterns into Union nodes, which
// breaks the multi-hop JOIN inference that these filters depend on.
//
// Once Issue #1 is fixed, uncomment and integrate these helpers.
// =============================================================================

/*
/// Information about an undirected relationship for uniqueness filtering
#[derive(Debug, Clone)]
pub struct UndirectedRelInfo {
    pub alias: String,          // Relationship alias (e.g., "r1")
    pub from_id_col: String,    // FROM ID column name
    pub to_id_col: String,      // TO ID column name
    pub edge_id_cols: Vec<String>, // Edge ID columns (for composite uniqueness)
}

/// Collect all undirected (Direction::Either) relationships from a logical plan.
/// Returns info needed to generate pairwise uniqueness filters.
pub(super) fn collect_undirected_relationships(plan: &LogicalPlan) -> Result<Vec<UndirectedRelInfo>, RenderBuildError> {
    fn collect(plan: &LogicalPlan, result: &mut Vec<UndirectedRelInfo>) -> Result<(), RenderBuildError> {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if this relationship is undirected
                if graph_rel.direction == Direction::Either {
                    // Extract relationship columns from the center (ViewScan)
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        // ViewScan should have these populated by query planner
                        let from_col = scan.from_id.clone()
                            .ok_or_else(|| RenderBuildError::ViewScanMissingRelationshipColumn("from_id".to_string()))?;
                        let to_col = scan.to_id.clone()
                            .ok_or_else(|| RenderBuildError::ViewScanMissingRelationshipColumn("to_id".to_string()))?;

                        // Try to get edge_id columns from schema
                        // First, try to look up the relationship schema by type
                        let edge_id_cols = if let Some(labels) = &graph_rel.labels {
                            if let Some(rel_type) = labels.first() {
                                // Look up relationship schema from task-local context
                                if let Some(schema) = crate::server::query_context::get_current_schema_with_fallback() {
                                    if let Some(rel_schema) = schema.get_relationships_schema_opt(rel_type) {
                                        match &rel_schema.edge_id {
                                            Some(id) => id.columns().iter().map(|s| s.to_string()).collect(),
                                            None => vec![from_col.clone(), to_col.clone()],
                                        }
                                    } else {
                                        vec![from_col.clone(), to_col.clone()]
                                    }
                                } else {
                                    vec![from_col.clone(), to_col.clone()]
                                }
                            } else {
                                vec![from_col.clone(), to_col.clone()]
                            }
                        } else {
                            vec![from_col.clone(), to_col.clone()]
                        };

                        result.push(UndirectedRelInfo {
                            alias: graph_rel.alias.clone(),
                            from_id_col: from_col,
                            to_id_col: to_col,
                            edge_id_cols,
                        });
                    }
                }

                // Recursively check children (for multi-hop patterns)
                collect(&graph_rel.left, result)?;
                collect(&graph_rel.center, result)?;
                collect(&graph_rel.right, result)?;
            }
            LogicalPlan::GraphNode(node) => collect(&node.input, result)?,
            LogicalPlan::GraphJoins(joins) => collect(&joins.input, result)?,
            LogicalPlan::Projection(proj) => collect(&proj.input, result)?,
            LogicalPlan::Filter(filter) => collect(&filter.input, result)?,
            LogicalPlan::GroupBy(gb) => collect(&gb.input, result)?,
            LogicalPlan::OrderBy(ob) => collect(&ob.input, result)?,
            LogicalPlan::Limit(limit) => collect(&limit.input, result)?,
            LogicalPlan::Skip(skip) => collect(&skip.input, result)?,
            LogicalPlan::Unwind(u) => collect(&u.input, result)?,
            _ => {}
        }
        Ok(())
    }

    let mut result = Vec::new();
    collect(plan, &mut result)?;
    Ok(result)
}

/// Generate pairwise relationship uniqueness filters for undirected patterns.
///
/// For undirected multi-hop patterns like `(a)-[r1]-(b)-[r2]-(c)`, we need to prevent
/// the same physical edge from being traversed twice (once in each direction).
///
/// For each pair (r1, r2), generates:
/// ```sql
/// NOT (
///     tuple(r1.col1, r1.col2, ...) = tuple(r2.col1, r2.col2, ...)
/// )
/// ```
pub(super) fn generate_undirected_uniqueness_filters(
    undirected_rels: &[UndirectedRelInfo],
) -> Option<RenderExpr> {
    if undirected_rels.len() < 2 {
        return None; // Need at least 2 relationships for pairwise comparison
    }

    let mut filters = Vec::new();

    // Generate pairwise filters for all combinations
    for i in 0..undirected_rels.len() {
        for j in (i + 1)..undirected_rels.len() {
            let r1 = &undirected_rels[i];
            let r2 = &undirected_rels[j];

            // Generate: NOT (tuple(r1.cols...) = tuple(r2.cols...))
            // This prevents the same physical edge from being used twice

            // Build tuple expressions for each relationship's edge_id columns
            let r1_tuple_args: Vec<RenderExpr> = r1.edge_id_cols.iter().map(|col| {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r1.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                })
            }).collect();

            let r2_tuple_args: Vec<RenderExpr> = r2.edge_id_cols.iter().map(|col| {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r2.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                })
            }).collect();

            // Create tuple expressions
            let r1_tuple = if r1_tuple_args.len() == 1 {
                r1_tuple_args.into_iter().next().unwrap()
            } else {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: r1_tuple_args,
                })
            };

            let r2_tuple = if r2_tuple_args.len() == 1 {
                r2_tuple_args.into_iter().next().unwrap()
            } else {
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: r2_tuple_args,
                })
            };

            // Generate: NOT (r1_tuple = r2_tuple)
            let equality_check = RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![r1_tuple, r2_tuple],
            });

            let not_equal = RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![equality_check],
            });

            filters.push(not_equal);
        }
    }

    if filters.is_empty() {
        return None;
    }

    // Combine all filters with AND
    Some(filters.into_iter().reduce(|acc, filter| {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![acc, filter],
        })
    }).unwrap())
}
*/

/// Check if a logical plan contains any variable-length path or shortest path pattern
/// These require CTE-based processing (recursive CTEs)
pub(super) fn has_variable_length_or_shortest_path(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check for variable-length patterns that need CTEs
            if let Some(spec) = &graph_rel.variable_length {
                // Fixed-length (exact hops, no shortest path) can use inline JOINs
                let is_fixed_length =
                    spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

                if !is_fixed_length {
                    // Variable-length or shortest path needs CTE
                    return true;
                }
            }
            // Also check shortest path without variable_length (edge case)
            if graph_rel.shortest_path_mode.is_some() {
                return true;
            }
            // Check child plans
            has_variable_length_or_shortest_path(&graph_rel.left)
                || has_variable_length_or_shortest_path(&graph_rel.right)
        }
        LogicalPlan::GraphJoins(joins) => has_variable_length_or_shortest_path(&joins.input),
        LogicalPlan::Projection(proj) => has_variable_length_or_shortest_path(&proj.input),
        LogicalPlan::Filter(filter) => has_variable_length_or_shortest_path(&filter.input),
        LogicalPlan::GraphNode(node) => has_variable_length_or_shortest_path(&node.input),
        LogicalPlan::GroupBy(gb) => has_variable_length_or_shortest_path(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_or_shortest_path(&ob.input),
        LogicalPlan::Limit(limit) => has_variable_length_or_shortest_path(&limit.input),
        LogicalPlan::Skip(skip) => has_variable_length_or_shortest_path(&skip.input),
        LogicalPlan::Unwind(u) => has_variable_length_or_shortest_path(&u.input),
        _ => false,
    }
}

#[cfg(test)] // test-only-live: exercised solely by unit tests (P2.10 dead_code sweep)
/// Generate polymorphic edge type filters for a GraphRel
///
/// When a relationship table uses type discrimination columns (type_column, from_label_column,
/// to_label_column), this function generates filters to select the correct edge types.
///
/// # Arguments
/// * `rel_alias` - The alias for the relationship table (e.g., "r", "f")
/// * `rel_type` - The Cypher relationship type (e.g., "FOLLOWS")
/// * `from_label` - The source node label (e.g., "User")
/// * `to_label` - The target node label (e.g., "Post")
///
/// # Returns
/// A RenderExpr representing the combined filters, or None if not a polymorphic edge
///
/// # Example
/// For a polymorphic relationship table:
/// ```yaml
/// relationships:
///   - polymorphic: true
///     table: interactions
///     type_column: interaction_type
///     from_label_column: from_type
///     to_label_column: to_type
/// ```
///
/// Query: `MATCH (u:User)-[:FOLLOWS]->(other:User)`
///
/// Generates: `r.interaction_type = 'FOLLOWS' AND r.from_type = 'User' AND r.to_type = 'User'`
/// Uses the task-local query schema.
pub(super) fn generate_polymorphic_edge_filters(
    rel_alias: &str,
    rel_type: &str,
    from_label: &str,
    to_label: &str,
) -> Option<RenderExpr> {
    use crate::server::query_context::get_current_schema_with_fallback as get_current_schema;

    let schema = get_current_schema()?;
    let rel_schema = schema.get_rel_schema(rel_type).ok()?;

    // Check if this is a polymorphic edge
    let type_col = rel_schema.type_column.as_ref()?;
    let from_label_col = rel_schema.from_label_column.as_ref();
    let to_label_col = rel_schema.to_label_column.as_ref();

    let mut filters = Vec::new();

    // Filter 1: type_column = 'FOLLOWS'
    let type_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(type_col.clone()),
            }),
            RenderExpr::Literal(Literal::String(rel_type.to_string())),
        ],
    });
    filters.push(type_filter);

    // Filter 2: from_label_column = 'User' (if present)
    if let Some(from_col) = from_label_col {
        let from_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias.to_string()),
                    column: PropertyValue::Column(from_col.clone()),
                }),
                RenderExpr::Literal(Literal::String(from_label.to_string())),
            ],
        });
        filters.push(from_filter);
    }

    // Filter 3: to_label_column = 'Post' (if present)
    if let Some(to_col) = to_label_col {
        let to_filter = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias.to_string()),
                    column: PropertyValue::Column(to_col.clone()),
                }),
                RenderExpr::Literal(Literal::String(to_label.to_string())),
            ],
        });
        filters.push(to_filter);
    }

    // Combine filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        }))
    }
}

// ============================================================================
// Plan utilities - moved from plan_builder.rs for better organization
// ============================================================================

/// Get property column mapping from ViewScan in the plan tree.
///
/// For denormalized virtual nodes, the ViewScan's property_mapping contains the
/// position-specific column mappings (e.g., `code -> origin_code` for FROM position).
/// This function traverses the plan tree to find the ViewScan for the given alias
/// and looks up the property in its property_mapping.
///
/// Returns the mapped column name if found, None otherwise.
fn get_property_from_viewscan(
    alias: &str,
    property: &str,
    plan: &LogicalPlan,
) -> Option<PropertyValue> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            // Found the matching GraphNode - check its input for ViewScan
            match node.input.as_ref() {
                LogicalPlan::ViewScan(scan) => {
                    // Look up property in the ViewScan's property_mapping
                    if let Some(prop_value) = scan.property_mapping.get(property) {
                        return Some(prop_value.clone());
                    }
                    // Also check from_node_properties and to_node_properties
                    if let Some(from_props) = &scan.from_node_properties {
                        if let Some(prop_value) = from_props.get(property) {
                            return Some(prop_value.clone());
                        }
                    }
                    if let Some(to_props) = &scan.to_node_properties {
                        if let Some(prop_value) = to_props.get(property) {
                            return Some(prop_value.clone());
                        }
                    }
                    None
                }
                LogicalPlan::Union(_) => {
                    // For denormalized nodes with Union, we need to get mapping from the specific branch
                    // This shouldn't happen at filter level since filters are per-branch
                    // But if it does, we can't determine which branch, so return None
                    log::debug!(
                        "get_property_from_viewscan: GraphNode '{}' has Union input - cannot determine branch",
                        alias
                    );
                    None
                }
                _ => None,
            }
        }
        LogicalPlan::GraphNode(node) => get_property_from_viewscan(alias, property, &node.input),
        LogicalPlan::GraphRel(rel) => get_property_from_viewscan(alias, property, &rel.left)
            .or_else(|| get_property_from_viewscan(alias, property, &rel.center))
            .or_else(|| get_property_from_viewscan(alias, property, &rel.right)),
        LogicalPlan::Filter(filter) => get_property_from_viewscan(alias, property, &filter.input),
        LogicalPlan::Projection(proj) => get_property_from_viewscan(alias, property, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_property_from_viewscan(alias, property, &joins.input),
        LogicalPlan::OrderBy(order_by) => {
            get_property_from_viewscan(alias, property, &order_by.input)
        }
        LogicalPlan::Skip(skip) => get_property_from_viewscan(alias, property, &skip.input),
        LogicalPlan::Limit(limit) => get_property_from_viewscan(alias, property, &limit.input),
        LogicalPlan::GroupBy(group_by) => {
            get_property_from_viewscan(alias, property, &group_by.input)
        }
        LogicalPlan::Cte(cte) => get_property_from_viewscan(alias, property, &cte.input),
        // #1007: a second independent MATCH plans as a CartesianProduct; the
        // property lookup must descend into both sides or a standalone
        // foreign-embedded node (`MATCH (c:Person)` scanning its own table)
        // falls through to the label-based mapping and picks up the
        // edge-embedded position column (mgr_id) instead of its own table
        // column (people.pid) → Code 47.
        LogicalPlan::CartesianProduct(cp) => get_property_from_viewscan(alias, property, &cp.left)
            .or_else(|| get_property_from_viewscan(alias, property, &cp.right)),
        LogicalPlan::ViewScan(_scan) => {
            // Bare ViewScan without wrapping GraphNode: skip.
            // Alias-based lookups should only match through GraphNode (line 2268)
            // which verifies the alias. Matching bare ViewScans causes cross-alias
            // contamination (e.g., f2.firstName picking up f's CTE property_mapping).
            None
        }
        _ => None,
    }
}

/// Apply property mapping to an expression
///
/// Main purpose: Convert TableAlias expressions to PropertyAccess for denormalized schemas.
/// For GROUP BY with a node alias like `b` in `(a)-[r1]->(b)-[r2]->(c)`, this converts
/// the TableAlias("b") to PropertyAccess { table_alias: "r2", column: "Origin" }
///
/// Also remaps PropertyAccess table aliases for nodes denormalized on edges.
/// For cross-table patterns like zeek logs, where `src` is denormalized on the DNS_REQUESTED
/// edge, this changes `src."id.orig_h"` to use the edge alias.
///
/// Note: Regular PropertyAccess property name mapping is handled in the FilterTagging analyzer pass.
pub(super) fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    apply_property_mapping_to_expr_shielded(expr, plan, &mut Vec::new());
}

/// As [`apply_property_mapping_to_expr`], but carries `shielded` — the set of
/// names currently bound by an enclosing `reduce()` lambda
/// (`accumulator`/`variable`). A denorm alias remap is keyed purely on the NAME
/// (`get_denormalized_node_id_reference`), so a lambda variable that SHADOWS a
/// denorm node alias must be left alone — rewriting it into the node's physical
/// column corrupts the lambda (#929 review: `reduce(acc=0, s IN [1,2] | acc + s)`
/// where `s` is also a bound denorm node → `acc + t1."id.orig_h"` = Code 43, or
/// silent-wrong when the acc name collides). Mirrors the `shielding` mechanism
/// `variable_scope.rs` already uses for the same reduce-shadowing hazard.
fn apply_property_mapping_to_expr_shielded(
    expr: &mut RenderExpr,
    plan: &LogicalPlan,
    shielded: &mut Vec<String>,
) {
    match expr {
        RenderExpr::TableAlias(alias) => {
            // Shadowed by an enclosing reduce binder — not a node alias here.
            if shielded.iter().any(|n| n == &alias.0) {
                return;
            }
            // For denormalized schemas, convert TableAlias to the proper ID column reference
            // Example: TableAlias("b") -> PropertyAccess { table_alias: "r2", column: "Origin" }
            if let Some((rel_alias, id_column)) = get_denormalized_node_id_reference(&alias.0, plan)
            {
                use crate::graph_catalog::expression_parser::PropertyValue;
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias),
                    column: PropertyValue::Column(id_column),
                });
            }
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // Shadowed by an enclosing reduce binder — a property access whose
            // base alias is a bound lambda name is not a node reference here, so
            // leave its (name-keyed) denorm remap alone. Defensive parity with
            // the `TableAlias` arm; the observed #929-review breakages were all
            // bare `TableAlias`, but the alias remap below is equally name-keyed.
            if shielded.iter().any(|n| n == &prop.table_alias.0) {
                return;
            }
            // #582: when `prop.table_alias` is the anchor of an OPTIONAL
            // denorm-scan CTE + LEFT JOIN pattern (`is_denorm_scan_anchor_
            // alias`) AND `prop.column` genuinely belongs to that anchor's
            // `__denorm_scan_{alias}` CTE, resolve it directly against the
            // CTE and skip the structural edge-based property mapping /
            // `denormalized_node_edges` alias remap below entirely — those
            // redirect to the nullable LEFT JOIN alias, which is wrong for a
            // property the anchor's own always-present CTE already exposes.
            //
            // The CTE exposes every property under its plain Cypher name
            // (its inner UNION already normalizes both from/to sides under
            // shared names — see `plan_builder.rs`'s `__denorm_scan_{alias}`
            // construction), so two cases both count as "belongs to the
            // CTE": `prop.column` is ALREADY that Cypher name (untouched —
            // covers the WHERE-predicate path, where nothing upstream
            // rewrote it), or it's a PHYSICAL edge-table column an earlier
            // analysis pass (`FilterTagging::apply_property_mapping`)
            // rewrote it to, structurally, with no knowledge of this
            // render-time-only CTE (covers the GROUP BY path) — reverse via
            // `denorm_scan_cte_anchor_reverse_property`.
            //
            // If NEITHER matches, the property is NOT one the anchor CTE
            // exposes at all — e.g. a genuinely EDGE-OWNED property
            // requested via `anchor.prop` syntax (the CTE was deliberately
            // built WITHOUT it, same collision the #475 guard in
            // `plan_builder.rs` skips adding to its own SELECT/GROUP BY
            // rewrite map for). Falling through to the existing structural
            // mapping + alias remap is then CORRECT (matches the golden
            // `ontime_flights` shape: `RETURN a.airport, count(r)` renders
            // `airport` via the edge alias `r`, not the anchor CTE, both
            // pre- and post-#582).
            if is_denorm_scan_anchor_alias(&prop.table_alias.0, plan) {
                let already_cte_column =
                    denorm_scan_cte_anchor_properties(plan, &prop.table_alias.0).is_some_and(
                        |props| props.iter().any(|(name, _)| name == prop.column.raw()),
                    ) || denorm_scan_cte_anchor_id_property(plan, &prop.table_alias.0).as_deref()
                        == Some(prop.column.raw());
                if already_cte_column {
                    return;
                }
                if let PropertyValue::Column(ref physical_col) = prop.column {
                    if let Some(cypher_name) = denorm_scan_cte_anchor_reverse_property(
                        plan,
                        &prop.table_alias.0,
                        physical_col,
                    ) {
                        prop.column = PropertyValue::Column(cypher_name);
                        return;
                    }
                }
            }
            // #1006: own-table resolution — mirror of select_builder's #1006
            // branch. When the alias has a lazily-injected own-table LEFT JOIN
            // (select/group-by/order-by registered a request for a property
            // absent from the edge's embedded map), keep the NODE alias (it
            // names the injected join) and resolve the column against the
            // node's OWN table — skipping the structural edge mapping and the
            // denormalized-alias remap below, which would redirect to the edge
            // table (a Code-47 "unknown identifier" for non-edge properties).
            // Denorm-scan anchors are never registered (select_builder guard),
            // so this cannot race the `is_denorm_scan_anchor_alias` branch.
            if !matches!(&prop.column, PropertyValue::Expression(_)) {
                if let Some(own) =
                    crate::server::query_context::own_table_join_requests().get(&prop.table_alias.0)
                {
                    let raw = prop.column.raw().to_string();
                    let task_schema =
                        crate::server::query_context::get_current_schema_with_fallback();
                    let node_schema = task_schema
                        .as_ref()
                        .and_then(|s| s.node_schema_opt(&own.node_label));
                    let matched = own.properties.iter().find(|p| {
                        **p == raw
                            || node_schema
                                .and_then(|ns| own_table_property_column(ns, p))
                                .as_deref()
                                == Some(raw.as_str())
                    });
                    if let Some(prop_key) = matched {
                        if let Some(ns) = node_schema {
                            if let Some(physical) = own_table_property_column(ns, prop_key) {
                                log::debug!(
                                    "🔍 #1006: '{}.{}' resolved against own table column '{}' (not in edge's embedded map)",
                                    prop.table_alias.0, prop.column.raw(), physical
                                );
                                prop.column = PropertyValue::Column(physical);
                                return;
                            }
                        }
                    }
                }
            }
            // If the column is already an Expression (resolved by FilterTagging analyzer),
            // skip property-name re-mapping — it's already the correct ClickHouse expression.
            // Still fall through to the denormalized alias remap below (table alias may
            // need updating even for expression-backed properties on denormalized nodes).
            if matches!(&prop.column, PropertyValue::Expression(_)) {
                log::debug!(
                    "🔍 PROPERTY MAPPING: Skipping property-name remap for already-resolved Expression '{}.{}'",
                    prop.table_alias.0,
                    prop.column.raw()
                );
            } else if let Some(mapped_pv) =
                // For denormalized virtual nodes, try to get property mapping from ViewScan first
                // This is needed because denormalized nodes have position-specific mappings
                // (from_node_properties vs to_node_properties)
                get_property_from_viewscan(
                    &prop.table_alias.0,
                    prop.column.raw(),
                    plan,
                )
            {
                log::debug!(
                    "🔍 PROPERTY MAPPING (ViewScan): '{}.{}' -> '{}'",
                    prop.table_alias.0,
                    prop.column.raw(),
                    mapped_pv.raw()
                );
                prop.column = mapped_pv;
            } else if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                log::debug!(
                    "🔍 PROPERTY MAPPING: Alias '{}' -> Label '{}', Property '{}' (before mapping)",
                    prop.table_alias.0,
                    node_label,
                    prop.column.raw()
                );

                // Map the property to the correct column, preserving Expression variant
                if let Ok(mapped_pv) =
                    crate::render_plan::cte_generation::map_property_to_property_value(
                        prop.column.raw(),
                        &node_label,
                    )
                {
                    log::debug!(
                        "🔍 PROPERTY MAPPING: '{}' -> '{}'",
                        prop.column.raw(),
                        mapped_pv.raw()
                    );
                    prop.column = mapped_pv;
                } else {
                    // Fallback to string-based mapping for denormalized/complex cases
                    let mapped_column = crate::render_plan::cte_generation::map_property_to_column_with_relationship_context(
                        prop.column.raw(),
                        &node_label,
                        None, None, None,
                    ).unwrap_or_else(|_| prop.column.raw().to_string());

                    log::debug!(
                        "🔍 PROPERTY MAPPING (fallback): '{}' -> '{}'",
                        prop.column.raw(),
                        mapped_column
                    );
                    prop.column = PropertyValue::Column(mapped_column);
                }
            } else if let Some(rel_type) =
                get_relationship_type_for_alias(&prop.table_alias.0, plan)
            {
                // Alias is a relationship - map relationship property to column
                log::debug!(
"🔍 RELATIONSHIP PROPERTY MAPPING: Alias '{}' -> Type '{}', Property '{}' (before mapping)",
                    prop.table_alias.0,
                    rel_type,
                    prop.column.raw()
                );

                // Map the relationship property, preserving Expression variant
                if let Ok(mapped_pv) =
                    crate::render_plan::cte_generation::map_rel_property_to_property_value(
                        prop.column.raw(),
                        &rel_type,
                    )
                {
                    log::debug!(
                        "🔍 RELATIONSHIP PROPERTY MAPPING: '{}' -> '{}'",
                        prop.column.raw(),
                        mapped_pv.raw()
                    );
                    prop.column = mapped_pv;
                } else {
                    // Fallback to string-based mapping
                    let mapped_column =
                        crate::render_plan::cte_generation::map_relationship_property_to_column(
                            prop.column.raw(),
                            &rel_type,
                            None,
                        )
                        .unwrap_or_else(|_| prop.column.raw().to_string());

                    log::debug!(
                        "🔍 RELATIONSHIP PROPERTY MAPPING (fallback): '{}' -> '{}'",
                        prop.column.raw(),
                        mapped_column
                    );
                    prop.column = PropertyValue::Column(mapped_column);
                }
            }

            // For denormalized nodes, remap the table alias to the edge alias
            // Example: PropertyAccess { table_alias: "src", column: "id.orig_h" }
            //       -> PropertyAccess { table_alias: "ad62047b83", column: "id.orig_h" }
            //
            // CRITICAL: Use task-local context (populated during planning) instead of
            // traversing the plan tree. This ensures coupled edges get the unified_alias.
            // See join_generation.rs::register_denormalized_aliases for where the mapping is set.
            if let Some(rel_alias) =
                crate::render_plan::get_denormalized_alias_mapping(&prop.table_alias.0)
            {
                // #492: cross-side fix for the WHERE/filter path. In a
                // denormalized multi-hop chain the shared middle node's column
                // may have been schema-mapped via a DIFFERENT adjacent edge's
                // side (e.g. `b.code` → t1's `Dest`) than the edge this remap
                // binds the alias to (t2, whose side for b is `Origin`).
                // Re-mapping the alias while keeping the column would read the
                // WRONG endpoint. Translate the column onto the bound edge's
                // side first (same recipe as the SELECT path).
                if matches!(&prop.column, PropertyValue::Column(_)) {
                    use crate::render_plan::properties_builder::PropertiesBuilder;
                    if let Ok((properties, Some(_))) =
                        plan.get_properties_with_table_alias(&prop.table_alias.0)
                    {
                        // #492/#491 interaction fix: `properties` may be
                        // structurally matched against a DIFFERENT edge than
                        // `rel_alias` (the registered edge this remap targets)
                        // — e.g. an OPTIONAL pattern kept an earlier required
                        // pattern's binding (#491) while the structural walk
                        // still finds the optional GraphRel first. Re-derive
                        // from the REGISTERED edge so the cross-side lookup
                        // below operates on the edge we're actually remapping
                        // onto, not whatever the structural walk found.
                        let properties =
                            crate::render_plan::select_builder::properties_for_registered_edge(
                                plan,
                                &prop.table_alias.0,
                                &rel_alias,
                            )
                            .unwrap_or(properties);

                        let col = prop.column.raw().to_string();
                        let bound_side_known = properties
                            .iter()
                            .any(|(name, db_col)| *name == col || *db_col == col);
                        if !bound_side_known {
                            if let Some(fixed) =
                                crate::render_plan::select_builder::translate_denorm_cross_side_column(
                                    plan,
                                    &prop.table_alias.0,
                                    &col,
                                    &properties,
                                )
                            {
                                prop.column = PropertyValue::Column(fixed);
                            }
                        }
                    }
                }
                prop.table_alias = TableAlias(rel_alias);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            // `initial_value` and `list` evaluate in the OUTER scope — remap
            // normally (they may legitimately reference an outer denorm prop,
            // the intended #929 win). The `expression` (body) BINDS
            // `accumulator` and `variable`, which shadow any same-named node
            // alias, so push them before descending into the body (#929 review:
            // otherwise `reduce(acc=0, s IN … | acc + s)` remaps the lambda `s`
            // into the node's physical column → Code 43 / silent-wrong).
            apply_property_mapping_to_expr_shielded(&mut reduce.initial_value, plan, shielded);
            apply_property_mapping_to_expr_shielded(&mut reduce.list, plan, shielded);
            shielded.push(reduce.accumulator.clone());
            shielded.push(reduce.variable.clone());
            apply_property_mapping_to_expr_shielded(&mut reduce.expression, plan, shielded);
            shielded.pop();
            shielded.pop();
        }
        // Every OTHER wrapper variant recurses structurally through the
        // EXHAUSTIVE `descend_render_expr_mut` (no `_` catch-all — a new
        // `RenderExpr` variant is a compile error here, not a silently-skipped
        // remap). Previously only Operator/Aggregate/ScalarFn recursed and every
        // other wrapper — `Case`, `List`, `ArraySubscript`, `ArraySlicing`,
        // `MapLiteral` — fell through `_ => {}`, dropping the denorm alias/
        // property remap for a property buried inside it (#929: `WHERE CASE WHEN
        // s.ip = … END` leaked the raw cypher alias `s` instead of the edge
        // alias → Code 47 on live ClickHouse). `ReduceExpr` is handled above
        // (binder shielding); genuine leaves (`Literal`/`Column`/`Parameter`/…)
        // and deliberately-not-descended nodes (`InSubquery`/`ExistsSubquery`/
        // `PatternCount`/`CteEntityRef`) are no-ops in `descend_render_expr_mut`,
        // matching the prior catch-all exactly.
        other => {
            let mut recur = |child: &mut RenderExpr| -> super::render_expr::MutVisit {
                apply_property_mapping_to_expr_shielded(child, plan, shielded);
                super::render_expr::MutVisit::Stop
            };
            super::render_expr::descend_render_expr_mut(other, &mut recur);
        }
    }
}

/// #633: WHERE-clause sibling of #584's aggregate-arg resolver. An FK-edge
/// coupled relationship variable shares one physical row with its node (the edge
/// table IS a node endpoint table). When `r` appears in a WHERE predicate but the
/// FROM binds the coupled NODE alias `o` (because `o` is referenced downstream —
/// RETURN or WITH), `apply_property_mapping_to_expr` maps the property NAME but
/// leaves the dangling `r` table alias → `WHERE r.customer_id > 2 FROM orders_fk
/// AS o` → Code 47. Rewrite `r.<col>` → the coupled node alias, reusing the same
/// helper + guards as #584.
///
/// Gate (identical to #584): only remap when `from_alias` is the coupled NODE
/// alias. When the FROM binds the rel var itself (only `r` referenced, e.g.
/// `RETURN r.customer_id`), `r.<col>` is already valid and remapping would unbind
/// it. `coupled_edge_render_alias_for_aggregate` additionally returns None for a
/// traditional separate edge table (standard schema → no-op) and withholds the
/// remap for a self-referencing FK-edge (keeps the loud error — #632, ground
/// rule 1). `from_alias` is the enclosing render plan's `from.0.alias`.
pub(super) fn remap_coupled_rel_vars_in_filter(
    expr: &mut RenderExpr,
    plan: &LogicalPlan,
    from_alias: Option<&str>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            if let Some(gr) = plan.find_graph_rel_by_rel_alias(&prop.table_alias.0) {
                if let Some(node_alias) = LogicalPlan::coupled_edge_render_alias_for_aggregate(
                    gr,
                    &gr.left_connection,
                    &gr.right_connection,
                    &prop.table_alias.0,
                ) {
                    if from_alias == Some(node_alias.as_str()) {
                        log::info!(
                            "🔧 #633: FK-edge coupled rel var '{}' → node alias '{}' (WHERE resolve)",
                            prop.table_alias.0,
                            node_alias
                        );
                        prop.table_alias = TableAlias(node_alias);
                    }
                }
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                remap_coupled_rel_vars_in_filter(arg, plan, from_alias);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &mut f.args {
                remap_coupled_rel_vars_in_filter(arg, plan, from_alias);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                remap_coupled_rel_vars_in_filter(operand, plan, from_alias);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(inner) = &mut case.expr {
                remap_coupled_rel_vars_in_filter(inner, plan, from_alias);
            }
            for (cond, then_expr) in &mut case.when_then {
                remap_coupled_rel_vars_in_filter(cond, plan, from_alias);
                remap_coupled_rel_vars_in_filter(then_expr, plan, from_alias);
            }
            if let Some(else_expr) = &mut case.else_expr {
                remap_coupled_rel_vars_in_filter(else_expr, plan, from_alias);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                remap_coupled_rel_vars_in_filter(item, plan, from_alias);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, value) in entries {
                remap_coupled_rel_vars_in_filter(value, plan, from_alias);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            remap_coupled_rel_vars_in_filter(array, plan, from_alias);
            remap_coupled_rel_vars_in_filter(index, plan, from_alias);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            remap_coupled_rel_vars_in_filter(array, plan, from_alias);
            if let Some(f) = from {
                remap_coupled_rel_vars_in_filter(f, plan, from_alias);
            }
            if let Some(t) = to {
                remap_coupled_rel_vars_in_filter(t, plan, from_alias);
            }
        }
        RenderExpr::InSubquery(insub) => {
            remap_coupled_rel_vars_in_filter(&mut insub.expr, plan, from_alias);
        }
        RenderExpr::ReduceExpr(reduce) => {
            remap_coupled_rel_vars_in_filter(&mut reduce.initial_value, plan, from_alias);
            remap_coupled_rel_vars_in_filter(&mut reduce.list, plan, from_alias);
            remap_coupled_rel_vars_in_filter(&mut reduce.expression, plan, from_alias);
        }
        _ => {}
    }
}

/// #582: true when `alias` is the render-time anchor of an OPTIONAL
/// denorm-scan CTE + LEFT JOIN pattern (`optional_denorm_union_anchor_is_left`,
/// consumed by `plan_builder.rs` ~1326 to build the `__denorm_scan_{alias}`
/// CTE). That CTE is a UNION of both the edge table's from-side and to-side
/// rows, GROUP BY the node's id column — it exists specifically so the
/// anchor's OWN properties are always present, independent of whether the
/// OPTIONAL edge actually matched (unlike the LEFT JOIN's edge alias, which
/// is NULL on a miss). It exposes every property under its plain Cypher
/// name (see the `SELECT min("city") AS "city", ...` shape built in
/// `plan_builder.rs`), rendered against an alias literally equal to
/// `alias` itself (`ViewTableRef { alias: Some(node_alias), .. }`).
///
/// `apply_property_mapping_to_expr`'s general denormalized-node handling
/// (structural property-name mapping via the edge's ViewScan, plus the
/// `denormalized_node_edges` registry's alias remap to the edge/join alias)
/// is for nodes whose ONLY source is the edge table — it does not know about
/// this special anchor CTE, and blindly redirecting the anchor's own
/// PropertyAccess through it produces `t1.origin_city` (a column on the
/// NULLABLE LEFT JOIN alias) instead of `a.city` (the anchor's own
/// always-present source). That's silently wrong for any WHERE predicate
/// that keeps the anchor's condition in the same expression tree as an
/// optional-side condition (e.g. an OR the two can't be split across) — see
/// #582. Callers should skip the general remap entirely for `alias` when
/// this returns true; the anchor CTE already exposes exactly what an
/// unmapped `alias.city_property_name` PropertyAccess needs.
pub(super) fn is_denorm_scan_anchor_alias(alias: &str, plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            if let Some(anchor_is_left) = optional_denorm_union_anchor_is_left(rel) {
                let anchor_connection = if anchor_is_left {
                    &rel.left_connection
                } else {
                    &rel.right_connection
                };
                if anchor_connection.as_str() == alias {
                    return true;
                }
            }
            is_denorm_scan_anchor_alias(alias, &rel.left)
                || is_denorm_scan_anchor_alias(alias, &rel.right)
        }
        LogicalPlan::GraphNode(node) => is_denorm_scan_anchor_alias(alias, &node.input),
        LogicalPlan::Filter(filter) => is_denorm_scan_anchor_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => is_denorm_scan_anchor_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => is_denorm_scan_anchor_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => is_denorm_scan_anchor_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => is_denorm_scan_anchor_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => is_denorm_scan_anchor_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => is_denorm_scan_anchor_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => is_denorm_scan_anchor_alias(alias, &cte.input),
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| is_denorm_scan_anchor_alias(alias, input)),
        _ => false,
    }
}

/// #1006: the physical own-table column for `prop` on `node_schema`, or
/// `None` when the node's own table cannot resolve it. `prop` may arrive as
/// either the Cypher property name (`name` → `full_name` via
/// `property_mappings`) or an already-mapped physical column name (matched
/// against the mapping's values — e.g. a pre-rewritten `full_name`).
///
/// Node-ID names NEVER resolve here: `node_id` identity mappings are
/// auto-generated (config.rs) even for VIRTUAL ids whose own table has no
/// such column (e.g. zeek_dns `node_id: ip_address` — no `ip_address`
/// column on `dns_log`; the property exists only through the edge's
/// embedded endpoint property map). The node id always resolves via
/// the edge's embedded map, so own-table resolution is restricted to
/// explicitly-declared properties.
pub(super) fn own_table_property_column(
    node_schema: &crate::graph_catalog::graph_schema::NodeSchema,
    prop: &str,
) -> Option<String> {
    if node_schema.node_id.columns().contains(&prop) {
        return None;
    }
    if let Some(pv) = node_schema.property_mappings.get(prop) {
        return Some(pv.raw().to_string());
    }
    node_schema
        .property_mappings
        .values()
        .find(|v| v.raw() == prop)
        .map(|v| v.raw().to_string())
}

/// #1006: register own-table join requests for every filter predicate in the
/// plan tree (standalone `Filter` nodes, `GraphRel.where_predicate`s) and
/// every ORDER BY / GROUP BY / HAVING expression. Called by join_builder's
/// GraphJoins arm IMMEDIATELY BEFORE `inject_own_table_joins` — the registry
/// must be populated before the join injection reads it, and before
/// `extract_filters` / `extract_order_by` (which run after join extraction in
/// the GraphJoins arm) hit the #1006 intercept.
///
/// Descends the same wrapper chain the clause extractors walk
/// (GraphJoins/Projection/GroupBy/OrderBy/Skip/Limit/GraphNode/Union/
/// CartesianProduct + nested GraphRels), but STOPS at `WithClause`/`Cte`:
/// WITH-body scopes register during their own render (their own GraphJoins
/// arm or the flat WITH-CTE path), and a request leaked from a CTE body could
/// otherwise materialize a spurious outer LEFT JOIN.
pub(super) fn register_own_table_requests_in_plan(plan: &LogicalPlan) {
    match plan {
        LogicalPlan::Filter(filter) => {
            if let Ok(pred) = RenderExpr::try_from(filter.predicate.clone()) {
                register_own_table_property_requests(&pred, plan);
            }
            register_own_table_requests_in_plan(&filter.input);
        }
        LogicalPlan::GraphRel(rel) => {
            if let Some(wp) = &rel.where_predicate {
                if let Ok(pred) = RenderExpr::try_from(wp.clone()) {
                    register_own_table_property_requests(&pred, plan);
                }
            }
            register_own_table_requests_in_plan(&rel.left);
            register_own_table_requests_in_plan(&rel.right);
        }
        LogicalPlan::OrderBy(order_by) => {
            for item in &order_by.items {
                if let Ok(expr) = RenderExpr::try_from(item.expression.clone()) {
                    register_own_table_property_requests(&expr, plan);
                }
            }
            register_own_table_requests_in_plan(&order_by.input);
        }
        LogicalPlan::GroupBy(group_by) => {
            for expr in &group_by.expressions {
                if let Ok(rendered) = RenderExpr::try_from(expr.clone()) {
                    register_own_table_property_requests(&rendered, plan);
                }
            }
            if let Some(having) = &group_by.having_clause {
                if let Ok(rendered) = RenderExpr::try_from(having.clone()) {
                    register_own_table_property_requests(&rendered, plan);
                }
            }
            register_own_table_requests_in_plan(&group_by.input);
        }
        LogicalPlan::GraphJoins(gj) => register_own_table_requests_in_plan(&gj.input),
        LogicalPlan::Projection(p) => register_own_table_requests_in_plan(&p.input),
        LogicalPlan::Skip(s) => register_own_table_requests_in_plan(&s.input),
        LogicalPlan::Limit(l) => register_own_table_requests_in_plan(&l.input),
        LogicalPlan::GraphNode(n) => register_own_table_requests_in_plan(&n.input),
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                register_own_table_requests_in_plan(input);
            }
        }
        LogicalPlan::CartesianProduct(cp) => {
            register_own_table_requests_in_plan(&cp.left);
            register_own_table_requests_in_plan(&cp.right);
        }
        // Scope barriers: WITH/CTE bodies register during their own render.
        LogicalPlan::WithClause(_) | LogicalPlan::Cte(_) => {}
        _ => {}
    }
}

/// #1006: resolve the node label behind an own-table join request. Tries, in
/// order:
///   1. the plan's own `GraphNode` label (labeled queries);
///   2. the canonical connection-node fallback (#551/#560/#562) for unlabeled
///      denormalized chain nodes, which works when the relationship TYPE is
///      declared (`rel.labels`);
///   3. a schema-wide match of the endpoint's embedded id key: the embedded
///      map's cypher KEY is the node's own-table id property (e.g. `pid` in
///      `pid: mgr_id`), so any node schema whose single-column id equals that
///      key AND whose own table resolves `col` is a candidate. Covers
///      `MATCH (a)-[r]->(b)` where NO relationship type is declared (case 2's
///      rel-type lookup cannot fire). A unique match is required — ambiguity
///      bails to `None` so callers keep their pre-#1006 fall-through.
pub(super) fn own_table_label_for_alias(
    plan: &LogicalPlan,
    alias: &str,
    embedded: &[(String, String)],
    col: &str,
) -> Option<String> {
    if let Some(label) = get_node_label_for_alias(alias, plan) {
        return Some(label);
    }
    let schema = crate::server::query_context::get_current_schema_with_fallback();
    if let Some(schema) = schema.as_ref() {
        if let Some(label) =
            super::plan_builder_utils::find_denorm_connection_node_label(plan, alias, schema)
        {
            return Some(label);
        }
    }
    let schema = schema.as_ref()?;
    let mut matches: Vec<&str> = Vec::new();
    for (label, ns) in schema.all_node_schemas() {
        let id_cols = ns.node_id.columns();
        if id_cols.len() != 1 {
            continue;
        }
        if embedded.iter().any(|(p, _)| p == id_cols[0])
            && own_table_property_column(ns, col).is_some()
        {
            matches.push(label);
        }
    }
    if matches.len() == 1 {
        Some(matches[0].to_string())
    } else {
        None
    }
}

/// #1006: register own-table join requests for every property access in
/// `expr` whose alias is a denormalized-edge endpoint whose property is
/// ABSENT from the edge's embedded property map but resolvable from the
/// node's own table. Called at filter/where-predicate build time — BEFORE
/// join injection — so the filter path reaches the #1006 intercept in
/// `apply_property_mapping_to_expr` with a populated registry. Without this,
/// a WHERE-only reference on a mixed-access (foreign-embedded) endpoint was
/// unmapped and fell through to the denormalized-alias remap → `t1.name` on
/// the edge table → ClickHouse Code 47 (the select/group-by/order-by
/// builders register eagerly, so only the filter path was broken).
///
/// The guard mirrors select_builder's #1006 branch exactly:
/// `get_properties_with_table_alias` must bind the alias to an edge (Some
/// override). That binding is only produced for NON-VLP denormalized roles —
/// variable-length / shortest-path endpoints return a `None` override (the
/// VLP rewriter owns their columns), so no request is ever registered for
/// them. Properties present in the embedded map (by cypher name OR physical
/// column) are left alone, as is everything the own table cannot resolve.
pub(super) fn register_own_table_property_requests(expr: &RenderExpr, plan: &LogicalPlan) {
    use crate::render_plan::properties_builder::PropertiesBuilder;
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let alias = &prop.table_alias.0;
            let col = prop.column.raw();
            if let Ok((embedded, Some(_edge_alias))) = plan.get_properties_with_table_alias(alias) {
                let embedded_hit = embedded.iter().any(|(p, c)| p == col || c == col);
                if !embedded_hit && !is_denorm_scan_anchor_alias(alias, plan) {
                    if let Some(label) = own_table_label_for_alias(plan, alias, &embedded, col) {
                        let schema =
                            crate::server::query_context::get_current_schema_with_fallback();
                        if let Some(ns) = schema.as_ref().and_then(|s| s.node_schema_opt(&label)) {
                            if own_table_property_column(ns, col).is_some() {
                                crate::server::query_context::register_own_table_join_request(
                                    alias, &label, col,
                                );
                            }
                        }
                    }
                }
            }
        }
        RenderExpr::ScalarFnCall(sf) => {
            for arg in &sf.args {
                register_own_table_property_requests(arg, plan);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                register_own_table_property_requests(arg, plan);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                register_own_table_property_requests(operand, plan);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(e) = &case.expr {
                register_own_table_property_requests(e, plan);
            }
            for (w, t) in &case.when_then {
                register_own_table_property_requests(w, plan);
                register_own_table_property_requests(t, plan);
            }
            if let Some(e) = &case.else_expr {
                register_own_table_property_requests(e, plan);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            register_own_table_property_requests(&reduce.initial_value, plan);
            register_own_table_property_requests(&reduce.list, plan);
            register_own_table_property_requests(&reduce.expression, plan);
        }
        RenderExpr::List(items) => {
            for item in items {
                register_own_table_property_requests(item, plan);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                register_own_table_property_requests(v, plan);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            register_own_table_property_requests(array, plan);
            register_own_table_property_requests(index, plan);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            register_own_table_property_requests(array, plan);
            if let Some(f) = from {
                register_own_table_property_requests(f, plan);
            }
            if let Some(t) = to {
                register_own_table_property_requests(t, plan);
            }
        }
        // Subqueries own a separate alias scope (InSubquery/ExistsSubquery/
        // PatternCount) and leaves carry no property accesses.
        _ => {}
    }
}

/// #1007 (M1/M2): own-table column resolution for a property access whose
/// alias has a registered own-table join request (see
/// `register_own_table_join_request`). Returns the node's OWN table physical
/// column when the property is among the request's properties (matched by
/// Cypher name or by physical column), else `None`. Mirrors the #1006
/// filter-path intercept in `apply_property_mapping_to_expr_shielded`: the
/// alias names the lazily-injected own-table LEFT JOIN, so the column must
/// resolve against the node's own table — NOT the edge's embedded map
/// (which lacks the property → Code 47 if redirected to the edge table).
/// A `None` here means "no request registered" — callers must fall through
/// to their pre-#1007 behavior, never register from here (registration is
/// purely a pre-injection concern; a request registered after the join
/// injection point would dangle and leak — see `OwnTableJoinGuard`).
pub(super) fn own_table_property_resolution(table_alias: &str, col: &str) -> Option<String> {
    let requests = crate::server::query_context::own_table_join_requests();
    let own = requests.get(table_alias)?;
    let task_schema = crate::server::query_context::get_current_schema_with_fallback();
    let node_schema = task_schema
        .as_ref()
        .and_then(|s| s.node_schema_opt(&own.node_label))?;
    let matched = own.properties.iter().find(|p| {
        **p == col || own_table_property_column(node_schema, p).as_deref() == Some(col)
    })?;
    own_table_property_column(node_schema, matched)
}

/// #1007 (M2): walk `expr` and resolve every property access whose alias has
/// a registered own-table join request against the node's OWN table physical
/// column. Used for the post-WITH WHERE→HAVING clause, which renders AFTER
/// the CTE body's join injection: the injected own-table LEFT JOIN binds the
/// NODE alias, so a non-embedded reference (`a.name` when only `pid` is
/// embedded) must point at the join's physical column, not dangle unbindable.
/// Deliberately a MIRROR of `register_own_table_property_requests`'s
/// traversal (same variant set) so registration and resolution can never
/// disagree about which expressions are reachable. No-ops for aliases
/// without a registered request — the pre-#1007 denorm remap path is
/// untouched.
pub(super) fn resolve_own_table_property_in_expr(expr: &mut RenderExpr) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let col = prop.column.raw().to_string();
            if let Some(physical) = own_table_property_resolution(&prop.table_alias.0, &col) {
                log::debug!(
                    "🔍 #1007: '{}.{}' resolved against own table column '{}' (WITH WHERE/HAVING)",
                    prop.table_alias.0,
                    col,
                    physical
                );
                prop.column =
                    crate::graph_catalog::expression_parser::PropertyValue::Column(physical);
            }
        }
        RenderExpr::ScalarFnCall(sf) => {
            for arg in &mut sf.args {
                resolve_own_table_property_in_expr(arg);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                resolve_own_table_property_in_expr(arg);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                resolve_own_table_property_in_expr(operand);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(e) = &mut case.expr {
                resolve_own_table_property_in_expr(e);
            }
            for (w, t) in &mut case.when_then {
                resolve_own_table_property_in_expr(w);
                resolve_own_table_property_in_expr(t);
            }
            if let Some(e) = &mut case.else_expr {
                resolve_own_table_property_in_expr(e);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            resolve_own_table_property_in_expr(&mut reduce.initial_value);
            resolve_own_table_property_in_expr(&mut reduce.list);
            resolve_own_table_property_in_expr(&mut reduce.expression);
        }
        RenderExpr::List(items) => {
            for item in items {
                resolve_own_table_property_in_expr(item);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                resolve_own_table_property_in_expr(v);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            resolve_own_table_property_in_expr(array);
            resolve_own_table_property_in_expr(index);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            resolve_own_table_property_in_expr(array);
            if let Some(f) = from {
                resolve_own_table_property_in_expr(f);
            }
            if let Some(t) = to {
                resolve_own_table_property_in_expr(t);
            }
        }
        // Subqueries own a separate alias scope (InSubquery/ExistsSubquery/
        // PatternCount) and leaves carry no property accesses.
        _ => {}
    }
}

/// Get the relationship alias and ID column for a denormalized node alias
/// For example, if `b` is the "to" node of `r1` or the "from" node of `r2`,
/// this returns (rel_alias, id_column_name).
/// IMPORTANT: This function should ONLY return a result for truly denormalized nodes
/// (where node properties are stored on the edge table, indicated by from_node_properties/to_node_properties).
/// For standard schemas where nodes have their own tables, this should return None so
/// the node alias stays pointing to the node table.
pub(super) fn get_denormalized_node_id_reference(
    alias: &str,
    plan: &LogicalPlan,
) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this node alias matches left or right connection
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // For multi-hop patterns like (a)-[r1]->(b)-[r2]->(c), we prefer
                // the "from" position because in GROUP BY b, we want r2.Origin
                // (where b is the origin/source of r2)

                // Check if node is the "from" node (left_connection) - this takes precedence
                // ONLY if the edge has from_node_properties (denormalized schema)
                if alias == rel.left_connection {
                    // Only remap if this is a denormalized node (properties on edge table)
                    if scan.from_node_properties.is_some() {
                        if let Some(from_id) = &scan.from_id {
                            return Some((rel.alias.clone(), from_id.to_string()));
                        }
                    }
                }
                // Check if node is the "to" node (right_connection)
                // ONLY if the edge has to_node_properties (denormalized schema)
                if alias == rel.right_connection {
                    // Only remap if this is a denormalized node (properties on edge table)
                    if scan.to_node_properties.is_some() {
                        if let Some(to_id) = &scan.to_id {
                            return Some((rel.alias.clone(), to_id.to_string()));
                        }
                    }
                }
            }

            // Recursively check branches (right first for more recent relationships)
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.right) {
                return Some(result);
            }
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.left) {
                return Some(result);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Check if this is a denormalized node
            if crate::graph_catalog::pattern_schema::node_denormalized_flag(node)
                && node.alias == alias
            {
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if let Some(from_id) = &scan.from_id {
                        return Some((alias.to_string(), from_id.to_string()));
                    }
                }
            }
            get_denormalized_node_id_reference(alias, &node.input)
        }
        LogicalPlan::Filter(filter) => get_denormalized_node_id_reference(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_denormalized_node_id_reference(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_denormalized_node_id_reference(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => {
            get_denormalized_node_id_reference(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => get_denormalized_node_id_reference(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_denormalized_node_id_reference(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => {
            get_denormalized_node_id_reference(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => get_denormalized_node_id_reference(alias, &cte.input),
        // #1007: a multi-MATCH query (`MATCH (a)-[:R]->(b) MATCH (c) ...`) plans
        // as a CartesianProduct; the injected own-table join must find the
        // denormalized edge for an endpoint alias on either side. Without this
        // arm the lookup fell into `_ => None`, so the join was never injected
        // and `a.name` (non-embedded) dangled unbindable in the SELECT/WHERE
        // → ClickHouse Code 47.
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(result) = get_denormalized_node_id_reference(alias, &cp.left) {
                return Some(result);
            }
            get_denormalized_node_id_reference(alias, &cp.right)
        }
        _ => None,
    }
}

/// Whether `p` is exactly the `SELECT 1 AS "_empty" WHERE false` placeholder
/// a branch renders as when it's pruned to `LogicalPlan::Empty` (e.g. an
/// unlabeled `MATCH (n)` whose property no node type has). Detected by exact
/// SHAPE — not by the alias name — so a column a user legitimately aliases
/// `_empty` (`RETURN x AS _empty`) is never mistaken for the placeholder.
///
/// Shared by [`normalize_union_branches`] (so the placeholder's `_empty`
/// column never enters the unified column set and forces a spurious
/// `NULL AS "_empty"` on every other branch), by the `#515` Cypher-UNION
/// column-name check in `plan_builder.rs` (so a branch that Track C pruned
/// to 0 rows isn't mistaken for a genuine column-name mismatch), and by the
/// #546 `ORDER BY id()` union-key salvage in the ClickHouse emitter (a
/// placeholder branch produces no rows, so it gets a same-shaped dummy key
/// instead of blocking the salvage).
pub(crate) fn is_empty_placeholder(p: &super::RenderPlan) -> bool {
    use crate::render_plan::render_expr::{Literal, RenderExpr};
    p.select.items.len() == 1
        && p.select.items[0].col_alias.as_ref().map(|a| a.0.as_str()) == Some("_empty")
        && matches!(
            p.select.items[0].expression,
            RenderExpr::Literal(Literal::Integer(1))
        )
        && matches!(
            p.filters.0,
            Some(RenderExpr::Literal(Literal::Boolean(false)))
        )
}

/// Normalize UNION branch SELECT items so all branches have the same columns.
/// For denormalized node queries where from_node_properties and to_node_properties
/// might have different property sets, we need to:
/// 1. Collect all unique column aliases across all branches
/// 2. For each branch, add NULL for any missing columns
///
/// Returns normalized RenderPlans with consistent SELECT items.
pub(super) fn normalize_union_branches(
    union_plans: Vec<super::RenderPlan>,
) -> Vec<super::RenderPlan> {
    use super::{RenderPlan, SelectItem, SelectItems};
    use std::collections::BTreeSet;

    if union_plans.is_empty() {
        return union_plans;
    }

    // If every branch is an empty placeholder (the whole UNION pruned to 0 rows),
    // leave them untouched: that already renders valid 0-row SQL. Normalizing to an
    // empty column set would emit a column-less `SELECT WHERE false`, which is invalid.
    if union_plans.iter().all(is_empty_placeholder) {
        return union_plans;
    }

    // Collect the unified column aliases from the REAL (non-placeholder) branches
    // only (sorted for deterministic order). Pruned placeholder branches then adopt
    // these columns as NULLs below and still return 0 rows (their `WHERE false` is
    // preserved); real branches keep their exact columns.
    let all_aliases: BTreeSet<String> = union_plans
        .iter()
        .filter(|plan| !is_empty_placeholder(plan))
        .flat_map(|plan| {
            plan.select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
        })
        .collect();

    log::debug!(
        "normalize_union_branches - {} branches, {} total unique aliases: {:?}",
        union_plans.len(),
        all_aliases.len(),
        all_aliases
    );

    // If all branches have the same aliases, no normalization needed
    let all_same = union_plans.iter().all(|plan| {
        let branch_aliases: BTreeSet<String> = plan
            .select
            .items
            .iter()
            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
            .collect();
        branch_aliases == all_aliases
    });

    if all_same {
        crate::debug_println!("DEBUG: normalize_union_branches - all branches have same aliases, no normalization needed");
        return union_plans;
    }

    crate::debug_println!(
        "DEBUG: normalize_union_branches - branches have different aliases, normalizing..."
    );

    // Normalize a single branch's SELECT to the unified `all_aliases`: wrap each
    // present column in a string cast and pad missing columns with NULL.
    //
    // CRITICAL: a branch may itself carry sibling UNION sub-branches in its
    // `union` field. The two direction branches of an undirected/bidirectional
    // relationship expansion render into ONE RenderPlan whose primary
    // SELECT/FROM is direction A and whose `union.input` holds direction B.
    // Both directions project the SAME RETURN columns, so they MUST receive the
    // SAME type coercion — otherwise direction A emits `toString(col)` while
    // direction B emits the raw column, and the UNION fails with ClickHouse
    // Code 386 NO_COMMON_TYPE (e.g. String vs Date). We therefore recurse into
    // the nested sub-branches and apply the identical coercion to each.
    fn normalize_branch(
        plan: super::RenderPlan,
        all_aliases: &BTreeSet<String>,
    ) -> super::RenderPlan {
        // Collect valid table aliases from FROM and JOINs for this branch
        let mut valid_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Some(ref from_ref) = plan.from.0 {
            if let Some(ref alias) = from_ref.alias {
                valid_aliases.insert(alias.clone());
            }
        }
        for join in &plan.joins.0 {
            valid_aliases.insert(join.table_alias.clone());
        }
        log::debug!(
            "normalize_union_branches: valid table aliases for branch: {:?}",
            valid_aliases
        );

        // Build a map of existing column aliases in this branch
        let existing: std::collections::HashMap<String, SelectItem> = plan
            .select
            .items
            .iter()
            .filter_map(|item| item.col_alias.as_ref().map(|a| (a.0.clone(), item.clone())))
            .collect();

        // Build normalized SELECT items in consistent order
        // IMPORTANT: Wrap all expressions in toString() to ensure type compatibility across UNION branches
        // This is needed because different node types may have different property types (e.g., Array vs Scalar)
        let normalized_items: Vec<SelectItem> = all_aliases
            .iter()
            .map(|alias| {
                if let Some(item) = existing.get(alias) {
                    // CRITICAL FIX: For denormalized relationships, the SELECT may reference
                    // a table alias (e.g., `r`) that doesn't exist in FROM/JOINs.
                    // We need to fix the table alias to a valid one.
                    let fixed_expr =
                        fix_invalid_table_aliases(&item.expression, &valid_aliases, &plan);

                    // Wrap the expression in a string cast for type
                    // compatibility across UNION branches.
                    SelectItem {
                        expression: RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                            name: current_function_mapper().cast_string().to_string(),
                            args: vec![fixed_expr],
                        }),
                        col_alias: item.col_alias.clone(),
                    }
                } else {
                    // Missing column - use NULL (which is compatible with any toString() result)
                    SelectItem {
                        expression: RenderExpr::Literal(Literal::Null),
                        col_alias: Some(super::ColumnAlias(alias.clone())),
                    }
                }
            })
            .collect();

        // Recurse into nested sibling UNION sub-branches (bidirectional
        // expansion) so every direction receives the identical coercion.
        let normalized_union = super::UnionItems(plan.union.0.map(|u| {
            super::Union {
                input: u
                    .input
                    .into_iter()
                    .map(|b| normalize_branch(b, all_aliases))
                    .collect(),
                union_type: u.union_type,
                is_cypher_union: u.is_cypher_union,
            }
        }));

        RenderPlan {
            select: SelectItems {
                items: normalized_items,
                distinct: plan.select.distinct,
            },
            union: normalized_union,
            ..plan
        }
    }

    // Normalize each branch
    union_plans
        .into_iter()
        .map(|plan| normalize_branch(plan, &all_aliases))
        .collect()
}

/// Fix invalid table aliases in expressions for UNION branches.
///
/// For denormalized relationships (e.g., AUTHORED stored on posts_bench),
/// the SELECT may reference a relationship alias `r` that doesn't exist in
/// FROM/JOINs. This function rewrites `r.column` to use the correct table
/// alias that actually contains that column.
///
/// Strategy:
/// 1. If table alias is valid, return expression unchanged
/// 2. If table alias is invalid (not in FROM/JOINs), try to find the FROM table
///    (for FK relationships, the FROM table usually has the relationship columns)
fn fix_invalid_table_aliases(
    expr: &RenderExpr,
    valid_aliases: &std::collections::HashSet<String>,
    plan: &super::RenderPlan,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            if valid_aliases.contains(table_alias) {
                // Table alias is valid, no change needed
                expr.clone()
            } else {
                // Table alias is invalid - this is a denormalized relationship
                // For FK edges, the FROM table contains the relationship columns
                log::info!(
                    "🔧 fix_invalid_table_aliases: '{}' not in valid aliases {:?}, using FROM table",
                    table_alias,
                    valid_aliases
                );

                // Use the FROM table alias as the replacement
                if let Some(ref from_ref) = plan.from.0 {
                    if let Some(ref from_alias) = from_ref.alias {
                        log::info!(
                            "🔧 Rewriting {}.{} → {}.{}",
                            table_alias,
                            match &prop.column {
                                PropertyValue::Column(c) => c.clone(),
                                _ => "<expr>".to_string(),
                            },
                            from_alias,
                            match &prop.column {
                                PropertyValue::Column(c) => c.clone(),
                                _ => "<expr>".to_string(),
                            }
                        );
                        return RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias.clone()),
                            column: prop.column.clone(),
                        });
                    }
                }
                // Fallback: return unchanged
                expr.clone()
            }
        }
        // Recursively fix nested expressions
        RenderExpr::ScalarFnCall(fn_call) => {
            let fixed_args: Vec<RenderExpr> = fn_call
                .args
                .iter()
                .map(|arg| fix_invalid_table_aliases(arg, valid_aliases, plan))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: fixed_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op_app.operator,
                operands: op_app
                    .operands
                    .iter()
                    .map(|arg| fix_invalid_table_aliases(arg, valid_aliases, plan))
                    .collect(),
            })
        }
        // Other expression types don't need fixing
        _ => expr.clone(),
    }
}

/// Add __label__ column to each UNION branch for node type identification.
///
/// For UNION queries across multiple node types (e.g., MATCH (n) RETURN n),
/// we need to know which node type each row belongs to. This function:
/// 1. Takes the normalized union branches
/// 2. Extracts the label from each branch's logical plan
/// 3. Adds a '__label__' column with the node type as a string literal
///
/// This enables the Bolt result transformer to construct proper Node objects
/// with correct labels array even for unlabeled queries.
pub(super) fn add_label_column_to_union_branches(
    union_plans: Vec<super::RenderPlan>,
    logical_branches: &[std::sync::Arc<LogicalPlan>],
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Vec<super::RenderPlan> {
    use super::{ColumnAlias, SelectItem};
    use crate::render_plan::cte_extraction::extract_node_label_from_viewscan_with_schema;

    if union_plans.len() != logical_branches.len() {
        log::warn!(
            "add_label_column_to_union_branches: mismatch {} render plans vs {} logical branches",
            union_plans.len(),
            logical_branches.len()
        );
        return union_plans;
    }

    union_plans
        .into_iter()
        .zip(logical_branches.iter())
        .map(|(mut plan, logical_branch)| {
            // Infer the node variable prefix from existing SELECT column aliases.
            // Columns like "n._tck_id", "n.name" → prefix "n".
            // Columns like "n.__label__" → already qualified, use that prefix.
            let node_prefix = plan
                .select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref())
                .find_map(|alias| {
                    let a = &alias.0;
                    a.find('.').map(|dot| a[..dot].to_string())
                });

            // Build the qualified label column alias: "n.__label__" or "__label__"
            let label_alias = if let Some(ref prefix) = node_prefix {
                format!("{prefix}.__label__")
            } else {
                "__label__".to_string()
            };

            // Check if __label__ already exists (qualified or unqualified) - skip if so
            let has_label = plan.select.items.iter().any(|item| {
                item.col_alias
                    .as_ref()
                    .is_some_and(|a| a.0 == "__label__" || a.0.ends_with(".__label__"))
            });

            if has_label {
                log::debug!(
                    "add_label_column_to_union_branches: __label__ already exists, skipping"
                );
                return plan;
            }

            // Extract label from the logical plan's ViewScan
            let full_label = extract_node_label_from_viewscan_with_schema(logical_branch, schema)
                .unwrap_or_default();

            // Extract just the base label (e.g., "User" from "brahmand::users_bench::User")
            // Use empty string for the synthetic unlabeled bucket.
            let base_label = if full_label == "__Unlabeled" {
                String::new()
            } else if full_label.contains("::") {
                full_label
                    .split("::")
                    .last()
                    .unwrap_or(&full_label)
                    .to_string()
            } else {
                full_label
            };

            log::debug!(
                "add_label_column_to_union_branches: branch has label {:?}, alias {:?}",
                base_label,
                label_alias
            );

            // Add the label column with the qualified alias (e.g. "n.__label__")
            let label_item = SelectItem {
                expression: RenderExpr::Literal(Literal::String(base_label)),
                col_alias: Some(ColumnAlias(label_alias)),
            };

            // Prepend __label__ to existing SELECT items
            let mut new_items = vec![label_item];
            new_items.extend(plan.select.items);
            plan.select.items = new_items;

            plan
        })
        .collect()
}

/// Add __start_label__ and __end_label__ columns to path UNION branches.
///
/// For UNION queries produced by PatternResolver for path queries (e.g., MATCH p=()-[r:FOLLOWS]->() RETURN p),
/// each branch has concrete start/end node types. We add these as literal columns so the
/// result transformer can determine node labels per row.
pub(super) fn add_path_label_columns_to_union_branches(
    union_plans: Vec<super::RenderPlan>,
    logical_branches: &[std::sync::Arc<LogicalPlan>],
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Vec<super::RenderPlan> {
    use super::{ColumnAlias, SelectItem};

    if union_plans.len() != logical_branches.len() {
        log::warn!(
            "add_path_label_columns: mismatch {} render plans vs {} logical branches",
            union_plans.len(),
            logical_branches.len()
        );
        return union_plans;
    }

    union_plans
        .into_iter()
        .zip(logical_branches.iter())
        .map(|(mut plan, logical_branch)| {
            // Check if __start_label__ already exists
            let has_start = plan.select.items.iter().any(|item| {
                item.col_alias
                    .as_ref()
                    .is_some_and(|a| a.0 == "__start_label__")
            });
            if has_start {
                return plan;
            }

            // Extract start and end labels from the GraphRel in this branch
            let (start_label, end_label) = extract_path_labels_from_plan(logical_branch, schema);

            log::debug!(
                "add_path_label_columns: start={:?}, end={:?}",
                start_label,
                end_label
            );

            let start_item = SelectItem {
                expression: RenderExpr::Literal(Literal::String(
                    start_label.unwrap_or_else(|| "Unknown".to_string()),
                )),
                col_alias: Some(ColumnAlias("__start_label__".to_string())),
            };
            let end_item = SelectItem {
                expression: RenderExpr::Literal(Literal::String(
                    end_label.unwrap_or_else(|| "Unknown".to_string()),
                )),
                col_alias: Some(ColumnAlias("__end_label__".to_string())),
            };

            // Prepend to SELECT items
            let mut new_items = vec![start_item, end_item];
            new_items.extend(plan.select.items);
            plan.select.items = new_items;

            plan
        })
        .collect()
}

/// Extract start and end node labels from a plan containing a GraphRel.
fn extract_path_labels_from_plan(
    plan: &LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> (Option<String>, Option<String>) {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            let start =
                crate::render_plan::cte_extraction::extract_node_label_from_viewscan_with_schema(
                    &gr.left, schema,
                );
            let end =
                crate::render_plan::cte_extraction::extract_node_label_from_viewscan_with_schema(
                    &gr.right, schema,
                );
            (start, end)
        }
        LogicalPlan::Projection(p) => extract_path_labels_from_plan(&p.input, schema),
        LogicalPlan::Filter(f) => extract_path_labels_from_plan(&f.input, schema),
        LogicalPlan::Limit(l) => extract_path_labels_from_plan(&l.input, schema),
        LogicalPlan::Skip(s) => extract_path_labels_from_plan(&s.input, schema),
        LogicalPlan::OrderBy(o) => extract_path_labels_from_plan(&o.input, schema),
        LogicalPlan::GraphJoins(gj) => extract_path_labels_from_plan(&gj.input, schema),
        _ => (None, None),
    }
}

/// Check if a GraphRel has a WithClause as its right side.
/// This indicates a "WITH ... MATCH" pattern that requires CTE-based processing.
/// The WITH clause creates a derived table that the subsequent MATCH must join against.
///
/// Note: The Projection(With) may have been transformed to Projection(Return) by analyzer passes,
/// but the structure is still identifiable by having GraphJoins/Union inside GraphRel.right
/// that contains a separate pattern (the first MATCH).
pub(super) fn has_with_clause_in_graph_rel(plan: &LogicalPlan) -> bool {
    // Helper to check if a plan contains actual WITH clause )
    fn contains_actual_with_clause(plan: &LogicalPlan) -> bool {
        match plan {
            // New WithClause type takes precedence
            LogicalPlan::WithClause(_wc) => {
                log::info!("🔍 contains_actual_with_clause: Found WithClause node");
                true
            }
            LogicalPlan::Projection(proj) => contains_actual_with_clause(&proj.input),
            LogicalPlan::GraphJoins(gj) => contains_actual_with_clause(&gj.input),
            LogicalPlan::GraphRel(gr) => {
                contains_actual_with_clause(&gr.left) || contains_actual_with_clause(&gr.right)
            }
            LogicalPlan::Filter(f) => contains_actual_with_clause(&f.input),
            LogicalPlan::GroupBy(gb) => contains_actual_with_clause(&gb.input),
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| contains_actual_with_clause(i)),
            LogicalPlan::CartesianProduct(cp) => {
                contains_actual_with_clause(&cp.left) || contains_actual_with_clause(&cp.right)
            }
            LogicalPlan::GraphNode(gn) => contains_actual_with_clause(&gn.input),
            LogicalPlan::Limit(l) => contains_actual_with_clause(&l.input),
            LogicalPlan::OrderBy(o) => contains_actual_with_clause(&o.input),
            LogicalPlan::Skip(s) => contains_actual_with_clause(&s.input),
            LogicalPlan::ViewScan(_) => false, // ViewScan is a leaf - no WITH here
            _ => false,
        }
    }

    match plan {
        // NEW: Direct WithClause at any level in the plan
        LogicalPlan::WithClause(_wc) => {
            log::info!("🔍 has_with_clause_in_graph_rel: Found WithClause at plan root");
            true
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if right side contains a Union or GraphJoins with nested patterns
            // This indicates a WITH+MATCH structure where the WITH clause output
            // was wrapped in Union (for undirected patterns) or GraphJoins
            let right_has_nested_pattern = match graph_rel.right.as_ref() {
                // NEW: Direct WithClause in GraphRel.right
                LogicalPlan::WithClause(_wc) => {
                    log::info!(
                        "🔍 has_with_clause_in_graph_rel: Found WithClause in GraphRel.right"
                    );
                    true
                }
                // Union containing GraphJoins - check if it actually contains WITH clause
                LogicalPlan::Union(union) => {
                    // Only flag as WITH pattern if there's an actual WITH clause inside
                    let has_with_inside = union
                        .inputs
                        .iter()
                        .any(|input| contains_actual_with_clause(input));
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found Union with WITH clause inside in GraphRel.right - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                // GraphJoins directly - check if it actually contains WITH clause
                LogicalPlan::GraphJoins(gj) => {
                    let has_with_inside = contains_actual_with_clause(&gj.input);
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found GraphJoins with WITH clause inside in GraphRel.right - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                _ => false,
            };

            if right_has_nested_pattern {
                return true;
            }

            // Also check left side (for incoming patterns)
            let left_has_nested_pattern = match graph_rel.left.as_ref() {
                // NEW: Direct WithClause in GraphRel.left
                LogicalPlan::WithClause(_wc) => {
                    log::info!(
                        "🔍 has_with_clause_in_graph_rel: Found WithClause in GraphRel.left"
                    );
                    true
                }
                LogicalPlan::Union(union) => {
                    let has_with_inside = union
                        .inputs
                        .iter()
                        .any(|input| contains_actual_with_clause(input));
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found Union with WITH clause inside in GraphRel.left - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                LogicalPlan::GraphJoins(gj) => {
                    let has_with_inside = contains_actual_with_clause(&gj.input);
                    if has_with_inside {
                        log::info!("🔍 has_with_clause_in_graph_rel: Found GraphJoins with WITH clause inside in GraphRel.left - WITH+MATCH pattern");
                    }
                    has_with_inside
                }
                _ => false,
            };

            if left_has_nested_pattern {
                return true;
            }

            // Recursively check nested GraphRels
            has_with_clause_in_graph_rel(&graph_rel.left)
                || has_with_clause_in_graph_rel(&graph_rel.right)
        }
        LogicalPlan::Projection(proj) => has_with_clause_in_graph_rel(&proj.input),
        LogicalPlan::Filter(filter) => has_with_clause_in_graph_rel(&filter.input),
        LogicalPlan::GroupBy(group_by) => has_with_clause_in_graph_rel(&group_by.input),
        LogicalPlan::GraphJoins(graph_joins) => has_with_clause_in_graph_rel(&graph_joins.input),
        LogicalPlan::Limit(limit) => has_with_clause_in_graph_rel(&limit.input),
        LogicalPlan::OrderBy(order_by) => has_with_clause_in_graph_rel(&order_by.input),
        LogicalPlan::Skip(skip) => has_with_clause_in_graph_rel(&skip.input),
        // Check Union at top level - WITH clauses might be inside Union branches
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| has_with_clause_in_graph_rel(input)),
        // Check CartesianProduct - WITH clauses might be in either branch
        LogicalPlan::CartesianProduct(cp) => {
            has_with_clause_in_graph_rel(&cp.left) || has_with_clause_in_graph_rel(&cp.right)
        }
        _ => false,
    }
}

// ============================================================================
// Predicate Analysis Helpers
// These functions help analyze and manipulate logical expressions (predicates)
// ============================================================================

use crate::query_planner::logical_expr::{
    LogicalExpr, Operator as LogicalOperator, OperatorApplication as LogicalOpApp,
};

/// Collect all table aliases referenced in a LogicalExpr.
/// Used to determine which aliases a predicate depends on.
pub(super) fn collect_aliases_from_logical_expr(expr: &LogicalExpr, aliases: &mut HashSet<String>) {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_logical_expr(operand, aliases);
            }
        }
        LogicalExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_aliases_from_logical_expr(arg, aliases);
            }
        }
        LogicalExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_aliases_from_logical_expr(arg, aliases);
            }
        }
        LogicalExpr::Case(case) => {
            if let Some(expr) = &case.expr {
                collect_aliases_from_logical_expr(expr, aliases);
            }
            for (cond, result) in &case.when_then {
                collect_aliases_from_logical_expr(cond, aliases);
                collect_aliases_from_logical_expr(result, aliases);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_aliases_from_logical_expr(else_expr, aliases);
            }
        }
        LogicalExpr::List(items) => {
            for item in items {
                collect_aliases_from_logical_expr(item, aliases);
            }
        }
        _ => {}
    }
}

/// Check if a LogicalExpr references ONLY the specified alias.
/// Returns true if the expression contains exactly one alias and it matches `alias`.
pub(super) fn references_only_alias_logical(expr: &LogicalExpr, alias: &str) -> bool {
    let mut aliases = HashSet::new();
    collect_aliases_from_logical_expr(expr, &mut aliases);
    aliases.len() == 1 && aliases.contains(alias)
}

/// Split an AND-connected LogicalExpr into individual predicates.
/// For example: `a AND b AND c` becomes `[a, b, c]`.
pub(super) fn split_and_predicates_logical(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::OperatorApplicationExp(op) if matches!(op.operator, LogicalOperator::And) => {
            let mut result = Vec::new();
            for operand in &op.operands {
                result.extend(split_and_predicates_logical(operand));
            }
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Combine multiple LogicalExpr predicates with AND.
/// Returns None if the input is empty.
pub(super) fn combine_predicates_with_and_logical(
    predicates: Vec<LogicalExpr>,
) -> Option<LogicalExpr> {
    if predicates.is_empty() {
        None
    } else if predicates.len() == 1 {
        Some(predicates.into_iter().next().unwrap())
    } else {
        Some(LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::And,
            operands: predicates,
        }))
    }
}

/// Extract predicates from a where_predicate that reference ONLY a specific alias.
/// Returns (predicates_for_alias, remaining_predicates).
/// This is used to move optional-alias predicates into LEFT JOIN pre_filter.
pub(super) fn extract_predicates_for_alias_logical(
    where_predicate: &Option<LogicalExpr>,
    target_alias: &str,
) -> (Option<RenderExpr>, Option<LogicalExpr>) {
    let predicate = match where_predicate {
        Some(p) => p,
        None => return (None, None),
    };

    let all_predicates = split_and_predicates_logical(predicate);
    let mut for_alias = Vec::new();
    let mut remaining = Vec::new();

    for pred in all_predicates {
        if references_only_alias_logical(&pred, target_alias) {
            for_alias.push(pred);
        } else {
            remaining.push(pred);
        }
    }

    // Convert for_alias predicates to RenderExpr
    let alias_filter = if for_alias.is_empty() {
        None
    } else {
        let combined = combine_predicates_with_and_logical(for_alias).unwrap();
        RenderExpr::try_from(combined).ok()
    };

    (alias_filter, combine_predicates_with_and_logical(remaining))
}

// ============================================================================
// JOIN Extraction Helpers
// These functions assist with extracting JOIN clauses from the logical plan
// ============================================================================

/// Extract schema filter from a LogicalPlan for LEFT JOIN pre_filter.
/// This ensures schema filters are applied BEFORE the LEFT JOIN (correct semantics).
pub(super) fn get_schema_filter_for_node(plan: &LogicalPlan, alias: &str) -> Option<RenderExpr> {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                if let Some(ref sf) = vs.schema_filter {
                    if let Ok(sql) = sf.to_sql(alias) {
                        return Some(RenderExpr::Raw(sql));
                    }
                }
            }
            None
        }
        LogicalPlan::ViewScan(vs) => {
            if let Some(ref sf) = vs.schema_filter {
                if let Ok(sql) = sf.to_sql(alias) {
                    return Some(RenderExpr::Raw(sql));
                }
            }
            None
        }
        _ => None,
    }
}

/// Generate polymorphic edge type filter for JOIN clauses.
/// For polymorphic edges, adds: r.type_column IN ('TYPE1', 'TYPE2') AND r.from_label = 'NodeType' AND r.to_label = 'NodeType'
/// For single type: r.type_column = 'EDGE_TYPE'
pub(super) fn get_polymorphic_edge_filter_for_join(
    center: &LogicalPlan,
    alias: &str,
    rel_types: &[String],
    from_label: &Option<String>,
    to_label: &Option<String>,
) -> Option<RenderExpr> {
    // Extract ViewScan from center (might be wrapped in GraphNode)
    let view_scan = match center {
        LogicalPlan::ViewScan(vs) => Some(vs.as_ref()),
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                Some(vs.as_ref())
            } else {
                None
            }
        }
        _ => None,
    }?;

    // Check if this is a polymorphic edge (has type_column, from_label_column, or to_label_column)
    let has_polymorphic_fields = view_scan.type_column.is_some()
        || view_scan.from_label_column.is_some()
        || view_scan.to_label_column.is_some();

    if !has_polymorphic_fields {
        return None;
    }

    log::debug!(
        "Generating polymorphic edge filter for alias='{}', rel_types={:?}, type_col={:?}, from_label_col={:?}, to_label_col={:?}",
        alias, rel_types, view_scan.type_column, view_scan.from_label_column, view_scan.to_label_column
    );

    let mut filters = Vec::new();

    // Filter 1: type_column = 'EDGE_TYPE' (single) OR type_column IN ('TYPE1', 'TYPE2') (multiple)
    if let Some(type_col) = &view_scan.type_column {
        if rel_types.len() == 1 {
            filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(type_col.clone()),
                    }),
                    RenderExpr::Literal(Literal::String(rel_types[0].clone())),
                ],
            }));
        } else if rel_types.len() > 1 {
            let type_list: Vec<RenderExpr> = rel_types
                .iter()
                .map(|t| RenderExpr::Literal(Literal::String(t.clone())))
                .collect();
            filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::In,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(type_col.clone()),
                    }),
                    RenderExpr::List(type_list),
                ],
            }));
        }
    }

    // Filter 2: from_label_column = 'FromNodeType' (if label provided and not $any)
    if let Some(from_label_col) = &view_scan.from_label_column {
        if let Some(from_label_str) = from_label {
            if !from_label_str.is_empty() && from_label_str != "$any" {
                filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(alias.to_string()),
                            column: PropertyValue::Column(from_label_col.clone()),
                        }),
                        RenderExpr::Literal(Literal::String(from_label_str.clone())),
                    ],
                }));
            }
        }
    }

    // Filter 3: to_label_column = 'ToNodeType' (if label provided and not $any)
    if let Some(to_label_col) = &view_scan.to_label_column {
        if let Some(to_label_str) = to_label {
            if !to_label_str.is_empty() && to_label_str != "$any" {
                filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(alias.to_string()),
                            column: PropertyValue::Column(to_label_col.clone()),
                        }),
                        RenderExpr::Literal(Literal::String(to_label_str.clone())),
                    ],
                }));
            }
        }
    }

    // Combine filters with AND
    combine_render_exprs_with_and(filters)
}

/// #554/#565: does EITHER endpoint of `gr` (anchor or optional side) share
/// its physical table with the relationship's own scan (`gr.center`)? —
/// i.e. does this `GraphRel` collapse to a SINGLE JOIN between the two node
/// aliases directly, with no genuinely separate edge table/JOIN in between?
///
/// This is the SAME structural question `apply_optional_node_pre_filters`
/// (join_builder.rs, the #474 mechanism) answers at render time via its
/// `connects_via_rel` check on the already-built JOIN's ON clause: true for
/// the "node IS the edge" shapes (FK-edge / denormalized, where EITHER
/// node's own table doubles as the edge table, so the optional node's LEFT
/// JOIN connects DIRECTLY to the anchor and #474 safely folds an
/// optional-node predicate into that JOIN's `pre_filter`); false for the
/// "traditional" separate-edge-table shape (standard / composite-key),
/// where the optional node instead joins THROUGH a distinct, unfiltered
/// edge LEFT JOIN — #474 deliberately declines that shape (folding only the
/// node subquery there would resurrect the edge row as a spurious
/// NULL-extended duplicate; see #474's own report) and leaves it to
/// `fold_optional_edge_node_join_with_predicate` (the #479/#552 family in
/// plan_optimizer.rs), which needs the predicate to survive in the render
/// plan's outer `filters` to find and fold it.
///
/// #565: checks BOTH endpoints, not just the optional side. An earlier
/// version checked only whether the OPTIONAL side embeds the edge — correct
/// for shapes like #554's (`(b)<-[:FOLLOWS]-(a)`, optional `b` on a separate
/// table), but wrong for an FK-edge shape where it's the ANCHOR that embeds
/// the edge and the optional side is a genuinely separate node table (e.g.
/// `MATCH (o:Order) OPTIONAL MATCH (o)-[:PLACED_BY]->(c:Customer) WHERE
/// c.name = 'Alice'`, `o`/`orders_fk` doubling as the edge, `c`/
/// `customers_fk` a plain separate table). That shape STILL collapses to a
/// single direct JOIN (`c` LEFT JOIN ... ON c.customer_id = o.customer_id`,
/// no separate edge alias) — #474 claims it exactly like the "optional side
/// embeds the edge" shapes — but the old optional-side-only check said
/// "not recoverable", so `collect_graphrel_predicates` kept the predicate
/// in the outer WHERE TOO, double-embedding it. Live-verified wrong result:
/// the redundant outer `WHERE c.name = 'Alice'` evaluated to NULL (not
/// true) against every NULL-extended (non-matching) anchor row and silently
/// dropped it — 3 rows returned instead of the correct 8 (5 orders whose
/// customer isn't Alice should survive NULL-extended, not vanish).
///
/// `collect_graphrel_predicates` below must agree with `apply_optional_node_
/// pre_filters` on which of these two mechanisms will actually claim a given
/// "references only the optional node" predicate — otherwise BOTH decline
/// (each independently assuming the other already handled it, or will), and
/// the predicate is silently dropped entirely (#554: `MATCH (a:User)
/// OPTIONAL MATCH (b:User)<-[:FOLLOWS]-(a) WHERE b.country='US'` rendered
/// with NO filter applied at all — 0 dropped rows on either mechanism's
/// ledger, just a vanished WHERE clause); or BOTH claim it, double-embedding
/// (#565).
///
/// Computed structurally (physical table identity), exactly mirroring the
/// render-time `connects_via_rel` check's spirit — not a raw schema-pattern
/// classification flag branch (CLAUDE.md rule 7): a genuinely separate edge
/// table can never equal either node's own table, while every "node IS the
/// edge" shape's defining trait IS that identity, regardless of WHICH node
/// plays that role.
fn optional_node_shares_table_with_edge(gr: &crate::query_planner::logical_plan::GraphRel) -> bool {
    // #533/#565: the denormalized standalone-scan CTE + LEFT JOIN render path
    // (`optional_denorm_union_anchor_is_left`, plan_builder.rs — the anchor
    // is rendered as its own `__denorm_scan_{alias}` CTE, entirely bypassing
    // the normal JOIN-building code `apply_optional_node_pre_filters` (#474,
    // join_builder.rs) lives in) is NEITHER structurally reachable NOR
    // currently claimed by #474 at all — confirmed by the still-open #533
    // characterization (`denorm_479_plain_optional_where_drops_null_extended_rows_known_broken`).
    // Table-identity overlap alone can't distinguish this shape from the
    // FK-edge "single JOIN" shape #474 DOES claim (both have the optional
    // node's own table equal to `gr.center`'s), so this must be checked
    // explicitly: never report "recoverable" here, or a genuinely
    // optional-node-only conjunct on this path would be dropped from the
    // outer WHERE (assuming #474 will pick it up) while #474 never does —
    // silently losing the filter entirely, worse than the pre-#565
    // (redundant-but-present) bare-outer-WHERE placement this function
    // otherwise wants to avoid.
    if is_optional_denorm_union_graphrel(gr) {
        return false;
    }

    fn source_tables(plan: &LogicalPlan, out: &mut HashSet<String>) {
        match plan {
            LogicalPlan::ViewScan(vs) => {
                out.insert(vs.source_table.clone());
            }
            LogicalPlan::GraphNode(gn) => source_tables(&gn.input, out),
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    source_tables(input, out);
                }
            }
            _ => {}
        }
    }

    let mut edge_tables = HashSet::new();
    source_tables(&gr.center, &mut edge_tables);
    if edge_tables.is_empty() {
        // Center isn't a plain ViewScan (e.g. a VLP CTE) — can't determine
        // structurally; conservatively assume NOT shared, so the predicate
        // stays in the outer filters (matches prior "keep" behavior).
        return false;
    }

    let mut left_tables = HashSet::new();
    source_tables(&gr.left, &mut left_tables);
    if !left_tables.is_disjoint(&edge_tables) {
        return true;
    }
    let mut right_tables = HashSet::new();
    source_tables(&gr.right, &mut right_tables);
    !right_tables.is_disjoint(&edge_tables)
}

/// #645: true "is the OPTIONAL-VLP anchor the outer-FROM-bound start node"
/// test — the anchor gate (`optional_anchor_where`) must reference ONLY the
/// pattern's `left_connection` (the from-id / VLP-`start_id` side, which the
/// outer `FROM … AS <left_connection>` binds). Replaces the old
/// `direction == Outgoing` proxy as an ADDITIVE admission: it lets the
/// reversed-written `(b)<-[*]-(a)` shape (anchor `a` IS the start node but the
/// pattern parses to Incoming) fold its gate like the equivalent outgoing
/// `(a)-[*]->(b)`. Returns false (→ do not newly admit) when the gate is
/// absent or references any alias other than `left_connection`.
fn anchor_gate_targets_left_connection(gr: &crate::query_planner::logical_plan::GraphRel) -> bool {
    let Some(ref anchor_where) = gr.optional_anchor_where else {
        return false;
    };
    let mut aliases = HashSet::new();
    collect_aliases_from_logical_expr(anchor_where, &mut aliases);
    // Foldable iff every referenced alias is the from-id/start side. An empty
    // alias set (a constant gate, e.g. `WHERE true`) has no outer-alias
    // dependency and folds harmlessly into the start-side ON.
    aliases.iter().all(|a| a == &gr.left_connection)
}

/// #597: the conjuncts of an OPTIONAL MATCH clause's own WHERE that reference
/// ONLY a mandatory anchor variable, recorded on
/// `GraphRel.optional_anchor_where` during lowering (the only stage where
/// "this WHERE belongs to the OPTIONAL clause" is unambiguous — see the field
/// doc). Per OPTIONAL MATCH semantics they must gate the match (fold into the
/// pattern's gating LEFT JOIN ON, NULL-extending on failure), never filter
/// the joined result set: the old outer-WHERE placement silently dropped
/// every NULL-extended anchor row.
///
/// Placement is coordinated across two sites that MUST agree, both calling
/// this helper so the classification can never diverge:
///   - `collect_graphrel_predicates` (below) drops these conjuncts from the
///     outer WHERE;
///   - `apply_optional_node_pre_filters` (join_builder.rs) appends them to
///     the gating LEFT JOIN's `joining_on` (a false LEFT-JOIN ON just
///     NULL-extends — the #472 post-WITH precedent).
///
/// Scope gates — empty result (old placement preserved) for:
///   - VLP / shortestPath GraphRels (their predicates are CTE-managed);
///   - pattern-union CTEs (`pattern_combinations` — CTE-internal joins);
///   - denormalized standalone-scan anchors (the #553 collector branch
///     already embeds anchor-only conjuncts into the `__denorm_scan` CTE);
///   - post-WITH patterns (`cte_references` non-empty — the #472 restructure
///     in plan_builder_utils.rs already moves ALL conjuncts into the ON);
///   - conjuncts that don't convert to a renderable `OperatorApplication`
///     (`joining_on` can only hold those; they stay in the outer WHERE).
pub(super) fn optional_anchor_gate_conjuncts(
    gr: &crate::query_planner::logical_plan::GraphRel,
) -> Vec<LogicalExpr> {
    if !gr.is_optional.unwrap_or(false)
        // #621/#645: a single-CTE variable-length OPTIONAL pattern is gateable
        // iff the anchor gate is bound by the outer FROM — i.e. it references the
        // pattern's `left_connection` (the from-id / VLP-`start_id` side, which
        // the outer `FROM <label> AS <left_connection>` binds via
        // `LEFT JOIN vlp_… AS vt0 ON <left_connection>.id = vt0.start_id`).
        //
        // This is expressed ADDITIVELY over the #621 rule (`direction ==
        // Outgoing`) rather than replacing it, to preserve main's exact
        // loud/silent behavior on every non-#645 shape (ground rule 1 — never
        // convert a loud error into a silent-wrong result):
        //   - outgoing `(a)-[*]->(b)` gate on a → Outgoing → FOLD (#621).
        //   - reversed `(b)<-[*]-(a)` gate on a → left_conn=a, gate={a} →
        //     `anchor_gate_targets_left_connection` → FOLD (#645). This is the
        //     ONLY shape this clause newly admits: same anchor-is-start-node
        //     semantics as #621, just written right-to-left so it parses to
        //     Incoming. Oracle-verified NULL-extends gated-out anchors.
        //   - genuine `(a)<-[*]-(b)` gate on a → left_conn=b, gate={a} → neither
        //     Outgoing nor targets-left → EXCLUDE (anchor is the END node, gated
        //     inside the CTE as `WHERE end_is_active`; unchanged from main).
        //   - outgoing `(a)-[*]->(b)` gate on b (anchor is END) → Outgoing →
        //     still folds, still Code 47 on main — a PRE-EXISTING end-anchored
        //     VLP render bug (wrong outer FROM / `vt0.end_name` projection). Left
        //     exactly as-is (stays loud); NOT newly excluded here, because
        //     excluding it would drop it to the broken-but-executing end-anchored
        //     path → silent-wrong. Tracked as a separate follow-up.
        // UNDIRECTED (`was_undirected`) stays excluded regardless (two-arm /
        // doubled-edge layout breaks the combined-anchor rewrite).
        || (gr.variable_length.is_some()
            && (gr.was_undirected == Some(true)
                || (gr.direction != crate::query_planner::logical_expr::Direction::Outgoing
                    && !anchor_gate_targets_left_connection(gr))))
        || gr.shortest_path_mode.is_some()
        || gr.pattern_combinations.is_some()
        || !gr.cte_references.is_empty()
        // Undirected (BidirectionalUnion) excluded: folding the gate into the
        // reversed arm's edge-join ON makes fold_optional_edge_node_join_with_
        // predicate decline its combined-anchor rewrite, leaving that arm's
        // out-of-order join layout unrepaired (Code 47). Directed only — the
        // same scope precedent as #603.
        || gr.was_undirected == Some(true)
        || is_optional_denorm_union_graphrel(gr)
        // #611: chained denorm — the Union sits DEEPER than a direct child;
        // see `subtree_contains_denormalized_union`. Both classification
        // sites (collect drop + join fold) go through this helper, so the
        // whole denorm family keeps its old placement consistently.
        || subtree_contains_denormalized_union(&gr.left)
        || subtree_contains_denormalized_union(&gr.right)
    {
        return Vec::new();
    }
    let Some(ref anchor_where) = gr.optional_anchor_where else {
        return Vec::new();
    };
    // Apply the same property-mapping rewrite `collect_graphrel_predicates`
    // applies to its predicate (#519 inline-map handling), so both sites
    // classify identical post-rewrite conjuncts.
    use crate::query_planner::logical_expr::expression_rewriter::{
        rewrite_expression_with_property_mapping, ExpressionRewriteContext,
    };
    let plan = LogicalPlan::GraphRel(gr.clone());
    let rewrite_ctx = ExpressionRewriteContext::new(&plan);
    let mapped = rewrite_expression_with_property_mapping(anchor_where, &rewrite_ctx);
    split_and_predicates_logical(&mapped)
        .into_iter()
        .filter(|p| {
            matches!(
                RenderExpr::try_from(p.clone()),
                Ok(RenderExpr::OperatorApplicationExp(_))
            )
        })
        .collect()
}

/// Collect all WHERE predicates from GraphRel nodes in the plan tree.
/// For optional patterns, filters out predicates that reference ONLY optional aliases
/// (those are moved to pre_filter for correct LEFT JOIN semantics).
/// For VLP patterns (variable_length is Some), predicates are already handled in the CTE,
/// so they are skipped here to avoid duplication in the outer query.
pub(super) fn collect_graphrel_predicates(plan: &LogicalPlan) -> Vec<RenderExpr> {
    // #611: gate conjuncts are dropped against the PLAN-WIDE set of tagged
    // optional-clause anchor gates, not just the current GraphRel's own tag.
    // FilterIntoGraphRel pools per-alias filters onto whichever GraphRel
    // FIRST references the alias — with multiple OPTIONAL clauses (or a
    // chained-optional entry) the conjunct's `where_predicate` copy can land
    // on a DIFFERENT GraphRel than the one carrying its tag, and the per-gr
    // check then failed to drop it, leaving a row-dropping duplicate in the
    // outer WHERE alongside the joined gate.
    let mut plan_wide_gates = Vec::new();
    collect_optional_anchor_gates(plan, &mut plan_wide_gates);
    collect_graphrel_predicates_inner(plan, &plan_wide_gates)
}

/// #611: gather `optional_anchor_gate_conjuncts` from every GraphRel in the
/// tree (same traversal as `collect_graphrel_predicates_inner`).
fn collect_optional_anchor_gates(plan: &LogicalPlan, out: &mut Vec<LogicalExpr>) {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            out.extend(optional_anchor_gate_conjuncts(gr));
            collect_optional_anchor_gates(&gr.left, out);
            collect_optional_anchor_gates(&gr.center, out);
            collect_optional_anchor_gates(&gr.right, out);
        }
        LogicalPlan::GraphNode(gn) => collect_optional_anchor_gates(&gn.input, out),
        LogicalPlan::Filter(f) => collect_optional_anchor_gates(&f.input, out),
        LogicalPlan::CartesianProduct(cp) => {
            collect_optional_anchor_gates(&cp.left, out);
            collect_optional_anchor_gates(&cp.right, out);
        }
        _ => {}
    }
}

fn collect_graphrel_predicates_inner(
    plan: &LogicalPlan,
    plan_wide_gates: &[LogicalExpr],
) -> Vec<RenderExpr> {
    let mut predicates = Vec::new();
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // 🔧 VLP FIX: Skip predicates from CTE-based VLP GraphRel nodes - they're already in the CTE.
            // Fixed-length VLPs (*N where N is exact) use inline chained JOINs instead of CTEs,
            // so their predicates MUST be included in the outer WHERE clause.
            if gr.variable_length.is_some() {
                let uses_cte = if let Some(ref spec) = gr.variable_length {
                    // #603: a DIRECTED OPTIONAL exact VLP is rerouted to the
                    // recursive CTE, so its predicates live INSIDE the CTE — they
                    // must NOT be re-collected into the outer WHERE (doing so
                    // emitted a spurious `t.end_id …` conjunct with the wrong
                    // fallback alias → Code 47). Mirror the flat-vs-CTE decision
                    // used everywhere else via the shared helper.
                    let is_fixed_length = spec.exact_hop_count().is_some()
                        && gr.shortest_path_mode.is_none()
                        && !crate::render_plan::from_builder::optional_directed_exact_vlp_uses_cte(
                            gr,
                        );
                    !is_fixed_length
                } else {
                    true
                };
                if uses_cte {
                    log::debug!(
                        "collect_graphrel_predicates: Skipping CTE-based VLP GraphRel '{}' predicates (already in CTE)",
                        gr.alias
                    );
                    // Still recurse into children to collect non-VLP predicates
                    predicates.extend(collect_graphrel_predicates_inner(&gr.left, plan_wide_gates));
                    predicates.extend(collect_graphrel_predicates_inner(
                        &gr.center,
                        plan_wide_gates,
                    ));
                    predicates.extend(collect_graphrel_predicates_inner(
                        &gr.right,
                        plan_wide_gates,
                    ));
                    return predicates;
                }
                // Fixed-length VLP: fall through to include predicates
            }

            // Add this GraphRel's predicate, but filter out optional-only predicates
            if let Some(ref pred) = gr.where_predicate {
                let is_optional = gr.is_optional.unwrap_or(false);

                // #553: an OPTIONAL MATCH whose anchor is a denormalized
                // "standalone scan" — the `__denorm_scan_{alias}` CTE built by
                // `materialize_standalone_denorm_scans` (type_inference.rs) —
                // ALREADY embeds every predicate that references ONLY the
                // anchor's own alias into that CTE, per-branch (its "Filter
                // over a materialized Union: push Filter INTO each branch"
                // handling). This is a SEPARATE mechanism from the generic
                // anchor/optional split below (which needs `gr.anchor_connection`
                // to be `Some`, and — deliberately, see `determine_optional_anchor`
                // in match_clause/helpers.rs — that stays `None` for the common
                // left-anchor/outgoing shape, falling into the "keep everything"
                // conservative branch a few lines down).
                //
                // For a NON-denormalized anchor, conservatively keeping an
                // anchor-only conjunct here is harmless: `apply_property_mapping`
                // maps it straight back onto the anchor's own (never-nullable)
                // table alias, so it's a no-op duplicate of what the plain
                // ViewScan-level filter already applies.
                //
                // For a denormalized anchor it's NOT harmless: the anchor has no
                // real physical table/columns of its own (its property mapping
                // is empty — every property is exposed only via the connected
                // edge's own role-specific node-property mapping), so resolving
                // the conjunct's column here falls through to the edge's
                // role-based mapping, re-targeting the predicate onto the
                // RELATIONSHIP alias (e.g. `a.code` -> `r.origin_code`). That
                // alias IS nullable (it's the LEFT-JOINed side of the OPTIONAL
                // MATCH), so evaluating a duplicate, already-satisfied anchor
                // constraint against it in the outer WHERE incorrectly drops the
                // NULL-extended row for every anchor with zero matches — the
                // exact OPTIONAL MATCH semantics violation this pass exists to
                // prevent. Drop those conjuncts here, before the rewrite, using
                // their PRE-rewrite alias (still the anchor's own, e.g. `a`).
                let pred_owned;
                let pred: &LogicalExpr =
                    if let Some(anchor_is_left) = optional_denorm_union_anchor_is_left(gr) {
                        let anchor_alias = if anchor_is_left {
                            &gr.left_connection
                        } else {
                            &gr.right_connection
                        };
                        // #533: ALSO drop a conjunct referencing ONLY the
                        // OPTIONAL node's own Cypher alias (e.g. `b`, NOT yet
                        // rewritten to the edge's alias at this point in the
                        // pipeline — only its COLUMN is role-mapped so far,
                        // e.g. `b.dest_city`) — this shape's OPTIONAL node has
                        // no table of its own; its properties are embedded IN
                        // the edge row. `plan_builder.rs`'s
                        // OPTIONAL-denorm-CTE-+-LEFT-JOIN render branch now
                        // folds this SAME conjunct (rewritten onto the edge's
                        // own alias) into the edge JOIN's `pre_filter`
                        // (mirroring #474's `apply_optional_node_pre_filters`
                        // for the shapes that mechanism DOES reach) —
                        // dropping it here too avoids double-embedding it
                        // (#565's exact failure mode) while a bare outer
                        // WHERE placement dropped NULL-extended anchor rows
                        // entirely (the #533 characterization this fixes).
                        let optional_alias = if anchor_is_left {
                            &gr.right_connection
                        } else {
                            &gr.left_connection
                        };
                        let remaining: Vec<LogicalExpr> = split_and_predicates_logical(pred)
                            .into_iter()
                            .filter(|p| {
                                !references_only_alias_logical(p, anchor_alias)
                                    && !references_only_alias_logical(p, optional_alias)
                            })
                            .collect();
                        match combine_predicates_with_and_logical(remaining) {
                            Some(combined) => {
                                pred_owned = combined;
                                &pred_owned
                            }
                            None => {
                                // Every conjunct was anchor-only and already covered
                                // by the CTE — nothing left to add for this GraphRel.
                                predicates.extend(collect_graphrel_predicates_inner(
                                    &gr.left,
                                    plan_wide_gates,
                                ));
                                predicates.extend(collect_graphrel_predicates_inner(
                                    &gr.center,
                                    plan_wide_gates,
                                ));
                                predicates.extend(collect_graphrel_predicates_inner(
                                    &gr.right,
                                    plan_wide_gates,
                                ));
                                return predicates;
                            }
                        }
                    } else {
                        pred
                    };

                // #519: a WHERE-clause predicate is already property-mapped by
                // the time it's folded into `gr.where_predicate` (an earlier
                // analyzer/optimizer stage rewrites it in place), but an
                // inline node property-map pattern (`(a:Airport {code:
                // 'JFK'})`) is NOT — `convert_properties`
                // (match_clause/helpers.rs) builds its equality expression
                // directly from the raw Cypher property key with no mapping
                // at all, and it lands in `gr.where_predicate` via the same
                // `filter_into_graph_rel` optimizer path as a WHERE clause.
                // Applying the SAME property-mapping rewrite the sibling
                // `LogicalPlan::Filter` branch below already does (15 lines
                // down) makes this uniform regardless of origin — a no-op
                // for an already-mapped WHERE-clause predicate (its physical
                // column name won't match any further Cypher property), and
                // the actual fix for the inline-map case (e.g. `code` ->
                // `origin_code` for a denormalized Airport in the origin
                // role, the exact same role-aware resolution a WHERE clause
                // on the same alias already gets).
                use crate::query_planner::logical_expr::expression_rewriter::{
                    rewrite_expression_with_property_mapping, ExpressionRewriteContext,
                };
                let rewrite_ctx = ExpressionRewriteContext::new(plan);
                let pred = rewrite_expression_with_property_mapping(pred, &rewrite_ctx);

                if is_optional {
                    // For OPTIONAL MATCH patterns, determine anchor vs optional
                    // aliases. #565: the OPTIONAL alias is now determined
                    // regardless of whether `anchor_connection` is `Some` or
                    // `None` — when `None`, it defaults to `left_connection`
                    // being the anchor (so `right_connection` is optional),
                    // the SAME default `apply_optional_node_pre_filters`
                    // (#474, join_builder.rs) and CLAUDE.md rule 4 already use.
                    let anchor_connection_is_set = gr.anchor_connection.is_some();
                    let anchor_is_left = gr
                        .anchor_connection
                        .as_deref()
                        .map(|a| a == gr.left_connection.as_str())
                        .unwrap_or(true);
                    let optional = if anchor_is_left {
                        &gr.right_connection
                    } else {
                        &gr.left_connection
                    };

                    // #554: only DROP an optional-node-only conjunct here when
                    // `apply_optional_node_pre_filters` (#474, join_builder.rs)
                    // will actually claim it — i.e. the optional node's own
                    // JOIN connects directly to the anchor (node-IS-edge
                    // shapes). For the traditional separate-edge shape, #474
                    // declines (by design) and the predicate must survive
                    // here so `fold_optional_edge_node_join_with_predicate`
                    // (#479/#552) can find and fold it — see
                    // `optional_node_shares_table_with_edge`'s doc comment.
                    //
                    // #565: previously, when `anchor_connection` was `None`
                    // (the common Outgoing/left-anchor default), this whole
                    // per-conjunct split was skipped entirely in favor of an
                    // unconditional "keep everything" fallback — safe for a
                    // REL-only conjunct (nothing else in the pipeline claims
                    // one for this shape; see
                    // `test_optional_match_filter_on_relationship`, which
                    // needs `r.since > '2020'` to survive here), but NOT safe
                    // for an OPTIONAL-NODE-only conjunct whenever #474
                    // independently ALSO claims it (an FK-edge anchor whose
                    // optional side is a genuinely separate table, e.g.
                    // `MATCH (o:Order) OPTIONAL MATCH (o)-[:PLACED_BY]->
                    // (c:Customer) WHERE c.name = 'Alice'`) — keeping it here
                    // TOO doubly embedded the predicate: once correctly as a
                    // pre_filter (preserving NULL-extension), once more in
                    // the outer WHERE, which then WRONGLY evaluated to NULL
                    // (not true) against every NULL-extended anchor row and
                    // silently dropped it — a live-verified OPTIONAL MATCH
                    // semantics violation, not merely redundant duplication.
                    // Fixed by applying the SAME optional-node-only
                    // recoverability drop regardless of `anchor_connection`,
                    // while gating the (pre-existing, unrelated) rel-only
                    // drop behind `anchor_connection_is_set` so it stays
                    // EXACTLY as before for the `None` shape.
                    let optional_only_is_recoverable = optional_node_shares_table_with_edge(gr);
                    // #597: anchor-only conjuncts of the OPTIONAL MATCH's own
                    // WHERE move into the gating LEFT JOIN ON (see
                    // `optional_anchor_gate_conjuncts`); emitting them in the
                    // outer WHERE dropped the NULL-extended anchor rows.
                    // `apply_optional_node_pre_filters` (join_builder.rs) adds
                    // them to the join via the SAME helper, so dropping them
                    // here can never lose a predicate.
                    // #611: consult the PLAN-WIDE gate set (see wrapper doc) —
                    // this GraphRel may carry another clause's pooled conjunct.
                    let all_preds = split_and_predicates_logical(&pred);
                    for p in all_preds {
                        let refs_only_rel = anchor_connection_is_set
                            && references_only_alias_logical(&p, &gr.alias);
                        let refs_only_optional = references_only_alias_logical(&p, optional)
                            && optional_only_is_recoverable;
                        let moved_to_join_on = plan_wide_gates.contains(&p);

                        // Keep if it references multiple aliases.
                        // Filter out if it references ONLY rel, ONLY the
                        // optional node, or (#597) is an OPTIONAL-clause
                        // anchor conjunct moved into the gating JOIN ON.
                        if !refs_only_rel && !refs_only_optional && !moved_to_join_on {
                            if let Ok(render_expr) = RenderExpr::try_from(p) {
                                predicates.push(render_expr);
                            }
                        }
                    }
                } else {
                    // Non-optional: include all predicates.
                    //
                    // #611 NOTE — deliberately NOT dropping plan-wide gate
                    // conjuncts here: when the base MATCH is itself a pattern,
                    // its GraphRel is the first to reference the shared anchor
                    // alias, and FilterIntoGraphRel's covered-aliases dedup
                    // leaves exactly ONE pooled copy of a conjunct that may
                    // serve BOTH a base-MATCH WHERE (mandatory, row-dropping)
                    // and an optional clause's gate. Dropping it here would
                    // silently lift the base constraint — a worse wrong than
                    // the redundant outer copy (which merely keeps the
                    // pattern-base-carrier variant of #611 at its old,
                    // main-identical behavior; tracked as a #611 follow-up).
                    if let Ok(render_expr) = RenderExpr::try_from(pred) {
                        predicates.push(render_expr);
                    }
                }
            }
            // Recursively collect from children
            predicates.extend(collect_graphrel_predicates_inner(&gr.left, plan_wide_gates));
            predicates.extend(collect_graphrel_predicates_inner(
                &gr.center,
                plan_wide_gates,
            ));
            predicates.extend(collect_graphrel_predicates_inner(
                &gr.right,
                plan_wide_gates,
            ));
        }
        LogicalPlan::GraphNode(gn) => {
            predicates.extend(collect_graphrel_predicates_inner(
                &gn.input,
                plan_wide_gates,
            ));
        }
        LogicalPlan::Filter(f) => {
            // 🔧 OPTIONAL MATCH FIX: Extract Filter predicates that wrap GraphNode
            // This handles queries like: MATCH (a) WHERE a.prop = X OPTIONAL MATCH (a)-[]->(b)
            // The Filter wraps the GraphNode for 'a' and needs to be included in the final WHERE
            log::debug!("collect_graphrel_predicates: Found Filter, extracting predicate");

            // Apply property mapping before converting to RenderExpr
            // This ensures properties are mapped to correct DB columns
            use crate::query_planner::logical_expr::expression_rewriter::{
                rewrite_expression_with_property_mapping, ExpressionRewriteContext,
            };
            let rewrite_ctx = ExpressionRewriteContext::new(&f.input);
            let rewritten_predicate =
                rewrite_expression_with_property_mapping(&f.predicate, &rewrite_ctx);

            if let Ok(render_expr) = RenderExpr::try_from(rewritten_predicate) {
                log::debug!(
                    "collect_graphrel_predicates: Adding Filter predicate to WHERE clause: {:?}",
                    render_expr
                );
                predicates.push(render_expr);
            }
            // Recurse into input to collect any other predicates
            predicates.extend(collect_graphrel_predicates_inner(&f.input, plan_wide_gates));
        }
        LogicalPlan::CartesianProduct(cp) => {
            predicates.extend(collect_graphrel_predicates_inner(&cp.left, plan_wide_gates));
            predicates.extend(collect_graphrel_predicates_inner(
                &cp.right,
                plan_wide_gates,
            ));
        }
        LogicalPlan::ViewScan(_scan) => {
            // ViewScan.view_filter should be empty after CleanupViewScanFilters optimizer
        }
        _ => {}
    }
    predicates
}

/// Collect schema filters from all ViewScans in the plan tree.
/// These are filters defined in the YAML schema configuration.
pub(super) fn collect_schema_filters(
    plan: &LogicalPlan,
    alias_hint: Option<&str>,
) -> Vec<RenderExpr> {
    let mut filters = Vec::new();
    match plan {
        LogicalPlan::ViewScan(scan) => {
            if let Some(ref schema_filter) = scan.schema_filter {
                let table_alias = alias_hint.unwrap_or(VLP_CTE_FROM_ALIAS);
                if let Ok(sql) = schema_filter.to_sql(table_alias) {
                    log::debug!(
                        "Collected schema filter for table '{}' with alias '{}': {}",
                        scan.source_table,
                        table_alias,
                        sql
                    );
                    filters.push(RenderExpr::Raw(sql));
                }
            }
        }
        LogicalPlan::GraphRel(gr) => {
            filters.extend(collect_schema_filters(&gr.left, Some(&gr.left_connection)));
            filters.extend(collect_schema_filters(&gr.center, Some(&gr.alias)));
            filters.extend(collect_schema_filters(
                &gr.right,
                Some(&gr.right_connection),
            ));
        }
        LogicalPlan::GraphNode(gn) => {
            filters.extend(collect_schema_filters(&gn.input, Some(&gn.alias)));
        }
        LogicalPlan::CartesianProduct(cp) => {
            filters.extend(collect_schema_filters(&cp.left, alias_hint));
            filters.extend(collect_schema_filters(&cp.right, alias_hint));
        }
        _ => {}
    }
    filters
}

/// Collect schema filters with alias and id_column info for NULL-safe wrapping.
/// Returns (filter_expr, alias, id_column) tuples so callers can wrap with OR IS NULL.
pub(super) fn collect_schema_filters_with_alias(
    plan: &LogicalPlan,
    alias_hint: Option<&str>,
) -> Vec<(RenderExpr, String, String)> {
    let mut filters = Vec::new();
    match plan {
        LogicalPlan::ViewScan(scan) => {
            if let Some(ref schema_filter) = scan.schema_filter {
                let table_alias = alias_hint.unwrap_or(VLP_CTE_FROM_ALIAS);
                if let Ok(sql) = schema_filter.to_sql(table_alias) {
                    filters.push((
                        RenderExpr::Raw(sql),
                        table_alias.to_string(),
                        scan.id_column.clone(),
                    ));
                }
            }
        }
        LogicalPlan::GraphRel(gr) => {
            filters.extend(collect_schema_filters_with_alias(
                &gr.left,
                Some(&gr.left_connection),
            ));
            filters.extend(collect_schema_filters_with_alias(
                &gr.center,
                Some(&gr.alias),
            ));
            filters.extend(collect_schema_filters_with_alias(
                &gr.right,
                Some(&gr.right_connection),
            ));
        }
        LogicalPlan::GraphNode(gn) => {
            filters.extend(collect_schema_filters_with_alias(
                &gn.input,
                Some(&gn.alias),
            ));
        }
        LogicalPlan::CartesianProduct(cp) => {
            filters.extend(collect_schema_filters_with_alias(&cp.left, alias_hint));
            filters.extend(collect_schema_filters_with_alias(&cp.right, alias_hint));
        }
        _ => {}
    }
    filters
}

/// Combine multiple RenderExpr filters with AND operator.
/// Returns None if empty, the single expr if one, or AND-combined if multiple.
pub(super) fn combine_render_exprs_with_and(filters: Vec<RenderExpr>) -> Option<RenderExpr> {
    match filters.len() {
        0 => None,
        1 => Some(filters.into_iter().next().unwrap()),
        _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        })),
    }
}

/// Combine multiple optional RenderExpr filters with AND operator.
/// Flattens the options first, then combines non-None values.
/// Returns None if all inputs are None.
pub(super) fn combine_optional_filters_with_and(
    filters: Vec<Option<RenderExpr>>,
) -> Option<RenderExpr> {
    let active: Vec<RenderExpr> = filters.into_iter().flatten().collect();
    combine_render_exprs_with_and(active)
}

/// Compare table aliases treating a trailing decimal suffix numerically
/// (`t9 < t10`), so topo-sort tie-breaks don't depend on the absolute values
/// handed out by the global alias counter (#626): plain string order flips
/// between `t10 < t9` and `t11 < t12` depending on how many aliases earlier
/// queries in the process allocated, making JOIN emission order flap run to
/// run. Numeric order always follows allocation order, which is plan order.
fn natural_alias_ord(a: &str, b: &str) -> std::cmp::Ordering {
    fn split(s: &str) -> (&str, Option<u64>) {
        let digit_count = s.bytes().rev().take_while(|b| b.is_ascii_digit()).count();
        let stem_len = s.len() - digit_count;
        match s[stem_len..].parse::<u64>() {
            Ok(n) if digit_count > 0 => (&s[..stem_len], Some(n)),
            _ => (s, None),
        }
    }
    let (a_stem, a_num) = split(a);
    let (b_stem, b_num) = split(b);
    a_stem
        .cmp(b_stem)
        .then(a_num.cmp(&b_num))
        .then_with(|| a.cmp(b))
}

/// Sort JOINs by dependency order to ensure referenced tables are defined before use.
///
/// For example, if JOIN A references table B in its ON clause, then B must appear
/// before A in the JOIN list. This is critical for OPTIONAL VLP queries where:
/// `LEFT JOIN vlp_cte AS vlp1 ON vlp1.start_id = message.id`
/// requires that `message` be defined in an earlier JOIN.
///
/// # Arguments
/// * `joins` - Vector of JOINs to sort
/// * `from_table` - Optional FROM table (already defined, can be referenced by JOINs)
///
/// # Returns
/// Sorted vector of JOINs in dependency order
pub fn sort_joins_by_dependency(
    mut joins: Vec<super::Join>,
    from_table: Option<&super::FromTable>,
) -> Vec<super::Join> {
    use std::collections::{HashMap, HashSet};

    log::debug!(
        "🔍 DEBUG sort_joins_by_dependency: Sorting {} JOINs by dependency",
        joins.len()
    );

    // Build a set of available aliases (FROM table + already processed JOINs)
    let mut available: HashSet<String> = HashSet::new();

    // Add FROM table alias if present
    if let Some(from) = from_table {
        if let Some(table_ref) = &from.table {
            if let Some(alias) = &table_ref.alias {
                available.insert(alias.clone());
                log::debug!("  DEBUG FROM alias: {}", alias);
            } else {
                // Use table name as implicit alias
                available.insert(table_ref.name.clone());
                log::debug!("  DEBUG FROM table (implicit alias): {}", table_ref.name);
            }
        }
    }

    // Build dependency map: JOIN -> set of aliases it references in ON clause
    let mut dependencies: HashMap<usize, HashSet<String>> = HashMap::new();

    for (idx, join) in joins.iter().enumerate() {
        let mut refs = HashSet::new();

        // Extract all aliases referenced in joining_on conditions
        for condition in &join.joining_on {
            extract_referenced_aliases_from_op(condition, &mut refs);
        }

        // Remove self-reference (the JOIN's own alias)
        refs.remove(&join.table_alias);

        log::debug!(
            "  DEBUG JOIN[{}] {} AS {} depends on: {:?}",
            idx,
            join.table_name,
            join.table_alias,
            refs
        );

        dependencies.insert(idx, refs);
    }

    // Topological sort: repeatedly find JOINs whose dependencies are all available
    let mut sorted = Vec::new();
    let mut remaining: Vec<usize> = (0..joins.len()).collect();
    let mut max_iterations = joins.len() * 2; // Prevent infinite loops

    log::debug!(
        "  DEBUG Starting topological sort with {} JOINs",
        remaining.len()
    );

    while !remaining.is_empty() && max_iterations > 0 {
        max_iterations -= 1;

        // Find ALL JOINs that can be added (all dependencies available)
        // Then pick deterministically by smallest alias name for stable ordering
        let ready_positions: Vec<usize> = remaining
            .iter()
            .enumerate()
            .filter(|(_, &idx)| {
                dependencies
                    .get(&idx)
                    .map(|deps| deps.iter().all(|dep| available.contains(dep)))
                    .unwrap_or(true)
            })
            .map(|(pos, _)| pos)
            .collect();

        // Among ready joins, pick the smallest table_alias (natural numeric
        // order, see natural_alias_ord) for determinism
        let best_pos = ready_positions.iter().copied().min_by(|&a, &b| {
            natural_alias_ord(
                &joins[remaining[a]].table_alias,
                &joins[remaining[b]].table_alias,
            )
        });

        if let Some(pos) = best_pos {
            let idx = remaining.remove(pos);

            // Add this JOIN's alias to available set
            available.insert(joins[idx].table_alias.clone());
            log::debug!(
                "  DEBUG Added JOIN[{}] {} AS {} to sorted list (available now: {:?})",
                idx,
                joins[idx].table_name,
                joins[idx].table_alias,
                available
            );

            sorted.push(idx);
        } else {
            // Circular dependency detected — pick the join with fewest unsatisfied
            // dependencies to break the cycle, then continue sorting
            log::warn!(
                "WARNING: Circular dependency detected - {} remaining JOINs",
                remaining.len()
            );
            log::debug!("  DEBUG Available aliases: {:?}", available);
            for &idx in &remaining {
                if let Some(deps) = dependencies.get(&idx) {
                    let missing: Vec<_> = deps.iter().filter(|d| !available.contains(*d)).collect();
                    log::debug!(
                        "    JOIN[{}] {} AS {} missing: {:?}",
                        idx,
                        joins[idx].table_name,
                        joins[idx].table_alias,
                        missing
                    );
                }
            }

            // Pick join with fewest missing deps; break ties by alias name
            let best = remaining
                .iter()
                .enumerate()
                .min_by(|(_, &a_idx), (_, &b_idx)| {
                    let a_missing = dependencies
                        .get(&a_idx)
                        .map(|d| d.iter().filter(|x| !available.contains(*x)).count())
                        .unwrap_or(0);
                    let b_missing = dependencies
                        .get(&b_idx)
                        .map(|d| d.iter().filter(|x| !available.contains(*x)).count())
                        .unwrap_or(0);
                    a_missing.cmp(&b_missing).then_with(|| {
                        natural_alias_ord(&joins[a_idx].table_alias, &joins[b_idx].table_alias)
                    })
                })
                .map(|(pos, _)| pos);

            if let Some(pos) = best {
                let idx = remaining.remove(pos);
                available.insert(joins[idx].table_alias.clone());
                log::debug!(
                    "  Breaking cycle: forced JOIN[{}] {} AS {}",
                    idx,
                    joins[idx].table_name,
                    joins[idx].table_alias
                );
                sorted.push(idx);
                // Continue loop — may resolve remaining deps now
            } else {
                break;
            }
        }
    }

    log::debug!(
        "  DEBUG Sorted order: {:?}",
        sorted
            .iter()
            .map(|&idx| format!("{} AS {}", joins[idx].table_name, joins[idx].table_alias))
            .collect::<Vec<_>>()
    );

    // Rebuild JOIN vector in sorted order
    let original_joins = joins.clone();
    joins.clear();
    for idx in sorted {
        joins.push(original_joins[idx].clone());
    }

    joins
}

/// Extract all table aliases referenced in an OperatorApplication's operands
fn extract_referenced_aliases_from_op(op: &OperatorApplication, refs: &mut HashSet<String>) {
    for operand in &op.operands {
        extract_referenced_aliases_from_expr(operand, refs);
    }
}

/// Extract all table aliases referenced in a RenderExpr
fn extract_referenced_aliases_from_expr(expr: &RenderExpr, refs: &mut HashSet<String>) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            refs.insert(prop.table_alias.0.clone());
        }
        RenderExpr::OperatorApplicationExp(op) => {
            extract_referenced_aliases_from_op(op, refs);
        }
        RenderExpr::ScalarFnCall(call) => {
            for arg in &call.args {
                extract_referenced_aliases_from_expr(arg, refs);
            }
        }
        RenderExpr::AggregateFnCall(call) => {
            for arg in &call.args {
                extract_referenced_aliases_from_expr(arg, refs);
            }
        }
        RenderExpr::TableAlias(alias) => {
            refs.insert(alias.0.clone());
        }
        // Literals, Star, CastExpr, etc. don't reference aliases
        _ => {}
    }
}

/// #590: Recursively rewrite property-access references on `edge_alias` to the
/// `(target_alias, cypher_prop)` the `col_map` maps their db column to. Shared
/// by the single-anchor OPTIONAL-denorm shim and the multi-anchor
/// CartesianProduct composition in `plan_builder.rs` so both apply the SAME
/// edge→anchor-CTE rewrite (formerly an inline nested fn in the single-anchor
/// shim).
pub(super) fn rewrite_denorm_refs(
    expr: &mut RenderExpr,
    edge_alias: &str,
    col_map: &DenormAnchorColMap,
) {
    match expr {
        RenderExpr::PropertyAccessExp(ref pa) if pa.table_alias.0 == edge_alias => {
            let db_col = pa.column.raw().to_string();
            if let Some((target_alias, cypher_prop)) = col_map.get(&db_col) {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(target_alias.clone()),
                    column: PropertyValue::Column(cypher_prop.clone()),
                });
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                rewrite_denorm_refs(arg, edge_alias, col_map);
            }
        }
        RenderExpr::ScalarFnCall(sf) => {
            for arg in &mut sf.args {
                rewrite_denorm_refs(arg, edge_alias, col_map);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                rewrite_denorm_refs(operand, edge_alias, col_map);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref mut e) = case.expr {
                rewrite_denorm_refs(e, edge_alias, col_map);
            }
            for (cond, result) in &mut case.when_then {
                rewrite_denorm_refs(cond, edge_alias, col_map);
                rewrite_denorm_refs(result, edge_alias, col_map);
            }
            if let Some(ref mut e) = case.else_expr {
                rewrite_denorm_refs(e, edge_alias, col_map);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                rewrite_denorm_refs(item, edge_alias, col_map);
            }
        }
        _ => {}
    }
}

/// #590: `db_column → (target_alias, cypher_property)` rewrite map produced by
/// [`build_denorm_anchor_col_map`] and consumed by [`rewrite_denorm_refs`].
pub(super) type DenormAnchorColMap = std::collections::HashMap<String, (String, String)>;

/// #590: Build the `edge_alias.db_col → (anchor_node_alias, cypher_prop)` rewrite
/// map for an OPTIONAL-denorm GraphRel `gr` (the map the SELECT/GROUP BY/ORDER BY
/// rewrite in the single-anchor shim applies). Returns `(edge_alias, col_map)`, or
/// `None` when `gr` isn't the special OPTIONAL-denorm-Union pattern (caller then
/// leaves references untouched).
///
/// Factored verbatim out of the single-anchor shim in `plan_builder.rs` so the
/// multi-anchor CartesianProduct composition can apply the identical rewrite
/// per-arm. `plan_ctx` supplies the pattern's edge-owned column set for the #475
/// guard (skip when absent, conservatively pre-#475).
pub(super) fn build_denorm_anchor_col_map(
    gr: &crate::query_planner::logical_plan::GraphRel,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> Option<(String, DenormAnchorColMap)> {
    // #506: the anchor Union can be on either side; default to the left/from-side
    // (outgoing) shape when unrecognized, matching prior behavior.
    let anchor_is_left = optional_denorm_union_anchor_is_left(gr).unwrap_or(true);
    let anchor_side_plan: &LogicalPlan = if anchor_is_left {
        gr.left.as_ref()
    } else {
        gr.right.as_ref()
    };

    let LogicalPlan::ViewScan(edge_vs) = gr.center.as_ref() else {
        return None;
    };
    let edge_alias = gr.alias.clone();
    let node_alias = if let LogicalPlan::Union(u) = anchor_side_plan {
        u.inputs.first().and_then(|i| {
            if let LogicalPlan::GraphNode(gn) = i.as_ref() {
                Some(gn.alias.clone())
            } else {
                None
            }
        })
    } else {
        None
    }?;

    // #508: resolve which edge side (from/to) the anchor actually occupies.
    let anchor_is_from_side =
        resolve_anchor_is_from_side(anchor_side_plan, edge_vs, anchor_is_left);

    let mut col_map: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();

    let anchor_side_props = crate::graph_catalog::pattern_schema::edge_side_node_properties(
        edge_vs,
        anchor_is_from_side,
    );
    if let Some(side_props) = anchor_side_props {
        // Sorted so the winner is deterministic when two properties map to the
        // same db column (HashMap iteration is per-process random, #480 class).
        let mut side_props: Vec<_> = side_props.iter().collect();
        side_props.sort_by(|a, b| a.0.cmp(b.0));
        for (prop, val) in side_props {
            col_map.insert(val.raw().to_string(), (node_alias.clone(), prop.clone()));
        }
    }

    // ALSO map the anchor's own from-side property columns (#475), guarded so an
    // edge-owned column name is never hijacked onto the anchor CTE. See the
    // single-anchor shim's own long comment for the full rationale.
    let edge_owned_columns = plan_ctx
        .and_then(|ctx| ctx.get_pattern_context(&gr.alias))
        .map(|pc| pc.edge_owned_columns());
    if let (LogicalPlan::Union(u), Some(edge_owned_columns)) =
        (anchor_side_plan, edge_owned_columns)
    {
        let anchor_gn = u.inputs.iter().find_map(|i| {
            if let LogicalPlan::GraphNode(gn) = i.as_ref() {
                if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                    if crate::graph_catalog::pattern_schema::edge_side_node_properties(
                        vs,
                        anchor_is_from_side,
                    )
                    .is_some()
                    {
                        return Some(gn);
                    }
                }
                None
            } else {
                None
            }
        });
        if let Some(gn) = anchor_gn {
            if let LogicalPlan::ViewScan(anchor_vs) = gn.input.as_ref() {
                let mut anchor_props: Vec<_> = anchor_vs.property_mapping.iter().collect();
                anchor_props.sort_by(|a, b| a.0.cmp(b.0));
                for (prop, val) in anchor_props {
                    let db_col = val.raw().to_string();
                    if edge_owned_columns.contains(&db_col) {
                        log::debug!(
                            "OPTIONAL denorm rewrite: skipping anchor property '{}' — its db column '{}' is edge-owned (#475 guard)",
                            prop,
                            db_col
                        );
                        continue;
                    }
                    col_map.insert(db_col, (node_alias.clone(), prop.clone()));
                }
            }
        }
    }

    Some((edge_alias, col_map))
}

/// #590: collect every OPTIONAL-denorm GraphRel in a rendered arm subtree (a
/// chained OPTIONAL produces more than one), so the multi-anchor composition can
/// apply each hop's edge→anchor-CTE rewrite. Order is outer-first.
pub(super) fn collect_optional_denorm_graphrels(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    let mut out = Vec::new();
    collect_optional_denorm_graphrels_into(plan, &mut out, 0);
    out
}

fn collect_optional_denorm_graphrels_into<'a>(
    plan: &'a LogicalPlan,
    out: &mut Vec<&'a LogicalPlan>,
    depth: usize,
) {
    if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
        return;
    }
    match plan {
        LogicalPlan::GraphRel(gr) => {
            if is_optional_denorm_union_graphrel(gr) {
                out.push(plan);
            }
            collect_optional_denorm_graphrels_into(&gr.left, out, depth + 1);
            collect_optional_denorm_graphrels_into(&gr.right, out, depth + 1);
        }
        LogicalPlan::GraphJoins(gj) => {
            collect_optional_denorm_graphrels_into(&gj.input, out, depth + 1)
        }
        LogicalPlan::Projection(p) => {
            collect_optional_denorm_graphrels_into(&p.input, out, depth + 1)
        }
        LogicalPlan::GroupBy(gb) => {
            collect_optional_denorm_graphrels_into(&gb.input, out, depth + 1)
        }
        LogicalPlan::Filter(f) => collect_optional_denorm_graphrels_into(&f.input, out, depth + 1),
        LogicalPlan::OrderBy(o) => collect_optional_denorm_graphrels_into(&o.input, out, depth + 1),
        LogicalPlan::Limit(l) => collect_optional_denorm_graphrels_into(&l.input, out, depth + 1),
        LogicalPlan::Skip(s) => collect_optional_denorm_graphrels_into(&s.input, out, depth + 1),
        LogicalPlan::CartesianProduct(cp) => {
            collect_optional_denorm_graphrels_into(&cp.left, out, depth + 1);
            collect_optional_denorm_graphrels_into(&cp.right, out, depth + 1);
        }
        _ => {}
    }
}

// `items_after_test_module` is allowed here: this file's test module sits
// in the middle and a couple of small helpers (`get_graph_rel_from_plan`,
// `build_format_row_json`) follow it. Reordering would shuffle ~360 lines
// for no behavioural gain.
#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{
        ColumnAlias, Literal, Operator, OperatorApplication, RenderExpr, TableAlias,
    };
    use crate::render_plan::{
        ArrayJoinItem, CteItems, FilterItems, FromTableItem, GroupByExpressions, JoinItems,
        LimitItem, OrderByItems, RenderPlan, SelectItem, SelectItems, SkipItem, UnionItems,
    };

    /// Regression: handling of the `_empty` placeholder a UNION branch renders when
    /// pruned to `LogicalPlan::Empty` (`SELECT 1 AS "_empty" WHERE false`, e.g. an
    /// unlabeled `MATCH (n)` whose property no node type has). Covers three cases:
    ///   1. placeholder + real branch → `_empty` dropped; both branches expose the
    ///      real RETURN columns (the placeholder adopts them as NULLs).
    ///   2. all branches pruned → returned unchanged (valid 0-row SQL, not a
    ///      column-less `SELECT WHERE false`).
    ///   3. a user column legitimately aliased `_empty` (NOT the placeholder shape)
    ///      is preserved — detection is shape-based, not name-based.
    #[test]
    fn normalize_union_branches_handles_empty_placeholder() {
        fn item(alias: &str) -> SelectItem {
            SelectItem {
                expression: RenderExpr::Literal(Literal::String(alias.to_string())),
                col_alias: Some(ColumnAlias(alias.to_string())),
            }
        }
        fn plan(items: Vec<SelectItem>, where_false: bool) -> RenderPlan {
            RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(vec![]),
                filters: FilterItems(if where_false {
                    Some(RenderExpr::Literal(Literal::Boolean(false)))
                } else {
                    None
                }),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
                fixed_path_info: None,
                is_multi_label_scan: false,
                variable_registry: None,
            }
        }
        // The exact placeholder a pruned branch renders: `SELECT 1 AS "_empty" WHERE false`.
        fn placeholder() -> RenderPlan {
            plan(
                vec![SelectItem {
                    expression: RenderExpr::Literal(Literal::Integer(1)),
                    col_alias: Some(ColumnAlias("_empty".to_string())),
                }],
                true,
            )
        }
        fn aliases_of(b: &RenderPlan) -> std::collections::BTreeSet<String> {
            b.select
                .items
                .iter()
                .filter_map(|i| i.col_alias.as_ref().map(|a| a.0.clone()))
                .collect()
        }
        let real_cols: std::collections::BTreeSet<String> = ["entity", "joined_at"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        // (1) placeholder + real branch
        let out = normalize_union_branches(vec![
            placeholder(),
            plan(vec![item("entity"), item("joined_at")], false),
        ]);
        for b in &out {
            assert!(
                !aliases_of(b).contains("_empty"),
                "placeholder _empty must not survive: {:?}",
                aliases_of(b)
            );
            assert_eq!(
                aliases_of(b),
                real_cols,
                "every branch must expose the declared RETURN columns"
            );
        }

        // (2) all branches pruned → unchanged (valid 0-row SQL)
        let out = normalize_union_branches(vec![placeholder(), placeholder()]);
        assert_eq!(out.len(), 2);
        let only_empty: std::collections::BTreeSet<String> =
            ["_empty".to_string()].into_iter().collect();
        for b in &out {
            assert_eq!(aliases_of(b), only_empty);
        }

        // (3) user column legitimately aliased `_empty` (string expr, no WHERE false)
        // is NOT a placeholder and must be preserved.
        let out = normalize_union_branches(vec![
            plan(vec![item("_empty"), item("x")], false),
            plan(vec![item("_empty")], false),
        ]);
        for b in &out {
            assert!(
                aliases_of(b).contains("_empty"),
                "a real user column aliased _empty must be preserved: {:?}",
                aliases_of(b)
            );
        }
    }

    /// Regression: an undirected/bidirectional relationship expands into TWO
    /// direction branches that render into a SINGLE RenderPlan — the primary
    /// SELECT/FROM is direction A and the nested `union.input` holds direction B.
    /// When `normalize_union_branches` coerces columns (e.g. a sibling placeholder
    /// branch forces `all_same=false`), BOTH directions must receive the IDENTICAL
    /// type coercion. Previously only the primary SELECT got wrapped in
    /// `toString(...)` while the nested direction kept the raw column, so a UNION
    /// over a non-String property (e.g. a Date) failed in ClickHouse with
    /// Code 386 NO_COMMON_TYPE. This asserts no mixed coercion across the
    /// nested sibling branch.
    #[test]
    fn normalize_union_branches_coerces_nested_bidirectional_branches_consistently() {
        fn item(alias: &str) -> SelectItem {
            // A raw column-style reference (NOT a string literal) — the kind that
            // would surface a String-vs-Date mismatch if coerced inconsistently.
            SelectItem {
                expression: RenderExpr::TableAlias(TableAlias(format!("r.{alias}"))),
                col_alias: Some(ColumnAlias(alias.to_string())),
            }
        }
        fn base(items: Vec<SelectItem>, union: UnionItems, where_false: bool) -> RenderPlan {
            RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items,
                    distinct: true,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(vec![]),
                filters: FilterItems(if where_false {
                    Some(RenderExpr::Literal(Literal::Boolean(false)))
                } else {
                    None
                }),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union,
                fixed_path_info: None,
                is_multi_label_scan: false,
                variable_registry: None,
            }
        }
        // Pruned node branch → exact placeholder shape; forces normalization.
        let placeholder = base(
            vec![SelectItem {
                expression: RenderExpr::Literal(Literal::Integer(1)),
                col_alias: Some(ColumnAlias("_empty".to_string())),
            }],
            UnionItems(None),
            true,
        );
        // Direction B (nested) — same RETURN columns as direction A.
        let direction_b = base(
            vec![item("entity"), item("since_date")],
            UnionItems(None),
            false,
        );
        // Direction A (primary) carrying direction B in its `union` field, exactly
        // as a bidirectional expansion renders.
        let bidirectional = base(
            vec![item("entity"), item("since_date")],
            UnionItems(Some(crate::render_plan::Union {
                input: vec![direction_b],
                union_type: crate::render_plan::UnionType::All,
                is_cypher_union: false,
            })),
            false,
        );

        let out = normalize_union_branches(vec![placeholder, bidirectional]);

        // Locate the real (non-placeholder) branch.
        let real = out
            .iter()
            .find(|b| b.union.0.is_some())
            .expect("bidirectional branch with nested union must survive");

        let all_cast = |items: &[SelectItem]| {
            items
                .iter()
                .all(|i| matches!(i.expression, RenderExpr::ScalarFnCall(_)))
        };

        // Primary direction columns are coerced...
        assert!(
            all_cast(&real.select.items),
            "primary direction columns must all be cast-wrapped: {:?}",
            real.select.items
        );
        // ...and the nested sibling direction must be coerced IDENTICALLY (no mix).
        let nested = real.union.0.as_ref().unwrap();
        assert_eq!(nested.input.len(), 1);
        assert!(
            all_cast(&nested.input[0].select.items),
            "nested bidirectional direction columns must be cast-wrapped consistently: {:?}",
            nested.input[0].select.items
        );
    }

    /// Test for TODO-8: rewrite_with_aliases_to_cte should rewrite TableAlias references
    /// that are in the with_aliases set to CTE references.
    #[test]
    fn test_rewrite_with_aliases_to_cte_basic() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // A simple TableAlias reference should be rewritten
        let expr = RenderExpr::TableAlias(TableAlias("follows".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(
            from_with,
            "Expression should be recognized as coming from WITH"
        );

        // Should be rewritten to grouped_data.follows
        match rewritten {
            RenderExpr::PropertyAccessExp(prop) => {
                assert_eq!(prop.table_alias.0, "grouped_data");
                assert_eq!(prop.column.raw(), "follows");
            }
            _ => panic!("Expected PropertyAccessExp, got {:?}", rewritten),
        }
    }

    /// Test that non-WITH aliases are NOT rewritten
    #[test]
    fn test_rewrite_with_aliases_to_cte_non_with_alias() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // A TableAlias NOT in with_aliases should NOT be rewritten
        let expr = RenderExpr::TableAlias(TableAlias("other_alias".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(!from_with, "Expression should NOT be from WITH");

        // Should remain unchanged
        match rewritten {
            RenderExpr::TableAlias(alias) => {
                assert_eq!(alias.0, "other_alias");
            }
            _ => panic!("Expected unchanged TableAlias, got {:?}", rewritten),
        }
    }

    /// Test that rewrite_with_aliases_to_cte handles aggregates with WITH aliases
    #[test]
    fn test_rewrite_with_aliases_to_cte_aggregate() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("follows".to_string());

        // AVG(follows) should become AVG(grouped_data.follows)
        let expr = RenderExpr::AggregateFnCall(AggregateFnCall {
            name: "AVG".to_string(),
            args: vec![RenderExpr::TableAlias(TableAlias("follows".to_string()))],
        });

        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(from_with, "Aggregate argument should be from WITH");

        // Should be AVG(grouped_data.follows)
        match rewritten {
            RenderExpr::AggregateFnCall(agg) => {
                assert_eq!(agg.name, "AVG");
                assert_eq!(agg.args.len(), 1);
                match &agg.args[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "grouped_data");
                        assert_eq!(prop.column.raw(), "follows");
                    }
                    _ => panic!(
                        "Expected PropertyAccessExp inside aggregate, got {:?}",
                        agg.args[0]
                    ),
                }
            }
            _ => panic!("Expected AggregateFnCall, got {:?}", rewritten),
        }
    }

    /// Test that nested expressions are rewritten correctly
    #[test]
    fn test_rewrite_with_aliases_to_cte_nested_operator() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("a".to_string());
        with_aliases.insert("b".to_string());

        // a + b should become cte.a + cte.b
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::TableAlias(TableAlias("a".to_string())),
                RenderExpr::TableAlias(TableAlias("b".to_string())),
            ],
        });

        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        assert!(from_with, "Both operands are from WITH");

        match rewritten {
            RenderExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operands.len(), 2);
                // Check first operand
                match &op.operands[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "a");
                    }
                    _ => panic!("Expected PropertyAccessExp for first operand"),
                }
                // Check second operand
                match &op.operands[1] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "b");
                    }
                    _ => panic!("Expected PropertyAccessExp for second operand"),
                }
            }
            _ => panic!("Expected OperatorApplicationExp, got {:?}", rewritten),
        }
    }

    /// Test that mixed expressions (WITH alias + non-WITH alias) are handled correctly
    #[test]
    fn test_rewrite_with_aliases_to_cte_mixed() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("from_with".to_string());

        // from_with + not_from_with should partially rewrite
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::TableAlias(TableAlias("from_with".to_string())),
                RenderExpr::TableAlias(TableAlias("not_from_with".to_string())),
            ],
        });

        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        // from_with should be false because not all operands are from WITH
        assert!(!from_with, "Mixed expression should not be fully from WITH");

        match rewritten {
            RenderExpr::OperatorApplicationExp(op) => {
                // First operand should be rewritten
                match &op.operands[0] {
                    RenderExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "cte");
                        assert_eq!(prop.column.raw(), "from_with");
                    }
                    _ => panic!("Expected first operand to be rewritten to PropertyAccessExp"),
                }
                // Second operand should NOT be rewritten
                match &op.operands[1] {
                    RenderExpr::TableAlias(alias) => {
                        assert_eq!(alias.0, "not_from_with");
                    }
                    _ => panic!("Expected second operand to remain as TableAlias"),
                }
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }

    /// Test that ColumnAlias references are also rewritten (not just TableAlias)
    #[test]
    fn test_rewrite_with_aliases_to_cte_column_alias() {
        let mut with_aliases = HashSet::new();
        with_aliases.insert("my_alias".to_string());

        let expr = RenderExpr::ColumnAlias(ColumnAlias("my_alias".to_string()));
        let (rewritten, from_with) =
            rewrite_with_aliases_to_cte(expr, &with_aliases, "grouped_data");

        assert!(
            from_with,
            "ColumnAlias should also be recognized as from WITH"
        );

        match rewritten {
            RenderExpr::PropertyAccessExp(prop) => {
                assert_eq!(prop.table_alias.0, "grouped_data");
                assert_eq!(prop.column.raw(), "my_alias");
            }
            _ => panic!("Expected PropertyAccessExp, got {:?}", rewritten),
        }
    }

    /// Test that literals are not rewritten and don't claim to be from WITH
    #[test]
    fn test_rewrite_with_aliases_to_cte_literal() {
        let with_aliases = HashSet::new();

        let expr = RenderExpr::Literal(Literal::Integer(42));
        let (rewritten, from_with) = rewrite_with_aliases_to_cte(expr, &with_aliases, "cte");

        assert!(!from_with, "Literal should not be from WITH");

        match rewritten {
            RenderExpr::Literal(Literal::Integer(n)) => assert_eq!(n, 42),
            _ => panic!("Expected unchanged Literal"),
        }
    }

    // ==========================================================================
    // Tests for predicate analysis helpers
    // ==========================================================================

    use crate::graph_catalog::expression_parser::PropertyValue as LogicalPropertyValue;
    use crate::query_planner::logical_expr::{
        Literal as LogicalLiteral, LogicalExpr, Operator as LogicalOperator,
        OperatorApplication as LogicalOpApp, PropertyAccess as LogicalPropertyAccess,
        TableAlias as LogicalTableAlias,
    };

    /// Test collect_aliases_from_logical_expr with simple property access
    #[test]
    fn test_collect_aliases_from_logical_expr_property() {
        let expr = LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
            table_alias: LogicalTableAlias("user".to_string()),
            column: LogicalPropertyValue::Column("name".to_string()),
        });

        let mut aliases = HashSet::new();
        collect_aliases_from_logical_expr(&expr, &mut aliases);

        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains("user"));
    }

    /// Test collect_aliases_from_logical_expr with operator containing multiple aliases
    #[test]
    fn test_collect_aliases_from_logical_expr_operator() {
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("a".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("b".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
            ],
        });

        let mut aliases = HashSet::new();
        collect_aliases_from_logical_expr(&expr, &mut aliases);

        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains("a"));
        assert!(aliases.contains("b"));
    }

    /// Test references_only_alias_logical - returns true when only one alias
    #[test]
    fn test_references_only_alias_logical_single() {
        let expr = LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
            table_alias: LogicalTableAlias("user".to_string()),
            column: LogicalPropertyValue::Column("name".to_string()),
        });

        assert!(references_only_alias_logical(&expr, "user"));
        assert!(!references_only_alias_logical(&expr, "other"));
    }

    /// Test references_only_alias_logical - returns false when multiple aliases
    #[test]
    fn test_references_only_alias_logical_multiple() {
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("a".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
                LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                    table_alias: LogicalTableAlias("b".to_string()),
                    column: LogicalPropertyValue::Column("id".to_string()),
                }),
            ],
        });

        assert!(!references_only_alias_logical(&expr, "a"));
        assert!(!references_only_alias_logical(&expr, "b"));
    }

    /// Test split_and_predicates_logical
    #[test]
    fn test_split_and_predicates_logical() {
        // Create: a.x = 1 AND b.y = 2
        let expr = LogicalExpr::OperatorApplicationExp(LogicalOpApp {
            operator: LogicalOperator::And,
            operands: vec![
                LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: LogicalOperator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                            table_alias: LogicalTableAlias("a".to_string()),
                            column: LogicalPropertyValue::Column("x".to_string()),
                        }),
                        LogicalExpr::Literal(LogicalLiteral::Integer(1)),
                    ],
                }),
                LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: LogicalOperator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(LogicalPropertyAccess {
                            table_alias: LogicalTableAlias("b".to_string()),
                            column: LogicalPropertyValue::Column("y".to_string()),
                        }),
                        LogicalExpr::Literal(LogicalLiteral::Integer(2)),
                    ],
                }),
            ],
        });

        let predicates = split_and_predicates_logical(&expr);
        assert_eq!(predicates.len(), 2);
    }

    /// Test combine_predicates_with_and_logical
    #[test]
    fn test_combine_predicates_with_and_logical() {
        // Empty list
        assert!(combine_predicates_with_and_logical(vec![]).is_none());

        // Single predicate
        let single = LogicalExpr::Literal(LogicalLiteral::Boolean(true));
        let combined = combine_predicates_with_and_logical(vec![single.clone()]);
        assert_eq!(combined, Some(single));

        // Multiple predicates
        let p1 = LogicalExpr::Literal(LogicalLiteral::Boolean(true));
        let p2 = LogicalExpr::Literal(LogicalLiteral::Boolean(false));
        let combined = combine_predicates_with_and_logical(vec![p1.clone(), p2.clone()]);

        match combined {
            Some(LogicalExpr::OperatorApplicationExp(op)) => {
                assert!(matches!(op.operator, LogicalOperator::And));
                assert_eq!(op.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplicationExp with AND"),
        }
    }

    /// #626: topo-sort tie-breaks must use NATURAL alias order (t9 < t10), not
    /// string order (t10 < t9). Generated `t{N}` aliases carry the value of a
    /// process-global counter, so string order made JOIN emission order depend
    /// on how many aliases earlier queries in the process had allocated —
    /// `partial_ref_undirected_2hop`'s Incoming-swapped branches (two edge
    /// joins both ready off the FROM node) flapped between orderings run to
    /// run. This fixes the exact shape: FROM b, with t9/t10 both depending
    /// only on b, and a/c depending on t9/t10 respectively.
    #[test]
    fn sort_joins_tie_break_is_natural_numeric_order() {
        fn eq_cond(l_alias: &str, l_col: &str, r_alias: &str, r_col: &str) -> OperatorApplication {
            use crate::graph_catalog::expression_parser::PropertyValue;
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(
                        crate::render_plan::render_expr::PropertyAccess {
                            table_alias: TableAlias(l_alias.to_string()),
                            column: PropertyValue::Column(l_col.to_string()),
                        },
                    ),
                    RenderExpr::PropertyAccessExp(
                        crate::render_plan::render_expr::PropertyAccess {
                            table_alias: TableAlias(r_alias.to_string()),
                            column: PropertyValue::Column(r_col.to_string()),
                        },
                    ),
                ],
            }
        }
        fn join(alias: &str, cond: OperatorApplication) -> crate::render_plan::Join {
            crate::render_plan::Join {
                table_name: "social.t".to_string(),
                table_alias: alias.to_string(),
                joining_on: vec![cond],
                join_type: crate::render_plan::JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
                is_cartesian: false,
            }
        }
        let from = crate::render_plan::FromTable {
            table: Some(crate::render_plan::ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: "social.users".to_string(),
                alias: Some("b".to_string()),
                use_final: false,
            }),
            joins: vec![],
        };
        // Input deliberately lists t10 before t9: both are immediately ready
        // (they only reference FROM alias b), so only the tie-break decides.
        let joins = vec![
            join("t10", eq_cond("t10", "follower_id", "b", "user_id")),
            join("t9", eq_cond("t9", "follower_id", "b", "user_id")),
            join("a", eq_cond("a", "user_id", "t9", "followed_id")),
            join("c", eq_cond("c", "user_id", "t10", "followed_id")),
        ];
        let sorted = sort_joins_by_dependency(joins, Some(&from));
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        // t9 wins the initial tie against t10 (natural order — string order
        // would pick t10 first). Then a becomes ready and wins on stem ("a" <
        // "t"), then t10, then c. Normalized this is the golden's
        // t0, a, t1, c branch shape.
        assert_eq!(order, vec!["t9", "a", "t10", "c"]);
    }

    #[test]
    fn natural_alias_ord_orders_numeric_suffixes() {
        use std::cmp::Ordering;
        assert_eq!(natural_alias_ord("t9", "t10"), Ordering::Less);
        assert_eq!(natural_alias_ord("t10", "t9"), Ordering::Greater);
        assert_eq!(natural_alias_ord("t2", "t2"), Ordering::Equal);
        // Different stems fall back to string order on the stem
        assert_eq!(natural_alias_ord("a", "t9"), Ordering::Less);
        // Non-numeric aliases: plain string order
        assert_eq!(natural_alias_ord("alpha", "beta"), Ordering::Less);
        // Numberless vs numbered same stem: bare stem sorts first
        assert_eq!(natural_alias_ord("t", "t1"), Ordering::Less);
    }
}

/// Recursively find GraphRel in a logical plan tree
/// Used to detect multi-type VLP patterns for correct table alias resolution
pub(super) fn get_graph_rel_from_plan(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::GraphRel> {
    use crate::query_planner::logical_plan::LogicalPlan;

    match plan {
        LogicalPlan::GraphRel(rel) => Some(rel),
        LogicalPlan::Filter(filter) => get_graph_rel_from_plan(&filter.input),
        LogicalPlan::Projection(proj) => get_graph_rel_from_plan(&proj.input),
        LogicalPlan::OrderBy(order) => get_graph_rel_from_plan(&order.input),
        LogicalPlan::Limit(limit) => get_graph_rel_from_plan(&limit.input),
        LogicalPlan::Skip(skip) => get_graph_rel_from_plan(&skip.input),
        LogicalPlan::GroupBy(group) => get_graph_rel_from_plan(&group.input),
        LogicalPlan::WithClause(with_clause) => get_graph_rel_from_plan(&with_clause.input),
        LogicalPlan::GraphJoins(joins) => get_graph_rel_from_plan(&joins.input),
        _ => None,
    }
}

/// Convert path UNION branches to JSON format for consistent schema
///
/// For path queries like `MATCH p=()-->() RETURN p`, each branch may have different
/// node/relationship types with different property counts. Convert to fixed schema:
/// - `p`: path tuple (unchanged)
/// - `_start_properties`: JSON with start node properties
/// - `_end_properties`: JSON with end node properties
/// - `_rel_properties`: JSON with relationship properties
pub(super) fn convert_path_branches_to_json(
    union_plans: Vec<super::RenderPlan>,
    logical_plans: Option<&[std::sync::Arc<crate::query_planner::logical_plan::LogicalPlan>]>,
) -> Vec<super::RenderPlan> {
    use super::render_expr::{Literal, RenderExpr};
    use super::{ColumnAlias, RenderPlan, SelectItem, SelectItems};

    log::debug!(
        "🔧 convert_path_branches_to_json: Processing {} branches",
        union_plans.len()
    );

    union_plans
        .into_iter()
        .enumerate()
        .map(|(branch_idx, plan)| {
            // Extract relationship type from logical plan (explicit, no guessing)
            let rel_type: Option<String> = logical_plans.and_then(|lp| {
                lp.get(branch_idx).and_then(|lp| {
                    super::cte_extraction::extract_relationship_type_from_plan(lp.as_ref())
                })
            });

            // Extract start/end node labels from logical plan (explicit, no guessing)
            let mut node_labels: Option<(String, String)> = logical_plans.and_then(|lp| {
                lp.get(branch_idx).and_then(|lp| {
                    super::cte_extraction::extract_path_node_labels_from_plan(lp.as_ref())
                })
            });

            // Fallback: when label extraction fails (e.g., CTE-backed nodes), derive from relationship schema.
            // Uses the active query's schema to avoid cross-schema ambiguity in multi-schema mode.
            if node_labels.is_none() {
                if let Some(ref rt) = rel_type {
                    if let Some(schema) =
                        crate::server::query_context::get_current_schema_with_fallback()
                    {
                        if let Ok(rs) = schema.get_rel_schema(rt) {
                            node_labels = Some((rs.from_node.clone(), rs.to_node.clone()));
                            log::info!(
                                "  Branch {}: derived labels from rel schema: from='{}', to='{}'",
                                branch_idx, rs.from_node, rs.to_node
                            );
                        }
                    }
                }
            }

            if let Some(ref rt) = rel_type {
                log::debug!("  Branch {}: extracted relationship type = '{}'", branch_idx, rt);
            }
            if let Some((ref sl, ref el)) = node_labels {
                log::debug!(
                    "  Branch {}: extracted node labels = start='{}', end='{}'",
                    branch_idx, sl, el
                );
            }
            // First, find the path tuple and extract aliases from it
            let mut path_item = None;
            let mut start_alias = String::new();
            let mut end_alias = String::new();
            let mut rel_alias = String::new();

            // Find path tuple and extract aliases
            for item in &plan.select.items {
                if matches!(&item.expression, RenderExpr::ScalarFnCall(fn_call) if fn_call.name == "tuple") {
                    if let RenderExpr::ScalarFnCall(fn_call) = &item.expression {
                        // tuple('fixed_path', start_alias, end_alias, rel_alias)
                        // Arguments are: [Literal("fixed_path"), Literal(start), Literal(end), Literal(rel)]
                        if fn_call.args.len() >= 4 {
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[1] {
                                start_alias = s.clone();
                            }
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[2] {
                                end_alias = s.clone();
                            }
                            if let RenderExpr::Literal(Literal::String(s)) = &fn_call.args[3] {
                                rel_alias = s.clone();
                            }
                        }
                    }
                }
            }

            log::debug!("  Branch {}: start='{}', end='{}', rel='{}'",
                      branch_idx, start_alias, end_alias, rel_alias);

            let mut start_items = Vec::new();
            let mut end_items = Vec::new();
            let mut rel_items = Vec::new();
            let mut other_items = Vec::new();

            // Now group items by their table alias prefix
            for item in plan.select.items {
                if let Some(alias) = &item.col_alias {
                    let alias_str = &alias.0;

                    // Path tuple: ScalarFnCall to tuple() function
                    if matches!(&item.expression, RenderExpr::ScalarFnCall(fn_call) if fn_call.name == "tuple") {
                        path_item = Some(item);
                    }
                    // Check if alias starts with start node table alias
                    else if !start_alias.is_empty() && alias_str.starts_with(&format!("{}.", start_alias)) {
                        start_items.push(item);
                    }
                    // Check if alias starts with end node table alias
                    else if !end_alias.is_empty() && alias_str.starts_with(&format!("{}.", end_alias)) {
                        end_items.push(item);
                    }
                    // Check if alias starts with relationship table alias
                    else if !rel_alias.is_empty() && alias_str.starts_with(&format!("{}.", rel_alias)) {
                        rel_items.push(item);
                    }
                    // Preserve non-path items (scalars, aggregations, etc.)
                    else {
                        other_items.push(item);
                    }
                } else {
                    other_items.push(item);
                }
            }

            log::debug!("  Branch {}: found {} start, {} end, {} rel, {} other items",
                      branch_idx, start_items.len(), end_items.len(), rel_items.len(), other_items.len());

            // Check if this is a denormalized schema (only one table in FROM clause)
            // For denormalized schemas, virtual node aliases don't exist as tables
            // so we need to use the relationship table alias for all column references
            let denorm_table_alias = if let super::FromTableItem(Some(ref view_ref)) = plan.from {
                // Check if the FROM table matches the relationship alias
                // If we only have the relationship table, this is denormalized
                if view_ref.alias.as_ref() == Some(&rel_alias) ||
                   view_ref.name.ends_with(&rel_alias) {
                    // For denormalized, use the actual table alias from FROM
                    view_ref.alias.as_deref()
                } else {
                    None
                }
            } else {
                None
            };

            if denorm_table_alias.is_some() {
                log::debug!("  Branch {}: denormalized schema detected, using table alias '{:?}'",
                          branch_idx, denorm_table_alias);
            }

            let mut new_items = Vec::new();

            // Collect aliases already present in other_items (e.g., from CTE references)
            // to avoid adding duplicate columns
            let existing_aliases: std::collections::HashSet<String> = other_items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                .collect();

            // 1. Keep path tuple as-is
            if let Some(p) = path_item {
                new_items.push(p);
            }

            // 2. Convert start node properties to JSON (prefix: _s_)
            if !start_items.is_empty() && !existing_aliases.contains("_start_properties") {
                let json_expr = build_format_row_json(&start_items, "_s_", denorm_table_alias, &start_alias);
                new_items.push(SelectItem {
                    expression: json_expr,
                    col_alias: Some(ColumnAlias("_start_properties".to_string())),
                });
            }

            // 3. Convert end node properties to JSON (prefix: _e_)
            if !end_items.is_empty() && !existing_aliases.contains("_end_properties") {
                let json_expr = build_format_row_json(&end_items, "_e_", denorm_table_alias, &end_alias);
                new_items.push(SelectItem {
                    expression: json_expr,
                    col_alias: Some(ColumnAlias("_end_properties".to_string())),
                });
            }

            // 4. Convert relationship properties to JSON (prefix: _r_) or empty object if none
            if !existing_aliases.contains("_rel_properties") {
                if !rel_items.is_empty() {
                    let json_expr = build_format_row_json(&rel_items, "_r_", denorm_table_alias, &rel_alias);
                    new_items.push(SelectItem {
                        expression: json_expr,
                        col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                    });
                } else {
                    // No relationship properties (denormalized) - empty JSON object
                    new_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String("{}".to_string())),
                        col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                    });
                }
            }

            // 5. Add explicit relationship type column (no guessing!)
            if let Some(ref rt) = rel_type {
                if !existing_aliases.contains("__rel_type__") {
                    new_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String(rt.clone())),
                        col_alias: Some(ColumnAlias("__rel_type__".to_string())),
                    });
                }
            }

            // 6. Add explicit start/end node label columns (no guessing!)
            if let Some((ref start_label, ref end_label)) = node_labels {
                if !existing_aliases.contains("__start_label__") {
                    new_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String(start_label.clone())),
                        col_alias: Some(ColumnAlias("__start_label__".to_string())),
                    });
                }
                if !existing_aliases.contains("__end_label__") {
                    new_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String(end_label.clone())),
                        col_alias: Some(ColumnAlias("__end_label__".to_string())),
                    });
                }
            }

            // 7. Preserve non-path items (scalars, aggregations, CTE columns)
            // Rewrite CTE alias references that may not exist as JOINs in all branches
            let available_aliases: std::collections::HashSet<String> = {
                let mut aliases = std::collections::HashSet::new();
                if let super::FromTableItem(Some(ref vr)) = plan.from {
                    if let Some(ref a) = vr.alias {
                        aliases.insert(a.clone());
                    }
                }
                for j in &plan.joins.0 {
                    aliases.insert(j.table_alias.clone());
                }
                aliases
            };
            for item in &mut other_items {
                if let RenderExpr::PropertyAccessExp(ref mut pa) = item.expression {
                    let ta = &pa.table_alias.0;
                    if !available_aliases.contains(ta) {
                        // table alias not available — find a JOIN to the same CTE
                        for j in &plan.joins.0 {
                            if ta.starts_with(&j.table_alias) || j.table_alias.starts_with(ta) {
                                log::info!(
                                    "  Rewriting other_item: {}.{} → {}.{}",
                                    ta, pa.column.raw(), j.table_alias, pa.column.raw()
                                );
                                pa.table_alias = super::render_expr::TableAlias(j.table_alias.clone());
                                break;
                            }
                        }
                    }
                }
            }
            new_items.extend(other_items);

            RenderPlan {
                select: SelectItems {
                    items: new_items,
                    distinct: plan.select.distinct,
                },
                ..plan
            }
        })
        .collect()
}

/// Helper to build JSON object from select items using formatRowNoNewline('JSONEachRow', ...)
/// Uses column aliases (AS prefix+clean_name) so JSON keys have unique prefixes
/// to avoid ClickHouse alias collision when same property names appear in both nodes.
/// The prefix (_s_, _e_, _r_) is stripped in the Bolt transformer.
///
/// # Arguments
/// * `items` - Select items to convert to JSON
/// * `prefix` - Prefix for property names (_s_, _e_, _r_)
/// * `table_alias_override` - Optional table alias to use instead of item's table_alias
///   (used for denormalized schemas where virtual node aliases don't exist as tables)
fn build_format_row_json(
    items: &[super::SelectItem],
    prefix: &str,
    table_alias_override: Option<&str>,
    node_alias: &str,
) -> RenderExpr {
    use super::render_expr::{Literal, RenderExpr};
    use crate::graph_catalog::expression_parser::PropertyValue;

    if items.is_empty() {
        return RenderExpr::Literal(Literal::String("{}".to_string()));
    }

    // Build aliased column expressions: t1_0.city AS _s_city, t1_0.full_name AS _s_name, ...
    // Prefix (_s_, _e_, _r_) ensures unique aliases even when start/end have same properties
    let mut aliased_cols = Vec::new();
    let cte_col_prefix = format!("{}_", node_alias);
    for item in items {
        if let Some(alias) = &item.col_alias {
            let alias_str = &alias.0;
            // Extract clean property name (after the dot, if any)
            let clean_name = if let Some(dot_pos) = alias_str.find('.') {
                &alias_str[dot_pos + 1..]
            } else {
                alias_str.as_str()
            };

            // Get the column expression
            if let RenderExpr::PropertyAccessExp(prop_access) = &item.expression {
                if let PropertyValue::Column(col_name) = &prop_access.column {
                    // Only override table alias for denormalized columns (raw DB names)
                    // CTE columns (prefixed with node_alias_, like a_code) keep their
                    // original table alias (the CTE JOIN alias)
                    let is_cte_column = col_name.starts_with(&cte_col_prefix);
                    let table_alias = match table_alias_override {
                        Some(override_alias) if !is_cte_column => override_alias,
                        _ => &prop_access.table_alias.0,
                    };
                    let col_expr = format!("{}.{}", table_alias, col_name);
                    // Use prefixed alias to avoid collision (e.g., _s_city, _e_city)
                    aliased_cols.push(format!("{} AS {}{}", col_expr, prefix, clean_name));
                }
            }
        }
    }

    if aliased_cols.is_empty() {
        return RenderExpr::Literal(Literal::String("{}".to_string()));
    }

    // Dialect-aware JSON object: CH formatRowNoNewline('JSONEachRow', cols),
    // Spark to_json(struct(cols)). Prefixed aliases ensure no collision in scope.
    let format_expr = crate::sql_generator::function_mapper::current_function_mapper()
        .json_row_object(&aliased_cols.join(", "));
    RenderExpr::Raw(format_expr)
}

/// #507: collapse a denorm-scan CTE's UNION body from TABLE grain (one row
/// per unique combination of every projected column) to NODE grain (one row
/// per distinct `id_column` value).
///
/// On a coupled cross-table denormalized schema, a node label can carry MORE
/// columns than just its identity — e.g. zeek's `IP@conn_log` also exposes a
/// `port` property. The anchor's standalone-scan Union (built by the
/// OPTIONAL-denorm CTE+LEFT JOIN path, #502/#505/#506) projects every
/// from/to-role property and combines roles with `UNION DISTINCT`, which
/// dedups on the FULL row — so one IP with several distinct port values
/// across its rows survives as several CTE rows instead of one. Any
/// downstream per-node aggregate that LEFT JOINs against this CTE (e.g.
/// `count(r)`) is then inflated by however many extra grain rows that node
/// has (observed: count 9 instead of 3 for one IP with 3 distinct ports).
///
/// Wraps `inner_sql` (a plain `SELECT ... UNION ... SELECT ...` producing one
/// row per (id, other columns...) combination) in an outer
/// `SELECT id, min(other) ... FROM (inner_sql) GROUP BY id`. `min()` (not
/// `any()`/`anyLast()`) is used for the non-identity columns so the render is
/// deterministic across repeated runs — the schema itself conflates several
/// physical rows under one node id, so Cypher's single-value `a.<property>`
/// needs SOME deterministic representative, and this preserves that
/// contract without changing which NODE ids the query returns.
///
/// R4 (adversarial review, documented trade-off — no code change): this
/// `min()`-collapse ALSO affects a plain, non-aggregate SELECT of the
/// anchor's own multi-valued column — e.g. `RETURN a.port` when a node
/// genuinely has several observed port values across its rows. The
/// returned `a.port` pins to ONE canonical (`min()`-picked) value
/// regardless of which specific row/edge the rest of that output row
/// actually correlates with — it is arbitrary-but-consistent, NOT the
/// value from the row you might expect it to be paired with. This is a
/// deliberate trade-off, not a bug: on a coupled cross-table schema, the
/// node's "own" property genuinely has no single correct value at node
/// grain (that's the whole reason table-grain fan-out existed pre-#507).
/// Confirmed via live testing this is NOT a regression — pre-#507 (main)
/// is arguably worse here: table grain produces MULTIPLE output rows per
/// node with a cross-join-style fan-out, pairing every one of that node's
/// port values against every one of its OWN unrelated edge matches
/// (equally arbitrary port/edge pairings, just spread across more rows
/// instead of collapsed into one). Callers that need the port value
/// correlated with a SPECIFIC edge should select it from the edge/relation
/// alias directly (e.g. `r.port`), not the anchor's own multi-valued
/// column.
pub(super) fn wrap_denorm_scan_cte_at_node_grain(
    inner_sql: &str,
    id_column: &str,
    exposed_columns: &std::collections::HashSet<String>,
) -> String {
    let mut others: Vec<&String> = exposed_columns
        .iter()
        .filter(|c| c.as_str() != id_column)
        .collect();
    // Nothing beyond the id column is exposed — `UNION DISTINCT` over just the
    // id is already node grain (the bug this wrap fixes only exists when
    // OTHER, non-identity columns can vary per id and get pulled into the
    // dedup key). Skip the wrap so single-property anchors render exactly as
    // before (no gratuitous SQL/golden churn for a no-op case).
    if others.is_empty() {
        return inner_sql.to_string();
    }
    others.sort();

    let mut select_list = format!("      \"{id}\" AS \"{id}\"", id = id_column);
    for col in &others {
        select_list.push_str(&format!(",\n      min(\"{col}\") AS \"{col}\""));
    }

    format!(
        "SELECT \n{select}\nFROM (\n{inner}\n)\nGROUP BY \"{id}\"\n",
        select = select_list,
        inner = inner_sql,
        id = id_column,
    )
}
