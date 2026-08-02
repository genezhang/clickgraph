//! WITH → CTE builders and their orbit (the "entangled core").
//!
//! P2.6 (`REFACTORING_SAFETY_PLAN.md` §5.1) moves the two giant WITH→CTE
//! builders (`build_chained_with_match_cte_plan`,
//! `replace_with_clause_with_cte_reference_v2`) plus their WITH-discovery /
//! pruning orbit out of `plan_builder_utils.rs` into this module, one
//! byte-identical sub-slice per PR. Decomposing the giants into smaller
//! functions is Phase 4 (§7.1) — NOT done here.
//!
//! Layout note: everything lands in this single `mod.rs` (a direct child of
//! `render_plan`, same module depth as `plan_builder_utils`), so the moved
//! bodies' `super::…` / `super::super::…` path-expressions resolve
//! byte-identically. A later Phase-4 decomposition may split this into
//! sub-files (which is a logic-edit slice, not a move).
//!
//! Extracted across P2.6 slices 1–4 (byte-identical moves, one PR each):
//!  - slice 1: the #529 shape-1 loud-guard property helpers
//!    `table_role_dependent_property_names` + `collect_property_accesses` (with
//!    their unit tests).
//!  - slice 2: `replace_with_clause_with_cte_reference_v2` (the WITH→CTE-reference
//!    rewriter).
//!  - slice 3: the WITH-discovery / join-pruning cluster
//!    (`find_all_with_clauses_grouped`, `collapse_passthrough_with`,
//!    `node_is_concrete_labeled`, `alias_has_pattern_correlation`,
//!    `prune_joins_covered_by_cte`).
//!  - slice 4: `build_chained_with_match_cte_plan` (~5,478 lines) + its two
//!    orbit structs `WithBarrierScope` / `CteNameAllocator`, and the widening of
//!    the 24 private `plan_builder_utils` helpers it calls back into.
//!
//! Names still called from outside this module are re-exported `pub(crate)` from
//! `plan_builder_utils` during the transition (notably
//! `build_chained_with_match_cte_plan` for `plan_builder.rs`); the rest resolve
//! as same-module siblings or via targeted imports at the top of this file.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::GraphSchema;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::plan_ctx::PlanCtx;
use crate::render_plan::cte_extraction::get_path_variable;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::plan_builder::RenderPlanBuilder;
use crate::render_plan::render_expr::{
    Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr,
    ScalarFnCall, TableAlias,
};
use crate::render_plan::JoinType;
use crate::render_plan::OrderByItem;
use crate::render_plan::SelectItem;
use crate::render_plan::ViewTableRef;
use crate::render_plan::{
    ArrayJoinItem, Cte, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join, JoinItems,
    LimitItem, OrderByItems, OrderByOrder, RenderPlan, SelectItems, SkipItem, Union, UnionItems,
};
use crate::sql_generator::function_mapper::current_function_mapper;
use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};
use crate::utils::cte_naming::generate_cte_name;
use crate::utils::with_clause_key::with_clause_key;
use std::collections::{HashMap, HashSet};
// `plan_contains_with_clause` is a `plan_predicates` (P2.4) predicate that
// `replace_with_clause_with_cte_reference_v2` gates its GraphRel recursion on.
use super::plan_predicates::plan_contains_with_clause;
// P2.10 import hygiene: the moved bodies (`build_chained_with_match_cte_plan` +
// the WITH-discovery cluster) reach a handful of `plan_builder_helpers` /
// `alias_utils` helpers by bare name — previously via two `*` globs the old file
// relied on. Named imports (the exact set the compiler requires) so shadowing is
// visible; other helpers are still reached by explicit `super::…::` paths.
use super::plan_builder_helpers::{
    combine_optional_filters_with_and, extract_predicates_for_alias_logical,
    has_with_clause_in_graph_rel, rewrite_logical_path_functions,
};
use super::utils::alias_utils::{collect_aliases_from_plan, find_cte_reference_alias};
// P2.6 slice 4: `build_chained_with_match_cte_plan` calls these P2.2/P2.5-moved
// rewriters (in sibling modules) by bare name — the old file reached them via
// its own re-exports/globs. Mirror of `pattern_comprehension_sql.rs`'s back-import.
use super::cte_graph_joins_rewrite::update_graph_joins_cte_refs;
use super::cte_rewrite::{
    collect_with_cte_table_aliases, remap_cte_names_in_render_plan,
    rewrite_join_conditions_for_cte_aliases, rewrite_operator_application_for_cte_join,
    strip_table_alias_from_resolved,
};
use super::pattern_comprehension_sql::{
    add_join_to_plan_or_union_branches, build_node_id_expr_for_join,
    build_pattern_comprehension_sql, find_node_id_column_from_schema, find_pc_cte_join_column,
    generate_and_replace_arraycount_pc_subqueries, generate_pattern_comprehension_cte,
    replace_count_star_placeholders_in_select_or_union, rewrite_logical_expr_aliases,
};
use super::vlp_rewrite::{
    rewrite_render_expr_for_vlp_with_from_alias, rewrite_vlp_aggregate_aliases,
    translate_db_columns_to_cypher_properties,
};
// P2.6 slice 4: 33 helpers `build_chained_with_match_cte_plan` calls that stay in
// `plan_builder_utils`, back-imported during the transition (24 previously-private
// ones were widened to `pub(crate)` at their definitions).
use super::plan_builder_utils::{
    apply_outer_scope_passes, build_property_mapping_from_columns,
    clear_stale_joins_for_cte_aliases, collect_aliases_from_single_render_expr,
    collect_analyzer_cte_names, collect_unwind_aliases, compute_cte_id_column_for_alias,
    count_plan_depth, expand_table_alias_to_group_by_id_only, expand_table_alias_to_select_items,
    expr_contains_aggregate, extract_correlation_predicates,
    extract_cte_join_condition_from_filter, extract_from_alias_from_cte_name, find_graphrel,
    find_graphrel_where_predicate, find_id_column_in_cte, find_unwind_aliases, hoist_nested_ctes,
    is_cte_reference, is_literal_expr, plan_has_denormalized_union, plan_has_shortest_path,
    rename_branch_aliases, resolve_denormalized_property_in_expr_impl,
    rewrite_count_to_conditional, rewrite_person_to_fk, rewrite_table_alias_in_render_plan,
    rewrite_vlp_union_branch_aliases, show_plan_structure, show_with_structure,
    split_render_and_conjuncts, try_flatten_head_collect_map_literal,
};
// `RenderCase` is used only by the `collect_property_accesses_tests` module
// below (via `use super::*`); gate it so the non-test lib build stays clean.
#[cfg(test)]
use crate::render_plan::render_expr::RenderCase;

/// Local alias mirroring `plan_builder_utils`'s private `RenderPlanBuilderResult`
/// (same `Result<T, RenderBuildError>`); each moved-out render module declares
/// its own, matching the P2.5 sibling modules (`vlp_rewrite`,
/// `cte_graph_joins_rewrite`).
type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

/// #529 shape 1, bug 3 (loud guard — NOT a fix): an undirected self-
/// referencing edge over a DENORMALIZED/embedded node (no separate physical
/// node table — identity is a role-dependent column embedded on the SAME
/// row as the relationship, e.g. Zeek's `IP` node on `conn_log` via
/// `id.orig_h`/`id.resp_h`) needs its per-role UNION branches to alternate
/// which embedded column identifies the anchor in each branch. That
/// alternation is NOT implemented — `find_id_column_for_alias`
/// (`render_plan/plan_builder.rs`) resolves a node's identity via the SAME
/// static `NodeSchema::node_id` field regardless of which role (from or to)
/// the node plays in a given direction branch, and `bidirectional_union.rs`'s
/// per-branch `column_swaps` map (built specifically to correct this for
/// Incoming-direction denormalized branches) is only ever threaded through
/// `Projection` items, never `WithClause` items — so it never reaches a bare
/// WITH-aggregate's pass-through/aggregate columns. The live CTE builder
/// (`build_chained_with_match_cte_plan`) compounds this: once a WITH
/// segment's pattern renders as a `Union` of 2+ direction branches (the
/// "non-denormalized-union" path — see its own doc comment there), it
/// attaches the WITH's projection (`select_items`, e.g. `a`, `count(r) AS c`)
/// ONLY to the base branch (`rendered.select`) and never to the other UNION
/// branches — `to_sql_query.rs`'s aggregate-UNION renderer then deliberately
/// reuses that ONE shared `plan.select` for every branch's inner SELECT
/// (`render_union_branch_sql` shows this codebase DOES have a working
/// per-branch-select convention elsewhere; the aggregate-UNION path just
/// doesn't use it). So every UNION branch ends up projecting the IDENTICAL
/// column, silently dropping rows that only ever appear in the un-selected
/// role and inflating counts for rows appearing in both roles (live-verified
/// against the zeek fixture:
/// `MATCH (a:IP)-[r:ACCESSED]-(b:IP) WITH a, count(r) AS c RETURN a.ip, c`
/// returns 3 wrong rows — missing `10.0.0.99` and `142.250.80.46` entirely,
/// and double-counting `93.184.216.34` — vs. the true 5 distinct IPs with
/// `count`s `{10.0.0.99: 1, 142.250.80.46: 1, 192.168.1.10: 1,
/// 192.168.1.20: 1, 93.184.216.34: 2}`).
///
/// Detects the schema-level precondition for this unfixed case: the set of
/// Cypher property names that are role-dependent (their physical column
/// genuinely differs between from/to role — see `NodeSchema::
/// role_dependent_property_names`, the canonical schema-catalog accessor
/// this routes through) for any node backed by `table`. A NORMALIZED
/// self-referencing edge (e.g. `(p:Person)-[:KNOWS]-(p2:Person)` via a
/// separate node table) is deliberately UNAFFECTED and must not trigger
/// this: its identity column is the same real physical ID regardless of
/// role, so sharing one SELECT across UNION branches there is correct, not
/// a bug — the accessor returns an empty set immediately for a normal node
/// schema.
///
/// Checking PROPERTY NAMES (not just "does this table back a role-dependent
/// node at all") matters: in a coupled/embedded schema, a relationship alias
/// (e.g. `r`) and a node alias (e.g. `a`) can share the exact same physical
/// table, but the relationship's OWN properties (e.g. `r`'s edge_id `uid`)
/// are role-INDEPENDENT even though the table also backs a role-dependent
/// node — flagging any reference to that TABLE regardless of which property
/// is accessed was an earlier draft's false positive, caught by
/// `undirected_optional_with_aggregate_coupled_anchor_group_by_529`.
// P2.6 slice 1: widened `fn` → `pub(crate) fn` (the sole change vs the
// original) so the transition re-export in `plan_builder_utils` reaches it;
// body is verbatim.
pub(crate) fn table_role_dependent_property_names(
    schema: &GraphSchema,
    table: &str,
) -> std::collections::HashSet<String> {
    schema
        .all_node_schemas()
        .values()
        .filter(|ns| ns.full_table_name() == table)
        .flat_map(|ns| ns.role_dependent_identifiers())
        .collect()
}

/// #529 (R6, post-adversarial-review): recursively collect
/// `(table_alias, property_name)` pairs from every `PropertyAccessExp`
/// reachable within `expr` — used by the #529 shape-1 loud guard to check
/// exactly which properties (not just which aliases) a WITH projection
/// references, since alias-level checking alone conflates a role-dependent
/// node's properties with a same-table relationship's own, role-independent
/// ones (see `table_role_dependent_property_names`'s doc comment).
///
/// R6 history: the ORIGINAL version of this function hand-rolled a partial
/// `match` covering only `PropertyAccessExp`/`OperatorApplicationExp`/
/// `AggregateFnCall`/`ScalarFnCall`/`Case`'s `when_then`+`else_expr`, with a
/// `_ => {}` catch-all silently dropping everything else — including
/// `List`, `MapLiteral`, `ArraySubscript`/`ArraySlicing`, `ReduceExpr`,
/// `InSubquery`, and `Case`'s own `expr` field (simple-CASE). Adversarial
/// review found this exploitable and non-theoretical: `WITH count(r) AS c,
/// [a.ip] AS tags RETURN c, tags` on the coupled zeek schema rendered and
/// executed with NO guard firing — the exact same non-alternating-branch
/// corruption, just reached through a `[a.ip]` list literal instead of a
/// bare `a.ip` select item.
///
/// Fixed by making this match EXHAUSTIVE over every `RenderExpr` variant —
/// no `_` catch-all, so the compiler forces this function to be updated
/// whenever a new variant is added, closing off the whole "one more shape
/// slips through" bug class rather than patching individual instances of
/// it. Deliberately mirrors (and is verified against) `references_alias`
/// (`render_plan/expression_utils.rs`), this codebase's existing exhaustive
/// `RenderExpr` walker for "does this expression reference alias X" — with
/// one correction: `references_alias`'s own `Case` arm ALSO never checks
/// `RenderCase.expr` (the scrutinee of a simple `CASE x WHEN ...`, as
/// opposed to a searched `CASE WHEN cond ...`) — a latent gap in that
/// function too, found while auditing this one arm-for-arm. Fixed HERE
/// (this function checks `case_expr.expr`) since a simple-CASE repro is
/// exactly what this round's verification requires; `references_alias`
/// itself is used by several other call sites with unverified blast
/// radius, so correcting its own gap is left as a follow-up rather than
/// bundled into this fix.
// P2.6 slice 1: widened `fn` → `pub(crate) fn` (the sole change vs the
// original) so the transition re-export in `plan_builder_utils` reaches it;
// body is verbatim.
pub(crate) fn collect_property_accesses<'a>(
    expr: &'a RenderExpr,
    out: &mut Vec<(&'a str, &'a str)>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            out.push((pa.table_alias.0.as_str(), pa.column.raw()));
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_property_accesses(operand, out);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_property_accesses(arg, out);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_property_accesses(arg, out);
            }
        }
        RenderExpr::List(exprs) => {
            for e in exprs {
                collect_property_accesses(e, out);
            }
        }
        RenderExpr::Case(c) => {
            // `expr` is the scrutinee of a simple `CASE x WHEN v1 THEN ...`
            // (`None` for a searched `CASE WHEN cond THEN ...`) — missing
            // from the original partial match (and from `references_alias`
            // too, see doc comment above).
            if let Some(scrutinee) = &c.expr {
                collect_property_accesses(scrutinee, out);
            }
            for (when, then) in &c.when_then {
                collect_property_accesses(when, out);
                collect_property_accesses(then, out);
            }
            if let Some(else_expr) = &c.else_expr {
                collect_property_accesses(else_expr, out);
            }
        }
        RenderExpr::InSubquery(subquery) => {
            collect_property_accesses(&subquery.expr, out);
        }
        RenderExpr::ReduceExpr(reduce) => {
            collect_property_accesses(&reduce.initial_value, out);
            collect_property_accesses(&reduce.list, out);
            collect_property_accesses(&reduce.expression, out);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                collect_property_accesses(v, out);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_property_accesses(array, out);
            collect_property_accesses(index, out);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_property_accesses(array, out);
            if let Some(f) = from {
                collect_property_accesses(f, out);
            }
            if let Some(t) = to {
                collect_property_accesses(t, out);
            }
        }
        // EXISTS subqueries and pre-rendered pattern-count SQL are
        // self-contained — no outer-scope alias/property reference to
        // collect (mirrors `references_alias`'s treatment of the same two
        // variants).
        RenderExpr::ExistsSubquery(_) => {}
        RenderExpr::PatternCount(_) => {}
        // Raw SQL text — opaque, can't reliably extract (alias, property)
        // pairs from arbitrary text (mirrors `references_alias`, which only
        // does a best-effort substring check here since it just needs a
        // bool, not a structured property name).
        RenderExpr::Raw(_) => {}
        // A bare alias reference (e.g. whole-node `RETURN a`, or `count(a)`
        // before normalization) has no property name attached — nothing to
        // collect as a (alias, property) pair.
        RenderExpr::TableAlias(_) => {}
        // CteEntityRef references CTE columns, not a source alias/property.
        RenderExpr::CteEntityRef(_) => {}
        // No sub-expressions to recurse into.
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => {}
    }
}

/// Helper function to hoist nested CTEs from a rendered plan to a parent CTE list.
///
/// This is used after rendering a plan that may contain nested CTEs (e.g., from
/// variable-length path queries) to pull those CTEs up to the parent level so they
/// can be defined BEFORE the main CTE that references them.
///
/// # Arguments
/// * `from` - The RenderPlan to extract CTEs from (will be emptied)
/// * `to` - The destination vector to append the CTEs to
///
/// # Example
/// ```rust
/// // Create sample plan and schema (would be provided in real usage)
/// // let plan = Arc::new(LogicalPlan::default()); // Placeholder
/// // let schema = GraphSchema::default(); // Placeholder
/// // let mut with_cte_render = render_without_with_detection(plan, &schema)?;
/// // let mut all_ctes = Vec::new();
/// // hoist_nested_ctes(&mut with_cte_render, &mut all_ctes);
/// // all_ctes now contains any VLP CTEs that were nested in with_cte_render
/// ```
///
/// Replace the WITH clause subplan with a CTE reference (ViewScan pointing to CTE).
///
/// This transforms the plan so the WITH clause output comes from the CTE instead of
/// recomputing it.
///
/// IMPORTANT: We look for WithClause nodes which mark the true scope boundary.
/// When found, we replace them with a CTE reference.
///
/// CRITICAL: We only replace a WithClause if its INPUT has NO nested WITH clauses.
/// This ensures we replace the INNERMOST WITH first, then the next one, etc.
///
/// V2 of replace_with_clause_with_cte_reference that also filters out pre-WITH joins.
///
/// When we replace a WITH clause with a CTE reference, the joins from before the WITH
/// boundary should be removed from GraphJoins in the outer query - they're now inside the CTE.
///
/// `pre_with_aliases` contains the table aliases that were defined INSIDE the WITH clause
/// (before the boundary). These should be filtered out from outer GraphJoins.
///
/// Check if a join is for the inner scope (part of the pre-WITH pattern)
///
/// This is determined by checking if the join references aliases that are
/// NOT in the post-WITH scope (i.e., they're part of the CTE content).
/// Find the INNERMOST WITH clause subplan in a nested plan structure.
///
/// KEY INSIGHT: With chained WITH clauses (e.g., WITH a MATCH...WITH a,b MATCH...),
/// we need to process them from innermost to outermost. The innermost WITH is
/// the one whose INPUT has NO other WITH clauses nested inside it.
///
/// This function recursively searches for WITH clauses and returns the one
/// whose input is "clean" (contains no nested WITH).
///
/// Returns (with_clause_plan, alias_name) if found.
/// Find all WITH clauses in a plan grouped by their alias.
/// Returns HashMap where each alias maps to all WITH clause plans with that alias.
/// This handles the case where Union branches each have their own WITH clause with the same alias.
/// Returns owned (cloned) LogicalPlans to avoid lifetime issues with mutations.
/// Prune joins from GraphJoins that are already covered by a CTE.
///
/// Collapse a passthrough WITH clause by replacing it with its input.
/// A passthrough WITH is one that simply wraps a CTE reference without any transformations:
/// - Single item that's just a TableAlias
/// - No DISTINCT, ORDER BY, SKIP, LIMIT, WHERE
///
/// This function finds the passthrough WITH for the given alias and replaces it with its input.
/// Uses the analyzer's CTE name to distinguish between multiple consecutive WITHs with same alias.
/// When we have a query like:
///   WITH a MATCH (a)-[:F]->(b) WITH a,b MATCH (b)-[:F]->(c)
///
/// After processing, we have:
/// - CTE: with_a_b_cte2 (contains the pattern for a→b)
/// - Final plan: GraphJoins with joins for [a→t1→b, b→t2→c]
///
/// The joins [a→t1→b] are already materialized in the CTE, so they should be removed.
/// Only [b→t2→c] should remain in the final query.
///
/// This function:
/// 1. Traverses the plan to find GraphJoins nodes
/// 2. Identifies CTE-backed joins and uses position-aware pruning
///    (see `prune_joins_covered_by_cte` for details)
/// 3. Replaces the WITH clause with a CTE reference
///
/// TRAVERSAL NOTE (CLAUDE.md rule 5): unlike the four detection/collection
/// walkers, this transform does NOT route through `LogicalPlan::map_children`.
/// That is deliberate, not drift:
///   - `map_children`'s signature is infallible (`FnMut(&LogicalPlan) ->
///     LogicalPlan`), whereas every recursive step here is fallible
///     (`RenderPlanBuilderResult<_>` via `?`). Adapting one to the other would
///     require panicking or smuggling errors — both worse than an explicit match.
///   - Almost every arm carries per-variant transform logic that a generic
///     child-map cannot express: WithClause performs the actual replacement,
///     GraphRel gates each branch on `plan_contains_with_clause`/`needs_processing`
///     and rebuilds with reset `cte_references`/`pattern_combinations`, Projection
///     remaps PropertyAccess to CTE columns, GraphJoins prunes pre-WITH joins,
///     GraphNode swaps matching nodes for CTE refs, and Union enforces #517
///     per-arm scope isolation. A blind `map_children` would preserve fields these
///     arms intentionally reset, silently changing query semantics.
///
/// The gating predicates it calls (`plan_contains_with_clause`,
/// `needs_processing`) ARE now backed by `children()`, so this function still
/// benefits from the unified traversal at its decision points.
// ── Helpers hoisted from `replace_with_clause_with_cte_reference_v2` (Phase-4
// §7.1): CTE-reference construction + PropertyAccess remapping. These were
// nested `fn`s (no captures) inside replace_v2; moved to module level verbatim.
// Helper to remap PropertyAccess expressions to use CTE column names
// CRITICAL: After creating a CTE reference, PropertyAccess expressions in downstream nodes
// (like Projection) still have the OLD column names from FilterTagging (which used the
// original ViewScan's property_mapping). FilterTagging may have resolved Cypher properties
// to DB columns already, so we need to REVERSE that using db_to_cypher mapping.
fn remap_property_access_for_cte(
    expr: crate::query_planner::logical_expr::LogicalExpr,
    cte_alias: &str,
    property_mapping: &HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
    db_to_cypher: &HashMap<String, String>,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::query_planner::logical_expr::LogicalExpr;

    match expr {
        LogicalExpr::PropertyAccessExp(mut prop) => {
            // Check if this PropertyAccess references the CTE alias
            if prop.table_alias.0 == cte_alias {
                let current_col = prop.column.raw();

                // CRITICAL: FilterTagging ALWAYS resolves Cypher properties to DB columns
                // So current_col is almost certainly a DB column name, not a Cypher property
                //
                // Strategy:
                // 1. PRIMARY: Try reverse mapping (DB column → Cypher property → CTE column)
                // 2. FALLBACK: Direct lookup (handles identity mappings where Cypher name = DB name)

                if let Some(cypher_prop) = db_to_cypher.get(current_col) {
                    // Found! current_col is a DB column - reverse it to Cypher property
                    if let Some(cte_col) = property_mapping.get(cypher_prop) {
                        log::debug!(
                            "🔧 remap_property_access: Remapped {}.{} → {} (DB '{}' → Cypher '{}' → CTE)",
                            cte_alias, current_col, cte_col.raw(), current_col, cypher_prop
                        );
                        prop.column = cte_col.clone();
                    } else {
                        log::debug!(
                            "🔧 remap_property_access: Reverse mapped DB '{}' to Cypher '{}' but no CTE column found!",
                            current_col, cypher_prop
                        );
                    }
                } else if let Some(cte_col) = property_mapping.get(current_col) {
                    // Fallback: Identity mapping where Cypher property = DB column
                    // Example: user_id: user_id → both "user_id" (Cypher) and "user_id" (DB)
                    log::debug!(
                        "🔧 remap_property_access: Remapped {}.{} → {} (direct/identity mapping)",
                        cte_alias,
                        current_col,
                        cte_col.raw()
                    );
                    prop.column = cte_col.clone();
                } else {
                    log::debug!(
                        "🔧 remap_property_access: Could not remap {}.{} - not in db_to_cypher or property_mapping",
                        cte_alias, current_col
                    );
                }
            }
            LogicalExpr::PropertyAccessExp(prop)
        }
        LogicalExpr::OperatorApplicationExp(mut op) => {
            op.operands = op
                .operands
                .into_iter()
                .map(|operand| {
                    remap_property_access_for_cte(
                        operand,
                        cte_alias,
                        property_mapping,
                        db_to_cypher,
                    )
                })
                .collect();
            LogicalExpr::OperatorApplicationExp(op)
        }
        LogicalExpr::AggregateFnCall(mut agg) => {
            agg.args = agg
                .args
                .into_iter()
                .map(|arg| {
                    remap_property_access_for_cte(arg, cte_alias, property_mapping, db_to_cypher)
                })
                .collect();
            LogicalExpr::AggregateFnCall(agg)
        }
        LogicalExpr::ScalarFnCall(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(|arg| {
                    remap_property_access_for_cte(arg, cte_alias, property_mapping, db_to_cypher)
                })
                .collect();
            LogicalExpr::ScalarFnCall(func)
        }
        LogicalExpr::List(list) => LogicalExpr::List(
            list.into_iter()
                .map(|item| {
                    remap_property_access_for_cte(item, cte_alias, property_mapping, db_to_cypher)
                })
                .collect(),
        ),
        LogicalExpr::Case(mut case_expr) => {
            if let Some(expr) = case_expr.expr {
                case_expr.expr = Some(Box::new(remap_property_access_for_cte(
                    *expr,
                    cte_alias,
                    property_mapping,
                    db_to_cypher,
                )));
            }
            case_expr.when_then = case_expr
                .when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        remap_property_access_for_cte(
                            when,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        ),
                        remap_property_access_for_cte(
                            then,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        ),
                    )
                })
                .collect();
            if let Some(else_expr) = case_expr.else_expr {
                case_expr.else_expr = Some(Box::new(remap_property_access_for_cte(
                    *else_expr,
                    cte_alias,
                    property_mapping,
                    db_to_cypher,
                )));
            }
            LogicalExpr::Case(case_expr)
        }
        // Other expressions don't contain PropertyAccess
        other => other,
    }
}

// Helper to remap PropertyAccess in a ProjectionItem
fn remap_projection_item(
    item: crate::query_planner::logical_plan::ProjectionItem,
    cte_alias: &str,
    property_mapping: &HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
    db_to_cypher: &HashMap<String, String>,
) -> crate::query_planner::logical_plan::ProjectionItem {
    crate::query_planner::logical_plan::ProjectionItem {
        expression: remap_property_access_for_cte(
            item.expression,
            cte_alias,
            property_mapping,
            db_to_cypher,
        ),
        col_alias: item.col_alias,
    }
}

// Helper to create a CTE reference node with proper property_mapping
fn create_cte_reference(
    cte_name: &str,
    with_alias: &str,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> LogicalPlan {
    use crate::graph_catalog::expression_parser::PropertyValue;
    // These were in scope via `replace_v2`'s top-of-body `use logical_plan::*` /
    // `use Arc` when this fn was nested inside it (Phase-4 §7.1 hoist).
    use crate::query_planner::logical_plan::{GraphNode, ViewScan};
    use std::sync::Arc;

    // CRITICAL: Use the original WITH alias (e.g., "a") as the GraphNode alias
    // This ensures property references like "a.user_id" work correctly
    // The FROM clause will render as: FROM with_a_cte1 AS a
    let table_alias = with_alias.to_string();

    // Build property_mapping using CYPHER PROPERTY NAMES ONLY
    // Store the ViewScan's DB mapping separately so we can reverse-resolve DB columns
    let (property_mapping, _db_to_cypher_mapping) = if let Some(meta) = cte_schemas.get(cte_name) {
        let mut mapping = HashMap::new();
        let mut db_to_cypher = HashMap::new(); // Reverse: DB column → Cypher property

        // Parse the composite with_alias into individual aliases
        // e.g., "fids_p" → ["fids", "p"] (from exported_aliases tracked earlier)
        // We need individual aliases to match CTE column names like "p1_p_id"

        // Build mappings from SelectItems
        for item in &meta.select_items {
            if let Some(cte_col_alias) = &item.col_alias {
                let cte_col_name = &cte_col_alias.0;

                // Use the proper p{N} CTE column naming parser for unambiguous decoding
                if let Some((col_alias, cypher_prop)) =
                    crate::utils::cte_column_naming::parse_cte_column(cte_col_name)
                {
                    // Primary: Cypher property → CTE column
                    // Key format: "alias.property" so downstream property access works
                    mapping.insert(
                        cypher_prop.to_string(),
                        PropertyValue::Column(cte_col_name.clone()),
                    );

                    // Reverse: DB column → Cypher property (for resolving FilterTagging's DB columns)
                    if let RenderExpr::PropertyAccessExp(prop_access) = &item.expression {
                        let db_col = prop_access.column.raw();

                        // Detect conflicts: multiple Cypher properties using same DB column
                        if let Some(existing_cypher) = db_to_cypher.get(db_col) {
                            if existing_cypher != &cypher_prop {
                                log::debug!(
                                    "🔧 create_cte_reference: CONFLICT - DB column '{}' used by both Cypher '{}' and '{}'. \
                                     Using '{}' (last wins). Queries using '{}.{}' may get wrong column!",
                                    db_col, existing_cypher, cypher_prop, cypher_prop, col_alias, existing_cypher
                                );
                            }
                        }

                        db_to_cypher.insert(db_col.to_string(), cypher_prop.to_string());

                        if db_col != cypher_prop {
                            log::debug!(
                                "🔧 create_cte_reference: Reverse mapping for '{}': DB '{}' ← Cypher '{}' → CTE '{}'",
                                col_alias, db_col, cypher_prop, cte_col_name
                            );
                        }
                    }
                } else if let Some(cypher_prop) =
                    cte_col_name.strip_prefix(&format!("{}_", with_alias))
                {
                    // Legacy fallback: try stripping composite alias prefix
                    mapping.insert(
                        cypher_prop.to_string(),
                        PropertyValue::Column(cte_col_name.clone()),
                    );
                } else {
                    // Fallback: identity mapping (for non-property columns like "fids")
                    mapping.insert(
                        cte_col_name.clone(),
                        PropertyValue::Column(cte_col_name.clone()),
                    );
                }
            }
        }

        // CRITICAL FIX: Add DB column mappings from stored_property_mapping
        // The stored_property_mapping has entries like ((u, full_name), u_name)
        // which tells us: DB column "full_name" should map to CTE column "u_name"
        // We need to add these to the ViewScan property_mapping as:
        // ("full_name", Column("u_name"))
        for ((alias, db_prop), cte_column) in meta.property_mapping.iter() {
            if alias == with_alias {
                // This is a mapping for our alias (e.g., "u")
                // Add it to the mapping if not already present
                if !mapping.contains_key(db_prop) {
                    mapping.insert(db_prop.clone(), PropertyValue::Column(cte_column.clone()));
                    log::debug!(
                        "🔧 create_cte_reference: Added DB column mapping from stored: ({}, {}) → {}",
                        alias,
                        db_prop,
                        cte_column
                    );
                }
            }
        }

        log::info!(
            "🔧 create_cte_reference: Built mappings for '{}': {} Cypher→CTE + {} DB→Cypher",
            cte_name,
            mapping.len(),
            db_to_cypher.len()
        );
        (mapping, db_to_cypher)
    } else {
        log::debug!(
            "🔧 create_cte_reference (v2): No schema found for CTE '{}', using empty property_mapping",
            cte_name
        );
        (HashMap::new(), HashMap::new())
    };

    // Look up the actual ID column from cte_schemas (alias → ID column mapping)
    // The alias_to_id stores prefixed names like "a_code", but ViewScan.id_column
    // should be unprefixed (e.g., "code") because resolve_cte_reference adds the prefix.
    let cte_id_column = cte_schemas
        .get(cte_name)
        .and_then(|meta| {
            // Try direct lookup first
            meta.alias_to_id
                .get(with_alias)
                .or_else(|| {
                    // Combined alias (e.g., "a_allNeighboursCount") won't match
                    // individual aliases (e.g., "a"). Try first matching key.
                    meta.alias_to_id
                        .keys()
                        .next()
                        .and_then(|k| meta.alias_to_id.get(k))
                })
                .map(|prefixed| {
                    // Strip any alias prefix: "a_code" → "code"
                    // Try with_alias first, then each key in alias_to_id
                    let stripped = prefixed
                        .strip_prefix(&format!("{}_", with_alias))
                        .or_else(|| {
                            meta.alias_to_id
                                .keys()
                                .find_map(|k| prefixed.strip_prefix(&format!("{}_", k)))
                        })
                        .unwrap_or(prefixed);
                    stripped.to_string()
                })
        })
        .unwrap_or_else(|| "id".to_string());
    log::info!(
        "🔧 create_cte_reference: CTE '{}' alias '{}' → id_column '{}'",
        cte_name,
        with_alias,
        cte_id_column
    );

    LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
            source_table: cte_name.to_string(),
            view_filter: None,
            property_mapping,
            id_column: cte_id_column.clone(),
            output_schema: vec!["id".to_string()],
            projections: vec![],
            from_id: None,
            to_id: None,
            input: None,
            view_parameter_names: None,
            view_parameter_values: None,
            use_final: false,
            is_denormalized: false,
            from_node_properties: None,
            to_node_properties: None,
            type_column: None,
            type_values: None,
            from_label_column: None,
            to_label_column: None,
            schema_filter: None,
            node_label: None,
        }))),
        alias: table_alias,
        label: None,
        is_denormalized: false,
        projected_columns: None,
        node_types: None,
    })
}

/// Handle the `Projection` arm of `replace_with_clause_with_cte_reference_v2`
/// (Phase-4 §7.1 extraction).
///
/// Recurse into the projection input; if it became a CTE reference for
/// `with_alias`, remap the projection items' PropertyAccess expressions onto the
/// CTE's column names (per-alias `property_mapping` / `db_to_cypher` rebuilt from
/// the CTE schema), then rebuild the `Projection` over the new input.
fn replace_v2_projection_arm(
    proj: &crate::query_planner::logical_plan::Projection,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::Projection;
    use std::sync::Arc;

    log::info!(
        "🔀 replace_v2: Processing Projection, input type: {:?}",
        std::mem::discriminant(proj.input.as_ref())
    );
    let new_input = replace_with_clause_with_cte_reference_v2(
        &proj.input,
        with_alias,
        cte_name,
        pre_with_aliases,
        cte_schemas,
    )?;
    log::info!(
        "🔀 replace_v2: Projection new_input type: {:?}",
        std::mem::discriminant(&new_input)
    );

    // CRITICAL: Check if new_input is a CTE reference (GraphNode wrapping ViewScan for CTE)
    // If so, remap PropertyAccess expressions in projection items to use CTE column names
    let should_remap = match &new_input {
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                vs.source_table.starts_with("with_") && gn.alias == with_alias
            } else {
                false
            }
        }
        _ => false,
    };

    let remapped_items = if should_remap {
        // Extract property_mapping from the CTE reference and rebuild per-alias mappings
        if let LogicalPlan::GraphNode(gn) = &new_input {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                // Build per-alias property mappings from CTE columns
                // For composite alias "fids_p", individual aliases are "fids" and "p"
                // CTE column "p1_p_id" maps to alias="p", property="id"
                let mut per_alias_mappings: HashMap<
                    String,
                    HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
                > = HashMap::new();
                let mut per_alias_db_to_cypher: HashMap<String, HashMap<String, String>> =
                    HashMap::new();

                if let Some(meta) = cte_schemas.get(&vs.source_table) {
                    for item in &meta.select_items {
                        if let Some(cte_col_alias) = &item.col_alias {
                            let cte_col_name = &cte_col_alias.0;
                            if let Some((col_alias, cypher_prop)) =
                                crate::utils::cte_column_naming::parse_cte_column(cte_col_name)
                            {
                                // Add to per-alias property mapping
                                per_alias_mappings
                                    .entry(col_alias.to_string())
                                    .or_default()
                                    .insert(
                                        cypher_prop.to_string(),
                                        crate::graph_catalog::expression_parser::PropertyValue::Column(
                                            cte_col_name.clone(),
                                        ),
                                    );

                                // Build reverse DB→Cypher mapping per alias
                                if let RenderExpr::PropertyAccessExp(prop_access) = &item.expression
                                {
                                    let db_col = prop_access.column.raw();
                                    per_alias_db_to_cypher
                                        .entry(col_alias.to_string())
                                        .or_default()
                                        .insert(db_col.to_string(), cypher_prop.to_string());
                                }
                            }
                        }
                    }
                }

                log::info!(
                    "🔧 replace_v2: Remapping Projection items for CTE '{}' (alias='{}') with {} per-alias mappings: {:?}",
                    vs.source_table,
                    with_alias,
                    per_alias_mappings.len(),
                    per_alias_mappings.keys().collect::<Vec<_>>()
                );

                // Remap each projection item against each individual alias
                let mut items: Vec<crate::query_planner::logical_plan::ProjectionItem> =
                    proj.items.clone();
                for (alias, alias_mapping) in &per_alias_mappings {
                    let alias_db_to_cypher = per_alias_db_to_cypher
                        .get(alias)
                        .cloned()
                        .unwrap_or_default();
                    items = items
                        .into_iter()
                        .map(|item| {
                            remap_projection_item(item, alias, alias_mapping, &alias_db_to_cypher)
                        })
                        .collect();
                }

                // Also remap against composite alias for non-property columns (e.g., "fids")
                let composite_db_to_cypher = HashMap::new();
                items = items
                    .into_iter()
                    .map(|item| {
                        remap_projection_item(
                            item,
                            with_alias,
                            &vs.property_mapping,
                            &composite_db_to_cypher,
                        )
                    })
                    .collect();

                items
            } else {
                proj.items.clone()
            }
        } else {
            proj.items.clone()
        }
    } else {
        proj.items.clone()
    };

    Ok(LogicalPlan::Projection(Projection {
        input: Arc::new(new_input),
        items: remapped_items,
        distinct: proj.distinct,
        pattern_comprehensions: proj.pattern_comprehensions.clone(),
    }))
}

/// Handle the `GraphJoins` arm of `replace_with_clause_with_cte_reference_v2`
/// (Phase-4 §7.1 extraction).
///
/// Recurse into the joins' input; if it became a CTE reference for `with_alias`,
/// rewrite the join list and anchor so they reference the CTE columns, then
/// rebuild the `GraphJoins` over the new input.
fn replace_v2_graph_joins_arm(
    graph_joins: &crate::query_planner::logical_plan::GraphJoins,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::GraphJoins;
    use std::sync::Arc;

    let new_input = replace_with_clause_with_cte_reference_v2(
        &graph_joins.input,
        with_alias,
        cte_name,
        pre_with_aliases,
        cte_schemas,
    )?;

    // Helper to check if a join condition references any stale alias
    fn condition_has_stale_refs(
        join: &crate::query_planner::logical_plan::Join,
        stale_aliases: &std::collections::HashSet<String>,
    ) -> bool {
        for op in &join.joining_on {
            for operand in &op.operands {
                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) =
                    operand
                {
                    if stale_aliases.contains(&pa.table_alias.0) {
                        return true;
                    }
                }
            }
        }
        false
    }

    // Filter out joins from the pre-WITH scope AND update joins for the WITH alias
    // Also filter out joins that have stale references in their conditions
    let updated_joins: Vec<crate::query_planner::logical_plan::Join> = graph_joins
        .joins
        .iter()
        .filter_map(|j| {
            // Filter out joins that are from the pre-WITH scope
            if pre_with_aliases.contains(&j.table_alias) {
                log::debug!(
                    "🔧 replace_v2: Filtering out pre-WITH join for alias '{}'",
                    j.table_alias
                );
                return None;
            }

            // Filter out joins whose conditions reference stale aliases
            if condition_has_stale_refs(j, pre_with_aliases) {
                log::debug!(
                    "🔧 replace_v2: Filtering out join with stale condition for alias '{}'",
                    j.table_alias
                );
                return None;
            }

            // Update joins that reference the WITH alias to use the CTE
            if j.table_alias == with_alias {
                log::debug!(
                    "🔧 replace_v2: Updating join for alias '{}' to use CTE '{}'",
                    with_alias,
                    cte_name
                );
                Some(crate::query_planner::logical_plan::Join {
                    table_name: cte_name.to_string(),
                    table_alias: j.table_alias.clone(),
                    joining_on: j.joining_on.clone(),
                    join_type: j.join_type.clone(),
                    pre_filter: j.pre_filter.clone(),
                    from_id_column: j.from_id_column.clone(),
                    to_id_column: j.to_id_column.clone(),
                    graph_rel: None,
                    is_cartesian: false,
                })
            } else {
                Some(j.clone())
            }
        })
        .collect();

    // Update anchor_table if it was in pre-WITH scope
    let new_anchor = if let Some(ref anchor) = graph_joins.anchor_table {
        if pre_with_aliases.contains(anchor) {
            log::debug!(
                "🔧 replace_v2: Updating anchor from '{}' to '{}'",
                anchor,
                with_alias
            );
            Some(with_alias.to_string())
        } else {
            Some(anchor.clone())
        }
    } else {
        None
    };

    Ok(LogicalPlan::GraphJoins(GraphJoins {
        input: Arc::new(new_input),
        joins: updated_joins,
        optional_aliases: graph_joins.optional_aliases.clone(),
        anchor_table: new_anchor,
        cte_references: graph_joins.cte_references.clone(),
        correlation_predicates: vec![],
    }))
}

/// Handle the `GraphRel` arm of `replace_with_clause_with_cte_reference_v2`
/// (Phase-4 §7.1 extraction).
///
/// Recurse into whichever GraphRel sub-plans still need processing (left/right
/// connection / center — a nested `needs_processing` walk decides), then rebuild
/// the `GraphRel` with the rewritten children (its own `cte_references` reset to
/// empty — the outer wrapper repopulates them).
fn replace_v2_graph_rel_arm(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::GraphRel;
    use std::sync::Arc;

    // Helper to check if we need to process this branch
    // We need to process it if:
    // 1. It contains a WITH clause, OR
    // 2. It has a GraphNode with the matching alias
    fn needs_processing(plan: &LogicalPlan, with_alias: &str, depth: usize) -> bool {
        if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
            log::warn!("needs_processing: depth limit {} exceeded", depth);
            return false;
        }
        let result = match plan {
            // A GraphNode is the structural target: this branch needs
            // processing iff its alias matches. Terminal by design — we
            // do NOT descend into node.input here (that narrowing is
            // intentional and preserved).
            LogicalPlan::GraphNode(node) => node.alias == with_alias,
            // Structural-wrapper variants: recurse into every direct
            // child looking for the target alias. Routed through the
            // exhaustive `children()` API instead of hand-listing each
            // child (this also makes nested GraphRel.center reachable,
            // which the caller clones verbatim, so the extra coverage is
            // behavior-neutral).
            LogicalPlan::WithClause(_)
            | LogicalPlan::GraphRel(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::GraphJoins(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Unwind(_)
            | LogicalPlan::CartesianProduct(_) => plan
                .children()
                .iter()
                .any(|child| needs_processing(child, with_alias, depth + 1)),
            // Any other variant: fall back to WITH-containment (the
            // original semantics — alias-matching does not descend
            // through these node types).
            _ => plan_contains_with_clause(plan),
        };
        log::debug!(
            "🔧 replace_v2: needs_processing({:?}, '{}') = {}",
            std::mem::discriminant(plan),
            with_alias,
            result
        );
        result
    }
    // Always recurse for WithClause - the WithClause case will handle replacement
    // Don't shortcut with is_innermost_with_clause check because the WithClause's input
    // might contain a GraphNode that needs updating from a previous iteration
    let new_left: Arc<LogicalPlan> = if plan_contains_with_clause(&graph_rel.left)
        || needs_processing(&graph_rel.left, with_alias, 0)
    {
        Arc::new(replace_with_clause_with_cte_reference_v2(
            &graph_rel.left,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        )?)
    } else {
        graph_rel.left.clone()
    };

    let new_right: Arc<LogicalPlan> = if plan_contains_with_clause(&graph_rel.right)
        || needs_processing(&graph_rel.right, with_alias, 0)
    {
        Arc::new(replace_with_clause_with_cte_reference_v2(
            &graph_rel.right,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        )?)
    } else {
        graph_rel.right.clone()
    };

    Ok(LogicalPlan::GraphRel(GraphRel {
        left: new_left,
        center: graph_rel.center.clone(),
        right: new_right,
        alias: graph_rel.alias.clone(),
        direction: graph_rel.direction.clone(),
        left_connection: graph_rel.left_connection.clone(),
        right_connection: graph_rel.right_connection.clone(),
        is_rel_anchor: graph_rel.is_rel_anchor,
        variable_length: graph_rel.variable_length.clone(),
        shortest_path_mode: graph_rel.shortest_path_mode.clone(),
        path_variable: graph_rel.path_variable.clone(),
        where_predicate: graph_rel.where_predicate.clone(),
        labels: graph_rel.labels.clone(),
        is_optional: graph_rel.is_optional,
        anchor_connection: graph_rel.anchor_connection.clone(),
        cte_references: std::collections::HashMap::new(),
        pattern_combinations: None,
        was_undirected: graph_rel.was_undirected,
        match_clause_index: graph_rel.match_clause_index, // #586
        optional_anchor_where: graph_rel.optional_anchor_where.clone(), // #597: preserve
    }))
}

/// Handle the `Union` arm of `replace_with_clause_with_cte_reference_v2`
/// (Phase-4 §7.1 extraction).
///
/// Recurse into each union branch and rebuild the `Union`. #593: a Cypher UNION
/// branch that has no WITH clause for `with_alias` is left untouched (rewriting
/// it would fold the untouched arm's alias into the OTHER arm's CTE — a cross-arm
/// contamination bug); a BidirectionalUnion (`is_cypher_union == false`) shares
/// one logical scope across branches so every branch is processed uniformly.
fn replace_v2_union_arm(
    union: &crate::query_planner::logical_plan::Union,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::Union;
    use std::sync::Arc;

    log::info!(
        "🔀 replace_v2: Processing Union with {} branches for alias '{}'",
        union.inputs.len(),
        with_alias
    );
    let new_inputs: Vec<Arc<LogicalPlan>> = union
        .inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            log::info!(
                "🔀 replace_v2: Processing Union branch {} type: {:?}",
                i,
                std::mem::discriminant(input.as_ref())
            );
            // #517: a genuine Cypher UNION's arms are INDEPENDENT
            // scopes — a WITH clause in one arm must never leak its
            // CTE substitution into a sibling arm, even when that
            // sibling reuses the same Cypher variable name (e.g.
            // `u` bound fresh in both arms of `MATCH (u) WITH u...
            // RETURN ... UNION MATCH (u) RETURN ...`). The
            // GraphNode-matching check below (`with_parts.contains
            // (&node.alias)`) is a plain by-name membership test
            // with no scope awareness, so recursing into every
            // branch unconditionally rewrites the untouched arm's
            // `u` into the OTHER arm's CTE reference (a duplicate-
            // alias self-join / cross-arm contamination bug).
            // BidirectionalUnion (`is_cypher_union == false`)
            // represents a single logical MATCH scope split purely
            // for SQL rendering, so every branch legitimately
            // shares the same WITH-derived scope there and must
            // keep being processed uniformly.
            if union.is_cypher_union
                && !find_all_with_clauses_grouped(input).contains_key(with_alias)
            {
                log::debug!(
                    "🔀 replace_v2: Cypher UNION branch {} has no WITH clause for key '{}' — leaving untouched",
                    i, with_alias
                );
                return Ok(input.clone());
            }
            replace_with_clause_with_cte_reference_v2(
                input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )
            .map(Arc::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    log::info!(
        "🔀 replace_v2: Union result has {} branches",
        new_inputs.len()
    );
    Ok(LogicalPlan::Union(Union {
        inputs: new_inputs,
        union_type: union.union_type.clone(),
        is_cypher_union: union.is_cypher_union,
    }))
}

/// Extract the node label from a plan tree by traversing through wrapper nodes.
/// Hoisted out of `replace_with_clause_with_cte_reference_v2` (Phase-4 §7.1);
/// renamed from `extract_node_label_from_plan` to avoid colliding with the
/// different-signature (`&LogicalPlan`) fn of that name in `cte_extraction.rs`.
fn extract_node_label_from_arc_plan(plan: &std::sync::Arc<LogicalPlan>) -> Option<String> {
    match plan.as_ref() {
        LogicalPlan::GraphNode(gn) => gn.label.clone(),
        LogicalPlan::Filter(f) => extract_node_label_from_arc_plan(&f.input),
        LogicalPlan::Projection(p) => extract_node_label_from_arc_plan(&p.input),
        LogicalPlan::WithClause(wc) => extract_node_label_from_arc_plan(&wc.input),
        _ => None,
    }
}

/// Handle the `WithClause` arm of `replace_with_clause_with_cte_reference_v2`
/// (Phase-4 §7.1 extraction).
///
/// If this WithClause is the innermost target (its key matches `with_alias` and
/// its input has no further nested WITH), replace it with a CTE reference
/// (preserving the underlying node label so VLP CTE extraction can determine the
/// start/end node type); if it is the target but still has nested WITH, or is a
/// non-target wrapper, recurse into the input and rebuild the WithClause.
fn replace_v2_with_clause_arm(
    wc: &crate::query_planner::logical_plan::WithClause,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use std::sync::Arc;

    // Generate key same way as find_all_with_clauses_grouped does
    let this_wc_key = with_clause_key(wc);
    let is_target_with = this_wc_key == with_alias;
    let has_nested = plan_contains_with_clause(&wc.input);
    log::debug!(
        "🔧 replace_v2: WithClause with key '{}', looking for '{}', is_target: {}, has_nested: {}",
        this_wc_key,
        with_alias,
        is_target_with,
        has_nested
    );

    if is_target_with && !plan_contains_with_clause(&wc.input) {
        // This is THE WithClause we're replacing, and it's innermost
        log::debug!(
            "🔧 replace_v2: FOUND AND REPLACING target innermost WithClause with key '{}' for alias '{}' with CTE '{}'",
            this_wc_key, with_alias, cte_name
        );
        log::debug!(
            "🔧 replace_v2: WithClause exported_aliases={:?}, input type={:?}",
            wc.exported_aliases,
            std::mem::discriminant(wc.input.as_ref())
        );
        let mut cte_ref = create_cte_reference(cte_name, with_alias, cte_schemas);
        // Preserve the original node label from the WithClause's underlying plan
        // so VLP CTE extraction can determine the start/end node type
        if let LogicalPlan::GraphNode(ref mut gn) = cte_ref {
            gn.label = extract_node_label_from_arc_plan(&wc.input);
        }
        Ok(cte_ref)
    } else if is_target_with {
        // This is THE WithClause, but it has nested WITH clauses - error case
        // (We should be processing inner ones first)
        log::debug!(
            "🔧 replace_v2: Target WithClause has nested WITH - should process inner first!"
        );
        let new_input = replace_with_clause_with_cte_reference_v2(
            &wc.input,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        )?;

        // DISABLED: Don't collapse passthrough WITHs here (same reason as above)
        // Let the iteration loop handle them properly
        //
        // // Check if after recursion, the new_input is a CTE reference
        // // and this WITH is a simple passthrough - if so, collapse it
        // if is_simple_cte_passthrough(&new_input, wc) {
        //     log::debug!(
        //         "🔧 replace_v2: Collapsing passthrough WithClause to CTE reference"
        //     );
        //     return Ok(new_input);
        // }

        log::debug!(
            "🔧 DEBUG replace_v2: Creating new outer WithClause with wc.cte_references = {:?}",
            wc.cte_references
        );

        Ok(LogicalPlan::WithClause(
            wc.with_new_input(Arc::new(new_input)),
        ))
    } else {
        // This is NOT the WithClause we're looking for, but we need to recurse
        // to find and replace the inner one
        log::debug!(
            "🔧 replace_v2: Not target WithClause (key='{}') - recursing into input to find '{}'",
            this_wc_key,
            with_alias
        );
        log::debug!(
            "🔧 DEBUG replace_v2: outer wc.cte_references = {:?}",
            wc.cte_references
        );
        let new_input = replace_with_clause_with_cte_reference_v2(
            &wc.input,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        )?;

        // DISABLED: Don't collapse passthrough WITHs here.
        // Instead, let the iteration loop handle them. When the outer WITH
        // is processed in the next iteration, its cte_references will tell us
        // the CTE name to use, and we can properly handle expression remapping.
        //
        // Previously, collapsing here caused expressions that reference the
        // collapsed WITH's CTE name to become stale (the CTE was never created).
        //
        // // Check if after recursion, the new_input is a CTE reference
        // // and this WITH is a simple passthrough - if so, collapse it
        // if is_simple_cte_passthrough(&new_input, wc) {
        //     log::debug!("🔧 replace_v2: Collapsing passthrough WithClause (not target) to CTE reference");
        //     return Ok(new_input);
        // }

        Ok(LogicalPlan::WithClause(
            wc.with_new_input(Arc::new(new_input)),
        ))
    }
}

pub(crate) fn replace_with_clause_with_cte_reference_v2(
    plan: &LogicalPlan,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    log::debug!(
        "🔧 replace_v2: Processing plan type {:?} for alias '{}'",
        std::mem::discriminant(plan),
        with_alias
    );

    match plan {
        // NEW: Handle WithClause type
        // Key insight: Check if this WithClause's generated key matches the alias we're looking for
        LogicalPlan::WithClause(wc) => {
            replace_v2_with_clause_arm(wc, with_alias, cte_name, pre_with_aliases, cte_schemas)
        }

        LogicalPlan::GraphRel(graph_rel) => replace_v2_graph_rel_arm(
            graph_rel,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        ),

        LogicalPlan::Projection(proj) => {
            replace_v2_projection_arm(proj, with_alias, cte_name, pre_with_aliases, cte_schemas)
        }

        LogicalPlan::Filter(filter) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &filter.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: filter.predicate.clone(),
            }))
        }

        LogicalPlan::GroupBy(group_by) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &group_by.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: group_by.expressions.clone(),
                having_clause: group_by.having_clause.clone(),
                is_materialization_boundary: group_by.is_materialization_boundary,
                exposed_alias: group_by.exposed_alias.clone(),
            }))
        }

        LogicalPlan::GraphJoins(graph_joins) => replace_v2_graph_joins_arm(
            graph_joins,
            with_alias,
            cte_name,
            pre_with_aliases,
            cte_schemas,
        ),

        LogicalPlan::Limit(limit) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &limit.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: limit.count,
            }))
        }

        LogicalPlan::OrderBy(order_by) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &order_by.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: order_by.items.clone(),
            }))
        }

        LogicalPlan::Skip(skip) => {
            let new_input = replace_with_clause_with_cte_reference_v2(
                &skip.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }

        LogicalPlan::Union(union) => {
            replace_v2_union_arm(union, with_alias, cte_name, pre_with_aliases, cte_schemas)
        }

        LogicalPlan::GraphNode(node) => {
            // CRITICAL FIX: Check if this GraphNode's alias is exported from the CTE
            // This handles patterns like: WITH a, b ... MATCH (b)-[]->(c)
            // where 'b' should come from the CTE, not a fresh table scan

            // First recurse into the input to handle nested structures
            let new_input = replace_with_clause_with_cte_reference_v2(
                &node.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?;

            // Check if this node's alias matches an exported alias from the CTE
            // For composite aliases like "friend_post", we need to check all parts
            let with_parts: Vec<&str> = with_alias.split('_').collect();
            let node_matches_cte = with_parts.contains(&node.alias.as_str());

            if node_matches_cte {
                log::debug!(
                    "🔧 replace_v2: GraphNode '{}' matches CTE exported alias '{}' - replacing with CTE reference '{}'",
                    node.alias, with_alias, cte_name
                );

                // Replace this GraphNode with a CTE reference
                // The CTE contains all the columns for the exported aliases
                Ok(create_cte_reference(cte_name, &node.alias, cte_schemas))
            } else {
                log::debug!(
                    "🔧 replace_v2: GraphNode '{}' does NOT match CTE - keeping with recursed input",
                    node.alias
                );
                // This GraphNode doesn't match - keep it but use the recursed input
                Ok(LogicalPlan::GraphNode(GraphNode {
                    input: Arc::new(new_input),
                    alias: node.alias.clone(),
                    label: node.label.clone(),
                    is_denormalized: node.is_denormalized,
                    projected_columns: None,
                    node_types: None,
                }))
            }
        }

        LogicalPlan::CartesianProduct(cp) => {
            // CartesianProduct is used for WITH...MATCH patterns where aliases don't overlap
            // Recurse into both sides to replace WITH clauses
            log::debug!(
                "🔧 replace_v2: Processing CartesianProduct - recursing into left and right"
            );
            let new_left = Arc::new(replace_with_clause_with_cte_reference_v2(
                &cp.left,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            let new_right = Arc::new(replace_with_clause_with_cte_reference_v2(
                &cp.right,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            Ok(LogicalPlan::CartesianProduct(CartesianProduct {
                left: new_left,
                right: new_right,
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }

        LogicalPlan::Unwind(unwind) => {
            let new_input = Arc::new(replace_with_clause_with_cte_reference_v2(
                &unwind.input,
                with_alias,
                cte_name,
                pre_with_aliases,
                cte_schemas,
            )?);
            Ok(LogicalPlan::Unwind(Unwind {
                input: new_input,
                expression: unwind.expression.clone(),
                alias: unwind.alias.clone(),
                label: unwind.label.clone(),
                tuple_properties: unwind.tuple_properties.clone(),
            }))
        }

        other => Ok(other.clone()),
    }
}

/// Find the INNERMOST WITH clause subplan in a nested plan structure.
///
/// KEY INSIGHT: With chained WITH clauses (e.g., WITH a MATCH...WITH a,b MATCH...),
/// we need to process them from innermost to outermost. The innermost WITH is
/// the one whose INPUT has NO other WITH clauses nested inside it.
///
/// This function recursively searches for WITH clauses and returns the one
/// whose input is "clean" (contains no nested WITH).
///
/// Returns (with_clause_plan, alias_name) if found.
///
/// Find all WITH clauses in a plan grouped by their alias.
///
/// Returns HashMap where each alias maps to all WITH clause plans with that alias.
/// This handles the case where Union branches each have their own WITH clause with the same alias.
/// Returns owned (cloned) LogicalPlans to avoid lifetime issues with mutations.
pub(crate) fn find_all_with_clauses_grouped(
    plan: &LogicalPlan,
) -> std::collections::HashMap<String, Vec<LogicalPlan>> {
    log::debug!(
        "🔍 find_all_with_clauses_grouped: Called with plan type: {:?}",
        std::mem::discriminant(plan)
    );
    use std::collections::HashMap;

    /// Find the first WITH clause key in a plan subtree (non-recursive into Union)
    fn find_first_with_key(plan: &LogicalPlan) -> Option<String> {
        log::debug!(
            "🔍 find_first_with_key: plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            // NEW: Handle WithClause type
            LogicalPlan::WithClause(wc) => Some(with_clause_key(wc)),
            LogicalPlan::GraphRel(graph_rel) => {
                // Check for WithClause in right
                if let LogicalPlan::WithClause(wc) = graph_rel.right.as_ref() {
                    return Some(with_clause_key(wc));
                }
                // Check for WithClause in left
                if let LogicalPlan::WithClause(wc) = graph_rel.left.as_ref() {
                    return Some(with_clause_key(wc));
                }
                if let LogicalPlan::GraphJoins(gj) = graph_rel.right.as_ref() {
                    if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                        return Some(with_clause_key(wc));
                    }
                }
                None
            }
            LogicalPlan::GraphJoins(gj) => find_first_with_key(&gj.input),
            LogicalPlan::Projection(p) => find_first_with_key(&p.input),
            LogicalPlan::Filter(f) => find_first_with_key(&f.input),
            _ => None,
        }
    }

    fn find_all_with_clauses_impl(
        plan: &LogicalPlan,
        results: &mut Vec<(LogicalPlan, String)>,
        depth: usize,
    ) {
        if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
            log::warn!(
                "find_all_with_clauses_impl: depth limit {} exceeded, stopping traversal",
                depth
            );
            return;
        }
        log::debug!(
            "🔍 find_all_with_clauses_impl: Checking plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            // NEW: Handle WithClause type directly
            LogicalPlan::WithClause(wc) => {
                let alias = with_clause_key(wc);
                log::debug!(
                    "🔍 find_all_with_clauses_impl: Found WithClause directly, key='{}'",
                    alias
                );
                results.push((plan.clone(), alias));
                // Recurse into input to find nested WITH clauses
                // They will be processed innermost-first due to sorting by underscore count
                find_all_with_clauses_impl(&wc.input, results, depth + 1);
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::debug!(
                    "🔍 find_all_with_clauses_impl: GraphRel - right type: {:?}, left type: {:?}",
                    std::mem::discriminant(graph_rel.right.as_ref()),
                    std::mem::discriminant(graph_rel.left.as_ref())
                );

                // Track which branches we've already recursed into to avoid duplicates
                let mut handled_right = false;
                let mut handled_left = false;

                // Check for WithClause in right
                if let LogicalPlan::WithClause(wc) = graph_rel.right.as_ref() {
                    let key = with_clause_key(wc);
                    let alias = if key == "with_var" {
                        graph_rel.right_connection.clone()
                    } else {
                        key
                    };
                    log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphRel.right, key='{}' (connection='{}')",
                               alias, graph_rel.right_connection);
                    results.push((graph_rel.right.as_ref().clone(), alias));
                    find_all_with_clauses_impl(&wc.input, results, depth + 1);
                    handled_right = true;
                }
                // Check for WithClause in left
                if let LogicalPlan::WithClause(wc) = graph_rel.left.as_ref() {
                    let key = with_clause_key(wc);
                    let alias = if key == "with_var" {
                        graph_rel.left_connection.clone()
                    } else {
                        key
                    };
                    log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphRel.left, key='{}' (connection='{}')",
                               alias, graph_rel.left_connection);
                    results.push((graph_rel.left.as_ref().clone(), alias));
                    find_all_with_clauses_impl(&wc.input, results, depth + 1);
                    handled_left = true;
                }
                // Also check GraphJoins wrapped inside GraphRel
                if !handled_right {
                    if let LogicalPlan::GraphJoins(gj) = graph_rel.right.as_ref() {
                        if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                            let key = with_clause_key(wc);
                            let alias = if key == "with_var" {
                                graph_rel.right_connection.clone()
                            } else {
                                key
                            };
                            log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphJoins inside GraphRel.right, key='{}' (connection='{}')",
                                       alias, graph_rel.right_connection);
                            results.push((gj.input.as_ref().clone(), alias));
                            find_all_with_clauses_impl(&wc.input, results, depth + 1);
                            handled_right = true;
                        }
                    }
                }
                if !handled_left {
                    if let LogicalPlan::GraphJoins(gj) = graph_rel.left.as_ref() {
                        if let LogicalPlan::WithClause(wc) = gj.input.as_ref() {
                            let key = with_clause_key(wc);
                            let alias = if key == "with_var" {
                                graph_rel.left_connection.clone()
                            } else {
                                key
                            };
                            log::debug!("🔍 find_all_with_clauses_impl: Found WithClause in GraphJoins inside GraphRel.left, key='{}' (connection='{}')",
                                       alias, graph_rel.left_connection);
                            results.push((gj.input.as_ref().clone(), alias));
                            find_all_with_clauses_impl(&wc.input, results, depth + 1);
                            handled_left = true;
                        }
                    }
                }

                // Continue traversal on branches not already handled
                if !handled_left {
                    find_all_with_clauses_impl(&graph_rel.left, results, depth + 1);
                }
                find_all_with_clauses_impl(&graph_rel.center, results, depth + 1);
                if !handled_right {
                    find_all_with_clauses_impl(&graph_rel.right, results, depth + 1);
                }
            }
            LogicalPlan::Union(union) => {
                // For Union (bidirectional patterns), check if WITH clauses exist inside.
                // If so, the entire Union should be treated as a single WITH-bearing structure,
                // not collected multiple times from each branch.
                //
                // Strategy: Check if all branches have matching WITH clauses (same key).
                // If yes, collect the WITH key but note that the Union itself needs to be rendered.
                // If branches have different WITH structures, recurse into each.

                let mut branch_with_keys: Vec<Option<String>> = Vec::new();
                for (i, input) in union.inputs.iter().enumerate() {
                    log::debug!(
                        "🔍 find_all_with_clauses_impl: Union branch {} plan type: {:?}",
                        i,
                        std::mem::discriminant(input.as_ref())
                    );
                    // Find the first Projection(With) in this branch
                    if let Some(key) = find_first_with_key(input) {
                        branch_with_keys.push(Some(key));
                    } else {
                        branch_with_keys.push(None);
                    }
                }

                // Check if all branches have the same WITH key
                let first_key = branch_with_keys.first().and_then(|k| k.clone());
                let all_same = branch_with_keys.iter().all(|k| k == &first_key);

                if all_same {
                    if let Some(key) = first_key.as_ref() {
                        // All branches have the same WITH key - this is a bidirectional pattern
                        // Collect from just the first branch to avoid duplicates
                        // The Union structure will be preserved when we render the parent GraphRel
                        log::debug!("🔍 find_all_with_clauses_impl: Union has matching WITH key '{}' in all branches, collecting from first only", key);
                        if let Some(first_input) = union.inputs.first() {
                            find_all_with_clauses_impl(first_input, results, depth + 1);
                        }
                    } else {
                        // All branches have None key — WITH clauses may be deeper in the tree
                        // Recurse into the first branch to find them
                        log::debug!("🔍 find_all_with_clauses_impl: Union branches have no top-level WITH key, recursing into first branch");
                        if let Some(first_input) = union.inputs.first() {
                            find_all_with_clauses_impl(first_input, results, depth + 1);
                        }
                    }
                } else {
                    // Branches have different WITH structures - recurse into each
                    for input in &union.inputs {
                        find_all_with_clauses_impl(input, results, depth + 1);
                    }
                }
            }
            // All other variants carry no special WITH-collection logic — they
            // simply recurse into every direct child. Route through the
            // exhaustive `children()` API so the traversal can never drift out
            // of sync with the plan structure (covers Projection, Filter,
            // GroupBy, GraphJoins, Limit, OrderBy, Skip, CartesianProduct,
            // ViewScan.input, GraphNode, Cte, Unwind, and write-op inputs;
            // leaves are a no-op). WithClause / GraphRel / Union are handled
            // above because they compute WITH keys / dedup, so they never reach
            // this arm.
            other => {
                other.for_each_child(|child| find_all_with_clauses_impl(child, results, depth + 1))
            }
        }
    }

    let mut all_withs: Vec<(LogicalPlan, String)> = Vec::new();
    find_all_with_clauses_impl(plan, &mut all_withs, 0);

    // Group by alias
    let mut grouped: HashMap<String, Vec<LogicalPlan>> = HashMap::new();
    for (plan, alias) in all_withs {
        grouped.entry(alias).or_default().push(plan);
    }

    grouped
}

/// Collapse a passthrough WITH clause by replacing it with its input.
/// A passthrough WITH is one that simply wraps a CTE reference without any transformations:
/// - Single item that's just a TableAlias
/// - No DISTINCT, ORDER BY, SKIP, LIMIT, WHERE
///
/// This function finds the passthrough WITH for the given alias and replaces it with its input.
/// Uses the analyzer's CTE name to distinguish between multiple consecutive WITHs with same alias.
pub(crate) fn collapse_passthrough_with(
    plan: &LogicalPlan,
    target_alias: &str,
    target_cte_name: &str, // Analyzer's CTE name (e.g., "with_lnm_cte_4")
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    log::debug!(
        "🔧 collapse_passthrough_with: ENTERING with plan type {:?}, target_alias='{}', target_cte_name='{}'",
        std::mem::discriminant(plan), target_alias, target_cte_name
    );

    match plan {
        LogicalPlan::WithClause(wc) => {
            let key = with_clause_key(wc);
            let this_cte_name = wc
                .cte_references
                .get(target_alias)
                .map(|s| s.as_str())
                .unwrap_or("");
            log::debug!(
                "🔧 collapse_passthrough_with: ENTERING WithClause match, wc.cte_references={:?}, exported_aliases={:?}",
                wc.cte_references, wc.exported_aliases
            );
            log::debug!(
                "🔧 collapse_passthrough_with: Checking WithClause key='{}' target='{}' this_cte='{}' target_cte='{}'",
                key, target_alias, this_cte_name, target_cte_name
            );
            if key == target_alias {
                // FORCE COLLAPSE for passthrough WITHs
                Ok(wc.input.as_ref().clone())
            } else {
                // Not the target - recurse into input
                let new_input =
                    collapse_passthrough_with(&wc.input, target_alias, target_cte_name)?;
                Ok(LogicalPlan::WithClause(
                    wc.with_new_input(Arc::new(new_input)),
                ))
            }
        }
        LogicalPlan::Projection(proj) => {
            let new_input = collapse_passthrough_with(&proj.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: proj.items.clone(),
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::Filter(f) => {
            let new_input = collapse_passthrough_with(&f.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: f.predicate.clone(),
            }))
        }
        LogicalPlan::Limit(lim) => {
            let new_input = collapse_passthrough_with(&lim.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: lim.count,
            }))
        }
        LogicalPlan::GraphJoins(gj) => {
            let new_input = collapse_passthrough_with(&gj.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: gj.joins.clone(),
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: gj.anchor_table.clone(),
                cte_references: gj.cte_references.clone(),
                correlation_predicates: gj.correlation_predicates.clone(),
            }))
        }
        LogicalPlan::Skip(skip) => {
            let new_input = collapse_passthrough_with(&skip.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }
        LogicalPlan::OrderBy(ob) => {
            let new_input = collapse_passthrough_with(&ob.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: ob.items.clone(),
            }))
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input = collapse_passthrough_with(&gb.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: gb.expressions.clone(),
                having_clause: gb.having_clause.clone(),
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            }))
        }
        LogicalPlan::GraphRel(gr) => {
            let new_left = collapse_passthrough_with(&gr.left, target_alias, target_cte_name)?;
            let new_right = collapse_passthrough_with(&gr.right, target_alias, target_cte_name)?;
            Ok(LogicalPlan::GraphRel(GraphRel {
                left: Arc::new(new_left),
                center: gr.center.clone(),
                right: Arc::new(new_right),
                alias: gr.alias.clone(),
                direction: gr.direction.clone(),
                left_connection: gr.left_connection.clone(),
                right_connection: gr.right_connection.clone(),
                is_rel_anchor: gr.is_rel_anchor,
                variable_length: gr.variable_length.clone(),
                shortest_path_mode: gr.shortest_path_mode.clone(),
                path_variable: gr.path_variable.clone(),
                where_predicate: gr.where_predicate.clone(),
                labels: gr.labels.clone(),
                is_optional: gr.is_optional,
                anchor_connection: gr.anchor_connection.clone(),
                cte_references: gr.cte_references.clone(),
                pattern_combinations: gr.pattern_combinations.clone(),
                was_undirected: gr.was_undirected,
                match_clause_index: gr.match_clause_index, // #586
                optional_anchor_where: gr.optional_anchor_where.clone(), // #597: preserve
            }))
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left = collapse_passthrough_with(&cp.left, target_alias, target_cte_name)?;
            let new_right = collapse_passthrough_with(&cp.right, target_alias, target_cte_name)?;
            Ok(LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }
        LogicalPlan::Union(u) => {
            let new_inputs = u
                .inputs
                .iter()
                .map(|i| collapse_passthrough_with(i, target_alias, target_cte_name).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: u.union_type.clone(),
                is_cypher_union: u.is_cypher_union,
            }))
        }
        LogicalPlan::Unwind(uw) => {
            let new_input = collapse_passthrough_with(&uw.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Unwind(Unwind {
                input: Arc::new(new_input),
                expression: uw.expression.clone(),
                alias: uw.alias.clone(),
                label: uw.label.clone(),
                tuple_properties: uw.tuple_properties.clone(),
            }))
        }
        LogicalPlan::Cte(c) => {
            let new_input = collapse_passthrough_with(&c.input, target_alias, target_cte_name)?;
            Ok(LogicalPlan::Cte(Cte {
                input: Arc::new(new_input),
                name: c.name.clone(),
            }))
        }
        // For other node types (leaves) return unchanged
        other => Ok(other.clone()),
    }
}

/// Find a `GraphNode` with the given `alias` anywhere in the plan and report
/// whether it is a *concrete, resolvable* node — i.e. it carries an explicit
/// label. Unlabeled endpoints (e.g. the `(o)` in a Neo4j-Browser `(a)--(o)`
/// expand) are the separate browser/denorm-foreign-edge bug family (see the
/// `browser-*` and `denorm-foreign-edge` notes) whose correlation resolution is
/// not solved here; requiring a label on BOTH endpoints keeps the CTE-join
/// hardening scoped to the labeled node-to-node family this fix addresses.
fn node_is_concrete_labeled(plan: &LogicalPlan, alias: &str) -> bool {
    use crate::query_planner::logical_plan::LogicalPlan;
    match plan {
        LogicalPlan::GraphNode(gn) if gn.alias == alias => gn.label.is_some(),
        LogicalPlan::GraphNode(gn) => node_is_concrete_labeled(&gn.input, alias),
        LogicalPlan::GraphRel(gr) => {
            node_is_concrete_labeled(&gr.left, alias)
                || node_is_concrete_labeled(&gr.center, alias)
                || node_is_concrete_labeled(&gr.right, alias)
        }
        LogicalPlan::GraphJoins(gj) => node_is_concrete_labeled(&gj.input, alias),
        LogicalPlan::Projection(p) => node_is_concrete_labeled(&p.input, alias),
        LogicalPlan::Filter(f) => node_is_concrete_labeled(&f.input, alias),
        LogicalPlan::GroupBy(g) => node_is_concrete_labeled(&g.input, alias),
        LogicalPlan::OrderBy(o) => node_is_concrete_labeled(&o.input, alias),
        LogicalPlan::Skip(s) => node_is_concrete_labeled(&s.input, alias),
        LogicalPlan::Limit(l) => node_is_concrete_labeled(&l.input, alias),
        LogicalPlan::Unwind(u) => node_is_concrete_labeled(&u.input, alias),
        LogicalPlan::WithClause(w) => node_is_concrete_labeled(&w.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            node_is_concrete_labeled(&cp.left, alias) || node_is_concrete_labeled(&cp.right, alias)
        }
        LogicalPlan::Union(u) => u.inputs.iter().any(|i| node_is_concrete_labeled(i, alias)),
        _ => false,
    }
}

/// Returns true if `alias` is joined by a *resolvable* graph-pattern edge to a
/// distinct node — a non-VLP `GraphRel` whose BOTH endpoints are concrete,
/// labeled nodes (the check is label presence only; denormalized nodes with a
/// label pass, but in practice denormalized patterns are transformed before
/// reaching the fallback this guards). When true, a WITH-CTE JOIN to `alias`
/// MUST carry a real ON condition; a cartesian `ON 1 = 1` would silently change
/// the query's semantics (wrong row count), so the renderer errors instead of
/// emitting it. Deliberately narrow: a scalar carry-forward (`WITH count(*)`)
/// has no such edge, and the unlabeled-endpoint browser family is intentionally
/// excluded (its correlation gaps predate and are out of scope for this fix —
/// see #451 scope note). Used to harden the CTE-JOIN fallback.
// P2.6 slice 3: widened `fn` → `pub(crate) fn` (the sole change vs the original)
// so the transition re-export in `plan_builder_utils` reaches it — it is called
// from `build_chained_with_match_cte_plan`, which still lives there; body is
// verbatim.
pub(crate) fn alias_has_pattern_correlation(root: &LogicalPlan, alias: &str) -> bool {
    fn walk(node: &LogicalPlan, root: &LogicalPlan, alias: &str) -> bool {
        use crate::query_planner::logical_plan::LogicalPlan;
        match node {
            LogicalPlan::GraphRel(gr) => {
                let l = gr.left_connection.as_str();
                let r = gr.right_connection.as_str();
                let counterpart = if l == alias {
                    Some(r)
                } else if r == alias {
                    Some(l)
                } else {
                    None
                };
                if let Some(cp) = counterpart {
                    if cp != alias
                        && !cp.is_empty()
                        && gr.variable_length.is_none()
                        && node_is_concrete_labeled(root, alias)
                        && node_is_concrete_labeled(root, cp)
                    {
                        return true;
                    }
                }
                walk(&gr.left, root, alias)
                    || walk(&gr.center, root, alias)
                    || walk(&gr.right, root, alias)
            }
            LogicalPlan::GraphJoins(gj) => walk(&gj.input, root, alias),
            LogicalPlan::Projection(p) => walk(&p.input, root, alias),
            LogicalPlan::Filter(f) => walk(&f.input, root, alias),
            LogicalPlan::GraphNode(gn) => walk(&gn.input, root, alias),
            LogicalPlan::GroupBy(g) => walk(&g.input, root, alias),
            LogicalPlan::OrderBy(o) => walk(&o.input, root, alias),
            LogicalPlan::Skip(s) => walk(&s.input, root, alias),
            LogicalPlan::Limit(l) => walk(&l.input, root, alias),
            LogicalPlan::Unwind(u) => walk(&u.input, root, alias),
            LogicalPlan::WithClause(w) => walk(&w.input, root, alias),
            LogicalPlan::CartesianProduct(cp) => {
                walk(&cp.left, root, alias) || walk(&cp.right, root, alias)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| walk(i, root, alias)),
            _ => false,
        }
    }
    walk(root, root, alias)
}

/// When we have a query like:
///   WITH a MATCH (a)-[:F]->(b) WITH a,b MATCH (b)-[:F]->(c)
///
/// After processing, we have:
/// - CTE: with_a_b_cte2 (contains the pattern for a→b)
/// - Final plan: GraphJoins with joins for [a→t1→b, b→t2→c]
///
/// The joins [a→t1→b] are already materialized in the CTE, so they should be removed.
/// Only [b→t2→c] should remain in the final query.
///
/// This function:
/// 1. Traverses the plan to find GraphJoins nodes
/// 2. Builds an adjacency graph from join ON conditions (alias connectivity)
/// 3. Seeds the removable set with CTE-backed aliases (exported_aliases)
/// 4. Fixed-point expansion: non-CTE joins are removable if ALL neighbors are removable
/// 5. Keeps joins whose alias is NOT in the removable set
pub(crate) fn prune_joins_covered_by_cte(
    plan: &LogicalPlan,
    cte_name: &str,
    exported_aliases: &std::collections::HashSet<&str>,
    _cte_schemas: &crate::render_plan::CteSchemas,
    removed_correlations: &mut Vec<crate::query_planner::logical_expr::LogicalExpr>,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    log::info!(
        "🔧 prune_joins_covered_by_cte: Processing plan for CTE '{}' with aliases {:?}",
        cte_name,
        exported_aliases
    );

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::info!(
                "🔧 prune_joins_covered_by_cte: Found GraphJoins with {} joins and anchor '{:?}'",
                gj.joins.len(),
                gj.anchor_table
            );

            // Build adjacency graph from join ON conditions, then use fixed-point
            // expansion to find all joins fully internal to the CTE subgraph.

            // Helper: extract table aliases from join condition operands
            fn extract_condition_aliases(
                operands: &[crate::query_planner::logical_expr::LogicalExpr],
                aliases: &mut std::collections::HashSet<String>,
            ) {
                for operand in operands {
                    match operand {
                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) => {
                            aliases.insert(pa.table_alias.0.clone());
                        }
                        crate::query_planner::logical_expr::LogicalExpr::OperatorApplicationExp(
                            nested,
                        ) => {
                            extract_condition_aliases(&nested.operands, aliases);
                        }
                        _ => {}
                    }
                }
            }

            // 1. Build adjacency graph from join conditions
            let mut adjacency: std::collections::HashMap<
                String,
                std::collections::HashSet<String>,
            > = std::collections::HashMap::new();

            // Register all join aliases and anchor
            if let Some(ref anchor) = gj.anchor_table {
                adjacency.entry(anchor.clone()).or_default();
            }
            for join in &gj.joins {
                adjacency.entry(join.table_alias.clone()).or_default();
            }

            // Add edges from join conditions
            for join in &gj.joins {
                let mut condition_aliases = std::collections::HashSet::new();
                for op in &join.joining_on {
                    extract_condition_aliases(&op.operands, &mut condition_aliases);
                }
                // Add bidirectional edges between join alias and all aliases in its conditions
                for alias in &condition_aliases {
                    if alias != &join.table_alias {
                        adjacency
                            .entry(join.table_alias.clone())
                            .or_default()
                            .insert(alias.clone());
                        adjacency
                            .entry(alias.clone())
                            .or_default()
                            .insert(join.table_alias.clone());
                    }
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Adjacency graph: {:?}",
                adjacency
            );

            // 2. Seed removable set with CTE-backed aliases
            let mut removable = std::collections::HashSet::new();
            for join in &gj.joins {
                if exported_aliases.contains(join.table_alias.as_str()) {
                    removable.insert(join.table_alias.clone());
                }
            }
            if let Some(ref anchor) = gj.anchor_table {
                if exported_aliases.contains(anchor.as_str()) {
                    removable.insert(anchor.clone());
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Initial removable set (CTE-backed): {:?}",
                removable
            );

            // 3. Fixed-point expansion: a non-CTE join is removable if ALL its neighbors
            //    are already removable
            //
            //    #461 investigation note (R4): tried tightening this to require
            //    2+ neighbors (reasoning: a join with exactly ONE neighbor that
            //    happens to be CTE-exported isn't necessarily already inside the
            //    CTE — it can be a fresh, post-WITH join hanging directly off a
            //    single-alias anchor, e.g. two sibling patterns sharing anchor
            //    `c`: `MATCH (c) WITH c MATCH (o)-[:PLACED_BY]->(c) OPTIONAL
            //    MATCH (o2)-[:PLACED_BY]->(c) ...` swept BOTH `o` and `o2` out of
            //    the final query even though neither was ever inside the CTE —
            //    #461 shape 1's actual over-pruning bug). That tightening DID fix
            //    the over-pruning for the star/branch shape above, but broke the
            //    single-OPTIONAL-pattern case this loop is ALSO relied on for
            //    (#453/#460/#462/#472/#473's family, e.g. `WITH c OPTIONAL MATCH
            //    (o)-[:PLACED_BY]->(c) ...`): there, the sole post-WITH `o` join
            //    similarly has exactly ONE neighbor (`c`) and genuinely MUST be
            //    pruned here — `build_chained_with_match_cte_plan`'s
            //    `post_with_optional_restructure` (#453) independently rebuilds
            //    the optional JOIN from `render_plan.from`/correlation predicates
            //    and expects `prune_joins_covered_by_cte` to have already removed
            //    the raw `o` join entry; leaving it in place breaks that
            //    restructure. Reverted — the fix needs to distinguish "single
            //    fresh join hanging off the anchor" (prune — #453 rebuilds it)
            //    from "MULTIPLE sibling fresh joins hanging off the same anchor"
            //    (don't prune any of them — #461 shape 1), not a blanket
            //    neighbor-count threshold. Deferred; see the `fk_edge_461_*`
            //    characterization test for the current (unfixed) state.
            loop {
                let mut changed = false;
                for join in &gj.joins {
                    if removable.contains(&join.table_alias) {
                        continue;
                    }
                    if let Some(neighbors) = adjacency.get(&join.table_alias) {
                        if !neighbors.is_empty() && neighbors.iter().all(|n| removable.contains(n))
                        {
                            removable.insert(join.table_alias.clone());
                            changed = true;
                        }
                    }
                }
                if !changed {
                    break;
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Final removable set: {:?}",
                removable
            );

            // 4. Partition joins into kept/removed
            let mut kept_joins = Vec::new();
            let mut removed_joins = Vec::new();
            for (idx, join) in gj.joins.iter().enumerate() {
                if removable.contains(&join.table_alias) {
                    log::debug!(
                        "🔧 prune_joins_covered_by_cte: REMOVING join {} to '{}'",
                        idx,
                        join.table_alias
                    );
                    // Capture cross-barrier correlations: a removed join whose ON
                    // condition references at least one NON-removable (fresh, post-WITH)
                    // alias is a graph-pattern correlation between the CTE and a fresh
                    // node (e.g. an FK-edge `c.customer_id = o.customer_id`). It is NOT
                    // reproduced by `original_correlation_predicates` (which only carries
                    // explicit WHERE-style predicates), so without capturing it here the
                    // CTE JOIN would degrade to a cartesian `ON 1 = 1`. #451
                    for op in &join.joining_on {
                        let mut cond_aliases = std::collections::HashSet::new();
                        extract_condition_aliases(&op.operands, &mut cond_aliases);
                        // Cross-barrier iff the condition ties a CTE-exported alias to a
                        // fresh (non-exported) alias. Compare against `exported_aliases`,
                        // NOT `removable` — the fixed-point expansion may have pulled the
                        // fresh endpoint into `removable`, which would mask the correlation.
                        let references_exported = cond_aliases
                            .iter()
                            .any(|a| exported_aliases.contains(a.as_str()));
                        let references_fresh = cond_aliases
                            .iter()
                            .any(|a| !exported_aliases.contains(a.as_str()));
                        if references_exported && references_fresh {
                            log::info!(
                                "🔧 prune_joins_covered_by_cte: Capturing cross-barrier correlation from removed join '{}': {:?}",
                                join.table_alias,
                                op
                            );
                            removed_correlations.push(
                                crate::query_planner::logical_expr::LogicalExpr::OperatorApplicationExp(
                                    op.clone(),
                                ),
                            );
                        }
                    }
                    removed_joins.push(join.clone());
                } else {
                    log::info!(
                        "🔧 prune_joins_covered_by_cte: KEEPING join {} to '{}'",
                        idx,
                        join.table_alias
                    );
                    kept_joins.push(join.clone());
                }
            }

            log::info!(
                "🔧 prune_joins_covered_by_cte: Kept {} joins, removed {} joins",
                kept_joins.len(),
                removed_joins.len()
            );

            // If we removed joins, update the anchor_table to use the GraphNode alias that references the CTE
            // The anchor should be the alias of the GraphNode whose ViewScan.source_table matches cte_name
            let new_anchor = if !removed_joins.is_empty() {
                // Find the GraphNode that references this CTE
                if let Some(cte_ref_alias) = find_cte_reference_alias(&gj.input, cte_name) {
                    log::debug!("🔧 prune_joins_covered_by_cte: Updating anchor from '{:?}' to CTE reference alias '{}'",
                               gj.anchor_table, cte_ref_alias);
                    Some(cte_ref_alias)
                } else {
                    log::debug!("🔧 prune_joins_covered_by_cte: Could not find GraphNode referencing CTE '{}'", cte_name);
                    gj.anchor_table.clone()
                }
            } else {
                gj.anchor_table.clone()
            };

            // Recursively process the input
            let new_input = prune_joins_covered_by_cte(
                &gj.input,
                cte_name,
                exported_aliases,
                _cte_schemas,
                removed_correlations,
            )?;

            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: kept_joins,
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: new_anchor,
                cte_references: gj.cte_references.clone(),
                correlation_predicates: vec![],
            }))
        }
        LogicalPlan::Projection(proj) => {
            let new_input = prune_joins_covered_by_cte(
                &proj.input,
                cte_name,
                exported_aliases,
                _cte_schemas,
                removed_correlations,
            )?;
            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: proj.items.clone(),
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::Limit(limit) => {
            let new_input = prune_joins_covered_by_cte(
                &limit.input,
                cte_name,
                exported_aliases,
                _cte_schemas,
                removed_correlations,
            )?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: limit.count,
            }))
        }
        LogicalPlan::OrderBy(order) => {
            let new_input = prune_joins_covered_by_cte(
                &order.input,
                cte_name,
                exported_aliases,
                _cte_schemas,
                removed_correlations,
            )?;
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: order.items.clone(),
            }))
        }
        _ => {
            log::debug!(
                "🔧 prune_joins_covered_by_cte: No pruning needed for plan type {:?}",
                std::mem::discriminant(plan)
            );
            Ok(plan.clone())
        }
    }
}

/// Owned scope state accumulated across WITH barriers while building a chained
/// WITH/MATCH CTE plan. Bundles the two bindings that are always managed as a
/// unit inside `build_chained_with_match_cte_plan`:
///   - `scope_cte_variables`: alias → CTE property mapping used to build a
///     `VariableScope` for rendering subsequent CTE bodies and the final plan.
///   - `var_registry`: unified variable registry attached to CTEs and the final
///     RenderPlan for use by the SQL renderer.
///
/// They are snapshotted together, cleared together at each WITH barrier,
/// populated together as each CTE is built, and consumed together.
struct WithBarrierScope {
    scope_cte_variables: HashMap<String, super::variable_scope::CteVariableInfo>,
    var_registry: crate::query_planner::typed_variable::VariableRegistry,
    /// #602: node labels an alias carried at ANY prior WITH barrier, keyed by the
    /// alias name it is published under. Unlike `scope_cte_variables` this is NOT
    /// cleared at `reset()` — it persists for the whole barrier chain. A passthrough
    /// WITH (e.g. `WITH u, fc`) re-derives an alias's label from the post-barrier
    /// plan, where the source `GraphNode` is gone (the alias is now a `with_`-sourced
    /// CTE ViewScan, label stripped), so a downstream MATCH re-anchoring on `u` loses
    /// the `User` label needed by `resolve_generic_id_in_cte` for generic `.id`
    /// resolution. This map lets the label survive every barrier the alias crosses.
    carried_labels: HashMap<String, Vec<String>>,
}

impl WithBarrierScope {
    /// Fresh, empty scope (matches the original inline init of both bindings).
    fn new() -> Self {
        Self {
            scope_cte_variables: HashMap::new(),
            var_registry: crate::query_planner::typed_variable::VariableRegistry::new(),
            carried_labels: HashMap::new(),
        }
    }

    /// Read-only access to the accumulated CTE variables.
    fn scope_cte_variables(&self) -> &HashMap<String, super::variable_scope::CteVariableInfo> {
        &self.scope_cte_variables
    }

    /// Mutable access to the accumulated CTE variables (for in-place patches
    /// such as `map_keys`).
    fn scope_cte_variables_mut(
        &mut self,
    ) -> &mut HashMap<String, super::variable_scope::CteVariableInfo> {
        &mut self.scope_cte_variables
    }

    /// Whether any CTE variables are currently in scope.
    fn is_empty(&self) -> bool {
        self.scope_cte_variables.is_empty()
    }

    /// WITH barrier: snapshot the current body registry (pre-clear) so it can be
    /// attached to the CTE for runtime resolution.
    fn snapshot_body_registry(
        &self,
    ) -> std::sync::Arc<crate::query_planner::typed_variable::VariableRegistry> {
        std::sync::Arc::new(self.var_registry.clone())
    }

    /// WITH barrier: clear accumulated scope so only the current CTE's exports
    /// are visible in the next scope. Preserves the exact clear ordering.
    fn reset(&mut self) {
        self.scope_cte_variables.clear();
        self.var_registry.clear();
    }

    /// Record one exported alias's property mapping into both the CTE variable
    /// map and the unified variable registry (scalar-vs-node branch preserved).
    ///
    /// `source_alias` is the alias the CTE columns are prefixed under — the same
    /// as `alias` for a plain export, but the ORIGINAL name for a rename
    /// (`WITH u AS u3` → `alias = "u3"`, `source_alias = "u"`). The label carry
    /// (#602/#662) keys on it so a rename that happens at the very barrier the
    /// label must cross still inherits the label recorded under the source name.
    fn publish_alias(
        &mut self,
        alias: &str,
        source_alias: &str,
        cte_name: &str,
        per_alias_mapping: &HashMap<String, String>,
        labels: &[String],
    ) {
        // #602: an alias's node label is re-derived per barrier from the
        // post-barrier plan (`plan_builder_utils.rs` label-compute site). After a
        // passthrough WITH (e.g. `WITH u, fc`) the source `GraphNode` is gone —
        // the alias is now a `with_`-sourced CTE ViewScan whose label was stripped
        // — so `labels` arrives EMPTY even though the alias still denotes the same
        // node. If a downstream MATCH re-anchors on it, `resolve_generic_id_in_cte`
        // (variable_scope.rs) then can't resolve the generic `.id` and the join
        // anchors on the wrong column (alphabetical `.id` fallback → #616 class).
        // Carry the label forward: prefer this barrier's freshly-computed label,
        // else — ONLY for a genuine node passthrough (a non-empty
        // `per_alias_mapping`) — reuse the most recent non-empty label the alias
        // published earlier. An EMPTY mapping means the alias was rebound to a
        // direct CTE column / scalar (e.g. `WITH u.email AS u`); such a scalar
        // must NOT inherit a stale node label, or a downstream `.id` on it would
        // route back through node resolution and silently resolve to the old
        // node's id column (loud→silent). Gating the carry on a non-empty mapping
        // keeps scalars scalar and preserves main's behavior for them.
        //
        // #662: a rename AT the crossing barrier (`WITH u AS u3`) publishes under
        // the NEW name (`u3`) while the label was recorded under the ORIGINAL
        // (`u`). Look up the carried label under BOTH the published name and the
        // `source_alias`, and record the carried-forward label under the new name
        // too so a further barrier keeps finding it.
        let effective_labels: Vec<String> = if !labels.is_empty() {
            self.carried_labels
                .insert(alias.to_string(), labels.to_vec());
            labels.to_vec()
        } else if !per_alias_mapping.is_empty() {
            let carried = self
                .carried_labels
                .get(alias)
                .or_else(|| self.carried_labels.get(source_alias))
                .cloned()
                .unwrap_or_default();
            if !carried.is_empty() && alias != source_alias {
                // Re-key under the new published name for subsequent barriers.
                self.carried_labels
                    .insert(alias.to_string(), carried.clone());
            }
            carried
        } else {
            Vec::new()
        };
        let labels: &[String] = &effective_labels;

        self.scope_cte_variables.insert(
            alias.to_string(),
            super::variable_scope::CteVariableInfo {
                cte_name: cte_name.to_string(),
                property_mapping: per_alias_mapping.clone(),
                labels: labels.to_vec(),
                from_alias_override: None,
                map_keys: None,
            },
        );

        // Update unified variable registry: define/overwrite variable as CTE-sourced
        // with its property mapping so the SQL renderer can resolve properties.
        {
            use crate::query_planner::typed_variable::VariableSource;

            // F1 (P-4 forward-resolution): an EMPTY `per_alias_mapping` means the
            // alias IS a direct CTE column (a scalar projection like
            // `WITH u.user_id AS id` or `WITH count(*) AS cnt`) — there is no
            // Cypher-property→column indirection, the column is named after the
            // alias itself. Without an entry the forward registry (M1) returned
            // `Unresolved`, so the render site fell through to the legacy M2
            // reparse (`get_cte_property_from_context`), which supplies exactly
            // this identity entry (keyed by the FROM alias). F1 retires that
            // fallback, so give the registry the SAME identity self-map here.
            //
            // This is gated on emptiness, NOT on the labels branch below: the
            // planner may still attach a node label to such a scalar (e.g. `id`
            // above derives from `u:User`, so `find_label_for_alias_in_plan`
            // reports `User`), which would otherwise route it through the node
            // branch with an empty map and leave `id.id` to be mis-resolved by
            // the id-pseudo-property block in `to_sql_query.rs`. The identity map
            // only ever matches a literal `alias.alias` access — a genuine node
            // property (`u.name`) is absent from it and still falls through — so
            // it is safe to apply regardless of label.
            //
            // Kept OUT of `scope_cte_variables`/`per_alias_mapping` on purpose:
            // M3 (`VariableScope`) keys its scalar-vs-node expansion on
            // `property_mapping.is_empty()` (variable_scope.rs), so a non-empty
            // scope map would wrongly expand the scalar as a node. The registry
            // is a separate channel consulted only at the render site.
            let mut registry_mapping = per_alias_mapping.clone();
            if registry_mapping.is_empty() {
                registry_mapping.insert(alias.to_string(), alias.to_string());
            }
            let cte_source = VariableSource::Cte {
                cte_name: cte_name.to_string(),
                property_mapping: Box::new(registry_mapping),
            };
            if labels.is_empty() {
                // No labels → scalar variable (e.g., computed column, count, etc.)
                self.var_registry
                    .define_scalar(alias.to_string(), cte_source);
            } else {
                // Has labels → node variable (relationship labels would need
                // more context; treating as node is correct for most WITH exports)
                self.var_registry
                    .define_node(alias.to_string(), labels.to_vec(), cte_source);
            }
        }

        // F0 TRANSITION-ASSERT (P-4 forward-resolution, slice F0):
        // #592 was that `define_node`/`define_scalar` DROPPED the caller's
        // `property_mapping` and rebuilt an empty one, so the unified
        // `VariableRegistry` (M1) could never resolve a WITH-CTE property — the
        // live forward resolution ran entirely through the parallel
        // `scope_cte_variables` map (M3: `variable_scope::VariableScope`) and
        // the task-local reparse (M2). F0 threads `property_mapping` through
        // `define_*`, so M1 now carries the SAME data M3 does — both are
        // populated here from the identical `per_alias_mapping`.
        //
        // This asserts that faithfulness at the point the data is born: for
        // every mapped Cypher property, M1 (`var_registry.resolve`) must return
        // exactly the CTE column M3 (`scope_cte_variables[alias].property_mapping`)
        // holds. This is the non-vacuous form of the doc's transition-assert:
        // the render-site variant (§5) is inert because expressions are already
        // M3-rewritten to CTE-column names before `to_sql` (see the DISCOVERY
        // note in to_sql_query.rs). Proving M1 == M3 here de-risks F1, which
        // will make the forward registry authoritative.
        //
        // If this fires, the two populators have diverged — investigate; do NOT
        // silence it (see FORWARD_RESOLUTION_PLAN.md §5 and the F0 report).
        #[cfg(debug_assertions)]
        {
            use crate::query_planner::typed_variable::ResolvedProperty;
            let schema = crate::server::query_context::get_current_schema();
            if let (Some(schema), Some(info)) = (schema, self.scope_cte_variables.get(alias)) {
                let expected_from_alias = info.effective_from_alias();
                for (cypher_prop, cte_col) in info.property_mapping.iter() {
                    match self.var_registry.resolve(alias, cypher_prop, &schema) {
                        ResolvedProperty::CteColumn { sql_alias, column } => {
                            debug_assert_eq!(
                                (sql_alias.as_str(), column.as_str()),
                                (expected_from_alias.as_str(), cte_col.as_str()),
                                "F0 M1/M3 divergence: registry resolved {}.{} → {}.{}, \
                                 but scope map holds {}.{}",
                                alias,
                                cypher_prop,
                                sql_alias,
                                column,
                                expected_from_alias,
                                cte_col
                            );
                        }
                        other => {
                            // A mapped property MUST resolve to a CteColumn now.
                            // Anything else means the threaded map did not reach
                            // the registry (the #592 defect regressing).
                            debug_assert!(
                                false,
                                "F0 M1 failed to resolve mapped property {}.{} \
                                 (expected {}.{}, got {:?}) — property_mapping \
                                 not threaded into VariableRegistry",
                                alias, cypher_prop, expected_from_alias, cte_col, other
                            );
                        }
                    }
                }
            }
        }
    }

    /// Add the COMPOSITE alias (e.g., "countWindow1_tag") to the CTE variable
    /// map, merging ALL individual aliases' mappings plus identity entries for
    /// scalar aliases. Preserves the original merge logic verbatim.
    fn publish_composite(
        &mut self,
        with_alias: &str,
        cte_name: &str,
        original_exported_aliases: &[String],
    ) {
        let mut composite_mapping: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for alias in original_exported_aliases {
            if let Some(info) = self.scope_cte_variables.get(alias) {
                // Merge Cypher→CTE column mappings from this individual alias
                composite_mapping.extend(
                    info.property_mapping
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
                // Scalar variables (empty property_mapping) are direct column names in the CTE
                if info.property_mapping.is_empty() {
                    composite_mapping.insert(alias.clone(), alias.clone());
                }
            }
        }
        self.scope_cte_variables.insert(
            with_alias.to_string(),
            super::variable_scope::CteVariableInfo {
                cte_name: cte_name.to_string(),
                property_mapping: composite_mapping.clone(),
                labels: Vec::new(),
                from_alias_override: None,
                map_keys: None,
            },
        );

        // F1 (P-4 forward-resolution): mirror the composite mapping into the
        // forward registry (M1), keyed by the composite alias, so a render-site
        // reference through the composite FROM alias (e.g. `e_id_n.id` from
        // `WITH u.name AS n, u.email AS e, u.user_id AS id`) resolves forward
        // instead of via the legacy M2 reparse this slice retires. Registered as
        // a scalar (no single label spans a heterogeneous composite); the merged
        // `composite_mapping` already carries each member's Cypher→CTE-column
        // entries plus identity entries for scalar members (see the loop above).
        // Same rationale as the scalar case in `publish_alias`: the registry is a
        // separate channel from `scope_cte_variables` (M3), whose empty-map test
        // drives node-vs-scalar expansion — so this stays OUT of M3's map.
        if !composite_mapping.is_empty() {
            use crate::query_planner::typed_variable::VariableSource;
            self.var_registry.define_scalar(
                with_alias.to_string(),
                VariableSource::Cte {
                    cte_name: cte_name.to_string(),
                    property_mapping: Box::new(composite_mapping.clone()),
                },
            );
        }
        log::info!(
            "🔧 build_chained: Added composite alias '{}' to scope_cte_variables with {} properties",
            with_alias,
            composite_mapping.len()
        );
    }

    /// Build a `VariableScope` from all accumulated CTE variables for a
    /// rendering pass (CTE body or final plan).
    fn build_final_scope<'a>(
        &self,
        schema: &'a GraphSchema,
        plan: &'a LogicalPlan,
    ) -> super::variable_scope::VariableScope<'a> {
        super::variable_scope::VariableScope::with_cte_variables(
            schema,
            plan,
            self.scope_cte_variables.clone(),
        )
    }

    /// Consume the scope and return the unified variable registry for the final
    /// attach to the outer render plan.
    fn take_registry(self) -> crate::query_planner::typed_variable::VariableRegistry {
        self.var_registry
    }
}

/// Owns the CTE-naming / dedup cluster used across WITH barriers while building a
/// chained WITH/MATCH CTE plan. Bundles the three loop-global maps that are always
/// managed together to "allocate a unique CTE name + record the analyzer→actual
/// name remapping":
///   - `sequence_numbers`: alias-key → next sequence number for generating unique
///     CTE names (format `with_<sorted_aliases>_cte_<seq>`).
///   - `used_names`: CTE names already emitted, to prevent duplicates.
///   - `name_remapping`: analyzer CTE name → actual CTE name, applied to the final
///     render plan for passthrough / collapsed WITHs.
struct CteNameAllocator {
    sequence_numbers: HashMap<String, usize>,
    used_names: HashSet<String>,
    name_remapping: HashMap<String, String>,
}

impl CteNameAllocator {
    /// Fresh, empty allocator (matches the original inline init of all three maps).
    fn new() -> Self {
        Self {
            sequence_numbers: HashMap::new(),
            used_names: HashSet::new(),
            name_remapping: HashMap::new(),
        }
    }

    /// FALLBACK ONLY: generate a unique CTE name when the analyzer didn't set
    /// `WithClause.cte_name`. Mirrors the original `.unwrap_or_else(...)` body.
    fn next_fallback_name(
        &mut self,
        aliases_key: &str,
        sorted_aliases: &[String],
        exported_aliases: &[String],
    ) -> String {
        // FALLBACK ONLY: If cte_name somehow not set (shouldn't happen after fix)
        // Generate unique CTE name using centralized utility
        // Format: with_<sorted_aliases>_cte_<seq>
        let seq_num = self
            .sequence_numbers
            .entry(aliases_key.to_string())
            .or_insert(1);
        let current_seq = *seq_num;
        let name = generate_cte_name(sorted_aliases, current_seq);
        *seq_num += 1; // Increment for next iteration
        log::debug!("🔧 build_chained_with_match_cte_plan: FALLBACK - WithClause.cte_name was None! Generated CTE name '{}' from aliases {:?} (sequence {}). This indicates analyzer didn't set cte_name properly.",
                   name, exported_aliases, current_seq);
        name
    }

    /// Ensure `used_names` contains any CTEs hoisted earlier in this pass.
    fn sync_hoisted(&mut self, all_ctes: &[Cte]) {
        // Ensure used_cte_names contains any CTEs hoisted earlier in this pass
        for existing in all_ctes {
            self.used_names.insert(existing.cte_name.clone());
        }
    }

    /// Resolve `proposed` to a name not already in `used_names`. If it collides,
    /// generate a fresh candidate, record the remapping, and advance the sequence.
    /// Then track the final name as used and advance the suffix-based counter.
    fn resolve_unique_name(
        &mut self,
        proposed: String,
        aliases_key: &str,
        sorted_aliases: &[String],
    ) -> String {
        let mut cte_name = proposed;

        // If analyzer provided a duplicate name (or hoisted CTE collided), generate a fresh one
        if self.used_names.contains(&cte_name) {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Duplicate CTE name '{}' detected, generating a unique name",
                cte_name
            );

            let seq_entry = self
                .sequence_numbers
                .entry(aliases_key.to_string())
                .or_insert(1);
            let mut next_seq = *seq_entry;
            let mut candidate = generate_cte_name(sorted_aliases, next_seq);
            while self.used_names.contains(&candidate) {
                next_seq += 1;
                candidate = generate_cte_name(sorted_aliases, next_seq);
            }

            // Remap the analyzer's name to the generated unique name
            self.name_remapping
                .insert(cte_name.clone(), candidate.clone());

            *seq_entry = next_seq + 1;
            cte_name = candidate;
        }

        // Track this name as used and advance the sequence counter based on its suffix
        self.used_names.insert(cte_name.clone());
        if let Some(suffix) = cte_name
            .rsplit('_')
            .next()
            .and_then(|s| s.parse::<usize>().ok())
        {
            let entry = self
                .sequence_numbers
                .entry(aliases_key.to_string())
                .or_insert(suffix + 1);
            if *entry <= suffix {
                *entry = suffix + 1;
            }
        }

        cte_name
    }

    /// Record remappings from analyzer CTE names that share the same base pattern as
    /// `final_cte_name` (but a different name) to `final_cte_name`.
    fn record_base_remapping(
        &mut self,
        final_cte_name: &str,
        all_analyzer_cte_names: &HashSet<String>,
    ) {
        // CRITICAL: Collect CTE name remapping from analyzer's CTE names to our generated name
        // The analyzer may have generated different CTE names (e.g., with_name_cte_2) for the same aliases.
        // When expressions reference the analyzer's name, we need to remap them to our name.
        //
        // Strategy: Any analyzer CTE name with the same base alias pattern should be remapped.
        // E.g., if we generate "with_name_cte_1", then "with_name_cte_2", "with_name_cte_3" should remap to it.
        let cte_base = final_cte_name
            .rsplit("_cte_")
            .skip(1)
            .collect::<Vec<_>>()
            .join("_cte_");
        log::info!(
            "🔧 build_chained_with_match_cte_plan: CTE base pattern for '{}' is '{}'",
            final_cte_name,
            cte_base
        );

        for analyzer_name in all_analyzer_cte_names {
            // Check if this analyzer name has the same base (e.g., "with_name")
            let analyzer_base = analyzer_name
                .rsplit("_cte_")
                .skip(1)
                .collect::<Vec<_>>()
                .join("_cte_");
            if analyzer_base == cte_base && analyzer_name.as_str() != final_cte_name {
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Recording CTE name remap: '{}' → '{}' (same base)",
                    analyzer_name, final_cte_name
                );
                self.name_remapping
                    .insert(analyzer_name.clone(), final_cte_name.to_string());
            }
        }
    }

    /// Record a single analyzer→actual name remapping (passthrough / collapsed WITH).
    fn record_remapping(&mut self, from: String, to: String) {
        self.name_remapping.insert(from, to);
    }

    /// Mark a CTE name as used (e.g., CTEs hoisted from a recursive call).
    fn mark_used(&mut self, name: String) {
        self.used_names.insert(name);
    }

    /// Whether any analyzer→actual name remappings were recorded.
    fn has_remappings(&self) -> bool {
        !self.name_remapping.is_empty()
    }

    /// The accumulated analyzer→actual name remapping (for the final fixup pass).
    fn remapping(&self) -> &HashMap<String, String> {
        &self.name_remapping
    }
}

/// Post-render reconciliation pass (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// The analyzer assigns CTE names with its own counter (e.g. `_cte_5`) but the
/// renderer creates CTEs with sequential numbering (`_cte_1`). Scan `render_plan`
/// for any `with_*_cte_N` table-alias references that don't match a real CTE and
/// remap each stale reference to the actual CTE that shares its base name (the
/// portion before `_cte_N`). No-op when every reference already resolves.
fn reconcile_stale_cte_name_references(render_plan: &mut RenderPlan, all_ctes: &[Cte]) {
    let actual_cte_names: std::collections::HashSet<String> =
        all_ctes.iter().map(|c| c.cte_name.clone()).collect();
    // Build base→actual mapping: strip _cte_N suffix to get base, map to actual name
    let mut base_to_actual: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for name in &actual_cte_names {
        if let Some(base) = name.rfind("_cte_").map(|pos| &name[..pos]) {
            base_to_actual.insert(base.to_string(), name.clone());
        }
    }
    // Collect all table aliases from render plan that look like CTE references
    let referenced = collect_with_cte_table_aliases(render_plan);
    let mut auto_remap: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for ref_name in &referenced {
        if actual_cte_names.contains(ref_name) {
            continue; // Already correct
        }
        if let Some(pos) = ref_name.rfind("_cte_") {
            let base = &ref_name[..pos];
            if let Some(actual) = base_to_actual.get(base) {
                auto_remap.insert(ref_name.clone(), actual.clone());
            }
        }
    }
    if !auto_remap.is_empty() {
        log::debug!(
            "🔧 build_chained: Auto-remapping {} stale CTE references",
            auto_remap.len()
        );
        for (from, to) in &auto_remap {
            log::debug!("🔧   {} → {}", from, to);
        }
        remap_cte_names_in_render_plan(render_plan, &auto_remap);
    }
}

/// Post-render fallback (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When every table reference in a chained-WITH query has been replaced by a
/// CTE, the final `render_plan.from` can come back empty. Point it at the last
/// `with_*` CTE so the query still has a FROM. The alias is the CTE's own
/// exported-alias part (`with_tag_total_cte_1` → `tag_total`). No-op when there
/// is no `with_*` CTE.
///
/// Caller gates this on `render_plan.from` being `None`, `all_ctes` non-empty,
/// and no `Union` (each union branch carries its own FROM).
fn apply_from_fallback_to_last_cte(render_plan: &mut RenderPlan, all_ctes: &[Cte]) {
    // FALLBACK: If FROM is None but we have CTEs, set FROM to the last CTE
    // This happens when WITH clauses are chained and all table references have been replaced with CTEs
    // Skip when Union branches exist — each branch has its own FROM
    if let Some(last_with_cte) = all_ctes
        .iter()
        .rev()
        .find(|cte| cte.cte_name.starts_with("with_"))
    {
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: FROM clause missing, setting to last CTE: {}",
            last_with_cte.cte_name
        );

        // Extract aliases from CTE name: "with_tag_total_cte_1" → "tag_total"
        let with_alias_part = if let Some(stripped) = last_with_cte.cte_name.strip_prefix("with_") {
            if let Some(cte_pos) = stripped.rfind("_cte") {
                &stripped[..cte_pos]
            } else {
                stripped
            }
        } else {
            ""
        };

        render_plan.from = FromTableItem(Some(ViewTableRef {
            source: std::sync::Arc::new(LogicalPlan::Empty),
            name: last_with_cte.cte_name.clone(),
            alias: Some(with_alias_part.to_string()),
            use_final: false,
        }));

        log::info!(
            "🔧 build_chained_with_match_cte_plan: Set FROM to: {} AS '{}'",
            last_with_cte.cte_name,
            with_alias_part
        );
    }
}

/// Post-render remapping pass (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When passthrough WITH clauses are skipped, expressions in `render_plan` may
/// still reference the analyzer's original CTE names. Rewrite them to the actual
/// names the renderer created, using the allocator's analyzer→actual remapping.
/// No-op when the allocator recorded no remappings.
fn apply_passthrough_cte_name_remappings(
    render_plan: &mut RenderPlan,
    cte_name_allocator: &CteNameAllocator,
) {
    if cte_name_allocator.has_remappings() {
        log::info!(
            "🔧 build_chained_with_match_cte_plan: Applying CTE name remapping ({} entries)",
            cte_name_allocator.remapping().len()
        );
        remap_cte_names_in_render_plan(render_plan, cte_name_allocator.remapping());
    }
}

/// Post-render union-shell fixup (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When the outer `render_plan.from` is `None` (a Union shell, produced when
/// Direct Union rendering moved every branch into `union.input` for
/// aggregation/GROUP BY) but CTE references exist, each union branch still needs
/// those CTEs in scope. Add a `1 = 1` cross-JOIN for every referenced CTE to
/// each branch that doesn't already join it. Sorted by CTE name for
/// deterministic emitted JOIN order (`cte_references` is a `HashMap` with
/// per-process-random iteration order, #480 class).
///
/// #593: never do this for a Cypher UNION — each arm is an independent query
/// that must not be cross-joined to a sibling arm's WITH-CTE. Caller passes
/// `is_cypher_union_plan` so this stays a no-op there.
fn add_cte_cross_joins_to_union_branches(
    render_plan: &mut RenderPlan,
    cte_references: &HashMap<String, String>,
    is_cypher_union_plan: bool,
) {
    if render_plan.from.0.is_none() && !cte_references.is_empty() && !is_cypher_union_plan {
        if let Some(ref mut union_data) = render_plan.union.0 {
            // Sorted: the emitted JOIN order in each Union branch follows this
            // iteration and `cte_references` is a HashMap whose iteration order
            // is per-process random (#480 class).
            let mut sorted_cte_names: Vec<&String> = cte_references.values().collect();
            sorted_cte_names.sort();
            for cte_name in sorted_cte_names {
                let cte_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    cte_name.clone()
                };

                for branch in union_data.input.iter_mut() {
                    let already_has = branch.joins.0.iter().any(|j| j.table_alias == cte_alias);
                    if !already_has {
                        use crate::render_plan::render_expr::Literal as RenderLiteral;
                        let cte_join = super::Join {
                            table_name: cte_name.clone(),
                            table_alias: cte_alias.clone(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::Literal(RenderLiteral::Integer(1)),
                                    RenderExpr::Literal(RenderLiteral::Integer(1)),
                                ],
                            }],
                            join_type: super::JoinType::Inner,
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                            is_cartesian: false,
                        };
                        branch.joins.0.insert(0, cte_join);
                        log::info!(
                            "🔧 build_chained_with_match_cte_plan: Added CTE cross-JOIN '{}' AS '{}' to Union branch (FROM=None shell)",
                            cte_name, cte_alias
                        );
                    }
                }
            }
        }
    }
}

/// Post-render weighted-shortestPath fixup (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When a weight CTE is registered (task-local `get_weight_cte_config`) and the
/// render plan contains VLP CTEs, the outer query would otherwise wrongly
/// cross-join the weight CTE with the VLP. Restructure it to use the first VLP
/// CTE as the sole `FROM` (aliased `t`), clear all joins, drop the UNION
/// branches (bidirectional shortestPath builds two VLPs but weighted mode needs
/// only one — both yield the same minimum weight), rewrite each SELECT item's
/// expression onto the VLP columns, and clear GROUP BY. No-op when no weight CTE
/// is configured or no VLP CTE is present.
fn apply_weighted_shortest_path_restructure(render_plan: &mut RenderPlan) {
    if let Some(weight_config) = crate::server::query_context::get_weight_cte_config() {
        // VLP CTEs are in render_plan.ctes (not all_ctes yet — they get moved later)
        let has_vlp = render_plan
            .ctes
            .0
            .iter()
            .any(|c| c.vlp_path_variable.is_some());
        log::info!(
            "🔧 Weighted shortestPath check: weight_config={}, has_vlp={}, render_plan.ctes={}",
            weight_config.cte_name,
            has_vlp,
            render_plan.ctes.0.len()
        );
        if has_vlp {
            // Find the first VLP CTE (the one that resolves person1→person2)
            if let Some(vlp_cte) = render_plan
                .ctes
                .0
                .iter()
                .find(|c| c.vlp_path_variable.is_some())
            {
                let vlp_cte_name = vlp_cte.cte_name.clone();
                let path_variable = vlp_cte.vlp_path_variable.clone();
                let start_alias = vlp_cte.vlp_cypher_start_alias.clone();
                let end_alias = vlp_cte.vlp_cypher_end_alias.clone();
                log::info!(
                    "🔧 Weighted shortestPath: restructuring outer query to use VLP CTE '{}' (weight CTE: '{}', path_var: {:?})",
                    vlp_cte_name,
                    weight_config.cte_name,
                    path_variable
                );

                // Replace FROM with VLP CTE
                render_plan.from = FromTableItem(Some(ViewTableRef {
                    source: std::sync::Arc::new(LogicalPlan::Empty),
                    name: vlp_cte_name,
                    alias: Some("t".to_string()),
                    use_final: false,
                }));

                // Clear all joins — VLP CTE is self-contained
                render_plan.joins = JoinItems(vec![]);

                // Remove UNION branches (bidirectional shortestPath creates two VLPs,
                // but with weighted mode we only need one — both give same minimum weight)
                render_plan.union = UnionItems(None);

                // Rewrite SELECT items using VLP column mappings
                // The RETURN expressions (nodes(path), cost(path)) are rewritten to
                // VLP CTE columns (t.path_nodes, t.total_weight)
                for item in &mut render_plan.select.items {
                    let rewritten =
                        crate::clickhouse_query_generator::to_sql_query::rewrite_expr_for_vlp(
                            &item.expression,
                            &start_alias,
                            &end_alias,
                            &path_variable,
                            false,
                        );
                    item.expression = rewritten;
                }

                // Remove group_by (no aggregation in outer query)
                render_plan.group_by = super::GroupByExpressions(vec![]);
            }
        }
    }
}

/// Post-render FROM resolution against the accumulated CTEs (finalization tail
/// of `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// After a WITH barrier the final `render_plan.from` may still name a Cypher
/// alias (e.g. `a`) that was exported through a CTE. Resolve it:
/// - **FROM alias is in `cte_references`** → repoint `render_plan.from` at that
///   CTE while KEEPING the original alias (SELECT/WHERE/JOINs reference `a.xxx`
///   and the CTE columns are prefixed with that alias), and rewrite any stale
///   `combined_alias` (`with_{aliases}_cte{N}` → `{aliases}`) references in the
///   plan back to the preserved alias.
/// - **FROM is `None`** with CTEs present and no union → delegate to
///   [`apply_from_fallback_to_last_cte`].
///
/// #593: never repoint the base arm of a Cypher-level UNION — that FROM is the
/// first arm's own independent scan which merely happens to reuse a Cypher alias
/// name a DIFFERENT arm exported through a WITH-CTE; each arm's FROM was already
/// resolved per-arm during branch rendering. The caller passes
/// `is_cypher_union_plan` to gate the repoint off there.
fn resolve_final_from_against_cte(
    render_plan: &mut RenderPlan,
    cte_references: &HashMap<String, String>,
    all_ctes: &[Cte],
    is_cypher_union_plan: bool,
) {
    if let FromTableItem(Some(from_ref)) = &render_plan.from {
        // Check if the FROM alias is in cte_references
        if let Some(alias) = &from_ref.alias {
            if let Some(cte_name) = cte_references.get(alias).filter(|_| !is_cypher_union_plan) {
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: FROM alias '{}' is in CTE '{}', replacing FROM",
                    alias,
                    cte_name
                );

                // Keep the original alias (e.g., "a") as the FROM alias.
                // The rest of the rendered plan (SELECT, WHERE, JOINs) references "a.xxx",
                // so the FROM alias must match. The CTE columns are prefixed with the
                // original alias (e.g., "a_customer_id"), which works with FROM alias "a".
                let preserved_alias = alias.clone();

                // Compute what the combined alias WOULD have been (e.g., "a_allNeighboursCount")
                // so we can rewrite any stale references in SELECT/WHERE/ORDER BY
                let combined_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    String::new()
                };

                render_plan.from = FromTableItem(Some(ViewTableRef {
                    source: std::sync::Arc::new(LogicalPlan::Empty),
                    name: cte_name.clone(),
                    alias: Some(preserved_alias.clone()),
                    use_final: false,
                }));

                // Rewrite stale references: combined alias → preserved alias
                // e.g., "a_allNeighboursCount.xxx" → "a.xxx" in SELECT, WHERE, JOINs
                if combined_alias != preserved_alias && !combined_alias.is_empty() {
                    log::debug!(
                        "🔧 Rewriting stale alias '{}' → '{}' in render plan",
                        combined_alias,
                        preserved_alias
                    );
                    rewrite_table_alias_in_render_plan(
                        render_plan,
                        &combined_alias,
                        &preserved_alias,
                    );
                }

                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Replaced FROM with: {} AS '{}'",
                    cte_name,
                    preserved_alias
                );
            }
        }
    } else if matches!(render_plan.from, FromTableItem(None))
        && !all_ctes.is_empty()
        && render_plan.union.0.is_none()
    {
        apply_from_fallback_to_last_cte(render_plan, all_ctes);
    }
}

/// VLP-specific WITH→CTE join-condition generator (Phase-4 §7.1 extraction from
/// `resolve_cross_table_with_cte_joins`).
///
/// When the outer FROM is a VLP CTE (`vlp_…`) and no correlation/filter join
/// condition was resolved, connect the WITH CTE's ID column to the VLP CTE's
/// `start_id`/`end_id` (handling composite IDs via `concat(toString(col), '|', …)`
/// and wrapping both sides in `toString()` for the String-typed VLP endpoints).
/// Pushes any generated condition into `join_conditions`; a no-op when FROM is not
/// a VLP CTE or the alias is neither the VLP start nor end. The caller gates this
/// on `join_conditions.is_empty()`.
fn generate_vlp_with_cte_join_conditions(
    render_plan: &RenderPlan,
    cte_name: &str,
    cte_alias: &str,
    cte_schemas: &crate::render_plan::CteSchemas,
    join_conditions: &mut Vec<OperatorApplication>,
) {
    if let FromTableItem(Some(from_ref)) = &render_plan.from {
        if from_ref.name.starts_with("vlp_") {
            // Find which VLP CTE this is and determine if the alias is start or end
            for vlp_cte in &render_plan.ctes.0 {
                if vlp_cte.cte_name == from_ref.name {
                    // Match when cte_alias equals or starts with the VLP alias
                    // e.g., cte_alias="a_allNeighboursCount" matches vlp start_alias="a"
                    let is_start = vlp_cte.vlp_cypher_start_alias.as_deref().is_some_and(|a| {
                        cte_alias == a || cte_alias.starts_with(&format!("{}_", a))
                    });
                    let is_end = vlp_cte.vlp_cypher_end_alias.as_deref().is_some_and(|a| {
                        cte_alias == a || cte_alias.starts_with(&format!("{}_", a))
                    });
                    if is_start || is_end {
                        let vlp_id_col = if is_start { "start_id" } else { "end_id" };
                        let from_alias = from_ref.alias.as_deref().unwrap_or("t");
                        // Find the ID column name in the WITH CTE
                        // Use cte_schemas which has the alias_to_id_column mapping
                        let vlp_alias = if is_start {
                            vlp_cte
                                .vlp_cypher_start_alias
                                .as_deref()
                                .unwrap_or(cte_alias)
                        } else {
                            vlp_cte.vlp_cypher_end_alias.as_deref().unwrap_or(cte_alias)
                        };
                        // Try cte_schemas first: look for {vlp_alias}_{something_id} in SELECT items
                        let id_col_name = if let Some(meta) = cte_schemas.get(cte_name) {
                            // First try direct alias_to_id lookup
                            meta.alias_to_id
                                .get(vlp_alias)
                                .cloned()
                                .or_else(|| {
                                    // Search SELECT items for {vlp_alias}_*_id pattern
                                    let prefix = format!("{}_", vlp_alias);
                                    meta.select_items.iter().find_map(|item| {
                                        if let Some(col_alias) = &item.col_alias {
                                            let name = &col_alias.0;
                                            if name.starts_with(&prefix)
                                                && (name.ends_with("_id") || name.ends_with("_id"))
                                            {
                                                return Some(name.clone());
                                            }
                                        }
                                        None
                                    })
                                })
                                .unwrap_or_else(|| {
                                    find_id_column_in_cte(cte_name, vlp_alias, &render_plan.ctes)
                                })
                        } else {
                            find_id_column_in_cte(cte_name, vlp_alias, &render_plan.ctes)
                        };

                        // Check if this node has a composite ID — if so, generate
                        // concat(toString(col1), '|', toString(col2)) to match
                        // the pipe-joined start_id/end_id in the VLP CTE
                        let rhs_expr = {
                            use crate::server::query_context::get_current_schema;
                            let composite_cols = get_current_schema().and_then(|schema| {
                                // Determine the node label from vlp_alias
                                let _label = if is_start {
                                    vlp_cte.vlp_cypher_start_alias.as_deref()
                                } else {
                                    vlp_cte.vlp_cypher_end_alias.as_deref()
                                };
                                // Look up by label_constraints or try all schemas
                                for ns in schema.all_node_schemas().values() {
                                    if ns.node_id.is_composite() {
                                        // Check if the id_col_name matches one of this schema's columns
                                        let prefix = format!("{}_", vlp_alias.to_owned());
                                        let id_cols = ns.node_id.columns();
                                        let first_cte_col = format!("{}{}", prefix, id_cols[0]);
                                        if id_col_name == first_cte_col || id_col_name == id_cols[0]
                                        {
                                            return Some(
                                                id_cols
                                                    .iter()
                                                    .map(|c| c.to_string())
                                                    .collect::<Vec<_>>(),
                                            );
                                        }
                                    }
                                }
                                None
                            });

                            if let Some(cols) = composite_cols {
                                // Composite ID: concat(toString(cte.a_col1), '|', toString(cte.a_col2))
                                let prefix = format!("{}_", vlp_alias);
                                let parts: Vec<RenderExpr> = cols.iter().enumerate().flat_map(|(i, col)| {
                                        let cte_col = format!("{}{}", prefix, col);
                                        let mut items = Vec::new();
                                        if i > 0 {
                                            items.push(RenderExpr::Literal(Literal::String("|".to_string())));
                                        }
                                        items.push(RenderExpr::ScalarFnCall(ScalarFnCall {
                                            name: current_function_mapper().cast_string().to_string(),
                                            args: vec![RenderExpr::Column(Column(
                                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                    format!("{}.{}", cte_alias, cte_col)
                                                )
                                            ))],
                                        }));
                                        items
                                    }).collect();
                                log::debug!(
                                        "🔧 VLP+WITH: Composite ID JOIN - concat {} columns for alias '{}'",
                                        cols.len(), vlp_alias
                                    );
                                RenderExpr::ScalarFnCall(ScalarFnCall {
                                    name: "concat".to_string(),
                                    args: parts,
                                })
                            } else {
                                // Single ID: toString() to match the String type of
                                // start_id / end_id stored in the VLP CTE
                                RenderExpr::ScalarFnCall(ScalarFnCall {
                                        name: current_function_mapper().cast_string().to_string(),
                                        args: vec![RenderExpr::Column(Column(
                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                format!("{}.{}", cte_alias, id_col_name)
                                            )
                                        ))],
                                    })
                            }
                        };

                        // Wrap VLP side in toString() too, ensuring both sides are String.
                        // VLP start_id/end_id may be UInt64 or String depending on
                        // generation path, and rhs_expr already uses toString().
                        let lhs_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: current_function_mapper().cast_string().to_string(),
                            args: vec![RenderExpr::Column(Column(
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    format!("{}.{}", from_alias, vlp_id_col),
                                ),
                            ))],
                        });
                        let join_cond = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![lhs_expr, rhs_expr],
                        };
                        log::debug!(
                            "🔧 VLP+WITH: Generated JOIN condition for alias '{}' (is_start={})",
                            vlp_alias,
                            is_start
                        );
                        join_conditions.push(join_cond);
                    }
                    break;
                }
            }
        }
    }
}

/// Post-WITH OPTIONAL MATCH anchor restructure, else insert the CTE JOIN
/// (Phase-4 §7.1 extraction from `resolve_cross_table_with_cte_joins`).
///
/// #453: when the fresh post-WITH pattern is OPTIONAL, the required side is the
/// WITH CTE and the fresh table is optional — so (tightly guarded) promote the
/// CTE to the FROM anchor and LEFT-join the optional table, recovering the
/// optional side's WHERE into the join `pre_filter`/`ON` (#460/#462/#472) rather
/// than dropping NULL-extended rows. Otherwise insert `cte_join` at the head of
/// the joins list (skipping a duplicate alias). Returns `Err` if an
/// OPTIONAL-side predicate cannot be placed without silently changing semantics.
fn restructure_post_with_optional_or_insert_cte_join(
    render_plan: &mut RenderPlan,
    current_plan: &LogicalPlan,
    cte_name: &str,
    cte_alias: &str,
    join_conditions: &[OperatorApplication],
    cte_join: &super::Join,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<()> {
    // #453: Post-WITH OPTIONAL MATCH anchoring. When the fresh
    // pattern after the WITH barrier is OPTIONAL, the *required* side
    // arrives here as the CTE (`with_..._cte_N`) and the fresh
    // pattern table is optional. The plain code below would leave the
    // optional table as the FROM driver and INNER-join the CTE onto
    // it — that both drops every anchor row with no match AND uses the
    // wrong join type, silently violating OPTIONAL MATCH semantics.
    // Instead, mirror the non-WITH OPTIONAL path: make the required
    // CTE the FROM anchor and LEFT-join the optional pattern to it.
    //
    // Guarded tightly: only a genuinely correlated (non-`ON 1 = 1`),
    // single-branch pattern whose FROM is a real optional table (not
    // already the CTE, a nested WITH CTE, or a VLP CTE).
    let post_with_optional_restructure = current_plan.is_optional_pattern()
        && !join_conditions.is_empty()
        && render_plan.union.0.is_none()
        && render_plan
            .from
            .0
            .as_ref()
            .map(|vr| {
                vr.alias.as_deref() != Some(cte_alias)
                    && !vr.name.starts_with("with_")
                    && !vr.name.starts_with("vlp_")
            })
            .unwrap_or(false);

    if post_with_optional_restructure {
        // Demote the optional-side table currently in FROM to a LEFT
        // JOIN, and promote the required CTE to the FROM anchor.
        let old_from = render_plan
            .from
            .0
            .take()
            .expect("FROM guaranteed Some by post_with_optional_restructure guard");
        render_plan.from = FromTableItem(Some(super::ViewTableRef {
            source: std::sync::Arc::new(LogicalPlan::Empty),
            name: cte_name.to_string(),
            alias: Some(cte_alias.to_string()),
            use_final: false,
        }));
        // Everything already joined into the pattern is optional
        // relative to the anchor, so a partial match must still
        // NULL-extend: demote inner/cross joins to LEFT.
        for j in render_plan.joins.0.iter_mut() {
            if matches!(j.join_type, super::JoinType::Inner | super::JoinType::Join) {
                j.join_type = super::JoinType::Left;
            }
        }
        // LEFT JOIN the old FROM table back onto the CTE via the
        // resolved correlation keys (`join_conditions`).
        let optional_from_alias = old_from
            .alias
            .clone()
            .unwrap_or_else(|| old_from.name.clone());

        // Recover the OPTIONAL pattern's WHERE predicate. A `WHERE`
        // attached to the post-WITH OPTIONAL MATCH lives on the fresh
        // pattern's `GraphRel.where_predicate`. In this reversed-anchor
        // shape `collect_graphrel_predicates` DROPS the conjuncts that
        // reference ONLY the optional node or ONLY the relationship
        // alias from the outer WHERE (destined for a pre_filter on a
        // join this restructure rebuilds), while cross-alias / OR
        // conjuncts are (wrongly) routed to the outer WHERE. Without
        // recovery those predicates change query semantics (ground-rule
        // #1). We re-place each predicate class in its correct spot:
        //
        //   • optional-NODE-only conjuncts  -> LEFT JOIN pre_filter (#460)
        //   • relationship-alias-only conjuncts, on FK-edge where the
        //     rel shares the optional node's physical table, remap to
        //     that table's column and also go in the pre_filter (#462
        //     GAP 2). Applied BEFORE the join so no-match anchor rows
        //     stay NULL-extended.
        //   • conjuncts spanning the optional side AND the anchor CTE
        //     (incl. unsplittable OR) -> LEFT JOIN ON condition (#462
        //     GAP 1), handled after the join is built (below), so the
        //     predicate filters the match, never the anchor rows.
        let opt_where = find_graphrel_where_predicate(current_plan);
        let node_pre_filter = opt_where
            .and_then(|wp| extract_predicates_for_alias_logical(wp, &optional_from_alias).0);

        // #462 GAP 2: recover relationship-alias-only conjuncts. Only
        // safe to fold into the optional NODE's pre_filter when the rel
        // and the node share the same physical table (the FK-edge
        // pattern), because the pre_filter renders as
        // `SELECT * FROM <node table> WHERE …` — the rel's columns must
        // exist on that table. Detect via the schema catalog (edge/node
        // `full_table_name()` equality, the same structural signal
        // `is_node_denormalized_on_edge` uses), never a raw pattern-flag
        // branch (axis-dispatch rule).
        let opt_graphrel = find_graphrel(current_plan);
        let rel_pre_filter = match (opt_where, opt_graphrel) {
            (Some(wp), Some(gr)) if !gr.alias.is_empty() => {
                let rel_shares_node_table = gr
                    .labels
                    .as_ref()
                    .and_then(|ls| ls.first())
                    .and_then(|rel_type| schema.get_relationships_schema_opt(rel_type))
                    .map(|rel_schema| rel_schema.full_table_name() == old_from.name)
                    .unwrap_or(false);
                if rel_shares_node_table {
                    extract_predicates_for_alias_logical(wp, &gr.alias).0
                } else {
                    // Rel is a distinct table: a separate join would be
                    // required, which this restructure does not build.
                    // Refuse to silently drop the predicate (#462).
                    let rel_only = extract_predicates_for_alias_logical(wp, &gr.alias).0;
                    if rel_only.is_some() {
                        return Err(RenderBuildError::InvalidRenderPlan(format!(
                            "post-WITH OPTIONAL MATCH has a WHERE on relationship alias '{}' \
                             whose edge table is not the optional node '{}' table; the \
                             predicate cannot be placed without a separate edge join and \
                             must not be silently dropped (would change semantics)",
                            gr.alias, optional_from_alias
                        )));
                    }
                    None
                }
            }
            _ => None,
        };

        let optional_pre_filter =
            combine_optional_filters_with_and(vec![node_pre_filter, rel_pre_filter]);
        if optional_pre_filter.is_some() {
            log::info!(
                "🔧 build_chained_with_match_cte_plan: #460/#462 recovered optional-side WHERE predicate into LEFT JOIN pre_filter for alias '{}'",
                optional_from_alias
            );
        }

        // #462/#472: every conjunct in `render_plan.filters` at this
        // point belongs EXCLUSIVELY to this post-WITH OPTIONAL MATCH's
        // own WHERE (the tight `post_with_optional_restructure` guard
        // above ensures a single-branch optional pattern; any WHERE
        // attached to the WITH projection itself was already folded
        // into the CTE body and is not visible here). `
        // collect_graphrel_predicates` routed the whole thing to the
        // outer WHERE — wrong for OPTIONAL MATCH, since the outer WHERE
        // drops the NULL-extended no-match anchor rows. Move EVERY
        // conjunct into the LEFT JOIN's ON condition, including
        // pure-anchor ones (#472): for a LEFT JOIN a false ON condition
        // just NULL-extends the row rather than dropping it, so folding
        // the whole predicate into ON is always safe and never changes
        // which rows are kept vs. dropped when the FROM side is the
        // anchor CTE. (Anchor `c` references were already resolved to
        // CTE columns, e.g. `c.p1_c_customer_id`, when the filter was
        // rendered.) Nothing is left behind in the outer WHERE for this
        // segment.
        let mut extra_on_conditions: Vec<OperatorApplication> = Vec::new();
        if let Some(filter_expr) = render_plan.filters.0.take() {
            for conj in split_render_and_conjuncts(filter_expr) {
                match conj {
                    RenderExpr::OperatorApplicationExp(op) => {
                        extra_on_conditions.push(op);
                    }
                    // A boolean conjunct that is not an operator
                    // application (e.g. a bare scalar-fn predicate)
                    // cannot be expressed as a joining_on
                    // `OperatorApplication`. Rather than silently leave
                    // it in the outer WHERE (wrong semantics) or drop
                    // it, refuse with a clean error (#462/#472).
                    other => {
                        return Err(RenderBuildError::InvalidRenderPlan(format!(
                            "post-WITH OPTIONAL MATCH WHERE conjunct is not an \
                             operator application and cannot be moved into the \
                             LEFT JOIN ON condition for alias '{}'; refusing to \
                             place it in the outer WHERE (would drop NULL-extended \
                             rows): {:?}",
                            optional_from_alias, other
                        )));
                    }
                }
            }
            // No `kept` remainder: the whole post-OPTIONAL WHERE moves
            // into the ON condition, so `render_plan.filters` stays
            // cleared (no outer WHERE for this segment).
        }
        if !extra_on_conditions.is_empty() {
            log::info!(
                "🔧 build_chained_with_match_cte_plan: #462/#472 moved {} WHERE conjunct(s) (incl. pure-anchor) into LEFT JOIN ON for alias '{}'",
                extra_on_conditions.len(),
                optional_from_alias
            );
        }

        let mut optional_joining_on = join_conditions.to_vec();
        optional_joining_on.extend(extra_on_conditions);

        let optional_from_join = super::Join {
            table_name: old_from.name.clone(),
            table_alias: optional_from_alias.clone(),
            joining_on: optional_joining_on,
            join_type: super::JoinType::Left,
            pre_filter: optional_pre_filter,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
            is_cartesian: false,
        };
        render_plan.joins.0.insert(0, optional_from_join);
        log::info!(
            "🔧 build_chained_with_match_cte_plan: #453 post-WITH OPTIONAL restructure — FROM={} AS {}, LEFT JOIN {} AS {}",
            cte_name, cte_alias, old_from.name, optional_from_alias
        );
    } else {
        // Insert the CTE join at the BEGINNING of the joins list
        // (CTE should be joined first so its columns are available)
        // BUT: skip if a JOIN for this CTE alias already exists (from extract_joins)
        // OR if the FROM table already uses this alias (avoid duplicate alias error)
        let already_has_cte_join = render_plan
            .joins
            .0
            .iter()
            .any(|j| j.table_alias == cte_alias);
        let from_already_uses_alias = render_plan
            .from
            .0
            .as_ref()
            .map(|vr| vr.alias.as_deref() == Some(cte_alias))
            .unwrap_or(false);
        if !already_has_cte_join && !from_already_uses_alias {
            render_plan.joins.0.insert(0, cte_join.clone());
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Added CTE JOIN: {} AS {}",
                cte_name,
                cte_alias
            );
        } else {
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Skipping CTE JOIN {} AS {} (already present from extract_joins)",
                cte_name,
                cte_alias
            );
        }
    }
    Ok(())
}

/// Post-render cross-table WITH CTE-JOIN pass (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Handles patterns like `WITH a, b MATCH (c)-[]->(d) WHERE a.x = c.x`: the
/// final `render_plan.from` is the fresh post-WITH table (`c`), but the aliases
/// exported by the WITH barrier (`a`, `b`) live in a CTE that isn't joined yet.
/// When FROM is a real table (not a `with_*` CTE) and CTE references exist, add
/// the CTE JOIN(s) — reconstructing each ON condition from the correlation
/// predicates captured before the plan was transformed
/// (`original_correlation_predicates`) / the pre-WITH filter, and rewrite the
/// affected SELECT/JOIN references onto the CTE composite alias.
///
/// #593: skipped for a Cypher-UNION base arm (the caller passes
/// `is_cypher_union_plan`) — that FROM is the first arm's own independent scan
/// and must never be cross-joined to another arm's WITH-CTE, since the
/// accumulated `cte_references` belongs to that other arm.
#[allow(clippy::too_many_arguments)]
fn resolve_cross_table_with_cte_joins(
    render_plan: &mut RenderPlan,
    cte_references: &HashMap<String, String>,
    cte_schemas: &crate::render_plan::CteSchemas,
    original_correlation_predicates: &[LogicalExpr],
    current_plan: &LogicalPlan,
    schema: &GraphSchema,
    scope: Option<&super::variable_scope::VariableScope>,
    is_cypher_union_plan: bool,
) -> RenderPlanBuilderResult<()> {
    if let FromTableItem(Some(from_ref)) = &render_plan.from {
        // #593: skip for a Cypher-UNION base arm — its FROM is the first arm's
        // own independent scan and must never be cross-joined to another arm's
        // WITH-CTE (the accumulated `cte_references` belongs to that other arm).
        // Check if FROM is NOT a CTE (i.e., it's a regular table from the second MATCH)
        if !from_ref.name.starts_with("with_")
            && !cte_references.is_empty()
            && !is_cypher_union_plan
        {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: FROM '{}' is not a CTE, checking for CTE joins needed",
                from_ref.name
            );
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Available CTE references: {:?}",
                cte_references
            );

            // Collect all CTE aliases that need to be joined
            // Group by CTE name since multiple aliases can come from the same CTE
            let mut cte_join_needed: HashMap<String, Vec<String>> = HashMap::new();
            for (alias, cte_name) in cte_references {
                cte_join_needed
                    .entry(cte_name.clone())
                    .or_default()
                    .push(alias.clone());
            }

            // For each CTE that's referenced, create a JOIN
            // Sort for deterministic ordering
            let mut sorted_cte_joins: Vec<_> = cte_join_needed.into_iter().collect();
            sorted_cte_joins.sort_by(|a, b| a.0.cmp(&b.0));
            for (cte_name, aliases) in sorted_cte_joins {
                // Extract CTE alias part from name: "with_a_b_cte_1" -> "a_b"
                let cte_alias = if let Some(stripped) = cte_name.strip_prefix("with_") {
                    if let Some(cte_pos) = stripped.rfind("_cte") {
                        stripped[..cte_pos].to_string()
                    } else {
                        stripped.to_string()
                    }
                } else {
                    cte_name.clone()
                };

                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Creating JOIN to CTE '{}' AS '{}' for aliases {:?}",
                    cte_name, cte_alias, aliases
                );

                // Use the correlation predicates that were extracted from the ORIGINAL plan
                // BEFORE transformations (stored in original_correlation_predicates)
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Using {} ORIGINAL correlation predicates",
                    original_correlation_predicates.len()
                );

                // Convert correlation predicates to join conditions using CTE column names
                let mut join_conditions: Vec<OperatorApplication> = Vec::new();

                // If we found correlation predicates, convert them to JOIN ON conditions
                for pred in original_correlation_predicates {
                    // Convert LogicalExpr predicate to RenderExpr and then extract OperatorApplication
                    if let Ok(RenderExpr::OperatorApplicationExp(op_app)) =
                        RenderExpr::try_from(pred.clone())
                    {
                        // Rewrite the operands to use CTE column names
                        let rewritten = rewrite_operator_application_for_cte_join(
                            &op_app,
                            &cte_alias,
                            cte_references,
                        );
                        log::debug!(
                            "🔧 build_chained_with_match_cte_plan: Added JOIN condition from correlation predicate: {:?}",
                            rewritten
                        );
                        join_conditions.push(rewritten);
                    }
                }

                // If we have no correlation conditions but have filter predicates, try those
                if join_conditions.is_empty() {
                    if let Some(filter_expr) = &render_plan.filters.0 {
                        log::debug!("🔧 build_chained_with_match_cte_plan: No correlation predicates, checking filters");
                        // Try to extract join conditions from filters
                        if let Some(join_cond) = extract_cte_join_condition_from_filter(
                            filter_expr,
                            &cte_alias,
                            &aliases,
                            cte_references,
                            cte_schemas,
                        ) {
                            join_conditions.push(join_cond);
                            log::debug!("🔧 build_chained_with_match_cte_plan: Extracted JOIN condition from filter");
                        }
                    }
                }

                // VLP-specific: when FROM is a VLP CTE, generate join condition
                // connecting the WITH CTE's ID column to the VLP CTE's start_id or end_id
                if join_conditions.is_empty() {
                    generate_vlp_with_cte_join_conditions(
                        render_plan,
                        &cte_name,
                        &cte_alias,
                        cte_schemas,
                        &mut join_conditions,
                    );
                }

                // Create the JOIN. `ON 1 = 1` (cartesian) is only correct for a
                // scalar / uncorrelated CTE carry-forward. If the CTE alias is
                // pattern-correlated to a fresh node (a graph edge connects them)
                // but we failed to resolve a join key, emitting `ON 1 = 1` would
                // silently produce a cartesian product with the wrong row count —
                // a semantics change the engine must never make. Return a clean
                // error instead. #451
                let cte_join_conditions = if join_conditions.is_empty() {
                    let correlated = aliases
                        .iter()
                        .any(|a| alias_has_pattern_correlation(current_plan, a))
                        || alias_has_pattern_correlation(current_plan, &cte_alias);
                    if correlated {
                        return Err(RenderBuildError::InvalidRenderPlan(format!(
                            "WITH-CTE '{}' is pattern-correlated to a fresh node via alias(es) {:?}, \
                             but no join key could be resolved; refusing to emit a cartesian \
                             `ON 1 = 1` join (would silently change query semantics)",
                            cte_name, aliases
                        )));
                    }
                    use crate::render_plan::render_expr::Literal as RenderLiteral;
                    vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::Literal(RenderLiteral::Integer(1)),
                            RenderExpr::Literal(RenderLiteral::Integer(1)),
                        ],
                    }]
                } else {
                    join_conditions.clone()
                };
                let cte_join = super::Join {
                    table_name: cte_name.clone(),
                    table_alias: cte_alias.clone(),
                    joining_on: cte_join_conditions,
                    join_type: super::JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                    is_cartesian: false,
                };

                // #453: post-WITH OPTIONAL restructure, else insert the CTE JOIN.
                restructure_post_with_optional_or_insert_cte_join(
                    render_plan,
                    current_plan,
                    &cte_name,
                    &cte_alias,
                    &join_conditions,
                    &cte_join,
                    schema,
                )?;

                // Also add the WITH CTE JOIN to each Union branch
                // The main plan's joins only apply to the first branch (outgoing).
                // Incoming branches in union.input[] need their own JOIN.
                if let Some(ref mut union) = render_plan.union.0 {
                    for branch in union.input.iter_mut() {
                        // Skip if this branch already has the CTE join
                        let branch_already_has =
                            branch.joins.0.iter().any(|j| j.table_alias == cte_alias);
                        if branch_already_has {
                            continue;
                        }

                        if let FromTableItem(Some(ref branch_from)) = branch.from {
                            if branch_from.name.starts_with("vlp_") {
                                // Find the VLP CTE metadata to determine the correct join column
                                let mut branch_join_cond = Vec::new();
                                for vlp_cte in &render_plan.ctes.0 {
                                    if vlp_cte.cte_name == branch_from.name {
                                        let is_start = vlp_cte.vlp_cypher_start_alias.as_deref()
                                            == Some(cte_alias.as_str());
                                        let is_end = vlp_cte.vlp_cypher_end_alias.as_deref()
                                            == Some(cte_alias.as_str());
                                        if is_start || is_end {
                                            let vlp_id_col =
                                                if is_start { "start_id" } else { "end_id" };
                                            let from_alias =
                                                branch_from.alias.as_deref().unwrap_or("t");
                                            let vlp_alias_for_id = if is_start {
                                                vlp_cte
                                                    .vlp_cypher_start_alias
                                                    .as_deref()
                                                    .unwrap_or(&cte_alias)
                                            } else {
                                                vlp_cte
                                                    .vlp_cypher_end_alias
                                                    .as_deref()
                                                    .unwrap_or(&cte_alias)
                                            };
                                            let id_col_name = if let Some(meta) =
                                                cte_schemas.get(&cte_name)
                                            {
                                                meta.alias_to_id
                                                    .get(vlp_alias_for_id)
                                                    .cloned()
                                                    .or_else(|| {
                                                        let prefix =
                                                            format!("{}_", vlp_alias_for_id);
                                                        meta.select_items.iter().find_map(|item| {
                                                            if let Some(col_alias) = &item.col_alias
                                                            {
                                                                let name = &col_alias.0;
                                                                if name.starts_with(&prefix)
                                                                    && name.ends_with("_id")
                                                                {
                                                                    return Some(name.clone());
                                                                }
                                                            }
                                                            None
                                                        })
                                                    })
                                                    .unwrap_or_else(|| {
                                                        find_id_column_in_cte(
                                                            &cte_name,
                                                            vlp_alias_for_id,
                                                            &render_plan.ctes,
                                                        )
                                                    })
                                            } else {
                                                find_id_column_in_cte(
                                                    &cte_name,
                                                    vlp_alias_for_id,
                                                    &render_plan.ctes,
                                                )
                                            };
                                            // Wrap BOTH sides in toString() to handle type mismatches:
                                            // VLP start_id/end_id may be UInt64 or String depending on generation path.
                                            // CTE columns are typically UInt64 (raw IDs). toString() on both sides
                                            // ensures consistent String comparison regardless of input types.
                                            let cond = OperatorApplication {
                                                operator: Operator::Equal,
                                                operands: vec![
                                                    RenderExpr::ScalarFnCall(ScalarFnCall {
                                                        name: current_function_mapper().cast_string().to_string(),
                                                        args: vec![RenderExpr::Column(Column(
                                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                                format!("{}.{}", from_alias, vlp_id_col)
                                                            )
                                                        ))],
                                                    }),
                                                    RenderExpr::ScalarFnCall(ScalarFnCall {
                                                        name: current_function_mapper().cast_string().to_string(),
                                                        args: vec![RenderExpr::Column(Column(
                                                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                                format!("{}.{}", cte_alias, id_col_name)
                                                            )
                                                        ))],
                                                    }),
                                                ],
                                            };
                                            log::debug!(
                                                "🔧 VLP+WITH (branch): Generated JOIN for '{}': {}.{} = {}.{}",
                                                branch_from.name, from_alias, vlp_id_col, cte_alias, id_col_name
                                            );
                                            branch_join_cond.push(cond);
                                        }
                                        break;
                                    }
                                }
                                let branch_cte_join = super::Join {
                                    table_name: cte_name.clone(),
                                    table_alias: cte_alias.clone(),
                                    joining_on: branch_join_cond,
                                    join_type: super::JoinType::Inner,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                    is_cartesian: false,
                                };
                                branch.joins.0.insert(0, branch_cte_join);
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Added CTE JOIN to Union branch FROM '{}'",
                                    branch_from.name
                                );

                                // Rewrite Union branch SELECT items to use CTE column names
                                // Use scope-based rewriting (replaces removed rewrite_cte_expression)
                                if let Some(scope) = scope {
                                    use super::variable_scope::rewrite_render_expr;
                                    for item in branch.select.items.iter_mut() {
                                        item.expression =
                                            rewrite_render_expr(&item.expression, scope);
                                    }
                                    log::info!(
                                        "🔧 build_chained_with_match_cte_plan: Rewrote Union branch SELECT via scope for CTE"
                                    );
                                }
                            } else {
                                // Non-VLP branch (regular table FROM): add CTE as cross-join (ON 1=1)
                                // This handles post-WITH MATCH patterns with undirected edges
                                // where UnionDistribution created Union branches with regular table FROM
                                use crate::render_plan::render_expr::Literal as RenderLiteral;
                                let branch_cte_join = super::Join {
                                    table_name: cte_name.clone(),
                                    table_alias: cte_alias.clone(),
                                    joining_on: if join_conditions.is_empty() {
                                        vec![OperatorApplication {
                                            operator: Operator::Equal,
                                            operands: vec![
                                                RenderExpr::Literal(RenderLiteral::Integer(1)),
                                                RenderExpr::Literal(RenderLiteral::Integer(1)),
                                            ],
                                        }]
                                    } else {
                                        join_conditions.clone()
                                    },
                                    join_type: super::JoinType::Inner,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                    is_cartesian: false,
                                };
                                branch.joins.0.insert(0, branch_cte_join);
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Added CTE cross-JOIN to non-VLP Union branch FROM '{}'",
                                    branch_from.name
                                );
                            }
                        }
                    }
                }
            }

            // After adding CTE joins, we need to rewrite SELECT items that reference CTE aliases
            // to use the CTE composite alias (e.g., a.name -> a_b.a_name)
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Rewriting SELECT items for CTE references"
            );
        }
    }
    Ok(())
}

/// Final outer-scope resolution pass (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Resolves bare node aliases (`b` → `b.id`, `a` → `cte.p1_a_id`) and composite
/// orphan aliases in the outer query's SELECT / WHERE / JOIN / GROUP BY, and
/// (`fix_orphan_table_aliases`) adds a CROSS JOIN for any scope CTE not already
/// in FROM/JOINs. The caller gates this on `!with_scope.is_empty()` and runs it
/// AFTER cross-table CTE JOINs are added so JOIN conditions are rewritten too.
///
/// #593: for a Cypher UNION each arm is an independent query already resolved
/// per-arm during branch rendering, yet `final_scope` carries EVERY arm's
/// WITH-CTE variables — running the whole-plan passes with it would leak one
/// arm's CTE into a sibling that merely reuses the same Cypher alias name (e.g.
/// `u.user_id` rewritten to another arm's `c_u.p1_u_user_id`, or a spurious
/// CROSS JOIN onto that CTE). So for a union we detach the arms first, process
/// the base in isolation, then each `union.input` branch on its own — none
/// recursing into siblings with the base's scope.
fn apply_final_outer_scope_passes(
    render_plan: &mut RenderPlan,
    final_scope: &super::variable_scope::VariableScope,
    is_cypher_union_plan: bool,
) {
    if is_cypher_union_plan {
        let detached_union = render_plan.union.0.take();
        apply_outer_scope_passes(render_plan, final_scope);
        if let Some(mut union_data) = detached_union {
            for branch in &mut union_data.input {
                apply_outer_scope_passes(branch, final_scope);
            }
            render_plan.union = UnionItems(Some(union_data));
        }
    } else {
        super::variable_scope::rewrite_bare_variables_in_plan(render_plan, final_scope);
        super::variable_scope::fix_orphan_table_aliases(render_plan, final_scope);
    }
}

/// Pre-render join pruning against the last CTE (finalization tail of
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// The last CTE (the one with the most exported aliases) already covers some of
/// the joins the final plan still carries. Pattern: `WITH a, b … MATCH
/// (b)-[]->(c)` leaves GraphJoins for `a→t1→b` and `b→t2→c`, but `a→t1→b` is
/// already inside `with_a_b_cte…`. Derive the exported aliases from the last CTE
/// name (`with_{aliases}_cte{N}` → `{aliases}`), prune the covered joins, and
/// refresh `GraphJoins.cte_references` with the latest mapping.
///
/// #451: cross-barrier correlations that pruning removes (e.g. an FK-edge join
/// `c.customer_id = o.customer_id` connecting the CTE alias to a fresh post-WITH
/// node) are captured and folded into `original_correlation_predicates`, so the
/// CTE JOIN is later rebuilt with the real ON condition instead of a cartesian
/// `ON 1 = 1`. No-op when there are no CTEs or the last CTE exports no aliases.
fn prune_joins_covered_by_last_cte(
    current_plan: &mut LogicalPlan,
    original_correlation_predicates: &mut Vec<LogicalExpr>,
    all_ctes: &[Cte],
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    with_scope: &WithBarrierScope,
) -> RenderPlanBuilderResult<()> {
    log::info!(
        "🔧 build_chained_with_match_cte_plan: PRE-RENDER CHECK - have {} CTEs",
        all_ctes.len()
    );

    if !all_ctes.is_empty() {
        // Get the last CTE's exported aliases (from its name, e.g., "with_a_b_cte2" → ["a", "b"])
        // Safety: !is_empty() guarantees last() returns Some
        let last_cte = all_ctes.last().expect("all_ctes is non-empty");
        let last_cte_name = &last_cte.cte_name;

        // Extract aliases from CTE name: "with_a_b_cte2" → "a_b"
        // Format is: with_{aliases}_cte{N}
        // Strategy: trim "with_", then remove "_cte{N}" suffix
        let alias_part = if let Some(stripped) = last_cte_name.strip_prefix("with_") {
            // Find the last occurrence of "_cte" and take everything before it
            if let Some(cte_pos) = stripped.rfind("_cte") {
                &stripped[..cte_pos]
            } else {
                stripped
            }
        } else {
            ""
        };

        log::info!(
            "🔧 build_chained_with_match_cte_plan: Last CTE '{}' exports alias_part: '{}'",
            last_cte_name,
            alias_part
        );

        // For composite aliases like "a_b", split into individual aliases
        if !alias_part.is_empty() {
            let exported_aliases: Vec<&str> = alias_part.split('_').collect();
            let exported_aliases_set: std::collections::HashSet<&str> =
                exported_aliases.iter().copied().collect();

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Exported aliases: {:?}",
                exported_aliases
            );

            // Now we need to prune joins from GraphJoins that are covered by this CTE
            // AND update any GraphNode that matches an exported alias to reference the CTE
            log::debug!(
                "🔀 UNION_TRACE before prune_joins: has_union={}",
                current_plan.has_union_anywhere()
            );
            // Cross-barrier correlations that prune removes (e.g. the FK-edge join
            // `c.customer_id = o.customer_id` connecting the CTE alias to a fresh
            // post-WITH node) are captured here and folded into the correlation
            // predicates so the CTE JOIN is rebuilt with the real ON condition
            // instead of a cartesian `ON 1 = 1`. #451
            let mut pruned_correlations: Vec<crate::query_planner::logical_expr::LogicalExpr> =
                Vec::new();
            *current_plan = prune_joins_covered_by_cte(
                current_plan,
                last_cte_name,
                &exported_aliases_set,
                cte_schemas,
                &mut pruned_correlations,
            )?;
            if !pruned_correlations.is_empty() {
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Recovered {} cross-barrier correlation(s) from pruned joins",
                    pruned_correlations.len()
                );
                original_correlation_predicates.extend(pruned_correlations);
            }

            // CRITICAL: Update all GraphJoins.cte_references with the latest CTE mapping
            // After replacement, the plan may have GraphJoins with stale cte_references from analyzer
            // Build property mappings from scope_cte_variables for column resolution
            let cte_prop_mappings: std::collections::HashMap<
                String,
                std::collections::HashMap<String, String>,
            > = with_scope
                .scope_cte_variables()
                .iter()
                .map(|(alias, info)| (alias.clone(), info.property_mapping.clone()))
                .collect();
            log::debug!("🔧 build_chained_with_match_cte_plan: Updating GraphJoins.cte_references with latest mapping: {:?}", cte_references);
            *current_plan =
                update_graph_joins_cte_refs(current_plan, cte_references, &cte_prop_mappings)?;
        }
    }
    Ok(())
}

/// Build the per-iteration WITH-processing work-list (a STEP of the main loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Given the WITH clauses `find_all_with_clauses_grouped` produced for the
/// current plan, this records the analyzer's CTE names (for later remapping
/// after nested WITHs collapse), keeps only the INNERMOST WITH per alias (those
/// whose input has no further nested WITH — the others process on subsequent
/// iterations), and returns the aliases to process this iteration sorted
/// innermost-first (fewer `_` = more inner, so `friend` before `friend_post`).
///
/// Returns `(all_analyzer_cte_names, filtered_grouped_withs, aliases_to_process)`.
#[allow(clippy::type_complexity)]
fn build_iteration_worklist(
    current_plan: &LogicalPlan,
    grouped_withs: std::collections::HashMap<String, Vec<LogicalPlan>>,
) -> (
    std::collections::HashSet<String>,
    std::collections::HashMap<String, Vec<LogicalPlan>>,
    Vec<(String, usize)>,
) {
    // CRITICAL: Collect ALL analyzer CTE names from ALL WITH clauses in the plan tree
    // This includes nested WITHs that will be collapsed later. We need to record
    // the analyzer's CTE names now so we can remap them after collapsing.

    let mut all_analyzer_cte_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    collect_analyzer_cte_names(current_plan, &mut all_analyzer_cte_names);
    log::info!(
        "🔧 build_chained_with_match_cte_plan: Collected {} analyzer CTE names: {:?}",
        all_analyzer_cte_names.len(),
        all_analyzer_cte_names
    );

    // CRITICAL FIX: For aliases with multiple WITH clauses (nested consecutive WITH with same alias),
    // we should only process the INNERMOST one per iteration. The others will be processed
    // in subsequent iterations after the inner one is converted to a CTE.
    //
    // Filter strategy: For each alias, only keep the WITH clause whose input has NO nested WITH clauses.
    // This is the "innermost" WITH that should be processed first.
    let mut filtered_grouped_withs: std::collections::HashMap<String, Vec<LogicalPlan>> =
        std::collections::HashMap::new();

    // Also track the original analyzer CTE name for each innermost WithClause
    let mut original_analyzer_cte_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    for (alias, plans) in grouped_withs {
        // NOTE: We do NOT skip aliases that were processed in previous iterations.
        // Multiple WITH clauses can share the same alias key (e.g., two consecutive
        // "WITH DISTINCT country, a, b" barriers). Each must be processed as a separate CTE.
        // The innermost filtering below handles ordering: only WITHs whose input has
        // no nested WITH clauses are processed in each iteration.

        // Record original count before filtering
        let original_count = plans.len();

        // Find plans that are innermost (no nested WITH in their input)
        let innermost_plans: Vec<LogicalPlan> = plans
                .into_iter()
                .filter(|plan| {
                    if let LogicalPlan::WithClause(wc) = plan {
                        let has_nested = plan_contains_with_clause(&wc.input);
                        if has_nested {
                            log::debug!("🔧 build_chained_with_match_cte_plan: Skipping WITH '{}' with nested WITH clauses (will process in next iteration). Input plan type: {:?}", alias, std::mem::discriminant(wc.input.as_ref()));
                            // Show what's inside this WITH's input tree
                            show_plan_structure(&wc.input, 0);
                        } else {
                            log::debug!("🔧 build_chained_with_match_cte_plan: Keeping innermost WITH '{}' for processing", alias);
                            // Capture the original analyzer CTE name for this innermost WithClause
                            if let Some(analyzer_cte_name) = wc.cte_references.get(&alias) {
                                original_analyzer_cte_names.insert(alias.clone(), analyzer_cte_name.clone());
                                log::debug!("🔧 build_chained_with_match_cte_plan: Captured original analyzer CTE name '{}' for alias '{}'", analyzer_cte_name, alias);
                            } else {
                                log::debug!("🔧 build_chained_with_match_cte_plan: No analyzer CTE name found for innermost WITH '{}'", alias);
                            }
                        }
                        !has_nested
                    } else {
                        log::debug!("🔧 build_chained_with_match_cte_plan: Plan for alias '{}' is not WithClause: {:?}", alias, std::mem::discriminant(plan));
                        true  // Not a WithClause, keep it
                    }
                })
                .collect();

        if !innermost_plans.is_empty() {
            log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}': filtered {} plan(s) to {} innermost",
                           alias, original_count, innermost_plans.len());
            filtered_grouped_withs.insert(alias, innermost_plans);
        } else {
            log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}': NO innermost plans after filtering {} total",
                           alias, original_count);
        }
    }

    // DEBUG: Log the contents of original_analyzer_cte_names right after population
    log::debug!(
        "🔧 DEBUG: original_analyzer_cte_names after innermost filtering: {:?}",
        original_analyzer_cte_names
    );

    // Collect alias info for processing (to avoid holding references across mutation)
    let mut aliases_to_process: Vec<(String, usize)> = filtered_grouped_withs
        .iter()
        .map(|(alias, plans)| (alias.clone(), plans.len()))
        .collect();

    // Sort aliases to process innermost first (simpler names = fewer underscores = more inner)
    // This ensures "friend" is processed before "friend_post"
    aliases_to_process.sort_by(|a, b| {
        let a_depth = a.0.matches('_').count();
        let b_depth = b.0.matches('_').count();
        a_depth.cmp(&b_depth)
    });
    log::info!(
        "🔧 build_chained_with_match_cte_plan: Sorted aliases: {:?}",
        aliases_to_process
            .iter()
            .map(|(a, _)| a)
            .collect::<Vec<_>>()
    );

    (
        all_analyzer_cte_names,
        filtered_grouped_withs,
        aliases_to_process,
    )
}

/// Prepare a WITH alias group's plans for rendering and collect its pre-WITH
/// aliases (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Refreshes every plan's `GraphJoins.cte_references` from the
/// previous-iterations-only snapshot (so GraphRel nodes see the CTEs available
/// before this alias's own CTE is built), then collects the table aliases
/// defined INSIDE the WITH clauses (`Projection(With)` input) that must be
/// filtered out of the outer query's joins — excluding the WITH boundary
/// variable itself and any alias that is already a CTE reference from an earlier
/// iteration (`processed_cte_aliases`).
///
/// Returns `(refreshed_with_plans, pre_with_aliases)`.
fn prepare_with_plans_and_pre_aliases(
    with_plans: Vec<LogicalPlan>,
    cte_references_for_rendering: &HashMap<String, String>,
    with_alias: &str,
    processed_cte_aliases: &std::collections::HashSet<String>,
) -> RenderPlanBuilderResult<(Vec<LogicalPlan>, std::collections::HashSet<String>)> {
    let with_plans: Vec<LogicalPlan> = with_plans
        .into_iter()
        .map(|plan| {
            update_graph_joins_cte_refs(
                &plan,
                cte_references_for_rendering,
                &std::collections::HashMap::new(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Collect aliases from the pre-WITH scope (inside the WITH clauses)
    // These aliases should be filtered out from the outer query's joins
    let mut pre_with_aliases = std::collections::HashSet::new();
    for with_plan in with_plans.iter() {
        // For Projection(With), the input contains the pre-WITH pattern
        if let LogicalPlan::Projection(proj) = with_plan {
            let inner_aliases = collect_aliases_from_plan(&proj.input);
            pre_with_aliases.extend(inner_aliases);
        }
    }
    // Don't filter out the WITH variable itself - it's the boundary variable
    pre_with_aliases.remove(with_alias);
    // Don't filter out aliases that are already CTEs (processed in earlier iterations)
    // These are now references to CTEs, not original tables
    for cte_alias in processed_cte_aliases {
        if pre_with_aliases.remove(cte_alias) {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Keeping '{}' (already a CTE reference)",
                cte_alias
            );
        }
    }
    log::info!(
        "🔧 build_chained_with_match_cte_plan: Pre-WITH aliases to filter: {:?}",
        pre_with_aliases
    );

    Ok((with_plans, pre_with_aliases))
}

/// Derive the CTE name (plus exported-alias / pattern-comprehension metadata)
/// for one WITH alias group (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Takes the exported aliases and pattern-comprehension metadata from the first
/// WITH plan, then resolves the CTE name: preferring `WithClause.cte_name` (set
/// once by the analyzer's `CteSchemaResolver` — the single source of truth,
/// consistent counters) and only falling back to a freshly generated name when
/// absent. The name is then deduped against hoisted/analyzer collisions
/// (`sync_hoisted` + `resolve_unique_name`, which advances the sequence
/// counter), and the analyzer→final remapping is recorded
/// (`record_base_remapping`) — hence the `&mut CteNameAllocator`.
///
/// Returns `(exported_aliases, pattern_comprehensions, cte_name)`.
fn derive_with_cte_name(
    with_plans: &[LogicalPlan],
    with_alias: &str,
    all_ctes: &[Cte],
    all_analyzer_cte_names: &std::collections::HashSet<String>,
    cte_name_allocator: &mut CteNameAllocator,
) -> (
    Vec<String>,
    Vec<crate::query_planner::logical_plan::PatternComprehensionMeta>,
    String,
) {
    // Extract ALL exported aliases from the first WITH clause plan
    // Use them to generate the CTE name (not just the grouped alias)
    // This matches what the analyzer expects: with_<all_aliases>_cte_<seq>
    let exported_aliases: Vec<String> = with_plans
        .first()
        .and_then(|plan| match plan {
            LogicalPlan::WithClause(wc) => Some(wc.exported_aliases.clone()),
            _ => None,
        })
        .unwrap_or_else(|| vec![with_alias.to_string()]);

    // Extract pattern comprehension metadata from the WithClause
    let pattern_comprehensions: Vec<crate::query_planner::logical_plan::PatternComprehensionMeta> = with_plans
        .first()
        .and_then(|plan| match plan {
            LogicalPlan::WithClause(wc) if !wc.pattern_comprehensions.is_empty() => {
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Found {} pattern comprehensions for alias '{}'",
                    wc.pattern_comprehensions.len(), with_alias
                );
                Some(wc.pattern_comprehensions.clone())
            }
            _ => None,
        })
        .unwrap_or_default();

    // Sorted aliases string used for sequence tracking and uniqueness
    let mut sorted_exported_aliases = exported_aliases.clone();
    sorted_exported_aliases.sort();
    let aliases_key = sorted_exported_aliases.join("_");

    // CRITICAL FIX: Use CTE name from analyzer's cte_references if available
    // **ARCHITECTURAL FIX (Jan 25, 2026)**: Use WithClause.cte_name directly from analysis phase
    // The CteSchemaResolver in the analyzer already generated the final CTE name with counter
    // and stored it in WithClause.cte_name. We should use it directly instead of regenerating.
    //
    // Why this is the right approach:
    // 1. CTE names are generated ONCE during analysis with consistent counters (plan_ctx.cte_counter)
    // 2. WithClause.cte_name stores the final name (e.g., "with_a_b_cte_1")
    // 3. Rendering should just USE this name, not try to regenerate with different counters
    //
    // The old approach tried to extract from cte_references HashMap, which is:
    // - Incomplete: only contains CTEs that other nodes explicitly reference
    // - Inconsistent: regenerates counters instead of using analysis phase values
    // - Source of two-phase mismatch: analysis generates "with_a_b_cte_1", rendering tries "with_a_b_cte_2"
    let cte_name = with_plans
        .first()
        .and_then(|plan| match plan {
            LogicalPlan::WithClause(wc) => {
                // Use the CTE name set by CteSchemaResolver in analysis phase
                // This is the single source of truth for the WITH clause's CTE name
                wc.cte_name.clone()
            }
            _ => None,
        })
        .unwrap_or_else(|| {
            cte_name_allocator.next_fallback_name(
                &aliases_key,
                &sorted_exported_aliases,
                &exported_aliases,
            )
        });

    // Ensure used_cte_names contains any CTEs hoisted earlier in this pass
    cte_name_allocator.sync_hoisted(all_ctes);

    // Resolve to a unique name (dedup against hoisted/analyzer collisions) and
    // advance the sequence counter based on its suffix.
    let cte_name =
        cte_name_allocator.resolve_unique_name(cte_name, &aliases_key, &sorted_exported_aliases);

    log::debug!(
        "🔧 build_chained_with_match_cte_plan: Using CTE name '{}' for exported aliases {:?}",
        cte_name,
        exported_aliases
    );

    // CRITICAL: Collect CTE name remapping from analyzer's CTE names to our generated name.
    // Any analyzer CTE name with the same base alias pattern is remapped to our name.
    cte_name_allocator.record_base_remapping(&cte_name, all_analyzer_cte_names);

    (exported_aliases, pattern_comprehensions, cte_name)
}

/// Combine the rendered WITH plans for one alias group into a single CTE body
/// (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// A single render becomes the CTE body directly. Multiple renders (the same
/// alias exported by several Union branches) are combined under a `UNION ALL`
/// wrapper: the per-branch ORDER BY / SKIP / LIMIT / HAVING are cleared and
/// lifted onto the wrapper (taken from the first render, since all branches
/// carry the same WITH-clause modifiers).
fn combine_with_renders_into_cte(
    mut rendered_plans: Vec<RenderPlan>,
    with_alias: &str,
    cte_name: &str,
) -> RenderPlan {
    // Extract ORDER BY, SKIP, LIMIT from first rendered plan (they should all have the same modifiers)
    // These come from the WithClause and were applied to each rendered plan earlier
    let first_order_by = if !rendered_plans.is_empty() && !rendered_plans[0].order_by.0.is_empty() {
        Some(rendered_plans[0].order_by.clone())
    } else {
        None
    };
    let first_skip = rendered_plans.first().and_then(|p| p.skip.0);
    let first_limit = rendered_plans.first().and_then(|p| p.limit.0);

    let with_cte_render = if rendered_plans.len() == 1 {
        // Safety: len() == 1 guarantees pop() returns Some
        rendered_plans
            .pop()
            .expect("rendered_plans has exactly one element")
    } else {
        // Multiple WITH clauses with same alias - create UNION ALL CTE
        log::debug!("🔧 build_chained_with_match_cte_plan: Combining {} WITH renders with UNION ALL for alias '{}'",
                   rendered_plans.len(), with_alias);

        // Clear ORDER BY/SKIP/LIMIT/HAVING from individual plans - they'll be applied to the UNION wrapper
        let first_having = rendered_plans.first().and_then(|p| p.having_clause.clone());
        for plan in &mut rendered_plans {
            plan.order_by = OrderByItems(vec![]);
            plan.skip = SkipItem(None);
            plan.limit = LimitItem(None);
            plan.having_clause = None;
        }

        // Create a wrapper RenderPlan with UnionItems, preserving ORDER BY/SKIP/LIMIT/HAVING
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![],
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(vec![]),
            array_join: ArrayJoinItem(Vec::new()),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: first_having,
            order_by: first_order_by.unwrap_or_else(|| OrderByItems(vec![])),
            skip: SkipItem(first_skip),
            limit: LimitItem(first_limit),
            union: UnionItems(Some(Union {
                input: rendered_plans,
                union_type: crate::render_plan::UnionType::All,
                is_cypher_union: false,
            })),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        }
    };

    log::info!(
        "🔧 build_chained_with_match_cte_plan: Created CTE '{}'",
        cte_name
    );

    with_cte_render
}

/// Apply the WITH clause's pattern comprehensions to its CTE (a STEP of the main
/// loop's `'alias_loop` in `build_chained_with_match_cte_plan`, Phase-4 §7.1
/// extraction).
///
/// If the WithClause carries pattern comprehensions, handle them: when
/// `pattern_hops` are populated (multi-hop / multi-correlation patterns),
/// generate pre-aggregated CTE + LEFT JOIN (for list-unconstrained PCs) and/or
/// inline `arrayCount` correlated subqueries (for list-constrained PCs);
/// otherwise fall back to the single-hop CTE+LEFT JOIN path. Mutates
/// `with_cte_render` (adds joins / rewrites SELECT) and appends the generated PC
/// CTEs to `all_ctes`. No-op when there are no pattern comprehensions.
#[allow(clippy::too_many_arguments)]
fn apply_pattern_comprehensions(
    with_cte_render: &mut RenderPlan,
    all_ctes: &mut Vec<Cte>,
    with_plans: &[LogicalPlan],
    pattern_comprehensions: &[crate::query_planner::logical_plan::PatternComprehensionMeta],
    cte_name: &str,
    with_alias: &str,
    exported_aliases: &[String],
    schema: &GraphSchema,
    plan_ctx: Option<&PlanCtx>,
    cte_schemas: &crate::render_plan::CteSchemas,
) {
    use super::CteContent;
    if !pattern_comprehensions.is_empty() {
        // Check if any PC has full pattern info for correlated subquery approach
        let has_pattern_hops = pattern_comprehensions
            .iter()
            .any(|pc| !pc.pattern_hops.is_empty());

        if has_pattern_hops {
            // ===== Pre-aggregated CTE + LEFT JOIN approach =====
            // For each PC with pattern_hops and no list_constraint, generate a
            // pre-aggregated CTE with GROUP BY on correlation columns, then LEFT JOIN
            // from the WITH CTE to the PC CTE. This avoids ClickHouse "Cannot clone
            // Union plan step" errors that occur with correlated subqueries + UNION ALL.

            // Separate PCs into CTE-based (no list_constraint) and arrayCount-based
            let cte_pcs: Vec<(
                usize,
                &crate::query_planner::logical_plan::PatternComprehensionMeta,
            )> = pattern_comprehensions
                .iter()
                .enumerate()
                .filter(|(_, pc)| !pc.pattern_hops.is_empty() && pc.list_constraint.is_none())
                .collect();
            let array_count_pcs: Vec<
                &crate::query_planner::logical_plan::PatternComprehensionMeta,
            > = pattern_comprehensions
                .iter()
                .filter(|pc| !pc.pattern_hops.is_empty() && pc.list_constraint.is_some())
                .collect();

            log::info!(
                "🔧 Pattern comprehensions for '{}': {} CTE-based, {} arrayCount-based",
                with_alias,
                cte_pcs.len(),
                array_count_pcs.len(),
            );

            // Phase A: Generate pre-aggregated CTEs for non-list-constraint PCs
            let mut pc_cte_names: Vec<(usize, String)> = Vec::new(); // (pc_index, cte_name)
            for (pc_idx, pc_meta) in &cte_pcs {
                let pc_cte_name = format!("pc_{}_{}", with_alias, pc_idx);

                if let Some(pc_result) = generate_pattern_comprehension_cte(pc_meta, schema) {
                    log::info!(
                        "🔧 PC CTE '{}': {} correlation columns",
                        pc_cte_name,
                        pc_result.correlation_columns.len()
                    );

                    // Push the CTE before the WITH CTE (ordering matters)
                    all_ctes.push(Cte::new(
                        pc_cte_name.clone(),
                        CteContent::RawSql(pc_result.cte_sql),
                        false,
                    ));

                    // Build LEFT JOIN to the PC CTE
                    // ON conditions: pc_cte.corr_N = <corresponding CTE column>
                    let mut join_conditions: Vec<OperatorApplication> = Vec::new();
                    for (cv_idx, (var_name, label, corr_alias)) in
                        pc_result.correlation_columns.iter().enumerate()
                    {
                        // Find the CTE column reference for this correlation variable.
                        // We need to resolve (var_name, id) to a column in the WITH CTE body.
                        let cte_col_ref = find_pc_cte_join_column(
                            var_name,
                            label,
                            schema,
                            with_cte_render,
                            cte_name,
                        );

                        if let Some(cte_ref) = cte_col_ref {
                            // Parse "alias.column" into PropertyAccessExp for proper
                            // dependency tracking in sort_joins_by_dependency
                            let lhs_expr = if let Some(dot_pos) = cte_ref.find('.') {
                                let alias_part = cte_ref[..dot_pos].trim_matches('"').to_string();
                                let col_part = cte_ref[dot_pos + 1..].trim_matches('"').to_string();
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(alias_part),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(col_part),
                                        })
                            } else {
                                RenderExpr::Raw(cte_ref)
                            };
                            join_conditions.push(OperatorApplication {
                                        operator: Operator::Equal,
                                        operands: vec![
                                            lhs_expr,
                                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(pc_cte_name.clone()),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                                    corr_alias.clone(),
                                                ),
                                            }),
                                        ],
                                    });
                        } else {
                            log::warn!(
                                        "⚠️ Could not find CTE column for PC correlation var '{}' (label='{}', cv_idx={})",
                                        var_name,
                                        label,
                                        cv_idx
                                    );
                        }
                    }

                    // Guard: require all correlation predicates to be resolved.
                    // An empty or incomplete join would produce ON 1=1 (Cartesian product).
                    if join_conditions.len() != pc_result.correlation_columns.len() {
                        log::warn!(
                                    "⚠️ PC CTE '{}': only {}/{} join conditions resolved — skipping join (will use 0)",
                                    pc_cte_name,
                                    join_conditions.len(),
                                    pc_result.correlation_columns.len()
                                );
                    } else {
                        let pc_join = Join {
                            table_name: pc_cte_name.clone(),
                            table_alias: pc_cte_name.clone(),
                            joining_on: join_conditions,
                            join_type: JoinType::Left,
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                            is_cartesian: false,
                        };

                        // Add LEFT JOIN to the WITH CTE body.
                        // For UNION plans, add to each branch.
                        add_join_to_plan_or_union_branches(with_cte_render, pc_join);

                        pc_cte_names.push((*pc_idx, pc_cte_name));
                    }
                } else {
                    log::warn!(
                                "⚠️ Could not generate PC CTE for pattern comprehension #{} — falling back to 0",
                                pc_idx
                            );
                }
            }

            // Phase B: Replace count(*) placeholders with COALESCE(pc_cte.result, 0)
            // Build replacement expressions indexed by PC position
            let mut pc_replacements: Vec<String> = Vec::new();
            let mut cte_name_iter = pc_cte_names.iter();
            let mut next_cte = cte_name_iter.next();
            for (pc_idx, pc_meta) in pattern_comprehensions.iter().enumerate() {
                if pc_meta.pattern_hops.is_empty() {
                    continue;
                }
                if pc_meta.list_constraint.is_some() {
                    // Will be handled by arrayCount path — put a placeholder
                    // that will be replaced below
                    pc_replacements.push("__arraycount_placeholder__".to_string());
                    continue;
                }
                if let Some((idx, ref name)) = next_cte {
                    if *idx == pc_idx {
                        pc_replacements.push(format!("COALESCE({}.result, 0)", name));
                        next_cte = cte_name_iter.next();
                    } else {
                        pc_replacements.push("0".to_string());
                    }
                } else {
                    pc_replacements.push("0".to_string());
                }
            }

            // Replace count(*) placeholders in SELECT items
            replace_count_star_placeholders_in_select_or_union(with_cte_render, &pc_replacements);

            // Phase C: Handle arrayCount PCs (list_constraint patterns)
            // These still use the inline approach since they don't need
            // correlated subqueries.
            if !array_count_pcs.is_empty() {
                generate_and_replace_arraycount_pc_subqueries(
                    with_cte_render,
                    pattern_comprehensions,
                    schema,
                    cte_name,
                );
            }

            // Phase D: Ensure node ID column is present in CTE SELECT.
            // When RETURN references path variables (not specific node properties),
            // property requirements may be empty, causing the CTE to omit node
            // columns. But the VLP JOIN needs the node's ID column to join on.
            // Check each exported table alias and ensure its ID column exists.
            log::info!(
                "🔧 Phase D: checking {} exported_aliases: {:?}, CTE select has {} items: {:?}",
                exported_aliases.len(),
                exported_aliases,
                with_cte_render.select.items.len(),
                with_cte_render
                    .select
                    .items
                    .iter()
                    .map(|i| i.col_alias.as_ref().map(|a| a.0.as_str()).unwrap_or("?"))
                    .collect::<Vec<_>>()
            );
            for ea in exported_aliases {
                // Skip non-table aliases (scalar expressions like allNeighboursCount)
                let has_id_col = with_cte_render.select.items.iter().any(|item| {
                    if let Some(ref ca) = item.col_alias {
                        let prefix = format!("p{}_", ea.len());
                        let id_suffix = "_id";
                        ca.0.starts_with(&format!("{}{}_", prefix, ea)) && ca.0.ends_with(id_suffix)
                    } else {
                        false
                    }
                });
                if !has_id_col {
                    // Skip aliases that are ARRAY JOIN scalars (from UNWIND).
                    // After UNWIND, the alias IS the value (e.g., a PersonId), not
                    // a table with columns. Adding `alias.id AS "pN_alias_id"` would
                    // produce invalid SQL because `alias` is a scalar, not a table.
                    //
                    // Detection: the CTE SELECT already has a column whose alias
                    // exactly matches the exported alias (e.g., `... AS "person"`).
                    // Normal node aliases produce pN-prefixed columns (e.g.,
                    // `p6_person_id`), never a bare alias match.
                    // Additionally verify via upstream CTE metadata: if alias_to_id
                    // maps the alias to itself, the scalar IS the ID value.
                    let is_bare_scalar_column = with_cte_render.select.items.iter().any(|item| {
                        item.col_alias.as_ref().map(|ca| ca.0.as_str()) == Some(ea.as_str())
                    });
                    let is_self_id_in_upstream_cte = cte_schemas.values().any(|meta| {
                        meta.alias_to_id.get(ea.as_str()).map(|id| id == ea) == Some(true)
                    });
                    let is_array_join_scalar = is_bare_scalar_column && is_self_id_in_upstream_cte;

                    if is_array_join_scalar {
                        // The scalar value IS the ID. Instead of skipping,
                        // emit `from_alias.scalar_col AS "pN_alias_id"` so
                        // downstream CTEs can resolve `alias.id` through the
                        // standard CTE column naming convention.
                        let from_alias_str = with_cte_render
                            .from
                            .0
                            .as_ref()
                            .and_then(|f| f.alias.as_deref());
                        if let Some(from_alias_str) = from_alias_str {
                            let cte_col =
                                crate::utils::cte_column_naming::cte_column_name(ea, "id");
                            log::info!(
                                "🔧 Phase D: ARRAY JOIN scalar '{}' — emitting {}.{} AS \"{}\"",
                                ea,
                                from_alias_str,
                                ea,
                                cte_col
                            );
                            with_cte_render.select.items.push(SelectItem {
                                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(from_alias_str.to_string()),
                                    column: PropertyValue::Column(ea.to_string()),
                                }),
                                col_alias: Some(ColumnAlias(cte_col)),
                            });
                        } else {
                            log::info!(
                                "🔧 Phase D: ARRAY JOIN scalar '{}' — no FROM alias, skipping",
                                ea
                            );
                        }
                        continue;
                    }

                    // Try to find the ID column from schema
                    if let Some(graph_schema) = crate::server::query_context::get_current_schema() {
                        // Look up table for this alias from plan_ctx
                        let label = plan_ctx.and_then(|ctx| {
                            ctx.get_table_ctx(ea).ok().and_then(|tc| tc.get_label_opt())
                        });
                        if let Some(label) = label {
                            if let Ok(ns) = graph_schema.node_schema(&label) {
                                let id_col = ns.node_id.id.first_column().to_string();
                                let cte_col =
                                    crate::utils::cte_column_naming::cte_column_name(ea, &id_col);
                                log::info!(
                                            "🔧 Phase D: Adding missing ID column '{}' to WITH CTE for alias '{}'",
                                            cte_col, ea
                                        );
                                with_cte_render.select.items.push(SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(ea.to_string()),
                                        column: PropertyValue::Column(id_col),
                                    }),
                                    col_alias: Some(ColumnAlias(cte_col)),
                                });
                            }
                        }
                    }
                }
            }

            log::info!(
                "✅ Pattern comprehension CTEs applied for '{}': {} CTEs created",
                with_alias,
                pc_cte_names.len(),
            );
        } else {
            // ===== LEGACY: CTE + LEFT JOIN approach (simple single-hop, single-correlation) =====
            log::info!(
                "🔧 Generating {} pattern comprehension CTE(s) for WITH alias '{}' (legacy path)",
                pattern_comprehensions.len(),
                with_alias
            );

            for (pc_idx, pc_meta) in pattern_comprehensions.iter().enumerate() {
                let pc_cte_name = format!("pattern_comp_{}_{}", with_alias, pc_idx);

                if let Some(pc_sql) = build_pattern_comprehension_sql(
                    &pc_meta.correlation_label,
                    &pc_meta.direction,
                    &pc_meta.rel_types,
                    &pc_meta.agg_type,
                    schema,
                    pc_meta.target_label.as_deref(),
                    pc_meta.target_property.as_deref(),
                    pc_meta.target_var.as_deref(),
                    pc_meta.where_clause.as_ref(),
                    pc_meta.target_projection.as_ref(),
                ) {
                    log::info!(
                        "🔧 Pattern comp CTE '{}': SQL = {}",
                        pc_cte_name,
                        &pc_sql[..pc_sql.len().min(200)]
                    );

                    let pc_cte = Cte::new(pc_cte_name.clone(), CteContent::RawSql(pc_sql), false);
                    all_ctes.push(pc_cte);

                    use crate::graph_catalog::expression_parser::PropertyValue;

                    let lhs_expr = if with_cte_render.union.0.is_some()
                        && with_cte_render.from.0.is_none()
                    {
                        let id_column = find_node_id_column_from_schema(
                            &pc_meta.correlation_var,
                            &pc_meta.correlation_label,
                            schema,
                        );
                        let node_alias = with_plans
                            .first()
                            .and_then(|p| match p {
                                LogicalPlan::WithClause(wc) => wc.exported_aliases.first().cloned(),
                                _ => None,
                            })
                            .unwrap_or_else(|| pc_meta.correlation_var.clone());
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("__union".to_string()),
                            column: PropertyValue::Column(cte_column_name(&node_alias, &id_column)),
                        })
                    } else {
                        build_node_id_expr_for_join(
                            &pc_meta.correlation_var,
                            &pc_meta.correlation_label,
                            schema,
                        )
                    };

                    let pc_alias = format!("__pc_{}", pc_idx);

                    let on_clause = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            lhs_expr,
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(pc_alias.clone()),
                                column: PropertyValue::Column("node_id".to_string()),
                            }),
                        ],
                    };

                    let join = Join {
                        table_name: pc_cte_name.clone(),
                        table_alias: pc_alias.clone(),
                        joining_on: vec![on_clause],
                        join_type: JoinType::Left,
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                        is_cartesian: false,
                    };
                    with_cte_render.joins.0.push(join);

                    if with_cte_render.union.0.is_some()
                        && with_cte_render.from.0.is_none()
                        && with_cte_render.select.items.is_empty()
                    {
                        with_cte_render.select.items.push(SelectItem {
                            expression: RenderExpr::Column(Column(PropertyValue::Column(
                                "__union.*".to_string(),
                            ))),
                            col_alias: None,
                        });
                    }

                    let result_col_alias = pc_meta.result_alias.clone();
                    let result_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: "coalesce".to_string(),
                        args: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(pc_alias.clone()),
                                column: PropertyValue::Column("result".to_string()),
                            }),
                            RenderExpr::Literal(Literal::Integer(0)),
                        ],
                    });
                    with_cte_render.select.items.push(SelectItem {
                        expression: result_expr,
                        col_alias: Some(ColumnAlias(result_col_alias)),
                    });

                    log::info!(
                        "✅ Added pattern comp CTE '{}' with LEFT JOIN to WITH CTE",
                        pc_cte_name
                    );
                } else {
                    log::debug!(
                                "⚠️ Could not generate pattern comp SQL for label '{}' — no matching edges in schema",
                                pc_meta.correlation_label
                            );
                }
            }
        }
    }
}

/// Build the CTE's column metadata from its rendered SELECT items (a STEP of the
/// main loop's `'alias_loop` in `build_chained_with_match_cte_plan`, Phase-4
/// §7.1 extraction).
///
/// Collects the schema-defining SELECT items (for a UNION CTE, from the first
/// branch plus any wrapping items such as pattern-comprehension results;
/// otherwise the CTE's own SELECT), their column-alias names, and per-column
/// `CteColumnMetadata` mapping `(cypher_alias, cypher_property) → cte_column_name`.
/// Each column alias is parsed with the new `p{N}_{alias}_{property}` format
/// first, falling back to the legacy `{alias}_{property}` split; either way the
/// alias must appear in `with_alias` (split on `_`) to be recorded.
///
/// Returns `(select_items_for_schema, property_names_for_schema, cte_columns)`.
fn build_cte_column_metadata(
    with_cte_render: &RenderPlan,
    with_alias: &str,
    cte_name: &str,
) -> (
    Vec<SelectItem>,
    Vec<String>,
    Vec<crate::render_plan::cte_manager::CteColumnMetadata>,
) {
    let (select_items_for_schema, property_names_for_schema) = match &with_cte_render.union {
        UnionItems(Some(union)) if !union.input.is_empty() => {
            // For UNION, take schema from first branch (all branches must have same schema)
            let mut items = union.input[0].select.items.clone();
            // Also include any wrapping SELECT items (e.g., pattern comprehension results)
            // These are in with_cte_render.select alongside __union.* pass-through
            for item in &with_cte_render.select.items {
                if item.col_alias.is_some() {
                    items.push(item.clone());
                }
            }
            let names: Vec<String> = items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                .collect();
            (items, names)
        }
        _ => {
            let items = with_cte_render.select.items.clone();
            let names: Vec<String> = items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                .collect();
            (items, names)
        }
    };

    // Build column metadata from SELECT items
    // This extracts: (cypher_alias, cypher_property) -> cte_column_name
    // Supports both new p{N} format and legacy underscore format
    let mut cte_columns: Vec<crate::render_plan::cte_manager::CteColumnMetadata> = Vec::new();
    for item in &select_items_for_schema {
        if let Some(col_alias) = &item.col_alias {
            let cte_col_name = col_alias.0.clone();

            // Try new p{N} format first
            if let Some((parsed_alias, parsed_property)) = parse_cte_column(&cte_col_name) {
                // Verify alias appears in with_alias
                let alias_parts: Vec<&str> = with_alias.split('_').collect();
                if alias_parts.contains(&parsed_alias.as_str()) {
                    cte_columns.push(crate::render_plan::cte_manager::CteColumnMetadata {
                        cypher_alias: parsed_alias.clone(),
                        cypher_property: parsed_property.clone(),
                        cte_column_name: cte_col_name.clone(),
                        db_column: parsed_property.clone(), // Approximation
                        is_id_column: parsed_property.ends_with("_id") || parsed_property == "id",
                        vlp_position: None,
                    });
                    log::debug!(
                        "  Added CTE column metadata (p{{N}}): ({}, {}) -> {}",
                        parsed_alias,
                        parsed_property,
                        cte_col_name
                    );
                }
            }
            // Fallback: legacy underscore format
            else if let Some(first_underscore) = cte_col_name.find('_') {
                let potential_alias = &cte_col_name[..first_underscore];
                let potential_property = &cte_col_name[first_underscore + 1..];

                // Verify this is likely correct by checking if alias appears in with_alias
                let alias_parts: Vec<&str> = with_alias.split('_').collect();
                if alias_parts.contains(&potential_alias) {
                    cte_columns.push(crate::render_plan::cte_manager::CteColumnMetadata {
                        cypher_alias: potential_alias.to_string(),
                        cypher_property: potential_property.to_string(),
                        cte_column_name: cte_col_name.clone(),
                        db_column: potential_property.to_string(), // Approximation
                        is_id_column: potential_property.ends_with("_id")
                            || potential_property == "id",
                        vlp_position: None,
                    });
                    log::debug!(
                        "  Added CTE column metadata (legacy): ({}, {}) -> {}",
                        potential_alias,
                        potential_property,
                        cte_col_name
                    );
                }
            }
        }
    }

    log::debug!(
        "🔧 Extracted {} column metadata entries for CTE '{}'",
        cte_columns.len(),
        cte_name
    );

    (
        select_items_for_schema,
        property_names_for_schema,
        cte_columns,
    )
}

/// Extract the WITH clause's original exported aliases and the renamed→original
/// alias map (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// `original_exported_aliases` are the WithClause's own exported aliases (falling
/// back to splitting `with_alias` on `_`). The rename map handles `WITH u AS
/// person`: it maps the renamed alias (`person`) back to the original (`u`) —
/// derived from each projection item whose expression is a `TableAlias` or
/// `PropertyAccessExp` and whose output name differs from that source alias — so
/// downstream lookups can find CTE columns prefixed with the original alias in
/// `property_mapping`.
///
/// Returns `(original_exported_aliases, alias_rename_map)`.
fn build_alias_rename_map(
    with_plans: &[LogicalPlan],
    with_alias: &str,
) -> (Vec<String>, HashMap<String, String>) {
    let original_exported_aliases: Vec<String> = with_plans
        .iter()
        .find_map(|plan| {
            if let LogicalPlan::WithClause(wc) = plan {
                Some(wc.exported_aliases.clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| with_alias.split('_').map(|s| s.to_string()).collect());

    // Build rename mapping: renamed_alias → original_alias
    // For "WITH u AS person", maps "person" → "u" so we can find
    // CTE columns prefixed with the original alias in property_mapping.
    let alias_rename_map: HashMap<String, String> = with_plans
        .iter()
        .find_map(|plan| {
            if let LogicalPlan::WithClause(wc) = plan {
                let mut renames = HashMap::new();
                for item in &wc.items {
                    if let Some(ref col_alias) = item.col_alias {
                        let renamed = &col_alias.0;
                        // Extract original alias from expression
                        let original = match &item.expression {
                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(ta) => {
                                Some(ta.0.clone())
                            }
                            crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                pa,
                            ) => Some(pa.table_alias.0.clone()),
                            _ => None,
                        };
                        if let Some(orig) = original {
                            if &orig != renamed {
                                renames.insert(renamed.clone(), orig);
                            }
                        }
                    }
                }
                Some(renames)
            } else {
                None
            }
        })
        .unwrap_or_default();

    (original_exported_aliases, alias_rename_map)
}

/// OPTIONAL-MATCH CTE-body bridge restructure (Phase-4 §7.1 extraction from
/// `restructure_post_with_optional_match`).
///
/// The WITH CTE arrived as a CROSS/INNER JOIN (or a bridge LEFT JOIN) while the
/// real anchor is the fresh optional-pattern table in FROM. Remove the CTE join
/// and its bridge join, promote the CTE to FROM, and re-attach the old FROM
/// table / pattern edges so OPTIONAL MATCH semantics hold. Mutates
/// `with_cte_render.joins`/`.from` in place; `cte_idx` is the CTE join index and
/// `from_name` the pre-cloned old-FROM table name (used only for logging).
fn restructure_optional_cte_bridge(
    with_cte_render: &mut RenderPlan,
    cte_idx: usize,
    from_name: &str,
) {
    log::info!("🔧 OPTIONAL MATCH CTE body restructuring: has_optional_match_input=true, FROM='{}', CTE join at idx {}",
                from_name, cte_idx);

    let cte_join = with_cte_render.joins.0.remove(cte_idx);
    let cte_table_name = cte_join.table_name.clone();
    let cte_alias_str = cte_join.table_alias.clone();

    // Find the "bridge join" — the LEFT JOIN whose ON condition references the CTE alias
    let bridge_idx = with_cte_render.joins.0.iter().position(|j| {
        j.joining_on.iter().any(|op| {
            op.operands.iter().any(|operand| {
                if let RenderExpr::PropertyAccessExp(pa) = operand {
                    pa.table_alias.0 == cte_alias_str
                } else {
                    false
                }
            })
        })
    });

    if let Some(bridge_idx) = bridge_idx {
        let bridge_join = with_cte_render.joins.0.remove(bridge_idx);
        log::info!(
            "🔧 OPTIONAL MATCH CTE body restructuring: bridge join '{}' connects CTE to pattern",
            bridge_join.table_alias
        );

        // Extract CTE column and pattern column from bridge join ON condition
        let mut cte_col: Option<String> = None;
        let mut pattern_alias: Option<String> = None;
        let mut pattern_col: Option<String> = None;
        for op in &bridge_join.joining_on {
            for operand in &op.operands {
                if let RenderExpr::PropertyAccessExp(pa) = operand {
                    if pa.table_alias.0 == cte_alias_str {
                        cte_col = Some(pa.column.raw().to_string());
                    } else {
                        pattern_alias = Some(pa.table_alias.0.clone());
                        pattern_col = Some(pa.column.raw().to_string());
                    }
                }
            }
        }

        if let (Some(cte_col), Some(pattern_alias), Some(pattern_col)) =
            (cte_col, pattern_alias, pattern_col)
        {
            // Save old FROM info
            let old_from = with_cte_render.from.0.take().unwrap();
            let old_from_alias = old_from
                .alias
                .clone()
                .unwrap_or_else(|| old_from.name.clone());

            // Set FROM to CTE
            with_cte_render.from = FromTableItem(Some(super::ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: cte_table_name.clone(),
                alias: Some(cte_alias_str.clone()),
                use_final: false,
            }));

            // Find the pattern table join that was referenced in the bridge
            let pattern_join_idx = with_cte_render
                .joins
                .0
                .iter()
                .position(|j| j.table_alias == pattern_alias);

            if let Some(pidx) = pattern_join_idx {
                // Find FK column pointing to the old FROM table
                let mut fk_col_to_old_from: Option<String> = None;
                for op in &with_cte_render.joins.0[pidx].joining_on {
                    for operand in &op.operands {
                        if let RenderExpr::PropertyAccessExp(pa) = operand {
                            if pa.table_alias.0 == pattern_alias {
                                fk_col_to_old_from = Some(pa.column.raw().to_string());
                            }
                        }
                    }
                }

                // Rewrite pattern join ON condition to reference CTE
                with_cte_render.joins.0[pidx].joining_on = vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(pattern_alias.clone()),
                            column: PropertyValue::Column(pattern_col.clone()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(cte_alias_str.clone()),
                            column: PropertyValue::Column(cte_col.clone()),
                        }),
                    ],
                }];

                // Reorder: move pattern join to position 0
                let pjoin = with_cte_render.joins.0.remove(pidx);
                with_cte_render.joins.0.insert(0, pjoin);

                // Add old FROM as LEFT JOIN after pattern join
                let old_from_join = super::Join {
                    table_name: old_from.name.clone(),
                    table_alias: old_from_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(old_from_alias.clone()),
                                column: PropertyValue::Column("id".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(pattern_alias.clone()),
                                column: PropertyValue::Column(
                                    fk_col_to_old_from.unwrap_or_else(|| "PostId".to_string()),
                                ),
                            }),
                        ],
                    }],
                    join_type: super::JoinType::Left,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                    is_cartesian: false,
                };
                with_cte_render.joins.0.insert(1, old_from_join);

                // Embed WHERE predicate into count() as countIf()
                // ClickHouse rejects complex LEFT JOIN ON expressions with join_use_nulls.
                // Instead of `count(x) WHERE cond`, use `countIf(x, cond)` with no WHERE.
                if let FilterItems(Some(where_expr)) = &with_cte_render.filters {
                    let where_clone = where_expr.clone();
                    // Find count() aggregate in SELECT and convert to countIf()
                    for item in with_cte_render.select.items.iter_mut() {
                        if let RenderExpr::AggregateFnCall(agg) = &mut item.expression {
                            if agg.name == "count" && !agg.args.is_empty() {
                                log::info!("🔧 OPTIONAL MATCH CTE body restructuring: converting count() to count-if with WHERE filter");
                                rewrite_count_to_conditional(agg, where_clone.clone());
                            }
                        }
                    }
                    with_cte_render.filters = FilterItems(None);
                }

                // Remove redundant joins: bridge target table (Forum)
                with_cte_render
                    .joins
                    .0
                    .retain(|j| j.table_alias != bridge_join.table_alias);

                // Remove spurious VLP CROSS JOINs
                with_cte_render.joins.0.retain(|j| {
                    !(j.table_name.starts_with("vlp_")
                        && (matches!(j.join_type, super::JoinType::Inner)
                            || matches!(j.join_type, super::JoinType::Join)))
                });

                // Remove Person join if only used for IN/has() check — use FK instead.
                // The Person node (e.g., otherPerson2) is only needed to provide
                // its ID for the IN check. We can use the relationship table's FK
                // column (e.g., Post_hasCreator_Person.PersonId) directly.
                let person_join_idx = with_cte_render.joins.0.iter().position(|j| {
                    matches!(j.join_type, super::JoinType::Left)
                            && j.table_alias != old_from_alias
                            && j.table_alias != pattern_alias
                            // Node tables: "ldbc.Person" (contains '.' but not '_' after the db prefix)
                            && j.table_name.split('.').next_back().is_some_and(|n| !n.contains('_'))
                            && j.joining_on.iter().any(|op| {
                                op.operands.iter().any(|operand| {
                                    if let RenderExpr::PropertyAccessExp(pa) = operand {
                                        pa.table_alias.0 == j.table_alias
                                            && pa.column.raw() == "id"
                                    } else {
                                        false
                                    }
                                })
                            })
                });
                if let Some(pidx2) = person_join_idx {
                    let person_alias = with_cte_render.joins.0[pidx2].table_alias.clone();
                    let mut select_aliases = std::collections::HashSet::new();
                    for item in &with_cte_render.select.items {
                        collect_aliases_from_single_render_expr(
                            &item.expression,
                            &mut select_aliases,
                        );
                    }
                    let person_still_needed = select_aliases.contains(&person_alias);
                    if !person_still_needed {
                        // Find the FK column that joins this Person to the relationship table
                        // e.g., otherPerson2.id = t2.PersonId → FK is "PersonId" on alias "t2"
                        let mut fk_info: Option<(String, String)> = None; // (rel_alias, fk_col)
                        for op in &with_cte_render.joins.0[pidx2].joining_on {
                            for operand in &op.operands {
                                if let RenderExpr::PropertyAccessExp(pa) = operand {
                                    if pa.table_alias.0 != person_alias {
                                        fk_info = Some((
                                            pa.table_alias.0.clone(),
                                            pa.column.raw().to_string(),
                                        ));
                                    }
                                }
                            }
                        }

                        // Rewrite IN operator references from person.id to rel.FK
                        // At this stage, IN is OperatorApplication(In), not ScalarFnCall("has")
                        if let Some((rel_alias, fk_col)) = &fk_info {
                            for j in with_cte_render.joins.0.iter_mut() {
                                for op in j.joining_on.iter_mut() {
                                    // Check if this is an IN operator with person_alias ref
                                    if matches!(op.operator, Operator::In) && op.operands.len() == 2
                                    {
                                        if let RenderExpr::PropertyAccessExp(pa) = &op.operands[0] {
                                            if pa.table_alias.0 == person_alias {
                                                op.operands[0] =
                                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(rel_alias.clone()),
                                                        column: PropertyValue::Column(
                                                            fk_col.clone(),
                                                        ),
                                                    });
                                                log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote IN to use {}.{}", rel_alias, fk_col);
                                            }
                                        }
                                    }
                                    // Also handle ScalarFnCall("has") form
                                    for operand in op.operands.iter_mut() {
                                        if let RenderExpr::ScalarFnCall(fn_call) = operand {
                                            if fn_call.name == "has" && fn_call.args.len() == 2 {
                                                if let RenderExpr::PropertyAccessExp(pa) =
                                                    &fn_call.args[1]
                                                {
                                                    if pa.table_alias.0 == person_alias {
                                                        fn_call.args[1] =
                                                            RenderExpr::PropertyAccessExp(
                                                                PropertyAccess {
                                                                    table_alias: TableAlias(
                                                                        rel_alias.clone(),
                                                                    ),
                                                                    column: PropertyValue::Column(
                                                                        fk_col.clone(),
                                                                    ),
                                                                },
                                                            );
                                                        log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote has() to use FK column");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Also rewrite person alias references in SELECT items
                            // (e.g., inside countIf args where the WHERE filter was moved)
                            for item in with_cte_render.select.items.iter_mut() {
                                rewrite_person_to_fk(
                                    &mut item.expression,
                                    &person_alias,
                                    rel_alias,
                                    fk_col,
                                );
                            }
                            log::info!("🔧 OPTIONAL MATCH CTE body restructuring: rewrote person refs in SELECT items");
                        }
                        with_cte_render.joins.0.remove(pidx2);
                        log::info!("🔧 OPTIONAL MATCH CTE body restructuring: removed redundant Person join '{}'", person_alias);
                    }
                }

                log::info!(
                    "🔧 OPTIONAL MATCH CTE body restructuring: complete. FROM='{}', {} joins",
                    cte_table_name,
                    with_cte_render.joins.0.len()
                );
            }
        }
    } else if !cte_join.joining_on.is_empty() {
        // No separate bridge join, but the CTE join itself has
        // a proper ON condition (e.g., friend.id = t3.PersonId).
        // The CTE join IS the bridge — extract info from it directly.
        let mut cte_col: Option<String> = None;
        let mut pattern_alias: Option<String> = None;
        let mut pattern_col: Option<String> = None;
        for op in &cte_join.joining_on {
            for operand in &op.operands {
                if let RenderExpr::PropertyAccessExp(pa) = operand {
                    if pa.table_alias.0 == cte_alias_str {
                        cte_col = Some(pa.column.raw().to_string());
                    } else {
                        pattern_alias = Some(pa.table_alias.0.clone());
                        pattern_col = Some(pa.column.raw().to_string());
                    }
                }
            }
        }

        if let (Some(_cte_col), Some(pattern_alias), Some(pattern_col)) =
            (cte_col, pattern_alias.clone(), pattern_col)
        {
            log::info!(
                        "🔧 OPTIONAL MATCH CTE body restructuring: CTE join has ON condition, using as bridge. Pattern: {}.{}",
                        pattern_alias, pattern_col
                    );

            // Save old FROM info
            let old_from = with_cte_render.from.0.take().unwrap();
            let old_from_alias = old_from
                .alias
                .clone()
                .unwrap_or_else(|| old_from.name.clone());

            // Set FROM to CTE
            with_cte_render.from = FromTableItem(Some(super::ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: cte_table_name.clone(),
                alias: Some(cte_alias_str.clone()),
                use_final: false,
            }));

            // Find the pattern table (edge table that the CTE was joined to)
            // and rewrite its ON condition to reference the CTE instead of the old FROM
            if let Some(pidx) = with_cte_render
                .joins
                .0
                .iter()
                .position(|j| j.table_alias == pattern_alias)
            {
                // The pattern table's current ON references old FROM (e.g., t3.PostId = post.id)
                // Rewrite to reference CTE column instead (e.g., t3.PersonId = friend.p6_friend_id)
                // Reuse CTE join's ON predicates verbatim — the CTE
                // operand stays, and the pattern operand was already
                // ordered for readability when the join was built.
                with_cte_render.joins.0[pidx].joining_on = cte_join.joining_on.clone();
                with_cte_render.joins.0[pidx].join_type = super::JoinType::Left;
            }

            // The old FROM table (e.g., Post) is no longer the FROM.
            // Add it as a LEFT JOIN using the edge table's FK to connect.
            // Find the edge table's FK that points to the old FROM
            let edge_to_old_from = with_cte_render
                .joins
                .0
                .iter()
                .find(|j| j.table_alias == pattern_alias)
                .and_then(|j| j.from_id_column.clone().or(j.to_id_column.clone()));

            // Find the ID column of the old FROM table
            let old_from_id_col = "id".to_string();

            if let Some(_edge_fk) = edge_to_old_from {
                // Post joins via edge's PostId → post.id
                // Already handled by the original join that connected Post to the edge
            }

            // Add old FROM as LEFT JOIN after edge table
            // e.g., LEFT JOIN Post AS post ON post.id = t3.PostId
            let old_from_join = super::Join {
                table_name: old_from.name.clone(),
                table_alias: old_from_alias.clone(),
                join_type: super::JoinType::Left,
                joining_on: vec![], // Will be populated below
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
                is_cartesian: false,
            };

            // Find the edge table's original ON condition that referenced old FROM
            // In the original rendering: t3.PostId = post.id
            // We need to reconstruct this for the old FROM join
            // Look for the edge table's from_id_column or to_id_column
            let edge_join = with_cte_render
                .joins
                .0
                .iter()
                .find(|j| j.table_alias == pattern_alias);
            let mut old_from_on = vec![];
            if let Some(ej) = edge_join {
                // Build ON: old_from.id = edge.FK
                if let Some(ref fk) = ej.from_id_column {
                    old_from_on.push(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(old_from_alias.clone()),
                                column: PropertyValue::Column(old_from_id_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(ej.table_alias.clone()),
                                column: PropertyValue::Column(fk.clone()),
                            }),
                        ],
                    });
                } else if let Some(ref fk) = ej.to_id_column {
                    old_from_on.push(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(old_from_alias.clone()),
                                column: PropertyValue::Column(old_from_id_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(ej.table_alias.clone()),
                                column: PropertyValue::Column(fk.clone()),
                            }),
                        ],
                    });
                }
            }

            let mut old_from_join = old_from_join;
            old_from_join.joining_on = old_from_on;

            // Insert old FROM join after the edge table
            let edge_pos = with_cte_render
                .joins
                .0
                .iter()
                .position(|j| j.table_alias == pattern_alias)
                .map(|p| p + 1)
                .unwrap_or(0);
            with_cte_render.joins.0.insert(edge_pos, old_from_join);

            log::info!(
                        "🔧 OPTIONAL MATCH CTE body restructuring (direct bridge): complete. FROM='{}', {} joins",
                        cte_table_name, with_cte_render.joins.0.len()
                    );
        } else {
            // Couldn't extract bridge info — put CTE join back
            with_cte_render.joins.0.insert(0, cte_join);
        }
    } else {
        // No bridge join found and CTE is a CROSS JOIN — put back
        with_cte_render.joins.0.insert(0, cte_join);
    }
}

/// Restructure a post-WITH OPTIONAL MATCH CTE body so the CTE drives the join
/// chain (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When a post-WITH pattern uses OPTIONAL MATCH, the CTE body would otherwise
/// render as `FROM pattern_table CROSS JOIN cte LEFT JOIN …`, which is
/// semantically wrong: FROM must be the CTE (to preserve all its rows for the
/// LEFT JOIN semantics) and the pattern tables must LEFT JOIN off it.
///
/// Applies only when `has_optional_match_input` (deterministic — from the input
/// plan's `is_optional_pattern()` / `GraphRel.is_optional`) AND the structure
/// matches: FROM is a regular table (not already a CTE/VLP) and a CTE CROSS JOIN
/// exists in the join list (added by `fix_orphan_table_aliases`). The transform:
///   1. make the CTE the FROM,
///   2. find the "bridge join" (LEFT JOIN whose ON references the CTE alias),
///   3. restructure the join chain: CTE → bridge_table → old_FROM → rest,
///   4. embed the WHERE predicate into a `countIf()` aggregate.
///
/// No-op when the guard or the structural requirements are not met.
fn restructure_post_with_optional_match(
    with_cte_render: &mut RenderPlan,
    has_optional_match_input: bool,
) {
    if has_optional_match_input {
        if let FromTableItem(Some(ref from_ref)) = with_cte_render.from {
            if !from_ref.name.starts_with("with_") && !from_ref.name.starts_with("vlp_") {
                // Find a CTE JOIN anywhere in the join list.
                // fix_orphan_table_aliases adds JoinType::Join (CROSS JOIN), but
                // the CTE may also appear as a LEFT JOIN when the join builder
                // connected it directly to the pattern's edge table.
                let cte_cross_join_idx = with_cte_render
                    .joins
                    .0
                    .iter()
                    .position(|j| j.table_name.starts_with("with_"));

                if let Some(cte_idx) = cte_cross_join_idx {
                    let cte_is_cross_join = matches!(
                        with_cte_render.joins.0[cte_idx].join_type,
                        super::JoinType::Inner | super::JoinType::Join
                    );

                    // Guard for non-CROSS-JOIN CTEs (e.g., LEFT JOIN):
                    // only restructure when FROM is referenced by at most 1 edge.
                    // If FROM is a join center (2+ edges), it must stay as FROM.
                    // CROSS JOIN CTEs always restructure (original behavior).
                    let from_alias = from_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| from_ref.name.clone());
                    let should_restructure = if cte_is_cross_join {
                        true
                    } else {
                        let from_ref_count = with_cte_render
                            .joins
                            .0
                            .iter()
                            .filter(|j| !j.table_name.starts_with("with_"))
                            .filter(|j| {
                                j.joining_on.iter().any(|op| {
                                    op.operands.iter().any(|operand| {
                                        if let RenderExpr::PropertyAccessExp(pa) = operand {
                                            pa.table_alias.0 == from_alias
                                        } else {
                                            false
                                        }
                                    })
                                })
                            })
                            .count();
                        if from_ref_count > 1 {
                            log::info!(
                                        "🔧 OPTIONAL MATCH CTE body restructuring: skipping — FROM '{}' is a join center (referenced by {} edges)",
                                        from_alias, from_ref_count
                                    );
                        }
                        from_ref_count <= 1
                    };
                    let from_name = from_ref.name.clone();

                    if !should_restructure {
                        // Skip — FROM is a join center for a non-CROSS-JOIN CTE
                    } else {
                        restructure_optional_cte_bridge(with_cte_render, cte_idx, &from_name);
                    } // else: !should_restructure
                }
            }
        }
    }
}

/// Materialize a bidirectional weight CTE for weighted shortest path (complex-14)
/// (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// A weight CTE is recognized by exactly 3 exported aliases — `source`,
/// `target`, `weight` — AND the query containing a `shortestPath()` call
/// downstream (the `plan_has_shortest_path` guard avoids false positives on
/// unrelated queries that merely reuse those alias names). When matched, append
/// a recursive `bidi_{cte}` CTE (base case = forward edges evaluated once;
/// recursive step swaps source/target for the reverse edges, then `__depth = 1`
/// stops the recursion), point the task-local weight config at it, and fix the
/// base CTE's GROUP BY to exclude the aggregate-derived `weight` column. No-op
/// when the query is not a weighted shortest path.
fn maybe_add_bidirectional_weight_cte(
    all_ctes: &mut Vec<Cte>,
    original_exported_aliases: &[String],
    cte_name: &str,
    plan: &LogicalPlan,
) {
    if original_exported_aliases.len() == 3
        && original_exported_aliases.contains(&"source".to_string())
        && original_exported_aliases.contains(&"target".to_string())
        && original_exported_aliases.contains(&"weight".to_string())
        && plan_has_shortest_path(plan)
    {
        log::info!(
            "🔧 Detected weight CTE '{}' for weighted shortest path",
            cte_name
        );
        // Create a bidirectional weight CTE using recursive materialization.
        // Base case: forward edges from weight CTE (evaluated once).
        // Recursive step: swaps source/target to produce reverse edges,
        // then __depth=1 causes WHERE __depth=0 to fail → recursion stops.
        // Result: forward + reverse edges materialized, weight CTE's
        // expensive multi-table join evaluated exactly once.
        let bidi_cte_name = format!("bidi_{}", cte_name);
        let cast_u8 = current_function_mapper().cast_uint8();
        let bidi_sql = format!(
            "SELECT source, target, weight, {cast_u8}(0) AS __depth FROM {cte} \
                     UNION ALL \
                     SELECT target AS source, source AS target, weight, __depth + 1 \
                     FROM {bidi} WHERE __depth = 0",
            cte = cte_name,
            bidi = bidi_cte_name,
        );
        let bidi_cte = super::Cte {
            cte_name: bidi_cte_name.clone(),
            content: super::CteContent::RawSql(bidi_sql),
            is_recursive: true,
            vlp_start_alias: None,
            vlp_end_alias: None,
            vlp_start_table: None,
            vlp_end_table: None,
            vlp_cypher_start_alias: None,
            vlp_cypher_end_alias: None,
            vlp_start_id_col: None,
            vlp_end_id_col: None,
            vlp_path_variable: None,
            columns: vec![],
            from_alias: None,
            outer_where_filters: None,
            with_exported_aliases: vec![
                "source".to_string(),
                "target".to_string(),
                "weight".to_string(),
            ],
            variable_registry: None,
        };
        all_ctes.push(bidi_cte);

        // Point the weight config to the bidirectional CTE
        crate::server::query_context::set_weight_cte_config(
            crate::clickhouse_query_generator::WeightCteConfig {
                cte_name: bidi_cte_name,
                source_column: "source".to_string(),
                target_column: "target".to_string(),
                weight_column: "weight".to_string(),
            },
        );

        // Fix GROUP BY: exclude aggregate-derived "weight" column.
        // The SQL generator's build_union_inner_select handles per-branch
        // aggregation correctly, but the GROUP BY must only contain
        // the non-aggregate columns (source, target).
        if let Some(cte) = all_ctes.iter_mut().find(|c| c.cte_name == cte_name) {
            if let super::CteContent::Structured(ref mut render_plan) = cte.content {
                let source_target_group_by: Vec<RenderExpr> = render_plan
                    .select
                    .items
                    .iter()
                    .filter(|item| {
                        item.col_alias
                            .as_ref()
                            .is_some_and(|a| a.0 == "source" || a.0 == "target")
                    })
                    .map(|item| item.expression.clone())
                    .collect();
                if !source_target_group_by.is_empty() {
                    render_plan.group_by = super::GroupByExpressions(source_target_group_by);
                }
            }
        }
    }
}

/// Compute each exported alias's ID column within the WITH CTE (a STEP of the
/// main loop's `'alias_loop` in `build_chained_with_match_cte_plan`, Phase-4
/// §7.1 extraction).
///
/// Maps `alias → the CTE column holding that alias's node id`, by priority:
/// (1) inherit from an upstream CTE when the alias is already CTE-backed (most
/// reliable for chained WITH — a plan-level lookup could otherwise pick a stale
/// VLP endpoint like `end_id` over the CTE's renamed `id`); else a direct
/// column match (bare UNWIND scalar); (2) the deterministic
/// `compute_cte_id_column_for_alias` over the inner plan then the current plan;
/// (3) ARRAY JOIN scalar detection — an `UNWIND … AS alias` makes the bare
/// column itself the id. Returns the `alias → id_column` map.
fn compute_alias_id_columns(
    exported_aliases: &[String],
    inner_plans_for_id: &[LogicalPlan],
    current_plan: &LogicalPlan,
    cte_references: &HashMap<String, String>,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_name: &str,
) -> HashMap<String, String> {
    let mut alias_to_id_column: HashMap<String, String> = HashMap::new();

    // Use individual exported aliases (e.g., ["a", "allNeighboursCount"]) not combined with_alias
    // compute_cte_id_column_for_alias needs the actual node alias to find the GraphNode.
    // CRITICAL: Check CTE references FIRST — if the alias is already CTE-backed from an
    // upstream WITH, inherit its ID column. Otherwise, plan-level lookup may find stale
    // VLP endpoints (e.g., end_id) instead of the CTE's renamed column (e.g., id).
    let id_lookup_plan = inner_plans_for_id.first().unwrap_or(current_plan);
    for alias in exported_aliases {
        // Priority 1: Inherit from upstream CTE (most reliable for chained WITH)
        if let Some(prev_cte_name) = cte_references.get(alias) {
            if let Some(meta) = cte_schemas.get(prev_cte_name) {
                if let Some(prev_id) = meta.alias_to_id.get(alias) {
                    log::info!(
                        "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (inherited from CTE '{}')",
                        cte_name,
                        alias,
                        prev_id,
                        prev_cte_name
                    );
                    alias_to_id_column.insert(alias.clone(), prev_id.clone());
                    continue;
                } else if meta.column_names.contains(alias) {
                    // Fallback: CTE has a direct column matching alias (e.g. UNWIND scalar)
                    log::info!(
                        "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (bare column from CTE '{}')",
                        cte_name,
                        alias,
                        alias,
                        prev_cte_name
                    );
                    alias_to_id_column.insert(alias.clone(), alias.clone());
                    continue;
                }
            }
        }
        // Priority 2: Compute from plan structure (inner plan first, then current)
        if let Some(id_col_name) = compute_cte_id_column_for_alias(alias, id_lookup_plan)
            .or_else(|| compute_cte_id_column_for_alias(alias, current_plan))
        {
            log::info!(
                "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (deterministic)",
                cte_name,
                alias,
                id_col_name
            );
            alias_to_id_column.insert(alias.clone(), id_col_name.clone());
            continue;
        }

        // Priority 3: ARRAY JOIN scalar detection.
        // If the plan has an Unwind node producing this alias (e.g., UNWIND ... AS person),
        // the alias IS the ID value (a scalar from ARRAY JOIN).
        let mut unwind_aliases = Vec::new();
        // Check all plans that contributed to this CTE
        for ip in inner_plans_for_id {
            find_unwind_aliases(ip, &mut unwind_aliases);
        }
        find_unwind_aliases(current_plan, &mut unwind_aliases);
        if unwind_aliases.contains(alias) {
            log::info!(
                        "📊 WITH CTE '{}': ID for alias '{}' -> '{}' (ARRAY JOIN scalar — bare column IS the ID)",
                        cte_name, alias, alias
                    );
            alias_to_id_column.insert(alias.clone(), alias.clone());
        }
    }

    alias_to_id_column
}

/// Build the WITH CTE's explicit `(cypher_alias, cypher_property) → cte_column`
/// property mapping (a STEP of the main loop's `'alias_loop` in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Starts from `build_property_mapping_from_columns` over the CTE's SELECT
/// items, rewrites dotted column names to underscores (WITH CTE columns are
/// `friend_id`, not `friend.id`), folds in the compound-key mappings collected
/// at generation time for flattened map literals (`flattened_compound_keys`,
/// each `(map_key.property, base_alias_mapkey_property)` matched to its base
/// alias by column-name prefix), and cross-references bare column aliases (an
/// UNWIND scalar `person` gets `(person, "id") → person`). Returns the mapping.
fn build_with_cte_property_mapping(
    select_items_for_schema: &[SelectItem],
    flattened_compound_keys: &[(String, String)],
    exported_aliases: &[String],
    alias_to_id_column: &HashMap<String, String>,
) -> HashMap<(String, String), String> {
    let mut property_mapping = build_property_mapping_from_columns(select_items_for_schema);

    log::debug!(
        "🔧 DEBUG: property_mapping BEFORE dot-to-underscore transformation: {} entries",
        property_mapping.len()
    );
    for ((alias, property), cte_column) in property_mapping.iter() {
        log::debug!("🔧   BEFORE: ({}, {}) → {}", alias, property, cte_column);
    }

    // Transform dotted column names to underscores for WITH CTEs
    // (WITH CTE columns use "friend_id", not "friend.id")
    property_mapping = property_mapping
        .into_iter()
        .map(|(k, v)| (k, v.replace('.', "_")))
        .collect();

    log::debug!(
        "🔧 DEBUG: property_mapping AFTER dot-to-underscore transformation: {} entries",
        property_mapping.len()
    );
    for ((alias, property), cte_column) in property_mapping.iter() {
        log::debug!("🔧   AFTER: ({}, {}) → {}", alias, property, cte_column);
    }

    // 🔧 FIX: Add compound key mappings for flattened map literal columns.
    // These were collected at generation time by try_flatten_head_collect_map_literal()
    // to avoid ambiguous reverse-engineering from underscore-delimited column names.
    // Each entry maps ("base_alias", "map_key.property") → "base_alias_mapkey_property".
    {
        for (compound_key, col_name) in flattened_compound_keys.iter() {
            // Find which exported alias this column belongs to
            for base_alias in exported_aliases {
                let prefix = format!("{}_", base_alias);
                if col_name.starts_with(&prefix) {
                    log::info!(
                        "🔧 property_mapping compound key: ({}, {}) → {}",
                        base_alias,
                        compound_key,
                        col_name
                    );
                    property_mapping
                        .insert((base_alias.clone(), compound_key.clone()), col_name.clone());
                    break;
                }
            }
        }
    }

    // Cross-reference: for bare column aliases (e.g. UNWIND scalar `person`),
    // add (alias, "id") → alias so `person.id` resolves to the "person" column
    for (alias, id_col) in alias_to_id_column {
        if id_col == alias {
            property_mapping
                .entry((alias.clone(), "id".to_string()))
                .or_insert_with(|| alias.clone());
            log::info!(
                "🔧 property_mapping cross-ref: ({}, id) → {} (bare column alias)",
                alias,
                alias
            );
        }
    }

    log::debug!(
        "🔧 DEBUG: property_mapping AFTER dot-to-underscore transformation: {} entries",
        property_mapping.len()
    );
    for ((alias, property), cte_column) in property_mapping.iter().take(10) {
        log::debug!("🔧   ({}, {}) → {}", alias, property, cte_column);
    }

    property_mapping
}

/// Register a freshly-built WITH CTE's alias references (a STEP of the main
/// loop's `'alias_loop` in `build_chained_with_match_cte_plan`, Phase-4 §7.1
/// extraction).
///
/// Marks `with_alias` processed (so later iterations don't filter it), points
/// `cte_references` at the NEW CTE name for both the composite key (`a_b`) and
/// each individual exported alias (`a`, `b` — so `WITH a, b, c` can resolve
/// columns for each from `with_a_b_cte_N`), and refreshes
/// `cte_references_for_rendering` from `cte_references` so subsequent WITH
/// clauses in THIS iteration can already reference the new CTE.
fn register_cte_alias_references(
    processed_cte_aliases: &mut std::collections::HashSet<String>,
    cte_references: &mut HashMap<String, String>,
    cte_references_for_rendering: &mut HashMap<String, String>,
    with_alias: &str,
    cte_name: &str,
    original_exported_aliases: &[String],
) {
    // Track that this alias is now a CTE (so subsequent iterations don't filter it)
    // Add the full composite alias
    processed_cte_aliases.insert(with_alias.to_string());

    // CRITICAL: Update cte_references to point to the NEW CTE name
    // This ensures subsequent references to this alias (in the final query or later CTEs)
    // use the MOST RECENT CTE, not the original one from the analyzer
    //
    // For composite aliases like "a_b", we need to add BOTH:
    // 1. The composite key "a_b" → CTE (for replacement logic)
    // 2. Individual aliases "a" → CTE and "b" → CTE (for expand_table_alias_to_select_items)
    //
    // This allows "WITH a, b, c" to find columns for both "a" and "b" from the "with_a_b_cte_1"
    cte_references.insert(with_alias.to_string(), cte_name.to_string());

    // Also add individual aliases — use exported_aliases from the WITH clause
    // (splitting with_alias by '_' fails for aliases containing underscores like "__expand")
    for alias in original_exported_aliases {
        if !alias.is_empty() {
            cte_references.insert(alias.clone(), cte_name.to_string());
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Added individual mapping: '{}' → '{}'",
                alias,
                cte_name
            );
        }
    }

    log::debug!("🔧 build_chained_with_match_cte_plan: Updated cte_references: '{}' → '{}' (plus {} individual aliases)",
               with_alias, cte_name, original_exported_aliases.len());

    // CRITICAL: Also update cte_references_for_rendering!
    // This allows subsequent WITH clauses in THIS ITERATION to reference the new CTE
    // Example: "WITH count(*) AS total" then "WITH total, year" - second WITH needs "total" in cte_references_for_rendering
    *cte_references_for_rendering = cte_references.clone();
    log::debug!("🔧 build_chained_with_match_cte_plan: Updated cte_references_for_rendering with {} entries", cte_references_for_rendering.len());
}

/// Publish each exported alias's CTE scope for downstream resolution (a STEP of
/// the main loop's `'alias_loop` in `build_chained_with_match_cte_plan`,
/// Phase-4 §7.1 extraction).
///
/// For every non-empty exported alias, extract its per-alias
/// `cypher_property → cte_column` mapping (looking up under the ORIGINAL alias
/// when it was renamed via `WITH u AS person`, since CTE columns are prefixed
/// with the original), resolve its node labels (renamed alias, then original,
/// then the WITH bodies — #411), and publish it to `with_scope` for the
/// variable registry PLUS to the narrow `set_cte_scope_for_correlation`
/// task-local channel that `generate_exists_sql` reads for EXISTS correlation
/// variables. Finally register any map-typed expression's keys on the scope var.
#[allow(clippy::too_many_arguments)]
fn publish_cte_alias_scopes(
    with_scope: &mut WithBarrierScope,
    original_exported_aliases: &[String],
    alias_rename_map: &HashMap<String, String>,
    property_mapping: &HashMap<(String, String), String>,
    current_plan: &LogicalPlan,
    with_plans: &[LogicalPlan],
    cte_name: &str,
    select_items_for_schema: &[SelectItem],
) {
    for alias in original_exported_aliases {
        if alias.is_empty() {
            continue;
        }
        // For renamed aliases (e.g., "person" from "WITH u AS person"),
        // look up properties under the original alias name ("u") since
        // CTE columns are prefixed with the original alias (p1_u_*).
        let lookup_alias = alias_rename_map.get(alias).unwrap_or(alias);

        // Extract per-alias property mapping: cypher_prop → cte_column
        let per_alias_mapping: HashMap<String, String> = property_mapping
            .iter()
            .filter(|((a, _), _)| a == lookup_alias)
            .map(|((_, prop), col)| (prop.clone(), col.clone()))
            .collect();

        // Get labels from current plan tree — try renamed alias first,
        // then fall back to original alias (for renamed variables).
        // After the WITH→CTE rewrite, `current_plan` may no longer expose the
        // source node (e.g. a graph-rel MATCH gets restructured), so also fall
        // back to the WITH bodies (`with_plans`), which still contain the source
        // GraphNode and its label. Missing labels break generic `.id` resolution
        // for renamed node_ids. (issue #411)
        use crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan;
        let labels = find_label_for_alias_in_plan(current_plan, alias)
            .or_else(|| find_label_for_alias_in_plan(current_plan, lookup_alias))
            .or_else(|| {
                with_plans.iter().find_map(|p| {
                    find_label_for_alias_in_plan(p, alias)
                        .or_else(|| find_label_for_alias_in_plan(p, lookup_alias))
                })
            })
            .map(|l| vec![l])
            .unwrap_or_default();

        with_scope.publish_alias(alias, lookup_alias, cte_name, &per_alias_mapping, &labels);

        // Publish this alias's CTE scope (FROM alias + Cypher-property →
        // CTE-column mapping) to a narrow, purpose-built task-local
        // channel for EXISTS correlation-variable resolution
        // (`render_expr::resolve_correlation_id_sql`).
        //
        // NOT the same as `var_registry` above: `VariableRegistry::
        // define_node`/`define_scalar` still construct an EMPTY
        // `property_mapping` for a WITH-CTE `VariableSource::Cte` node
        // export, so `resolve_with_current_registry` does not carry a
        // per-property node map here. Ordinary property access across
        // CTE barriers resolves forward via the render-site registry
        // identity self-map + `plan_ctx` CTE columns (the F0/F1
        // forward-resolution work; the legacy task-local
        // `cte_property_mappings` reparse was removed in F2a). This
        // channel remains the purpose-built path for the one case the
        // forward registry cannot serve mid-build: an EXISTS
        // correlation variable that must resolve through the CTE scope
        // active *at this exact point* in `build_chained_with_match_cte_plan`,
        // before the variable moves on to a later WITH clause's CTE. It
        // is written only here and read only by `generate_exists_sql`'s
        // `GraphRel` branch, so it cannot affect any other resolution.
        crate::server::query_context::set_cte_scope_for_correlation(
            alias.clone(),
            extract_from_alias_from_cte_name(cte_name).to_string(),
            per_alias_mapping.clone(),
        );

        log::info!(
            "🔧 build_chained: scope_cte_variables updated for alias '{}' → CTE '{}'",
            alias,
            cte_name
        );
        // Detect map-typed expressions and register map keys
        for item in select_items_for_schema {
            if let Some(col_alias) = &item.col_alias {
                if col_alias.0 == *alias {
                    if let Some(keys) =
                        super::variable_scope::extract_map_keys_from_expr(&item.expression)
                    {
                        if let Some(info) = with_scope.scope_cte_variables_mut().get_mut(alias) {
                            info.map_keys = Some(keys);
                        }
                    }
                    break;
                }
            }
        }
    }
}

/// Rewrite a rendered WITH-CTE body's join conditions onto CTE columns and prune
/// orphaned JOINs (a STEP of the main loop's inner render-loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// The analyzer emits joins with base-table columns (e.g. `friend.id`), but after
/// a WITH barrier `friend` is a CTE with prefixed columns (e.g. `p6_friend_id`);
/// `rewrite_join_conditions_for_cte_aliases` remaps them. That can leave a
/// base-table JOIN whose ON condition no longer references the joined table (all
/// refs rewritten to CTE columns) — those orphaned JOINs are removed, folding
/// their conditions into the previous JOIN. No-op when there are no CTE
/// references.
fn rewrite_cte_join_conditions_and_prune_orphans(
    rendered: &mut RenderPlan,
    cte_references: &HashMap<String, String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) {
    if !cte_references.is_empty() {
        rewrite_join_conditions_for_cte_aliases(rendered, cte_references, cte_schemas);

        // Remove orphaned JOINs: when a CTE-backed node appears as a graph
        // endpoint, a base-table JOIN is created. After CTE rewriting, the
        // ON condition may no longer reference the joined table (all refs
        // rewritten to CTE columns). Remove such JOINs, folding conditions
        // into the previous JOIN.
        {
            use super::expression_utils::references_alias;
            let mut orphaned_indices: Vec<usize> = Vec::new();
            for (i, join) in rendered.joins.0.iter().enumerate() {
                // Only consider JOINs for CTE-backed aliases
                if !cte_references.contains_key(&join.table_alias) {
                    continue;
                }
                // Skip CTE/VLP table JOINs — only remove base table JOINs
                if join.table_name.starts_with("with_") || join.table_name.starts_with("vlp_") {
                    continue;
                }
                // Skip if JOIN has pre_filter or no conditions
                if join.pre_filter.is_some() || join.joining_on.is_empty() {
                    continue;
                }
                let alias = &join.table_alias;

                // Only remove if no MEANINGFUL non-CTE-backed JOINs follow.
                // A trailing JOIN is "meaningful" if it's referenced in
                // SELECT/WHERE/ORDER BY (i.e., it's not itself orphaned).
                // This prevents removing mid-chain JOINs that downstream
                // restructuring code (complex-5 countIf) depends on.
                let has_meaningful_non_cte_after = rendered.joins.0[i + 1..].iter().any(|j| {
                    if cte_references.contains_key(&j.table_alias)
                        || j.table_name.starts_with("with_")
                        || j.table_name.starts_with("vlp_")
                    {
                        return false; // CTE/VLP JOINs don't block
                    }
                    let ja = &j.table_alias;
                    rendered
                        .select
                        .items
                        .iter()
                        .any(|item| references_alias(&item.expression, ja))
                        || matches!(
                            &rendered.filters,
                            FilterItems(Some(ref f)) if references_alias(f, ja)
                        )
                        || rendered
                            .order_by
                            .0
                            .iter()
                            .any(|item| references_alias(&item.expression, ja))
                        || rendered
                            .group_by
                            .0
                            .iter()
                            .any(|item| references_alias(item, ja))
                });
                if has_meaningful_non_cte_after {
                    continue;
                }

                // Check if ON condition still references the joined table
                let references_self = join.joining_on.iter().any(|cond| {
                    references_alias(&RenderExpr::OperatorApplicationExp(cond.clone()), alias)
                });
                if references_self {
                    continue;
                }

                // Check if alias is referenced ANYWHERE else in the query
                let used_in_select = rendered
                    .select
                    .items
                    .iter()
                    .any(|item| references_alias(&item.expression, alias));
                let used_in_filter = matches!(
                    &rendered.filters,
                    FilterItems(Some(ref f)) if references_alias(f, alias)
                );
                let used_in_order = rendered
                    .order_by
                    .0
                    .iter()
                    .any(|item| references_alias(&item.expression, alias));
                let used_in_group_by = rendered
                    .group_by
                    .0
                    .iter()
                    .any(|item| references_alias(item, alias));
                let used_in_having = rendered
                    .having_clause
                    .as_ref()
                    .is_some_and(|h| references_alias(h, alias));
                let used_in_other_joins = rendered.joins.0.iter().enumerate().any(|(j, jn)| {
                    j != i
                        && (jn.joining_on.iter().any(|c| {
                            references_alias(&RenderExpr::OperatorApplicationExp(c.clone()), alias)
                        }) || jn
                            .pre_filter
                            .as_ref()
                            .is_some_and(|pf| references_alias(pf, alias)))
                });

                if used_in_select
                    || used_in_filter
                    || used_in_order
                    || used_in_group_by
                    || used_in_having
                    || used_in_other_joins
                {
                    log::info!(
                        "Orphan JOIN check: keeping {} (sel={} filt={} ord={} grp={} hav={} jn={})",
                        alias,
                        used_in_select,
                        used_in_filter,
                        used_in_order,
                        used_in_group_by,
                        used_in_having,
                        used_in_other_joins
                    );
                    continue;
                }

                log::info!(
                    "Orphan JOIN removal: removing orphaned JOIN {} (table {})",
                    alias,
                    join.table_name
                );
                orphaned_indices.push(i);
            }

            for &i in orphaned_indices.iter().rev() {
                let removed = rendered.joins.0.remove(i);
                // Fold conditions into previous JOIN
                if i > 0 {
                    if let Some(prev_join) = rendered.joins.0.get_mut(i - 1) {
                        for cond in removed.joining_on {
                            prev_join.joining_on.push(cond);
                        }
                    }
                }
            }
        }

        // Fix INNER→LEFT in OPTIONAL MATCH CTE bodies.
        // When a CTE reference is LEFT JOINed (indicating OPTIONAL MATCH),
        // any INNER JOINs after it should also be LEFT — the inference may
        // generate INNER for endpoints (e.g., person2) that weren't in the
        // optional_aliases set.
        // We specifically require the CTE JOIN itself to be LEFT, not just
        // any LEFT JOIN in the body, to avoid converting genuinely INNER JOINs
        // in non-OPTIONAL contexts.
        {
            let first_left_cte_idx = rendered.joins.0.iter().position(|j| {
                matches!(j.join_type, super::JoinType::Left)
                    && (j.table_name.starts_with("with_") || j.table_name.starts_with("vlp_"))
            });
            if let Some(cte_idx) = first_left_cte_idx {
                for j in rendered.joins.0[cte_idx..].iter_mut() {
                    if matches!(j.join_type, super::JoinType::Inner) {
                        log::info!(
                            "OPTIONAL MATCH fix: converting INNER→LEFT for JOIN {} ({})",
                            j.table_alias,
                            j.table_name
                        );
                        j.join_type = super::JoinType::Left;
                    }
                }
            }
        }

        // Remove spurious auto-generated duplicate JOINs.
        // When the system creates aliases like t13 and t13_1 for the same
        // relationship table, the suffixed one (t13_1) is a duplicate.
        // Only remove suffixed duplicates (alias_N where alias also exists),
        // and only if the suffixed alias is NOT referenced in SELECT/WHERE/etc.
        {
            use super::expression_utils::references_alias;
            let alias_set: std::collections::HashSet<String> = rendered
                .joins
                .0
                .iter()
                .map(|j| j.table_alias.clone())
                .collect();
            let mut dup_indices: Vec<usize> = Vec::new();
            for (i, j) in rendered.joins.0.iter().enumerate() {
                // Check if alias matches pattern "base_N" where "base" also exists
                if let Some(pos) = j.table_alias.rfind('_') {
                    let base = &j.table_alias[..pos];
                    let suffix = &j.table_alias[pos + 1..];
                    if suffix.chars().all(|c| c.is_ascii_digit()) && alias_set.contains(base) {
                        // Verify the suffixed alias isn't referenced anywhere
                        let alias = &j.table_alias;
                        let used = rendered
                            .select
                            .items
                            .iter()
                            .any(|item| references_alias(&item.expression, alias))
                            || matches!(
                                &rendered.filters,
                                FilterItems(Some(ref f)) if references_alias(f, alias)
                            )
                            || rendered
                                .order_by
                                .0
                                .iter()
                                .any(|item| references_alias(&item.expression, alias))
                            || rendered
                                .group_by
                                .0
                                .iter()
                                .any(|item| references_alias(item, alias))
                            || rendered.joins.0.iter().enumerate().any(|(j2, jn)| {
                                j2 != i
                                    && jn.joining_on.iter().any(|c| {
                                        references_alias(
                                            &RenderExpr::OperatorApplicationExp(c.clone()),
                                            alias,
                                        )
                                    })
                            });
                        if !used {
                            dup_indices.push(i);
                        }
                    }
                }
            }
            if !dup_indices.is_empty() {
                dup_indices.sort_unstable();
                dup_indices.dedup();
                for &i in dup_indices.iter().rev() {
                    log::info!(
                        "Removing spurious duplicate JOIN {} ({})",
                        rendered.joins.0[i].table_alias,
                        rendered.joins.0[i].table_name
                    );
                    rendered.joins.0.remove(i);
                }
            }
        }
    }
}

/// Rewrite orphaned composite alias references and apply the augmented CTE scope
/// to a rendered WITH-CTE body (a STEP of the main loop's inner render-loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// A CTE body may reference composite aliases (e.g. `countWindow1_tag`) while its
/// FROM/JOINs use the individual aliases (`tag`). Build an augmented `VariableScope`
/// = the current WITH scope PLUS the variable→column mappings extracted from any
/// VLP CTEs already hoisted into `all_ctes` (so bare vars like `friend` resolve to
/// `t.end_id`), then run the bare-variable / orphan-table-alias / CTE-property
/// rewrites over `rendered`. Must run AFTER all other modifications to `rendered`.
fn fix_composite_alias_refs_and_augment_scope(
    rendered: &mut RenderPlan,
    all_ctes: &[Cte],
    cte_references: &HashMap<String, String>,
    current_plan: &LogicalPlan,
    with_scope: &WithBarrierScope,
    schema: &GraphSchema,
    body_scope_ref: Option<&super::variable_scope::VariableScope>,
) {
    // Fix composite alias references in the rendered plan.
    // CTE body plans may have expressions using composite aliases (e.g., "countWindow1_tag")
    // while the FROM/JOINs use individual aliases (e.g., "tag"). This post-processing step
    // rewrites orphaned composite alias references to match the actual FROM/JOIN aliases.
    // MUST be called AFTER all modifications to `rendered` (SELECT, GROUP BY, ORDER BY, etc.)
    //
    // Build an augmented scope that includes VLP-derived variables from all_ctes.
    // VLP CTEs have been hoisted into all_ctes by this point, so we can extract
    // variable→column mappings for bare variable rewriting (e.g., `friend` → `t.end_id`).
    let augmented_scope = {
        let mut vars = with_scope.scope_cte_variables().clone();
        for vlp_cte in all_ctes {
            // Only process actual VLP CTEs (which have from_alias set).
            // Normal WITH CTEs may have non-empty columns but no from_alias.
            if vlp_cte.columns.is_empty() || vlp_cte.from_alias.is_none() {
                continue;
            }
            let vlp_from_alias = vlp_cte.from_alias.clone().unwrap();
            // Group columns by cypher_alias to build per-alias property mappings
            let mut alias_props: HashMap<String, HashMap<String, String>> = HashMap::new();
            let mut alias_labels: HashMap<String, Vec<String>> = HashMap::new();
            for col in &vlp_cte.columns {
                if col.cypher_alias.is_empty() {
                    continue;
                }
                alias_props
                    .entry(col.cypher_alias.clone())
                    .or_default()
                    .insert(col.cypher_property.clone(), col.cte_column_name.clone());
            }
            if let Some(ref start_alias) = vlp_cte.vlp_cypher_start_alias {
                if let Some(ref table) = vlp_cte.vlp_start_table {
                    let label = table.rsplit('.').next().unwrap_or(table);
                    alias_labels
                        .entry(start_alias.clone())
                        .or_default()
                        .push(label.to_string());
                }
            }
            if let Some(ref end_alias) = vlp_cte.vlp_cypher_end_alias {
                if let Some(ref table) = vlp_cte.vlp_end_table {
                    let label = table.rsplit('.').next().unwrap_or(table);
                    alias_labels
                        .entry(end_alias.clone())
                        .or_default()
                        .push(label.to_string());
                }
            }
            for (alias, prop_map) in alias_props {
                if vars.contains_key(&alias) {
                    continue; // Don't overwrite prior WITH variables
                }
                log::debug!(
                "🔧 Augmenting scope with VLP variable '{}' from CTE '{}' ({} props, from_alias='{}')",
                alias, vlp_cte.cte_name, prop_map.len(), vlp_from_alias
            );
                vars.insert(
                    alias.clone(),
                    super::variable_scope::CteVariableInfo {
                        cte_name: vlp_cte.cte_name.clone(),
                        property_mapping: prop_map,
                        labels: alias_labels.remove(&alias).unwrap_or_default(),
                        from_alias_override: Some(vlp_from_alias.clone()),
                        map_keys: None,
                    },
                );
            }
        }
        // Also add composite alias entries from cte_references, but ONLY if
        // the composite alias is actually referenced as a table prefix in the
        // rendered plan. This avoids spurious CROSS JOINs for unreferenced CTEs.
        // After scope_cte_variables.clear(), composite aliases from earlier WITHs
        // are lost (only individual aliases from the current WITH are present).
        // This allows fix_orphan_table_aliases to map composite aliases
        // (e.g., "country_messageCount_months_zombie") to the correct FROM/JOIN alias.
        let mut used_aliases = std::collections::HashSet::new();
        for item in &rendered.select.items {
            collect_aliases_from_single_render_expr(&item.expression, &mut used_aliases);
        }
        if let FilterItems(Some(ref filter)) = rendered.filters {
            collect_aliases_from_single_render_expr(filter, &mut used_aliases);
        }
        for gi in &rendered.group_by.0 {
            collect_aliases_from_single_render_expr(gi, &mut used_aliases);
        }
        for oi in &rendered.order_by.0 {
            collect_aliases_from_single_render_expr(&oi.expression, &mut used_aliases);
        }
        if let Some(ref having) = rendered.having_clause {
            collect_aliases_from_single_render_expr(having, &mut used_aliases);
        }
        for (ref_alias, ref_cte_name) in cte_references {
            if vars.contains_key(ref_alias) {
                continue; // Already in scope (individual alias or VLP)
            }
            if !ref_cte_name.starts_with("with_") {
                continue;
            }
            if !used_aliases.contains(ref_alias) {
                continue; // Not referenced in rendered expressions
            }
            // Only add TRUE composite aliases (multi-alias combinations like
            // "country_messageCount_months_zombie"). Skip individual aliases
            // to avoid polluting the scope with stale CTE references.
            // A composite alias has the form "alias1_alias2_..." and the CTE
            // name is "with_{composite}_cte_{N}".
            let expected_cte_prefix = format!("with_{}_cte_", ref_alias);
            if !ref_cte_name.starts_with(&expected_cte_prefix) {
                continue; // Not a composite alias for this CTE
            }
            // Build property mapping from the CTE's columns in all_ctes
            let mut cte_prop_map: HashMap<String, String> = HashMap::new();
            for cte in all_ctes {
                if cte.cte_name == *ref_cte_name {
                    for col in &cte.columns {
                        if !col.cypher_property.is_empty() {
                            cte_prop_map
                                .insert(col.cypher_property.clone(), col.cte_column_name.clone());
                        }
                    }
                    break;
                }
            }
            log::debug!(
                "🔧 Augmenting scope with composite alias '{}' → CTE '{}' ({} props)",
                ref_alias,
                ref_cte_name,
                cte_prop_map.len()
            );
            vars.insert(
                ref_alias.clone(),
                super::variable_scope::CteVariableInfo {
                    cte_name: ref_cte_name.clone(),
                    property_mapping: cte_prop_map,
                    labels: vec![],
                    from_alias_override: None,
                    map_keys: None,
                },
            );
        }
        vars
    };
    let has_augmented = !augmented_scope.is_empty();
    if has_augmented || body_scope_ref.is_some() {
        let aug_scope = super::variable_scope::VariableScope::with_cte_variables(
            schema,
            current_plan,
            augmented_scope,
        );
        // Order matters: rewrite_bare_variables converts bare TableAlias/ColumnAlias
        // (e.g., "score") into PropertyAccessExp (e.g., "composite_alias.score").
        // Then fix_orphan_table_aliases rewrites the composite alias to the actual
        // FROM/JOIN alias (e.g., "person1.score"). Running fix_orphan first would
        // miss expressions that rewrite_bare_variables creates later.
        super::variable_scope::rewrite_bare_variables_in_plan(rendered, &aug_scope);
        super::variable_scope::fix_orphan_table_aliases(rendered, &aug_scope);
        super::variable_scope::rewrite_cte_property_columns(rendered, &aug_scope);
    }
}

/// Render a WITH segment's body plan to a `RenderPlan` (a STEP of the main loop's
/// inner render-loop in `build_chained_with_match_cte_plan`, Phase-4 §7.1
/// extraction).
///
/// When the body itself contains nested WITH clauses, recurse through
/// `build_chained_with_match_cte_plan` (our own logic, avoiding infinite loops);
/// otherwise render directly via `to_render_plan_with_ctx`. Both paths thread the
/// accumulated `body_scope_ref` so CTE-body rendering resolves variables from prior
/// WITHs.
fn render_with_cte_body(
    plan_to_render: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&PlanCtx>,
    body_scope_ref: Option<&super::variable_scope::VariableScope>,
) -> RenderPlanBuilderResult<RenderPlan> {
    if has_with_clause_in_graph_rel(plan_to_render) {
        // The plan has nested WITH clauses - process them using our own logic
        log::debug!("🔧 build_chained_with_match_cte_plan: Plan has nested WITH clauses, processing recursively with our own logic");
        build_chained_with_match_cte_plan(plan_to_render, schema, plan_ctx, body_scope_ref)
    } else {
        // No nested WITH clauses - render directly
        log::debug!("🔧 build_chained_with_match_cte_plan: Plan has no nested WITH clauses, rendering directly with plan_ctx");
        plan_to_render.to_render_plan_with_ctx(schema, plan_ctx, body_scope_ref)
    }
}

/// Destructure a WITH-alias-group plan into the parts the render loop needs (a
/// STEP of the main loop's inner render-loop in `build_chained_with_match_cte_plan`,
/// Phase-4 §7.1 extraction).
///
/// Returns `(plan_to_render, with_items, with_distinct, with_order_by, with_skip,
/// with_limit, with_where_clause, input_cte_refs)`. For a `WithClause` this unwraps
/// its input + modifiers + analyzer CTE references; for any other plan shape it
/// renders the plan as-is with no modifiers. `plan_to_render` borrows `with_plan`.
#[allow(clippy::type_complexity)]
fn extract_with_plan_parts<'a>(
    with_plan: &'a LogicalPlan,
    with_alias: &str,
) -> (
    &'a LogicalPlan,
    Option<Vec<crate::query_planner::logical_plan::ProjectionItem>>,
    bool,
    Option<Vec<crate::query_planner::logical_plan::OrderByItem>>,
    Option<u64>,
    Option<u64>,
    Option<crate::query_planner::logical_expr::LogicalExpr>,
    HashMap<String, String>,
) {
    match with_plan {
        LogicalPlan::WithClause(wc) => {
            log::debug!("� DEBUG: Unwrapping WithClause for alias '{}'", with_alias);
            log::debug!("🐛 DEBUG: WithClause has {} items", wc.items.len());
            for (i, item) in wc.items.iter().enumerate() {
                log::debug!("🐛 DEBUG: wc.items[{}]: {:?}", i, item);
            }
            log::debug!(
                "�🔧 build_chained_with_match_cte_plan: Unwrapping WithClause, rendering input"
            );

            // Use CTE references from this WithClause (populated by analyzer)
            let input_cte_refs = wc.cte_references.clone();
            log::info!(
                "🔧 build_chained_with_match_cte_plan: CTE refs from WithClause: {:?}",
                input_cte_refs
            );
            // Debug: if it's GraphJoins, log the joins
            if let LogicalPlan::GraphJoins(gj) = wc.input.as_ref() {
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: wc.input is GraphJoins with {} joins",
                    gj.joins.len()
                );
                for (i, join) in gj.joins.iter().enumerate() {
                    log::debug!("🔧 build_chained_with_match_cte_plan: GraphJoins join {}: table_name={}, table_alias={}, joining_on={:?}",
                    i, join.table_name.as_str(), join.table_alias.as_str(), join.joining_on);
                }
            }
            (
                wc.input.as_ref(),
                Some(wc.items.clone()),
                wc.distinct,
                wc.order_by.clone(),
                wc.skip,
                wc.limit,
                wc.where_clause.clone(),
                input_cte_refs,
            )
        }
        LogicalPlan::Projection(proj) => {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: WITH projection input type: {:?}",
                std::mem::discriminant(proj.input.as_ref())
            );
            // Check if input contains CTE reference
            if let LogicalPlan::Filter(filter) = proj.input.as_ref() {
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Filter input type: {:?}",
                    std::mem::discriminant(filter.input.as_ref())
                );
            }
            (
                with_plan as &LogicalPlan,
                None,
                false,
                None,
                None,
                None,
                None,
                std::collections::HashMap::new(),
            )
        }
        _ => (
            with_plan as &LogicalPlan,
            None,
            false,
            None,
            None,
            None,
            None,
            std::collections::HashMap::new(),
        ),
    }
}

/// Collapse a simple passthrough WITH whose input is already a CTE reference (a
/// STEP of the main loop's inner render-loop in `build_chained_with_match_cte_plan`,
/// Phase-4 §7.1 extraction).
///
/// A passthrough is `WITH x` after an existing CTE for `x` — a single TableAlias
/// item with no ORDER BY / SKIP / LIMIT / DISTINCT / WHERE. When detected, collapse
/// it out of `current_plan`, map its exported aliases (and analyzer CTE-name
/// remaps) onto the existing CTE, set `any_processed_this_iteration`, and return
/// `Ok(true)` to signal the caller to `break 'alias_loop` and restart iteration
/// (because `current_plan` changed). Returns `Ok(false)` when this WITH is not a
/// collapsible passthrough (caller proceeds to render it normally).
fn try_collapse_passthrough_with(
    with_plan: &LogicalPlan,
    with_alias: &str,
    current_plan: &mut LogicalPlan,
    cte_references: &mut HashMap<String, String>,
    cte_name_allocator: &mut CteNameAllocator,
    any_processed_this_iteration: &mut bool,
) -> RenderPlanBuilderResult<bool> {
    if let LogicalPlan::WithClause(wc) = with_plan {
        if let Some(existing_cte) = is_cte_reference(&wc.input) {
            // Check if this is a simple passthrough (same alias, no modifications)
            let is_simple_passthrough = wc.items.len() == 1
                && wc.order_by.is_none()
                && wc.skip.is_none()
                && wc.limit.is_none()
                && !wc.distinct
                && wc.where_clause.is_none()  // CRITICAL: WHERE clause makes it not a passthrough!
                && matches!(
                    &wc.items[0].expression,
                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
                );

            log::debug!("🔧 build_chained_with_match_cte_plan: Checking passthrough: items={}, order_by={}, skip={}, limit={}, distinct={}, where_clause={}, is_table_alias={}, is_passthrough={}",
                       wc.items.len(), wc.order_by.is_some(), wc.skip.is_some(), wc.limit.is_some(), wc.distinct,
                       wc.where_clause.is_some(),
                       matches!(&wc.items[0].expression, crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)),
                       is_simple_passthrough);

            if is_simple_passthrough {
                log::debug!("TEST: This should show up");
                log::debug!(
                    "🔧 DEBUG: ENTERING passthrough collapse for '{}'",
                    with_alias
                );

                // CRITICAL FIX: For passthrough WITHs, we need to collapse them too!
                // They wrap an existing CTE reference and should be removed.
                // For passthrough, use empty string to indicate passthrough collapse
                let target_cte = "".to_string();
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: Collapsing passthrough WITH for '{}' with CTE '{}'",
                    with_alias, target_cte
                );
                *current_plan = collapse_passthrough_with(current_plan, with_alias, &target_cte)?;
                log::debug!(
                    "🔧 build_chained_with_match_cte_plan: After passthrough collapse, plan discriminant: {:?}",
                    std::mem::discriminant(&*current_plan)
                );

                // CRITICAL FIX: Update cte_references to map the skipped WITH's aliases
                // to the actual CTE name. This ensures the final SELECT uses the correct CTE.
                //
                // Problem: Analyzer generates unique CTE names for each WITH clause
                //   (e.g., with_name_cte_1, with_name_cte_2), but when passthrough WITHs
                //   are skipped, the outer expressions still reference the skipped WITH's CTE name.
                //
                // Solution: Map all exported aliases of the skipped WITH to the existing CTE.
                // ALSO: Extract the analyzer's CTE name for this WITH to collapse it properly.
                for alias in &wc.exported_aliases {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Mapping skipped alias '{}' → existing CTE '{}'",
                        alias, existing_cte
                    );
                    cte_references.insert(alias.clone(), existing_cte.clone());

                    // Also record CTE name remapping: analyzer's CTE name → actual CTE name
                    // The analyzer assigned a unique CTE name to this WITH, but we're skipping it.
                    // We need to remap expressions that reference the analyzer's name.
                    log::debug!(
                        "🔧 DEBUG: wc.cte_references = {:?}, looking for alias '{}'",
                        wc.cte_references,
                        alias
                    );
                    if let Some(analyzer_cte_name) = wc.cte_references.get(alias) {
                        log::debug!(
                            "🔧 DEBUG: Found analyzer_cte_name '{}', existing_cte = '{}'",
                            analyzer_cte_name,
                            existing_cte
                        );
                        if analyzer_cte_name != &existing_cte {
                            log::info!(
                                "🔧 build_chained_with_match_cte_plan: Recording CTE name remap: '{}' → '{}'",
                                analyzer_cte_name, existing_cte
                            );
                            cte_name_allocator
                                .record_remapping(analyzer_cte_name.clone(), existing_cte.clone());
                        }
                    }
                }

                // Mark that we processed something (collapsing passthrough is processing)
                *any_processed_this_iteration = true;

                // Collapsed a passthrough WITH — signal the caller to break
                // 'alias_loop and restart iteration (current_plan changed).
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Apply the WITH segment's ORDER BY / SKIP / LIMIT and WHERE→HAVING to the
/// rendered CTE body (a STEP of the main loop's inner render-loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Builds a scope-aware `ExpressionRewriteContext` (mapping Cypher property names
/// to CTE columns), then: applies ORDER BY (stripping table aliases when CTE scope
/// was used), SKIP, and LIMIT; and applies the WITH WHERE — reverse-mapping renamed
/// aliases, rewriting through scope + denormalized-property resolution, and routing
/// it to HAVING when GROUP BY is present (else combining into the filters, including
/// the BidirectionalUnion / denorm from/to UNION branches). Mutates `rendered`; the
/// four modifier params are consumed. Returns `Err` only on WHERE `try_into`.
#[allow(clippy::too_many_arguments)]
fn apply_with_order_by_skip_limit_where(
    rendered: &mut RenderPlan,
    plan_to_render: &LogicalPlan,
    body_scope_ref: Option<&super::variable_scope::VariableScope>,
    with_order_by: Option<Vec<crate::query_planner::logical_plan::OrderByItem>>,
    with_skip: Option<u64>,
    with_limit: Option<u64>,
    with_where_clause: Option<crate::query_planner::logical_expr::LogicalExpr>,
    with_items: &Option<Vec<crate::query_planner::logical_plan::ProjectionItem>>,
    cte_from_alias: &Option<String>,
) -> RenderPlanBuilderResult<()> {
    // Build scope-aware rewrite context for ORDER BY and WHERE/HAVING
    // from WithClause. This maps Cypher property names to CTE column names.
    use crate::query_planner::logical_expr::expression_rewriter::{
        rewrite_expression_with_property_mapping, ExpressionRewriteContext,
    };
    let with_rewrite_ctx = if let Some(s) = body_scope_ref {
        ExpressionRewriteContext::with_scope(plan_to_render, s)
    } else {
        ExpressionRewriteContext::new(plan_to_render)
    };

    // Apply WithClause's ORDER BY, SKIP, LIMIT to the rendered plan
    if let Some(order_by_items) = with_order_by {
        log::debug!("🔧 build_chained_with_match_cte_plan: Applying ORDER BY from WithClause");
        let has_cte_scope = body_scope_ref.is_some();
        let render_order_by: Vec<OrderByItem> = order_by_items
            .iter()
            .filter_map(|item| {
                let rewritten =
                    rewrite_expression_with_property_mapping(&item.expression, &with_rewrite_ctx);
                let expr_result: Result<RenderExpr, _> = rewritten.try_into();
                expr_result.ok().map(|expr| {
                    // Strip table aliases only when CTE scope was used.
                    // CTE scope resolves to CTE names as table aliases,
                    // which need stripping for bare output column references
                    // (especially after GROUP BY over UNION subqueries).
                    // Without scope (first WITH), keep original table aliases
                    // since they reference actual FROM/JOIN tables.
                    let final_expr = if has_cte_scope {
                        strip_table_alias_from_resolved(&expr)
                    } else {
                        expr
                    };
                    OrderByItem {
                        expression: final_expr,
                        order: match item.order {
                            crate::query_planner::logical_plan::OrderByOrder::Asc => {
                                OrderByOrder::Asc
                            }
                            crate::query_planner::logical_plan::OrderByOrder::Desc => {
                                OrderByOrder::Desc
                            }
                        },
                    }
                })
            })
            .collect();
        rendered.order_by = OrderByItems(render_order_by);
    }
    if let Some(skip_count) = with_skip {
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Applying SKIP {} from WithClause",
            skip_count
        );
        rendered.skip = SkipItem(Some(skip_count as i64));
    }
    if let Some(limit_count) = with_limit {
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Applying LIMIT {} from WithClause",
            limit_count
        );
        rendered.limit = LimitItem(Some(limit_count as i64));
    }

    // Apply WHERE clause from WITH - becomes HAVING if we have GROUP BY
    if let Some(where_predicate) = with_where_clause {
        log::debug!("🔧 build_chained_with_match_cte_plan: Applying WHERE clause from WITH");

        // 🔧 FIX: Rewrite renamed aliases back to source aliases in WHERE clause.
        // For `WITH u AS person WHERE person.user_id = 1`, "person" must become "u"
        // so that property mapping resolution can find the correct schema mappings.
        let where_predicate = if let Some(ref items) = with_items {
            let mut reverse_map = std::collections::HashMap::new();
            for item in items {
                if let (LogicalExpr::TableAlias(ta), Some(col_alias)) =
                    (&item.expression, &item.col_alias)
                {
                    reverse_map.insert(col_alias.0.clone(), ta.0.clone());
                }
            }
            if reverse_map.is_empty() {
                where_predicate
            } else {
                log::info!("🔧 Rewriting WITH WHERE aliases: {:?}", reverse_map);
                rewrite_logical_expr_aliases(&where_predicate, &reverse_map)
            }
        } else {
            where_predicate
        };

        // Rewrite through scope to map Cypher properties to CTE columns
        let where_rewritten =
            rewrite_expression_with_property_mapping(&where_predicate, &with_rewrite_ctx);
        let mut where_render_expr: RenderExpr = where_rewritten.try_into()?;

        // #633: resolve an FK-edge coupled relationship variable in the
        // post-WITH WHERE (`r.<col>`) to its coupled node alias when the
        // CTE body's FROM binds the node (not the rel var). Same gate +
        // self-ref guard as the pre-WITH filter path; no-op otherwise.
        super::plan_builder_helpers::remap_coupled_rel_vars_in_filter(
            &mut where_render_expr,
            plan_to_render,
            cte_from_alias.as_deref(),
        );

        if !rendered.group_by.0.is_empty() {
            // We have GROUP BY - WHERE becomes HAVING
            log::debug!("🔧 build_chained_with_match_cte_plan: Converting WHERE to HAVING (GROUP BY present)");
            rendered.having_clause = Some(where_render_expr);
        } else {
            // No GROUP BY - apply as regular WHERE filter
            log::debug!("🔧 build_chained_with_match_cte_plan: Applying WHERE as filter predicate");

            // Combine with existing filters (base plan = first UNION branch)
            let new_filter = if let Some(existing_filter) = rendered.filters.0.take() {
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![existing_filter, where_render_expr.clone()],
                })
            } else {
                where_render_expr.clone()
            };
            rendered.filters = FilterItems(Some(new_filter));

            // 🔧 FIX: Also propagate WHERE to UNION branches
            // When the CTE body is a UNION ALL (from BidirectionalUnion),
            // the base plan's filter only applies to branch 1.
            // We must also apply the post-WITH WHERE to all remaining branches.
            // Use where_render_expr (the raw WHERE predicate), not new_filter
            // (which already includes branch 1's existing filters).
            //
            // For a coupled-denormalized from/to UNION, `where_render_expr` was
            // resolved position-blind — the label→column mapping always yields
            // the from/origin column (e.g. `a.origin_state`). Copying it verbatim
            // to the dest branch filters the WRONG physical column (`origin_state`
            // instead of `dest_state`), polluting the exported node set (#456,
            // with_match_chain: 7 rows vs 4). Re-point each column reference per
            // branch to that branch's OWN column for the same exported property,
            // using the branch SELECT items (property alias ↔ db column). For a
            // homogeneous UNION (non-denorm BidirectionalUnion) every branch
            // projects the same columns, so the remap is the identity.
            if let Some(ref mut union) = rendered.union.0 {
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Propagating post-WITH WHERE to {} UNION branches",
                    union.input.len()
                );

                /// Re-points a filter's column references to a target
                /// branch's columns: `db_col → exported alias → branch db_col`.
                /// Implemented on `ExprVisitor` so every expression wrapper
                /// (CASE, lists, subscripts/slices, subqueries, …) is walked
                /// by the default recursive `transform_expr` — a hand-rolled
                /// walk here previously missed CASE and left the dest branch
                /// filtering the origin column inside it (#456 follow-up).
                struct BranchWhereColRemapper<'a> {
                    col_to_alias: &'a std::collections::HashMap<String, String>,
                    alias_to_col: &'a std::collections::HashMap<String, String>,
                }
                impl super::expression_utils::ExprVisitor for BranchWhereColRemapper<'_> {
                    fn transform_property_access(&mut self, prop: &PropertyAccess) -> RenderExpr {
                        let cur = prop.column.raw().to_string();
                        if let Some(alias) = self.col_to_alias.get(&cur) {
                            if let Some(new_col) = self.alias_to_col.get(alias) {
                                if *new_col != cur {
                                    return RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: prop.table_alias.clone(),
                                        column: PropertyValue::Column(new_col.clone()),
                                    });
                                }
                            }
                        }
                        RenderExpr::PropertyAccessExp(prop.clone())
                    }
                }

                // Global db_column → exported property alias (col_alias),
                // gathered across all branches (e.g. origin_state→p1_a_state
                // AND dest_state→p1_a_state).
                //
                // HAZARD: this map is keyed on the bare db column and the
                // remap ignores `pa.table_alias` (last write wins). If a CTE
                // body ever materializes TWO aliases whose branches project
                // the SAME physical column under DIFFERENT exported aliases,
                // the predicate could be re-pointed through the wrong export.
                // Today the denorm from/to UNION materializes a single node
                // alias, so the keys are unambiguous per CTE.
                let mut col_to_alias: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                for branch in union.input.iter() {
                    for item in &branch.select.items {
                        if let (RenderExpr::PropertyAccessExp(pa), Some(ca)) =
                            (&item.expression, &item.col_alias)
                        {
                            col_to_alias.insert(pa.column.raw().to_string(), ca.0.clone());
                        }
                    }
                }

                for branch in union.input.iter_mut() {
                    // This branch's exported alias → db column.
                    let mut alias_to_col: std::collections::HashMap<String, String> =
                        std::collections::HashMap::new();
                    for item in &branch.select.items {
                        if let (RenderExpr::PropertyAccessExp(pa), Some(ca)) =
                            (&item.expression, &item.col_alias)
                        {
                            alias_to_col.insert(ca.0.clone(), pa.column.raw().to_string());
                        }
                    }

                    use super::expression_utils::ExprVisitor as _;
                    let mut remapper = BranchWhereColRemapper {
                        col_to_alias: &col_to_alias,
                        alias_to_col: &alias_to_col,
                    };
                    let branch_where = remapper.transform_expr(&where_render_expr);

                    branch.filters = match branch.filters.0.take() {
                        Some(existing) => FilterItems(Some(RenderExpr::OperatorApplicationExp(
                            OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing, branch_where],
                            },
                        ))),
                        None => FilterItems(Some(branch_where)),
                    };
                }
            }
        }
    }

    Ok(())
}

/// Build the WITH-projection `SelectItem`s (Phase-4 §7.1 extraction from
/// `apply_with_items_projection`).
///
/// Expands each WITH item into CTE SELECT columns: a bare `TableAlias` fans out
/// to all of the node's columns (UNWIND aliases stay simple column refs), while
/// non-alias expressions get path-function rewriting, property-mapping, `collect`
/// → `groupArray` expansion, `head(collect(MapLiteral))` flattening (accumulating
/// compound keys into `flattened_compound_keys`), and VLP-CTE column rewriting
/// when FROM is a `vlp_…` CTE. Pure w.r.t. `rendered` (reads `rendered.from` only).
#[allow(clippy::too_many_arguments)]
fn build_with_projection_select_items(
    items: &[crate::query_planner::logical_plan::ProjectionItem],
    plan_to_render: &LogicalPlan,
    rendered: &RenderPlan,
    has_aggregation: bool,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references_for_rendering: &HashMap<String, String>,
    cte_from_alias: &Option<String>,
    plan_ctx: Option<&PlanCtx>,
    body_scope_ref: Option<&super::variable_scope::VariableScope>,
    vlp_cte_metadata: &HashMap<String, (String, Vec<super::CteColumnMetadata>)>,
    flattened_compound_keys: &std::cell::RefCell<Vec<(String, String)>>,
) -> Vec<SelectItem> {
    let mut unwind_aliases = std::collections::HashSet::new();
    collect_unwind_aliases(plan_to_render, &mut unwind_aliases);

    items.iter()
                    .flat_map(|item| {
                        // Check if this is a TableAlias that needs expansion to ALL columns
                        match &item.expression {
                            crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                // UNWIND aliases are ARRAY JOIN columns — emit a simple column reference
                                if unwind_aliases.contains(alias.0.as_str()) {
                                    // Honor the WITH rename (`WITH x AS y`): the CTE
                                    // body physically has the ARRAY JOIN column
                                    // `alias.0` (x), but the exported column must be
                                    // named after `item.col_alias` (y) so downstream
                                    // references (`count(y)` → `y.y`) resolve. Before
                                    // #864 this hard-coded `alias.0` for BOTH sides,
                                    // so a rename produced `SELECT x AS x` while the
                                    // outer query aliased the CTE `AS y` and looked
                                    // for `y.y` — Code 47. Passthrough (no rename)
                                    // has col_alias == alias.0, so it is unchanged.
                                    let out_name = item
                                        .col_alias
                                        .as_ref()
                                        .map(|a| a.0.clone())
                                        .unwrap_or_else(|| alias.0.clone());
                                    log::debug!("🔧 build_chained_with_match_cte_plan: UNWIND alias '{}' — simple column reference AS '{}'", alias.0, out_name);
                                    return vec![SelectItem {
                                        expression: super::render_expr::RenderExpr::ColumnAlias(
                                            super::render_expr::ColumnAlias(alias.0.clone()),
                                        ),
                                        col_alias: Some(ColumnAlias(out_name)),
                                    }];
                                }

                                // Use unified expansion helper (Dec 2025)
                                // CRITICAL: Use cte_references_for_rendering (includes ALL previous CTEs),
                                // NOT with_cte_refs (only includes CTEs visible in this WITH's immediate input)
                                // This allows "WITH a, b, c" to find "a" and "b" from previous CTEs
                                //
                                // The unified helper automatically handles anyLast() wrapping when has_aggregation=true
                                let expanded = expand_table_alias_to_select_items(
                                    &alias.0,
                                    plan_to_render,
                                    cte_schemas,
                                    cte_references_for_rendering,
                                    has_aggregation,  // Enables anyLast() wrapping in unified function
                                    plan_ctx,  // Pass Option<&PlanCtx> for property pruning
                                    Some(vlp_cte_metadata)  // Pass VLP CTE metadata for FROM alias lookup
                                );
                                log::debug!("🔧 build_chained_with_match_cte_plan: Expanded alias '{}' to {} items (aggregation={})",
                                           alias.0, expanded.len(), has_aggregation);

                                expanded
                            }
                            _ => {
                                // Not a TableAlias, convert normally
                                // First, check if we need to rewrite path functions
                                // For variable-length paths, convert length(path) → hop_count, etc.
                                let logical_expr = if let Some(path_var_name) = get_path_variable(plan_to_render) {
                                    // Rewrite path functions in the logical expression BEFORE converting to RenderExpr
                                    rewrite_logical_path_functions(&item.expression, path_var_name.as_str())
                                } else {
                                    item.expression.clone()
                                };

                                // 🔧 CRITICAL FIX: Apply property mapping for WITH expressions
                                // Maps Cypher property names (e.g., u.name) to DB columns (e.g., full_name)
                                // This is the same rewriting that RETURN clause does
                                // SCOPE: Use body_scope_ref to resolve CTE-scoped variables
                                // (e.g., post.creationDate → CTE column after a prior WITH)
                                use crate::query_planner::logical_expr::expression_rewriter::{
                                    ExpressionRewriteContext, rewrite_expression_with_property_mapping,
                                };
                                let rewrite_ctx = if let Some(s) = body_scope_ref {
                                    ExpressionRewriteContext::with_scope(plan_to_render, s)
                                } else {
                                    ExpressionRewriteContext::new(plan_to_render)
                                };
                                let rewritten_expr = rewrite_expression_with_property_mapping(&logical_expr, &rewrite_ctx);
                                log::info!(
                                    "🔧 build_chained_with_match_cte_plan: Rewrote WITH expression with property mapping"
                                );

                                // CRITICAL: Expand collect(node) to groupArray(tuple(...)) BEFORE converting to RenderExpr
                                // This must happen in WITH context too, not just in extract_select_items()
                                let expanded_expr = if let crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(ref agg) = rewritten_expr {
                                    if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                                        if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                                            log::debug!("🔧 WITH context: Expanding collect({}) to groupArray(tuple(...))", alias.0);

                                            // Extract property requirements for pruning
                                            let property_requirements = plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                                            // Get all properties for this alias
                                            match plan_to_render.get_properties_with_table_alias(&alias.0) {
                                                Ok((props, _actual_alias)) if !props.is_empty() => {
                                                    log::debug!("🔧 Found {} properties for alias '{}', expanding", props.len(), alias.0);

                                                    // For collect(node), only collect the ID property to produce groupArray(id).
                                                    // Semantically, collect(node) gathers node identities, and groupArray(id)
                                                    // is compatible with downstream IN/has() checks (Array(T) vs scalar T).
                                                    // groupArray(tuple(...)) would produce Array(Tuple) which fails has() type checks.
                                                    let id_only_props: Vec<_> = props.iter()
                                                        .filter(|(prop_name, _)| prop_name == "id")
                                                        .cloned()
                                                        .collect();
                                                    let collect_props = if id_only_props.is_empty() {
                                                        log::debug!("🔧 collect({}): no 'id' property found, using all {} properties", alias.0, props.len());
                                                        props
                                                    } else {
                                                        log::debug!("🔧 collect({}): using ID-only for groupArray (compatible with IN/has())", alias.0);
                                                        id_only_props
                                                    };

                                                    // Use centralized expansion utility with property requirements
                                                    use crate::render_plan::property_expansion::expand_collect_to_group_array;
                                                    expand_collect_to_group_array(&alias.0, collect_props, property_requirements)
                                                }
                                                _ => {
                                                    log::warn!("⚠️  Could not expand collect({}) in WITH - no properties found, keeping as-is", alias.0);
                                                    rewritten_expr
                                                }
                                            }
                                        } else {
                                            rewritten_expr
                                        }
                                    } else {
                                        rewritten_expr
                                    }
                                } else {
                                    rewritten_expr
                                };

                                // 🔧 FIX: Flatten head(collect(MapLiteral)) with node values
                                // ClickHouse map() requires homogeneous value types, but nodes
                                // have no single value. Expand each map entry to separate CTE columns.
                                log::info!("🔧 Checking for head(collect(MapLiteral)) flattening, alias={:?}, expanded_expr={:?}",
                                    item.col_alias.as_ref().map(|a| &a.0),
                                    std::mem::discriminant(&expanded_expr));
                                if let Some((flattened_items, compound_keys)) = try_flatten_head_collect_map_literal(
                                    &expanded_expr,
                                    item.col_alias.as_ref().map(|a| a.0.as_str()),
                                    plan_to_render,
                                    plan_ctx,
                                    body_scope_ref,
                                ) {
                                    log::info!("🔧 Flattened head(collect(MapLiteral)) into {} columns with {} compound keys",
                                        flattened_items.len(), compound_keys.len());
                                    flattened_compound_keys.borrow_mut().extend(compound_keys);
                                    return flattened_items;
                                }

                                let expr_result: Result<RenderExpr, _> = expanded_expr.try_into();
                                expr_result.ok().map(|mut expr| {
                                    // Rewrite denormalized node aliases (e.g., a → r)
                                    resolve_denormalized_property_in_expr_impl(&mut expr, plan_to_render, cte_from_alias.as_deref());

                                    // 🔧 FIX: VLP CTE column rewriting for non-TableAlias WITH items
                                    // When FROM is a VLP/multi-type CTE, PropertyAccess references
                                    // (e.g., message.content) must be rewritten to CTE columns
                                    // (e.g., t.start_content)
                                    if let Some(from_ref) = &rendered.from.0 {
                                        if from_ref.name.starts_with("vlp_") {
                                            let from_alias = from_ref.alias.as_deref().unwrap_or("t");
                                            // Build mappings: cypher_alias → "start_node" or "end_node"
                                            if let Some((_from_alias_meta, col_metadata)) = vlp_cte_metadata.get(&from_ref.name) {
                                                let mut mappings: HashMap<String, String> = HashMap::new();
                                                for col_meta in col_metadata {
                                                    if !mappings.contains_key(&col_meta.cypher_alias) {
                                                        if let Some(pos) = &col_meta.vlp_position {
                                                            let internal_alias = match pos {
                                                                super::cte_manager::VlpColumnPosition::Start => "start_node".to_string(),
                                                                super::cte_manager::VlpColumnPosition::End => "end_node".to_string(),
                                                            };
                                                            mappings.insert(col_meta.cypher_alias.clone(), internal_alias);
                                                        }
                                                    }
                                                }
                                                if !mappings.is_empty() {
                                                    // Build DB→Cypher property name mapping for VLP column name translation.
                                                    // VLP CTE columns use Cypher names (start_name) but PropertyAccessExp
                                                    // may have DB column names (full_name) after schema resolution.
                                                    let mut db_to_cypher: HashMap<(String, String), String> = HashMap::new();
                                                    for col_meta in col_metadata {
                                                        if col_meta.db_column != col_meta.cypher_property {
                                                            db_to_cypher.insert(
                                                                (col_meta.cypher_alias.clone(), col_meta.db_column.clone()),
                                                                col_meta.cypher_property.clone(),
                                                            );
                                                        }
                                                    }
                                                    if !db_to_cypher.is_empty() {
                                                        translate_db_columns_to_cypher_properties(&mut expr, &db_to_cypher);
                                                    }
                                                    // #620: rewrite endpoint id-property access
                                                    // (a.user_id) to the VLP CTE's authoritative id
                                                    // column (t.start_id/t.end_id) BEFORE the generic
                                                    // prefix rewrite, which would otherwise blindly
                                                    // build `start_user_id` (a column the CTE never
                                                    // projects → Code 47).
                                                    let mut id_columns: HashMap<String, String> = HashMap::new();
                                                    for col_meta in col_metadata {
                                                        if col_meta.is_id_column {
                                                            id_columns.insert(
                                                                col_meta.cypher_alias.clone(),
                                                                col_meta.cypher_property.clone(),
                                                            );
                                                        }
                                                    }
                                                    if !id_columns.is_empty() {
                                                        crate::render_plan::vlp_rewrite::rewrite_vlp_id_property_columns(
                                                            &mut expr, &mappings, &id_columns, from_alias,
                                                        );
                                                    }
                                                    log::debug!("🔧 VLP WITH item rewrite: mappings={:?}, from_alias={}", mappings, from_alias);
                                                    rewrite_render_expr_for_vlp_with_from_alias(&mut expr, &mappings, from_alias);
                                                }
                                            }
                                        }
                                    }
                                    SelectItem {
                                        expression: expr,
                                        col_alias: item.col_alias.as_ref().map(|a| crate::render_plan::render_expr::ColumnAlias(a.0.clone())),
                                    }
                                }).into_iter().collect()
                            }
                        }
                    })
                    .collect()
}

/// Apply the WITH-items projection to a rendered CTE body (a STEP of the main
/// loop's inner render-loop in `build_chained_with_match_cte_plan`, Phase-4 §7.1
/// extraction).
///
/// Handles `WITH friend.firstName AS name`, `WITH count(friend) AS cnt`, and bare
/// `WITH a` (TableAlias) items — expanding, rewriting (scope-aware, with
/// denormalized-property resolution), filtering out pattern-comprehension result
/// aliases, and setting `rendered.select` / `rendered.group_by` (plus the
/// denormalized-UNION restructuring path). No-op when the segment carries no WITH
/// items. Returns `Err` only on the unsupported-feature path. Mutates `rendered`;
/// all other params are read-only inputs.
#[allow(clippy::too_many_arguments)]
fn apply_with_items_projection(
    rendered: &mut RenderPlan,
    plan_to_render: &LogicalPlan,
    with_items: &Option<Vec<crate::query_planner::logical_plan::ProjectionItem>>,
    with_distinct: bool,
    pc_result_aliases: &std::collections::HashSet<String>,
    pc_correlated_aliases: &std::collections::HashSet<String>,
    with_plans: &[LogicalPlan],
    with_alias: &str,
    cte_from_alias: &Option<String>,
    schema: &GraphSchema,
    plan_ctx: Option<&PlanCtx>,
    body_scope_ref: Option<&super::variable_scope::VariableScope>,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references_for_rendering: &HashMap<String, String>,
    vlp_cte_metadata: &HashMap<String, (String, Vec<super::CteColumnMetadata>)>,
    flattened_compound_keys: &std::cell::RefCell<Vec<(String, String)>>,
) -> RenderPlanBuilderResult<()> {
    if let Some(ref items) = with_items {
        log::debug!("🐛 DEBUG: with_items is Some, has {} items", items.len());
        for (i, item) in items.iter().enumerate() {
            log::debug!("🐛 DEBUG: with_item[{}]: {:?}", i, item);
        }

        // Filter out pattern comprehension items — their results come from CTE LEFT JOINs
        let items: &Vec<_> = items;
        let items_filtered: Vec<_> = if pc_result_aliases.is_empty() {
            items.clone()
        } else {
            items
                .iter()
                .filter(|item| {
                    let alias_str = item
                        .col_alias
                        .as_ref()
                        .map(|a| a.0.clone())
                        .unwrap_or_default();
                    if pc_result_aliases.contains(&alias_str) {
                        log::info!("🔧 Filtering out pattern comp WITH item '{}'", alias_str);
                        false
                    } else {
                        true
                    }
                })
                .cloned()
                .collect()
        };
        let _items = &items_filtered;
        let items = &items_filtered;

        let needs_projection = items.iter().any(|item| {
            !matches!(
                &item.expression,
                crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
            )
        });

        let has_aggregation = items.iter().any(|item| {
            // Skip items that are pattern comprehension correlated subquery
            // placeholders (count(*)) — they will be replaced with scalar
            // subqueries and should not trigger aggregate mode.
            if let Some(ref alias) = item.col_alias {
                if pc_correlated_aliases.contains(&alias.0) {
                    return false;
                }
            }
            expr_contains_aggregate(&item.expression)
        });

        let has_table_alias = items.iter().any(|item| {
            matches!(
                &item.expression,
                crate::query_planner::logical_expr::LogicalExpr::TableAlias(_)
            )
        });

        log::debug!(
            "🐛 DEBUG: needs_projection={}, has_aggregation={}, has_table_alias={}",
            needs_projection,
            has_aggregation,
            has_table_alias
        );

        // Apply projection if we have non-TableAlias items, aggregations, OR TableAlias items
        // TableAlias items need projection to generate CTE columns with simple names
        if needs_projection || has_aggregation || has_table_alias {
            log::debug!("🔧 build_chained_with_match_cte_plan: Applying WITH items projection (needs_projection={}, has_aggregation={}, has_table_alias={})",
                               needs_projection, has_aggregation, has_table_alias);

            // Convert LogicalExpr items to RenderExpr SelectItems
            // CRITICAL: Expand TableAlias to ALL columns (not just ID)
            // When WITH friend appears, it means "all properties of friend"
            //
            // Performance optimization: Wrap non-ID columns with ANY() when aggregating
            // This allows GROUP BY to only include ID column (more efficient)

            // Resolve denormalized property access in RenderExpr.
            // For denormalized schemas (e.g., Airport properties in flights table):
            // 1. Rewrites table alias: a → r (node alias → edge alias)
            // 2. Rewrites property name: uses get_properties_with_table_alias()
            //    to map Cypher property → correct DB column (from_node vs to_node aware)
            //
            // This centralizes what was previously split across:
            // - rewrite_expression_with_property_mapping (property names via schema)
            // - separate alias rewriting (table aliases)
            // Both aspects are now handled here using the plan's
            // get_properties_with_table_alias(), which knows the from/to position.

            // Extract ALL UNWIND aliases from plan — UNWIND aliases are simple
            // ARRAY JOIN column references, not table aliases to expand.
            // Must recurse through wrapping nodes (Filter, Projection, etc.)
            // AND through nested UNWINDs: `UNWIND .. AS x UNWIND .. AS y` binds
            // both x and y, and each must be emitted as a column (not expanded
            // as a graph alias, which would drop it from the CTE projection). (#404)
            //
            // NOTE: this deliberately STOPS at a WithClause barrier (no
            // `WithClause` arm) — `plan_to_render` is already `wc.input`, so any
            // deeper WithClause is a prior segment whose UNWIND vars are now CTE
            // columns and must NOT be re-emitted as bare columns. The similar
            // `find_unwind_aliases` helper below DOES cross the barrier on purpose
            // (for ID-column detection), so the two are not interchangeable.
            let select_items = build_with_projection_select_items(
                items,
                plan_to_render,
                rendered,
                has_aggregation,
                cte_schemas,
                cte_references_for_rendering,
                cte_from_alias,
                plan_ctx,
                body_scope_ref,
                vlp_cte_metadata,
                flattened_compound_keys,
            );

            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Total select_items after expansion: {}",
                select_items.len()
            );

            if !select_items.is_empty() {
                // Check if the logical plan has a denormalized Union.
                // Denormalized Unions already have per-branch SELECT items with
                // correct column resolution (origin_code vs dest_code). We must NOT
                // overwrite them with a flat projection from one branch only.
                // Instead, rename aliases in each branch: "code" → "a_code".
                let is_denorm_union =
                    plan_has_denormalized_union(plan_to_render) && rendered.union.0.is_some();

                if is_denorm_union {
                    // Denormalized Union: the RenderPlan stores first branch in
                    // (select, from, filters) and remaining branches in union.input[].
                    // For CTE content, we need ALL branches in a flat UNION DISTINCT.
                    // Move first branch into union.input and clear plan-level fields.
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Denormalized Union detected, restructuring for WITH '{}'",
                        with_alias
                    );

                    // Use the first exported alias (the node alias) for column renaming,
                    // not the combined with_alias. This ensures columns like "code" become
                    // "a_code" (not "a_allNeighboursCount_code") for unambiguous parsing.
                    let rename_alias = with_plans
                        .first()
                        .and_then(|p| match p {
                            LogicalPlan::WithClause(wc) => wc.exported_aliases.first().cloned(),
                            _ => None,
                        })
                        .unwrap_or_else(|| with_alias.to_string());

                    // Build first branch RenderPlan from the parent plan's fields
                    let mut first_branch = RenderPlan {
                        ctes: CteItems(vec![]),
                        select: rendered.select.clone(),
                        from: rendered.from.clone(),
                        joins: rendered.joins.clone(),
                        array_join: ArrayJoinItem(Vec::new()),
                        filters: rendered.filters.clone(),
                        group_by: GroupByExpressions(vec![]),
                        having_clause: None,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                        fixed_path_info: None,
                        is_multi_label_scan: false,
                        variable_registry: None,
                    };
                    rename_branch_aliases(&mut first_branch.select, &rename_alias);

                    // Rename aliases in remaining branches
                    if let UnionItems(Some(ref mut union)) = rendered.union {
                        for branch in &mut union.input {
                            rename_branch_aliases(&mut branch.select, &rename_alias);
                        }
                        // Insert first branch at the beginning
                        union.input.insert(0, first_branch);
                    }

                    // Clear plan-level fields so CTE renders union directly
                    rendered.select = SelectItems {
                        items: vec![],
                        distinct: false,
                    };
                    rendered.from = FromTableItem(None);
                    rendered.filters = FilterItems(None);
                    rendered.joins = JoinItems(vec![]);
                } else {
                    // Non-denormalized UNION: propagate plan-level filters
                    // to each UNION branch. Filters (inline property filters,
                    // WHERE predicates, NOT EXISTS) live in rendered.filters
                    // but UNION branches render independently during SQL generation,
                    // so each branch needs its own copy.
                    // NOTE: rendered.filters is kept (not cleared) because the SQL
                    // generator uses it for the first UNION branch's WHERE clause.
                    if rendered.filters.0.is_some() {
                        if let Some(ref mut union) = rendered.union.0 {
                            let filter_expr = rendered.filters.0.clone().unwrap();
                            for branch in union.input.iter_mut() {
                                branch.filters = match branch.filters.0.take() {
                                    Some(existing) => FilterItems(Some(
                                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                                            operator: Operator::And,
                                            operands: vec![existing, filter_expr.clone()],
                                        }),
                                    )),
                                    None => FilterItems(Some(filter_expr.clone())),
                                };
                            }
                        }
                    }

                    // Apply projection to SELECT
                    rendered.select = SelectItems {
                        items: select_items,
                        distinct: with_distinct,
                    };

                    // #529 shape 1 (loud guard — NOT a fix; see
                    // `table_has_role_dependent_denorm_identity`'s doc
                    // comment for the full mechanism): the WITH
                    // projection we just attached to `rendered.select`
                    // is about to be shared, byte-for-byte, across
                    // EVERY branch of this multi-branch UNION
                    // (`to_sql_query.rs`'s aggregate-UNION renderer
                    // reuses one shared `plan.select` per branch,
                    // never each branch's own). If this table backs a
                    // node whose identity genuinely depends on which
                    // role (from/to) it plays — a coupled/embedded
                    // schema, e.g. Zeek's `IP` node on `conn_log` —
                    // every branch will silently project the SAME
                    // column instead of alternating roles, dropping
                    // rows that only appear in the unselected role
                    // and inflating counts for rows appearing in
                    // both. Fail loudly here instead.
                    //
                    // Scoping history (R5, post-adversarial-review):
                    // v1 fired only when NO branch had ANY join —
                    // reasoning that a multi-hop pattern's per-branch
                    // JOIN CONDITION (not its SELECT list) encodes
                    // role. Live-verified WRONG: a 2-hop UNDIRECTED
                    // CHAIN grouping by the first hop's own anchor
                    // (`(a)-[r:ACCESSED]-(x)-[r2:ACCESSED]-(c) WITH a,
                    // count(r) AS n ...`) has joins too (for the
                    // second hop), so v1 let it through — silently
                    // dropped 2 of 5 groups and corrupted every
                    // remaining count on both `zeek_merged_test.yaml`
                    // and `flights_denormalized.yaml`.
                    //
                    // v2 tightened to "does the WITH projection
                    // reference the FROM ANCHOR's own alias" —
                    // reasoning that a JOINED alias (reached through a
                    // JOIN condition that varies per branch) is safe,
                    // only the bare anchor (no join to disambiguate
                    // it) is not. This fixed the v1 gap, but a DEEPER
                    // re-verification (prompted by re-checking the
                    // "safe" `denorm_with_aggregate_group_by_middle_
                    // node_no_null_collapse_465_blocking` case that
                    // motivated v1's exemption) found v2's premise
                    // itself false: `MATCH (a:Airport)-[:FLIGHT]-
                    // (b:Airport)-[:FLIGHT]-(c:Airport) WITH b,
                    // count(*) AS n ...` groups by `b` — reached
                    // through JOIN `t2`, never the anchor `t1` — yet
                    // live execution returns `San Francisco=2` for an
                    // airport (`SFO`) that appears in exactly ONE
                    // flight row (graph degree 1), which makes ZERO
                    // valid 2-hop chains through it — an impossible
                    // result, proving corruption. Root cause: the
                    // JOIN condition varies per branch (`t2.origin_code
                    // = t1.dest_code` vs. `t2.dest_code = t1.dest_code`
                    // etc.), but the SELECTed column for `b` is
                    // ALWAYS `t2.origin_code`, even in branches whose
                    // join tied `t2.dest_code` — the exact same
                    // "SELECT never alternates with the join role"
                    // bug, just one join-hop removed from the anchor.
                    // So `denorm_with_aggregate_group_by_middle_node_
                    // no_null_collapse_465_blocking`'s prior "verified
                    // correct" claim was itself wrong (or verified
                    // against different fixture data) — it is NOT a
                    // safe exemption and is now guarded too (moved to
                    // `..._known_broken_529` in this round).
                    //
                    // v3 (current): fire whenever the WITH projection
                    // references a SPECIFIC property that is itself
                    // role-dependent (per `NodeSchema::
                    // role_dependent_property_names` — the Cypher
                    // property names whose physical column genuinely
                    // differs between from/to role, e.g. Zeek IP's
                    // `ip`) on ANY alias in scope — FROM anchor OR any
                    // JOINed alias — not just the anchor. Checking the
                    // PROPERTY, not just "alias's table is
                    // role-dependent", matters: shape 2's already-
                    // fixed `MATCH (a:IP) OPTIONAL MATCH (a)-[r:
                    // ACCESSED]-(b:IP) WITH a, count(r) AS c ...`
                    // resolves `count(r)` to `r.uid` — `r` (the
                    // relationship alias) happens to share its
                    // physical table with the role-dependent `IP`
                    // node, but `uid` itself is the edge's own,
                    // role-INDEPENDENT identity (not a key in
                    // `from_properties`/`to_properties` at all) — a
                    // naive "any reference to a role-dependent
                    // TABLE" check (an earlier draft of this v3) was
                    // a false positive here, caught by this exact
                    // regression test. No known-safe exemption
                    // remains within this codebase today; if one is
                    // found later it needs its own live, degree-based
                    // ground-truth verification (not just "the
                    // numbers look plausible") before being carved
                    // out again.
                    if let Some(ref union) = rendered.union.0 {
                        if !union.input.is_empty() {
                            let mut alias_to_table: std::collections::HashMap<&str, &str> =
                                std::collections::HashMap::new();
                            if let Some(ref from_ref) = rendered.from.0 {
                                if let Some(ref alias) = from_ref.alias {
                                    alias_to_table.insert(alias.as_str(), from_ref.name.as_str());
                                }
                            }
                            for join in &rendered.joins.0 {
                                alias_to_table
                                    .insert(join.table_alias.as_str(), join.table_name.as_str());
                            }

                            let mut accesses: Vec<(&str, &str)> = Vec::new();
                            for item in &rendered.select.items {
                                collect_property_accesses(&item.expression, &mut accesses);
                            }

                            let corrupt_alias_referenced =
                                accesses.iter().any(|(alias, property)| {
                                    alias_to_table.get(alias).is_some_and(|table| {
                                        table_role_dependent_property_names(schema, table)
                                            .contains(*property)
                                    })
                                });
                            if corrupt_alias_referenced {
                                return Err(RenderBuildError::UnsupportedFeature(
                                    "Undirected self-referencing relationship over a \
                                     denormalized/embedded node table is not yet \
                                     supported for WITH-aggregate queries (#529 shape \
                                     1): the per-role UNION branches would project the \
                                     identical embedded identity column instead of \
                                     alternating from/to roles, silently dropping and \
                                     double-counting rows. Rewrite the query with an \
                                     explicit direction (e.g. `-[r]->` instead of \
                                     `-[r]-`) to work around this, or track \
                                     https://github.com/genezhang/clickgraph/issues/529."
                                        .to_string(),
                                ));
                            }
                        }
                    }
                } // end is_denorm_union else

                // FIX: When the input plan was Empty (no MATCH before WITH),
                // the rendered plan has FROM=None and filters=WHERE false.
                // After overwriting SELECT with WITH items (pure constants like map literals),
                // the WHERE false is spurious — clear it so the CTE returns one row.
                if rendered.from.0.is_none()
                    && rendered.joins.0.is_empty()
                    && matches!(
                        &rendered.filters.0,
                        Some(RenderExpr::Literal(super::render_expr::Literal::Boolean(
                            false
                        )))
                    )
                {
                    rendered.filters = FilterItems(None);
                }

                // If there's aggregation, add GROUP BY for non-aggregate expressions
                // PERFORMANCE: Only GROUP BY the ID column(s) for TableAlias items
                // (non-ID columns are wrapped with ANY() above, so they don't need to be grouped)
                //
                // This is efficient because:
                // 1. node_id is the primary key (unique identifier)
                // 2. ANY() picks the single value in each group (safe for PK)
                // 3. GROUP BY 1 column is much faster than GROUP BY 7 columns
                if has_aggregation {
                    // #637: a grouping key can be BURIED inside a WITH item that
                    // also contains an aggregate (e.g. `a.city` in
                    // `WITH a.city + count(b) AS x`). Build the key set as:
                    //  - each aggregate-FREE, non-literal item, WHOLE (preserves
                    //    the prior per-item GROUP BY behavior byte-for-byte,
                    //    including the ID-only / ArraySubscript / property-mapping
                    //    handling below);
                    //  - the buried non-aggregate sub-expressions of each
                    //    aggregate-CONTAINING item, via the shared
                    //    `collect_grouping_keys` (empty for aggregates-only items).
                    // Without the buried extraction the CTE emitted an aggregate
                    // with NO GROUP BY → ClickHouse Code 215.
                    use crate::query_planner::logical_expr::LogicalExpr;
                    let mut key_exprs: Vec<LogicalExpr> = Vec::new();
                    let push_key = |k: LogicalExpr, keys: &mut Vec<LogicalExpr>| {
                        if !keys.contains(&k) {
                            keys.push(k);
                        }
                    };
                    for item in items.iter() {
                        if matches!(&item.expression, LogicalExpr::AggregateFnCall(_)) {
                            continue;
                        }
                        if expr_contains_aggregate(&item.expression) {
                            for key in
                                crate::query_planner::logical_expr::visitors::collect_grouping_keys(
                                    &item.expression,
                                )
                            {
                                push_key(key, &mut key_exprs);
                            }
                        } else if !is_literal_expr(&item.expression) {
                            push_key(item.expression.clone(), &mut key_exprs);
                        }
                    }

                    let group_by_exprs: Vec<RenderExpr> = key_exprs.iter()
                                .flat_map(|key_expr| {
                                    // For TableAlias, only GROUP BY the ID column
                                    // (other columns are wrapped with ANY() in SELECT)
                                    match key_expr {
                                        crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                            // Use ID-only helper for efficient GROUP BY
                                            // Pass VLP CTE metadata for deterministic lookups
                                            expand_table_alias_to_group_by_id_only(
                                                &alias.0,
                                                plan_to_render,
                                                schema,
                                                cte_schemas,
                                                cte_references_for_rendering,
                                                Some(vlp_cte_metadata),
                                            )
                                        }
                                        crate::query_planner::logical_expr::LogicalExpr::ArraySubscript { array, .. } => {
                                            // For array subscripts (e.g., labels(x)[1]), only GROUP BY the array part
                                            // ClickHouse can't GROUP BY an array element, only the array itself
                                            let expr_vec: Vec<RenderExpr> = (**array).clone().try_into().ok().into_iter().collect();
                                            expr_vec
                                        }
                                        _ => {
                                            // Apply property mapping rewriting before converting to RenderExpr.
                                            // This ensures CTE-scoped columns resolve correctly (e.g., message.length → p7_message_length).
                                            use crate::query_planner::logical_expr::expression_rewriter::{
                                                ExpressionRewriteContext, rewrite_expression_with_property_mapping,
                                            };
                                            let rewrite_ctx = if let Some(s) = body_scope_ref {
                                                ExpressionRewriteContext::with_scope(plan_to_render, s)
                                            } else {
                                                ExpressionRewriteContext::new(plan_to_render)
                                            };
                                            let rewritten = rewrite_expression_with_property_mapping(key_expr, &rewrite_ctx);
                                            let expr_vec: Vec<RenderExpr> = rewritten.try_into().ok().map(|mut expr: RenderExpr| {
                                                resolve_denormalized_property_in_expr_impl(&mut expr, plan_to_render, cte_from_alias.as_deref());
                                                expr
                                            }).into_iter().collect();
                                            expr_vec
                                        }
                                    }
                                })
                                .collect();
                    rendered.group_by = GroupByExpressions(group_by_exprs);
                }
            }
        }
    }
    Ok(())
}

/// Re-attach UNWIND array joins to a nested-WITH-rendered CTE body (a STEP of the
/// main loop's inner render-loop in `build_chained_with_match_cte_plan`, Phase-4
/// §7.1 extraction).
///
/// When a WITH segment contains an UNWIND (e.g. `UNWIND [1,2,3] AS x WITH x ...`),
/// the CTE-body render path can drop the array expansion, leaving the unwound
/// variable undefined. Re-extract the array joins from the segment and attach them
/// so the CTE keeps its ARRAY JOIN / LATERAL VIEW. No-op when the render already
/// carries array joins. (issue #401)
fn reattach_unwind_array_joins(
    rendered: &mut RenderPlan,
    plan_to_render: &LogicalPlan,
) -> RenderPlanBuilderResult<()> {
    if rendered.array_join.0.is_empty() {
        let array_joins =
            <LogicalPlan as super::join_builder::JoinBuilder>::extract_array_join(plan_to_render)?;
        if !array_joins.is_empty() {
            rendered.array_join = ArrayJoinItem(array_joins);
        }
    }
    Ok(())
}

/// Compute the pattern-comprehension result aliases to skip in WITH-item
/// projection (a STEP of the main loop's inner render-loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// Returns `(pc_result_aliases, pc_correlated_aliases)`. For the LEGACY CTE+JOIN
/// path (no `pattern_hops`), PC result aliases go in the first set so their WITH
/// items are skipped (results arrive via CTE LEFT JOINs). For the correlated
/// subquery path (`pattern_hops` populated), they go in the second set so the
/// items are kept and `count(*)` can be replaced inline. Pure function of the
/// segment's first WITH plan.
fn compute_pc_skip_aliases(
    with_plans: &[LogicalPlan],
) -> (
    std::collections::HashSet<String>,
    std::collections::HashSet<String>,
) {
    let (pc_result_aliases, pc_correlated_aliases): (
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    ) = with_plans
        .first()
        .and_then(|plan| match plan {
            LogicalPlan::WithClause(wc) if !wc.pattern_comprehensions.is_empty() => {
                // If any PC has pattern_hops, use correlated subquery path → don't skip
                let has_pattern_hops = wc
                    .pattern_comprehensions
                    .iter()
                    .any(|pc| !pc.pattern_hops.is_empty());
                if has_pattern_hops {
                    // Correlated subquery path: collect aliases that contain count(*)
                    // placeholders — these will be replaced with scalar subqueries,
                    // so they should NOT trigger has_aggregation.
                    let correlated: std::collections::HashSet<String> = wc
                        .pattern_comprehensions
                        .iter()
                        .map(|pc| pc.result_alias.clone())
                        .collect();
                    Some((std::collections::HashSet::new(), correlated))
                } else {
                    let legacy: std::collections::HashSet<String> = wc
                        .pattern_comprehensions
                        .iter()
                        .map(|pc| pc.result_alias.clone())
                        .collect();
                    Some((legacy, std::collections::HashSet::new()))
                }
            }
            _ => None,
        })
        .unwrap_or_default();
    (pc_result_aliases, pc_correlated_aliases)
}

/// Register schemas for VLP CTEs used by this render segment (a STEP of the main
/// loop's inner render-loop in `build_chained_with_match_cte_plan`, Phase-4 §7.1
/// extraction).
///
/// VLP CTEs are `RawSql`, so their schema cannot be read off the CTE body. Two
/// registration paths cover them: (1) when the segment ends in a UNION over VLP
/// results, extract column/ID/property mappings from the UNION's first SELECT and
/// register them under the pseudo-CTE `__union_vlp`; (2) when the segment's FROM
/// is a `vlp_`-prefixed CTE, register each correlation alias → VLP CTE mapping
/// and populate `cte_schemas` from `vlp_cte_metadata` so GROUP-BY property
/// mapping resolves the VLP columns. Reads `rendered`; mutates `cte_schemas` and
/// `cte_references_for_rendering`.
fn register_vlp_cte_schemas(
    rendered: &RenderPlan,
    cte_schemas: &mut crate::render_plan::CteSchemas,
    cte_references_for_rendering: &mut HashMap<String, String>,
    vlp_cte_metadata: &HashMap<String, (String, Vec<super::CteColumnMetadata>)>,
) {
    // CRITICAL: Extract schema from UNION (for VLP CTEs)
    // VLP CTEs are RawSql so we can't extract schema from them directly
    // But the UNION that uses them has SELECT items with aliases like "friend.id", "p.firstName"
    if let UnionItems(Some(union)) = &rendered.union {
        if !union.input.is_empty() {
            let union_select_items = &union.input[0].select.items;
            let union_property_names: Vec<String> = union_select_items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                .collect();

            // Extract ID column mappings from UNION columns
            // Store FULL column name (e.g., "friend.id"), not just "id"
            let mut union_alias_to_id: HashMap<String, String> = HashMap::new();
            for item in union_select_items {
                if let Some(col_alias) = &item.col_alias {
                    let alias_str = col_alias.0.as_str();
                    if let Some(dot_pos) = alias_str.rfind('.') {
                        let (prefix, suffix) = alias_str.split_at(dot_pos);
                        if suffix == ".id" {
                            // Store FULL column name
                            union_alias_to_id.insert(prefix.to_string(), alias_str.to_string());
                            log::debug!(
                                "📊 UNION: Found ID column for alias '{}' -> '{}'",
                                prefix,
                                alias_str
                            );
                        }
                    }
                }
            }

            // Build explicit property mapping for UNION (VLP results)
            let union_property_mapping = build_property_mapping_from_columns(union_select_items);

            // Register the UNION schema as a pseudo-CTE for alias lookups
            // This allows WITH clauses to reference VLP results
            let union_cte_name = "__union_vlp";
            log::info!(
                        "🔧 Extracted UNION schema (VLP results): {} columns, {} aliases with ID: {:?}, {} property mappings",
                        union_property_names.len(), union_alias_to_id.len(), union_alias_to_id.keys(), union_property_mapping.len()
                    );
            cte_schemas.insert(
                union_cte_name.to_string(),
                crate::render_plan::CteSchemaMetadata {
                    select_items: union_select_items.clone(),
                    column_names: union_property_names,
                    alias_to_id: union_alias_to_id.clone(),
                    property_mapping: union_property_mapping,
                },
            );

            // Also register for each alias that appears in the UNION
            // This allows direct alias lookups
            for alias in union_alias_to_id.keys() {
                cte_references_for_rendering.insert(alias.clone(), union_cte_name.to_string());
                log::info!(
                    "🔧 Registered alias '{}' -> CTE '{}'",
                    alias,
                    union_cte_name
                );
            }
        }
    }

    // 🔧 FIX (Feb 9, 2026): Pattern comprehension GROUP BY property bug
    // When FROM clause is a VLP CTE (e.g., FROM vlp_multi_type_a_t31 AS t),
    // and WITH items reference correlation variables (e.g., WITH a),
    // register the correlation variable → VLP CTE mapping so that
    // expand_table_alias_to_select_items generates t.start_* columns
    // instead of trying to reference the non-existent alias 'a'
    log::debug!(
        "DEBUG 0: About to check FROM clause, has from_ref: {}",
        rendered.from.0.is_some()
    );
    log::debug!("DEBUG: Checking FROM clause for VLP CTE");
    if let Some(from_ref) = &rendered.from.0 {
        let from_name = &from_ref.name;
        log::debug!("DEBUG: FROM name = '{}'", from_name);
        // Check if FROM is a VLP CTE (starts with "vlp_")
        if from_name.starts_with("vlp_") {
            log::debug!("DEBUG: FROM is VLP CTE!");
            log::info!(
                "🔧 FROM is VLP CTE '{}', checking for correlation variables in WITH items",
                from_name
            );

            // Extract correlation variables from VLP CTE name
            // Actual format: vlp_multi_type_a_b (multiple aliases, no _t suffix)
            // or: vlp_a (single alias)
            log::debug!(
                "STEP 0.1: Extracting correlation variables from '{}'",
                from_name
            );
            let aliases = if from_name.starts_with("vlp_multi_type_") {
                // Multi-type VLP: vlp_multi_type_a_b -> ["a", "b"]
                from_name
                    .strip_prefix("vlp_multi_type_")
                    .map(|s| s.split('_').map(|a| a.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            } else {
                // Single-type VLP: vlp_a -> ["a"]
                from_name
                    .strip_prefix("vlp_")
                    .map(|s| vec![s.to_string()])
                    .unwrap_or_default()
            };
            log::debug!("STEP 0.2: Extracted aliases: {:?}", aliases);

            // Register each alias → VLP CTE mapping
            for corr_var in &aliases {
                log::info!("🔧 VLP CTE correlation variable: '{}'", corr_var);
                // Register: correlation_var → VLP CTE name
                // This tells expand_table_alias_to_select_items to use VLP CTE columns
                cte_references_for_rendering.insert(corr_var.to_string(), from_name.clone());
                log::info!(
                    "🔧 Registered VLP correlation: '{}' → '{}'",
                    corr_var,
                    from_name
                );
            }

            // Populate cte_schemas from VLP CTE metadata (CRITICAL for GROUP BY property mapping)
            // This ensures expand_table_alias_to_select_items finds the CTE schema
            if !aliases.is_empty() {
                if let Some((_cypher_alias, col_metadata)) = vlp_cte_metadata.get(from_name) {
                    log::debug!(
                        "STEP 1: VLP CTE '{}' found, {} columns",
                        from_name,
                        col_metadata.len()
                    );

                    // Convert CteColumnMetadata to SelectItem format
                    let mut select_items = Vec::new();
                    let mut property_names = Vec::new();
                    let mut alias_to_id_column: HashMap<String, String> = HashMap::new();

                    log::debug!("STEP 2: Starting column iteration");
                    for (idx, col_meta) in col_metadata.iter().enumerate() {
                        log::debug!(
                            "STEP 2.{}: Processing column '{}'",
                            idx,
                            col_meta.cte_column_name
                        );

                        // The CTE already exists with these columns - we just need to track them
                        // The expression here is not used for rendering, only for metadata
                        let col_expr = crate::render_plan::render_expr::RenderExpr::Raw(
                            col_meta.cte_column_name.clone(),
                        );
                        log::debug!("STEP 2.{}.1: Created RenderExpr", idx);

                        let select_item = SelectItem {
                            expression: col_expr,
                            col_alias: Some(crate::render_plan::render_expr::ColumnAlias(
                                col_meta.cte_column_name.clone(),
                            )),
                        };
                        log::debug!("STEP 2.{}.2: Created SelectItem", idx);

                        select_items.push(select_item);
                        property_names.push(col_meta.cte_column_name.clone());
                        log::debug!("STEP 2.{}.3: Pushed to vectors", idx);

                        // Track ID columns for GROUP BY
                        if col_meta.is_id_column {
                            alias_to_id_column.insert(
                                col_meta.cypher_alias.clone(),
                                col_meta.cte_column_name.clone(),
                            );
                            log::debug!("STEP 2.{}.4: Tracked ID column", idx);
                        }
                    }

                    log::debug!("STEP 3: Column iteration complete, building property mapping");
                    // Build property mapping using existing function
                    let property_mapping = build_property_mapping_from_columns(&select_items);
                    log::debug!(
                        "STEP 4: Property mapping built with {} entries",
                        property_mapping.len()
                    );

                    log::debug!("STEP 5: Inserting into cte_schemas");
                    // Insert into cte_schemas so expand_table_alias_to_select_items can find it
                    cte_schemas.insert(
                        from_name.clone(),
                        crate::render_plan::CteSchemaMetadata {
                            select_items,
                            column_names: property_names,
                            alias_to_id: alias_to_id_column,
                            property_mapping,
                        },
                    );
                    log::debug!("STEP 6: SUCCESS - Schema populated for '{}'", from_name);
                } else {
                    log::debug!("⚠️ VLP CTE '{}' not found in vlp_cte_metadata", from_name);
                }
            }
        }
    }
}

/// Extract the schemas of CTEs produced by a nested WITH render and register
/// them (a STEP of the main loop's inner render-loop in
/// `build_chained_with_match_cte_plan`, Phase-4 §7.1 extraction).
///
/// When a WITH segment's body itself contains nested WITHs, the recursive render
/// builds CTEs whose schemas the outer build needs. For each such CTE in
/// `rendered.ctes`, record its `CteSchemaMetadata` in `cte_schemas`, mark its
/// name used in `cte_name_allocator`, map its exported aliases into
/// `cte_references`, and hoist it into `all_ctes`. RawSql (VLP) CTEs are skipped
/// here — their schema is inferred from the UNION that uses them. No-op when the
/// rendered plan carries no CTEs.
fn extract_nested_cte_schemas(
    rendered: &mut RenderPlan,
    all_ctes: &mut Vec<Cte>,
    cte_schemas: &mut crate::render_plan::CteSchemas,
    cte_references: &mut HashMap<String, String>,
    cte_name_allocator: &mut CteNameAllocator,
    vlp_cte_metadata: &mut HashMap<String, (String, Vec<super::CteColumnMetadata>)>,
    processed_cte_aliases: &mut std::collections::HashSet<String>,
) {
    if !rendered.ctes.0.is_empty() {
        for cte in &rendered.ctes.0 {
            let select_items = match &cte.content {
                super::CteContent::Structured(plan) => match &plan.union {
                    UnionItems(Some(union)) if !union.input.is_empty() => {
                        union.input[0].select.items.clone()
                    }
                    _ => plan.select.items.clone(),
                },
                super::CteContent::RawSql(_) => {
                    // VLP CTEs are RawSql - can't extract schema directly
                    // But we can infer from the UNION that uses them
                    // Skip for now, will be handled when we see the UNION
                    log::debug!("🔧 Skipping RawSql CTE '{}' (VLP CTE - schema will be inferred from UNION)", cte.cte_name);
                    continue;
                }
            };
            let property_names: Vec<String> = select_items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                .collect();

            // For nested CTEs, we don't have direct access to the plan to compute ID columns
            // deterministically. These are typically VLP CTEs with dotted notation (friend.id)
            // which we can safely extract since they follow a fixed pattern from VLP generation.
            let mut alias_to_id_column: HashMap<String, String> = HashMap::new();
            for item in &select_items {
                if let Some(col_alias) = &item.col_alias {
                    let alias_str = col_alias.0.as_str();
                    // VLP CTEs use "alias.id" pattern which is unambiguous
                    if let Some(dot_pos) = alias_str.rfind('.') {
                        let (prefix, suffix) = alias_str.split_at(dot_pos);
                        if suffix == ".id" {
                            alias_to_id_column.insert(prefix.to_string(), alias_str.to_string());
                            log::debug!(
                                "📊 CTE '{}': Found ID column for alias '{}' -> '{}'",
                                cte.cte_name,
                                prefix,
                                alias_str
                            );
                        }
                    }
                    // Note: We do NOT try to parse underscore patterns here as they are unreliable
                    // The caller (build_chained_with_match_cte_plan) will compute these deterministically
                }
            }

            // Build explicit property mapping
            let property_mapping = build_property_mapping_from_columns(&select_items);

            log::info!(
                    "🔧 build_chained_with_match_cte_plan: Extracted nested CTE schema '{}': {} columns, {} aliases with ID, {} property mappings",
                    cte.cte_name, property_names.len(), alias_to_id_column.len(), property_mapping.len()
                );

            cte_schemas.insert(
                cte.cte_name.clone(),
                crate::render_plan::CteSchemaMetadata {
                    select_items,
                    column_names: property_names,
                    alias_to_id: alias_to_id_column,
                    property_mapping,
                },
            );
        }

        // CRITICAL FIX (Jan 2026): Hoist CTEs from recursive call to prevent duplicates
        // The recursive call created CTEs - we need to:
        // 1. Add them to our all_ctes (so they appear in final SQL)
        // 2. Track their names in used_cte_names (so we don't create duplicates)
        // 3. Track their aliases in processed_cte_aliases (so we don't re-process them)
        // 4. Capture VLP column metadata for deterministic lookups (Phase 3 CTE integration)
        for cte in &rendered.ctes.0 {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Hoisting CTE '{}' from recursive call",
                cte.cte_name
            );
            cte_name_allocator.mark_used(cte.cte_name.clone());

            // Capture VLP CTE metadata for deterministic column lookups
            // This replaces heuristic lookups in expand_table_alias_to_group_by_id_only
            if !cte.columns.is_empty() && cte.from_alias.is_some() {
                let from_alias = cte.from_alias.clone().unwrap();
                log::info!(
                    "🔧 Capturing VLP CTE metadata: '{}' with {} columns, from_alias='{}'",
                    cte.cte_name,
                    cte.columns.len(),
                    from_alias
                );
                vlp_cte_metadata.insert(cte.cte_name.clone(), (from_alias, cte.columns.clone()));
            }

            // Extract aliases from the CTE's stored exported_aliases (preferred)
            // or from CTE name (fallback, may fail for aliases with underscores)
            let aliases = if !cte.with_exported_aliases.is_empty() {
                cte.with_exported_aliases.clone()
            } else {
                crate::utils::cte_naming::extract_aliases_from_cte_name(&cte.cte_name)
                    .unwrap_or_default()
            };
            for alias in aliases {
                if !alias.is_empty() {
                    processed_cte_aliases.insert(alias.clone());
                    cte_references.insert(alias, cte.cte_name.clone());
                }
            }
        }
        // Now hoist the actual CTEs
        hoist_nested_ctes(rendered, all_ctes);
    }
}

pub(crate) fn build_chained_with_match_cte_plan(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&PlanCtx>,
    scope: Option<&super::variable_scope::VariableScope>,
) -> RenderPlanBuilderResult<RenderPlan> {
    use super::CteContent;

    // See `CteScopeGenerationGuard`/`enter_cte_scope_generation` doc: this
    // scopes `cte_scope_for_correlation` entries (published below, per WITH
    // alias) to THIS invocation (and its own nested recursive calls), so an
    // independent later subplan reusing the same Cypher alias name (e.g. a
    // sibling UNION arm or cartesian-product side with a fresh, non-CTE
    // `MATCH (a) WHERE EXISTS {...}`) can never resolve through a stale entry
    // left behind by this one.
    let _cte_scope_generation_guard =
        crate::server::query_context::CteScopeGenerationGuard::enter();

    log::debug!(
        "build_chained_with_match_cte_plan ENTRY: plan_ctx available: {}",
        plan_ctx.is_some()
    );
    // Safety limit to prevent infinite loops due to excessive plan tree depth
    // Complex queries with many nested structures (projections, filters, WITH clauses, etc.)
    // can create deep plan trees that require many iterations to process
    const MAX_PLAN_DEPTH: usize = 500;

    let mut current_plan = plan.clone();
    let mut all_ctes: Vec<Cte> = Vec::new();
    let mut iteration = 0;

    // Collect compound key mappings from flattened map literals.
    // Written during CTE SELECT item generation, read during property_mapping construction.
    let flattened_compound_keys: std::cell::RefCell<Vec<(String, String)>> =
        std::cell::RefCell::new(Vec::new());

    // Track CTE schemas: map CTE name to:
    // 1. Vec<SelectItem>: Column definitions
    // 2. Vec<String>: Property names
    // 3. HashMap<String, String>: alias → ID column name
    // 4. HashMap<(String, String), String>: (alias, property) → CTE column name (EXPLICIT MAPPING)
    let mut cte_schemas: crate::render_plan::CteSchemas = std::collections::HashMap::new();

    // Track VLP CTEs with column metadata for deterministic lookups
    // Maps CTE name → (Cypher alias → column metadata)
    // This replaces heuristic lookups in expand_table_alias_to_group_by_id_only
    let mut vlp_cte_metadata: std::collections::HashMap<
        String,
        (String, Vec<super::CteColumnMetadata>), // (from_alias, columns)
    > = std::collections::HashMap::new();

    // Track aliases that have been converted to CTEs across ALL iterations
    // This prevents re-processing the same alias in subsequent iterations
    // (important for chained WITH like `WITH DISTINCT fof WITH fof`)
    let mut processed_cte_aliases: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // Track the CTE-naming / dedup cluster (sequence numbers, used names, and the
    // analyzer→actual name remapping) as a single cohesive unit. See
    // `CteNameAllocator` for the invariants each map upholds.
    let mut cte_name_allocator = CteNameAllocator::new();

    // Track CTE references as we build them (alias → CTE name)
    // Start EMPTY and populate as each CTE is created
    // This ensures we only reference CTEs that have actually been built in previous iterations
    let mut cte_references: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // CRITICAL: Extract correlation predicates from the ORIGINAL plan BEFORE any transformations!
    // These predicates (e.g., a.user_id = c.user_id from WHERE clause in cross-table WITH patterns)
    // are stored in CartesianProduct.join_condition and will be lost after the plan is transformed.
    // We need them later to create proper JOIN ON conditions for CTE joins.
    let mut original_correlation_predicates = extract_correlation_predicates(&current_plan);
    log::debug!(
        "🔧 build_chained_with_match_cte_plan: Extracted {} correlation predicates from ORIGINAL plan",
        original_correlation_predicates.len()
    );
    for (i, pred) in original_correlation_predicates.iter().enumerate() {
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Original correlation predicate[{}]: {:?}",
            i,
            pred
        );
    }

    log::debug!("🔧 build_chained_with_match_cte_plan: Starting iterative WITH processing");

    // Accumulate CTE variable info for scope-aware resolution and the unified
    // variable registry as one cohesive unit. As each WITH is processed, we record
    // the alias → CTE property mapping (used to build a VariableScope for rendering
    // subsequent CTE bodies and the final plan) and define/overwrite variables in
    // the registry (attached to CTEs and the final RenderPlan for the SQL renderer).
    let mut with_scope = WithBarrierScope::new();

    // Process WITH clauses iteratively until none remain
    while has_with_clause_in_graph_rel(&current_plan) {
        iteration += 1;
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: ========== ITERATION {} ==========",
            iteration
        );

        let plan_depth = count_plan_depth(&current_plan);
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Plan tree depth = {} (iteration {})",
            plan_depth,
            iteration
        );

        if iteration > MAX_PLAN_DEPTH {
            return Err(RenderBuildError::InvalidRenderPlan(format!(
                "Query plan too deeply nested (depth > {}). This usually indicates a bug in query planning.",
                MAX_PLAN_DEPTH
            )));
        }

        log::debug!(
            "🔧 build_chained_with_match_cte_plan: Iteration {} - processing WITH clause",
            iteration
        );

        // Find ALL WITH clauses grouped by alias
        // This handles Union branches that each have their own WITH clause with the same alias
        // Note: We collect the data without holding references across the mutation
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: About to call find_all_with_clauses_grouped"
        );
        let grouped_withs = find_all_with_clauses_grouped(&current_plan);

        log::debug!("🔧 build_chained_with_match_cte_plan: Found {} alias groups from find_all_with_clauses_grouped", grouped_withs.len());
        for (alias, plans) in &grouped_withs {
            log::debug!(
                "🔧 build_chained_with_match_cte_plan:   Alias '{}': {} plan(s)",
                alias,
                plans.len()
            );
            for (i, plan) in plans.iter().enumerate() {
                if let LogicalPlan::WithClause(wc) = plan {
                    log::debug!(
                        "🔧     Plan {}: WithClause with exported_aliases={:?}, items.len()={}",
                        i,
                        wc.exported_aliases,
                        wc.items.len()
                    );
                    let has_nested = plan_contains_with_clause(&wc.input);
                    log::debug!("🔧     Plan {}: has_nested_with_clause={}", i, has_nested);
                }
            }
        }

        if grouped_withs.is_empty() {
            log::debug!("🔧 build_chained_with_match_cte_plan: has_with_clause_in_graph_rel returned true but no WITH clauses found");
            break;
        }

        // Build this iteration's work-list: record analyzer CTE names, keep only
        // the innermost WITH per alias, and sort aliases innermost-first.
        let (all_analyzer_cte_names, filtered_grouped_withs, aliases_to_process) =
            build_iteration_worklist(&current_plan, grouped_withs);

        // Track if any alias was actually processed in this iteration
        let mut any_processed_this_iteration = false;

        // Process each alias group
        // For aliases with multiple WITH clauses (from Union branches), combine them with UNION ALL
        'alias_loop: for (with_alias, plan_count) in aliases_to_process {
            log::info!(
                "🔧 build_chained_with_match_cte_plan: Processing {} WITH clause(s) for alias '{}'",
                plan_count,
                with_alias
            );

            // CRITICAL: Create a snapshot of cte_references that only includes CTEs from PREVIOUS iterations
            // Do NOT include the CTE we're about to build for this alias!
            // This prevents resolve_cte_reference from using future CTEs that don't exist yet
            let mut cte_references_for_rendering = cte_references.clone();
            log::info!(
                "🔧 build_chained_with_match_cte_plan: cte_references for rendering '{}': {:?}",
                with_alias,
                cte_references_for_rendering
            );

            // Get the WITH plans from our filtered map
            let with_plans = match filtered_grouped_withs.get(&with_alias) {
                Some(plans) => {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: Found {} plan(s) for alias '{}' in filtered map",
                        plans.len(),
                        with_alias
                    );
                    plans.clone() // Clone the Vec<LogicalPlan> to avoid moving from borrowed data
                }
                None => {
                    log::debug!("🔧 build_chained_with_match_cte_plan: Alias '{}' not in filtered map (all WITH clauses had nested WITH), skipping", with_alias);
                    continue;
                }
            };

            // CRITICAL: Update cte_references for ALL plans BEFORE rendering them
            // GraphRel nodes inside these plans need to know about available CTEs
            // Use the snapshot from PREVIOUS iterations only (not including current alias)
            log::debug!("🔧 build_chained_with_match_cte_plan: Updating cte_references for {} plans before rendering. Using previous CTEs: {:?}", with_plans.len(), cte_references_for_rendering);

            let (with_plans, pre_with_aliases) = prepare_with_plans_and_pre_aliases(
                with_plans,
                &cte_references_for_rendering,
                &with_alias,
                &processed_cte_aliases,
            )?;

            // Render each WITH clause plan
            let mut rendered_plans: Vec<RenderPlan> = Vec::new();
            let mut inner_plans_for_id: Vec<LogicalPlan> = Vec::new();
            let mut has_optional_match_input = false;
            for with_plan in with_plans.iter() {
                log::debug!("🔧 build_chained_with_match_cte_plan: Rendering WITH plan for '{}' - plan type: {:?}",
                           with_alias, std::mem::discriminant(with_plan));

                // Collapse a simple passthrough WITH onto its existing CTE, if this
                // is one. On collapse, current_plan changed → break 'alias_loop to
                // restart iteration. See `try_collapse_passthrough_with`.
                if try_collapse_passthrough_with(
                    with_plan,
                    &with_alias,
                    &mut current_plan,
                    &mut cte_references,
                    &mut cte_name_allocator,
                    &mut any_processed_this_iteration,
                )? {
                    break 'alias_loop;
                }

                // Extract the plan to render, WITH items, and modifiers (ORDER BY, SKIP, LIMIT, WHERE)
                // CRITICAL: Also extract CTE references from this WITH's input - these tell us which
                // variables come from previous CTEs in the chain
                let (
                    plan_to_render,
                    with_items,
                    with_distinct,
                    with_order_by,
                    with_skip,
                    with_limit,
                    with_where_clause,
                    _with_cte_refs,
                ) = extract_with_plan_parts(with_plan, &with_alias);

                // Save plan_to_render for ID column computation (used after loop)
                inner_plans_for_id.push(plan_to_render.clone());

                // Track whether this WITH clause's input contains an OPTIONAL MATCH.
                // This is used later for deterministic CTE body restructuring.
                if plan_to_render.is_optional_pattern() {
                    has_optional_match_input = true;
                }

                // Render the plan (even if it contains nested WITHs)
                // Instead of calling to_render_plan recursively (which causes infinite loops),
                // process the plan directly using the same logic as the main function
                //
                // Build a scope from accumulated CTE variables for this rendering pass.
                // This ensures CTE body rendering resolves variables from prior WITHs correctly.
                let body_scope = with_scope.build_final_scope(schema, plan_to_render);
                let body_scope_ref = if with_scope.is_empty() && scope.is_none() {
                    None // No scope needed for first WITH (or when called without outer scope)
                } else {
                    Some(&body_scope)
                };
                // Render the plan body (recursing on nested WITHs, else direct).
                // See `render_with_cte_body`.
                let mut rendered =
                    render_with_cte_body(plan_to_render, schema, plan_ctx, body_scope_ref)?;

                // Re-attach UNWIND array joins the CTE-body render may have dropped.
                // See `reattach_unwind_array_joins` (issue #401).
                reattach_unwind_array_joins(&mut rendered, plan_to_render)?;

                // Extract + register the schemas of any CTEs produced by a nested
                // WITH render. See `extract_nested_cte_schemas`.
                extract_nested_cte_schemas(
                    &mut rendered,
                    &mut all_ctes,
                    &mut cte_schemas,
                    &mut cte_references,
                    &mut cte_name_allocator,
                    &mut vlp_cte_metadata,
                    &mut processed_cte_aliases,
                );

                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Rendered SQL FROM: {:?}",
                    rendered.from
                );
                log::info!(
                    "🔧 build_chained_with_match_cte_plan: Rendered SQL JOINs: {} join(s)",
                    rendered.joins.0.len()
                );
                for (i, join) in rendered.joins.0.iter().enumerate() {
                    log::info!(
                        "🔧 build_chained_with_match_cte_plan: JOIN {}: {:?}",
                        i,
                        join
                    );
                }

                // #584: the alias the CTE body's FROM binds. Gates the coupled
                // rel-var remap in resolve_denormalized_property_in_expr: `r.<col>`
                // → node alias `o` is only valid when FROM actually binds `o`
                // (node carried past the WITH barrier). When FROM binds the rel
                // var itself (only `r` carried), the remap must not fire.
                let cte_from_alias: Option<String> =
                    rendered.from.0.as_ref().and_then(|ft| ft.alias.clone());

                // Register VLP CTE schemas (UNION-derived + FROM-vlp correlation).
                register_vlp_cte_schemas(
                    &rendered,
                    &mut cte_schemas,
                    &mut cte_references_for_rendering,
                    &vlp_cte_metadata,
                );

                // Extract pattern comprehension aliases to skip in WITH item projection
                // (their results come from CTE LEFT JOINs, not from regular WITH item processing)
                // NOTE: Only skip items for LEGACY CTE+JOIN path. For the new correlated subquery
                // path (pattern_hops populated), keep items so count(*) can be replaced inline.
                let (pc_result_aliases, pc_correlated_aliases) =
                    compute_pc_skip_aliases(&with_plans);

                // Apply WITH items projection if present (handles `WITH x.p AS n`,
                // `WITH count(x) AS c`, bare `WITH a`, and the denormalized-UNION
                // restructuring path). See `apply_with_items_projection`.
                apply_with_items_projection(
                    &mut rendered,
                    plan_to_render,
                    &with_items,
                    with_distinct,
                    &pc_result_aliases,
                    &pc_correlated_aliases,
                    &with_plans,
                    &with_alias,
                    &cte_from_alias,
                    schema,
                    plan_ctx,
                    body_scope_ref,
                    &cte_schemas,
                    &cte_references_for_rendering,
                    &vlp_cte_metadata,
                    &flattened_compound_keys,
                )?;

                // Apply the WITH segment's ORDER BY / SKIP / LIMIT and WHERE→HAVING.
                // See `apply_with_order_by_skip_limit_where`.
                apply_with_order_by_skip_limit_where(
                    &mut rendered,
                    plan_to_render,
                    body_scope_ref,
                    with_order_by,
                    with_skip,
                    with_limit,
                    with_where_clause,
                    &with_items,
                    &cte_from_alias,
                )?;

                // Rewrite join conditions that reference CTE aliases to use CTE column names,
                // then prune orphaned JOINs. See `rewrite_cte_join_conditions_and_prune_orphans`.
                rewrite_cte_join_conditions_and_prune_orphans(
                    &mut rendered,
                    &cte_references,
                    &cte_schemas,
                );

                // Rewrite orphaned composite alias references + apply the augmented
                // CTE scope. See `fix_composite_alias_refs_and_augment_scope`.
                fix_composite_alias_refs_and_augment_scope(
                    &mut rendered,
                    &all_ctes,
                    &cte_references,
                    &current_plan,
                    &with_scope,
                    schema,
                    body_scope_ref,
                );
                rendered_plans.push(rendered);
            }

            if rendered_plans.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(format!(
                    "Could not render any WITH clause for alias '{}'",
                    with_alias
                )));
            }

            // Derive the CTE name (and the exported-alias / pattern-comprehension
            // metadata) for this WITH alias group. See `derive_with_cte_name`.
            let (exported_aliases, pattern_comprehensions, cte_name) = derive_with_cte_name(
                &with_plans,
                &with_alias,
                &all_ctes,
                &all_analyzer_cte_names,
                &mut cte_name_allocator,
            );

            // Create CTE content - if multiple renders, combine with UNION ALL.
            // See `combine_with_renders_into_cte`.
            let mut with_cte_render =
                combine_with_renders_into_cte(rendered_plans, &with_alias, &cte_name);

            // Extract nested CTEs from the rendered plan (e.g., VLP recursive CTEs)
            // These need to be hoisted to the top level before the WITH CTE
            hoist_nested_ctes(&mut with_cte_render, &mut all_ctes);

            // Pattern Comprehension: correlated subquery or CTE+LEFT JOIN.
            // See `apply_pattern_comprehensions`.
            apply_pattern_comprehensions(
                &mut with_cte_render,
                &mut all_ctes,
                &with_plans,
                &pattern_comprehensions,
                &cte_name,
                &with_alias,
                &exported_aliases,
                schema,
                plan_ctx,
                &cte_schemas,
            );

            // NOTE: Previously had intermediate_reverse_mapping block here (~180 lines)
            // that built reverse mapping from CTE columns and rewrote CTE body expressions.
            // Removed in Phase 3: scope-based resolution in CTE body rendering
            // (via VariableScope passed to to_render_plan_with_ctx) now handles this.

            // Extract SELECT items to build column metadata BEFORE creating the CTE.
            // This allows the Cte to store column information for later CTE registry
            // population. See `build_cte_column_metadata`.
            let (select_items_for_schema, property_names_for_schema, cte_columns) =
                build_cte_column_metadata(&with_cte_render, &with_alias, &cte_name);

            // Extract original WITH exported aliases + the renamed→original alias
            // map for cte_references / property_mapping lookups.
            // See `build_alias_rename_map`.
            let (original_exported_aliases, alias_rename_map) =
                build_alias_rename_map(&with_plans, &with_alias);

            // Post-WITH OPTIONAL MATCH CTE body restructuring (CTE must drive the
            // LEFT JOIN chain). See `restructure_post_with_optional_match`.
            restructure_post_with_optional_match(&mut with_cte_render, has_optional_match_input);

            // Create the CTE with column metadata.
            // If the UNION + correlated subquery split happened, use RawSql content.
            let pc_union_sql = with_cte_render
                .ctes
                .0
                .iter()
                .find(|c| c.cte_name == "__pc_union_sql__")
                .and_then(|c| match &c.content {
                    CteContent::RawSql(sql) => Some(sql.clone()),
                    _ => None,
                });
            let cte_content = if let Some(union_sql) = pc_union_sql {
                // Clear the marker CTE
                with_cte_render.ctes = CteItems(vec![]);
                CteContent::RawSql(union_sql)
            } else {
                CteContent::Structured(Box::new(with_cte_render.clone()))
            };
            let mut with_cte = Cte::new(cte_name.clone(), cte_content, false);
            with_cte.columns = cte_columns;
            with_cte.with_exported_aliases = original_exported_aliases.clone();

            all_ctes.push(with_cte);

            // Detect + materialize a bidirectional weight CTE for weighted
            // shortest path. See `maybe_add_bidirectional_weight_cte`.
            maybe_add_bidirectional_weight_cte(
                &mut all_ctes,
                &original_exported_aliases,
                &cte_name,
                plan,
            );

            // Store CTE schema for later reference creation

            // Compute ID column mappings for this CTE (alias → CTE column holding
            // the id). See `compute_alias_id_columns`.
            let alias_to_id_column = compute_alias_id_columns(
                &exported_aliases,
                &inner_plans_for_id,
                &current_plan,
                &cte_references,
                &cte_schemas,
                &cte_name,
            );

            // Build explicit property mapping for WITH CTE (column resolution +
            // dot→underscore + compound-key + bare-alias cross-ref).
            // See `build_with_cte_property_mapping`.
            let property_mapping = build_with_cte_property_mapping(
                &select_items_for_schema,
                &flattened_compound_keys.borrow(),
                &exported_aliases,
                &alias_to_id_column,
            );

            // Store CTE schema with full property mapping
            cte_schemas.insert(
                cte_name.clone(),
                crate::render_plan::CteSchemaMetadata {
                    select_items: select_items_for_schema.clone(),
                    column_names: property_names_for_schema.clone(),
                    alias_to_id: alias_to_id_column,
                    property_mapping: property_mapping.clone(),
                },
            );

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Stored schema for CTE '{}': {:?}, {} property mappings",
                cte_name,
                property_names_for_schema, property_mapping.len()
            );

            // Replacing WITH clauses with this alias with CTE reference
            // Also pass pre_with_aliases so joins from the pre-WITH scope can be filtered out
            log::debug!("🔧 build_chained_with_match_cte_plan: Replacing WITH clauses for alias '{}' with CTE '{}'", with_alias, cte_name);
            log::debug!("🔧 build_chained_with_match_cte_plan: BEFORE replacement - plan discriminant: {:?}", std::mem::discriminant(&current_plan));

            // Debug: Show WITH structure before replacement
            log::debug!("🔧 PLAN STRUCTURE BEFORE REPLACEMENT:");
            show_with_structure(&current_plan, 0);

            current_plan = replace_with_clause_with_cte_reference_v2(
                &current_plan,
                &with_alias,
                &cte_name,
                &pre_with_aliases,
                &cte_schemas,
            )?;
            log::info!(
                "🔧 build_chained_with_match_cte_plan: AFTER replacement - plan discriminant: {:?}",
                std::mem::discriminant(&current_plan)
            );
            log::debug!(
                "🔀 UNION_TRACE after replace_v2: has_union={}",
                current_plan.has_union_anywhere()
            );

            log::debug!("🔧 PLAN STRUCTURE AFTER REPLACEMENT:");
            show_with_structure(&current_plan, 0);

            log::debug!(
                "🔧 build_chained_with_match_cte_plan: Replacement complete for '{}'",
                with_alias
            );

            // Register this alias as CTE-backed: mark it processed, point
            // cte_references (composite + individual aliases) at the new CTE, and
            // refresh the per-iteration snapshot. See `register_cte_alias_references`.
            register_cte_alias_references(
                &mut processed_cte_aliases,
                &mut cte_references,
                &mut cte_references_for_rendering,
                &with_alias,
                &cte_name,
                &original_exported_aliases,
            );

            // Update scope CTE variables: record each exported alias's property mapping
            // so downstream rendering resolves CTE variables correctly.
            //
            // WITH barrier: snapshot body registry, then clear accumulated scope
            // so only current CTE's exports are visible in the next scope.
            let body_registry = with_scope.snapshot_body_registry();
            with_scope.reset();

            // Publish each exported alias's CTE scope (variable registry +
            // EXISTS-correlation channel). See `publish_cte_alias_scopes`.
            publish_cte_alias_scopes(
                &mut with_scope,
                &original_exported_aliases,
                &alias_rename_map,
                &property_mapping,
                &current_plan,
                &with_plans,
                &cte_name,
                &select_items_for_schema,
            );

            // CRITICAL FIX: Also add the COMPOSITE alias (e.g., "countWindow1_tag") to scope_cte_variables.
            // The analyzer creates expressions with the composite alias as table_alias in PropertyAccessExp.
            // Without this, scope-aware rewriting in subsequent CTE bodies can't resolve composite aliases.
            // The composite alias's property_mapping merges ALL individual aliases' mappings, plus
            // identity entries for scalar aliases (which are direct CTE column names).
            if original_exported_aliases.len() > 1 {
                with_scope.publish_composite(&with_alias, &cte_name, &original_exported_aliases);
            }

            // Attach body registry (pre-barrier snapshot) to the CTE for runtime resolution
            if let Some(last_cte) = all_ctes.last_mut() {
                if last_cte.cte_name == cte_name {
                    last_cte.variable_registry = Some(body_registry);
                }
            }

            log::info!(
                "🔧 build_chained_with_match_cte_plan: Added '{}' to processed_cte_aliases",
                with_alias
            );

            // DON'T add individual parts - this causes issues with detecting duplicates
            // Example: "b_c" should not add "b" and "c" separately, because that would
            // prevent processing "b_c" again if it appears multiple times in the plan

            // Mark that we processed something this iteration
            any_processed_this_iteration = true;

            log::debug!("🔧 build_chained_with_match_cte_plan: Replaced WITH clauses for alias '{}' with CTE reference (processed_cte_aliases: {:?})",
                       with_alias, processed_cte_aliases);

            // CRITICAL FIX (Jan 2026): Break after processing ONE alias to re-discover plan structure.
            // Problem: When we process multiple aliases in one iteration, the `with_plans` for later
            // aliases were captured BEFORE we replaced earlier aliases. This causes:
            // 1. Nested WITH clauses to be processed twice (once by outer, once by recursive call)
            // 2. Duplicate CTE names to be generated
            //
            // Solution: Process one alias, update current_plan, then let the while loop iterate
            // again with fresh find_all_with_clauses_grouped() on the updated plan.
            log::debug!("🔧 build_chained_with_match_cte_plan: Breaking after processing '{}' to re-discover plan structure", with_alias);
            break 'alias_loop;
        }

        // DEBUG: Summary at end of iteration
        let plan_depth_after = count_plan_depth(&current_plan);
        log::debug!(
            "🔧 build_chained_with_match_cte_plan: END ITERATION {} - Plan depth: {} → {} (processed: {})",
            iteration,
            plan_depth,
            plan_depth_after,
            any_processed_this_iteration
        );

        // If no aliases were processed this iteration, break to avoid infinite loop
        // This can happen when all remaining WITH clauses are passthrough wrappers
        if !any_processed_this_iteration {
            log::debug!("🔧 build_chained_with_match_cte_plan: No aliases processed in iteration {}, breaking out", iteration);
            break;
        }

        log::debug!("🔧 build_chained_with_match_cte_plan: Iteration {} complete, checking for more WITH clauses", iteration);
    }

    log::debug!(
        "🔀 UNION_TRACE after all WITH iterations: has_union={}",
        current_plan.has_union_anywhere()
    );

    // Verify that all WITH clauses were actually processed
    // If any remain, it means we failed to process them and should not continue
    // to avoid triggering a fresh recursive call that loses our accumulated CTEs
    if has_with_clause_in_graph_rel(&current_plan) {
        let remaining_withs = find_all_with_clauses_grouped(&current_plan);
        let remaining_aliases: Vec<_> = remaining_withs.keys().collect();
        log::error!(
            "🔧 build_chained_with_match_cte_plan: Unprocessed WITH clauses remain after {} iterations: {:?}",
            iteration, remaining_aliases
        );
        log::error!(
            "🔧 build_chained_with_match_cte_plan: Accumulated CTEs: {:?}",
            all_ctes.iter().map(|c| &c.cte_name).collect::<Vec<_>>()
        );
        return Err(RenderBuildError::InvalidRenderPlan(format!(
            "Failed to process all WITH clauses after {} iterations. Remaining aliases: {:?}. This may indicate nested WITH clauses that couldn't be resolved.",
            iteration, remaining_aliases
        )));
    }

    log::debug!("🔧 build_chained_with_match_cte_plan: All WITH clauses processed ({} CTEs), rendering final plan", all_ctes.len());

    // DEBUG: Log the full plan structure before rendering
    log::debug!("🐛 DEBUG FINAL PLAN structure (after WITH processing):");
    show_plan_structure(&current_plan, 0);

    // DEBUG: Log the current_plan structure before rendering
    log::debug!(
        "🐛 DEBUG FINAL PLAN before render: discriminant={:?}",
        std::mem::discriminant(&current_plan)
    );
    if let LogicalPlan::Projection(proj) = &current_plan {
        log::debug!(
            "🐛 DEBUG: Projection -> input discriminant={:?}",
            std::mem::discriminant(proj.input.as_ref())
        );
        if let LogicalPlan::GraphJoins(gj) = proj.input.as_ref() {
            log::debug!("🐛 DEBUG: Found GraphJoins with {} joins:", gj.joins.len());
            for (i, j) in gj.joins.iter().enumerate() {
                log::debug!(
                    "🐛 DEBUG:   JOIN {}: table='{}', alias='{}', joining_on.len()={}",
                    i,
                    j.table_name,
                    j.table_alias,
                    j.joining_on.len()
                );
            }
            log::debug!(
                "🐛 DEBUG: GraphJoins.cte_references = {:?}",
                gj.cte_references
            );
        }
    }

    // CRITICAL FIX: Before rendering, prune GraphJoins covered by the LAST CTE
    // (the one with the most aliases). See `prune_joins_covered_by_last_cte`.
    prune_joins_covered_by_last_cte(
        &mut current_plan,
        &mut original_correlation_predicates,
        &all_ctes,
        &cte_schemas,
        &cte_references,
        &with_scope,
    )?;

    // Scope-aware join cleanup: remove ALL pre-computed joins whose aliases are now CTE-scoped.
    // These joins are stale — they reference table-level tables from before the WITH barrier.
    // The CTE references in the plan tree will produce the correct FROM/JOIN via extract_joins().
    if !with_scope.is_empty() {
        let cte_aliases: std::collections::HashSet<&str> = with_scope
            .scope_cte_variables()
            .keys()
            .map(|s| s.as_str())
            .collect();
        current_plan = clear_stale_joins_for_cte_aliases(&current_plan, &cte_aliases);
        log::info!(
            "🔧 build_chained: Cleared stale joins for CTE aliases: {:?}",
            cte_aliases
        );
    }

    // All WITH clauses have been processed, now render the final plan
    // Build scope from all accumulated CTE variables for the final rendering pass.
    let final_scope = with_scope.build_final_scope(schema, &current_plan);
    let final_scope_ref = if with_scope.is_empty() && scope.is_none() {
        None
    } else {
        Some(&final_scope)
    };
    // Use render_plan_with_ctx to pass plan_ctx for VLP property selection
    let mut render_plan =
        current_plan.to_render_plan_with_ctx(schema, plan_ctx, final_scope_ref)?;

    log::info!(
        "🔧 build_chained_with_match_cte_plan: Final render complete. FROM: {:?}, SELECT items: {}",
        render_plan.from,
        render_plan.select.items.len()
    );

    // CRITICAL FIX: Apply CTE name remapping for passthrough WITHs
    // When WITHs are skipped, expressions may still reference the analyzer's CTE names.
    // Remap them to the actual CTE names that were created.
    apply_passthrough_cte_name_remappings(&mut render_plan, &cte_name_allocator);

    // Comprehensive CTE name fixup: the analyzer assigns CTE names with its own counter
    // (e.g., _cte_5) but the renderer creates CTEs with sequential numbering (_cte_1).
    // Scan render plan for any with_*_cte_N references that don't match actual CTEs.
    reconcile_stale_cte_name_references(&mut render_plan, &all_ctes);

    // Resolve the final FROM against the accumulated CTEs (repoint a WITH-exported
    // alias onto its CTE, or fall back to the last CTE when FROM is None). Gated
    // off for a Cypher-UNION base arm (#593) — see the fn doc. `is_cypher_union_plan`
    // is also consumed by later passes, so it is bound here.
    let is_cypher_union_plan = render_plan
        .union
        .0
        .as_ref()
        .is_some_and(|u| u.is_cypher_union);
    resolve_final_from_against_cte(
        &mut render_plan,
        &cte_references,
        &all_ctes,
        is_cypher_union_plan,
    );

    // ==========================================================================
    // CRITICAL FIX: Cross-table WITH pattern - add CTE JOINs (see fn doc + #593).
    // WITH a, b MATCH (c)-[]->(d) WHERE a.x = c.x — FROM is table 'c', so JOIN
    // the CTE holding a,b to make those aliases available for SELECT/WHERE.
    // ==========================================================================
    resolve_cross_table_with_cte_joins(
        &mut render_plan,
        &cte_references,
        &cte_schemas,
        &original_correlation_predicates,
        &current_plan,
        schema,
        scope,
        is_cypher_union_plan,
    )?;

    // When FROM is None (Union shell) but CTE references exist, add CTE cross-joins
    // to each Union branch directly. This handles the case where Direct Union rendering
    // moved all branches into union.input (for aggregation/GROUP BY).
    //
    // #593: never do this for a Cypher UNION — each arm is an independent query
    // that must not be cross-joined to a sibling arm's WITH-CTE.
    add_cte_cross_joins_to_union_branches(&mut render_plan, &cte_references, is_cypher_union_plan);

    // Apply bare variable rewriting + orphan-alias fixing to the final (outer)
    // render plan. These resolve bare node aliases (e.g. `b` → `b.id`, `a` →
    // `cte.p1_a_id`) and composite orphan aliases in the outer query's SELECT /
    // WHERE / JOIN / GROUP BY, and (`fix_orphan_table_aliases`) add a CROSS JOIN
    // for any scope CTE not already in FROM/JOINs.
    //
    // Must run AFTER CTE JOINs are added (above) so JOIN conditions are rewritten too.
    if !with_scope.is_empty() {
        apply_final_outer_scope_passes(&mut render_plan, &final_scope, is_cypher_union_plan);
    }

    // Weighted shortestPath fix: restructure outer query to use VLP CTE as FROM.
    // When weight CTE is detected and VLP CTEs exist, the outer query incorrectly
    // cross-joins the weight CTE with VLP. Fix: use VLP CTE as sole FROM source,
    // remove all joins and UNION branches, and rewrite SELECT to use VLP columns.
    apply_weighted_shortest_path_restructure(&mut render_plan);

    // Add all CTEs (innermost first, which is correct order for SQL)
    all_ctes.extend(render_plan.ctes.0);
    render_plan.ctes = CteItems(all_ctes);

    // Skip validation - CTEs are hoisted progressively through recursion
    // ClickHouse will validate CTE references when executing the SQL
    // Validation here causes false failures when nested calls reference outer CTEs
    // that haven't been hoisted yet but will be present in the final SQL

    // Apply VLP alias rewriting for path functions in WITH clauses
    // This fixes "Unknown expression identifier `t.hop_count`" errors where
    // length(path) was converted to t.hop_count but t needs to be rewritten to the actual VLP alias
    rewrite_vlp_union_branch_aliases(&mut render_plan, plan, schema)?;

    // 🔧 FIX: Rewrite aggregate arguments for VLP end nodes
    // Problem: COUNT(DISTINCT b) where b is VLP end node generates b.end_id
    // But b doesn't exist in SQL - the VLP CTE is joined as "t"
    // Solution: Rewrite b.end_id -> t.end_id using VLP CTE metadata
    rewrite_vlp_aggregate_aliases(&mut render_plan)?;

    // Attach the final variable registry to the outer render plan
    render_plan.variable_registry = Some(std::sync::Arc::new(with_scope.take_registry()));

    log::info!(
        "🔧 build_chained_with_match_cte_plan: Success - final plan has {} CTEs",
        render_plan.ctes.0.len()
    );

    Ok(render_plan)
}

/// #529 R6: per-variant coverage tests for `collect_property_accesses`,
/// directly exercising the exact shapes adversarial review found (and
/// verified) slip past a partial `match` — a list literal, map literal,
/// array subscript/slicing, simple-CASE's `expr` scrutinee, and
/// `ReduceExpr`/`InSubquery`. Unit-testing the function directly (rather
/// than only fishing for Cypher syntax that happens to reach it) is the
/// more reliable check: some of these `RenderExpr` variants are hard or
/// impossible to trigger from real Cypher syntax specifically INSIDE a
/// WITH-clause projection item (e.g. `InSubquery` is normally a WHERE-level
/// construct), but the guard's correctness depends on the traversal
/// function itself being exhaustive regardless of which caller reaches it.
#[cfg(test)]
mod collect_property_accesses_tests {
    use super::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::render_plan::render_expr::{ColumnAlias, PropertyAccess, TableAlias};

    fn prop(alias: &str, col: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(col.to_string()),
        })
    }

    fn minimal_render_plan() -> RenderPlan {
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![],
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(vec![]),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
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

    fn collect(expr: &RenderExpr) -> Vec<(&str, &str)> {
        let mut out = Vec::new();
        collect_property_accesses(expr, &mut out);
        out
    }

    #[test]
    fn list_literal_recurses_into_elements() {
        let expr = RenderExpr::List(vec![prop("a", "ip"), prop("b", "port")]);
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "port")]);
    }

    #[test]
    fn map_literal_recurses_into_values() {
        let expr = RenderExpr::MapLiteral(vec![
            ("k1".to_string(), prop("a", "ip")),
            ("k2".to_string(), prop("b", "port")),
        ]);
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "port")]);
    }

    #[test]
    fn array_subscript_recurses_into_array_and_index() {
        let expr = RenderExpr::ArraySubscript {
            array: Box::new(prop("a", "ip")),
            index: Box::new(prop("b", "idx")),
        };
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "idx")]);
    }

    #[test]
    fn array_slicing_recurses_into_array_from_and_to() {
        let expr = RenderExpr::ArraySlicing {
            array: Box::new(prop("a", "ip")),
            from: Some(Box::new(prop("b", "lo"))),
            to: Some(Box::new(prop("c", "hi"))),
        };
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "lo"), ("c", "hi")]);
    }

    #[test]
    fn array_slicing_with_no_bounds_still_recurses_into_array() {
        let expr = RenderExpr::ArraySlicing {
            array: Box::new(prop("a", "ip")),
            from: None,
            to: None,
        };
        assert_eq!(collect(&expr), vec![("a", "ip")]);
    }

    #[test]
    fn simple_case_recurses_into_expr_scrutinee() {
        // `CASE a.ip WHEN 'x' THEN b.port ELSE c.name END` — the scrutinee
        // `a.ip` (the `expr` field) is exactly the field the pre-R6 guard
        // (and `references_alias`, incidentally) never checked.
        let expr = RenderExpr::Case(RenderCase {
            expr: Some(Box::new(prop("a", "ip"))),
            when_then: vec![(
                RenderExpr::Literal(Literal::String("x".to_string())),
                prop("b", "port"),
            )],
            else_expr: Some(Box::new(prop("c", "name"))),
        });
        let found = collect(&expr);
        assert!(
            found.contains(&("a", "ip")),
            "simple-CASE scrutinee (expr field) must be collected: {found:?}"
        );
        assert!(found.contains(&("b", "port")));
        assert!(found.contains(&("c", "name")));
    }

    #[test]
    fn searched_case_recurses_into_when_then_and_else() {
        let expr = RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(prop("a", "flag"), prop("b", "port"))],
            else_expr: Some(Box::new(prop("c", "name"))),
        });
        let found = collect(&expr);
        assert!(found.contains(&("a", "flag")));
        assert!(found.contains(&("b", "port")));
        assert!(found.contains(&("c", "name")));
    }

    #[test]
    fn reduce_expr_recurses_into_initial_value_list_and_expression() {
        let expr = RenderExpr::ReduceExpr(super::super::render_expr::ReduceExpr {
            accumulator: "acc".to_string(),
            initial_value: Box::new(prop("a", "init")),
            variable: "x".to_string(),
            list: Box::new(prop("b", "items")),
            expression: Box::new(prop("c", "step")),
        });
        let found = collect(&expr);
        assert!(found.contains(&("a", "init")));
        assert!(found.contains(&("b", "items")));
        assert!(found.contains(&("c", "step")));
    }

    #[test]
    fn in_subquery_recurses_into_expr() {
        let expr = RenderExpr::InSubquery(super::super::render_expr::InSubquery {
            expr: Box::new(prop("a", "ip")),
            subplan: Box::new(minimal_render_plan()),
        });
        assert_eq!(collect(&expr), vec![("a", "ip")]);
    }

    #[test]
    fn operator_application_still_recurses() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![prop("a", "ip"), prop("b", "ip")],
        });
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "ip")]);
    }

    #[test]
    fn nested_list_of_case_of_map_all_recurse() {
        // A deliberately nested combination: List[ Case[ Map{ .. } ] ] — if
        // any single arm silently drops instead of recursing, this loses
        // the innermost access.
        let inner_map = RenderExpr::MapLiteral(vec![("k".to_string(), prop("deep", "prop"))]);
        let case_expr = RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(RenderExpr::Literal(Literal::Boolean(true)), inner_map)],
            else_expr: None,
        });
        let list_expr = RenderExpr::List(vec![case_expr]);
        assert_eq!(collect(&list_expr), vec![("deep", "prop")]);
    }

    #[test]
    fn leaf_variants_with_no_references_collect_nothing() {
        for expr in [
            RenderExpr::Literal(Literal::Integer(1)),
            RenderExpr::Star,
            RenderExpr::ColumnAlias(ColumnAlias("x".to_string())),
            RenderExpr::Column(crate::render_plan::render_expr::Column(
                PropertyValue::Column("x".to_string()),
            )),
            RenderExpr::Parameter("p".to_string()),
            RenderExpr::TableAlias(TableAlias("a".to_string())),
            RenderExpr::Raw("a.foo".to_string()),
        ] {
            assert!(
                collect(&expr).is_empty(),
                "expected no property accesses collected from {expr:?}"
            );
        }
    }
}
