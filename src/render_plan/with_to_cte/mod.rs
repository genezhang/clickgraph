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
//! Extracted so far (P2.6 slice 1): the #529 shape-1 loud-guard property
//! helpers `table_role_dependent_property_names` + `collect_property_accesses`
//! (with their unit tests). Both are re-exported `pub(crate)` from
//! `plan_builder_utils` during the transition so the giant builder's call
//! sites keep resolving.

use crate::graph_catalog::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::render_expr::RenderExpr;
// `find_all_with_clauses_grouped` still lives in `plan_builder_utils` (it moves
// into this module in P2.6 slice 3); import it during the transition.
use super::plan_builder_utils::find_all_with_clauses_grouped;
// `plan_contains_with_clause` is a `plan_predicates` (P2.4) predicate that
// `replace_with_clause_with_cte_reference_v2` gates its GraphRel recursion on.
use super::plan_predicates::plan_contains_with_clause;
// Types referenced by bare name from the `#[cfg(test)]` module below via its
// `use super::*` — kept here so that glob keeps resolving after the move.
#[cfg(test)]
use crate::render_plan::render_expr::{Literal, Operator, OperatorApplication, RenderCase};
#[cfg(test)]
use crate::render_plan::{
    ArrayJoinItem, CteItems, FilterItems, FromTableItem, GroupByExpressions, JoinItems, LimitItem,
    OrderByItems, RenderPlan, SelectItems, SkipItem, UnionItems,
};

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
pub(crate) fn replace_with_clause_with_cte_reference_v2(
    plan: &LogicalPlan,
    with_alias: &str,
    cte_name: &str,
    pre_with_aliases: &std::collections::HashSet<String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    log::debug!(
        "🔧 replace_v2: Processing plan type {:?} for alias '{}'",
        std::mem::discriminant(plan),
        with_alias
    );

    /// Extract the node label from a plan tree by traversing through wrapper nodes
    fn extract_node_label_from_plan(plan: &Arc<LogicalPlan>) -> Option<String> {
        match plan.as_ref() {
            LogicalPlan::GraphNode(gn) => gn.label.clone(),
            LogicalPlan::Filter(f) => extract_node_label_from_plan(&f.input),
            LogicalPlan::Projection(p) => extract_node_label_from_plan(&p.input),
            LogicalPlan::WithClause(wc) => extract_node_label_from_plan(&wc.input),
            _ => None,
        }
    }

    // Helper to generate a key for a WithClause (matches the key generation in find_all_with_clauses_grouped)
    fn get_with_clause_key(wc: &crate::query_planner::logical_plan::WithClause) -> String {
        if !wc.exported_aliases.is_empty() {
            let mut aliases = wc.exported_aliases.clone();
            aliases.sort();
            return aliases.join("_");
        }
        "with_var".to_string()
    }

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
                            cte_alias, current_col, cte_col.raw()
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
                        remap_property_access_for_cte(
                            arg,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect();
                LogicalExpr::AggregateFnCall(agg)
            }
            LogicalExpr::ScalarFnCall(mut func) => {
                func.args = func
                    .args
                    .into_iter()
                    .map(|arg| {
                        remap_property_access_for_cte(
                            arg,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
                    })
                    .collect();
                LogicalExpr::ScalarFnCall(func)
            }
            LogicalExpr::List(list) => LogicalExpr::List(
                list.into_iter()
                    .map(|item| {
                        remap_property_access_for_cte(
                            item,
                            cte_alias,
                            property_mapping,
                            db_to_cypher,
                        )
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

        // CRITICAL: Use the original WITH alias (e.g., "a") as the GraphNode alias
        // This ensures property references like "a.user_id" work correctly
        // The FROM clause will render as: FROM with_a_cte1 AS a
        let table_alias = with_alias.to_string();

        // Build property_mapping using CYPHER PROPERTY NAMES ONLY
        // Store the ViewScan's DB mapping separately so we can reverse-resolve DB columns
        let (property_mapping, _db_to_cypher_mapping) = if let Some(meta) =
            cte_schemas.get(cte_name)
        {
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

    match plan {
        // NEW: Handle WithClause type
        // Key insight: Check if this WithClause's generated key matches the alias we're looking for
        LogicalPlan::WithClause(wc) => {
            // Generate key same way as find_all_with_clauses_grouped does
            let this_wc_key = get_with_clause_key(wc);
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
                    gn.label = extract_node_label_from_plan(&wc.input);
                }
                Ok(cte_ref)
            } else if is_target_with {
                // This is THE WithClause, but it has nested WITH clauses - error case
                // (We should be processing inner ones first)
                log::debug!("🔧 replace_v2: Target WithClause has nested WITH - should process inner first!");
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

                log::debug!("🔧 DEBUG replace_v2: Creating new outer WithClause with wc.cte_references = {:?}", wc.cte_references);

                Ok(LogicalPlan::WithClause(
                    wc.with_new_input(Arc::new(new_input)),
                ))
            } else {
                // This is NOT the WithClause we're looking for, but we need to recurse
                // to find and replace the inner one
                log::debug!("🔧 replace_v2: Not target WithClause (key='{}') - recursing into input to find '{}'", this_wc_key, with_alias);
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

        LogicalPlan::GraphRel(graph_rel) => {
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

        LogicalPlan::Projection(proj) => {
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
                                        crate::utils::cte_column_naming::parse_cte_column(
                                            cte_col_name,
                                        )
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
                                        if let RenderExpr::PropertyAccessExp(prop_access) =
                                            &item.expression
                                        {
                                            let db_col = prop_access.column.raw();
                                            per_alias_db_to_cypher
                                                .entry(col_alias.to_string())
                                                .or_default()
                                                .insert(
                                                    db_col.to_string(),
                                                    cypher_prop.to_string(),
                                                );
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
                                    remap_projection_item(
                                        item,
                                        alias,
                                        alias_mapping,
                                        &alias_db_to_cypher,
                                    )
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

        LogicalPlan::GraphJoins(graph_joins) => {
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
                        if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                            pa,
                        ) = operand
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
