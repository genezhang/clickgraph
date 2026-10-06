//! #1175: a fixed hop and a CTE-backed variable-length path in ONE `MATCH` must not traverse
//! the same relationship.
//!
//! Cypher's relationship-uniqueness rule covers the whole pattern. Two fixed hops get a
//! pairwise guard from the analyzer (`correlation_predicates`), but the recursive CTE of a
//! path only knows its OWN edges (`path_edges`), so `(c)-[:R]->(a)-[:R*1..2]->(b)` let the
//! path walk back over the edge the hop had just used — a silent over-count.
//!
//! The guard is added to the outer WHERE as `NOT has(<path>.path_edges, <hop's edge
//! identity>)`. The identity is spelled exactly as the path's own recursive arm spells it
//! (`spell_edge_identity` over the relationship's `edge_id`, else `(from, to)`), so it is the
//! same value the CTE stored.
//!
//! Only the shape verified against a brute-force oracle is handled (see [`guards`]); every
//! other shape is left exactly as it was.

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::pattern_schema::JoinStrategy;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::{GraphRel, LogicalPlan};
use crate::render_plan::render_expr::RenderExpr;
use crate::sql_generator::emitters::clickhouse::variable_length_cte::spell_edge_identity;

/// All `GraphRel`s of one scope, or `None` when the scope holds anything but a plain
/// pattern: a `WITH` (the scope is rebuilt from a CTE), a UNION or an UNWIND.
///
/// A comma/cartesian pattern is descended (#1287): it only binds more nodes (an endpoint
/// matched by an earlier clause, or carried through WITH), and `guards` pairs a hop with the
/// path only when both are in the path's own MATCH clause — any hop of another clause in the
/// scope still leaves the scope unguarded.
fn collect<'a>(node: &'a LogicalPlan, rels: &mut Vec<&'a GraphRel>) -> Option<()> {
    match node {
        LogicalPlan::WithClause(_) | LogicalPlan::Union(_) | LogicalPlan::Unwind(_) => None,
        LogicalPlan::GraphRel(gr) => {
            rels.push(gr);
            node.children()
                .into_iter()
                .try_for_each(|c| collect(c, rels))
        }
        _ => node
            .children()
            .into_iter()
            .try_for_each(|c| collect(c, rels)),
    }
}

/// The `NOT has(path.path_edges, <hop edge>)` conjuncts for the fixed hops of `plan`.
///
/// Verified shape — anything else returns no guard and renders as before:
/// - one plain pattern (no WITH / comma / UNION / UNWIND) with EXACTLY ONE path, and that path
///   is a required, directed (written forward), single-type recursive CTE with a recursive arm
///   (so its `path_edges` holds edge identities; `*0..N` included since #1230) and not
///   shortestPath or closed;
/// - the hops of the path's own `MATCH` clause are required, directed, single-hop, of the
///   SAME single relationship type (hops of other clauses are not bound by it and ignored);
/// - the relationship is a standard (separate node table) or denormalized (single table)
///   edge with plain single-column endpoints.
pub(super) fn guards(plan: &LogicalPlan) -> Vec<RenderExpr> {
    let Some(schema) = crate::server::query_context::get_current_schema() else {
        return vec![];
    };
    let mut rels: Vec<&GraphRel> = Vec::new();
    if collect(plan, &mut rels).is_none() {
        return vec![];
    }

    let is_path = |gr: &GraphRel| gr.variable_length.is_some();
    let paths: Vec<&&GraphRel> = rels.iter().filter(|gr| is_path(gr)).collect();
    let [path] = paths.as_slice() else {
        return vec![];
    };
    let path: &GraphRel = path;
    if super::from_builder::is_fixed_length_vlp(path)
        || path.is_optional.unwrap_or(false)
        || path.shortest_path_mode.is_some()
        || path.direction == Direction::Either
        // An undirected path is one doubled-edge walk (#617) whose `path_edges` keeps each edge's
        // ORIGINAL (from, to) — the same value the hop's own columns spell. A legacy two-arm
        // split (denormalized layout) is not.
        || (path.was_undirected == Some(true)
            && !crate::query_planner::analyzer::bidirectional_union::undirected_vlp_single_walk_core(
                path, &schema,
            ))
        || path.left_connection == path.right_connection
        // `*0..N` is edge-unique like every other lower bound since #1230 (its zero-hop base
        // seeds a typed-empty `path_edges`); only `*0..0` has no recursion and no `path_edges`.
        || path
            .variable_length
            .as_ref()
            .is_none_or(|spec| spec.max_hops == Some(0))
        || path
            .pattern_combinations
            .as_ref()
            .is_some_and(|combos| combos.len() > 1)
    {
        return vec![];
    }
    let Some([label]) = path.labels.as_deref() else {
        return vec![];
    };

    // Relationship uniqueness binds only the hops of the path's own MATCH clause: a hop of
    // another clause (an earlier comma-joined MATCH, a later OPTIONAL MATCH) may reuse the
    // path's edges, so it is neither guarded nor a reason to leave the clause unguarded.
    let hops: Vec<&GraphRel> = rels
        .iter()
        .copied()
        .filter(|gr| !is_path(gr) && gr.match_clause_index == path.match_clause_index)
        .collect();
    if hops.is_empty()
        || hops.iter().any(|hop| {
            // `was_undirected` is NOT excluded (#1233): a REQUIRED undirected hop is the legacy
            // two-arm split, each arm a directed hop over the SAME edge row (its from/to columns
            // spell the same identity the path stored). Only the OPTIONAL single-hop rewrite
            // renders a doubled-edge subquery with swapped columns, and optional hops are out above.
            hop.is_optional.unwrap_or(false)
                || hop.direction == Direction::Either
                || hop.labels.as_deref() != Some(std::slice::from_ref(label))
                || hop
                    .pattern_combinations
                    .as_ref()
                    .is_some_and(|combos| combos.len() > 1)
        })
    {
        return vec![];
    }

    // The path's relationship must be a layout whose CTE spells the plain edge identity.
    let Ok(rel_schema) = schema.get_rel_schema(label) else {
        return vec![];
    };
    let (Identifier::Single(from_col), Identifier::Single(to_col)) =
        (&rel_schema.from_id, &rel_schema.to_id)
    else {
        return vec![];
    };
    let plain = |s: &str| {
        !s.is_empty()
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !s.starts_with(|c: char| c.is_ascii_digit())
    };
    let edge_id_cols: Vec<&str> = rel_schema
        .edge_id
        .as_ref()
        .map(|id| id.columns())
        .unwrap_or_default();
    if !plain(from_col) || !plain(to_col) || !edge_id_cols.iter().all(|c| plain(c)) {
        return vec![];
    }
    for gr in std::iter::once(path).chain(hops.iter().copied()) {
        let Ok(ctx) = super::cte_extraction::recreate_pattern_schema_context(gr, &schema, None)
        else {
            // #1294: a hop between two WITH-carried nodes has no endpoint labels left to
            // rebuild its context from. It has the path's single relationship type (checked
            // above), so the path's verdict on the layout stands for it.
            if std::ptr::eq(gr, path) {
                return vec![];
            }
            continue;
        };
        if !matches!(
            ctx.join_strategy,
            JoinStrategy::Traditional { .. } | JoinStrategy::SingleTableScan { .. }
        ) {
            return vec![];
        }
    }

    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
    let path_alias = crate::server::query_context::vlp_from_alias();
    hops.iter()
        .map(|hop| {
            let identity = spell_edge_identity(
                mapper.tuple_constructor(),
                &rel_schema.edge_id,
                &hop.alias,
                from_col,
                to_col,
                |c| c,
            );
            RenderExpr::Raw(format!(
                "NOT {}({}.path_edges, {})",
                mapper.array_contains(),
                path_alias,
                identity
            ))
        })
        .collect()
}
