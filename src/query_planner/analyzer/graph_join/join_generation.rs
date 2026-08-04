//! Clean join generation: one generic algorithm, schema-driven decisions.
//!
//! ## Design
//!
//! Traditional (node-edge-node) is the base case: 3 tables, 2 JOINs.
//! All other JoinStrategy variants are optimizations that skip some JOINs.
//! The ordering algorithm is schema-independent topological sort.
//!
//! ## Algorithm
//!
//! ```text
//! for each (a)-[r]->(b):
//!     joins += generate_pattern_joins(strategy, a, r, b)
//! anchor = select_anchor(joins)
//! ordered = topo_sort_joins(joins, anchor)
//! ```

use std::collections::HashSet;

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::RelationshipSchema;
use crate::graph_catalog::pattern_schema::{
    JoinStrategy, NodeAccessStrategy, NodePosition, PatternSchemaContext,
};
use crate::query_planner::analyzer::errors::AnalyzerError;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{Join, JoinType};
use crate::query_planner::plan_ctx::PlanCtx;

use super::helpers::{self, JoinBuilder};

type AnalyzerResult<T> = Result<T, AnalyzerError>;

// =============================================================================
// Resolved table info passed by caller
// =============================================================================

/// All resolved names for one `(a)-[r]->(b)` pattern.
/// Caller resolves CTE vs base-table names before calling generate_pattern_joins.
pub struct ResolvedTables<'a> {
    pub left_alias: &'a str,
    pub left_table: &'a str,
    pub left_cte_name: &'a str,
    pub rel_alias: &'a str,
    pub rel_table: &'a str,
    pub rel_cte_name: &'a str,
    pub right_alias: &'a str,
    pub right_table: &'a str,
    pub right_cte_name: &'a str,
}

// =============================================================================
// Step 1: Generate joins for one pattern — schema decides WHAT joins
// =============================================================================

/// Generate joins for a single `(a)-[r]->(b)` pattern based on JoinStrategy.
///
/// Anchor-aware: if a node is already available (from a prior pattern), no FROM
/// marker is generated for it. The edge table anchors on whichever node is available.
///
/// Four cases for Traditional:
///   Neither available: FROM left, JOIN edge ON left, JOIN right ON edge
///   Left available:    JOIN edge ON left, JOIN right ON edge
///   Right available:   JOIN edge ON right, JOIN left ON edge
///   Both available:    JOIN edge ON left AND right
pub fn generate_pattern_joins(
    ctx: &PatternSchemaContext,
    t: &ResolvedTables,
    rel_schema: &RelationshipSchema,
    plan_ctx: &PlanCtx,
    pre_filter: Option<LogicalExpr>,
    already_available: &HashSet<String>,
) -> AnalyzerResult<Vec<Join>> {
    let joins = match &ctx.join_strategy {
        JoinStrategy::Traditional {
            left_join_col,
            right_join_col,
        } => {
            let left_id = own_table_id(&ctx.left_node, "Traditional left")?;
            let right_id = own_table_id(&ctx.right_node, "Traditional right")?;

            // Resolve columns through CTE mappings
            let r_left_id = helpers::resolve_identifier(&left_id, t.left_cte_name, plan_ctx);
            let r_left_join = helpers::resolve_identifier(left_join_col, t.rel_cte_name, plan_ctx);
            let r_right_id = helpers::resolve_identifier(&right_id, t.right_cte_name, plan_ctx);
            let r_right_join =
                helpers::resolve_identifier(right_join_col, t.rel_cte_name, plan_ctx);

            // #1010: same loud guard as the composite FkEdgeJoin branches — a
            // Traditional composite join zips the node_id columns against the
            // edge from/to_id columns positionally, so a same-name-set/different-
            // order pairing crosses silently. No-op for single-column ids,
            // same-order pairings, and the different-NAME cross-table case (the
            // usual shape, where the edge FK columns are named differently from
            // the node PK columns).
            guard_composite_positional_pairing(&r_left_id, &r_left_join, "Traditional left")?;
            guard_composite_positional_pairing(&r_right_id, &r_right_join, "Traditional right")?;

            let left_avail = already_available.contains(t.left_alias);
            let right_avail = already_available.contains(t.right_alias);

            // #623: a fixed hop adjacent to a variable-length hop binds its
            // shared node via the VLP CTE (t.start_id / t.end_id), not via a
            // plain node table JOIN. When the shared endpoint is a VLP
            // endpoint, emitting a separate `node_table AS alias` JOIN whose
            // condition `apply_vlp_rewrites` later rewrites to `t.start_id =
            // edge.col` leaves that node table correlated to NOTHING (its own
            // id column drops out of the condition) — an unconstrained
            // cartesian fan (~N× over-count). Instead fold the correlation
            // directly onto the EDGE join (edge.to_col = t.<cte_col>) and emit
            // NO separate node table JOIN, mirroring `SingleTableScan`'s #521
            // handling and producing the same topology the trailing case
            // already gets (where the shared node is the FROM marker the VLP
            // CTE takes over).
            //
            // #623 composite guard: the fold binds a SINGLE edge column
            // (`first_col(rel from/to id)`) against the VLP CTE's single
            // `start_id`/`end_id` column. For a COMPOSITE key the CTE column is
            // a `concat(...)` string and the edge side has multiple columns, so
            // a single-column bind silently never matches (→ 0 rows). Rather
            // than emit that silent-wrong join, only fold when BOTH the edge's
            // connecting id and the endpoint node's id are single-column;
            // otherwise fall through to the node table join (which, on a
            // composite schema, fails loudly at execution — #604 — an honest
            // error beats a silent 0, ground rule 1). Composite composed VLP is
            // a separate follow-up.
            let rel_from_single = matches!(rel_schema.from_id, Identifier::Single(_));
            let rel_to_single = matches!(rel_schema.to_id, Identifier::Single(_));
            let left_id_single = matches!(left_id, Identifier::Single(_));
            let right_id_single = matches!(right_id, Identifier::Single(_));
            let left_is_vlp =
                plan_ctx.is_vlp_endpoint(t.left_alias) && rel_from_single && left_id_single;
            let right_is_vlp =
                plan_ctx.is_vlp_endpoint(t.right_alias) && rel_to_single && right_id_single;

            // Edge join, optionally binding one/both endpoints to the VLP CTE
            // instead of to a node table. `bind_left`/`bind_right` add the
            // edge↔node equality; when the endpoint is a VLP endpoint the RHS
            // is the CTE column (t.start_id/end_id) rather than node.id.
            let edge_join = |bind_left: bool, bind_right: bool| -> Join {
                let mut b = JoinBuilder::new(t.rel_table, t.rel_alias)
                    .pre_filter(pre_filter.clone())
                    .from_id(rel_schema.from_id.first_column().to_string())
                    .to_id(rel_schema.to_id.first_column().to_string());
                if bind_left {
                    if left_is_vlp {
                        let from_col = rel_schema.from_id.first_column().to_string();
                        let (va, vc) = plan_ctx.get_vlp_join_reference(t.left_alias, &from_col);
                        b = b.add_condition(t.rel_alias, from_col, va, vc);
                    } else {
                        b = b.add_identifier_condition(
                            t.rel_alias,
                            &r_left_join,
                            t.left_alias,
                            &r_left_id,
                        );
                    }
                }
                if bind_right {
                    if right_is_vlp {
                        let to_col = rel_schema.to_id.first_column().to_string();
                        let (va, vc) = plan_ctx.get_vlp_join_reference(t.right_alias, &to_col);
                        b = b.add_condition(t.rel_alias, to_col, va, vc);
                    } else {
                        b = b.add_identifier_condition(
                            t.rel_alias,
                            &r_right_join,
                            t.right_alias,
                            &r_right_id,
                        );
                    }
                }
                b.build()
            };

            // The separate right/left node table JOIN — emitted ONLY when the
            // node is a real table (not a VLP-CTE endpoint, which is folded
            // into the edge join above).
            let right_node_join = || -> Join {
                JoinBuilder::new(t.right_table, t.right_alias)
                    .add_identifier_condition(
                        t.right_alias,
                        &r_right_id,
                        t.rel_alias,
                        &r_right_join,
                    )
                    .build()
            };
            let left_node_join = || -> Join {
                JoinBuilder::new(t.left_table, t.left_alias)
                    .add_identifier_condition(t.left_alias, &r_left_id, t.rel_alias, &r_left_join)
                    .build()
            };

            match (left_avail, right_avail) {
                // Neither available: first pattern — FROM left, JOIN edge, JOIN right
                (false, false) => {
                    // #987 facet 1: a CLOSED single-hop (`(a)-[:R]->(a)`, same
                    // variable at both endpoints, `left_alias == right_alias`)
                    // must bind the shared node ONCE. The default layout emits a
                    // FROM marker for the left node AND a separate right-node JOIN
                    // with the SAME alias → duplicate table alias → ClickHouse
                    // Code 179. Instead, keep the single FROM marker and fold BOTH
                    // endpoint equalities onto the edge join
                    // (`edge.from = a.id AND edge.to = a.id`), which is
                    // self-contained (it implies the self-loop `edge.from =
                    // edge.to`) and references `a` exactly once. Scoped to the
                    // non-VLP, non-available case; the closed VLP routes through
                    // the recursive CTE (#625) and never reaches here, and a closed
                    // hop can only land in `(false, false)` or `(true, true)` (both
                    // avail flags read the same alias), the latter already emitting
                    // just `edge_join(true, true)`. Denorm (single edge=node scan)
                    // and FK-edge take their own strategies, not Traditional here.
                    let is_closed_single_hop =
                        t.left_alias == t.right_alias && !left_is_vlp && !right_is_vlp;
                    if is_closed_single_hop {
                        vec![
                            JoinBuilder::from_marker(t.left_table, t.left_alias).build(),
                            edge_join(true, true),
                        ]
                    } else {
                        let mut v = vec![
                            JoinBuilder::from_marker(t.left_table, t.left_alias).build(),
                            edge_join(true, right_is_vlp),
                        ];
                        if !right_is_vlp {
                            v.push(right_node_join());
                        }
                        v
                    }
                }
                // Left available: edge anchors on left, right via edge
                (true, false) => {
                    let mut v = vec![edge_join(true, right_is_vlp)];
                    if !right_is_vlp {
                        v.push(right_node_join());
                    }
                    v
                }
                // Right available: edge anchors on right, left via edge
                (false, true) => {
                    let mut v = vec![edge_join(left_is_vlp, true)];
                    if !left_is_vlp {
                        v.push(left_node_join());
                    }
                    v
                }
                // Both available: only the edge table, joining both sides
                (true, true) => vec![edge_join(true, true)],
            }
        }

        // Optimization: fully denormalized, 0 JOINs
        JoinStrategy::SingleTableScan { .. } => {
            if already_available.contains(t.rel_alias) {
                vec![] // Edge already available, nothing to add
            } else {
                // #987 (FK-edge facet, count/aggregate path): a CLOSED
                // self-referencing FK-edge single-hop (`(a)-[:PARENT]->(a)`) whose
                // node properties are unreferenced (e.g. bare `count(*)`) takes the
                // SingleTableScan strategy — the edge table (== the node table) is
                // scanned once as `t.rel_alias`. Without the self-loop constraint
                // it silently counts ALL rows instead of only self-loops (the row
                // whose FK equals its own PK). The property form takes the
                // FkEdgeJoin strategy and is fixed separately (see the
                // `closed_self_ref` branch there). AND the self-loop
                // (`rel_alias.from_id = rel_alias.to_id`, per-column, composite-safe
                // via `self_loop_filter`) into the FROM-marker `pre_filter` — it
                // lives on the single scanned table so it survives even when the
                // count(*) form prunes everything else. Denorm closed self-hops
                // are not foreign-key edges and are handled by the render-side #983
                // filter, so there is no double-add. Gated to the closed
                // (`left_alias == right_alias`) self-referencing FK-edge.
                let self_loop_pre_filter = if rel_schema.is_self_referencing_fk_edge()
                    && t.left_alias == t.right_alias
                {
                    helpers::self_loop_filter(t.rel_alias, &rel_schema.from_id, &rel_schema.to_id)
                } else {
                    None
                };
                let merged_pre_filter = match (pre_filter, self_loop_pre_filter) {
                    (Some(p), Some(s)) => {
                        crate::query_planner::logical_expr::combinators::and(vec![p, s])
                    }
                    (Some(p), None) => Some(p),
                    (None, s) => s,
                };
                let mut builder = JoinBuilder::new(t.rel_table, t.rel_alias)
                    .pre_filter(merged_pre_filter)
                    .from_id(rel_schema.from_id.first_column().to_string())
                    .to_id(rel_schema.to_id.first_column().to_string());

                // #521: a fixed hop adjacent to a variable-length hop (e.g.
                // `(a)-[:FLIGHT*1..2]->(b)-[:FLIGHT]->(c)`) binds its left
                // (or right) node via the VLP CTE rather than via a plain
                // FROM/JOIN of its own. On a fully denormalized/virtual-node
                // schema there's no separate node table to correlate against
                // — `left_alias`/`right_alias` ARE columns embedded in this
                // very edge table — so without an explicit join condition
                // here this degenerates into an unconditional FROM marker
                // with a dangling alias (`t.left_alias`/`right_alias`
                // referenced downstream but never bound). Correlate directly
                // to the VLP CTE's start_id/end_id column, mirroring how
                // `apply_vlp_rewrites` fixes up the analogous Traditional-
                // strategy join condition (which already has a condition to
                // rewrite; SingleTableScan's bare FROM-marker has none).
                if plan_ctx.is_vlp_endpoint(t.left_alias) {
                    let from_col = rel_schema.from_id.first_column().to_string();
                    let (vlp_alias, vlp_col) =
                        plan_ctx.get_vlp_join_reference(t.left_alias, &from_col);
                    builder = builder.add_condition(t.rel_alias, from_col, vlp_alias, vlp_col);
                } else if plan_ctx.is_vlp_endpoint(t.right_alias) {
                    let to_col = rel_schema.to_id.first_column().to_string();
                    let (vlp_alias, vlp_col) =
                        plan_ctx.get_vlp_join_reference(t.right_alias, &to_col);
                    builder = builder.add_condition(t.rel_alias, to_col, vlp_alias, vlp_col);
                }

                vec![builder.build()]
            }
        }

        // Optimization: one node embedded in edge, 1 JOIN
        JoinStrategy::MixedAccess {
            joined_node,
            join_col,
        } => {
            let (node_alias, node_table, node_cte, node_strategy) = match joined_node {
                NodePosition::Left => (t.left_alias, t.left_table, t.left_cte_name, &ctx.left_node),
                NodePosition::Right => (
                    t.right_alias,
                    t.right_table,
                    t.right_cte_name,
                    &ctx.right_node,
                ),
            };
            let node_id = own_table_id(node_strategy, "MixedAccess joined node")?;
            let r_node_id = helpers::resolve_identifier(&node_id, node_cte, plan_ctx);
            let r_join_col =
                Identifier::Single(helpers::resolve_column(join_col, t.rel_cte_name, plan_ctx));

            let edge_avail = already_available.contains(t.rel_alias);
            let node_avail = already_available.contains(node_alias);

            match (edge_avail, node_avail) {
                (false, false) => vec![
                    JoinBuilder::from_marker(t.rel_table, t.rel_alias)
                        .pre_filter(pre_filter)
                        .from_id(rel_schema.from_id.first_column().to_string())
                        .to_id(rel_schema.to_id.first_column().to_string())
                        .build(),
                    JoinBuilder::new(node_table, node_alias)
                        .add_identifier_condition(node_alias, &r_node_id, t.rel_alias, &r_join_col)
                        .build(),
                ],
                (true, false) => vec![
                    // Edge available, just join the node
                    JoinBuilder::new(node_table, node_alias)
                        .add_identifier_condition(node_alias, &r_node_id, t.rel_alias, &r_join_col)
                        .build(),
                ],
                (false, true) => vec![
                    // Node available, edge anchors on node
                    JoinBuilder::new(t.rel_table, t.rel_alias)
                        .add_identifier_condition(t.rel_alias, &r_join_col, node_alias, &r_node_id)
                        .pre_filter(pre_filter)
                        .from_id(rel_schema.from_id.first_column().to_string())
                        .to_id(rel_schema.to_id.first_column().to_string())
                        .build(),
                ],
                (true, true) => vec![], // Both available, nothing to add
            }
        }

        // Optimization: multi-hop / shared-node denormalized, edge chains
        // directly to previous edge table(s) — one condition per shared node
        // (issue #482: cross-table shared-node correlation).
        JoinStrategy::EdgeToEdge { links } => {
            let mut b = JoinBuilder::new(t.rel_table, t.rel_alias)
                .pre_filter(pre_filter)
                .from_id(rel_schema.from_id.first_column().to_string())
                .to_id(rel_schema.to_id.first_column().to_string());
            for link in links {
                b = b.add_condition(
                    t.rel_alias,
                    &link.curr_edge_col,
                    &link.prev_edge_alias,
                    &link.prev_edge_col,
                );
            }
            vec![b.build()]
        }

        // Optimization: same physical row, 0 JOINs
        JoinStrategy::CoupledSameRow { .. } => {
            vec![]
        }

        // Optimization: FK on node table, 1 JOIN
        //   join_side=Left  → right node IS the edge table, JOIN left
        //   join_side=Right → left node IS the edge table, JOIN right
        JoinStrategy::FkEdgeJoin {
            from_id,
            to_id,
            join_side,
            is_self_referencing,
        } => {
            match join_side {
                NodePosition::Left => {
                    // Right node IS the edge table (anchor). Left needs JOIN.
                    let left_id = own_table_id(&ctx.left_node, "FkEdgeJoin left")?;
                    let r_left_id =
                        helpers::resolve_identifier(&left_id, t.left_cte_name, plan_ctx);
                    // #672: composite-aware (mirrors the Right branch and the
                    // self-ref path below). The old
                    // `Identifier::Single(resolve_column(from_id, …))` stringified
                    // a composite non-self-ref `from_id` into one bogus quoted
                    // column; wrapping via `from_comma_separated` + per-column
                    // resolution lets `add_identifier_condition` zip the node_id and
                    // FK column sets into per-column equalities. Byte-identical for
                    // single-column FKs.
                    let r_from_id = helpers::resolve_identifier(
                        &Identifier::from_comma_separated(from_id),
                        t.right_cte_name,
                        plan_ctx,
                    );
                    // #632: a SELF-REFERENCING FK-edge (from_node == to_node, one
                    // physical table aliased twice) needs DIFFERENT columns on the
                    // two aliases: the FROM/child row carries the FK column
                    // pointing at the TO/parent row's PK (= the node_id). The
                    // non-self-ref pairing (`left.node_id = right.from_id`) is a
                    // shared-column equi-join, correct only when the two aliases
                    // are DIFFERENT tables; for a self-join it degenerates to
                    // e.g. `c.object_id = p.parent_id` (parent↔child inverted).
                    //
                    // The FK column is whichever of {from_id, to_id} is NOT the
                    // node_id — the schemas declare it inconsistently:
                    //   filesystem PARENT: node_id=object_id, from_id=parent_id
                    //     (FK), to_id=object_id (PK) → FK = from_id.
                    //   ldbc REPLY_OF: node_id=commentId, from_id=commentId (PK),
                    //     to_id=replyOfCommentId (FK) → FK = to_id.
                    // Correct join is always `child(from-node).FK = parent(to-node)
                    // .node_id`, verified live against both schemas.
                    //
                    // #646: composite-aware. `from_id`/`to_id` are comma-joined
                    // strings; `left_id` is the node_id Identifier. Compare the
                    // FULL column sets (not just `first_col`) — the PK is the side
                    // whose columns equal node_id's, the FK is the other side. For
                    // single-column ids this is byte-identical to the old
                    // `from_id == first_col(node_id)` test. The composite pairing
                    // (e.g. `child.parent_region = parent.region AND
                    // child.parent_id = parent.object_id`) is emitted by
                    // `add_identifier_condition`, which zips the two Identifiers
                    // positionally — so the schema must declare the FK columns in
                    // the SAME order as the node_id columns (an author invariant,
                    // mirroring the non-self-ref composite FK-edge contract).
                    let from_id_ident = Identifier::from_comma_separated(from_id);
                    let node_id_cols = left_id.columns();
                    let from_id_is_pk = from_id_ident.columns() == node_id_cols;
                    let fk_ident = if from_id_is_pk {
                        Identifier::from_comma_separated(to_id)
                    } else {
                        from_id_ident
                    };
                    let r_selfref_left =
                        helpers::resolve_identifier(&fk_ident, t.left_cte_name, plan_ctx);
                    let r_selfref_right =
                        helpers::resolve_identifier(&left_id, t.right_cte_name, plan_ctx);
                    let (cond_left_id, cond_right_id) = if *is_self_referencing {
                        (&r_selfref_left, &r_selfref_right)
                    } else {
                        (&r_left_id, &r_from_id)
                    };
                    // #672 (part 1): fail loud if the composite FK/node_id columns
                    // are the same name-set in a different order (provably crossed
                    // positional zip). No-op for single-column and different-name
                    // pairings.
                    guard_composite_positional_pairing(
                        cond_left_id,
                        cond_right_id,
                        "FkEdgeJoin Left",
                    )?;
                    let right_avail = already_available.contains(t.right_alias);
                    let left_avail = already_available.contains(t.left_alias);

                    // #987 (FK-edge facet): a CLOSED self-referencing FK-edge
                    // single-hop (`(a)-[:PARENT]->(a)`, `left_alias ==
                    // right_alias`) must bind the shared node ONCE. The default
                    // `(false, false)` layout emits a FROM marker for the right
                    // node plus a separate JOIN for the left node, both with the
                    // SAME alias (the edge IS the node table, self-referencing) →
                    // duplicate table alias → ClickHouse Code 179 for the property
                    // form, and — because the self-loop equality lives ONLY in
                    // that JOIN's ON — the bare `count(*)` form silently drops the
                    // constraint when `remove_unreferenced_joins` elides the
                    // unreferenced node join, counting ALL rows instead of only
                    // self-loops. Emit a SINGLE FROM-marker scan and attach the
                    // self-loop equality (`fk == node_id` on the one alias, e.g.
                    // `a.parent_region = a.region AND a.parent_id = a.object_id`)
                    // as its `pre_filter` — `from_marker_pre_filter` promotes it to
                    // the outer WHERE, which lives on the FROM anchor (never
                    // pruned) so it survives `count(*)` elision. Composite-safe:
                    // built from the same `cond_left_id`/`cond_right_id` Identifier
                    // column sets the JOIN would have used, zipped per-column and
                    // AND-combined (never a bare `format!`, which would comma-join
                    // a composite Identifier into invalid SQL). Gated to
                    // `is_self_referencing && left_alias == right_alias`, so
                    // non-self-ref FK-edges (distinct tables) and OPEN self-ref
                    // FK-edges (distinct aliases) are untouched.
                    let closed_self_ref = *is_self_referencing && t.left_alias == t.right_alias;
                    if closed_self_ref && !right_avail && !left_avail {
                        let self_loop =
                            helpers::self_loop_filter(t.right_alias, cond_left_id, cond_right_id);
                        return Ok(vec![JoinBuilder::from_marker(t.right_table, t.right_alias)
                            .from_id(from_id.clone())
                            .to_id(to_id.clone())
                            .pre_filter(self_loop)
                            .build()]);
                    }

                    match (right_avail, left_avail) {
                        (false, false) => vec![
                            JoinBuilder::from_marker(t.right_table, t.right_alias)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                            JoinBuilder::new(t.left_table, t.left_alias)
                                .add_identifier_condition(
                                    t.left_alias,
                                    cond_left_id,
                                    t.right_alias,
                                    cond_right_id,
                                )
                                .build(),
                        ],
                        (true, false) => vec![JoinBuilder::new(t.left_table, t.left_alias)
                            .add_identifier_condition(
                                t.left_alias,
                                cond_left_id,
                                t.right_alias,
                                cond_right_id,
                            )
                            .build()],
                        (false, true) => vec![JoinBuilder::new(t.right_table, t.right_alias)
                            .add_identifier_condition(
                                t.right_alias,
                                cond_right_id,
                                t.left_alias,
                                cond_left_id,
                            )
                            .from_id(from_id.clone())
                            .to_id(to_id.clone())
                            .build()],
                        (true, true) => vec![],
                    }
                }
                NodePosition::Right => {
                    // Left node IS the edge table (anchor). Right needs JOIN.
                    let right_id = own_table_id(&ctx.right_node, "FkEdgeJoin right")?;
                    let r_right_id =
                        helpers::resolve_identifier(&right_id, t.right_cte_name, plan_ctx);
                    // #672: composite-aware. `to_id` is a comma-joined string; wrap
                    // it as a full Identifier (single OR composite) and resolve each
                    // column, mirroring the self-ref path above. The old
                    // `Identifier::Single(resolve_column(to_id, …))` stringified a
                    // composite `to_id` into one bogus quoted column
                    // (`c."forum_region, forum_id"`); `add_identifier_condition`
                    // then zips the node_id and FK column sets positionally into
                    // per-column equalities (`f.region = c.forum_region AND
                    // f.forum_id = c.forum_id`). Byte-identical for single-column
                    // FKs. The author invariant (FK columns declared in the SAME
                    // order as the target node_id) mirrors the composite self-ref
                    // and SeparateTable FK-edge contracts.
                    let r_to_id = helpers::resolve_identifier(
                        &Identifier::from_comma_separated(to_id),
                        t.left_cte_name,
                        plan_ctx,
                    );
                    // #672 (part 1): loud guard on same-name-set/different-order
                    // composite pairing (see FkEdgeJoin Left).
                    guard_composite_positional_pairing(&r_right_id, &r_to_id, "FkEdgeJoin Right")?;
                    let left_avail = already_available.contains(t.left_alias);
                    let right_avail = already_available.contains(t.right_alias);

                    match (left_avail, right_avail) {
                        (false, false) => vec![
                            JoinBuilder::from_marker(t.left_table, t.left_alias)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                            JoinBuilder::new(t.right_table, t.right_alias)
                                .add_identifier_condition(
                                    t.right_alias,
                                    &r_right_id,
                                    t.left_alias,
                                    &r_to_id,
                                )
                                .build(),
                        ],
                        (true, false) => vec![JoinBuilder::new(t.right_table, t.right_alias)
                            .add_identifier_condition(
                                t.right_alias,
                                &r_right_id,
                                t.left_alias,
                                &r_to_id,
                            )
                            .build()],
                        (false, true) => vec![JoinBuilder::new(t.left_table, t.left_alias)
                            .add_identifier_condition(
                                t.left_alias,
                                &r_to_id,
                                t.right_alias,
                                &r_right_id,
                            )
                            .from_id(from_id.clone())
                            .to_id(to_id.clone())
                            .build()],
                        (true, true) => vec![],
                    }
                }
            }
        }
    };

    Ok(joins)
}

// =============================================================================
// Step 2: Collect + deduplicate across multiple patterns
// =============================================================================

/// Collect joins from a new pattern into the accumulated list.
/// Handles deduplication: when an alias is already present, its conditions
/// are redistributed to the join that references it (correlation).
pub fn collect_with_dedup(all_joins: &mut Vec<Join>, new_joins: Vec<Join>) {
    let existing_aliases: HashSet<String> =
        all_joins.iter().map(|j| j.table_alias.clone()).collect();

    for join in new_joins {
        if existing_aliases.contains(&join.table_alias) {
            // Alias already in the list. Two cases:
            // 1. FROM marker (no conditions) → just skip it
            // 2. Has conditions → redistribute them to the referencing join
            if !join.joining_on.is_empty() {
                redistribute_conditions(all_joins, &join);
            }
        } else {
            all_joins.push(join);
        }
    }
}

/// When a duplicate join has conditions (e.g., `right.id = edge.to_id`),
/// move those conditions to the OTHER join they reference.
///
/// Example: pattern 2 generates `JOIN b ON b.id = r2.to_id` but b already exists.
/// We find join `r2` and add `r2.to_id = b.id` to its conditions.
fn redistribute_conditions(joins: &mut [Join], duplicate: &Join) {
    for condition in &duplicate.joining_on {
        let refs = extract_aliases_from_condition(condition);
        // Find the "other" alias (not the duplicate's own alias)
        let target_alias = refs.iter().find(|a| *a != &duplicate.table_alias);
        if let Some(target) = target_alias {
            if let Some(target_join) = joins.iter_mut().find(|j| j.table_alias == **target) {
                target_join.joining_on.push(condition.clone());
                log::debug!(
                    "🔄 Redistributed condition from duplicate '{}' to '{}'",
                    duplicate.table_alias,
                    target
                );
            }
        }
    }
}

// =============================================================================
// Step 3: Select anchor + topological sort — schema-independent
// =============================================================================

/// Select the anchor (FROM) table from collected joins.
///
/// Priority:
/// 1. Non-optional FROM markers with inline property filters (most selective)
/// 2. Non-optional FROM markers without filters (alphabetically smallest)
/// 3. Optional FROM markers (LEFT JOIN, alphabetically smallest)
///
/// When `plan_ctx` is provided, aliases with inline property filters (like
/// `{name: $tag}`) are preferred because they're highly selective — starting
/// FROM a filtered table lets ClickHouse process far fewer rows through
/// downstream JOINs.
///
/// ## Stats-informed ranking (S1, `docs/design/STATS_PLANNING.md`)
///
/// When the current query carries a table-stats snapshot (attached to the
/// task-local `QueryContext` by the server, only under
/// `CLICKGRAPH_STATS_ENABLED=true`), candidates WITHIN each priority tier are
/// ranked by ascending row count before the alphabetical tie-break. Tables
/// with unknown counts sort after known ones, alphabetically among
/// themselves — so with no snapshot (the default: sql_only, tests, flag off)
/// or all-unknown counts, the ordering is exactly the historical alphabetical
/// one. This changes only WHICH semantically-equivalent anchor is picked
/// (ordering), never row membership (PRIORITIES.md §1.7).
pub fn select_anchor(joins: &[Join], plan_ctx: Option<&PlanCtx>) -> Option<String> {
    let stats = crate::server::query_context::get_current_table_stats();

    // Sort candidate (alias, table_name) pairs: by ascending row count when a
    // stats snapshot is present (unknown counts last), then alphabetically by
    // alias. Without a snapshot this is a pure alphabetical sort — identical
    // to the stats-less engine.
    let rank = |candidates: &mut Vec<(&str, &str)>| {
        candidates.sort_by(|a, b| {
            let key = |(_, table): &(&str, &str)| {
                stats
                    .as_ref()
                    .and_then(|s| s.row_count(table))
                    .unwrap_or(u64::MAX)
            };
            key(a).cmp(&key(b)).then_with(|| a.0.cmp(b.0))
        });
    };

    // Collect non-optional FROM markers
    let mut non_optional: Vec<(&str, &str)> = Vec::new();
    for join in joins {
        if join.joining_on.is_empty() && join.join_type != JoinType::Left {
            non_optional.push((&join.table_alias, &join.table_name));
        }
    }

    if !non_optional.is_empty() {
        // If plan_ctx is available, prefer aliases with selective filters
        // (inline properties like {name: $tag} or pushed-down WHERE predicates)
        if let Some(ctx) = plan_ctx {
            let mut filtered: Vec<(&str, &str)> = non_optional
                .iter()
                .copied()
                .filter(|(alias, _)| {
                    ctx.get_table_ctx(alias)
                        .is_ok_and(|tc| tc.has_selective_filters())
                })
                .collect();
            if !filtered.is_empty() {
                rank(&mut filtered);
                return Some(filtered[0].0.to_string());
            }
        }

        // No filtered nodes — rank by row count (when stats present), then
        // alphabetically.
        rank(&mut non_optional);
        return Some(non_optional[0].0.to_string());
    }

    // Fall back to any FROM marker (including Left), same ranking
    let mut optional: Vec<(&str, &str)> = Vec::new();
    for join in joins {
        if join.joining_on.is_empty() {
            optional.push((&join.table_alias, &join.table_name));
        }
    }
    rank(&mut optional);
    optional.first().map(|(alias, _)| alias.to_string())
}

/// Topological sort of joins ensuring each JOIN only references already-available tables.
///
/// FROM markers (empty conditions) are placed first — they have no dependencies.
/// Then greedily picks joins whose dependencies are all satisfied.
/// Errors on unresolvable joins (circular dependency = upstream bug).
pub fn topo_sort_joins(
    joins: Vec<Join>,
    extra_available: &HashSet<String>,
) -> AnalyzerResult<Vec<Join>> {
    let mut available: HashSet<String> = extra_available.clone();
    let mut ordered: Vec<Join> = Vec::new();
    let mut remaining: Vec<Join> = Vec::new();

    // Phase 1: FROM markers first (no dependencies), sorted for determinism
    let mut from_markers = Vec::new();
    for join in joins {
        if join.joining_on.is_empty() {
            from_markers.push(join);
        } else {
            remaining.push(join);
        }
    }
    from_markers.sort_by(|a, b| a.table_alias.cmp(&b.table_alias));
    for join in from_markers {
        available.insert(join.table_alias.clone());
        ordered.push(join);
    }

    // Phase 2: Greedy topological sort with deterministic tie-breaking.
    // When multiple joins have their dependencies satisfied in the same round,
    // sort them by table_alias to ensure deterministic ordering regardless of
    // HashMap iteration order in upstream code.
    while !remaining.is_empty() {
        let before = remaining.len();
        let mut next_remaining = Vec::new();
        let mut ready_this_round = Vec::new();

        for join in remaining {
            let deps = extract_join_dependencies(&join);
            if deps.is_subset(&available) {
                ready_this_round.push(join);
            } else {
                next_remaining.push(join);
            }
        }

        // Sort ready joins by alias for deterministic ordering
        ready_this_round.sort_by(|a, b| a.table_alias.cmp(&b.table_alias));
        for join in ready_this_round {
            available.insert(join.table_alias.clone());
            ordered.push(join);
        }

        remaining = next_remaining;
        if remaining.len() == before {
            // No progress — unresolvable dependencies
            let stuck: Vec<_> = remaining
                .iter()
                .map(|j| {
                    let deps = extract_join_dependencies(j);
                    let missing: Vec<_> = deps.difference(&available).cloned().collect();
                    format!("'{}' needs {:?}", j.table_alias, missing)
                })
                .collect();
            return Err(AnalyzerError::OptimizerError {
                message: format!("Unresolvable join dependencies: {}", stuck.join(", ")),
            });
        }
    }

    Ok(ordered)
}

// =============================================================================
// Step 4: Post-processing passes (applied once after sort)
// =============================================================================

/// Mark joins as LEFT JOIN for aliases in the optional set.
pub fn apply_optional_marking(joins: &mut [Join], optional_aliases: &HashSet<String>) {
    // Find the first optional alias — everything from there on should be LEFT JOIN
    let first_optional_idx = joins
        .iter()
        .position(|j| optional_aliases.contains(&j.table_alias));

    if let Some(start) = first_optional_idx {
        for join in &mut joins[start..] {
            if join.join_type == JoinType::Inner {
                join.join_type = JoinType::Left;
            }
        }
    }
}

// =============================================================================
// Step 5: VLP endpoint rewriting (post-pass before topo_sort)
// =============================================================================

/// Rewrite join conditions to reference VLP CTE endpoints instead of base node aliases.
///
/// When a node is accessed via a VLP CTE (e.g., `t.end_id` instead of `u2.user_id`),
/// join conditions that reference that node must be rewritten.
///
/// # #544: multiple independent VLPs in one scope
///
/// `PlanCtx::get_vlp_join_reference` always maps a VLP endpoint onto the SAME
/// placeholder outer-query alias (`VLP_CTE_FROM_ALIAS`, `"t"`) regardless of
/// which variable-length-path relationship it belongs to — the render phase
/// currently emits every VLP CTE's outer reference under that one alias (see
/// the `TODO(multi-vlp)` note in `cte_extraction.rs`; a real fix needs the
/// render phase to thread through the per-VLP `vlp_alias` that `PlanCtx`
/// already tracks, which is a larger, separate change). When the joins passed
/// here need to correlate against endpoints belonging to TWO OR MORE DISTINCT
/// VLP relationships (e.g. a fixed hop bridging `(a)-[*1..2]->(b)-[:REL]->(c)-[*2..3]->(d)`,
/// where `b` and `c` are endpoints of two independent VLPs), both endpoints
/// would silently collapse onto the same `"t"` alias: one VLP's CTE becomes
/// unreferenced and is dropped, and its endpoint gets silently conflated with
/// the other VLP's endpoint (wrong data, no error). Detect that ambiguity here
/// and fail loudly instead — a clear error beats silently wrong results.
fn distinct_vlp_rel_aliases(joins: &[Join], plan_ctx: &PlanCtx) -> Vec<String> {
    use crate::graph_catalog::expression_parser::PropertyValue;

    let mut seen = std::collections::BTreeSet::new();
    for join in joins {
        for condition in &join.joining_on {
            for operand in &condition.operands {
                if let LogicalExpr::PropertyAccessExp(pa) = operand {
                    if !matches!(pa.column, PropertyValue::Column(_)) {
                        continue;
                    }
                    if let Some(vlp_info) = plan_ctx.get_vlp_endpoint(&pa.table_alias.0) {
                        seen.insert(vlp_info.rel_alias.clone());
                    }
                }
            }
        }
    }
    seen.into_iter().collect()
}

pub fn apply_vlp_rewrites(joins: &mut [Join], plan_ctx: &PlanCtx) -> AnalyzerResult<()> {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{PropertyAccess, TableAlias};

    let rel_aliases = distinct_vlp_rel_aliases(joins, plan_ctx);
    if rel_aliases.len() > 1 {
        return Err(AnalyzerError::UnsupportedPattern {
            message: format!(
                "(#544) a fixed-length hop in this MATCH must correlate against \
                 endpoints of {} distinct variable-length-path relationships \
                 in the same scope ({}). ClickGraph does not yet support \
                 multiple independent variable-length paths bridged by a fixed \
                 hop within one MATCH scope — the render phase aliases every \
                 VLP CTE identically, which would silently drop one VLP's \
                 results and conflate its endpoint with the other VLP's. \
                 Workaround: split the pattern across a WITH clause so each \
                 variable-length path is resolved in its own scope.",
                rel_aliases.len(),
                rel_aliases.join(", ")
            ),
        });
    }

    for join in joins.iter_mut() {
        for condition in &mut join.joining_on {
            for operand in &mut condition.operands {
                if let LogicalExpr::PropertyAccessExp(pa) = operand {
                    let alias = pa.table_alias.0.clone();
                    let col = match &pa.column {
                        PropertyValue::Column(c) => c.clone(),
                        _ => continue,
                    };
                    let (new_alias, new_col) = plan_ctx.get_vlp_join_reference(&alias, &col);
                    if new_alias != alias || new_col != col {
                        log::debug!(
                            "🔄 VLP rewrite: {}.{} → {}.{}",
                            alias,
                            col,
                            new_alias,
                            new_col
                        );
                        *operand = LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(new_alias),
                            column: PropertyValue::Column(new_col),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

// =============================================================================
// Step 6: Register denormalized aliases (side effects)
// =============================================================================

/// Register denormalized alias information in plan_ctx based on JoinStrategy.
/// This is the ONLY place side effects happen — separated from join generation.
pub fn register_denormalized_aliases(
    ctx: &PatternSchemaContext,
    left_alias: &str,
    rel_alias: &str,
    right_alias: &str,
    plan_ctx: &mut PlanCtx,
    left_already_bound: bool,
) {
    let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
    // Is THIS pattern's relationship optional (introduced by OPTIONAL MATCH)?
    // An OPTIONAL pattern must not steal the owning-edge binding of a node that
    // an EARLIER pattern already bound: the registry drives both planner
    // property resolution (`get_node_strategy`) and render alias mapping, so
    // re-binding e.g. `b` in `MATCH (a)-[t1]->(b) OPTIONAL MATCH (b)-[t2]->(c)`
    // to the LEFT-JOINed `t2` makes `b`'s properties NULL on exactly the rows
    // the OPTIONAL hop misses, even though `b` matched (#491). Registration
    // runs in plan order, so "existing entry" == "bound by an earlier pattern".
    let rel_is_optional = plan_ctx.is_optional(rel_alias);

    match &ctx.join_strategy {
        JoinStrategy::SingleTableScan { .. } => {
            // Both nodes embedded in edge table
            register_if_embedded(
                &ctx.left_node,
                left_alias,
                rel_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
            register_if_embedded(
                &ctx.right_node,
                right_alias,
                rel_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
        }
        JoinStrategy::MixedAccess { joined_node, .. } => {
            // One node is embedded
            let (embedded_strategy, embedded_alias) = match joined_node {
                NodePosition::Right => (&ctx.left_node, left_alias),
                NodePosition::Left => (&ctx.right_node, right_alias),
            };
            register_if_embedded(
                embedded_strategy,
                embedded_alias,
                rel_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
        }
        JoinStrategy::EdgeToEdge { .. } => {
            // Both nodes embedded in edge
            register_if_embedded(
                &ctx.left_node,
                left_alias,
                rel_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
            register_if_embedded(
                &ctx.right_node,
                right_alias,
                rel_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
        }
        JoinStrategy::CoupledSameRow { unified_alias } => {
            // Both nodes embedded, but on the UNIFIED alias (not rel_alias)
            register_if_embedded(
                &ctx.left_node,
                left_alias,
                unified_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
            register_if_embedded(
                &ctx.right_node,
                right_alias,
                unified_alias,
                &rel_type,
                rel_is_optional,
                plan_ctx,
            );
        }
        JoinStrategy::FkEdgeJoin { join_side, .. } => {
            // Relationship is embedded on the anchor node table
            let anchor_alias = match join_side {
                NodePosition::Left => right_alias, // Right is anchor
                NodePosition::Right => left_alias, // Left is anchor
            };
            plan_ctx.register_denormalized_alias(
                rel_alias.to_string(),
                anchor_alias.to_string(),
                false,
                String::new(),
                rel_type,
            );
            // #622: when this denormalized edge is CHAINED after an earlier
            // pattern (its left node already bound), render folds its redundant
            // edge scan into the anchor node join and the edge alias leaves the
            // FROM/JOIN list. Publish edge→anchor task-locally so the ctx-less
            // render SELECT path (plan_builder `GraphJoins` arm passes
            // `plan_ctx = None`) resolves `RETURN r.prop` onto the folded node
            // instead of a now-absent table (Code 47). A STANDALONE denormalized
            // edge keeps its own scan as the FROM anchor, so it must NOT get this
            // remap — doing so would rewrite `r` onto a node alias that is not in
            // the query.
            //
            // Restrict to `join_side == Left` (edge table == the TO/right node's
            // table): that is the exact shape the render fold targets — it folds
            // the edge into `graph_rel.right_connection` (the right node). For
            // `join_side == Right` (edge table == the FROM/left node table) the
            // render fold's `end_table == rel_table` test is false (the right
            // node is a DIFFERENT table that keeps its own join), the edge scan
            // is NOT suppressed, and the anchor would be the LEFT node — so a
            // remap here would diverge from render. Gating on Left keeps the
            // analyzer remap and the render fold firing on exactly the same set.
            if left_already_bound && matches!(join_side, NodePosition::Left) {
                crate::server::query_context::register_denormalized_alias(rel_alias, anchor_alias);
            }
        }
        JoinStrategy::Traditional { .. } => {
            // No denormalized aliases for traditional pattern
        }
    }
}

/// Helper: register a node as denormalized if it's EmbeddedInEdge
fn register_if_embedded(
    strategy: &NodeAccessStrategy,
    node_alias: &str,
    edge_alias: &str,
    rel_type: &str,
    rel_is_optional: bool,
    plan_ctx: &mut PlanCtx,
) {
    if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = strategy {
        // #491: an OPTIONAL pattern must NOT overwrite an existing binding.
        // The node's value is fixed by the earlier (plan-order) pattern that
        // bound it; resolving its properties through the optional pattern's
        // LEFT-JOINed table alias returns NULL on OPTIONAL-miss rows even
        // though the node matched. Required patterns keep last-write-wins
        // (#481): equality of the shared node is enforced by an inner join,
        // so either binding carries the same value there.
        if rel_is_optional && plan_ctx.get_denormalized_alias_info(node_alias).is_some() {
            log::debug!(
                "register_if_embedded: keeping existing binding for '{}' — optional pattern '{}' does not overwrite (#491)",
                node_alias,
                edge_alias
            );
            return;
        }
        // Register in PlanCtx (for query planning phase)
        plan_ctx.register_denormalized_alias(
            node_alias.to_string(),
            edge_alias.to_string(),
            *is_from_node,
            String::new(),
            rel_type.to_string(),
        );
        // ALSO register in task-local context (for rendering phase)
        // This is critical for coupled edges where edge_alias is the unified_alias
        crate::server::query_context::register_denormalized_alias(node_alias, edge_alias);
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract the ID column from a node that must be OwnTable.
fn own_table_id(strategy: &NodeAccessStrategy, context: &str) -> AnalyzerResult<Identifier> {
    match strategy {
        NodeAccessStrategy::OwnTable { id_column, .. } => Ok(id_column.clone()),
        _ => Err(AnalyzerError::OptimizerError {
            message: format!("{} requires OwnTable node, got {:?}", context, strategy),
        }),
    }
}

/// #672 / #1010: loud guard against a provably-crossed composite positional join
/// pairing.
///
/// A composite join pairs two column vectors POSITIONALLY (`add_identifier_
/// condition` zips `left.columns()[i]` with `right.columns()[i]`) — the FK
/// columns against the target node_id columns for an FK-edge (#672), or the
/// node_id columns against the edge from/to_id columns for a Traditional join
/// (#1010). The pairing is a documented author invariant: the schema must
/// declare the two column sets in the SAME order. When the two column-name sets
/// DIFFER (the normal cross-table case, e.g. FK `parent_id` → PK `object_id`, or
/// edge `forum_ref` → node `forum_id`), a mis-order is fundamentally undetectable
/// from the schema alone — no guard is possible.
///
/// But there is one detectable, provably-wrong case: when the two column-name
/// vectors are the SAME SET of names in a DIFFERENT order (e.g. `[object_id,
/// region]` zipped against `[region, object_id]`). The positional zip then pairs
/// each column with a DIFFERENT-named column from the same set — `object_id =
/// region AND region = object_id` — which can never be the intended join and
/// silently returns wrong rows (ground-rule-1 violation). This function detects
/// exactly that case and fails loud instead.
///
/// Single-column ids can never be a mis-ordered permutation of themselves, so
/// this is a no-op for them (byte-identical). It also does not fire when the
/// name sets differ (the undetectable cross-table case) — only a same-set,
/// different-order pairing trips it.
///
/// Called at the composite `FkEdgeJoin` branches (#672) and the composite
/// `Traditional` pairings (#1010). Other strategies either pair single columns
/// (`add_condition`) or present a length-mismatched pair (e.g. `MixedAccess`
/// zips a possibly-composite node_id against a single-column edge join_col — a
/// separate composite-mixed gap, #927), both of which this guard no-ops.
fn guard_composite_positional_pairing(
    left_id: &Identifier,
    right_id: &Identifier,
    context: &str,
) -> AnalyzerResult<()> {
    let left_cols = left_id.columns();
    let right_cols = right_id.columns();

    // Only composite (len >= 2) pairings can be mis-ordered permutations.
    if left_cols.len() < 2 || left_cols.len() != right_cols.len() {
        return Ok(());
    }

    // Same set of names?
    let left_set: HashSet<&str> = left_cols.iter().copied().collect();
    let right_set: HashSet<&str> = right_cols.iter().copied().collect();
    if left_set != right_set {
        // Different names → normal cross-table pairing; a mis-order here is
        // undetectable from the schema, so we do not (and cannot) guard it.
        return Ok(());
    }

    // Same set, but is the positional order identical? If so the pairing is a
    // clean shared-column equi-join (each column equals itself) — fine.
    if left_cols == right_cols {
        return Ok(());
    }

    // Same set, different order → the positional zip provably crosses columns.
    Err(AnalyzerError::UnsupportedPattern {
        message: format!(
            "composite join has two column sets that are the same set of names in \
             DIFFERENT order ({context}: {left_cols:?} vs {right_cols:?}). The join \
             pairs columns positionally, so this would silently cross them (e.g. \
             `{a} = {b} AND {b} = {a}`) and return wrong rows. Declare the join \
             columns (edge FK / node id) in the SAME order on both sides. \
             (#672/#1010)",
            a = left_cols[0],
            b = right_cols[0],
        ),
    })
}

/// Extract all table aliases referenced in a join's ON conditions.
/// The join's own alias is excluded — these are its DEPENDENCIES.
fn extract_join_dependencies(join: &Join) -> HashSet<String> {
    let mut refs = HashSet::new();
    for condition in &join.joining_on {
        collect_aliases_from_expr_list(&condition.operands, &mut refs);
    }
    refs.remove(&join.table_alias);
    refs
}

/// Extract aliases from a single ON condition (for redistribution).
fn extract_aliases_from_condition(
    condition: &crate::query_planner::logical_expr::OperatorApplication,
) -> HashSet<String> {
    let mut refs = HashSet::new();
    collect_aliases_from_expr_list(&condition.operands, &mut refs);
    refs
}

/// Recursively collect table aliases from expressions.
fn collect_aliases_from_expr_list(exprs: &[LogicalExpr], refs: &mut HashSet<String>) {
    for expr in exprs {
        collect_aliases_from_expr(expr, refs);
    }
}

fn collect_aliases_from_expr(expr: &LogicalExpr, refs: &mut HashSet<String>) {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            refs.insert(pa.table_alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            collect_aliases_from_expr_list(&op.operands, refs);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topo_sort_linear_chain() {
        // a (FROM) → r (deps: a) → b (deps: r)
        let joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "user_id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "user_id", "r", "to_id")
                .build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(order, vec!["a", "r", "b"]);
    }

    #[test]
    fn test_topo_sort_out_of_order() {
        // Given in reverse: b, r, a(FROM)
        let joins = vec![
            JoinBuilder::new("users", "b")
                .add_condition("b", "user_id", "r", "to_id")
                .build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "user_id")
                .build(),
            JoinBuilder::from_marker("users", "a").build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(order, vec!["a", "r", "b"]);
    }

    #[test]
    fn test_topo_sort_diamond() {
        // a(FROM) → r1(deps:a) → b(deps:r1), a → r2(deps:a,b — redistributed)
        let joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "a", "id")
                .add_condition("r2", "to_id", "b", "id")
                .build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        // r2 must come after both a and b
        let a_pos = order.iter().position(|&x| x == "a").unwrap();
        let b_pos = order.iter().position(|&x| x == "b").unwrap();
        let r2_pos = order.iter().position(|&x| x == "r2").unwrap();
        assert!(r2_pos > a_pos);
        assert!(r2_pos > b_pos);
    }

    #[test]
    fn test_topo_sort_unresolvable() {
        // Circular: r depends on b, b depends on r, no FROM
        let joins = vec![
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "b", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r", "to_id")
                .build(),
        ];

        let result = topo_sort_joins(joins, &HashSet::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_collect_with_dedup_shared_node() {
        // Pattern 1: FROM a, JOIN r1 ON r1.from=a.id, JOIN b ON b.id=r1.to
        // Pattern 2: FROM b (duplicate), JOIN r2 ON r2.from=b.id, JOIN c ON c.id=r2.to
        let mut all = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
        ];

        let new = vec![
            JoinBuilder::from_marker("users", "b").build(), // duplicate
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "b", "id")
                .build(),
            JoinBuilder::new("users", "c")
                .add_condition("c", "id", "r2", "to_id")
                .build(),
        ];

        collect_with_dedup(&mut all, new);
        let aliases: Vec<&str> = all.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(aliases, vec!["a", "r1", "b", "r2", "c"]);

        // Verify b's duplicate FROM marker was skipped (no extra conditions on r2)
        let r2 = all.iter().find(|j| j.table_alias == "r2").unwrap();
        assert_eq!(r2.joining_on.len(), 1); // only r2.from=b.id
    }

    #[test]
    fn test_collect_with_dedup_both_sides_bound() {
        // Pattern 1: FROM a, JOIN r1, JOIN b
        // Pattern 2: FROM a (dup), JOIN r2 ON r2.from=a.id, JOIN b ON b.id=r2.to (dup)
        // b's condition should be redistributed to r2
        let mut all = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
        ];

        let new = vec![
            JoinBuilder::from_marker("users", "a").build(), // duplicate
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b") // duplicate with conditions
                .add_condition("b", "id", "r2", "to_id")
                .build(),
        ];

        collect_with_dedup(&mut all, new);
        let aliases: Vec<&str> = all.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(aliases, vec!["a", "r1", "b", "r2"]);

        // r2 should have 2 conditions: r2.from=a.id AND r2.to=b.id (redistributed)
        let r2 = all.iter().find(|j| j.table_alias == "r2").unwrap();
        assert_eq!(r2.joining_on.len(), 2);
    }

    #[test]
    fn test_select_anchor_prefers_inner() {
        let joins = vec![
            JoinBuilder::from_marker("t1", "a")
                .join_type(JoinType::Left)
                .build(),
            JoinBuilder::from_marker("t2", "b")
                .join_type(JoinType::Inner)
                .build(),
        ];
        assert_eq!(select_anchor(&joins, None), Some("b".to_string()));
    }

    #[test]
    fn test_apply_optional_marking() {
        let mut joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r", "to_id")
                .build(),
        ];
        let optional: HashSet<String> = ["r", "b"].iter().map(|s| s.to_string()).collect();
        apply_optional_marking(&mut joins, &optional);

        assert_ne!(joins[0].join_type, JoinType::Left); // a stays Inner
        assert_eq!(joins[1].join_type, JoinType::Left); // r marked Left
        assert_eq!(joins[2].join_type, JoinType::Left); // b marked Left
    }

    #[test]
    fn test_select_anchor_prefers_filtered_alias() {
        use crate::query_planner::logical_expr::LogicalExpr;
        use crate::query_planner::plan_ctx::{PlanCtx, TableCtx};

        // Two non-optional FROM markers: "a" and "tag"
        let joins = vec![
            JoinBuilder::from_marker("users", "a")
                .join_type(JoinType::Inner)
                .build(),
            JoinBuilder::from_marker("tags", "tag")
                .join_type(JoinType::Inner)
                .build(),
        ];

        // Without plan_ctx: alphabetically "a" wins
        assert_eq!(select_anchor(&joins, None), Some("a".to_string()));

        // With plan_ctx where "tag" has a filter predicate
        let mut plan_ctx = PlanCtx::new_empty();
        let mut tag_ctx = TableCtx::build(
            "tag".to_string(),
            Some(vec!["Tag".to_string()]),
            vec![],
            false,
            true,
        );
        // Simulate a pushed-down WHERE filter (e.g., tag.name = 'Databases')
        tag_ctx.insert_filter(LogicalExpr::Literal(
            crate::query_planner::logical_expr::Literal::String("placeholder".to_string()),
        ));
        plan_ctx.insert_table_ctx("tag".to_string(), tag_ctx);

        // "a" has no filters
        let a_ctx = TableCtx::build(
            "a".to_string(),
            Some(vec!["User".to_string()]),
            vec![],
            false,
            true,
        );
        plan_ctx.insert_table_ctx("a".to_string(), a_ctx);

        // With plan_ctx: "tag" wins because it has filters
        assert_eq!(
            select_anchor(&joins, Some(&plan_ctx)),
            Some("tag".to_string())
        );
    }

    // ========================================================================
    // S1 stats-informed anchor selection (docs/design/STATS_PLANNING.md)
    // ========================================================================

    /// Helper: run `f` inside a task-local QueryContext carrying `stats`.
    fn with_stats_fixture<F: FnOnce()>(stats: &[(&str, u64)], f: F) {
        use crate::graph_catalog::table_stats::TableStatsSnapshot;
        use crate::server::query_context::{
            set_current_table_stats, with_query_context, QueryContext,
        };

        let counts: std::collections::HashMap<String, u64> =
            stats.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(with_query_context(QueryContext::new(None), async move {
            set_current_table_stats(std::sync::Arc::new(TableStatsSnapshot::from_counts(counts)));
            f();
        }));
    }

    #[test]
    fn test_select_anchor_stats_prefer_smaller_table() {
        // Alphabetical order picks "a" (users, 1M rows); with stats the
        // anchor flips to "t" (tags, 100 rows).
        let joins = vec![
            JoinBuilder::from_marker("social.users", "a")
                .join_type(JoinType::Inner)
                .build(),
            JoinBuilder::from_marker("social.tags", "t")
                .join_type(JoinType::Inner)
                .build(),
        ];

        // Stats disabled (no task-local snapshot): alphabetical, "a" wins.
        assert_eq!(select_anchor(&joins, None), Some("a".to_string()));

        // Stats enabled: smaller table wins.
        with_stats_fixture(&[("social.users", 1_000_000), ("social.tags", 100)], || {
            assert_eq!(select_anchor(&joins, None), Some("t".to_string()));
        });
    }

    #[test]
    fn test_select_anchor_stats_unknown_counts_fall_back_alphabetical() {
        let joins = vec![
            JoinBuilder::from_marker("social.zzz", "z")
                .join_type(JoinType::Inner)
                .build(),
            JoinBuilder::from_marker("social.aaa", "a")
                .join_type(JoinType::Inner)
                .build(),
        ];

        // Snapshot present but neither table known: identical to stats-less
        // (alphabetical by alias).
        with_stats_fixture(&[("social.other", 5)], || {
            assert_eq!(select_anchor(&joins, None), Some("a".to_string()));
        });

        // Known count beats unknown, even when the unknown is alphabetically
        // smaller.
        with_stats_fixture(&[("social.zzz", 5)], || {
            assert_eq!(select_anchor(&joins, None), Some("z".to_string()));
        });
    }

    #[test]
    fn test_select_anchor_stats_rank_within_filtered_tier_only() {
        use crate::query_planner::logical_expr::LogicalExpr;
        use crate::query_planner::plan_ctx::{PlanCtx, TableCtx};

        // "big" (filtered, 1M rows) vs "s" (unfiltered, 10 rows): the filter
        // tier still wins outright — stats rank candidates WITHIN a tier, they
        // never promote an unfiltered table over a filtered one.
        let joins = vec![
            JoinBuilder::from_marker("social.big_table", "big")
                .join_type(JoinType::Inner)
                .build(),
            JoinBuilder::from_marker("social.small_table", "s")
                .join_type(JoinType::Inner)
                .build(),
        ];

        let mut plan_ctx = PlanCtx::new_empty();
        let mut big_ctx = TableCtx::build(
            "big".to_string(),
            Some(vec!["Big".to_string()]),
            vec![],
            false,
            true,
        );
        big_ctx.insert_filter(LogicalExpr::Literal(
            crate::query_planner::logical_expr::Literal::String("placeholder".to_string()),
        ));
        plan_ctx.insert_table_ctx("big".to_string(), big_ctx);
        plan_ctx.insert_table_ctx(
            "s".to_string(),
            TableCtx::build(
                "s".to_string(),
                Some(vec!["Small".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        with_stats_fixture(
            &[("social.big_table", 1_000_000), ("social.small_table", 10)],
            || {
                assert_eq!(
                    select_anchor(&joins, Some(&plan_ctx)),
                    Some("big".to_string())
                );
            },
        );
    }

    #[test]
    fn test_select_anchor_stats_rank_among_multiple_filtered() {
        use crate::query_planner::logical_expr::LogicalExpr;
        use crate::query_planner::plan_ctx::{PlanCtx, TableCtx};

        // Both candidates filtered: ascending row count decides ("t" smaller),
        // beating the alphabetical pick ("a").
        let joins = vec![
            JoinBuilder::from_marker("social.users", "a")
                .join_type(JoinType::Inner)
                .build(),
            JoinBuilder::from_marker("social.tags", "t")
                .join_type(JoinType::Inner)
                .build(),
        ];

        let mut plan_ctx = PlanCtx::new_empty();
        for alias in ["a", "t"] {
            let mut tc = TableCtx::build(
                alias.to_string(),
                Some(vec!["X".to_string()]),
                vec![],
                false,
                true,
            );
            tc.insert_filter(LogicalExpr::Literal(
                crate::query_planner::logical_expr::Literal::String("placeholder".to_string()),
            ));
            plan_ctx.insert_table_ctx(alias.to_string(), tc);
        }

        with_stats_fixture(&[("social.users", 1_000_000), ("social.tags", 100)], || {
            assert_eq!(
                select_anchor(&joins, Some(&plan_ctx)),
                Some("t".to_string())
            );
        });
    }

    #[test]
    fn guard_composite_positional_pairing_rejects_same_nameset_different_order_672() {
        // Same set of names, reversed order → provably crossed → loud.
        let node_id = Identifier::Composite(vec!["region".into(), "forum_id".into()]);
        let fk = Identifier::Composite(vec!["forum_id".into(), "region".into()]);
        let err = guard_composite_positional_pairing(&node_id, &fk, "test").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("#672") && msg.contains("DIFFERENT order"),
            "{msg}"
        );
    }

    #[test]
    fn guard_composite_positional_pairing_allows_different_names_672() {
        // Different names (the normal cross-table FK↔PK case) → undetectable
        // mis-order → NOT guarded (must be a no-op).
        let node_id = Identifier::Composite(vec!["region".into(), "forum_id".into()]);
        let fk = Identifier::Composite(vec!["forum_region".into(), "forum_id".into()]);
        assert!(guard_composite_positional_pairing(&node_id, &fk, "test").is_ok());
    }

    #[test]
    fn guard_composite_positional_pairing_allows_same_order_672() {
        // Same names, same order → clean shared-column equi-join → no-op.
        let node_id = Identifier::Composite(vec!["region".into(), "forum_id".into()]);
        let fk = Identifier::Composite(vec!["region".into(), "forum_id".into()]);
        assert!(guard_composite_positional_pairing(&node_id, &fk, "test").is_ok());
    }

    #[test]
    fn guard_composite_positional_pairing_noop_single_column_672() {
        // Single-column ids can never be a mis-ordered permutation → no-op.
        let node_id = Identifier::Single("id".into());
        let fk = Identifier::Single("parent_id".into());
        assert!(guard_composite_positional_pairing(&node_id, &fk, "test").is_ok());
    }
}
