//! # Optional Cartesian Distribution Pass (#590)
//!
//! Distributes an OPTIONAL `GraphRel` over a `CartesianProduct` of disconnected
//! anchors, hoisting the disconnected (non-participating) anchor arm OUT of the
//! optional hop so each anchor keeps its own independent subtree.
//!
//! ## Problem addressed
//!
//! `MATCH (a:Airport), (x:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b)
//!  OPTIONAL MATCH (x)-[:FLIGHT]-(y) RETURN ...` lowers to
//!
//! ```text
//! GraphRel(t2, opt){ left: GraphRel(t1, opt){ left: CartesianProduct(A, X), ... }, ... }
//! ```
//!
//! where `A` / `X` are the standalone from/to-Union scans of anchors `a` / `x`.
//! Each optional edge textually attaches to ONE anchor (`t1`→`a`, `t2`→`x`), but
//! structurally the edge's `GraphRel` wraps the WHOLE `CartesianProduct` — both
//! anchors — as its `left`. The denormalized render path
//! (`find_inner_optional_denorm_graphrel` + the `__denorm_scan_{alias}` CTE
//! machinery in `plan_builder.rs`) only recognizes a `GraphRel` whose `left`/
//! `right` is DIRECTLY the anchor Union, so it never fires here; the generic path
//! then collapses both anchors onto a single alias and degrades the sibling
//! optional to `ON 1 = 1` (#590 — silent wrong results).
//!
//! ## Transformation
//!
//! For an optional `GraphRel` whose anchor side is a `CartesianProduct` and whose
//! anchor connection lives in exactly one arm (the OTHER arm being a disconnected
//! denormalized anchor scan), rewrite
//!
//! ```text
//! GraphRel(opt){ left: CartesianProduct(anchor_arm, other_arm), center, right }
//!   → CartesianProduct( GraphRel(opt){ left: anchor_arm, center, right }, other_arm )
//! ```
//!
//! Applied bottom-up, the chained example becomes
//!
//! ```text
//! CartesianProduct( GraphRel(t1, opt){ A → b }, GraphRel(t2, opt){ X → y } )
//! ```
//!
//! — two independent single-anchor denormalized OPTIONAL subtrees, each of which
//! the EXISTING (single-anchor, byte-golden-locked #505/#506/#508/#575/#582)
//! render machinery handles unchanged, with the `CartesianProduct` render arm
//! doing the CTE-merge + CROSS JOIN composition it already knows how to do.
//!
//! ## Scope
//!
//! Deliberately narrow on two axes:
//!
//! 1. **Denormalized only**: fires ONLY when the hoisted (disconnected) arm is a
//!    denormalized standalone node-scan Union. The standard / FK-edge disconnected
//!    multi-anchor OPTIONAL shape already renders correctly through the generic
//!    `CartesianProduct(GraphNode, GraphNode)` + join path (#601), so this pass
//!    must not perturb it. Uses the same denorm-node detection the sibling
//!    `UnionDistribution` guards use, per CLAUDE.md rule 7 (schema-catalog APIs, no
//!    raw flag branching).
//!
//! 2. **Directed hops only**: an UNDIRECTED (`Direction::Either`) optional hop is
//!    split by BidirectionalUnion into a per-direction `Union[GR_out, GR_in]`, and
//!    distributing it over the CartesianProduct yields `CP(Union[…], Union[…])` —
//!    a shape whose two per-arm UNION renders cannot be composed under a CROSS JOIN
//!    by the #590 render arm without emitting invalid SQL (branches missing their
//!    SELECT). Undirected disconnected-multi-anchor OPTIONAL patterns are therefore
//!    a KNOWN LIMITATION: `try_distribute_graphrel` skips them, so they fall
//!    through to the generic path (which correctly emits per-anchor OR-keyed LEFT
//!    JOINs — no `ON 1=1` conflation) unchanged. The DIRECTED shape, where the #590
//!    conflation was reported and is live-verified, is fully handled here.

use std::sync::Arc;

use crate::query_planner::logical_plan::{CartesianProduct, GraphRel, LogicalPlan};

/// Entry point: rewrite the plan tree, distributing optional GraphRels over
/// disconnected denormalized-anchor CartesianProducts (see module docs).
pub fn distribute_optional_over_cartesian(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    rewrite(plan, 0)
}

fn rewrite(plan: Arc<LogicalPlan>, depth: usize) -> Arc<LogicalPlan> {
    if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
        log::warn!("distribute_optional_over_cartesian: depth limit exceeded");
        return plan;
    }

    match plan.as_ref() {
        LogicalPlan::GraphRel(gr) => {
            // Rewrite children first (bottom-up), so a lower optional hop's own
            // distribution has already run before we inspect this hop's `left`.
            let new_left = rewrite(gr.left.clone(), depth + 1);
            let new_center = rewrite(gr.center.clone(), depth + 1);
            let new_right = rewrite(gr.right.clone(), depth + 1);

            let rebuilt = Arc::new(LogicalPlan::GraphRel(GraphRel {
                left: new_left,
                center: new_center,
                right: new_right,
                ..gr.clone()
            }));

            try_distribute_graphrel(rebuilt, depth)
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left = rewrite(cp.left.clone(), depth + 1);
            let new_right = rewrite(cp.right.clone(), depth + 1);
            Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                left: new_left,
                right: new_right,
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }
        LogicalPlan::Projection(p) => {
            let new_input = rewrite(p.input.clone(), depth + 1);
            Arc::new(LogicalPlan::Projection(
                crate::query_planner::logical_plan::Projection {
                    input: new_input,
                    ..p.clone()
                },
            ))
        }
        LogicalPlan::Filter(f) => {
            let new_input = rewrite(f.input.clone(), depth + 1);
            Arc::new(LogicalPlan::Filter(
                crate::query_planner::logical_plan::Filter {
                    input: new_input,
                    predicate: f.predicate.clone(),
                },
            ))
        }
        LogicalPlan::GraphJoins(gj) => {
            let new_input = rewrite(gj.input.clone(), depth + 1);
            Arc::new(LogicalPlan::GraphJoins(
                crate::query_planner::logical_plan::GraphJoins {
                    input: new_input,
                    ..gj.clone()
                },
            ))
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input = rewrite(gb.input.clone(), depth + 1);
            Arc::new(LogicalPlan::GroupBy(
                crate::query_planner::logical_plan::GroupBy {
                    input: new_input,
                    ..gb.clone()
                },
            ))
        }
        LogicalPlan::OrderBy(o) => {
            let new_input = rewrite(o.input.clone(), depth + 1);
            Arc::new(LogicalPlan::OrderBy(
                crate::query_planner::logical_plan::OrderBy {
                    input: new_input,
                    ..o.clone()
                },
            ))
        }
        LogicalPlan::Skip(s) => {
            let new_input = rewrite(s.input.clone(), depth + 1);
            Arc::new(LogicalPlan::Skip(
                crate::query_planner::logical_plan::Skip {
                    input: new_input,
                    ..s.clone()
                },
            ))
        }
        LogicalPlan::Limit(l) => {
            let new_input = rewrite(l.input.clone(), depth + 1);
            Arc::new(LogicalPlan::Limit(
                crate::query_planner::logical_plan::Limit {
                    input: new_input,
                    ..l.clone()
                },
            ))
        }
        _ => plan,
    }
}

/// If `plan` is an optional `GraphRel` whose anchor side is a `CartesianProduct`
/// with the anchor in one arm and a disconnected denormalized anchor scan in the
/// other, distribute: hoist the disconnected arm above the `GraphRel`.
fn try_distribute_graphrel(plan: Arc<LogicalPlan>, _depth: usize) -> Arc<LogicalPlan> {
    let LogicalPlan::GraphRel(gr) = plan.as_ref() else {
        return plan;
    };
    if !gr.is_optional.unwrap_or(false) {
        return plan;
    }
    // Scope to DIRECTED optional hops. An undirected (`Direction::Either`) hop is
    // split by BidirectionalUnion into a per-direction Union, and distributing it
    // over the CartesianProduct produces `CP(Union[GR_out,GR_in], …)` — a shape
    // whose two per-arm UNION renders cannot be composed under a CROSS JOIN by the
    // #590 render arm without emitting invalid SQL (missing per-branch SELECT).
    // Leaving undirected disconnected multi-anchor OPTIONAL patterns to the generic
    // path is a KNOWN LIMITATION (documented in the module header) — the DIRECTED
    // shape, which is where the #590 conflation was reported and verified, is fully
    // handled. Gating here also keeps the analyzer plan valid for the generic path.
    if gr.direction == crate::query_planner::logical_expr::Direction::Either {
        return plan;
    }

    // Which structural side carries the anchor (the pre-existing node the edge
    // attaches to)? The other side is the freshly-scanned optional endpoint.
    // For a disconnected-cartesian anchor, the CartesianProduct sits on whichever
    // side holds the anchor connection. Try both sides.
    let anchor_conn = &gr.left_connection;
    let opt_conn = &gr.right_connection;

    // Case A: the CartesianProduct is on gr.left (outgoing / anchor-is-left).
    if let LogicalPlan::CartesianProduct(cp) = gr.left.as_ref() {
        if let Some(rebuilt) =
            distribute_side(gr, cp, /*cp_is_left=*/ true, anchor_conn, opt_conn)
        {
            return rebuilt;
        }
    }
    // Case B: the CartesianProduct is on gr.right (incoming / anchor-is-right, #506).
    if let LogicalPlan::CartesianProduct(cp) = gr.right.as_ref() {
        if let Some(rebuilt) =
            distribute_side(gr, cp, /*cp_is_left=*/ false, anchor_conn, opt_conn)
        {
            return rebuilt;
        }
    }

    plan
}

/// Attempt the distribution when the anchor side of `gr` is `cp`.
/// `cp_is_left` = the CartesianProduct is `gr.left` (so `gr.right` is the fresh
/// optional endpoint); `false` = mirror image (`gr.right` is the CP).
fn distribute_side(
    gr: &GraphRel,
    cp: &CartesianProduct,
    cp_is_left: bool,
    anchor_conn: &str,
    opt_conn: &str,
) -> Option<Arc<LogicalPlan>> {
    // The anchor alias is whichever connection is NOT the fresh optional endpoint.
    // The fresh endpoint (`opt_conn` when cp_is_left, else `anchor_conn`) is the
    // GraphNode on the non-CP side of the GraphRel; the anchor lives inside `cp`.
    let anchor_alias = if cp_is_left { anchor_conn } else { opt_conn };

    // Identify which CP arm contains the anchor and which is the disconnected
    // (to-be-hoisted) arm. Narrow scope guard: the ANCHOR arm must be a
    // denormalized standalone node-scan Union (this is the denorm shape the
    // single-anchor render machinery expects — see module docs). The hoisted
    // arm is simply "the other arm", whatever it is: a bare denorm anchor Union
    // OR an already-distributed sibling subtree (`GraphRel(...)`) from a lower
    // optional hop processed earlier in this bottom-up pass. The standard /
    // FK-edge disconnected shape (`CartesianProduct(GraphNode, GraphNode)`) has
    // no denorm Union arm, so this never fires there.
    let (anchor_arm, other_arm) = if is_denorm_node_scan_union(&cp.left)
        && arm_binds_anchor(&cp.left, anchor_alias)
        && !arm_binds_anchor(&cp.right, anchor_alias)
    {
        (cp.left.clone(), cp.right.clone())
    } else if is_denorm_node_scan_union(&cp.right)
        && arm_binds_anchor(&cp.right, anchor_alias)
        && !arm_binds_anchor(&cp.left, anchor_alias)
    {
        (cp.right.clone(), cp.left.clone())
    } else {
        return None;
    };

    // Rebuild the GraphRel with the anchor arm in place of the whole CP, then
    // wrap in a CartesianProduct that hoists the disconnected `other_arm` out.
    let new_graph_rel = if cp_is_left {
        GraphRel {
            left: anchor_arm,
            ..gr.clone()
        }
    } else {
        GraphRel {
            right: anchor_arm,
            ..gr.clone()
        }
    };

    log::info!(
        "🔀 OptionalCartesianDistribution (#590): hoisting disconnected denorm anchor arm above OPTIONAL GraphRel '{}' (anchor '{}')",
        gr.alias,
        anchor_alias
    );

    Some(Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
        left: Arc::new(LogicalPlan::GraphRel(new_graph_rel)),
        right: other_arm,
        // The hoisted arm is a disconnected REQUIRED anchor (CROSS JOIN), not an
        // optional endpoint — mirror the non-optional disconnected-cartesian
        // shape (#601). The optionality of the edge stays inside the GraphRel.
        is_optional: false,
        join_condition: None,
    })))
}

/// Does this CP arm bind `anchor_alias` (directly or nested)?
fn arm_binds_anchor(arm: &LogicalPlan, anchor_alias: &str) -> bool {
    plan_binds_alias(arm, anchor_alias, 0)
}

fn plan_binds_alias(plan: &LogicalPlan, alias: &str, depth: usize) -> bool {
    if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
        return false;
    }
    match plan {
        LogicalPlan::GraphNode(gn) => {
            gn.alias == alias || plan_binds_alias(&gn.input, alias, depth + 1)
        }
        LogicalPlan::GraphRel(gr) => {
            gr.left_connection == alias
                || gr.right_connection == alias
                || plan_binds_alias(&gr.left, alias, depth + 1)
                || plan_binds_alias(&gr.right, alias, depth + 1)
        }
        LogicalPlan::CartesianProduct(cp) => {
            plan_binds_alias(&cp.left, alias, depth + 1)
                || plan_binds_alias(&cp.right, alias, depth + 1)
        }
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|b| plan_binds_alias(b, alias, depth + 1)),
        LogicalPlan::Filter(f) => plan_binds_alias(&f.input, alias, depth + 1),
        LogicalPlan::Projection(p) => plan_binds_alias(&p.input, alias, depth + 1),
        LogicalPlan::GraphJoins(gj) => plan_binds_alias(&gj.input, alias, depth + 1),
        _ => false,
    }
}

/// Is `plan` a Union whose branches are ALL denormalized standalone node scans
/// (the from/to role split of a bare denormalized anchor)? Mirrors
/// `union_distribution::is_all_denormalized_nodes`, kept local to avoid a
/// cross-module `pub` and to stay pinned to the schema-catalog dispatch APIs
/// (CLAUDE.md rule 7).
fn is_denorm_node_scan_union(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Union(union) = plan else {
        return false;
    };
    !union.inputs.is_empty()
        && union.inputs.iter().all(|input| match input.as_ref() {
            LogicalPlan::GraphNode(gn) => {
                crate::graph_catalog::pattern_schema::node_denormalized_flag(gn)
                    || matches!(gn.input.as_ref(), LogicalPlan::ViewScan(vs)
                        if crate::graph_catalog::pattern_schema::scan_denormalized_flag(vs))
            }
            _ => false,
        })
}
