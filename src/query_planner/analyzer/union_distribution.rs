//! Union Distribution Pass
//!
//! Hoists Union nodes from inside GraphRel/CartesianProduct/Filter chains to just
//! below GroupBy/Projection. This runs AFTER BidirectionalUnion (which creates
//! Union for undirected edges) but BEFORE GraphJoinInference (which collects JOIN
//! specs and skips Union nodes).
//!
//! Without this pass, when an undirected edge occurs in a post-WITH MATCH:
//!   `WITH ... MATCH (a)-[:KNOWS]-(b)<-[:HAS_CREATOR]-(c) ...`
//! The Union for KNOWS directions ends up buried inside:
//!   `Filter(CartesianProduct(CTE_ref, GraphRel(...Union...)))`
//! GraphJoinInference's `collect_graph_joins` skips Union nodes, so KNOWS joins
//! are invisible to the outer GraphJoins — producing incomplete SQL.
//!
//! This pass distributes the wrapping nodes over Union using algebraic identities:
//!   `CP(A, Union(B0, B1))` → `Union(CP(A, B0), CP(A, B1))`
//!   `σ_p(Union(B0, B1))` → `Union(σ_p(B0), σ_p(B1))`
//!   `GraphRel(Union(B0, B1), R, C)` → `Union(GraphRel(B0, R, C), GraphRel(B1, R, C))`
//!
//! After distribution, each Union branch is a self-contained plan that inference
//! can process independently, producing complete JOIN specs per branch.

use std::sync::Arc;

use crate::graph_catalog::GraphSchema;
use crate::query_planner::analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult};
use crate::query_planner::logical_plan::{
    CartesianProduct, Filter, GraphNode, GraphRel, LogicalPlan, Union,
};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::query_planner::transformed::Transformed;

pub struct UnionDistribution;

impl AnalyzerPass for UnionDistribution {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        // Quick check: does this plan tree even have a Union buried inside GraphRel/CP?
        if !has_buried_union(&logical_plan) {
            return Ok(Transformed::No(logical_plan));
        }

        log::info!("🔀 UnionDistribution: Found buried Union, distributing...");
        let distributed = distribute_union(&logical_plan);
        Ok(Transformed::Yes(Arc::new(distributed)))
    }
}

/// Check if a plan tree has a Union buried inside GraphRel/CartesianProduct chains
/// (i.e., NOT already at a suitable level like directly under GroupBy/Projection).
///
/// The "is there a union ANYWHERE below this GraphRel/CartesianProduct connection"
/// check used to be a second, hand-maintained walker (`has_any_union`) that covered
/// a different — and narrower — set of variants than this function's own wrapper-node
/// recursion (e.g. it had no `WithClause`/`Projection`/`GroupBy`/`OrderBy`/`Skip`/
/// `Limit`/`Unwind` arms). Two walkers doing the same "does a union exist in this
/// subtree" job with different coverage is exactly the kind of drift that silently
/// stops this pass from firing when a Union is buried behind an unvisited wrapper.
/// Delegating to `LogicalPlan::has_union_anywhere()` (the single already-exhaustive,
/// unconditional "any union below here" query used elsewhere in the codebase)
/// removes the duplicate definition entirely, so the two questions ("is a union here
/// at all" vs "is it buried at an unsuitable level") can no longer disagree on
/// coverage — only on the deliberate "already at a suitable level" exclusions below.
fn has_buried_union(plan: &LogicalPlan) -> bool {
    match plan {
        // Union inside these data-processing nodes means it needs hoisting
        LogicalPlan::CartesianProduct(cp) => {
            cp.left.has_union_anywhere() || cp.right.has_union_anywhere()
        }
        LogicalPlan::GraphRel(gr) => {
            gr.left.has_union_anywhere()
                || gr.center.has_union_anywhere()
                || gr.right.has_union_anywhere()
        }
        // Recurse through wrapper nodes
        LogicalPlan::Projection(p) => has_buried_union(&p.input),
        LogicalPlan::GroupBy(gb) => has_buried_union(&gb.input),
        LogicalPlan::Filter(f) => has_buried_union(&f.input),
        LogicalPlan::OrderBy(o) => has_buried_union(&o.input),
        LogicalPlan::Limit(l) => has_buried_union(&l.input),
        LogicalPlan::Skip(s) => has_buried_union(&s.input),
        LogicalPlan::GraphNode(gn) => has_buried_union(&gn.input),
        LogicalPlan::WithClause(wc) => has_buried_union(&wc.input),
        LogicalPlan::Unwind(u) => has_buried_union(&u.input),
        LogicalPlan::Cte(c) => has_buried_union(&c.input),
        // A Union at this level isn't itself "buried" (it IS already at a
        // suitable level for whatever wraps it), but each of ITS OWN branches
        // may independently contain a buried union further down (e.g. a
        // UNION arm whose own body is `WITH ... MATCH (a)-[:R]-(b)<-...-(c)`).
        LogicalPlan::Union(u) => u.inputs.iter().any(|b| has_buried_union(b)),
        _ => false,
    }
}

/// Clone a GraphRel with overridden left/center/right children.
fn graph_rel_with(
    gr: &GraphRel,
    left: Arc<LogicalPlan>,
    center: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
) -> GraphRel {
    GraphRel {
        left,
        center,
        right,
        ..gr.clone()
    }
}

/// Check if all Union branches are denormalized GraphNodes (standalone denormalized node scan).
fn is_all_denormalized_nodes(union: &Union) -> bool {
    !union.inputs.is_empty()
        && union.inputs.iter().all(|input| match input.as_ref() {
            LogicalPlan::GraphNode(gn) => {
                crate::graph_catalog::pattern_schema::node_denormalized_flag(gn)
                    || matches!(gn.input.as_ref(), LogicalPlan::ViewScan(vs) if crate::graph_catalog::pattern_schema::scan_denormalized_flag(vs))
            }
            _ => false,
        })
}

/// Distribute Union over the branches of a parent node, producing `Union(parent(br0), parent(br1), ...)`.
fn distribute_over_union<F>(union: &Union, make_branch: F) -> LogicalPlan
where
    F: Fn(Arc<LogicalPlan>) -> LogicalPlan,
{
    LogicalPlan::Union(Union {
        inputs: union
            .inputs
            .iter()
            .map(|branch| Arc::new(make_branch(branch.clone())))
            .collect(),
        union_type: union.union_type.clone(),
        is_cypher_union: union.is_cypher_union,
    })
}

/// Recursively distribute wrapping nodes (GraphRel, CartesianProduct, Filter)
/// over Union, hoisting Union upward. Stops at GroupBy/Projection boundaries.
fn distribute_union(plan: &LogicalPlan) -> LogicalPlan {
    distribute_union_impl(plan, 0)
}

fn distribute_union_impl(plan: &LogicalPlan, depth: usize) -> LogicalPlan {
    if depth > crate::render_plan::MAX_TRAVERSAL_DEPTH {
        log::warn!(
            "distribute_union: depth limit {} exceeded, returning plan unchanged",
            depth
        );
        return plan.clone();
    }
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // First, recurse into children
            let new_left = distribute_union_impl(&gr.left, depth + 1);
            let new_center = distribute_union_impl(&gr.center, depth + 1);
            let new_right = distribute_union_impl(&gr.right, depth + 1);

            // If left became Union after distribution, distribute GraphRel over it
            // EXCEPTION: Skip when the GraphRel is OPTIONAL and the Union is a
            // denormalized standalone node scan. Distribution would push the OPTIONAL
            // MATCH edge into each Union branch, losing LEFT JOIN semantics (every
            // flight row has airports, so no airport would appear "without flights").
            if let LogicalPlan::Union(union) = &new_left {
                let skip = gr.is_optional.unwrap_or(false) && is_all_denormalized_nodes(union);

                if skip {
                    log::info!(
                        "🔀 UnionDistribution: SKIPPING GraphRel '{}' distribution over denormalized OPTIONAL Union — preserving LEFT JOIN semantics",
                        gr.alias
                    );
                    // Keep the Union as-is; wrap in GraphRel without distributing
                } else {
                    log::debug!(
                        "🔀 UnionDistribution: distributing GraphRel '{}' over left Union ({} branches)",
                        gr.alias,
                        union.inputs.len()
                    );
                    let center = Arc::new(new_center);
                    let right = Arc::new(new_right);
                    return distribute_over_union(union, |branch| {
                        LogicalPlan::GraphRel(graph_rel_with(
                            gr,
                            branch,
                            center.clone(),
                            right.clone(),
                        ))
                    });
                }
            }

            // If right became Union after distribution, distribute GraphRel over it.
            // EXCEPTION: same as the left-Union case above (#506) — an INCOMING
            // OPTIONAL MATCH (`(a)<-[:R]-(b)`) reverses which side is structurally
            // "left"/"right" (CLAUDE.md rule 4: anchor-aware FROM/JOIN reversal), so
            // the anchor's denormalized standalone-scan Union can land on the RIGHT
            // instead of the left. Distributing unconditionally here pushed the
            // OPTIONAL MATCH edge into each Union branch, losing LEFT JOIN semantics
            // entirely (no anchor CTE, no JOIN — the union of the edge's origin/dest
            // property variants was emitted as the top-level query, referencing an
            // alias never introduced in FROM: invalid SQL, #506).
            if let LogicalPlan::Union(union) = &new_right {
                let skip = gr.is_optional.unwrap_or(false) && is_all_denormalized_nodes(union);

                if skip {
                    log::info!(
                        "🔀 UnionDistribution: SKIPPING GraphRel '{}' distribution over denormalized OPTIONAL Union (right side) — preserving LEFT JOIN semantics (#506)",
                        gr.alias
                    );
                    // Keep the Union as-is; wrap in GraphRel without distributing
                } else {
                    log::debug!(
                        "🔀 UnionDistribution: distributing GraphRel '{}' over right Union ({} branches)",
                        gr.alias,
                        union.inputs.len()
                    );
                    let left = Arc::new(new_left);
                    let center = Arc::new(new_center);
                    return distribute_over_union(union, |branch| {
                        LogicalPlan::GraphRel(graph_rel_with(
                            gr,
                            left.clone(),
                            center.clone(),
                            branch,
                        ))
                    });
                }
            }

            LogicalPlan::GraphRel(graph_rel_with(
                gr,
                Arc::new(new_left),
                Arc::new(new_center),
                Arc::new(new_right),
            ))
        }

        LogicalPlan::CartesianProduct(cp) => {
            let new_left = distribute_union_impl(&cp.left, depth + 1);
            let new_right = distribute_union_impl(&cp.right, depth + 1);

            // CP(left, Union(br0, br1)) → Union(CP(left, br0), CP(left, br1))
            if let LogicalPlan::Union(union) = &new_right {
                log::debug!(
                    "🔀 UnionDistribution: distributing CP over right Union ({} branches)",
                    union.inputs.len()
                );
                let left = Arc::new(new_left);
                return distribute_over_union(union, |branch| {
                    LogicalPlan::CartesianProduct(CartesianProduct {
                        left: left.clone(),
                        right: branch,
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    })
                });
            }
            // CP(Union(br0, br1), right) → Union(CP(br0, right), CP(br1, right))
            // EXCEPTION: Skip distribution when:
            // - The CartesianProduct is optional (OPTIONAL MATCH pattern)
            // - The Union is a denormalized standalone node scan
            // Distribution would push the OPTIONAL MATCH into each Union branch,
            // losing LEFT JOIN semantics (every branch would scan the edge table
            // directly, making all airports appear to have flights).
            if let LogicalPlan::Union(union) = &new_left {
                if cp.is_optional && is_all_denormalized_nodes(union) {
                    log::info!(
                        "🔀 UnionDistribution: SKIPPING distribution of OPTIONAL CP over denormalized Union ({} branches) — preserving LEFT JOIN semantics",
                        union.inputs.len()
                    );
                    // Don't distribute; keep the CartesianProduct intact
                } else {
                    log::debug!(
                        "🔀 UnionDistribution: distributing CP over left Union ({} branches)",
                        union.inputs.len()
                    );
                    let right = Arc::new(new_right);
                    return distribute_over_union(union, |branch| {
                        LogicalPlan::CartesianProduct(CartesianProduct {
                            left: branch,
                            right: right.clone(),
                            is_optional: cp.is_optional,
                            join_condition: cp.join_condition.clone(),
                        })
                    });
                }
            }

            LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            })
        }

        LogicalPlan::Filter(f) => {
            let new_input = distribute_union_impl(&f.input, depth + 1);
            // If input became Union after distribution, distribute Filter over it
            if let LogicalPlan::Union(union) = &new_input {
                log::debug!(
                    "🔀 UnionDistribution: distributing Filter over Union ({} branches)",
                    union.inputs.len()
                );
                return distribute_over_union(union, |branch| {
                    // #530: each branch may be a denormalized node scan with its OWN
                    // role-specific property mapping (e.g. origin/dest) — remap the
                    // predicate through THAT branch's mapping rather than cloning the
                    // raw, unmapped Cypher property name unchanged into every branch.
                    // No-op for non-denormalized branches (see helper's own doc).
                    let predicate =
                        crate::query_planner::logical_expr::expression_rewriter::remap_predicate_for_denorm_union_branch(
                            &f.predicate,
                            &branch,
                        );
                    LogicalPlan::Filter(Filter {
                        input: branch,
                        predicate,
                    })
                });
            }
            LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: f.predicate.clone(),
            })
        }

        LogicalPlan::GraphNode(gn) => {
            let new_input = distribute_union_impl(&gn.input, depth + 1);
            // If input became Union, distribute GraphNode over it
            if let LogicalPlan::Union(union) = &new_input {
                log::debug!(
                    "🔀 UnionDistribution: distributing GraphNode '{}' over Union",
                    gn.alias
                );
                return distribute_over_union(union, |branch| {
                    LogicalPlan::GraphNode(GraphNode {
                        input: branch,
                        alias: gn.alias.clone(),
                        label: gn.label.clone(),
                        projected_columns: gn.projected_columns.clone(),
                        is_denormalized: gn.is_denormalized,
                        node_types: gn.node_types.clone(),
                    })
                });
            }
            LogicalPlan::GraphNode(GraphNode {
                input: Arc::new(new_input),
                alias: gn.alias.clone(),
                label: gn.label.clone(),
                projected_columns: gn.projected_columns.clone(),
                is_denormalized: gn.is_denormalized,
                node_types: gn.node_types.clone(),
            })
        }

        // Write variants — left untouched (byte-identical to the pre-migration
        // `other => other.clone()` catch-all, which was a hard STOP and never
        // recursed into these). The recursing default below would descend into
        // their `.input` (a read pipeline that CAN hold a Union, e.g.
        // `MATCH (a)-[:R]-(b) WITH a,b CREATE (a)-[:R2]->(b)`), silently
        // distributing it and diverging from main. Not constructible as a live
        // divergence today only because CREATE support is independently broken;
        // excluded here so repairing CREATE can't arm the landmine. Mirrors the
        // identical exclusion in `scoping_with_collapse::collapse_recursive`
        // (commit aebd43a4).
        LogicalPlan::Create(_)
        | LogicalPlan::SetProperties(_)
        | LogicalPlan::Delete(_)
        | LogicalPlan::Remove(_) => plan.clone(),

        // All other nodes (Projection, GroupBy, OrderBy, Limit, Skip, Unwind,
        // WithClause, Cte, Union, leaves, ...): no Union-distribution logic
        // applies AT this node itself (GroupBy/Projection aggregation must
        // apply to the combined result, not be pushed into each branch), but
        // any buried Union further down their children must still be found
        // and distributed. Uses the exhaustive `LogicalPlan::map_children()`
        // API to recurse into every child uniformly instead of a hand-picked
        // list of wrapper arms — a prior hand-picked list here was exactly
        // the kind of drift BUG1/BUG2 in this same walker-inventory pass
        // flagged (variants silently skipped instead of descended into).
        // Leaves (Empty, PageRank, a childless ViewScan, ...) round-trip to
        // an equivalent clone via `map_children`, matching the old
        // `other => other.clone()` catch-all exactly.
        other => other.map_children(|child| distribute_union_impl(child, depth + 1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::Direction;
    use crate::query_planner::logical_plan::{GraphNode, ProjectionItem, WithClause};

    fn leaf_node(alias: &str) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: alias.to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        }))
    }

    fn union_of(branches: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Union(Union {
            inputs: branches,
            union_type: crate::query_planner::logical_plan::UnionType::All,
            is_cypher_union: false,
        }))
    }

    fn graph_rel(alias: &str, left: Arc<LogicalPlan>, right: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center: Arc::new(LogicalPlan::Empty),
            right,
            alias: alias.to_string(),
            direction: Direction::Outgoing,
            left_connection: "p".to_string(),
            right_connection: "x".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: None,
            is_optional: None,
            anchor_connection: None,
            cte_references: Default::default(),
            pattern_combinations: None,
            was_undirected: None,
            match_clause_index: 0, // #586 (synthetic/test)
            optional_anchor_where: None,
        }))
    }

    /// Regression for BUG2: the old `has_any_union` helper (used internally by
    /// `has_buried_union` to check a GraphRel/CartesianProduct connection's
    /// subtree) had no `WithClause` arm, unlike `has_buried_union` itself.
    /// A GraphRel whose immediate left child is a WithClause wrapping a
    /// buried Union (`GraphRel(left=WithClause(input=Union(...)), ...)`) —
    /// the natural shape for `WITH ... MATCH (a)-[:R]-(b)` chains — was
    /// silently invisible to the pass: `has_buried_union` would return
    /// `false` and `distribute_union` would never run for that Union.
    ///
    /// Fixed by removing the duplicate `has_any_union` walker entirely and
    /// delegating to the already-exhaustive `LogicalPlan::has_union_anywhere()`.
    #[test]
    fn union_buried_two_hops_below_with_clause_is_detected() {
        let buried_union = union_of(vec![leaf_node("b_fwd"), leaf_node("b_rev")]);
        let with_clause = Arc::new(LogicalPlan::WithClause(WithClause {
            input: buried_union,
            items: vec![ProjectionItem {
                expression: crate::query_planner::logical_expr::LogicalExpr::TableAlias(
                    crate::query_planner::logical_expr::TableAlias("p".to_string()),
                ),
                col_alias: None,
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["p".to_string()],
            cte_name: Some("with_p_cte_0".to_string()),
            cte_references: Default::default(),
            pattern_comprehensions: Vec::new(),
        }));

        let rel = graph_rel("t1", with_clause, leaf_node("x"));

        assert!(
            has_buried_union(&rel),
            "a Union nested inside a GraphRel's WithClause child must be \
             detected as buried — this is the exact live-repro shape for \
             `WITH ... MATCH (a)-[:R]-(b)`"
        );
    }

    /// Regression for BUG2's `has_buried_union` gap: a Union at the TOP of
    /// the checked subtree previously had no wrapper arm, so a buried union
    /// nested inside one of ITS OWN branches (e.g. a UNION arm whose body is
    /// itself `WITH ... MATCH (a)-[:R]-(b)-...`) was never found.
    #[test]
    fn union_branch_containing_its_own_buried_union_is_detected() {
        let inner_buried = union_of(vec![leaf_node("b_fwd"), leaf_node("b_rev")]);
        let branch_with_buried_union = graph_rel("t1", inner_buried, leaf_node("x"));
        let clean_branch = leaf_node("y");

        let top_union = LogicalPlan::Union(Union {
            inputs: vec![branch_with_buried_union, clean_branch],
            union_type: crate::query_planner::logical_plan::UnionType::All,
            is_cypher_union: true,
        });

        assert!(
            has_buried_union(&top_union),
            "a buried union nested inside one UNION branch must be detected \
             even though the top-level node is itself a Union"
        );
    }

    #[test]
    fn no_union_anywhere_is_not_buried() {
        let rel = graph_rel("t1", leaf_node("p"), leaf_node("x"));
        assert!(
            !has_buried_union(&rel),
            "a plan with no Union anywhere must never be reported as buried"
        );
    }

    /// Regression (Phase 1 Slice2 review): `distribute_union_impl`'s migrated
    /// default arm (`other => other.map_children(...)`) must NOT recurse into
    /// write-op variants' inputs. The pre-migration catch-all
    /// (`other => other.clone()`) was a hard STOP that never touched
    /// Create/SetProperties/Delete/Remove. `Create.input` IS a reachable read
    /// pipeline that can hold a Union (e.g.
    /// `MATCH (a)-[:R]-(b) WITH a,b CREATE (a)-[:R2]->(b)` — the undirected
    /// edge produces a Union under the Create's input). Under the recursing
    /// default that Union would be distributed (the Create's subtree rewritten);
    /// this test pins the byte-identical behavior — the write-op input is
    /// returned untouched. Mirrors
    /// `scoping_with_collapse::test_collapse_does_not_recurse_into_write_op_input`
    /// (commit aebd43a4). Verified to FAIL on the pre-fix recursing default.
    #[test]
    fn distribute_does_not_recurse_into_write_op_input() {
        use crate::query_planner::logical_plan::Create;

        // A GraphRel whose left child is a bare Union — this IS a "buried
        // union" shape that distribute_union_impl WOULD rewrite if it reached
        // it (distributing the GraphRel over the Union's branches).
        let buried = union_of(vec![leaf_node("b_fwd"), leaf_node("b_rev")]);
        let rel_with_buried_union = graph_rel("t1", buried, leaf_node("x"));

        // Sanity: confirm that same subtree, when NOT under a write op, DOES
        // get rewritten into a top-level Union — otherwise this test proves
        // nothing about the exclusion.
        let distributed_bare = distribute_union_impl(&rel_with_buried_union, 0);
        assert!(
            matches!(distributed_bare, LogicalPlan::Union(_)),
            "precondition: a GraphRel over a bare Union must distribute into a \
             top-level Union — otherwise the write-op exclusion below is untested"
        );

        // Now wrap that exact subtree as a Create's input.
        let create = LogicalPlan::Create(Create {
            input: Arc::new(rel_with_buried_union.as_ref().clone()),
            patterns: vec![],
        });

        let result = distribute_union_impl(&create, 0);

        // The Create wrapper is preserved AND its `.input` is returned byte-for-
        // byte unchanged (still the GraphRel-over-Union, NOT distributed).
        match result {
            LogicalPlan::Create(c) => assert_eq!(
                c.input.as_ref(),
                rel_with_buried_union.as_ref(),
                "Create.input must be returned unchanged — distribute_union_impl \
                 must not recurse into write-op inputs (byte-identical to the \
                 pre-migration `other => other.clone()` hard stop)"
            ),
            other => panic!("expected Create wrapper to be preserved, got {other:?}"),
        }
    }
}
