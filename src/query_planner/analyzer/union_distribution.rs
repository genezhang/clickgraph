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
    CartesianProduct, Filter, GraphNode, GraphRel, GroupBy, LogicalPlan, Projection, Union,
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
fn has_buried_union(plan: &LogicalPlan) -> bool {
    match plan {
        // Union inside these data-processing nodes means it needs hoisting
        LogicalPlan::CartesianProduct(cp) => has_any_union(&cp.left) || has_any_union(&cp.right),
        LogicalPlan::GraphRel(gr) => {
            has_any_union(&gr.left) || has_any_union(&gr.center) || has_any_union(&gr.right)
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
        _ => false,
    }
}

/// Check if a plan tree contains any Union node.
fn has_any_union(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Union(_) => true,
        LogicalPlan::CartesianProduct(cp) => has_any_union(&cp.left) || has_any_union(&cp.right),
        LogicalPlan::GraphRel(gr) => {
            has_any_union(&gr.left) || has_any_union(&gr.right) || has_any_union(&gr.center)
        }
        LogicalPlan::GraphNode(gn) => has_any_union(&gn.input),
        LogicalPlan::Filter(f) => has_any_union(&f.input),
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
    })
}

/// Recursively distribute wrapping nodes (GraphRel, CartesianProduct, Filter)
/// over Union, hoisting Union upward. Stops at GroupBy/Projection boundaries.
fn distribute_union(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // First, recurse into children
            let new_left = distribute_union(&gr.left);
            let new_center = distribute_union(&gr.center);
            let new_right = distribute_union(&gr.right);

            // If left became Union after distribution, distribute GraphRel over it
            if let LogicalPlan::Union(union) = &new_left {
                log::debug!(
                    "🔀 UnionDistribution: distributing GraphRel '{}' over left Union ({} branches)",
                    gr.alias,
                    union.inputs.len()
                );
                let center = Arc::new(new_center);
                let right = Arc::new(new_right);
                return distribute_over_union(union, |branch| {
                    LogicalPlan::GraphRel(graph_rel_with(gr, branch, center.clone(), right.clone()))
                });
            }

            // If right became Union after distribution, distribute GraphRel over it
            if let LogicalPlan::Union(union) = &new_right {
                log::debug!(
                    "🔀 UnionDistribution: distributing GraphRel '{}' over right Union ({} branches)",
                    gr.alias,
                    union.inputs.len()
                );
                let left = Arc::new(new_left);
                let center = Arc::new(new_center);
                return distribute_over_union(union, |branch| {
                    LogicalPlan::GraphRel(graph_rel_with(gr, left.clone(), center.clone(), branch))
                });
            }

            LogicalPlan::GraphRel(graph_rel_with(
                gr,
                Arc::new(new_left),
                Arc::new(new_center),
                Arc::new(new_right),
            ))
        }

        LogicalPlan::CartesianProduct(cp) => {
            let new_left = distribute_union(&cp.left);
            let new_right = distribute_union(&cp.right);

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
            if let LogicalPlan::Union(union) = &new_left {
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

            LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            })
        }

        LogicalPlan::Filter(f) => {
            let new_input = distribute_union(&f.input);
            // If input became Union after distribution, distribute Filter over it
            if let LogicalPlan::Union(union) = &new_input {
                log::debug!(
                    "🔀 UnionDistribution: distributing Filter over Union ({} branches)",
                    union.inputs.len()
                );
                return distribute_over_union(union, |branch| {
                    LogicalPlan::Filter(Filter {
                        input: branch,
                        predicate: f.predicate.clone(),
                    })
                });
            }
            LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: f.predicate.clone(),
            })
        }

        LogicalPlan::GraphNode(gn) => {
            let new_input = distribute_union(&gn.input);
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

        // Wrapper nodes: recurse but do NOT distribute over Union
        // (GroupBy/Projection aggregation must apply to the combined result)
        LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection {
            input: Arc::new(distribute_union(&p.input)),
            items: p.items.clone(),
            distinct: p.distinct,
            pattern_comprehensions: p.pattern_comprehensions.clone(),
        }),
        LogicalPlan::GroupBy(gb) => LogicalPlan::GroupBy(GroupBy {
            input: Arc::new(distribute_union(&gb.input)),
            expressions: gb.expressions.clone(),
            having_clause: gb.having_clause.clone(),
            is_materialization_boundary: gb.is_materialization_boundary,
            exposed_alias: gb.exposed_alias.clone(),
        }),
        LogicalPlan::OrderBy(o) => {
            LogicalPlan::OrderBy(crate::query_planner::logical_plan::OrderBy {
                input: Arc::new(distribute_union(&o.input)),
                items: o.items.clone(),
            })
        }
        LogicalPlan::Limit(l) => LogicalPlan::Limit(crate::query_planner::logical_plan::Limit {
            input: Arc::new(distribute_union(&l.input)),
            count: l.count,
        }),
        LogicalPlan::Skip(s) => LogicalPlan::Skip(crate::query_planner::logical_plan::Skip {
            input: Arc::new(distribute_union(&s.input)),
            count: s.count,
        }),
        LogicalPlan::Unwind(u) => LogicalPlan::Unwind(crate::query_planner::logical_plan::Unwind {
            input: Arc::new(distribute_union(&u.input)),
            expression: u.expression.clone(),
            alias: u.alias.clone(),
            label: u.label.clone(),
            tuple_properties: u.tuple_properties.clone(),
        }),

        // Leaf/other nodes: no transformation needed
        other => other.clone(),
    }
}
