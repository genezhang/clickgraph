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
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::GraphRel(GraphRel {
                                left: branch.clone(),
                                center: Arc::new(new_center.clone()),
                                right: Arc::new(new_right.clone()),
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
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
                });
            }

            // If right became Union after distribution, distribute GraphRel over it
            // This happens when undirected edges (Union from BidirectionalUnion) appear
            // on the right side of a GraphRel (e.g., the friend/person end of a chain)
            if let LogicalPlan::Union(union) = &new_right {
                log::debug!(
                    "🔀 UnionDistribution: distributing GraphRel '{}' over right Union ({} branches)",
                    gr.alias,
                    union.inputs.len()
                );
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::GraphRel(GraphRel {
                                left: Arc::new(new_left.clone()),
                                center: Arc::new(new_center.clone()),
                                right: branch.clone(),
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
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
                });
            }

            LogicalPlan::GraphRel(GraphRel {
                left: Arc::new(new_left),
                center: Arc::new(new_center),
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
            })
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
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                                left: Arc::new(new_left.clone()),
                                right: branch.clone(),
                                is_optional: cp.is_optional,
                                join_condition: cp.join_condition.clone(),
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
                });
            }
            // CP(Union(br0, br1), right) → Union(CP(br0, right), CP(br1, right))
            if let LogicalPlan::Union(union) = &new_left {
                log::debug!(
                    "🔀 UnionDistribution: distributing CP over left Union ({} branches)",
                    union.inputs.len()
                );
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                                left: branch.clone(),
                                right: Arc::new(new_right.clone()),
                                is_optional: cp.is_optional,
                                join_condition: cp.join_condition.clone(),
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
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
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::Filter(Filter {
                                input: branch.clone(),
                                predicate: f.predicate.clone(),
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
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
                return LogicalPlan::Union(Union {
                    inputs: union
                        .inputs
                        .iter()
                        .map(|branch| {
                            Arc::new(LogicalPlan::GraphNode(GraphNode {
                                input: branch.clone(),
                                alias: gn.alias.clone(),
                                label: gn.label.clone(),
                                projected_columns: gn.projected_columns.clone(),
                                is_denormalized: gn.is_denormalized,
                                node_types: gn.node_types.clone(),
                            }))
                        })
                        .collect(),
                    union_type: union.union_type.clone(),
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
