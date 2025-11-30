//! Bidirectional Pattern to UNION ALL Transformation
//!
//! This optimizer pass transforms undirected relationship patterns `(a)-[r]-(b)`
//! from a single GraphRel with Direction::Either into a Union of two directed patterns:
//! - One for outgoing: (a)-[r]->(b)
//! - One for incoming: (a)<-[r]-(b)
//!
//! This is necessary because ClickHouse doesn't handle OR conditions in JOINs correctly,
//! leading to missing rows. UNION ALL ensures both directions are properly matched.

use std::sync::Arc;

use crate::graph_catalog::GraphSchema;
use crate::query_planner::analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult};
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::{
    Filter, GraphNode, GraphRel, GroupBy, LogicalPlan, Projection, Union, UnionType,
};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::query_planner::transformed::Transformed;

pub struct BidirectionalUnion;

impl AnalyzerPass for BidirectionalUnion {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        transform_bidirectional(&logical_plan, plan_ctx)
    }
}

fn transform_bidirectional(
    plan: &Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if this is a bidirectional pattern
            if graph_rel.direction == Direction::Either {
                eprintln!(
                    "🔄 BidirectionalUnion: Transforming Either pattern for rel '{}' into UNION ALL",
                    graph_rel.alias
                );

                // Transform left and right subtrees first
                let transformed_left = transform_bidirectional(&graph_rel.left, plan_ctx)?;
                let left = match transformed_left {
                    Transformed::Yes(new_plan) => new_plan,
                    Transformed::No(plan) => plan,
                };

                let transformed_center = transform_bidirectional(&graph_rel.center, plan_ctx)?;
                let center = match transformed_center {
                    Transformed::Yes(new_plan) => new_plan,
                    Transformed::No(plan) => plan,
                };

                let transformed_right = transform_bidirectional(&graph_rel.right, plan_ctx)?;
                let right = match transformed_right {
                    Transformed::Yes(new_plan) => new_plan,
                    Transformed::No(plan) => plan,
                };

                // Create outgoing branch: (a)-[r]->(b)
                let outgoing_rel = GraphRel {
                    left: left.clone(),
                    center: center.clone(),
                    right: right.clone(),
                    alias: graph_rel.alias.clone(),
                    direction: Direction::Outgoing,
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
                };

                // Create incoming branch: (a)<-[r]-(b)
                // For incoming, we swap the semantics: a is on the to_id side, b is on from_id side
                let incoming_rel = GraphRel {
                    left: left.clone(),
                    center: center.clone(),
                    right: right.clone(),
                    alias: graph_rel.alias.clone(),
                    direction: Direction::Incoming,
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
                };

                // Create Union of both branches
                let union = Union {
                    inputs: vec![
                        Arc::new(LogicalPlan::GraphRel(outgoing_rel)),
                        Arc::new(LogicalPlan::GraphRel(incoming_rel)),
                    ],
                    union_type: UnionType::All,
                };

                eprintln!(
                    "🔄 BidirectionalUnion: Created UNION ALL for rel '{}'",
                    graph_rel.alias
                );

                Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(union))))
            } else {
                // Not bidirectional, just recurse into children
                let transformed_left = transform_bidirectional(&graph_rel.left, plan_ctx)?;
                let transformed_center = transform_bidirectional(&graph_rel.center, plan_ctx)?;
                let transformed_right = transform_bidirectional(&graph_rel.right, plan_ctx)?;

                if matches!(
                    (&transformed_left, &transformed_center, &transformed_right),
                    (Transformed::No(_), Transformed::No(_), Transformed::No(_))
                ) {
                    Ok(Transformed::No(plan.clone()))
                } else {
                    let new_rel = GraphRel {
                        left: match transformed_left {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        center: match transformed_center {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        right: match transformed_right {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        ..graph_rel.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                }
            }
        }

        LogicalPlan::Projection(proj) => {
            let transformed = transform_bidirectional(&proj.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    // If the input transformed into a Union (from bidirectional pattern),
                    // we need to push the Projection down into each Union branch
                    if let LogicalPlan::Union(union) = new_input.as_ref() {
                        eprintln!(
                            "🔄 BidirectionalUnion: Pushing Projection into {} Union branches",
                            union.inputs.len()
                        );
                        
                        // Create a Projection for each branch
                        let projected_branches: Vec<Arc<LogicalPlan>> = union.inputs.iter().map(|branch| {
                            Arc::new(LogicalPlan::Projection(Projection {
                                input: branch.clone(),
                                items: proj.items.clone(),
                                kind: proj.kind.clone(),
                                distinct: proj.distinct,
                            }))
                        }).collect();
                        
                        // Return Union with projected branches
                        let new_union = Union {
                            inputs: projected_branches,
                            union_type: union.union_type.clone(),
                        };
                        Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(new_union))))
                    } else {
                        // Not a Union, just wrap with the same Projection
                        let new_proj = Projection {
                            input: new_input,
                            items: proj.items.clone(),
                            kind: proj.kind.clone(),
                            distinct: proj.distinct,
                        };
                        Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                            new_proj,
                        ))))
                    }
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Filter(filter) => {
            let transformed = transform_bidirectional(&filter.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_filter = Filter {
                        input: new_input,
                        predicate: filter.predicate.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Union(union) => {
            let mut any_transformed = false;
            let new_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .map(|input| {
                    let result = transform_bidirectional(input, plan_ctx);
                    match result {
                        Ok(Transformed::Yes(new_plan)) => {
                            any_transformed = true;
                            new_plan
                        }
                        Ok(Transformed::No(plan)) => plan,
                        Err(_) => input.clone(), // On error, keep original
                    }
                })
                .collect();

            if any_transformed {
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(Union {
                    inputs: new_inputs,
                    union_type: union.union_type.clone(),
                }))))
            } else {
                Ok(Transformed::No(plan.clone()))
            }
        }

        LogicalPlan::Limit(limit) => {
            let transformed = transform_bidirectional(&limit.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: new_input,
                        count: limit.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::OrderBy(order_by) => {
            let transformed = transform_bidirectional(&order_by.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_order_by = crate::query_planner::logical_plan::OrderBy {
                        input: new_input,
                        items: order_by.items.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(
                        new_order_by,
                    ))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Skip(skip) => {
            let transformed = transform_bidirectional(&skip.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_skip = crate::query_planner::logical_plan::Skip {
                        input: new_input,
                        count: skip.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::GraphNode(graph_node) => {
            let transformed = transform_bidirectional(&graph_node.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_node = GraphNode {
                        input: new_input,
                        alias: graph_node.alias.clone(),
                        label: graph_node.label.clone(),
                        is_denormalized: graph_node.is_denormalized,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(new_node))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::GroupBy(group_by) => {
            // Transform bidirectional patterns inside GroupBy
            let transformed = transform_bidirectional(&group_by.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_group_by = GroupBy {
                        input: new_input,
                        expressions: group_by.expressions.clone(),
                        having_clause: group_by.having_clause.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_group_by))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Unwind(unwind) => {
            let transformed = transform_bidirectional(&unwind.input, plan_ctx)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_unwind = crate::query_planner::logical_plan::Unwind {
                        input: new_input,
                        expression: unwind.expression.clone(),
                        alias: unwind.alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_unwind))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        // Leaf nodes - no transformation needed
        LogicalPlan::ViewScan(_)
        | LogicalPlan::Empty
        | LogicalPlan::PageRank(_)
        | LogicalPlan::GraphJoins(_)
        | LogicalPlan::Scan(_)
        | LogicalPlan::Cte(_) => Ok(Transformed::No(plan.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_plan::ViewScan;
    use std::collections::HashMap;

    #[test]
    fn test_bidirectional_detection() {
        // Create a simple bidirectional GraphRel
        let left_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "users".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let center_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "follows".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let right_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "users".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let left_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: left_scan,
            alias: "a".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
        }));

        let right_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: right_scan,
            alias: "b".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
        }));

        let graph_rel = GraphRel {
            left: left_node,
            center: center_scan,
            right: right_node,
            alias: "r".to_string(),
            direction: Direction::Either,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["FOLLOWS".to_string()]),
            is_optional: None,
            anchor_connection: None,
        };

        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));
        let mut plan_ctx = PlanCtx::default();

        let result = transform_bidirectional(&plan, &mut plan_ctx);
        assert!(result.is_ok());

        match result.unwrap() {
            Transformed::Yes(new_plan) => {
                // Should be a Union now
                match new_plan.as_ref() {
                    LogicalPlan::Union(union) => {
                        assert_eq!(union.inputs.len(), 2);
                        assert!(matches!(union.union_type, UnionType::All));

                        // Check first branch is Outgoing
                        if let LogicalPlan::GraphRel(rel) = union.inputs[0].as_ref() {
                            assert_eq!(rel.direction, Direction::Outgoing);
                        } else {
                            panic!("Expected GraphRel in first union branch");
                        }

                        // Check second branch is Incoming
                        if let LogicalPlan::GraphRel(rel) = union.inputs[1].as_ref() {
                            assert_eq!(rel.direction, Direction::Incoming);
                        } else {
                            panic!("Expected GraphRel in second union branch");
                        }
                    }
                    _ => panic!("Expected Union plan"),
                }
            }
            Transformed::No(_) => panic!("Expected transformation to occur"),
        }
    }
}
