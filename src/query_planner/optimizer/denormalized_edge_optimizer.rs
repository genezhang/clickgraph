/// Optimizer pass that marks denormalized nodes in the logical plan
/// 
/// This optimizer handles all 4 edge table patterns:
/// 1. Traditional: Both nodes have separate tables → No changes
/// 2. FullyDenormalized: Both nodes on edge table → Mark both as denormalized
/// 3. Mixed (from_denormalized=true): Left node on edge → Mark left as denormalized
/// 4. Mixed (to_denormalized=true): Right node on edge → Mark right as denormalized
/// 
/// After this pass, GraphNode.is_denormalized flag is set correctly,
/// allowing RenderPlan to skip creating CTEs/JOINs for denormalized nodes.

use std::sync::Arc;

use crate::query_planner::{
    logical_plan::{GraphNode, GraphRel, LogicalPlan},
    optimizer::{
        errors::{OptimizerError, Pass},
        optimizer_pass::{OptimizerPass, OptimizerResult},
    },
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct DenormalizedEdgeOptimizer;

impl DenormalizedEdgeOptimizer {
    pub fn new() -> Self {
        Self
    }

    /// Recursively walk the plan tree and mark denormalized nodes
    fn mark_denormalized_nodes(
        plan: Arc<LogicalPlan>,
        plan_ctx: &PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if left/right nodes are denormalized (already detected by analyzer)
                let left_is_denorm = Self::is_node_denormalized(&graph_rel.left, plan_ctx);
                let right_is_denorm = Self::is_node_denormalized(&graph_rel.right, plan_ctx);

                log::debug!(
                    "DenormalizedEdgeOptimizer: Checking GraphRel - left_denorm={}, right_denorm={}",
                    left_is_denorm,
                    right_is_denorm
                );

                // Recursively process children
                let left = Self::mark_denormalized_nodes(graph_rel.left.clone(), plan_ctx)?;
                let center = Self::mark_denormalized_nodes(graph_rel.center.clone(), plan_ctx)?;
                let right = Self::mark_denormalized_nodes(graph_rel.right.clone(), plan_ctx)?;

                // Mark left node if denormalized
                let new_left = if left_is_denorm {
                    Self::mark_graph_node_as_denormalized(left.get_plan())?
                } else {
                    left.get_plan()
                };

                // Mark right node if denormalized
                let new_right = if right_is_denorm {
                    Self::mark_graph_node_as_denormalized(right.get_plan())?
                } else {
                    right.get_plan()
                };

                // Reconstruct GraphRel with updated children
                let new_graph_rel = GraphRel {
                    left: new_left,
                    center: center.get_plan(),
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
                };

                Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(
                    new_graph_rel,
                ))))
            }

            LogicalPlan::GraphNode(graph_node) => {
                // Recursively process input
                let input = Self::mark_denormalized_nodes(graph_node.input.clone(), plan_ctx)?;

                let new_graph_node = GraphNode {
                    input: input.get_plan(),
                    alias: graph_node.alias.clone(),
                    is_denormalized: graph_node.is_denormalized, // Preserve existing value
                };

                Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                    new_graph_node,
                ))))
            }

            LogicalPlan::Filter(filter) => {
                let input = Self::mark_denormalized_nodes(filter.input.clone(), plan_ctx)?;
                let new_filter = crate::query_planner::logical_plan::Filter {
                    input: input.get_plan(),
                    predicate: filter.predicate.clone(),
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(
                    new_filter,
                ))))
            }

            LogicalPlan::Projection(proj) => {
                let input = Self::mark_denormalized_nodes(proj.input.clone(), plan_ctx)?;
                let new_proj = crate::query_planner::logical_plan::Projection {
                    input: input.get_plan(),
                    items: proj.items.clone(),
                    kind: proj.kind.clone(),
                    distinct: proj.distinct,
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                    new_proj,
                ))))
            }

            LogicalPlan::GraphJoins(joins) => {
                let input = Self::mark_denormalized_nodes(joins.input.clone(), plan_ctx)?;
                let new_joins = crate::query_planner::logical_plan::GraphJoins {
                    input: input.get_plan(),
                    joins: joins.joins.clone(),
                    optional_aliases: joins.optional_aliases.clone(),
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(
                    new_joins,
                ))))
            }

            LogicalPlan::GroupBy(gb) => {
                let input = Self::mark_denormalized_nodes(gb.input.clone(), plan_ctx)?;
                let new_gb = crate::query_planner::logical_plan::GroupBy {
                    input: input.get_plan(),
                    expressions: gb.expressions.clone(),
                    having_clause: gb.having_clause.clone(),
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_gb))))
            }

            LogicalPlan::OrderBy(order) => {
                let input = Self::mark_denormalized_nodes(order.input.clone(), plan_ctx)?;
                let new_order = crate::query_planner::logical_plan::OrderBy {
                    input: input.get_plan(),
                    items: order.items.clone(),
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(
                    new_order,
                ))))
            }

            LogicalPlan::Skip(skip) => {
                let input = Self::mark_denormalized_nodes(skip.input.clone(), plan_ctx)?;
                let new_skip = crate::query_planner::logical_plan::Skip {
                    input: input.get_plan(),
                    count: skip.count,
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
            }

            LogicalPlan::Limit(limit) => {
                let input = Self::mark_denormalized_nodes(limit.input.clone(), plan_ctx)?;
                let new_limit = crate::query_planner::logical_plan::Limit {
                    input: input.get_plan(),
                    count: limit.count,
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
            }

            // Leaf nodes - no transformation needed
            LogicalPlan::ViewScan(_) | LogicalPlan::Scan(_) | LogicalPlan::Empty => {
                Ok(Transformed::No(plan))
            }

            // Other nodes - pass through (shouldn't encounter in denormalized edge context)
            LogicalPlan::Cte(_) | LogicalPlan::Union(_) | LogicalPlan::PageRank(_) => {
                Ok(Transformed::No(plan))
            }
        }
    }

    /// Check if a node alias is in the denormalized_aliases map
    fn is_node_denormalized(node_plan: &Arc<LogicalPlan>, plan_ctx: &PlanCtx) -> bool {
        if let LogicalPlan::GraphNode(graph_node) = node_plan.as_ref() {
            let is_denorm = plan_ctx.is_denormalized_alias(&graph_node.alias);
            if is_denorm {
                log::info!(
                    "✓ Optimizer: Node '{}' is denormalized",
                    graph_node.alias
                );
            }
            is_denorm
        } else {
            false
        }
    }

    /// Mark a GraphNode as denormalized by setting its flag
    fn mark_graph_node_as_denormalized(
        plan: Arc<LogicalPlan>,
    ) -> OptimizerResult<Arc<LogicalPlan>> {
        match plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                log::info!(
                    "✓ Optimizer: Marking node '{}' as denormalized",
                    graph_node.alias
                );

                let new_graph_node = GraphNode {
                    input: graph_node.input.clone(),
                    alias: graph_node.alias.clone(),
                    is_denormalized: true, // ← Set the flag!
                };

                Ok(Arc::new(LogicalPlan::GraphNode(new_graph_node)))
            }
            _ => {
                // Not a GraphNode - return as-is
                Ok(plan)
            }
        }
    }
}

impl OptimizerPass for DenormalizedEdgeOptimizer {
    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        log::debug!("DenormalizedEdgeOptimizer: Starting optimization");
        log::debug!(
            "DenormalizedEdgeOptimizer: Found {} denormalized aliases in plan_ctx",
            plan_ctx.get_denormalized_aliases().len()
        );

        // Only run if we have denormalized aliases
        if plan_ctx.get_denormalized_aliases().is_empty() {
            log::debug!("DenormalizedEdgeOptimizer: No denormalized aliases, skipping");
            return Ok(Transformed::No(plan));
        }

        // Walk tree and mark denormalized nodes
        let result = Self::mark_denormalized_nodes(plan, plan_ctx)?;

        if result.is_yes() {
            log::info!("✓ DenormalizedEdgeOptimizer: Successfully marked denormalized nodes");
        } else {
            log::debug!("DenormalizedEdgeOptimizer: No changes made");
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_plan::ViewScan;
    use crate::query_planner::logical_expr::Direction;
    use std::collections::HashMap;

    #[test]
    fn test_marks_fully_denormalized_nodes() {
        // Create a plan: GraphRel with left and right nodes both denormalized
        let left_view = Arc::new(ViewScan::new(
            "flights".to_string(),
            None,
            HashMap::new(),
            "code".to_string(),
            vec![],
            vec![],
        ));
        let left_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(left_view)),
            alias: "origin".to_string(),
            is_denormalized: false, // Not yet marked
        }));

        let center_view = Arc::new(ViewScan::new_relationship(
            "flights".to_string(),
            None,
            HashMap::new(),
            "Origin".to_string(),
            vec![],
            vec![],
            "Origin".to_string(),
            "Dest".to_string(),
        ));
        let center = Arc::new(LogicalPlan::ViewScan(center_view));

        let right_view = Arc::new(ViewScan::new(
            "flights".to_string(),
            None,
            HashMap::new(),
            "code".to_string(),
            vec![],
            vec![],
        ));
        let right_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(right_view)),
            alias: "dest".to_string(),
            is_denormalized: false, // Not yet marked
        }));

        let graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
            left: left_node,
            center,
            right: right_node,
            alias: "f".to_string(),
            direction: Direction::Right,
            left_connection: "Origin".to_string(),
            right_connection: "Dest".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: None,
            is_optional: None,
        }));

        // Create plan_ctx with denormalized aliases (simulating analyzer output)
        let mut plan_ctx = PlanCtx::new(None);
        plan_ctx.register_denormalized_alias(
            "origin".to_string(),
            "f".to_string(),
            true,
            "Airport".to_string(),
        );
        plan_ctx.register_denormalized_alias(
            "dest".to_string(),
            "f".to_string(),
            false,
            "Airport".to_string(),
        );

        // Run optimizer
        let optimizer = DenormalizedEdgeOptimizer::new();
        let result = optimizer.optimize(graph_rel, &mut plan_ctx).unwrap();

        // Verify both nodes are marked
        if let LogicalPlan::GraphRel(optimized_rel) = result.get_plan().as_ref() {
            if let LogicalPlan::GraphNode(left) = optimized_rel.left.as_ref() {
                assert!(
                    left.is_denormalized,
                    "Left node should be marked as denormalized"
                );
            } else {
                panic!("Left should be GraphNode");
            }

            if let LogicalPlan::GraphNode(right) = optimized_rel.right.as_ref() {
                assert!(
                    right.is_denormalized,
                    "Right node should be marked as denormalized"
                );
            } else {
                panic!("Right should be GraphNode");
            }
        } else {
            panic!("Result should be GraphRel");
        }
    }

    #[test]
    fn test_skips_traditional_nodes() {
        // Create a plan with traditional (non-denormalized) nodes
        let left_view = Arc::new(ViewScan::new(
            "airports".to_string(), // Different table!
            None,
            HashMap::new(),
            "code".to_string(),
            vec![],
            vec![],
        ));
        let left_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(left_view)),
            alias: "origin".to_string(),
            is_denormalized: false,
        }));

        let center_view = Arc::new(ViewScan::new_relationship(
            "flights".to_string(),
            None,
            HashMap::new(),
            "Origin".to_string(),
            vec![],
            vec![],
            "Origin".to_string(),
            "Dest".to_string(),
        ));
        let center = Arc::new(LogicalPlan::ViewScan(center_view));

        let right_view = Arc::new(ViewScan::new(
            "airports".to_string(), // Different table!
            None,
            HashMap::new(),
            "code".to_string(),
            vec![],
            vec![],
        ));
        let right_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(right_view)),
            alias: "dest".to_string(),
            is_denormalized: false,
        }));

        let graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
            left: left_node,
            center,
            right: right_node,
            alias: "f".to_string(),
            direction: Direction::Right,
            left_connection: "Origin".to_string(),
            right_connection: "Dest".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: None,
            is_optional: None,
        }));

        // Create plan_ctx WITHOUT denormalized aliases (traditional pattern)
        let mut plan_ctx = PlanCtx::new(None);

        // Run optimizer
        let optimizer = DenormalizedEdgeOptimizer::new();
        let result = optimizer.optimize(graph_rel, &mut plan_ctx).unwrap();

        // Verify nodes are NOT marked (traditional pattern)
        if let LogicalPlan::GraphRel(optimized_rel) = result.get_plan().as_ref() {
            if let LogicalPlan::GraphNode(left) = optimized_rel.left.as_ref() {
                assert!(
                    !left.is_denormalized,
                    "Traditional left node should not be marked"
                );
            }

            if let LogicalPlan::GraphNode(right) = optimized_rel.right.as_ref() {
                assert!(
                    !right.is_denormalized,
                    "Traditional right node should not be marked"
                );
            }
        }
    }
}
