use std::sync::Arc;

use crate::query_planner::{
    logical_plan::{LogicalPlan, ViewScan},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

/// Optimizer pass that clears ViewScan.view_filter after filters have been moved to GraphRel
///
/// This pass runs AFTER FilterIntoGraphRel and removes redundant view_filter fields from
/// ViewScan nodes. After FilterIntoGraphRel consolidates all filters into GraphRel.where_predicate,
/// the view_filter fields become redundant and cause duplicate filter collection during rendering.
///
/// By clearing view_filter, we ensure filters are only collected from GraphRel.where_predicate,
/// preventing duplicates in the generated SQL WHERE clause.
pub struct CleanupViewScanFilters;

impl OptimizerPass for CleanupViewScanFilters {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::ViewScan(scan) => {
                // Clear view_filter - filters should come from GraphRel.where_predicate only
                if scan.view_filter.is_some() {
                    let new_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
                        view_filter: None,
                        ..scan.as_ref().clone()
                    })));
                    Transformed::Yes(new_scan)
                } else {
                    Transformed::No(logical_plan)
                }
            }
            
            // Recursively process all other node types
            LogicalPlan::Projection(proj) => {
                let input_tf = self.optimize(proj.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::Projection(
                            crate::query_planner::logical_plan::Projection {
                                input: new_input,
                                items: proj.items.clone(),
                                kind: proj.kind.clone(),
                                distinct: proj.distinct,
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::Filter(filter) => {
                let input_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::Filter(
                            crate::query_planner::logical_plan::Filter {
                                input: new_input,
                                predicate: filter.predicate.clone(),
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx)?;
                
                match (left_tf, center_tf, right_tf) {
                    (Transformed::No(_), Transformed::No(_), Transformed::No(_)) => {
                        Transformed::No(logical_plan)
                    }
                    (left, center, right) => {
                        let new_left = match left {
                            Transformed::Yes(l) => l,
                            Transformed::No(l) => l,
                        };
                        let new_center = match center {
                            Transformed::Yes(c) => c,
                            Transformed::No(c) => c,
                        };
                        let new_right = match right {
                            Transformed::Yes(r) => r,
                            Transformed::No(r) => r,
                        };
                        
                        Transformed::Yes(Arc::new(LogicalPlan::GraphRel(
                            crate::query_planner::logical_plan::GraphRel {
                                left: new_left,
                                center: new_center,
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
                                is_optional: graph_rel.is_optional.clone(),
                            },
                        )))
                    }
                }
            }
            
            LogicalPlan::GraphNode(graph_node) => {
                let input_tf = self.optimize(graph_node.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                            crate::query_planner::logical_plan::GraphNode {
                                input: new_input,
                                alias: graph_node.alias.clone(),
                                label: graph_node.label.clone(),
                                is_denormalized: graph_node.is_denormalized,
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::GroupBy(group_by) => {
                let input_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::GroupBy(
                            crate::query_planner::logical_plan::GroupBy {
                                input: new_input,
                                expressions: group_by.expressions.clone(),
                                having_clause: group_by.having_clause.clone(),
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::OrderBy(order_by) => {
                let input_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::OrderBy(
                            crate::query_planner::logical_plan::OrderBy {
                                input: new_input,
                                items: order_by.items.clone(),
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::Limit(limit) => {
                let input_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::Limit(
                            crate::query_planner::logical_plan::Limit {
                                input: new_input,
                                count: limit.count,
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::Skip(skip) => {
                let input_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::Skip(
                            crate::query_planner::logical_plan::Skip {
                                input: new_input,
                                count: skip.count,
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            LogicalPlan::GraphJoins(graph_joins) => {
                let input_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                match input_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(
                            crate::query_planner::logical_plan::GraphJoins {
                                input: new_input,
                                joins: graph_joins.joins.clone(),
                                optional_aliases: graph_joins.optional_aliases.clone(),
                                anchor_table: graph_joins.anchor_table.clone(),
                            },
                        )))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
            
            // Leaf nodes - no transformation needed
            LogicalPlan::Empty
            | LogicalPlan::Scan(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Cte(_) => Transformed::No(logical_plan),
        };

        Ok(transformed_plan)
    }
}
