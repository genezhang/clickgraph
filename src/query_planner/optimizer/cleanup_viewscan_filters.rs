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
/// ViewScan nodes that are INSIDE GraphRel contexts. After FilterIntoGraphRel consolidates
/// all filters into GraphRel.where_predicate, the view_filter fields become redundant.
///
/// IMPORTANT: ViewScan nodes that are NOT inside a GraphRel (e.g., simple node-only queries
/// like `MATCH (u:User) WHERE u.country = 'USA' RETURN u.name`) must KEEP their view_filter
/// because there is no GraphRel.where_predicate to hold the filter.
///
/// By clearing view_filter only in GraphRel contexts, we ensure filters are only collected
/// from GraphRel.where_predicate for relationship queries, while node-only queries still
/// work correctly.
pub struct CleanupViewScanFilters;

impl CleanupViewScanFilters {
    /// Recursively optimize, tracking whether we're inside a GraphRel context
    fn optimize_with_context(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        inside_graph_rel: bool,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::ViewScan(scan) => {
                // Only clear view_filter if inside a GraphRel context
                // For node-only queries (GraphNode → ViewScan), we MUST keep the view_filter
                if inside_graph_rel && scan.view_filter.is_some() {
                    log::debug!("CleanupViewScanFilters: Clearing view_filter (inside GraphRel)");
                    let new_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
                        view_filter: None,
                        ..scan.as_ref().clone()
                    })));
                    Transformed::Yes(new_scan)
                } else {
                    if scan.view_filter.is_some() {
                        log::debug!(
                            "CleanupViewScanFilters: Keeping view_filter (NOT inside GraphRel)"
                        );
                    }
                    Transformed::No(logical_plan)
                }
            }

            // Recursively process all other node types, propagating inside_graph_rel context
            LogicalPlan::Projection(proj) => {
                let input_tf =
                    self.optimize_with_context(proj.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(
                        LogicalPlan::Projection(crate::query_planner::logical_plan::Projection {
                            input: new_input,
                            items: proj.items.clone(),
                            distinct: proj.distinct,
                        }),
                    )),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_tf =
                    self.optimize_with_context(filter.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Filter(
                        crate::query_planner::logical_plan::Filter {
                            input: new_input,
                            predicate: filter.predicate.clone(),
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::GraphRel(graph_rel) => {
                // Mark that we're now inside a GraphRel context
                let left_tf = self.optimize_with_context(graph_rel.left.clone(), plan_ctx, true)?;
                let center_tf =
                    self.optimize_with_context(graph_rel.center.clone(), plan_ctx, true)?;
                let right_tf =
                    self.optimize_with_context(graph_rel.right.clone(), plan_ctx, true)?;

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
                                anchor_connection: graph_rel.anchor_connection.clone(),
                                cte_references: graph_rel.cte_references.clone(),
                            },
                        )))
                    }
                }
            }

            LogicalPlan::GraphNode(graph_node) => {
                // GraphNode is NOT a GraphRel, so don't set inside_graph_rel = true
                let input_tf = self.optimize_with_context(
                    graph_node.input.clone(),
                    plan_ctx,
                    inside_graph_rel,
                )?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(
                        LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
                            input: new_input,
                            alias: graph_node.alias.clone(),
                            label: graph_node.label.clone(),
                            is_denormalized: graph_node.is_denormalized,
            projected_columns: None,
                        }),
                    )),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::GroupBy(group_by) => {
                let input_tf =
                    self.optimize_with_context(group_by.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(
                        LogicalPlan::GroupBy(crate::query_planner::logical_plan::GroupBy {
                            input: new_input,
                            expressions: group_by.expressions.clone(),
                            having_clause: group_by.having_clause.clone(),
                            is_materialization_boundary: group_by.is_materialization_boundary,
                            exposed_alias: group_by.exposed_alias.clone(),
                        }),
                    )),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::OrderBy(order_by) => {
                let input_tf =
                    self.optimize_with_context(order_by.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(
                        LogicalPlan::OrderBy(crate::query_planner::logical_plan::OrderBy {
                            input: new_input,
                            items: order_by.items.clone(),
                        }),
                    )),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::Limit(limit) => {
                let input_tf =
                    self.optimize_with_context(limit.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Limit(
                        crate::query_planner::logical_plan::Limit {
                            input: new_input,
                            count: limit.count,
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::Skip(skip) => {
                let input_tf =
                    self.optimize_with_context(skip.input.clone(), plan_ctx, inside_graph_rel)?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Skip(
                        crate::query_planner::logical_plan::Skip {
                            input: new_input,
                            count: skip.count,
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::GraphJoins(graph_joins) => {
                let input_tf = self.optimize_with_context(
                    graph_joins.input.clone(),
                    plan_ctx,
                    inside_graph_rel,
                )?;
                match input_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(
                        LogicalPlan::GraphJoins(crate::query_planner::logical_plan::GraphJoins {
                            input: new_input,
                            joins: graph_joins.joins.clone(),
                            optional_aliases: graph_joins.optional_aliases.clone(),
                            anchor_table: graph_joins.anchor_table.clone(),
                            cte_references: graph_joins.cte_references.clone(),
                    correlation_predicates: vec![],
                        }),
                    )),
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }

            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left =
                    self.optimize_with_context(cp.left.clone(), plan_ctx, inside_graph_rel)?;
                let transformed_right =
                    self.optimize_with_context(cp.right.clone(), plan_ctx, inside_graph_rel)?;

                if matches!(
                    (&transformed_left, &transformed_right),
                    (Transformed::No(_), Transformed::No(_))
                ) {
                    Transformed::No(logical_plan)
                } else {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: match transformed_left {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        right: match transformed_right {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(new_cp)))
                }
            }

            // Leaf nodes - no transformation needed
            LogicalPlan::Empty

            | LogicalPlan::PageRank(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::Unwind(_) => Transformed::No(logical_plan),

            LogicalPlan::WithClause(with_clause) => {
                let child_tf = self.optimize_with_context(
                    with_clause.input.clone(),
                    plan_ctx,
                    inside_graph_rel,
                )?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            input: new_input,
                            items: with_clause.items.clone(),
                            distinct: with_clause.distinct,
                            order_by: with_clause.order_by.clone(),
                            skip: with_clause.skip,
                            limit: with_clause.limit,
                            where_clause: with_clause.where_clause.clone(),
                            exported_aliases: with_clause.exported_aliases.clone(),
                            cte_references: with_clause.cte_references.clone(),
                        };
                        Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_with)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan),
                }
            }
        };

        Ok(transformed_plan)
    }
}

impl OptimizerPass for CleanupViewScanFilters {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        // Start with inside_graph_rel = false; it will be set to true when we enter a GraphRel
        self.optimize_with_context(logical_plan, plan_ctx, false)
    }
}
