//! Projection pushdown optimization pass.
//!
//! Eliminates unused columns early in the plan by pushing projection
//! requirements down toward data sources. This reduces I/O and memory usage.
//!
//! # Optimization Strategy
//!
//! - Tracks required columns through the plan
//! - Eliminates unreferenced columns at scan level
//! - Preserves columns needed for filters, joins, and output
//!
//! # Example
//!
//! ```text
//! Before: Projection([a.x], ViewScan(a, columns=[x, y, z]))
//! After:  Projection([a.x], ViewScan(a, columns=[x]))
//! ```

use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::{
        optimizer_pass::{OptimizerPass, OptimizerResult},
    },
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct ProjectionPushDown;

impl OptimizerPass for ProjectionPushDown {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.optimize(cte.input.clone(), plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                let child_tf = self.optimize(projection.input.clone(), plan_ctx)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.optimize(input_plan.clone(), plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = self.optimize(u.input.clone(), plan_ctx)?;
                // Use rebuild_or_clone to automatically preserve metadata
                u.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left = self.optimize(cp.left.clone(), plan_ctx)?;
                let transformed_right = self.optimize(cp.right.clone(), plan_ctx)?;

                if matches!(
                    (&transformed_left, &transformed_right),
                    (Transformed::No(_), Transformed::No(_))
                ) {
                    Transformed::No(logical_plan.clone())
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
            LogicalPlan::WithClause(with_clause) => {
                let child_tf = self.optimize(with_clause.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            cte_name: None,
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
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
        };
        Ok(transformed_plan)
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        ProjectionPushDown
    }
}
