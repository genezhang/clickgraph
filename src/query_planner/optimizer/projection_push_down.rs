use std::sync::Arc;

use crate::query_planner::{
    logical_plan::{LogicalPlan, Projection},
    optimizer::{
        errors::{OptimizerError, Pass},
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
            LogicalPlan::Scan(scan) => {
                let table_ctx_opt = plan_ctx
                    .get_mut_table_ctx_opt_from_alias_opt(&scan.table_alias)
                    .map_err(|e| OptimizerError::PlanCtx {
                        pass: Pass::ProjectionPushDown,
                        source: e,
                    })?;
                if let Some(table_ctx) = table_ctx_opt {
                    if !table_ctx.get_projections().is_empty() {
                        let projections = table_ctx.get_projections().clone();

                        println!(
                            "\nProjectionPushDown: Creating new Projection for Scan(alias={:?})",
                            scan.table_alias
                        );
                        println!(
                            "ProjectionPushDown: Number of projection items: {}",
                            projections.len()
                        );
                        for (i, item) in projections.iter().enumerate() {
                            use crate::query_planner::logical_expr::LogicalExpr;
                            println!(
                                "ProjectionPushDown: Item {} discriminant: {:?}",
                                i,
                                std::mem::discriminant(&item.expression)
                            );
                            if let LogicalExpr::PropertyAccessExp(pa) = &item.expression {
                                println!(
                                    "ProjectionPushDown: Item {} is PropertyAccessExp(alias={}, column={})",
                                    i, pa.table_alias, pa.column.raw()
                                );
                            } else if let LogicalExpr::Literal(_) = &item.expression {
                                println!("ProjectionPushDown: Item {} is Literal!!!", i);
                            }
                        }

                        let new_proj = Arc::new(LogicalPlan::Projection(Projection {
                            input: logical_plan.clone(),
                            items: projections,
                            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
                            distinct: false,
                        }));
                        Transformed::Yes(new_proj)
                    } else {
                        Transformed::No(logical_plan.clone())
                    }
                } else {
                    Transformed::No(logical_plan.clone())
                }
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
                match child_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(crate::query_planner::logical_plan::Unwind {
                        input: new_input,
                        expression: u.expression.clone(),
                        alias: u.alias.clone(),
                    }))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left = self.optimize(cp.left.clone(), plan_ctx)?;
                let transformed_right = self.optimize(cp.right.clone(), plan_ctx)?;
                
                if matches!((&transformed_left, &transformed_right), (Transformed::No(_), Transformed::No(_))) {
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
        };
        Ok(transformed_plan)
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        ProjectionPushDown
    }
}
