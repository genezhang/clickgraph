use std::sync::Arc;

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, Projection, ProjectionItem, Scan},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct PlanSanitization;

impl AnalyzerPass for PlanSanitization {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        self.sanitize_plan(logical_plan, false)
    }
}

impl PlanSanitization {
    pub fn new() -> Self {
        PlanSanitization {}
    }

    fn sanitize_plan(
        &self,
        logical_plan: Arc<LogicalPlan>,
        mut last_node_traversed: bool,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Scan(scan) => {
                if last_node_traversed {
                    let sanitized_scan = self.sanitize_scan(scan);
                    Transformed::Yes(Arc::new(sanitized_scan))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.sanitize_plan(graph_node.input.clone(), last_node_traversed)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = if last_node_traversed {
                    self.sanitize_plan(graph_rel.left.clone(), last_node_traversed)?
                } else {
                    // Left can be an empty node.
                    if !matches!(graph_rel.left.as_ref(), LogicalPlan::Empty) {
                        last_node_traversed = true;
                    }
                    Transformed::No(graph_rel.left.clone())
                };
                // We want to sanitize relationships scans irrespective of last_node_traversed or not, so pass true here.
                let center_tf = self.sanitize_plan(graph_rel.center.clone(), true)?;
                let right_tf = self.sanitize_plan(graph_rel.right.clone(), last_node_traversed)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = self.sanitize_plan(filter.input.clone(), last_node_traversed)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                if last_node_traversed {
                    let sanitized_input =
                        self.sanitize_plan(projection.input.clone(), last_node_traversed)?;
                    let sanitized_projection = self.sanitize_projection(&projection.items);
                    let sanitized_projection_plan = LogicalPlan::Projection(Projection {
                        input: sanitized_input.get_plan(),
                        items: sanitized_projection,
                    });
                    Transformed::Yes(Arc::new(sanitized_projection_plan))
                } else {
                    let child_tf =
                        self.sanitize_plan(projection.input.clone(), last_node_traversed)?;
                    projection.rebuild_or_clone(child_tf, logical_plan.clone())
                }
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.sanitize_plan(group_by.input.clone(), last_node_traversed)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.sanitize_plan(order_by.input.clone(), last_node_traversed)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.sanitize_plan(skip.input.clone(), last_node_traversed)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.sanitize_plan(limit.input.clone(), last_node_traversed)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.sanitize_plan(cte.input.clone(), last_node_traversed)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    self.sanitize_plan(graph_joins.input.clone(), last_node_traversed)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.sanitize_plan(input_plan.clone(), last_node_traversed)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }

    fn sanitize_scan(&self, scan: &Scan) -> LogicalPlan {
        let sanitized_scan = Scan {
            table_name: scan.table_name.clone(),
            table_alias: None,
        };
        LogicalPlan::Scan(sanitized_scan)
    }

    fn sanitize_projection(&self, projection_items: &[ProjectionItem]) -> Vec<ProjectionItem> {
        let mut sanitized_projection_items: Vec<ProjectionItem> = vec![];
        for proj_item in projection_items.iter() {
            if let LogicalExpr::PropertyAccessExp(pro_acc) = &proj_item.expression {
                let sanitized_proj_item = ProjectionItem {
                    expression: LogicalExpr::Column(pro_acc.column.clone()),
                    col_alias: None,
                };
                sanitized_projection_items.push(sanitized_proj_item);
            } else {
                sanitized_projection_items.push(proj_item.clone());
            }
        }
        sanitized_projection_items
    }
}
