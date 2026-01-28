//! View-specific optimizations for ViewScan nodes

use std::sync::Arc;

use crate::query_planner::{
    logical_expr::{LogicalExpr, Operator, OperatorApplication},
    logical_plan::{LogicalPlan, ViewScan},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

/// Optimizer pass for view-based query optimizations
pub struct ViewOptimizer {
    /// Enable filter push down into views
    pub enable_filter_pushdown: bool,
    /// Enable property access optimization
    pub enable_property_optimization: bool,
    /// Enable join order optimization for view scans
    /// Reserved for future optimization feature
    #[allow(dead_code)]
    pub enable_join_optimization: bool,
}

impl Default for ViewOptimizer {
    fn default() -> Self {
        ViewOptimizer {
            enable_filter_pushdown: true,
            enable_property_optimization: true,
            enable_join_optimization: true,
        }
    }
}

impl ViewOptimizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply view-specific optimizations to a ViewScan node
    fn optimize_view_scan(
        &self,
        view_scan: &ViewScan,
        _plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<ViewScan>>> {
        let mut optimized_scan = view_scan.clone();
        let mut changed = false;

        // 1. Property access optimization
        if self.enable_property_optimization {
            let property_optimized = optimized_scan.optimize_property_access();
            if property_optimized != optimized_scan {
                optimized_scan = property_optimized;
                changed = true;
            }
        }

        // 2. Filter optimization: consolidate and simplify filters
        if self.enable_filter_pushdown {
            if let Some(filter) = &optimized_scan.view_filter {
                let simplified_filter = self.simplify_filter_expression(filter);
                if simplified_filter != *filter {
                    optimized_scan.view_filter = Some(simplified_filter);
                    changed = true;
                }
            }
        }

        // 3. Projection optimization: eliminate unused projections
        // TODO: Implement projection elimination based on actual usage

        if changed {
            Ok(Transformed::Yes(Arc::new(optimized_scan)))
        } else {
            Ok(Transformed::No(Arc::new(optimized_scan)))
        }
    }

    /// Simplify filter expressions by applying basic optimizations
    fn simplify_filter_expression(&self, filter: &LogicalExpr) -> LogicalExpr {
        match filter {
            // Simplify nested AND operations: (A AND B) AND C -> A AND B AND C
            LogicalExpr::OperatorApplicationExp(op) if op.operator == Operator::And => {
                let mut flattened_operands = Vec::new();
                self.flatten_and_operands(&op.operands, &mut flattened_operands);

                if flattened_operands.len() == 1 {
                    flattened_operands
                        .into_iter()
                        .next()
                        .expect("flattened_operands has len==1, next() must return Some")
                } else if flattened_operands.len() != op.operands.len() {
                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: flattened_operands,
                    })
                } else {
                    filter.clone()
                }
            }

            // Recursively optimize nested expressions
            LogicalExpr::OperatorApplicationExp(op) => {
                let optimized_operands = op
                    .operands
                    .iter()
                    .map(|operand| self.simplify_filter_expression(operand))
                    .collect();

                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: optimized_operands,
                })
            }

            // For other expression types, return as-is for now
            _ => filter.clone(),
        }
    }

    /// Flatten nested AND operations into a single level
    fn flatten_and_operands(&self, operands: &[LogicalExpr], result: &mut Vec<LogicalExpr>) {
        for operand in operands {
            match operand {
                LogicalExpr::OperatorApplicationExp(nested_op)
                    if nested_op.operator == Operator::And =>
                {
                    // Recursively flatten nested AND operations
                    self.flatten_and_operands(&nested_op.operands, result);
                }
                _ => {
                    // Recursively optimize the operand and add to result
                    result.push(self.simplify_filter_expression(operand));
                }
            }
        }
    }

    /// Optimize join ordering for view scans (placeholder for future implementation)
    #[allow(dead_code)]
    fn optimize_join_order(&self, _view_scan: &ViewScan) -> ViewScan {
        // TODO: Implement join order optimization
        // This could include:
        // - Reordering multiple view scans based on selectivity
        // - Optimizing the order of filter applications
        // - Choosing optimal join algorithms for view-based operations
        _view_scan.clone()
    }
}

impl OptimizerPass for ViewOptimizer {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::ViewScan(view_scan) => {
                let optimized_scan = self.optimize_view_scan(view_scan, plan_ctx)?;
                match optimized_scan {
                    Transformed::Yes(new_scan) => {
                        Transformed::Yes(Arc::new(LogicalPlan::ViewScan(new_scan)))
                    }
                    Transformed::No(scan) => Transformed::No(Arc::new(LogicalPlan::ViewScan(scan))),
                }
            }

            // Recursively optimize child plans for non-ViewScan nodes
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

            // Base cases - no further optimization needed
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::Cte(cte) => {
                let child_tf = self.optimize(cte.input.clone(), plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = self.optimize(u.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(
                        crate::query_planner::logical_plan::Unwind {
                            input: new_input,
                            expression: u.expression.clone(),
                            alias: u.alias.clone(),
                            label: u.label.clone(),
                            tuple_properties: u.tuple_properties.clone(),
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::Literal;
    use std::collections::HashMap;

    #[test]
    fn test_view_optimizer_creation() {
        let optimizer = ViewOptimizer::new();
        assert!(optimizer.enable_filter_pushdown);
        assert!(optimizer.enable_property_optimization);
        assert!(optimizer.enable_join_optimization);
    }

    #[test]
    fn test_filter_simplification() {
        let optimizer = ViewOptimizer::new();

        // Create a simple filter: true
        let simple_filter = LogicalExpr::Literal(Literal::Boolean(true));

        let simplified = optimizer.simplify_filter_expression(&simple_filter);
        assert_eq!(simplified, simple_filter);
    }

    #[test]
    fn test_flatten_and_operands() {
        let optimizer = ViewOptimizer::new();
        let mut result = Vec::new();

        let operands = vec![
            LogicalExpr::Literal(Literal::Boolean(true)),
            LogicalExpr::Literal(Literal::Boolean(false)),
        ];

        optimizer.flatten_and_operands(&operands, &mut result);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_view_scan_optimization() {
        let optimizer = ViewOptimizer::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Create a test ViewScan
        let view_scan = ViewScan::new(
            "test_table".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec!["col1".to_string()],
            vec![],
        );

        let result = optimizer.optimize_view_scan(&view_scan, &mut plan_ctx);
        assert!(result.is_ok());
    }
}
