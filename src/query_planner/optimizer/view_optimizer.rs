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
        // P1.4: exhaustive bottom-up driver replaces the hand-rolled per-variant
        // dispatch. The only real rewrite is on ViewScan (optimizing the scan in
        // isolation); every other arm was pure recursion, which transform_up
        // supplies for free. transform_up additionally recurses into
        // `ViewScan.input` (a real child the old walker treated as a leaf) — a
        // no-op divergence here since optimize_view_scan reads only the scan's
        // own `view_filter`/property-access, never its input, and the goldens +
        // 1,082-query corpus render byte-identical.
        LogicalPlan::transform_up(&logical_plan, &mut |node| {
            if let LogicalPlan::ViewScan(view_scan) = node.as_ref() {
                Ok(match self.optimize_view_scan(view_scan, plan_ctx)? {
                    Transformed::Yes(new_scan) => {
                        Transformed::Yes(Arc::new(LogicalPlan::ViewScan(new_scan)))
                    }
                    // Preserve the original Arc identity on no-op so transform_up
                    // can keep the whole subtree unchanged.
                    Transformed::No(_) => Transformed::No(Arc::clone(node)),
                })
            } else {
                Ok(Transformed::No(Arc::clone(node)))
            }
        })
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
