//! Trivial WITH Clause Elimination Optimizer
//!
//! Eliminates unnecessary WITH clauses that don't add value:
//!
//! BEFORE:
//! ```cypher
//! MATCH (a)-[r]->(b)
//! WITH a, b
//! RETURN a.name, b.name
//! ```
//!
//! AFTER:
//! ```cypher
//! MATCH (a)-[r]->(b)
//! RETURN a.name, b.name
//! ```
//!
//! A WITH clause is trivial if:
//! - It's a simple pass-through (no aggregations, no DISTINCT)
//! - No ORDER BY, SKIP, or LIMIT
//! - No WHERE clause
//! - All items are simple aliases (no expressions)
//! - Immediately followed by RETURN or another WITH

use crate::query_planner::{
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, ProjectionItem, WithClause},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};
use std::sync::Arc;

pub struct TrivialWithElimination;

impl OptimizerPass for TrivialWithElimination {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let optimized = Self::optimize_node(logical_plan.clone())?;
        let changed = !Arc::ptr_eq(&logical_plan, &optimized);
        if changed {
            Ok(Transformed::Yes(optimized))
        } else {
            Ok(Transformed::No(optimized))
        }
    }
}

impl TrivialWithElimination {
    fn optimize_node(plan: Arc<LogicalPlan>) -> OptimizerResult<Arc<LogicalPlan>> {
        match plan.as_ref() {
            // Look for Projection/WithClause that has trivial WITH as input
            LogicalPlan::Projection(proj) => {
                let optimized_input = Self::optimize_node(proj.input.clone())?;

                // Check if input is a trivial WITH that we can eliminate
                if let LogicalPlan::WithClause(ref with) = optimized_input.as_ref() {
                    if Self::is_trivial_with(with) {
                        log::info!("🔥 TrivialWithElimination: Removing trivial WITH clause");
                        // Skip the WITH, use its input directly
                        return Ok(Arc::new(LogicalPlan::Projection(
                            crate::query_planner::logical_plan::Projection {
                                input: with.input.clone(),
                                items: proj.items.clone(),
                                distinct: proj.distinct,
                            },
                        )));
                    }
                }

                Ok(Arc::new(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: optimized_input,
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                    },
                )))
            }

            LogicalPlan::WithClause(with) => {
                let optimized_input = Self::optimize_node(with.input.clone())?;

                // Check if input is also a trivial WITH
                if let LogicalPlan::WithClause(ref inner_with) = optimized_input.as_ref() {
                    if Self::is_trivial_with(inner_with) {
                        log::info!(
                            "🔥 TrivialWithElimination: Removing nested trivial WITH clause"
                        );
                        // Skip the inner WITH
                        return Ok(Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
                            input: inner_with.input.clone(),
                            items: with.items.clone(),
                            distinct: with.distinct,
                            order_by: with.order_by.clone(),
                            skip: with.skip,
                            limit: with.limit,
                            where_clause: with.where_clause.clone(),
                            exported_aliases: with.exported_aliases.clone(),
                            cte_references: with.cte_references.clone(),
                        })));
                    }
                }

                Ok(Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
                    input: optimized_input,
                    items: with.items.clone(),
                    distinct: with.distinct,
                    order_by: with.order_by.clone(),
                    skip: with.skip,
                    limit: with.limit,
                    where_clause: with.where_clause.clone(),
                    exported_aliases: with.exported_aliases.clone(),
                    cte_references: with.cte_references.clone(),
                })))
            }

            LogicalPlan::Filter(filter) => {
                let optimized_input = Self::optimize_node(filter.input.clone())?;
                Ok(Arc::new(LogicalPlan::Filter(
                    crate::query_planner::logical_plan::Filter {
                        input: optimized_input,
                        predicate: filter.predicate.clone(),
                    },
                )))
            }

            LogicalPlan::OrderBy(ob) => {
                let optimized_input = Self::optimize_node(ob.input.clone())?;
                Ok(Arc::new(LogicalPlan::OrderBy(
                    crate::query_planner::logical_plan::OrderBy {
                        input: optimized_input,
                        items: ob.items.clone(),
                    },
                )))
            }

            LogicalPlan::Limit(l) => {
                let optimized_input = Self::optimize_node(l.input.clone())?;
                Ok(Arc::new(LogicalPlan::Limit(
                    crate::query_planner::logical_plan::Limit {
                        input: optimized_input,
                        count: l.count,
                    },
                )))
            }

            LogicalPlan::Skip(s) => {
                let optimized_input = Self::optimize_node(s.input.clone())?;
                Ok(Arc::new(LogicalPlan::Skip(
                    crate::query_planner::logical_plan::Skip {
                        input: optimized_input,
                        count: s.count,
                    },
                )))
            }

            // Base cases - no optimization needed
            _ => Ok(plan),
        }
    }

    /// Check if a WITH clause is trivial (can be eliminated)
    fn is_trivial_with(with: &WithClause) -> bool {
        // Must not have ORDER BY, SKIP, LIMIT, WHERE
        if with.order_by.is_some()
            || with.skip.is_some()
            || with.limit.is_some()
            || with.where_clause.is_some()
        {
            return false;
        }

        // Must not be DISTINCT
        if with.distinct {
            return false;
        }

        // All items must be simple pass-throughs (TableAlias expressions only, no aggregations)
        for item in &with.items {
            match &item.expression {
                LogicalExpr::TableAlias(_) => {
                    // Simple pass-through, OK
                }
                LogicalExpr::AggregateFnCall(_) => {
                    // Aggregation, not trivial
                    return false;
                }
                LogicalExpr::ScalarFnCall(_) => {
                    // Function call, not trivial
                    return false;
                }
                LogicalExpr::Operator(_) | LogicalExpr::OperatorApplicationExp(_) => {
                    // Expression, not trivial
                    return false;
                }
                _ => {
                    // Any other complex expression, not trivial
                    return false;
                }
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifies_trivial_with() {
        // WITH a, b (simple pass-through)
        let with = WithClause {
            cte_name: None,
            input: Arc::new(LogicalPlan::Empty),
            items: vec![ProjectionItem {
                expression: LogicalExpr::TableAlias(
                    crate::query_planner::logical_expr::TableAlias("a".to_string()),
                ),
                col_alias: Some(crate::query_planner::logical_expr::ColumnAlias(
                    "a".to_string(),
                )),
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["a".to_string()],
            cte_references: std::collections::HashMap::new(),
        };

        assert!(TrivialWithElimination::is_trivial_with(&with));
    }

    #[test]
    fn test_identifies_non_trivial_with_distinct() {
        let mut with = WithClause {
            cte_name: None,
            input: Arc::new(LogicalPlan::Empty),
            items: vec![],
            distinct: true, // DISTINCT makes it non-trivial
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec![],
            cte_references: std::collections::HashMap::new(),
        };

        assert!(!TrivialWithElimination::is_trivial_with(&with));
    }

    #[test]
    fn test_identifies_non_trivial_with_aggregation() {
        let with = WithClause {
            cte_name: None,
            input: Arc::new(LogicalPlan::Empty),
            items: vec![ProjectionItem {
                expression: LogicalExpr::AggregateFnCall(
                    crate::query_planner::logical_expr::AggregateFnCall {
                        name: "count".to_string(),
                        args: vec![],
                    },
                ),
                col_alias: Some(crate::query_planner::logical_expr::ColumnAlias(
                    "cnt".to_string(),
                )),
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["cnt".to_string()],
            cte_references: std::collections::HashMap::new(),
        };

        assert!(!TrivialWithElimination::is_trivial_with(&with));
    }
}
