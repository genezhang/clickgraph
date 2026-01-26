use std::sync::Arc;

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_expr::LogicalExpr,
    logical_plan::{GroupBy, LogicalPlan, Projection, ProjectionItem},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct GroupByBuilding;

impl GroupByBuilding {
    /// Check if an expression references a projection alias (from WITH clause).
    /// Returns true if the expression contains a TableAlias that is registered as a projection alias.
    fn references_projection_alias(expr: &LogicalExpr, plan_ctx: &PlanCtx) -> bool {
        match expr {
            LogicalExpr::TableAlias(alias) => {
                // Check if this alias is a projection alias (not a table alias)
                plan_ctx.is_projection_alias(&alias.0)
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // Check if any operand references a projection alias
                op.operands
                    .iter()
                    .any(|operand| Self::references_projection_alias(operand, plan_ctx))
            }
            LogicalExpr::ScalarFnCall(func) => {
                // Check if any argument references a projection alias
                func.args
                    .iter()
                    .any(|arg| Self::references_projection_alias(arg, plan_ctx))
            }
            LogicalExpr::AggregateFnCall(func) => {
                // Check if any argument references a projection alias
                func.args
                    .iter()
                    .any(|arg| Self::references_projection_alias(arg, plan_ctx))
            }
            LogicalExpr::List(list) => {
                // Check if any list element references a projection alias
                list.iter()
                    .any(|item| Self::references_projection_alias(item, plan_ctx))
            }
            // Other expression types don't contain aliases
            _ => false,
        }
    }

    /// Check if an expression contains any aggregate function calls (recursively).
    /// This is needed to detect computed aggregates like COUNT(b) * 10.
    fn contains_aggregate(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::AggregateFnCall(_) => true,
            LogicalExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .any(|operand| Self::contains_aggregate(operand)),
            LogicalExpr::ScalarFnCall(func) => {
                func.args.iter().any(|arg| Self::contains_aggregate(arg))
            }
            LogicalExpr::List(list) => list.iter().any(|item| Self::contains_aggregate(item)),
            LogicalExpr::Case(case_expr) => {
                // Check if CASE expression contains aggregates in:
                // 1. The optional simple CASE expression
                // 2. Any WHEN condition or THEN value
                // 3. The optional ELSE expression
                if let Some(expr) = &case_expr.expr {
                    if Self::contains_aggregate(expr) {
                        return true;
                    }
                }
                for (when_cond, then_val) in &case_expr.when_then {
                    if Self::contains_aggregate(when_cond) || Self::contains_aggregate(then_val) {
                        return true;
                    }
                }
                if let Some(else_expr) = &case_expr.else_expr {
                    if Self::contains_aggregate(else_expr) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }
}

// In the final projections, if there is an aggregate fn then add other projections in group by clause
// build group by plan after projection tagging.
impl AnalyzerPass for GroupByBuilding {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                // Use contains_aggregate to properly detect aggregates including computed expressions
                let non_agg_projections: Vec<ProjectionItem> = projection
                    .items
                    .iter()
                    .filter(|item| !Self::contains_aggregate(&item.expression))
                    .cloned()
                    .collect();

                let agg_count = projection.items.len() - non_agg_projections.len();
                println!(
                    "GroupByBuilding: Found {} aggregations, {} non-aggregations",
                    agg_count,
                    non_agg_projections.len()
                );

                // First check if RETURN has its own aggregations
                if non_agg_projections.len() < projection.items.len()
                    && !non_agg_projections.is_empty()
                {
                    // RETURN has aggregations - this is potentially a two-level aggregation pattern
                    // First, analyze the child to create inner GroupBy (for WITH)
                    let child_tf = self.analyze(projection.input.clone(), _plan_ctx)?;
                    let analyzed_child = child_tf.get_plan();

                    // Check if this is a two-level aggregation pattern:
                    // RETURN has aggregations AND its child is a GroupBy (from WITH)
                    // In this case, we need to wrap the GroupBy in a CTE structure
                    if let LogicalPlan::GroupBy(_inner_group_by) = analyzed_child.as_ref() {
                        println!(
                                "GroupByBuilding: Two-level aggregation detected - RETURN aggregates over WITH GroupBy"
                            );

                        // Create a nested GroupBy structure:
                        // Outer GroupBy (RETURN's aggregation) wraps the Projection which references the inner GroupBy
                        let new_projection = Arc::new(LogicalPlan::Projection(Projection {
                            input: analyzed_child.clone(),
                            items: projection.items.clone(),
                            distinct: projection.distinct,
                        }));

                        Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                            input: new_projection,
                            expressions: non_agg_projections
                                .into_iter()
                                .map(|item| item.expression)
                                .collect(),
                            having_clause: None,
                            is_materialization_boundary: false,
                            exposed_alias: None,
                        })))
                    } else {
                        // Single-level aggregation - just wrap the analyzed child
                        let new_projection = Arc::new(LogicalPlan::Projection(Projection {
                            input: analyzed_child,
                            items: projection.items.clone(),
                            distinct: projection.distinct,
                        }));

                        println!(
                            "GroupByBuilding: Creating GroupBy node with {} grouping expressions",
                            non_agg_projections.len()
                        );
                        Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                            input: new_projection,
                            expressions: non_agg_projections
                                .into_iter()
                                .map(|item| item.expression)
                                .collect(),
                            having_clause: None,
                            is_materialization_boundary: false,
                            exposed_alias: None,
                        })))
                    }
                } else {
                    // No aggregations in RETURN - recurse into child first, then check for optimization
                    println!(
                        "GroupByBuilding: No aggregations in this Projection, recursing into child"
                    );
                    let child_tf = self.analyze(projection.input.clone(), _plan_ctx)?;

                    // Get the transformed plan (clone it so we can check and potentially return it)
                    let transformed_child = child_tf.clone().get_plan();

                    // OPTIMIZATION: Check if the TRANSFORMED child is now a GroupBy
                    // and if RETURN just passes through WITH aliases unchanged
                    if let LogicalPlan::GroupBy(group_by) = transformed_child.as_ref() {
                        println!(
                                "GroupByBuilding: Transformed child is GroupBy, checking for pass-through optimization"
                            );

                        // Extract aliases from the inner Projection (WITH clause)
                        if let LogicalPlan::Projection(inner_proj) = group_by.input.as_ref() {
                            let with_aliases: std::collections::HashSet<String> = inner_proj
                                .items
                                .iter()
                                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                                .collect();

                            // Check if all RETURN items are simple TableAlias pass-throughs
                            let all_pass_through = projection.items.iter().all(|item| {
                                if let LogicalExpr::TableAlias(alias) = &item.expression {
                                    // Check if alias is from WITH and output alias matches
                                    with_aliases.contains(&alias.0)
                                        && item
                                            .col_alias
                                            .as_ref()
                                            .map_or(false, |col| col.0 == alias.0)
                                } else {
                                    false
                                }
                            });

                            if all_pass_through && projection.items.len() == with_aliases.len() {
                                println!(
                                        "GroupByBuilding: OPTIMIZATION - RETURN passes through all WITH aliases unchanged, eliminating outer Projection"
                                    );
                                // Just return the GroupBy directly, skipping the outer Projection
                                return Ok(child_tf);
                            }
                        }
                    }

                    // Not a pass-through or child isn't GroupBy - keep the Projection
                    projection.rebuild_or_clone(child_tf, logical_plan.clone())
                }
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.analyze(group_by.input.clone(), _plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), _plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), _plan_ctx)?;
                let center_tf = self.analyze(graph_rel.center.clone(), _plan_ctx)?;
                let right_tf = self.analyze(graph_rel.right.clone(), _plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.analyze(cte.input.clone(), _plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }

            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze(graph_joins.input.clone(), _plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                println!("GroupByBuilding: Processing Filter node");

                // First, analyze the child to potentially create GroupBy nodes
                let child_tf = self.analyze(filter.input.clone(), _plan_ctx)?;

                println!("GroupByBuilding: Filter child analyzed, checking if it's GroupBy");
                // Check if child became a GroupBy and if filter references projection aliases
                match child_tf {
                    Transformed::Yes(ref plan) | Transformed::No(ref plan) => {
                        if let LogicalPlan::GroupBy(group_by) = plan.as_ref() {
                            println!(
                                "GroupByBuilding: Filter child IS GroupBy, checking for projection alias references"
                            );
                            // Check if the filter predicate references projection aliases
                            let refs_proj_alias =
                                Self::references_projection_alias(&filter.predicate, _plan_ctx);
                            println!(
                                "GroupByBuilding: Filter references projection alias: {}",
                                refs_proj_alias
                            );
                            if refs_proj_alias {
                                println!("GroupByBuilding: Converting Filter to HAVING clause");
                                // Move the filter into the GroupBy as HAVING clause
                                return Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(
                                    GroupBy {
                                        input: group_by.input.clone(),
                                        expressions: group_by.expressions.clone(),
                                        having_clause: Some(filter.predicate.clone()),
                                        is_materialization_boundary: group_by
                                            .is_materialization_boundary,
                                        exposed_alias: group_by.exposed_alias.clone(),
                                    },
                                ))));
                            } else {
                                println!(
                                    "GroupByBuilding: Filter does NOT reference projection alias, keeping as WHERE"
                                );
                            }
                        } else {
                            println!(
                                "GroupByBuilding: Filter child is NOT GroupBy (it's {:?})",
                                std::mem::discriminant(plan.as_ref())
                            );
                        }
                    }
                }

                // Default: rebuild Filter with transformed child
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze(order_by.input.clone(), _plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze(skip.input.clone(), _plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze(limit.input.clone(), _plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.analyze(input_plan.clone(), _plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = self.analyze(u.input.clone(), _plan_ctx)?;
                // Use rebuild_or_clone to automatically preserve metadata
                u.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left = self.analyze(cp.left.clone(), _plan_ctx)?;
                let transformed_right = self.analyze(cp.right.clone(), _plan_ctx)?;

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
                let child_tf = self.analyze(with_clause.input.clone(), _plan_ctx)?;
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

impl GroupByBuilding {
    pub fn new() -> Self {
        GroupByBuilding
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::query_planner::logical_expr::{AggregateFnCall, Column, PropertyAccess, TableAlias};
    use crate::query_planner::logical_plan::{LogicalPlan, Projection};

    fn create_property_access(table: &str, column: &str) -> LogicalExpr {
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table.to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                column.to_string(),
            ),
        })
    }

    fn create_aggregate_function(name: &str, arg_table: &str, arg_column: &str) -> LogicalExpr {
        LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: name.to_string(),
            args: vec![create_property_access(arg_table, arg_column)],
        })
    }

    fn create_scan(alias: Option<String>, table_name: Option<String>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Empty)
    }

    #[test]
    fn test_projection_with_mixed_aggregate_and_non_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
            distinct: false,
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy plan wrapping the projection
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        // GroupBy should wrap the original projection
                        assert_eq!(group_by.input, projection);

                        // Group expressions should contain only non-aggregate expressions
                        assert_eq!(group_by.expressions.len(), 1);
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.table_alias.0, "user");
                                assert_eq!(prop_acc.column.raw(), "name");
                            }
                            _ => panic!("Expected PropertyAccess in group expressions"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_projection_with_only_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT COUNT(order.id), SUM(order.amount) FROM ...
        let scan = create_scan(Some("order".to_string()), Some("orders".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("sum", "order", "amount"),
                    col_alias: None,
                },
            ],
            distinct: false,
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (only aggregates, no grouping needed)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_only_non_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, user.email FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "email"),
                    col_alias: None,
                },
            ],
            distinct: false,
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (no aggregates present)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for non-aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_multiple_non_aggregates_and_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, user.city, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "city"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
            distinct: false,
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy with both non-aggregate expressions
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        assert_eq!(group_by.expressions.len(), 2);

                        // First group expression: user.name
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.raw(), "name");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }

                        // Second group expression: user.city
                        match &group_by.expressions[1] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.raw(), "city");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_empty_projection_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test empty projection
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![],
            distinct: false,
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (empty projection)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection);
            }
            _ => panic!("Expected no transformation for empty projection"),
        }
    }
}
