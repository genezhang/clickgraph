//! CTE Column Resolver - Resolves property access expressions to use CTE column names
//!
//! This pass runs after GraphJoinInference and resolves all PropertyAccess expressions
//! that reference CTEs to use the actual CTE column names instead of graph property names.
//!
//! Example:
//!   Input:  PropertyAccess { table_alias: "p", column: "firstName" }
//!   Output: PropertyAccess { table_alias: "p", column: "p_firstName" }
//!           (if "p" references a CTE that exports "firstName" as "p_firstName")
//!
//! This eliminates the need for the renderer to do property name resolution.

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
        logical_expr::{
            AggregateFnCall, LogicalCase, LogicalExpr, OperatorApplication, PropertyAccess,
            ScalarFnCall,
        },
        logical_plan::{
            Filter, GraphJoins, GroupBy, LogicalPlan, OrderBy, OrderByItem, Projection,
            ProjectionItem, WithClause,
        },
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct CteColumnResolver;

impl CteColumnResolver {
    /// Resolve a PropertyAccess expression to use CTE column names if applicable
    fn resolve_property_access(prop_access: &PropertyAccess, plan_ctx: &PlanCtx) -> PropertyAccess {
        let table_alias = &prop_access.table_alias.0;

        // Check if this table alias refers to a CTE
        if !plan_ctx.is_cte(table_alias) {
            // Not a CTE reference, return as-is
            return prop_access.clone();
        }

        // Get the property name from the column
        let property_name = match &prop_access.column {
            crate::graph_catalog::expression_parser::PropertyValue::Column(col) => col,
            crate::graph_catalog::expression_parser::PropertyValue::Expression(expr) => expr,
        };

        // Look up the CTE column name
        if let Some(cte_column) = plan_ctx.get_cte_column(table_alias, property_name) {
            log::debug!(
                "🔧 CteColumnResolver: Resolved {}.{} → {}.{} (CTE column)",
                table_alias,
                property_name,
                table_alias,
                cte_column
            );

            // Create a new PropertyAccess with the resolved column name
            PropertyAccess {
                table_alias: prop_access.table_alias.clone(),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    cte_column.to_string(),
                ),
            }
        } else {
            // CTE column not found (shouldn't happen), return as-is
            log::warn!(
                "🔧 CteColumnResolver: Could not resolve {}.{} - property not in CTE mapping",
                table_alias,
                property_name
            );
            prop_access.clone()
        }
    }

    /// Recursively resolve PropertyAccess expressions in a LogicalExpr
    fn resolve_expr(expr: &LogicalExpr, plan_ctx: &PlanCtx) -> LogicalExpr {
        match expr {
            LogicalExpr::PropertyAccessExp(prop_access) => {
                LogicalExpr::PropertyAccessExp(Self::resolve_property_access(prop_access, plan_ctx))
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                let resolved_operands = op_app
                    .operands
                    .iter()
                    .map(|operand| Self::resolve_expr(operand, plan_ctx))
                    .collect();
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op_app.operator,
                    operands: resolved_operands,
                })
            }
            LogicalExpr::AggregateFnCall(agg) => {
                let resolved_args = agg
                    .args
                    .iter()
                    .map(|arg| Self::resolve_expr(arg, plan_ctx))
                    .collect();
                LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name.clone(),
                    args: resolved_args,
                })
            }
            LogicalExpr::ScalarFnCall(func) => {
                let resolved_args = func
                    .args
                    .iter()
                    .map(|arg| Self::resolve_expr(arg, plan_ctx))
                    .collect();
                LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: func.name.clone(),
                    args: resolved_args,
                })
            }
            LogicalExpr::Case(case_expr) => {
                let resolved_expr = case_expr
                    .expr
                    .as_ref()
                    .map(|e| Box::new(Self::resolve_expr(e, plan_ctx)));
                let resolved_when_then = case_expr
                    .when_then
                    .iter()
                    .map(|(when, then)| {
                        (
                            Self::resolve_expr(when, plan_ctx),
                            Self::resolve_expr(then, plan_ctx),
                        )
                    })
                    .collect();
                let resolved_else = case_expr
                    .else_expr
                    .as_ref()
                    .map(|e| Box::new(Self::resolve_expr(e, plan_ctx)));
                LogicalExpr::Case(LogicalCase {
                    expr: resolved_expr,
                    when_then: resolved_when_then,
                    else_expr: resolved_else,
                })
            }
            LogicalExpr::Lambda(lambda) => {
                // Lambda expressions contain inner expressions that may reference CTEs
                let resolved_body = Box::new(Self::resolve_expr(&lambda.body, plan_ctx));
                LogicalExpr::Lambda(crate::query_planner::logical_expr::LambdaExpr {
                    params: lambda.params.clone(),
                    body: resolved_body,
                })
            }
            // Other expression types don't contain PropertyAccess, return as-is
            _ => expr.clone(),
        }
    }
}

impl AnalyzerPass for CteColumnResolver {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::debug!(
            "🔧 CteColumnResolver: Processing plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );

        Ok(match logical_plan.as_ref() {
            // Recursively process child plans
            LogicalPlan::GraphNode(node) => {
                let child_tf =
                    self.analyze_with_graph_schema(node.input.clone(), plan_ctx, _graph_schema)?;
                node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(rel) => {
                let left_tf =
                    self.analyze_with_graph_schema(rel.left.clone(), plan_ctx, _graph_schema)?;
                let center_tf =
                    self.analyze_with_graph_schema(rel.center.clone(), plan_ctx, _graph_schema)?;
                let right_tf =
                    self.analyze_with_graph_schema(rel.right.clone(), plan_ctx, _graph_schema)?;

                // Rebuild if any child changed
                if left_tf.is_yes() || center_tf.is_yes() || right_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(
                        crate::query_planner::logical_plan::GraphRel {
                            left: left_tf.get_plan(),
                            center: center_tf.get_plan(),
                            right: right_tf.get_plan(),
                            ..rel.clone()
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Projection - resolve all projection items
            LogicalPlan::Projection(proj) => {
                log::debug!(
                    "🔧 CteColumnResolver: Processing Projection with {} items",
                    proj.items.len()
                );

                let child_tf =
                    self.analyze_with_graph_schema(proj.input.clone(), plan_ctx, _graph_schema)?;

                // Resolve expressions in projection items
                let mut transformed = false;
                let resolved_items: Vec<ProjectionItem> = proj
                    .items
                    .iter()
                    .map(|item| {
                        let resolved_expr = Self::resolve_expr(&item.expression, plan_ctx);
                        if !matches!((&item.expression, &resolved_expr), (a, b) if std::ptr::eq(a, b))
                        {
                            transformed = true;
                        }
                        ProjectionItem {
                            expression: resolved_expr,
                            col_alias: item.col_alias.clone(),
                        }
                    })
                    .collect();

                if child_tf.is_yes() || transformed {
                    log::debug!("🔧 CteColumnResolver: Projection transformed");
                    Transformed::Yes(Arc::new(LogicalPlan::Projection(Projection {
                        input: child_tf.get_plan(),
                        items: resolved_items,
                        distinct: proj.distinct,
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Filter - resolve the filter predicate
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, _graph_schema)?;

                let resolved_predicate = Self::resolve_expr(&filter.predicate, plan_ctx);
                let predicate_transformed = !matches!((&filter.predicate, &resolved_predicate), (a, b) if std::ptr::eq(a, b));

                if child_tf.is_yes() || predicate_transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                        input: child_tf.get_plan(),
                        predicate: resolved_predicate,
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // GroupBy - resolve the group by expressions and having clause
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.analyze_with_graph_schema(
                    group_by.input.clone(),
                    plan_ctx,
                    _graph_schema,
                )?;

                let resolved_expressions: Vec<LogicalExpr> = group_by
                    .expressions
                    .iter()
                    .map(|expr| Self::resolve_expr(expr, plan_ctx))
                    .collect();

                let resolved_having = group_by
                    .having_clause
                    .as_ref()
                    .map(|h| Self::resolve_expr(h, plan_ctx));

                if child_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                        input: child_tf.get_plan(),
                        expressions: resolved_expressions,
                        having_clause: resolved_having,
                        is_materialization_boundary: group_by.is_materialization_boundary,
                        exposed_alias: group_by.exposed_alias.clone(),
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // OrderBy - resolve the order by expressions
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze_with_graph_schema(
                    order_by.input.clone(),
                    plan_ctx,
                    _graph_schema,
                )?;

                let resolved_items: Vec<OrderByItem> = order_by
                    .items
                    .iter()
                    .map(|item| OrderByItem {
                        expression: Self::resolve_expr(&item.expression, plan_ctx),
                        order: item.order.clone(),
                    })
                    .collect();

                if child_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::OrderBy(OrderBy {
                        input: child_tf.get_plan(),
                        items: resolved_items,
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // WithClause - resolve expressions in items and where clause
            LogicalPlan::WithClause(wc) => {
                let child_tf =
                    self.analyze_with_graph_schema(wc.input.clone(), plan_ctx, _graph_schema)?;

                // Resolve projection items
                let resolved_items: Vec<ProjectionItem> = wc
                    .items
                    .iter()
                    .map(|item| ProjectionItem {
                        expression: Self::resolve_expr(&item.expression, plan_ctx),
                        col_alias: item.col_alias.clone(),
                    })
                    .collect();

                // Resolve WHERE clause
                let resolved_where = wc
                    .where_clause
                    .as_ref()
                    .map(|w| Self::resolve_expr(w, plan_ctx));

                // Resolve ORDER BY
                let resolved_order_by = wc.order_by.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|item| OrderByItem {
                            expression: Self::resolve_expr(&item.expression, plan_ctx),
                            order: item.order.clone(),
                        })
                        .collect()
                });

                if child_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::WithClause(WithClause {
                        cte_name: wc.cte_name.clone(),
                        input: child_tf.get_plan(),
                        items: resolved_items,
                        where_clause: resolved_where,
                        order_by: resolved_order_by,
                        ..wc.clone()
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // GraphJoins - already resolved during GraphJoinInference, just process input
            LogicalPlan::GraphJoins(joins) => {
                let child_tf =
                    self.analyze_with_graph_schema(joins.input.clone(), plan_ctx, _graph_schema)?;

                if child_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                        input: child_tf.get_plan(),
                        ..joins.clone()
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Other plan types - just recurse
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, _graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, _graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut transformed = false;
                let resolved_inputs: Vec<Arc<LogicalPlan>> = union
                    .inputs
                    .iter()
                    .map(|input| {
                        let child_tf = self
                            .analyze_with_graph_schema(input.clone(), plan_ctx, _graph_schema)
                            .unwrap();
                        if child_tf.is_yes() {
                            transformed = true;
                        }
                        child_tf.get_plan()
                    })
                    .collect();

                if transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Union(
                        crate::query_planner::logical_plan::Union {
                            inputs: resolved_inputs,
                            union_type: union.union_type.clone(),
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Leaf nodes - no recursion needed
            LogicalPlan::Empty
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Unwind(_)
            | LogicalPlan::CartesianProduct(_) => Transformed::No(logical_plan.clone()),
        })
    }
}
