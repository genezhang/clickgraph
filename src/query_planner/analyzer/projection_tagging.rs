use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::{
            AggregateFnCall, Column, LogicalCase, LogicalExpr, Operator, OperatorApplication,
            PropertyAccess, ScalarFnCall, TableAlias,
        },
        logical_plan::{LogicalPlan, Projection, ProjectionItem},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct ProjectionTagging;

impl AnalyzerPass for ProjectionTagging {
    // Check if the projection item is only * then check for explicitly mentioned aliases and add * as their projection.
    // in the final projection, put all explicit alias.*

    // If there is any projection on relationship then use edgelist of that relation.
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                println!("🔍 ProjectionTagging: BEFORE processing Projection - distinct={}", projection.distinct);
                // First, recursively process the input child to preserve transformations
                // from previous analyzer passes (like FilterTagging)
                let child_tf = self.analyze_with_graph_schema(
                    projection.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;

                // handler select all. e.g. -
                //
                // MATCH (u:User)-[c:Created]->(p:Post)
                //      RETURN *;
                //
                // We will treat it as -
                //
                // MATCH (u:User)-[c:Created]->(p:Post)
                // RETURN u, c, p;
                //
                // To achieve this we will convert `RETURN *` into `RETURN u, c, p`
                let mut proj_items_to_mutate: Vec<ProjectionItem> =
                    if self.select_all_present(&projection.items) {
                        // we will create projection items with only table alias as return item. tag_projection will handle the proper tagging and overall projection manupulation.
                        let explicit_aliases = self.get_explicit_aliases(plan_ctx);
                        explicit_aliases
                            .iter()
                            .map(|exp_alias| {
                                let table_alias = TableAlias(exp_alias.clone());
                                ProjectionItem {
                                    expression: LogicalExpr::TableAlias(table_alias.clone()),
                                    col_alias: None,
                                }
                            })
                            .collect()
                    } else {
                        projection.items.clone()
                    };

                for item in &mut proj_items_to_mutate {
                    Self::tag_projection(item, plan_ctx, graph_schema)?;
                }

                let result = Transformed::Yes(Arc::new(LogicalPlan::Projection(Projection {
                    input: child_tf.get_plan(), // Use transformed child instead of original
                    items: proj_items_to_mutate,
                    kind: projection.kind.clone(),
                    distinct: projection.distinct,
                })));
                println!("🔍 ProjectionTagging: AFTER creating new Projection - distinct={}", projection.distinct);
                result
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_node.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                // let self_tf = self.analyze_with_graph_schema(graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf =
                    self.analyze_with_graph_schema(graph_rel.left.clone(), plan_ctx, graph_schema)?;
                let center_tf = self.analyze_with_graph_schema(
                    graph_rel.center.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                let right_tf = self.analyze_with_graph_schema(
                    graph_rel.right.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    self.analyze_with_graph_schema(cte.input.clone(), plan_ctx, graph_schema)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_joins.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(group_by.input.clone(), plan_ctx, graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(order_by.input.clone(), plan_ctx, graph_schema)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        self.analyze_with_graph_schema(input_plan.clone(), plan_ctx, graph_schema)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_view_scan) => Transformed::No(logical_plan.clone()),
        };
        Ok(transformed_plan)
    }
}

impl ProjectionTagging {
    pub fn new() -> Self {
        ProjectionTagging
    }

    fn select_all_present(&self, projection_items: &[ProjectionItem]) -> bool {
        projection_items
            .iter()
            .any(|item| item.expression == LogicalExpr::Star)
    }

    fn get_explicit_aliases(&self, plan_ctx: &mut PlanCtx) -> Vec<String> {
        plan_ctx
            .get_alias_table_ctx_map()
            .iter()
            .filter_map(|(alias, table_ctx)| {
                if table_ctx.is_explicit_alias() {
                    Some(alias.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn tag_projection(
        item: &mut ProjectionItem,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<()> {
        match item.expression.clone() {
            LogicalExpr::TableAlias(table_alias) => {
                // Check if this is a projection alias (from WITH clause) rather than a table alias
                if plan_ctx.is_projection_alias(&table_alias.0) {
                    // This is a projection alias (e.g., "follows" from "COUNT(b) as follows")
                    // Keep it as-is - it will be resolved during query execution
                    return Ok(());
                }

                // if just table alias i.e MATCH (p:Post) Return p; then For final overall projection keep p.* and for p's projection keep *.

                let table_ctx = plan_ctx.get_mut_table_ctx(&table_alias.0).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::ProjectionTagging,
                        source: e,
                    }
                })?;

                // Check if this is a path variable (has no label - path variables are registered without labels)
                // Path variables should be kept as-is, not expanded to .*
                let is_path_variable =
                    table_ctx.get_label_opt().is_none() && !table_ctx.is_relation();

                if is_path_variable {
                    // This is a path variable - don't expand to .*, keep it as TableAlias
                    // The render layer will handle converting it to the appropriate map() construction
                    // No changes to item.expression needed
                    Ok(())
                } else {
                    // Regular table alias - expand to .*
                    let tagged_proj = ProjectionItem {
                        expression: LogicalExpr::Star,
                        col_alias: None,
                        // belongs_to_table: Some(table_alias.clone()),
                    };
                    // table_ctx.projection_items = vec![tagged_proj];
                    table_ctx.set_projections(vec![tagged_proj]);

                    // update the overall projection
                    item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: table_alias.clone(),
                        column: Column("*".to_string()),
                    });
                    Ok(())
                }
            }
            LogicalExpr::PropertyAccessExp(property_access) => {
                let table_ctx = plan_ctx
                    .get_mut_table_ctx(&property_access.table_alias.0)
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::ProjectionTagging,
                        source: e,
                    })?;
                table_ctx.insert_projection(item.clone());

                // Don't set an alias - let ClickHouse return just the column name
                // SQL will be: SELECT u.name (returns as "name" not "u.name")

                Ok(())
            }
            LogicalExpr::OperatorApplicationExp(operator_application) => {
                // Recursively process operands and collect the transformed expressions
                let mut transformed_operands = Vec::new();
                for operand in &operator_application.operands {
                    let mut operand_return_item = ProjectionItem {
                        expression: operand.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut operand_return_item, plan_ctx, graph_schema)?;
                    transformed_operands.push(operand_return_item.expression);
                }
                
                // Update the item's expression with transformed operands
                item.expression = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: operator_application.operator.clone(),
                    operands: transformed_operands,
                });
                Ok(())
            }
            LogicalExpr::ScalarFnCall(scalar_fn_call) => {
                // Recursively process arguments and collect transformed expressions
                let mut transformed_args = Vec::new();
                for arg in &scalar_fn_call.args {
                    let mut arg_return_item = ProjectionItem {
                        expression: arg.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut arg_return_item, plan_ctx, graph_schema)?;
                    transformed_args.push(arg_return_item.expression);
                }
                
                // Update the item's expression with transformed arguments
                item.expression = LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: scalar_fn_call.name.clone(),
                    args: transformed_args,
                });
                Ok(())
            }
            // For now I am not tagging Aggregate fns, but I will tag later for aggregate pushdown when I implement the aggregate push down optimization
            // For now if there is a tableAlias in agg fn args and fn name is Count then convert the table alias to node Id
            LogicalExpr::AggregateFnCall(aggregate_fn_call) => {
                for arg in &aggregate_fn_call.args {
                    // Handle COUNT(a) or COUNT(DISTINCT a)
                    let table_alias_opt = match arg {
                        LogicalExpr::TableAlias(TableAlias(t_alias)) => Some(t_alias.as_str()),
                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator,
                            operands,
                        }) if *operator == Operator::Distinct && operands.len() == 1 => {
                            // Handle DISTINCT a inside COUNT(DISTINCT a)
                            if let LogicalExpr::TableAlias(TableAlias(t_alias)) = &operands[0] {
                                Some(t_alias.as_str())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(t_alias) = table_alias_opt {
                        if aggregate_fn_call.name.to_lowercase() == "count" {
                            let table_ctx = plan_ctx.get_mut_table_ctx(t_alias).map_err(|e| {
                                AnalyzerError::PlanCtx {
                                    pass: Pass::ProjectionTagging,
                                    source: e,
                                }
                            })?;

                            if table_ctx.is_relation() {
                                // For relationships, count the relationship records
                                // Convert count(r) or count(distinct r) to count(*) for the relationship table
                                item.expression = LogicalExpr::AggregateFnCall(AggregateFnCall {
                                    name: aggregate_fn_call.name.clone(),
                                    args: vec![LogicalExpr::Star],
                                });
                            } else {
                                // For nodes, count distinct node IDs
                                let table_label = table_ctx.get_label_str().map_err(|e| {
                                    AnalyzerError::PlanCtx {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    }
                                })?;
                                let table_schema = graph_schema
                                    .get_node_schema(&table_label)
                                    .map_err(|e| AnalyzerError::GraphSchema {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    })?;
                                let table_node_id = table_schema.node_id.column.clone();

                                // Preserve DISTINCT if it was in the original expression
                                let new_arg = if matches!(arg, LogicalExpr::OperatorApplicationExp(OperatorApplication { operator, .. }) if *operator == Operator::Distinct)
                                {
                                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                        operator: Operator::Distinct,
                                        operands: vec![LogicalExpr::PropertyAccessExp(
                                            PropertyAccess {
                                                table_alias: TableAlias(t_alias.to_string()),
                                                column: Column(table_node_id),
                                            },
                                        )],
                                    })
                                } else {
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(t_alias.to_string()),
                                        column: Column(table_node_id),
                                    })
                                };

                                item.expression = LogicalExpr::AggregateFnCall(AggregateFnCall {
                                    name: aggregate_fn_call.name.clone(),
                                    args: vec![new_arg],
                                });
                            }
                        }
                    }
                }
                Ok(())
            }
            LogicalExpr::Case(logical_case) => {
                // Process the optional simple CASE expression
                let transformed_expr = if let Some(expr) = &logical_case.expr {
                    let mut expr_item = ProjectionItem {
                        expression: (**expr).clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut expr_item, plan_ctx, graph_schema)?;
                    Some(Box::new(expr_item.expression))
                } else {
                    None
                };

                // Process WHEN conditions and THEN values
                let mut transformed_when_then = Vec::new();
                for (when_cond, then_val) in &logical_case.when_then {
                    let mut when_item = ProjectionItem {
                        expression: when_cond.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut when_item, plan_ctx, graph_schema)?;

                    let mut then_item = ProjectionItem {
                        expression: then_val.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut then_item, plan_ctx, graph_schema)?;

                    transformed_when_then.push((when_item.expression, then_item.expression));
                }

                // Process the optional ELSE expression
                let transformed_else = if let Some(else_expr) = &logical_case.else_expr {
                    let mut else_item = ProjectionItem {
                        expression: (**else_expr).clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut else_item, plan_ctx, graph_schema)?;
                    Some(Box::new(else_item.expression))
                } else {
                    None
                };

                // Update the item's expression with all transformed parts
                item.expression = LogicalExpr::Case(LogicalCase {
                    expr: transformed_expr,
                    when_then: transformed_when_then,
                    else_expr: transformed_else,
                });
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

