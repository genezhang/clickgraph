use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::Direction,
        logical_plan::LogicalPlan,
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct QueryValidation;

impl AnalyzerPass for QueryValidation {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf = self.analyze_with_graph_schema(
                    projection.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_node.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                self.analyze_with_graph_schema(graph_rel.right.clone(), plan_ctx, graph_schema)?;

                // Skip validation for variable-length paths - they don't need relationship schemas
                if graph_rel.variable_length.is_some() {
                    return Ok(Transformed::No(logical_plan));
                }

                let left_alias = &graph_rel.left_connection;
                let right_alias = &graph_rel.right_connection;

                // Check if nodes actually have table names - skip for anonymous patterns
                // For patterns like ()-[r:FOLLOWS]->(), nodes are Empty Scans with table_name: None
                let left_has_table = match graph_rel.left.as_ref() {
                    LogicalPlan::GraphNode(gn) => match gn.input.as_ref() {
                        LogicalPlan::Scan(scan) => scan.table_name.is_some(),
                        LogicalPlan::ViewScan(_) => true,
                        _ => true,
                    },
                    _ => true,
                };

                let right_has_table = match graph_rel.right.as_ref() {
                    LogicalPlan::GraphNode(gn) => match gn.input.as_ref() {
                        LogicalPlan::Scan(scan) => scan.table_name.is_some(),
                        LogicalPlan::ViewScan(_) => true,
                        _ => true,
                    },
                    _ => true,
                };

                // Skip validation if BOTH nodes are anonymous (no table names)
                // This allows edge-driven queries like ()-[r:FOLLOWS]->()
                if !left_has_table && !right_has_table {
                    return Ok(Transformed::No(logical_plan));
                }

                // Try to get table contexts for validation
                let left_ctx_opt = plan_ctx.get_table_ctx_from_alias_opt(&Some(left_alias.clone()));
                let right_ctx_opt =
                    plan_ctx.get_table_ctx_from_alias_opt(&Some(right_alias.clone()));

                // If contexts don't exist yet, skip (will be validated in later passes)
                if left_ctx_opt.is_err() || right_ctx_opt.is_err() {
                    return Ok(Transformed::No(logical_plan));
                }

                let left_ctx = left_ctx_opt.unwrap();
                let right_ctx = right_ctx_opt.unwrap();

                // Double-check labels exist (should always be true if !should_skip)
                if left_ctx.get_label_opt().is_none() || right_ctx.get_label_opt().is_none() {
                    return Ok(Transformed::No(logical_plan));
                }

                let left_label = left_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::QueryValidation,
                        source: e,
                    })?;
                let right_label =
                    right_ctx
                        .get_label_str()
                        .map_err(|e| AnalyzerError::PlanCtx {
                            pass: Pass::QueryValidation,
                            source: e,
                        })?;

                let (from, to) = if graph_rel.direction == Direction::Incoming {
                    (right_label, left_label)
                } else {
                    // Outgoing or Either: from=left, to=right
                    (left_label, right_label)
                };

                let rel_ctx = plan_ctx.get_mut_table_ctx(&graph_rel.alias).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::QueryValidation,
                        source: e,
                    }
                })?;

                // Skip validation for relationships with multiple types (e.g., [:FOLLOWS|FRIENDS_WITH])
                // The CTE generation will handle validation for multiple relationships
                if rel_ctx
                    .get_labels()
                    .map(|labels| labels.len() > 1)
                    .unwrap_or(false)
                {
                    return Ok(Transformed::No(logical_plan));
                }

                let rel_lable = rel_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::QueryValidation,
                        source: e,
                    })?;

                let rel_schema = graph_schema.get_rel_schema(&rel_lable).map_err(|e| {
                    AnalyzerError::GraphSchema {
                        pass: Pass::QueryValidation,
                        source: e,
                    }
                })?;

                if rel_schema.from_node == *from && rel_schema.to_node == *to
                    || (graph_rel.direction == Direction::Either
                        && [rel_schema.from_node.clone(), rel_schema.to_node.clone()]
                            .contains(&from)
                        && [rel_schema.from_node.clone(), rel_schema.to_node.clone()].contains(&to))
                {
                    // valid graph - ClickGraph only supports edge list (relationships as explicit tables)
                    Transformed::No(logical_plan.clone())
                } else {
                    // return error
                    Err(AnalyzerError::InvalidRelationInQuery {
                        rel: graph_rel.alias.clone(),
                    })?
                }
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
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
        };
        Ok(transformed_plan)
    }
}

impl QueryValidation {
    pub fn new() -> Self {
        QueryValidation
    }
}
