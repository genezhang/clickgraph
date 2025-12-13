use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
            graph_context::{self, GraphContext},
        },
        logical_expr::{Column, ColumnAlias, Direction, InSubquery, LogicalExpr, PropertyAccess},
        logical_plan::{
            self, {Cte, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan, Union, UnionType},
        },
        plan_ctx::{PlanCtx, TableCtx},
        transformed::Transformed,
    },
};

pub struct GraphTRaversalPlanning;

impl AnalyzerPass for GraphTRaversalPlanning {
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
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphRel(graph_rel) => {
                // Skip traversal planning for variable-length paths and simple relationships
                // Variable-length paths are handled by SQL generator
                // Simple relationships should use direct JOINs, not CTEs/InSubquery
                let should_skip = graph_rel.variable_length.is_some()
                    || graph_rel
                        .labels
                        .as_ref()
                        .map_or(true, |labels| labels.len() <= 1);
                if should_skip {
                    return Ok(Transformed::No(logical_plan));
                }

                // If no graphRel at the right means we have reached at the bottom of the tree i.e. right is anchor.
                if !matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_)) {
                    let (new_graph_rel, ctxs_to_update) =
                        self.infer_traversal(graph_rel, plan_ctx, graph_schema, true)?;

                    for mut ctx in ctxs_to_update.into_iter() {
                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&ctx.alias) {
                            // Preserve multiple labels for relationships (e.g., [:FOLLOWS|LIKES])
                            // Only overwrite if there's a single label or none
                            let existing_labels = table_ctx.get_labels();
                            let should_preserve_labels = existing_labels
                                .map(|labels| labels.len() > 1)
                                .unwrap_or(false);
                            if !should_preserve_labels {
                                table_ctx.set_labels(Some(vec![ctx.label]));
                            }
                            // table_ctx.projection_items.append(&mut ctx.projections);
                            if let Some(plan_expr) = ctx.insubquery {
                                table_ctx.insert_filter(plan_expr);
                            }
                            if ctx.override_projections {
                                table_ctx.set_projections(ctx.projections);
                            } else {
                                table_ctx.append_projection(&mut ctx.projections);
                            }
                        } else {
                            // add new table contexts
                            let mut new_table_ctx = TableCtx::build(
                                ctx.alias.clone(),
                                Some(vec![ctx.label]),
                                vec![],
                                ctx.is_rel,
                                true,
                            );
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            }
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))
                } else {
                    let right_tf = self.analyze_with_graph_schema(
                        graph_rel.right.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;

                    let updated_graph_rel = GraphRel {
                        right: right_tf.get_plan(),
                        ..graph_rel.clone()
                    };
                    let (new_graph_rel, ctxs_to_update) =
                        self.infer_traversal(&updated_graph_rel, plan_ctx, graph_schema, false)?;

                    for mut ctx in ctxs_to_update.into_iter() {
                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&ctx.alias) {
                            // Preserve multiple labels for relationships (e.g., [:FOLLOWS|LIKES])
                            // Only overwrite if there's a single label or none
                            let existing_labels = table_ctx.get_labels();
                            let should_preserve_labels = existing_labels
                                .map(|labels| labels.len() > 1)
                                .unwrap_or(false);
                            if !should_preserve_labels {
                                table_ctx.set_labels(Some(vec![ctx.label]));
                            }
                            // table_ctx.projection_items.append(&mut ctx.projections);
                            if let Some(plan_expr) = ctx.insubquery {
                                table_ctx.insert_filter(plan_expr);
                            }
                            if ctx.override_projections {
                                table_ctx.set_projections(ctx.projections);
                            } else {
                                table_ctx.append_projection(&mut ctx.projections);
                            }
                        } else {
                            // add new table contexts
                            let mut new_table_ctx = TableCtx::build(
                                ctx.alias.clone(),
                                Some(vec![ctx.label]),
                                vec![],
                                ctx.is_rel,
                                true,
                            );
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            }
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))
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
            LogicalPlan::Unwind(u) => {
                let child_tf =
                    self.analyze_with_graph_schema(u.input.clone(), plan_ctx, graph_schema)?;
                match child_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(
                        crate::query_planner::logical_plan::Unwind {
                            input: new_input,
                            expression: u.expression.clone(),
                            alias: u.alias.clone(),
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left =
                    self.analyze_with_graph_schema(cp.left.clone(), plan_ctx, graph_schema)?;
                let transformed_right =
                    self.analyze_with_graph_schema(cp.right.clone(), plan_ctx, graph_schema)?;

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
                let child_tf = self.analyze_with_graph_schema(
                    with_clause.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            input: new_input,
                            items: with_clause.items.clone(),
                            distinct: with_clause.distinct,
                            order_by: with_clause.order_by.clone(),
                            skip: with_clause.skip,
                            limit: with_clause.limit,
                            where_clause: with_clause.where_clause.clone(),
                            exported_aliases: with_clause.exported_aliases.clone(),
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

#[derive(Debug, PartialEq, Clone)]
pub struct CtxToUpdate {
    alias: String,
    label: String,
    projections: Vec<ProjectionItem>,
    insubquery: Option<LogicalExpr>,
    override_projections: bool,
    is_rel: bool,
}

impl GraphTRaversalPlanning {
    pub fn new() -> Self {
        GraphTRaversalPlanning
    }

    fn infer_traversal(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        is_anchor_traversal: bool,
    ) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        // Check for $any nodes - skip graph traversal planning for polymorphic wildcards
        let left_alias = &graph_rel.left_connection;
        let right_alias = &graph_rel.right_connection;

        if let Ok(left_ctx) = plan_ctx.get_node_table_ctx(left_alias) {
            if let Ok(left_label) = left_ctx.get_label_str() {
                if left_label == "$any" {
                    log::debug!("Skipping graph traversal planning for $any left node");
                    return Ok((graph_rel.clone(), vec![]));
                }
            }
        }

        if let Ok(right_ctx) = plan_ctx.get_node_table_ctx(right_alias) {
            if let Ok(right_label) = right_ctx.get_label_str() {
                if right_label == "$any" {
                    log::debug!("Skipping graph traversal planning for $any right node");
                    return Ok((graph_rel.clone(), vec![]));
                }
            }
        }

        let graph_context = graph_context::get_graph_context(
            graph_rel,
            plan_ctx,
            graph_schema,
            Pass::GraphTraversalPlanning,
        )?;

        // left is traversed irrespective of anchor node or intermediate node
        let star_found = graph_context
            .left
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context
            .left
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| match &item.expression {
                LogicalExpr::Column(Column(col)) => {
                    col.as_str() == graph_context.left.id_column.as_str()
                }
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                    column.raw() == graph_context.left.id_column
                }
                _ => false,
            });
        let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> =
                vec![(graph_context.left.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        let star_found = graph_context
            .right
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context
            .right
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| match &item.expression {
                LogicalExpr::Column(Column(col)) => {
                    col.as_str() == graph_context.right.id_column.as_str()
                }
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                    column.raw() == graph_context.right.id_column
                }
                _ => false,
            });
        let right_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> =
                vec![(graph_context.right.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        // ClickGraph always uses EDGE LIST traversal (relationship as explicit table)
        self.handle_edge_list_traversal(
            graph_rel,
            graph_context,
            left_projections,
            right_projections,
            is_anchor_traversal,
        )
    }

    fn handle_edge_list_traversal(
        &self,
        graph_rel: &GraphRel,
        graph_context: GraphContext,
        left_projections: Vec<ProjectionItem>,
        right_projections: Vec<ProjectionItem>,
        is_anchor_traversal: bool,
    ) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];

        let mut rel_ctxs_to_update: Vec<CtxToUpdate>;

        let (r_cte_name, r_plan, r_ctxs_to_update) = self.get_rel_ctx_for_edge_list(
            graph_rel,
            &graph_context,
            graph_context.right.cte_name.clone(),
            graph_context.right.id_column.clone(),
            graph_rel.is_rel_anchor,
        );
        let rel_cte_name: String = r_cte_name;
        rel_ctxs_to_update = r_ctxs_to_update;
        let rel_plan: Arc<LogicalPlan> = r_plan;

        // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id" of the relationship.
        // Based on that we decide, how the left and right nodes are connected with relationship in subqueries.
        let (right_sub_plan_column, left_sub_plan_column) =
            if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
                ("from_id".to_string(), "to_id".to_string())
            } else {
                ("to_id".to_string(), "from_id".to_string())
            };

        let right_insubquery: LogicalExpr = self.build_insubquery(
            graph_context.right.id_column.clone(),
            rel_cte_name.clone(),
            right_sub_plan_column,
            graph_context.right.alias.to_string(), // Pass node alias for qualification
        );

        let left_insubquery: LogicalExpr = self.build_insubquery(
            graph_context.left.id_column,
            rel_cte_name.clone(),
            left_sub_plan_column,
            graph_context.left.alias.to_string(), // Pass node alias for qualification
        );

        if graph_rel.is_rel_anchor {
            let right_ctx_to_update = CtxToUpdate {
                alias: graph_context.right.alias.to_string(),
                label: graph_context.right.label,
                projections: right_projections,
                insubquery: Some(right_insubquery),
                override_projections: false,
                is_rel: true,
            };
            ctxs_to_update.push(right_ctx_to_update);

            rel_ctxs_to_update
                .first_mut()
                .ok_or(AnalyzerError::NoRelationshipContextsFound {
                    pass: Pass::GraphTraversalPlanning,
                })?
                .insubquery = None;

            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false,
            };
            ctxs_to_update.push(left_ctx_to_update);

            let new_graph_rel = GraphRel {
                left: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.left.clone(),
                    name: graph_context.left.cte_name,
                })),
                center: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.right.clone(),
                    name: graph_context.right.cte_name,
                })),
                right: Arc::new(LogicalPlan::Cte(Cte {
                    input: rel_plan.clone(),
                    name: rel_cte_name,
                })),
                ..graph_rel.clone()
            };

            Ok((new_graph_rel, ctxs_to_update))
        } else {
            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false,
            };
            ctxs_to_update.push(left_ctx_to_update);

            if is_anchor_traversal {
                let right_ctx_to_update = CtxToUpdate {
                    alias: graph_context.right.alias.to_string(),
                    label: graph_context.right.label,
                    projections: right_projections,
                    insubquery: None,
                    override_projections: false,
                    is_rel: false,
                };
                ctxs_to_update.push(right_ctx_to_update);

                let new_graph_rel = GraphRel {
                    left: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.left.clone(),
                        name: graph_context.left.cte_name,
                    })),
                    center: Arc::new(LogicalPlan::Cte(Cte {
                        input: rel_plan.clone(),
                        name: rel_cte_name,
                    })),
                    right: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.right.clone(),
                        name: graph_context.right.cte_name,
                    })),
                    ..graph_rel.clone()
                };
                Ok((new_graph_rel, ctxs_to_update))
            } else {
                let new_graph_rel = GraphRel {
                    left: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.left.clone(),
                        name: graph_context.left.cte_name,
                    })),
                    center: Arc::new(LogicalPlan::Cte(Cte {
                        input: rel_plan.clone(),
                        name: rel_cte_name,
                    })),
                    right: graph_rel.right.clone(),
                    ..graph_rel.clone()
                };

                Ok((new_graph_rel, ctxs_to_update))
            }
        }
    }

    // BITMAP traversal removed - ClickGraph only supports EDGE LIST (relationships as explicit tables)

    fn get_rel_ctx_for_edge_list(
        &self,
        graph_rel: &GraphRel,
        graph_context: &GraphContext,
        connected_node_cte_name: String,
        connected_node_id_column: String,
        is_rel_anchor: bool,
    ) -> (String, Arc<LogicalPlan>, Vec<CtxToUpdate>) {
        let star_found = graph_context
            .rel
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);

        // Extract the fully qualified table name from the ViewScan
        let rel_table_name = if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
            scan.source_table.clone()
        } else {
            // Fallback to fully qualified schema table name if not a ViewScan
            format!(
                "{}.{}",
                graph_context.rel.schema.database, graph_context.rel.schema.table_name
            )
        };

        // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        // TODO - currently Either direction on anchor relation is not supported. FIX this
        if graph_rel.direction == Direction::Either
            && graph_context.left.label == graph_context.right.label
            && !is_rel_anchor
        {
            // let new_rel_label = format!("{}_{}", graph_context.rel.label, Direction::Either); //"Direction::Either);

            let rel_cte_name = format!("{}_{}", graph_context.rel.label, graph_context.rel.alias);

            let outgoing_alias = logical_plan::generate_id();
            let incoming_alias = logical_plan::generate_id();

            // let outgoing_label = format!("{}_{}", graph_context.rel.label, Direction::Outgoing);
            // let incoming_label = format!("{}_{}", graph_context.rel.label, Direction::Incoming);

            let rel_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Union(Union {
                inputs: vec![
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(outgoing_alias.clone()),
                        table_name: Some(rel_table_name.clone()),
                    })),
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(incoming_alias.clone()),
                        table_name: Some(rel_table_name.clone()),
                    })),
                ],
                union_type: UnionType::Distinct,
            }));

            let rel_insubquery: LogicalExpr = self.build_insubquery(
                "from_id".to_string(),
                connected_node_cte_name.clone(),
                connected_node_id_column.clone(),
                outgoing_alias.clone(), // Pass relationship alias for variable-length path
            );

            let from_edge_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (
                        // Use actual schema columns with table alias qualification
                        format!("{}.{}", outgoing_alias, graph_context.rel.schema.from_id),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        // Use actual schema columns with table alias qualification
                        format!("{}.{}", outgoing_alias, graph_context.rel.schema.to_id),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

            let from_edge_projections = self.build_projections(from_edge_proj_input);

            let from_edge_ctx_to_update = CtxToUpdate {
                alias: outgoing_alias,
                label: graph_context.rel.label.clone(),
                projections: from_edge_projections,
                insubquery: Some(rel_insubquery.clone()),
                override_projections: false,
                is_rel: true,
            };

            let to_edge_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (
                        // Use actual schema columns with table alias qualification
                        // Note: for variable-length, the direction is reversed for incoming edges
                        format!("{}.{}", incoming_alias, graph_context.rel.schema.to_id),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        // Use actual schema columns with table alias qualification
                        format!("{}.{}", incoming_alias, graph_context.rel.schema.from_id),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

            let to_edge_projections = self.build_projections(to_edge_proj_input);

            let to_edge_ctx_to_update = CtxToUpdate {
                alias: incoming_alias,
                label: graph_context.rel.label.clone(),
                projections: to_edge_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            (
                rel_cte_name,
                rel_plan,
                vec![from_edge_ctx_to_update, to_edge_ctx_to_update],
            )
        } else {
            let rel_cte_name = format!(
                "{}_{}",
                graph_context.rel.label.clone(),
                graph_context.rel.alias
            );

            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (
                        // Use actual schema columns with table alias qualification
                        format!(
                            "{}.{}",
                            graph_context.rel.alias, graph_context.rel.schema.from_id
                        ),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        // Use actual schema columns with table alias qualification
                        format!(
                            "{}.{}",
                            graph_context.rel.alias, graph_context.rel.schema.to_id
                        ),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

            let rel_projections = self.build_projections(rel_proj_input);

            // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id" of the relationship.
            // Based on that we decide, how the relationship is connected with right node as we traverse in graph traversal planning from right to left i.e. bottom to top.
            // Relationship direction integrity is already checked during query validation. If there is wrong direction then plan won't come to this stage. So we don't have to check direction here.

            // IMPORTANT: Use actual schema column names (e.g., user1_id, user2_id), not output aliases (from_id, to_id)
            // The WHERE clause needs to reference the actual table columns
            let sub_in_expr_str =
                if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
                    graph_context.rel.schema.from_id.clone() // Use schema column (e.g., user1_id)
                } else {
                    graph_context.rel.schema.to_id.clone() // Use schema column (e.g., user2_id)
                };

            let rel_insubquery = self.build_insubquery(
                sub_in_expr_str,
                connected_node_cte_name,
                connected_node_id_column,
                graph_context.rel.alias.to_string(), // Pass relationship alias for qualification
            );

            let rel_plan = graph_rel.center.clone();

            let rel_ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: graph_context.rel.label.clone(),
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            (rel_cte_name, rel_plan, vec![rel_ctx_to_update])
        }
    }

    // get_rel_ctx_for_bitmaps function removed - ClickGraph only supports EDGE LIST

    fn build_projections(&self, items: Vec<(String, Option<ColumnAlias>)>) -> Vec<ProjectionItem> {
        items
            .into_iter()
            .map(|(expr_str, alias)| ProjectionItem {
                expression: LogicalExpr::Column(Column(expr_str)),
                col_alias: alias,
            })
            .collect()
    }

    fn build_insubquery(
        &self,
        sub_in_exp: String,
        sub_plan_table: String,
        sub_plan_column: String,
        table_alias: String, // Add table alias for column qualification
    ) -> LogicalExpr {
        // Qualify the column with table alias to avoid ambiguity
        let qualified_column = format!("{}.{}", table_alias, sub_in_exp);
        LogicalExpr::InSubquery(InSubquery {
            expr: Box::new(LogicalExpr::Column(Column(qualified_column))),
            subplan: self.get_subplan(sub_plan_table, sub_plan_column),
        })
    }

    fn get_subplan(&self, table_name: String, table_column: String) -> Arc<LogicalPlan> {
        // Use consistent alias 't' for subquery table references
        let table_alias = "t".to_string();

        Arc::new(LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::Scan(Scan {
                table_alias: Some(table_alias.clone()),
                table_name: Some(table_name),
            })),
            items: vec![ProjectionItem {
                expression: LogicalExpr::Column(Column(format!(
                    "{}.{}",
                    table_alias, table_column
                ))),
                col_alias: None,
            }],
            distinct: false,
        }))
    }
}
