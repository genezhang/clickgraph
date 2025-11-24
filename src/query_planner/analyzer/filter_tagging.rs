use std::{collections::HashSet, sync::Arc};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::{
            AggregateFnCall, Column, LogicalExpr, Operator, OperatorApplication, PropertyAccess,
            ScalarFnCall, TableAlias,
        },
        logical_plan::{Filter, GroupBy, LogicalPlan, ProjectionItem},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct FilterTagging;

impl AnalyzerPass for FilterTagging {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        println!(
            "FilterTagging: analyze_with_graph_schema called with plan: {:?}",
            logical_plan
        );
        println!(
            "FilterTagging: analyze_with_graph_schema called with plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );
        let variant_name = match &*logical_plan {
            LogicalPlan::Empty => "Empty",
            LogicalPlan::Scan(_) => "Scan",
            LogicalPlan::ViewScan(_) => "ViewScan",
            LogicalPlan::GraphNode(_) => "GraphNode",
            LogicalPlan::GraphRel(_) => "GraphRel",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::GroupBy(_) => "GroupBy",
            LogicalPlan::OrderBy(_) => "OrderBy",
            LogicalPlan::Skip(_) => "Skip",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::Cte(_) => "Cte",
            LogicalPlan::GraphJoins(_) => "GraphJoins",
            LogicalPlan::Union(_) => "Union",
            LogicalPlan::PageRank(_) => "PageRank",
        };
        println!("FilterTagging: About to match on variant: {}", variant_name);
        Ok(match logical_plan.as_ref() {
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_node.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
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
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_joins.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                println!("FilterTagging: ENTERING Filter case - processing Filter node");
                println!(
                    "FilterTagging: Processing Filter node with predicate: {:?}",
                    filter.predicate
                );
                let child_tf =
                    self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, graph_schema)?;
                // Apply property mapping to the filter predicate
                let mapped_predicate =
                    self.apply_property_mapping(filter.predicate.clone(), plan_ctx, graph_schema)?;
                println!("FilterTagging: Mapped predicate: {:?}", mapped_predicate);

                // Check if this filter references projection aliases (HAVING clause)
                if Self::references_projection_alias(&mapped_predicate, plan_ctx) {
                    println!(
                        "FilterTagging: Filter references projection alias - converting to HAVING clause"
                    );
                    // This filter should become a HAVING clause on the child GroupBy
                    match &child_tf {
                        Transformed::Yes(plan) | Transformed::No(plan) => {
                            if let LogicalPlan::GroupBy(group_by) = plan.as_ref() {
                                println!(
                                    "FilterTagging: Child is GroupBy, attaching filter as HAVING clause"
                                );
                                let new_group_by = LogicalPlan::GroupBy(GroupBy {
                                    input: group_by.input.clone(),
                                    expressions: group_by.expressions.clone(),
                                    having_clause: Some(mapped_predicate.clone()),
                                });
                                return Ok(Transformed::Yes(Arc::new(new_group_by)));
                            } else {
                                println!(
                                    "FilterTagging: WARNING - projection alias reference but child is not GroupBy!"
                                );
                            }
                        }
                    }
                }

                // call filter tagging and get new filter
                let final_filter_opt = self.extract_filters(mapped_predicate, plan_ctx)?;
                println!("FilterTagging: Final filter option: {:?}", final_filter_opt);
                // if final filter has some predicate left then create new filter else remove the filter node and return the child input
                if let Some(final_filter) = final_filter_opt {
                    Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                        input: child_tf.get_plan().clone(),
                        predicate: final_filter,
                    })))
                } else {
                    Transformed::Yes(child_tf.get_plan().clone())
                }
            }
            LogicalPlan::Projection(projection) => {
                println!("🔍 FilterTagging: BEFORE processing Projection - distinct={}", projection.distinct);
                println!("FilterTagging: Processing Projection, analyzing child input");
                println!(
                    "FilterTagging: Projection input type: {:?}",
                    std::mem::discriminant(&*projection.input)
                );
                println!(
                    "FilterTagging: About to call analyze_with_graph_schema on child input: {:?}",
                    projection.input
                );
                let child_tf = self.analyze_with_graph_schema(
                    projection.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                println!(
                    "FilterTagging: Finished analyzing child input, result: {:?}",
                    child_tf
                );
                println!(
                    "FilterTagging: Projection child processed, applying property mapping to projection items"
                );
                // Apply property mapping to projection expressions
                let mut mapped_items = Vec::new();
                for item in &projection.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;
                    mapped_items.push(ProjectionItem {
                        expression: mapped_expr.clone(),
                        col_alias: item.col_alias.clone(),
                    });

                    // Register projection aliases for HAVING clause support
                    // If this projection item has an alias (e.g., COUNT(b) as follows),
                    // register it so filters can reference it
                    if let Some(col_alias) = &item.col_alias {
                        println!(
                            "FilterTagging: Registering projection alias: {} -> {:?}",
                            col_alias.0, mapped_expr
                        );
                        plan_ctx.register_projection_alias(col_alias.0.clone(), mapped_expr);
                    }
                }
                let result = Transformed::Yes(Arc::new(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: child_tf.get_plan(),
                        items: mapped_items,
                        kind: projection.kind.clone(),
                        distinct: projection.distinct,  // PRESERVE distinct flag from original projection
                    },
                )));
                println!("🔍 FilterTagging: AFTER creating new Projection - distinct={}", projection.distinct);
                result
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(group_by.input.clone(), plan_ctx, graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(order_by.input.clone(), plan_ctx, graph_schema)?;
                // Apply property mapping to order by expressions
                let mut mapped_items = Vec::new();
                for item in &order_by.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;
                    mapped_items.push(crate::query_planner::logical_plan::OrderByItem {
                        expression: mapped_expr,
                        order: item.order.clone(),
                    });
                }
                Transformed::Yes(Arc::new(LogicalPlan::OrderBy(
                    crate::query_planner::logical_plan::OrderBy {
                        input: child_tf.get_plan(),
                        items: mapped_items,
                    },
                )))
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
        })
    }
}

impl FilterTagging {
    pub fn new() -> Self {
        FilterTagging
    }

    /// Apply property mapping to a LogicalExpr, converting Cypher property names to database column names
    pub fn apply_property_mapping(
        &self,
        expr: LogicalExpr,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<LogicalExpr> {
        match expr {
            LogicalExpr::PropertyAccessExp(property_access) => {
                println!(
                    "FilterTagging: apply_property_mapping for alias '{}', property '{}'",
                    property_access.table_alias.0, property_access.column.raw()
                );
                // Get the table context for this alias
                let table_ctx = plan_ctx
                    .get_table_ctx(&property_access.table_alias.0)
                    .map_err(|e| {
                        eprintln!(
                            "FilterTagging: ERROR - Failed to get table_ctx for alias '{}': {:?}",
                            property_access.table_alias.0, e
                        );
                        AnalyzerError::PlanCtx {
                            pass: Pass::FilterTagging,
                            source: e,
                        }
                    })?;

                println!(
                    "FilterTagging: Found table_ctx, is_relation={}, label={:?}",
                    table_ctx.is_relation(),
                    table_ctx.get_label_opt()
                );

                // Get the label for this table
                let label = table_ctx.get_label_opt().ok_or_else(|| {
                    eprintln!(
                        "FilterTagging: ERROR - No label found for alias '{}', is_relation={}",
                        property_access.table_alias.0,
                        table_ctx.is_relation()
                    );
                    AnalyzerError::PropertyNotFound {
                        entity_type: "node".to_string(),
                        entity_name: property_access.table_alias.0.clone(),
                        property: property_access.column.raw().to_string(),
                    }
                })?;

                // Use view resolver to map the property
                let view_resolver =
                    crate::query_planner::analyzer::view_resolver::ViewResolver::from_schema(
                        graph_schema,
                    );
                println!(
                    "FilterTagging: About to call resolve_node_property, is_relation={}, label={}, property={}",
                    table_ctx.is_relation(),
                    label,
                    property_access.column.raw()
                );
                let mapped_column = if table_ctx.is_relation() {
                    let result = view_resolver
                        .resolve_relationship_property(&label, &property_access.column.raw());
                    println!(
                        "FilterTagging: resolve_relationship_property result: {:?}",
                        result
                    );
                    result?
                } else {
                    let result =
                        view_resolver.resolve_node_property(&label, &property_access.column.raw());
                    println!("FilterTagging: resolve_node_property result: {:?}", result);
                    result?
                };
                println!(
                    "FilterTagging: Successfully mapped property '{}' to column '{}'",
                    property_access.column.raw(), mapped_column.raw()
                );

                Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: property_access.table_alias,
                    column: mapped_column,
                }))
            }
            LogicalExpr::OperatorApplicationExp(mut op) => {
                // Recursively apply property mapping to operands
                let mut mapped_operands = Vec::new();
                for operand in op.operands {
                    mapped_operands.push(self.apply_property_mapping(
                        operand,
                        plan_ctx,
                        graph_schema,
                    )?);
                }
                op.operands = mapped_operands;
                Ok(LogicalExpr::OperatorApplicationExp(op))
            }
            LogicalExpr::ScalarFnCall(mut fn_call) => {
                // Recursively apply property mapping to function arguments
                let mut mapped_args = Vec::new();
                for arg in fn_call.args {
                    mapped_args.push(self.apply_property_mapping(arg, plan_ctx, graph_schema)?);
                }
                fn_call.args = mapped_args;
                Ok(LogicalExpr::ScalarFnCall(fn_call))
            }
            LogicalExpr::AggregateFnCall(mut agg_call) => {
                // Recursively apply property mapping to aggregate function arguments
                let mut mapped_args = Vec::new();
                for arg in agg_call.args {
                    mapped_args.push(self.apply_property_mapping(arg, plan_ctx, graph_schema)?);
                }
                agg_call.args = mapped_args;
                Ok(LogicalExpr::AggregateFnCall(agg_call))
            }
            LogicalExpr::List(mut list) => {
                // Recursively apply property mapping to list elements
                let mut mapped_elements = Vec::new();
                for element in list {
                    mapped_elements.push(self.apply_property_mapping(
                        element,
                        plan_ctx,
                        graph_schema,
                    )?);
                }
                Ok(LogicalExpr::List(mapped_elements))
            }
            // For other expression types, return as-is
            other => Ok(other),
        }
    }

    // If there is any filter on relationship then use edgelist of that relation.
    pub fn extract_filters(
        &self,
        filter_predicate: LogicalExpr,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Option<LogicalExpr>> {
        println!(
            "FilterTagging: extract_filters called with predicate: {:?}",
            filter_predicate
        );
        let mut extracted_filters: Vec<OperatorApplication> = vec![];
        let mut extracted_projections: Vec<PropertyAccess> = vec![];

        let remaining = Self::process_expr(
            filter_predicate,
            &mut extracted_filters,
            &mut extracted_projections,
            false,
        );

        println!(
            "FilterTagging: Extracted {} filters, {} projections, remaining: {:?}",
            extracted_filters.len(),
            extracted_projections.len(),
            remaining
        );

        // tag extracted filters to respective table data
        for extracted_filter in extracted_filters {
            let table_alias = Self::get_table_alias_if_single_table_condition(
                &LogicalExpr::OperatorApplicationExp(extracted_filter.clone()),
                true,
            )
            .unwrap_or_default();
            println!(
                "FilterTagging: Extracted filter for table alias: '{}'",
                table_alias
            );
            // let mut table_alias = "";
            // for operand in &extracted_filter.operands {
            //     match operand {
            //         LogicalExpr::PropertyAccessExp(property_access) => {
            //             table_alias = &property_access.table_alias.0;
            //         },
            //         // in case of fn, we check for any argument is of type prop access
            //         LogicalExpr::ScalarFnCall(scalar_fn_call) => {
            //             for arg in &scalar_fn_call.args {
            //                 if let LogicalExpr::PropertyAccessExp(property_access) = arg {
            //                     table_alias = &property_access.table_alias.0;
            //                 }
            //             }
            //         },
            //         // in case of fn, we check for any argument is of type prop access
            //         LogicalExpr::AggregateFnCall(aggregate_fn_call) => {
            //             for arg in &aggregate_fn_call.args {
            //                 if let LogicalExpr::PropertyAccessExp(property_access) = arg {
            //                     table_alias = &property_access.table_alias.0;
            //                 }
            //             }
            //         },
            //         _ => ()
            //     }
            // }

            if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&table_alias) {
                // FIXED: Keep PropertyAccessExp with table_alias instead of converting to Column
                // The table_alias is needed for correct SQL generation (e.g., a.name not just name)
                // Property mapping was already done above, so column names are correct
                table_ctx.insert_filter(LogicalExpr::OperatorApplicationExp(extracted_filter));
            } else {
                return Err(AnalyzerError::OrphanAlias {
                    pass: Pass::FilterTagging,
                    alias: table_alias.to_string(),
                });
            }
        }

        // add extracted_projections to their respective nodes.
        for prop_acc in extracted_projections {
            let table_alias = prop_acc.table_alias.clone();
            if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&table_alias.0) {
                table_ctx.insert_projection(ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(prop_acc),
                    col_alias: None,
                });
            } else {
                return Err(AnalyzerError::OrphanAlias {
                    pass: Pass::FilterTagging,
                    alias: table_alias.to_string(),
                });
            }
        }

        Ok(remaining)
    }

    fn convert_prop_acc_to_column(expr: LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::PropertyAccessExp(property_access) => {
                LogicalExpr::Column(Column(property_access.column.raw().to_string()))
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                let mut new_operands: Vec<LogicalExpr> = vec![];
                for operand in op_app.operands {
                    let new_operand = Self::convert_prop_acc_to_column(operand);
                    new_operands.push(new_operand);
                }
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op_app.operator,
                    operands: new_operands,
                })
            }
            LogicalExpr::List(exprs) => {
                let mut new_exprs = Vec::new();
                for sub_expr in exprs {
                    let new_expr = Self::convert_prop_acc_to_column(sub_expr);
                    new_exprs.push(new_expr);
                }
                LogicalExpr::List(new_exprs)
            }
            LogicalExpr::ScalarFnCall(fc) => {
                let mut new_args = Vec::new();
                for arg in fc.args {
                    let new_arg = Self::convert_prop_acc_to_column(arg);
                    new_args.push(new_arg);
                }
                LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: fc.name,
                    args: new_args,
                })
            }

            LogicalExpr::AggregateFnCall(fc) => {
                let mut new_args = Vec::new();
                for arg in fc.args {
                    let new_arg = Self::convert_prop_acc_to_column(arg);
                    new_args.push(new_arg);
                }
                LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: fc.name,
                    args: new_args,
                })
            }
            other => other,
        }
    }

    fn process_expr(
        expr: LogicalExpr,
        extracted_filters: &mut Vec<OperatorApplication>,
        extracted_projections: &mut Vec<PropertyAccess>,
        in_or: bool,
    ) -> Option<LogicalExpr> {
        match expr {
            // When we have an operator application, process it separately.
            LogicalExpr::OperatorApplicationExp(mut op_app) => {
                // Check if the current operator is an Or.
                let current_is_or = op_app.operator == Operator::Or;

                if current_is_or {
                    let cloned_op_app = LogicalExpr::OperatorApplicationExp(op_app.clone());
                    // If the entire OR belongs to single table then we extract it. This OR should not have any agg fns.
                    if Self::get_table_alias_if_single_table_condition(&cloned_op_app, false)
                        .is_some()
                    {
                        extracted_filters.push(op_app);
                        return None;
                    }
                }
                // Update our flag: once inside an Or, we stay inside.
                let new_in_or = in_or || current_is_or;

                // Process each operand recursively, passing the flag.
                let mut new_operands = Vec::new();
                for operand in op_app.operands {
                    if let Some(new_operand) = Self::process_expr(
                        operand,
                        extracted_filters,
                        extracted_projections,
                        new_in_or,
                    ) {
                        new_operands.push(new_operand);
                    }
                }
                // Update the operator application with the processed operands.
                op_app.operands = new_operands;

                // TODO ALl aggregated functions will be evaluated in final where clause. We have to check what kind of fns we can put here.
                // because if we put aggregated fns like count() then it will mess up the final result because we want the count of all joined entries in the set,
                // in case of anchor node this could lead incorrect answers.

                // let mut should_extract: bool = false;
                let mut temp_prop_acc: Vec<PropertyAccess> = vec![];
                let mut condition_belongs_to: HashSet<&str> = HashSet::new();
                let mut agg_operand_found = false;

                for operand in &op_app.operands {
                    // if any of the fn argument belongs to one table then extract it.
                    if let LogicalExpr::ScalarFnCall(fc) = operand {
                        for arg in &fc.args {
                            if let LogicalExpr::PropertyAccessExp(prop_acc) = arg {
                                condition_belongs_to.insert(&prop_acc.table_alias.0);
                                temp_prop_acc.push(prop_acc.clone());
                                // should_extract = true;
                            }
                        }
                    }
                    if let LogicalExpr::AggregateFnCall(fc) = operand {
                        for arg in &fc.args {
                            if let LogicalExpr::PropertyAccessExp(prop_acc) = arg {
                                condition_belongs_to.insert(&prop_acc.table_alias.0);
                                temp_prop_acc.push(prop_acc.clone());
                                // should_extract = false;
                                agg_operand_found = true;
                            }
                        }
                    } else if let LogicalExpr::PropertyAccessExp(prop_acc) = operand {
                        condition_belongs_to.insert(&prop_acc.table_alias.0);
                        temp_prop_acc.push(prop_acc.clone());
                        // should_extract = true;
                    }
                }

                // print!("\n\nOperator {:?}\n", op_app.operator);
                // println!("current_is_or: {}, new_in_or: {}, agg_operand_found: {}, condition_belongs_to: {:?}", current_is_or, new_in_or, agg_operand_found, condition_belongs_to);

                // if current_is_or && new_in_or {
                //     println!("\n operands: {:?}\n", op_app.operands);
                // }
                // if it is a multinode condition then we are not extracting. It will be kept at overall conditions
                // and applied at the end in the final query. This applies to OR conditions.
                // We won't extract OR conditions but add projections to their respective tables.
                if !new_in_or && !agg_operand_found && condition_belongs_to.len() == 1 {
                    extracted_filters.push(op_app);
                    return None;
                } else if new_in_or || condition_belongs_to.len() > 1 {
                    extracted_projections.append(&mut temp_prop_acc);
                }

                // If after processing there is only one operand left and it is not unary then collapse the operator application.
                if op_app.operands.len() == 1 && op_app.operator != Operator::Not {
                    return Some(op_app.operands.into_iter().next().unwrap()); // unwrap is safe we are checking the len in condition
                }

                // if both operands has been extracted then remove the parent op
                if op_app.operands.is_empty() {
                    return None;
                }

                // Otherwise, return the rebuilt operator application.
                Some(LogicalExpr::OperatorApplicationExp(op_app))
            }

            // If we have a function call, DO NOT process arguments recursively
            // Function arguments should remain intact - they're part of the function expression
            // Previously, this was extracting property access expressions from inside function arguments
            // which broke functions like abs(u.age - $param) by extracting the subtraction as a filter
            LogicalExpr::ScalarFnCall(fc) => {
                // Return function call unchanged - don't recurse into arguments
                Some(LogicalExpr::ScalarFnCall(fc))
            }

            LogicalExpr::AggregateFnCall(fc) => {
                // Return function call unchanged - don't recurse into arguments
                Some(LogicalExpr::AggregateFnCall(fc))
            }

            // For a list, process each element.
            LogicalExpr::List(exprs) => {
                let mut new_exprs = Vec::new();
                for sub_expr in exprs {
                    if let Some(new_expr) = Self::process_expr(
                        sub_expr,
                        extracted_filters,
                        extracted_projections,
                        in_or,
                    ) {
                        new_exprs.push(new_expr);
                    }
                }
                Some(LogicalExpr::List(new_exprs))
            }

            // Base cases – literals, variables, and property accesses remain unchanged.
            other => Some(other),
        }
    }

    // this function is used to get the table alias from an expression. We use this for OR conditions.
    // it is used to check if all the operands of an operator application have the same table alias.
    // if they don't then we return None.
    fn get_table_alias_if_single_table_condition(
        expr: &LogicalExpr,
        with_agg_fn: bool,
    ) -> Option<String> {
        match &expr {
            LogicalExpr::PropertyAccessExp(prop_acc) => Some(prop_acc.table_alias.0.clone()),
            LogicalExpr::OperatorApplicationExp(op_app) => {
                let mut found_table_alias_opt: Option<String> = None;
                for operand in &op_app.operands {
                    if let Some(current_table_alias) =
                        Self::get_table_alias_if_single_table_condition(operand, with_agg_fn)
                    {
                        if let Some(found_table_alias) = found_table_alias_opt.as_ref() {
                            if *found_table_alias != current_table_alias {
                                return None;
                            }
                        } else {
                            found_table_alias_opt = Some(current_table_alias.clone());
                        }
                    }
                }
                found_table_alias_opt
            }
            LogicalExpr::ScalarFnCall(scalar_fn_call) => {
                let mut found_table_alias_opt: Option<String> = None;
                for arg in &scalar_fn_call.args {
                    if let Some(current_table_alias) =
                        Self::get_table_alias_if_single_table_condition(arg, with_agg_fn)
                    {
                        if let Some(found_table_alias) = found_table_alias_opt.as_ref() {
                            if *found_table_alias != current_table_alias {
                                return None;
                            }
                        } else {
                            found_table_alias_opt = Some(current_table_alias.clone());
                        }
                    }
                }
                found_table_alias_opt
            }
            LogicalExpr::AggregateFnCall(aggregate_fn_call) => {
                let mut found_table_alias_opt: Option<String> = None;
                if with_agg_fn {
                    for arg in &aggregate_fn_call.args {
                        if let Some(current_table_alias) =
                            Self::get_table_alias_if_single_table_condition(arg, with_agg_fn)
                        {
                            if let Some(found_table_alias) = found_table_alias_opt.as_ref() {
                                if *found_table_alias != current_table_alias {
                                    return None;
                                }
                            } else {
                                found_table_alias_opt = Some(current_table_alias.clone());
                            }
                        }
                    }
                }
                found_table_alias_opt
            }
            _ => None,
        }
    }

    /// Check if an expression references any projection aliases
    /// Used to determine if a filter should become a HAVING clause
    fn references_projection_alias(expr: &LogicalExpr, plan_ctx: &PlanCtx) -> bool {
        match expr {
            LogicalExpr::TableAlias(TableAlias(alias)) => plan_ctx.is_projection_alias(alias),
            LogicalExpr::OperatorApplicationExp(op_app) => op_app
                .operands
                .iter()
                .any(|operand| Self::references_projection_alias(operand, plan_ctx)),
            LogicalExpr::ScalarFnCall(fn_call) => fn_call
                .args
                .iter()
                .any(|arg| Self::references_projection_alias(arg, plan_ctx)),
            LogicalExpr::AggregateFnCall(agg_call) => agg_call
                .args
                .iter()
                .any(|arg| Self::references_projection_alias(arg, plan_ctx)),
            LogicalExpr::List(exprs) => exprs
                .iter()
                .any(|e| Self::references_projection_alias(e, plan_ctx)),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Column, Literal, PropertyAccess, TableAlias};
    use crate::query_planner::logical_plan::{Filter, GraphNode, LogicalPlan, Scan};
    use crate::query_planner::plan_ctx::TableCtx;

    fn create_property_access(table: &str, column: &str) -> LogicalExpr {
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table.to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(column.to_string()),
        })
    }

    fn create_simple_filter(table: &str, column: &str, value: i64) -> LogicalExpr {
        LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                create_property_access(table, column),
                LogicalExpr::Literal(Literal::Integer(value)),
            ],
        })
    }

    fn setup_plan_ctx_with_tables() -> PlanCtx {
        let mut plan_ctx = PlanCtx::default();

        // Add user table (node)
        plan_ctx.insert_table_ctx(
            "user".to_string(),
            TableCtx::build(
                "user".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        // Add follows table (relationship)
        plan_ctx.insert_table_ctx(
            "follows".to_string(),
            TableCtx::build(
                "follows".to_string(),
                Some(vec!["FOLLOWS".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        // Add company table (node)
        plan_ctx.insert_table_ctx(
            "company".to_string(),
            TableCtx::build(
                "company".to_string(),
                Some(vec!["Company".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        plan_ctx
    }

    fn setup_test_graph_schema() -> GraphSchema {
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
        use std::collections::HashMap;

        let mut node_schemas = HashMap::new();
        let mut rel_schemas = HashMap::new();

        // Person node with properties
        let mut person_props = HashMap::new();
        person_props.insert("age".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("age".to_string()));
        person_props.insert("status".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("status".to_string()));
        person_props.insert("name".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()));

        node_schemas.insert(
            "Person".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "users".to_string(),
                column_names: vec![
                    "user_id".to_string(),
                    "name".to_string(),
                    "age".to_string(),
                    "status".to_string(),
                ],
                primary_keys: "user_id".to_string(),
                node_id: NodeIdSchema {
                    column: "user_id".to_string(),
                    dtype: "UInt32".to_string(),
                },
                property_mappings: person_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
            },
        );

        // Company node
        let mut company_props = HashMap::new();
        company_props.insert("name".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()));
        company_props.insert("owner_id".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("owner_id".to_string()));

        node_schemas.insert(
            "Company".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "companies".to_string(),
                column_names: vec![
                    "company_id".to_string(),
                    "name".to_string(),
                    "owner_id".to_string(),
                ],
                primary_keys: "company_id".to_string(),
                node_id: NodeIdSchema {
                    column: "company_id".to_string(),
                    dtype: "UInt32".to_string(),
                },
                property_mappings: company_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
            },
        );

        // FOLLOWS relationship
        let mut follows_props = HashMap::new();
        follows_props.insert("since".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("created_at".to_string()));

        rel_schemas.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "follows".to_string(),
                column_names: vec![
                    "from_node_id".to_string(),
                    "to_node_id".to_string(),
                    "created_at".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Person".to_string(),
                from_id: "from_node_id".to_string(),
                to_id: "to_node_id".to_string(),
                from_node_id_dtype: "UInt32".to_string(),
                to_node_id_dtype: "UInt32".to_string(),
                property_mappings: follows_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_node_properties: None,
                to_node_properties: None,
            },
        );

        GraphSchema::build(1, "test_db".to_string(), node_schemas, rel_schemas)
    }
    #[test]
    fn test_single_table_filter_extraction() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test filter: user.age = 25
        let filter_expr = create_simple_filter("user", "age", 25);
        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should extract the filter completely (no remaining filter)
        assert!(result.is_none());

        // Should tag the filter to user table
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);

        // Filter should keep PropertyAccessExp with table_alias (not converted to Column)
        match &user_ctx.get_filters()[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                match &op_app.operands[0] {
                    LogicalExpr::PropertyAccessExp(prop_acc) => {
                        assert_eq!(prop_acc.table_alias.0, "user");
                        assert_eq!(prop_acc.column.raw(), "age");
                    }
                    _ => panic!("Expected PropertyAccessExp (not Column) to preserve table_alias"),
                }
                match &op_app.operands[1] {
                    LogicalExpr::Literal(Literal::Integer(val)) => assert_eq!(*val, 25),
                    _ => panic!("Expected Integer literal"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_relationship_filter_sets_edge_list() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test filter on relationship: follows.since > 2020
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                create_property_access("follows", "since"),
                LogicalExpr::Literal(Literal::Integer(2020)),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();
        assert!(result.is_none());

        // Filter should be tagged to follows table
        let follows_ctx = plan_ctx.get_table_ctx("follows").unwrap();
        assert_eq!(follows_ctx.get_filters().len(), 1);
    }

    #[test]
    fn test_multi_table_condition_not_extracted() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test multi-table condition: user.id = company.owner_id
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                create_property_access("user", "id"),
                create_property_access("company", "owner_id"),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should NOT extract the filter (remains in final where clause)
        assert!(result.is_some());
        match result.unwrap() {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                assert_eq!(op_app.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplication to remain"),
        }

        // Should add projections to both tables but no filters
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        let company_ctx = plan_ctx.get_table_ctx("company").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 0);
        assert_eq!(company_ctx.get_filters().len(), 0);
        assert_eq!(user_ctx.get_projections().len(), 1);
        assert_eq!(company_ctx.get_projections().len(), 1);
    }

    #[test]
    fn test_or_condition_single_table_extracted() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test OR condition: user.age = 25 OR user.status = 'active'
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Or,
            operands: vec![
                create_simple_filter("user", "age", 25),
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        create_property_access("user", "status"),
                        LogicalExpr::Literal(Literal::String("active".to_string())),
                    ],
                }),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should NOT extract filters inside OR (remains in final where clause)
        assert!(result.is_none());

        // Should extract filters to user table but should add projections
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);
    }

    #[test]
    fn test_or_condition_multi_table_not_extracted() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test OR condition: user.age = 25 OR company.status = 'active'
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Or,
            operands: vec![
                create_simple_filter("user", "age", 25),
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        create_property_access("company", "status"),
                        LogicalExpr::Literal(Literal::String("active".to_string())),
                    ],
                }),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should NOT extract filters inside OR (remains in final where clause)
        assert!(result.is_some());
        match result.unwrap() {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Or);
                assert_eq!(op_app.operands.len(), 2);
            }
            _ => panic!("Expected OR condition to remain"),
        }

        // Should not extract any filters to user table but should add projections
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 0);
        // Should add projections for the property accesses in OR condition
        assert_eq!(user_ctx.get_projections().len(), 1); // age 

        // Should not extract any filters to user table but should add projections
        let company_ctx = plan_ctx.get_table_ctx("company").unwrap();
        assert_eq!(company_ctx.get_filters().len(), 0);
        // Should add projections for the property accesses in OR condition
        assert_eq!(company_ctx.get_projections().len(), 1); // status 
    }

    #[test]
    fn test_aggregate_function_not_extracted() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test aggregate condition: count(user.id) > 5
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: "count".to_string(),
                    args: vec![create_property_access("user", "id")],
                }),
                LogicalExpr::Literal(Literal::Integer(5)),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should NOT extract aggregate conditions (remains in final where clause)
        assert!(result.is_some());
        match result.unwrap() {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::GreaterThan);
                assert_eq!(op_app.operands.len(), 2);
            }
            _ => panic!("Expected aggregate condition to remain"),
        }

        // Should not extract any filters but may add projections
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 0);
    }

    #[test]
    fn test_scalar_function_extraction() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test scalar function: length(user.name) > 10
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: "length".to_string(),
                    args: vec![create_property_access("user", "name")],
                }),
                LogicalExpr::Literal(Literal::Integer(10)),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should extract scalar function filters
        assert!(result.is_none());

        // Should tag the filter to user table with converted function
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);

        match &user_ctx.get_filters()[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                match &op_app.operands[0] {
                    LogicalExpr::ScalarFnCall(fc) => {
                        assert_eq!(fc.name, "length");
                        // Function arg should keep PropertyAccessExp to preserve table_alias
                        match &fc.args[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.table_alias.0, "user");
                                assert_eq!(prop_acc.column.raw(), "name");
                            }
                            _ => panic!("Expected PropertyAccessExp to preserve table_alias"),
                        }
                    }
                    _ => panic!("Expected ScalarFnCall"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_and_condition_with_mixed_extractable_filters() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();

        // Test AND with extractable and non-extractable: user.age = 25 AND user.id = company.owner_id
        let filter_expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![
                create_simple_filter("user", "age", 25), // Extractable (single table)
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    // Not extractable (multi-table)
                    operator: Operator::Equal,
                    operands: vec![
                        create_property_access("user", "id"),
                        create_property_access("company", "owner_id"),
                    ],
                }),
            ],
        });

        let result = analyzer
            .extract_filters(filter_expr, &mut plan_ctx)
            .unwrap();

        // Should partially extract: single-table filter extracted, multi-table remains
        assert!(result.is_some());
        match result.unwrap() {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal); // The multi-table condition remains
                assert_eq!(op_app.operands.len(), 2);
            }
            _ => panic!("Expected remaining multi-table condition"),
        }

        // Should extract the single-table filter to user
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);

        // Should add projections for multi-table condition
        assert!(user_ctx.get_projections().len() >= 1);
        let company_ctx = plan_ctx.get_table_ctx("company").unwrap();
        assert!(company_ctx.get_projections().len() >= 1);
    }

    #[test]
    fn test_filter_node_removal_when_all_extracted() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();
        let graph_schema = setup_test_graph_schema();

        // Create a Filter node with completely extractable predicate
        let scan = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("user".to_string()),
            table_name: Some("users".to_string()),
        }));

        let filter = Arc::new(LogicalPlan::Filter(Filter {
            input: scan.clone(),
            predicate: create_simple_filter("user", "age", 30),
        }));

        let result = analyzer
            .analyze_with_graph_schema(filter, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should remove Filter node and return the scan directly
        match result {
            Transformed::Yes(new_plan) => {
                assert_eq!(new_plan, scan); // Should return the scan directly
            }
            _ => panic!("Expected transformation that removes filter"),
        }

        // Filter should be tagged to user table
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);
    }

    #[test]
    fn test_complex_nested_logical_plan_traversal() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = setup_plan_ctx_with_tables();
        let graph_schema = setup_test_graph_schema();

        // Create complex nested plan: GraphNode -> Filter -> Scan
        let scan = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("user".to_string()),
            table_name: Some("users".to_string()),
        }));

        let filter = Arc::new(LogicalPlan::Filter(Filter {
            input: scan,
            predicate: create_simple_filter("user", "status", 1),
        }));

        let graph_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: filter,
            alias: "user".to_string(),
            is_denormalized: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(graph_node, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should transform the nested structure
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GraphNode(node) => {
                        // The filter should be removed and scan should be direct input
                        assert_eq!(node.alias, "user");
                        match node.input.as_ref() {
                            LogicalPlan::Scan(scan) => {
                                assert_eq!(scan.table_alias, Some("user".to_string()));
                            }
                            _ => panic!("Expected scan as direct input after filter removal"),
                        }
                    }
                    _ => panic!("Expected GraphNode at top level"),
                }
            }
            _ => panic!("Expected transformation"),
        }

        // Filter should be tagged to user table
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_filters().len(), 1);
    }

    #[test]
    fn test_orphan_alias_error() {
        let analyzer = FilterTagging::new();
        let mut plan_ctx = PlanCtx::default(); // Empty plan context

        // Test filter referencing non-existent table
        let filter_expr = create_simple_filter("nonexistent", "column", 42);
        let result = analyzer.extract_filters(filter_expr, &mut plan_ctx);

        // Should return error for orphan alias
        assert!(result.is_err());
        match result.unwrap_err() {
            AnalyzerError::OrphanAlias { pass, alias } => {
                assert_eq!(pass, Pass::FilterTagging);
                assert_eq!(alias, "nonexistent");
            }
            _ => panic!("Expected OrphanAlias error"),
        }
    }

    #[test]
    fn test_get_table_alias_single_property_access() {
        // Test single property access: user.name
        let expr = create_property_access("user", "name");
        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);

        assert_eq!(result, Some("user".to_string()));
    }

    #[test]
    fn test_get_table_alias_operator_application_same_table() {
        // Test operator with same table: user.age = 25
        let expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                create_property_access("user", "age"),
                LogicalExpr::Literal(Literal::Integer(25)),
            ],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, Some("user".to_string()));
    }

    #[test]
    fn test_get_table_alias_operator_application_different_tables() {
        // Test operator with different tables: user.id = company.owner_id
        let expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                create_property_access("user", "id"),
                create_property_access("company", "owner_id"),
            ],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_table_alias_scalar_function_same_table() {
        // Test scalar function with same table: length(user.name)
        let expr = LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: "length".to_string(),
            args: vec![create_property_access("user", "name")],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, Some("user".to_string()));
    }

    #[test]
    fn test_get_table_alias_scalar_function_different_tables() {
        // Test scalar function with different tables: concat(user.first_name, company.suffix)
        let expr = LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: "concat".to_string(),
            args: vec![
                create_property_access("user", "first_name"),
                create_property_access("company", "suffix"),
            ],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_table_alias_aggregate_function_with_agg_fn_true() {
        // Test aggregate function with with_agg_fn=true: count(user.id)
        let expr = LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![create_property_access("user", "id")],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, true);
        assert_eq!(result, Some("user".to_string()));
    }

    #[test]
    fn test_get_table_alias_aggregate_function_with_agg_fn_false() {
        // Test aggregate function with with_agg_fn=false: count(user.id)
        let expr = LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![create_property_access("user", "id")],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, None); // Should return None when with_agg_fn is false
    }

    #[test]
    fn test_get_table_alias_mixed_expression_same_table() {
        // Test mixed expression with scalar function and property: length(user.name) > user.min_length
        let expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: "length".to_string(),
                    args: vec![create_property_access("user", "name")],
                }),
                create_property_access("user", "min_length"),
            ],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, Some("user".to_string()));
    }

    #[test]
    fn test_get_table_alias_literals_only() {
        // Test expression with only literals: 42 = 42
        let expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                LogicalExpr::Literal(Literal::Integer(42)),
                LogicalExpr::Literal(Literal::Integer(42)),
            ],
        });

        let result = FilterTagging::get_table_alias_if_single_table_condition(&expr, false);
        assert_eq!(result, None); // No property accesses, should return None
    }
}
