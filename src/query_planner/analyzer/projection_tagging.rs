use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::{
            AggregateFnCall, ColumnAlias, LambdaExpr, LogicalCase, LogicalExpr, Operator,
            OperatorApplication, PropertyAccess, ScalarFnCall, TableAlias,
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
                crate::debug_println!(
                    "🔍 ProjectionTagging: BEFORE processing Projection - distinct={}",
                    projection.distinct
                );
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
                crate::debug_print!(
                    "ProjectionTagging: input items count={}, items={:?}",
                    projection.items.len(),
                    projection.items
                );
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
                    distinct: projection.distinct,
                })));
                crate::debug_println!(
                    "🔍 ProjectionTagging: AFTER creating new Projection - distinct={}",
                    projection.distinct
                );
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
            LogicalPlan::Unwind(u) => {
                let child_tf =
                    self.analyze_with_graph_schema(u.input.clone(), plan_ctx, graph_schema)?;

                // Transform the UNWIND expression - resolve property mappings for denormalized nodes
                let transformed_expr =
                    self.transform_unwind_expression(&u.expression, plan_ctx, graph_schema)?;

                // Check if anything changed
                let expr_changed = transformed_expr != u.expression;

                match (&child_tf, expr_changed) {
                    (Transformed::Yes(new_input), _) => Transformed::Yes(Arc::new(
                        LogicalPlan::Unwind(crate::query_planner::logical_plan::Unwind {
                            input: new_input.clone(),
                            expression: transformed_expr,
                            alias: u.alias.clone(),
                        }),
                    )),
                    (Transformed::No(_), true) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(
                        crate::query_planner::logical_plan::Unwind {
                            input: u.input.clone(),
                            expression: transformed_expr,
                            alias: u.alias.clone(),
                        },
                    ))),
                    (Transformed::No(_), false) => Transformed::No(logical_plan.clone()),
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

    /// Transform UNWIND expression to resolve property mappings for denormalized nodes
    /// For example: UNWIND rip.ips -> UNWIND rip.answers (when ips maps to answers column)
    fn transform_unwind_expression(
        &self,
        expr: &LogicalExpr,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<LogicalExpr> {
        match expr {
            LogicalExpr::PropertyAccessExp(property_access) => {
                // Get the table context for this alias - if not found, just return as-is
                let table_ctx = match plan_ctx.get_table_ctx(&property_access.table_alias.0) {
                    Ok(ctx) => ctx,
                    Err(_) => return Ok(expr.clone()),
                };

                let label = table_ctx.get_label_opt().unwrap_or_default();

                // Check if this is a denormalized node
                if let Ok(node_schema) = graph_schema.get_node_schema(&label) {
                    if node_schema.is_denormalized {
                        // Try to resolve from to_node_properties first (common for denormalized end nodes)
                        // then from_node_properties
                        let mapped_column = if let Some(ref to_props) = node_schema.to_properties {
                            if let Some(mapped) = to_props.get(property_access.column.raw()) {
                                Some(crate::graph_catalog::expression_parser::PropertyValue::Column(mapped.clone()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }.or_else(|| {
                            if let Some(ref from_props) = node_schema.from_properties {
                                if let Some(mapped) = from_props.get(property_access.column.raw()) {
                                    Some(crate::graph_catalog::expression_parser::PropertyValue::Column(mapped.clone()))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        });

                        if let Some(mapped) = mapped_column {
                            log::debug!(
                                "UNWIND property mapping: {}.{} -> {:?}",
                                property_access.table_alias.0,
                                property_access.column.raw(),
                                mapped
                            );
                            return Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: property_access.table_alias.clone(),
                                column: mapped,
                            }));
                        }
                    }
                }

                // No mapping needed - return as-is
                Ok(expr.clone())
            }
            // For other expression types, return as-is
            _ => Ok(expr.clone()),
        }
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
                    // IMPORTANT: Set col_alias to preserve the original alias name (e.g., "src.*")
                    // This allows later processing (especially in denormalized schemas) to
                    // recover which node's properties should be expanded
                    item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: table_alias.clone(),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            "*".to_string(),
                        ),
                    });
                    item.col_alias = Some(ColumnAlias(format!("{}.*", table_alias.0)));
                    Ok(())
                }
            }
            LogicalExpr::PropertyAccessExp(property_access) => {
                crate::debug_print!(
                    "tag_projection PropertyAccessExp: table_alias='{}', column='{}'",
                    property_access.table_alias.0,
                    property_access.column.raw()
                );

                let table_ctx = plan_ctx
                    .get_mut_table_ctx(&property_access.table_alias.0)
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::ProjectionTagging,
                        source: e,
                    })?;

                crate::debug_print!(
                    "tag_projection: table_ctx label={:?}, is_relation={}",
                    table_ctx.get_label_opt(),
                    table_ctx.is_relation()
                );

                // Get label for property resolution
                let label = table_ctx.get_label_opt().unwrap_or_default();
                let is_relation = table_ctx.is_relation();

                // Resolve property to actual column name using ViewResolver
                // This handles standard property_mappings
                // TODO: For denormalized nodes, we need to check from_node_properties/to_node_properties
                let view_resolver =
                    crate::query_planner::analyzer::view_resolver::ViewResolver::from_schema(
                        graph_schema,
                    );

                let mapped_column = if is_relation {
                    // Get connected node labels for polymorphic relationship resolution
                    let from_node = table_ctx.get_from_node_label().map(|s| s.as_str());
                    let to_node = table_ctx.get_to_node_label().map(|s| s.as_str());
                    view_resolver
                        .resolve_relationship_property(&label, property_access.column.raw(), from_node, to_node)?
                } else {
                    // Check if this node is denormalized by looking up the schema
                    if let Ok(node_schema) = graph_schema.get_node_schema(&label) {
                        if node_schema.is_denormalized {
                            // For denormalized nodes, prefer from_node_properties
                            // (TO position would need UNION ALL which we handle separately)
                            if let Some(ref from_props) = node_schema.from_properties {
                                if let Some(mapped) = from_props.get(property_access.column.raw()) {
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        mapped.clone(),
                                    )
                                } else {
                                    // Property not in from_props, try to_props
                                    if let Some(ref to_props) = node_schema.to_properties {
                                        if let Some(mapped) =
                                            to_props.get(property_access.column.raw())
                                        {
                                            crate::graph_catalog::expression_parser::PropertyValue::Column(mapped.clone())
                                        } else {
                                            // Fallback to identity
                                            crate::graph_catalog::expression_parser::PropertyValue::Column(property_access.column.raw().to_string())
                                        }
                                    } else {
                                        crate::graph_catalog::expression_parser::PropertyValue::Column(property_access.column.raw().to_string())
                                    }
                                }
                            } else if let Some(ref to_props) = node_schema.to_properties {
                                if let Some(mapped) = to_props.get(property_access.column.raw()) {
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        mapped.clone(),
                                    )
                                } else {
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        property_access.column.raw().to_string(),
                                    )
                                }
                            } else {
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    property_access.column.raw().to_string(),
                                )
                            }
                        } else {
                            // Standard node - use ViewResolver
                            view_resolver
                                .resolve_node_property(&label, property_access.column.raw())?
                        }
                    } else {
                        // Label not found in schema - use property as column name (identity mapping)
                        crate::graph_catalog::expression_parser::PropertyValue::Column(
                            property_access.column.raw().to_string(),
                        )
                    }
                };

                // Update the property access with the mapped column
                let updated_property_access = PropertyAccess {
                    table_alias: property_access.table_alias.clone(),
                    column: mapped_column,
                };

                // Create updated projection item with mapped column
                let updated_item = ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(updated_property_access.clone()),
                    col_alias: item.col_alias.clone(),
                };

                table_ctx.insert_projection(updated_item.clone());

                // Update the item's expression with the mapped column
                item.expression = LogicalExpr::PropertyAccessExp(updated_property_access);

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
                let fn_name_lower = scalar_fn_call.name.to_lowercase();

                // Handle graph introspection functions specially
                // These functions take a node/relationship alias and shouldn't be expanded to .*
                if matches!(fn_name_lower.as_str(), "type" | "id" | "labels" | "label") {
                    // Get the first argument (the node/relationship alias)
                    if let Some(LogicalExpr::TableAlias(TableAlias(alias))) =
                        scalar_fn_call.args.first()
                    {
                        let table_ctx = plan_ctx.get_mut_table_ctx(alias).map_err(|e| {
                            AnalyzerError::PlanCtx {
                                pass: Pass::ProjectionTagging,
                                source: e,
                            }
                        })?;

                        match fn_name_lower.as_str() {
                            "type" => {
                                // For type(r):
                                // - Polymorphic edge with type_column -> PropertyAccessExp(r.type_column)
                                // - Non-polymorphic -> Literal string of the relationship type
                                if table_ctx.is_relation() {
                                    // If no explicit alias, use "type(r)" as the column alias
                                    if item.col_alias.is_none() {
                                        item.col_alias =
                                            Some(ColumnAlias(format!("type({})", alias)));
                                    }
                                    if let Some(labels) = table_ctx.get_labels() {
                                        if let Some(first_label) = labels.first() {
                                            // Check if this relationship has a type_column (polymorphic)
                                            if let Ok(rel_schema) =
                                                graph_schema.get_rel_schema(first_label)
                                            {
                                                if let Some(ref type_col) = rel_schema.type_column {
                                                    // Polymorphic: return type column
                                                    item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(alias.clone()),
                                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(type_col.clone()),
                                                    });
                                                } else {
                                                    // Non-polymorphic: return literal type name
                                                    item.expression = LogicalExpr::Literal(
                                                        crate::query_planner::logical_expr::Literal::String(first_label.clone())
                                                    );
                                                }
                                                return Ok(());
                                            }
                                        }
                                    }
                                    // Fallback: return '*' (unknown type)
                                    item.expression = LogicalExpr::Literal(
                                        crate::query_planner::logical_expr::Literal::String(
                                            "*".to_string(),
                                        ),
                                    );
                                    return Ok(());
                                }
                                // type() on a node doesn't make sense in standard Cypher, keep as-is
                            }
                            "id" => {
                                // For id(n): return the id column(s) as PropertyAccessExp or Tuple
                                // If no explicit alias, use "id(r)" or "id(n)" as the column alias
                                if item.col_alias.is_none() {
                                    item.col_alias = Some(ColumnAlias(format!("id({})", alias)));
                                }
                                if let Ok(label) = table_ctx.get_label_str() {
                                    if table_ctx.is_relation() {
                                        // Relationship ID - may be single or composite
                                        if let Ok(rel_schema) = graph_schema.get_rel_schema(&label)
                                        {
                                            if let Some(ref edge_id) = rel_schema.edge_id {
                                                let columns = edge_id.columns();
                                                if columns.len() == 1 {
                                                    // Single column edge ID
                                                    item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                        table_alias: TableAlias(alias.clone()),
                                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(columns[0].to_string()),
                                                    });
                                                } else {
                                                    // Composite edge ID - return as tuple (List)
                                                    // This enables round-trip: id(r) returns (col1, col2, ...)
                                                    // and WHERE id(r) = (val1, val2, ...) works
                                                    let tuple_exprs: Vec<LogicalExpr> = columns.iter()
                                                        .map(|col| LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                            table_alias: TableAlias(alias.clone()),
                                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(col.to_string()),
                                                        }))
                                                        .collect();
                                                    item.expression =
                                                        LogicalExpr::List(tuple_exprs);
                                                }
                                            } else {
                                                // No edge_id defined - use from_id as default
                                                item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                    table_alias: TableAlias(alias.clone()),
                                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_schema.from_id.clone()),
                                                });
                                            }
                                            return Ok(());
                                        }
                                        // Fallback for unknown relationship
                                        item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(alias.clone()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column("id".to_string()),
                                        });
                                        return Ok(());
                                    } else {
                                        // Node ID column - use first column for composite IDs
                                        let id_column = if let Ok(node_schema) =
                                            graph_schema.get_node_schema(&label)
                                        {
                                            node_schema
                                                .node_id
                                                .columns()
                                                .first()
                                                .unwrap_or(&"id")
                                                .to_string()
                                        } else {
                                            "id".to_string()
                                        };
                                        item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(alias.clone()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_column),
                                        });
                                        return Ok(());
                                    }
                                }
                            }
                            "labels" => {
                                // For labels(n): return an array literal with the node's label(s)
                                // If no explicit alias, use "labels(n)" as the column alias
                                if item.col_alias.is_none() {
                                    item.col_alias =
                                        Some(ColumnAlias(format!("labels({})", alias)));
                                }
                                if !table_ctx.is_relation() {
                                    if let Some(labels) = table_ctx.get_labels() {
                                        // Create array literal: ['Label1', 'Label2', ...]
                                        let label_exprs: Vec<LogicalExpr> = labels.iter()
                                            .map(|l| LogicalExpr::Literal(
                                                crate::query_planner::logical_expr::Literal::String(l.clone())
                                            ))
                                            .collect();
                                        item.expression = LogicalExpr::List(label_exprs);
                                        return Ok(());
                                    }
                                }
                            }
                            "label" => {
                                // For label(n): return a single label as a scalar string
                                // This is useful when you know a node has exactly one label
                                // If no explicit alias, use "label(n)" as the column alias
                                if item.col_alias.is_none() {
                                    item.col_alias = Some(ColumnAlias(format!("label({})", alias)));
                                }
                                if !table_ctx.is_relation() {
                                    if let Some(labels) = table_ctx.get_labels() {
                                        if let Some(first_label) = labels.first() {
                                            // Return the first label as a scalar string
                                            item.expression = LogicalExpr::Literal(
                                                crate::query_planner::logical_expr::Literal::String(
                                                    first_label.clone(),
                                                ),
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }

                // Generic scalar function - recursively process arguments
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
                    // Track whether DISTINCT was used in the original expression
                    let (table_alias_opt, is_distinct) = match arg {
                        LogicalExpr::TableAlias(TableAlias(t_alias)) => {
                            (Some(t_alias.as_str()), false)
                        }
                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator,
                            operands,
                        }) if *operator == Operator::Distinct && operands.len() == 1 => {
                            // Handle DISTINCT a inside COUNT(DISTINCT a)
                            if let LogicalExpr::TableAlias(TableAlias(t_alias)) = &operands[0] {
                                (Some(t_alias.as_str()), true)
                            } else {
                                (None, false)
                            }
                        }
                        _ => (None, false),
                    };

                    if let Some(t_alias) = table_alias_opt {
                        if aggregate_fn_call.name.to_lowercase() == "count" {
                            // First check if this is a projection alias (from WITH clause)
                            // If so, resolve it to the underlying table alias
                            let resolved_alias: String = if plan_ctx.is_projection_alias(t_alias) {
                                // Try to resolve the projection alias to its underlying expression
                                if let Some(underlying_expr) =
                                    plan_ctx.get_projection_alias_expr(t_alias)
                                {
                                    // If the underlying expr is a TableAlias, use that
                                    match underlying_expr {
                                        LogicalExpr::TableAlias(TableAlias(underlying_alias)) => {
                                            underlying_alias.clone()
                                        }
                                        _ => {
                                            // If it's not a simple alias (e.g., it's an aggregate),
                                            // just use count(*) since we can't resolve to a table
                                            item.expression =
                                                LogicalExpr::AggregateFnCall(AggregateFnCall {
                                                    name: aggregate_fn_call.name.clone(),
                                                    args: vec![LogicalExpr::Star],
                                                });
                                            return Ok(());
                                        }
                                    }
                                } else {
                                    t_alias.to_string()
                                }
                            } else {
                                t_alias.to_string()
                            };

                            let table_ctx =
                                plan_ctx.get_mut_table_ctx(&resolved_alias).map_err(|e| {
                                    AnalyzerError::PlanCtx {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    }
                                })?;

                            if table_ctx.is_relation() {
                                // For relationships:
                                // - count(r) -> count(*) (count all relationship rows)
                                // - count(DISTINCT r) -> count(DISTINCT (edge_id_columns...))
                                if is_distinct {
                                    // Get the relationship type from the table context
                                    if let Some(rel_type) = table_ctx.get_label_opt() {
                                        // Look up the relationship schema
                                        if let Some(rel_schema) =
                                            graph_schema.get_relationships_schema_opt(&rel_type)
                                        {
                                            // Get edge_id columns or default to (from_id, to_id)
                                            let edge_columns: Vec<String> =
                                                match &rel_schema.edge_id {
                                                    Some(id) => id
                                                        .columns()
                                                        .iter()
                                                        .map(|s| s.to_string())
                                                        .collect(),
                                                    None => vec![
                                                        rel_schema.from_id.clone(),
                                                        rel_schema.to_id.clone(),
                                                    ],
                                                };

                                            // Create PropertyAccess expressions for each edge column
                                            let column_exprs: Vec<LogicalExpr> = edge_columns.iter().map(|col| {
                                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                    table_alias: TableAlias(t_alias.to_string()),
                                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(col.clone()),
                                                })
                                            }).collect();

                                            // Create the DISTINCT tuple expression
                                            let distinct_arg = if column_exprs.len() == 1 {
                                                // Single column - just use DISTINCT on that column
                                                LogicalExpr::OperatorApplicationExp(
                                                    OperatorApplication {
                                                        operator: Operator::Distinct,
                                                        operands: column_exprs,
                                                    },
                                                )
                                            } else {
                                                // Multiple columns - create tuple using scalar function: DISTINCT tuple(col1, col2, ...)
                                                LogicalExpr::OperatorApplicationExp(
                                                    OperatorApplication {
                                                        operator: Operator::Distinct,
                                                        operands: vec![LogicalExpr::ScalarFnCall(
                                                            ScalarFnCall {
                                                                name: "tuple".to_string(),
                                                                args: column_exprs,
                                                            },
                                                        )],
                                                    },
                                                )
                                            };

                                            item.expression =
                                                LogicalExpr::AggregateFnCall(AggregateFnCall {
                                                    name: aggregate_fn_call.name.clone(),
                                                    args: vec![distinct_arg],
                                                });
                                        } else {
                                            // Fallback to count(*) if schema lookup fails
                                            item.expression =
                                                LogicalExpr::AggregateFnCall(AggregateFnCall {
                                                    name: aggregate_fn_call.name.clone(),
                                                    args: vec![LogicalExpr::Star],
                                                });
                                        }
                                    } else {
                                        // No relationship type found, fallback to count(*)
                                        item.expression =
                                            LogicalExpr::AggregateFnCall(AggregateFnCall {
                                                name: aggregate_fn_call.name.clone(),
                                                args: vec![LogicalExpr::Star],
                                            });
                                    }
                                } else {
                                    // count(r) without DISTINCT -> count(*)
                                    item.expression =
                                        LogicalExpr::AggregateFnCall(AggregateFnCall {
                                            name: aggregate_fn_call.name.clone(),
                                            args: vec![LogicalExpr::Star],
                                        });
                                }
                            } else if table_ctx.is_path_variable() {
                                // For path variables (e.g., count(p) where p is from MATCH p = ...),
                                // count the number of paths which equals the number of rows
                                item.expression = LogicalExpr::AggregateFnCall(AggregateFnCall {
                                    name: aggregate_fn_call.name.clone(),
                                    args: vec![LogicalExpr::Star],
                                });
                            } else {
                                // For nodes, count distinct node IDs
                                // Check if this is a denormalized node first
                                let table_label = table_ctx.get_label_str().map_err(|e| {
                                    AnalyzerError::PlanCtx {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    }
                                })?;

                                // Check if node is denormalized
                                if graph_schema.is_denormalized_node(&table_label) {
                                    // For denormalized nodes, get the node schema to find the id_column property name
                                    // The id_column specifies which property represents the node's identity
                                    // e.g., for IP nodes, id_column = "ip", so count(distinct ip) -> count(distinct ip.ip)
                                    let node_schema = graph_schema
                                        .get_node_schema(&table_label)
                                        .map_err(|e| AnalyzerError::GraphSchema {
                                            pass: Pass::ProjectionTagging,
                                            source: e,
                                        })?;
                                    let id_property_name = node_schema
                                        .node_id
                                        .columns()
                                        .first()
                                        .unwrap_or(&"id")
                                        .to_string();

                                    log::debug!(
                                        "ProjectionTagging: Denormalized node '{}' (label={}), using id property '{}'",
                                        t_alias, table_label, id_property_name
                                    );

                                    // Check if DISTINCT was specified
                                    let is_distinct = matches!(arg, LogicalExpr::OperatorApplicationExp(OperatorApplication { operator, .. }) if *operator == Operator::Distinct);

                                    // Create PropertyAccess expression for node.id_property
                                    let property_expr = LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(t_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_property_name),
                                    });

                                    let new_arg = if is_distinct {
                                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                            operator: Operator::Distinct,
                                            operands: vec![property_expr],
                                        })
                                    } else {
                                        property_expr
                                    };

                                    item.expression =
                                        LogicalExpr::AggregateFnCall(AggregateFnCall {
                                            name: aggregate_fn_call.name.clone(),
                                            args: vec![new_arg],
                                        });
                                } else {
                                    // Standard node - use node schema's ID column
                                    let table_schema = graph_schema
                                        .get_node_schema(&table_label)
                                        .map_err(|e| AnalyzerError::GraphSchema {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    })?;
                                    let table_node_id = table_schema
                                        .node_id
                                        .columns()
                                        .first()
                                        .unwrap_or(&"id")
                                        .to_string();

                                    // Preserve DISTINCT if it was in the original expression
                                    let new_arg = if matches!(arg, LogicalExpr::OperatorApplicationExp(OperatorApplication { operator, .. }) if *operator == Operator::Distinct)
                                    {
                                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                            operator: Operator::Distinct,
                                            operands: vec![LogicalExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: TableAlias(t_alias.to_string()),
                                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(table_node_id),
                                                },
                                            )],
                                        })
                                    } else {
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(t_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(table_node_id),
                                        })
                                    };

                                    item.expression =
                                        LogicalExpr::AggregateFnCall(AggregateFnCall {
                                            name: aggregate_fn_call.name.clone(),
                                            args: vec![new_arg],
                                        });
                                }
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
            LogicalExpr::Lambda(lambda_expr) => {
                // Lambda expressions need special handling:
                // - Lambda parameters are local variables (don't resolve them)
                // - Lambda body may contain references that need resolution
                // We recursively transform the body expression
                let mut body_item = ProjectionItem {
                    expression: (*lambda_expr.body).clone(),
                    col_alias: None,
                };
                Self::tag_projection(&mut body_item, plan_ctx, graph_schema)?;
                
                item.expression = LogicalExpr::Lambda(LambdaExpr {
                    params: lambda_expr.params.clone(),
                    body: Box::new(body_item.expression),
                });
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
