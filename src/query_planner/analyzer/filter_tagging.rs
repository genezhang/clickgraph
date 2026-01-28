//! Filter tagging pass for optimizing filter placement
//!
//! This analyzer pass handles the tagging and placement of filter predicates
//! to enable optimal SQL WHERE clause generation.
//!
//! ## Key Responsibilities
//!
//! - **Filter Extraction**: Move single-table conditions closer to their source tables
//! - **Property Mapping**: Convert Cypher properties to database column names
//! - **Edge-list Tagging**: Mark filters that apply to relationships
//! - **Projection Alias Detection**: Handle aggregation results and HAVING clauses
//!
//! ## Architecture
//!
//! The pass traverses the logical plan tree and:
//! 1. Identifies filter predicates that reference single tables
//! 2. Tags filters with their owning table for later optimization
//! 3. Handles denormalized patterns where node data is in edge tables
//! 4. Preserves multi-table conditions for proper JOIN ordering
//!
//! Some methods in this module are reserved for future filter optimization passes.
#![allow(dead_code)]

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
            LogicalPlan::Empty => "Empty",
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
            LogicalPlan::Unwind(_) => "Unwind",
            LogicalPlan::CartesianProduct(_) => "CartesianProduct",
            LogicalPlan::WithClause(_) => "WithClause",
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
                // Get a reference to the child plan without moving
                let child_plan_ref = match &child_tf {
                    Transformed::Yes(plan) | Transformed::No(plan) => plan.as_ref(),
                };
                // Apply property mapping to the filter predicate
                let mapped_predicate = self.apply_property_mapping(
                    filter.predicate.clone(),
                    plan_ctx,
                    graph_schema,
                    Some(child_plan_ref),
                )?;
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
                                    is_materialization_boundary: group_by
                                        .is_materialization_boundary,
                                    exposed_alias: group_by.exposed_alias.clone(),
                                });
                                return Ok(Transformed::Yes(Arc::new(new_group_by)));
                            } else if Self::has_cartesian_product_descendant(plan.as_ref()) {
                                // CROSS-TABLE JOIN CONDITION:
                                // This filter references a projection alias (from WITH clause) AND has a
                                // CartesianProduct in its descendants. This is a cross-table join condition
                                // that should be preserved in the plan for CartesianJoinExtraction to handle.
                                // Do NOT extract it - keep it as a Filter node with property-mapped predicate.
                                println!(
                                    "FilterTagging: Preserving cross-table join condition (has CartesianProduct descendant)"
                                );
                                return Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(
                                    Filter {
                                        input: child_tf.get_plan().clone(),
                                        predicate: mapped_predicate,
                                    },
                                ))));
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
                crate::debug_println!(
                    "🔍 FilterTagging: BEFORE processing Projection - distinct={}",
                    projection.distinct
                );
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
                // Get a reference to the child plan without moving
                let child_plan_ref = match &child_tf {
                    Transformed::Yes(plan) | Transformed::No(plan) => plan.as_ref(),
                };
                // Apply property mapping to projection expressions
                let mut mapped_items = Vec::new();
                for item in &projection.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                        Some(child_plan_ref),
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
                        distinct: projection.distinct, // PRESERVE distinct flag from original projection
                    },
                )));
                crate::debug_println!(
                    "🔍 FilterTagging: AFTER creating new Projection - distinct={}",
                    projection.distinct
                );
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
                // Get a reference to the child plan without moving
                let child_plan_ref = match &child_tf {
                    Transformed::Yes(plan) | Transformed::No(plan) => plan.as_ref(),
                };
                // Apply property mapping to order by expressions
                let mut mapped_items = Vec::new();
                for item in &order_by.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                        Some(child_plan_ref),
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
                // For Union queries, each branch is self-contained.
                // We need to apply property mapping to filters, but NOT extract them into plan_ctx
                // because plan_ctx is shared and would incorrectly combine filters from different branches.
                // Instead, process each branch with property mapping only, keeping Filter nodes in tree.
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        self.analyze_union_branch(input_plan.clone(), plan_ctx, graph_schema)?;
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
                            label: u.label.clone(),
                            tuple_properties: u.tuple_properties.clone(),
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                let left_tf =
                    self.analyze_with_graph_schema(cp.left.clone(), plan_ctx, graph_schema)?;
                let right_tf =
                    self.analyze_with_graph_schema(cp.right.clone(), plan_ctx, graph_schema)?;
                match (&left_tf, &right_tf) {
                    (Transformed::No(_), Transformed::No(_)) => {
                        Transformed::No(logical_plan.clone())
                    }
                    _ => Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(
                        crate::query_planner::logical_plan::CartesianProduct {
                            left: left_tf.get_plan().clone(),
                            right: right_tf.get_plan().clone(),
                            is_optional: cp.is_optional,
                            join_condition: cp.join_condition.clone(),
                        },
                    ))),
                }
            }
            LogicalPlan::WithClause(with_clause) => {
                let child_tf = self.analyze_with_graph_schema(
                    with_clause.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;

                // Apply property mapping to WITH clause items (resolves LabelExpression, etc.)
                let mut mapped_items = Vec::new();
                for item in &with_clause.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                        Some(with_clause.input.as_ref()),
                    )?;
                    mapped_items.push(ProjectionItem {
                        expression: mapped_expr.clone(),
                        col_alias: item.col_alias.clone(),
                    });

                    // Register WITH clause aliases as projection aliases
                    if let Some(col_alias) = &item.col_alias {
                        println!(
                            "FilterTagging: Registering WITH clause alias: {} -> {:?}",
                            col_alias.0, mapped_expr
                        );
                        plan_ctx.register_projection_alias(col_alias.0.clone(), mapped_expr);
                    }
                }

                let new_with = crate::query_planner::logical_plan::WithClause {
                    cte_name: with_clause.cte_name.clone(), // PRESERVE from CteSchemaResolver
                    input: child_tf.get_plan().clone(),
                    items: mapped_items,
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
        })
    }
}

// ============================================================================
// FilterTagging Implementation
// ============================================================================

impl FilterTagging {
    pub fn new() -> Self {
        FilterTagging
    }

    // ========================================================================
    // VLP and CTE Detection Helpers
    // ========================================================================

    /// Check if an alias is exported from a WITH clause in the plan tree.
    /// This helps detect CTE-sourced variables during FilterTagging.
    /// Check if an alias is the endpoint of a multi-type VLP pattern.
    /// A multi-type VLP pattern has a GraphRel with variable_length and multiple labels (relationship types).
    fn is_multi_type_vlp_endpoint(plan: &LogicalPlan, alias: &str) -> bool {
        use crate::query_planner::logical_plan::LogicalPlan;

        match plan {
            LogicalPlan::GraphRel(gr) => {
                // Check if this GraphRel:
                // 1. Has variable length pattern
                // 2. Has multiple relationship types (labels)
                // 3. The alias matches the right_connection (endpoint)
                if gr.variable_length.is_some() {
                    if let Some(labels) = &gr.labels {
                        if labels.len() > 1 && gr.right_connection == alias {
                            return true;
                        }
                    }
                }
                // Recursively check children
                Self::is_multi_type_vlp_endpoint(gr.left.as_ref(), alias)
                    || Self::is_multi_type_vlp_endpoint(gr.center.as_ref(), alias)
                    || Self::is_multi_type_vlp_endpoint(gr.right.as_ref(), alias)
            }
            LogicalPlan::Filter(f) => Self::is_multi_type_vlp_endpoint(f.input.as_ref(), alias),
            LogicalPlan::Projection(p) => Self::is_multi_type_vlp_endpoint(p.input.as_ref(), alias),
            LogicalPlan::Limit(l) => Self::is_multi_type_vlp_endpoint(l.input.as_ref(), alias),
            LogicalPlan::Skip(s) => Self::is_multi_type_vlp_endpoint(s.input.as_ref(), alias),
            LogicalPlan::OrderBy(o) => Self::is_multi_type_vlp_endpoint(o.input.as_ref(), alias),
            LogicalPlan::GraphNode(gn) => {
                Self::is_multi_type_vlp_endpoint(gn.input.as_ref(), alias)
            }
            LogicalPlan::Union(u) => u
                .inputs
                .iter()
                .any(|input| Self::is_multi_type_vlp_endpoint(input.as_ref(), alias)),
            LogicalPlan::CartesianProduct(cp) => {
                Self::is_multi_type_vlp_endpoint(cp.left.as_ref(), alias)
                    || Self::is_multi_type_vlp_endpoint(cp.right.as_ref(), alias)
            }
            _ => false,
        }
    }

    /// Analyze a Union branch with property mapping but WITHOUT filter extraction.
    /// This keeps Filter nodes in the tree because each Union branch is self-contained
    /// and should not share filter state through plan_ctx.
    fn analyze_union_branch(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Filter(filter) => {
                // Process child first
                let child_tf =
                    self.analyze_union_branch(filter.input.clone(), plan_ctx, graph_schema)?;
                let child_plan_ref = match &child_tf {
                    Transformed::Yes(p) | Transformed::No(p) => p.as_ref(),
                };
                // Apply property mapping to predicate (this maps Cypher props to DB columns)
                let mapped_predicate = self.apply_property_mapping(
                    filter.predicate.clone(),
                    plan_ctx,
                    graph_schema,
                    Some(child_plan_ref),
                )?;
                // Keep the Filter node in tree - don't extract to plan_ctx
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                    input: child_tf.get_plan().clone(),
                    predicate: mapped_predicate,
                }))))
            }
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    self.analyze_union_branch(projection.input.clone(), plan_ctx, graph_schema)?;
                let child_plan_ref = match &child_tf {
                    Transformed::Yes(p) | Transformed::No(p) => p.as_ref(),
                };
                // Apply property mapping to projection items
                let mut mapped_items = Vec::new();
                for item in &projection.items {
                    let mapped_expr = self.apply_property_mapping(
                        item.expression.clone(),
                        plan_ctx,
                        graph_schema,
                        Some(child_plan_ref),
                    )?;
                    mapped_items.push(ProjectionItem {
                        expression: mapped_expr,
                        col_alias: item.col_alias.clone(),
                    });
                }
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: child_tf.get_plan().clone(),
                        items: mapped_items,
                        distinct: projection.distinct,
                    },
                ))))
            }
            LogicalPlan::GraphNode(gn) => {
                let child_tf =
                    self.analyze_union_branch(gn.input.clone(), plan_ctx, graph_schema)?;
                Ok(gn.rebuild_or_clone(child_tf, plan.clone()))
            }
            LogicalPlan::GraphJoins(gj) => {
                let child_tf =
                    self.analyze_union_branch(gj.input.clone(), plan_ctx, graph_schema)?;
                Ok(gj.rebuild_or_clone(child_tf, plan.clone()))
            }
            // Leaf nodes - no transformation
            LogicalPlan::ViewScan(_) | LogicalPlan::Empty => Ok(Transformed::No(plan.clone())),
            // For any other node types, fall back to regular analysis
            _ => self.analyze_with_graph_schema(plan, plan_ctx, graph_schema),
        }
    }

    // ========================================================================
    // Property Mapping
    // ========================================================================

    /// Apply property mapping to a LogicalExpr, converting Cypher property names to database column names
    pub fn apply_property_mapping(
        &self,
        expr: LogicalExpr,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
        plan: Option<&LogicalPlan>,
    ) -> AnalyzerResult<LogicalExpr> {
        match expr {
            LogicalExpr::PropertyAccessExp(property_access) => {
                println!(
                    "FilterTagging: apply_property_mapping for alias '{}', property '{}'",
                    property_access.table_alias.0,
                    property_access.column.raw()
                );

                // DEBUG: Always write to file to see what properties we're processing
                if property_access.table_alias.0 == "person"
                    || property_access.table_alias.0 == "post"
                {
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/clickgraph_debug_labels.txt")
                    {
                        writeln!(
                            file,
                            "\n=== FilterTagging processing {}.{} ===",
                            property_access.table_alias.0,
                            property_access.column.raw()
                        )
                        .ok();
                        writeln!(file, "About to call get_table_ctx...").ok();
                    }
                }

                // NOTE: We used to convert temporal property names (year, month, day) to function
                // calls here, but that was wrong. In Cypher, `r.year` is only a temporal accessor
                // if `r` is a date/datetime type. Since we don't have type information at this
                // stage, we should treat them as property accesses first. Only if the property
                // is not found in the schema AND the base is known to be a temporal type, we
                // would convert to a function call. For now, we always try property lookup first.

                // Get the table context for this alias
                let table_ctx = plan_ctx
                    .get_table_ctx(&property_access.table_alias.0)
                    .map_err(|e| {
                        // DEBUG: Write to file when table_ctx lookup fails
                        use std::io::Write;
                        if let Ok(mut file) = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("/tmp/clickgraph_debug_labels.txt")
                        {
                            writeln!(file, "\n=== FilterTagging get_table_ctx Failed ===").ok();
                            writeln!(
                                file,
                                "Looking for alias: '{}'",
                                property_access.table_alias.0
                            )
                            .ok();
                            writeln!(file, "Error: {:?}", e).ok();
                            writeln!(file, "Available aliases in plan_ctx:").ok();
                            for (alias, ctx) in plan_ctx.iter_table_contexts() {
                                writeln!(
                                    file,
                                    "  - '{}': is_rel={}, label={:?}",
                                    alias,
                                    ctx.is_relation(),
                                    ctx.get_label_opt()
                                )
                                .ok();
                            }
                            writeln!(file, "=== End ===\n").ok();
                        }

                        crate::debug_print!(
                            "FilterTagging: ERROR - Failed to get table_ctx for alias '{}': {:?}",
                            property_access.table_alias.0,
                            e
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

                // Check if this is a multi-type VLP node (has multiple labels OR is endpoint of multi-type VLP)
                // For multi-type VLP, property extraction happens at runtime via JSON
                // so we skip strict compile-time validation
                //
                // Two ways to detect multi-type VLP:
                // 1. table_ctx already has multiple labels set by TypeInference
                // 2. No label set yet, but this node is the endpoint of a GraphRel with multiple edge types
                let is_multi_type_vlp = if let Some(labels) = table_ctx.get_labels() {
                    // Case 1: Labels already set by TypeInference
                    labels.len() > 1
                } else if plan.is_some() {
                    // Case 2: Check if this is endpoint of multi-type VLP GraphRel
                    Self::is_multi_type_vlp_endpoint(plan.unwrap(), &property_access.table_alias.0)
                } else {
                    false
                };

                if is_multi_type_vlp {
                    log::info!(
                        "🎯 filter_tagging: Skipping property validation for multi-type VLP node '{}' (labels: {:?})",
                        property_access.table_alias.0,
                        table_ctx.get_labels()
                    );
                    // For multi-type VLP, return property as-is without validation
                    // SQL generation will handle JSON extraction
                    return Ok(LogicalExpr::PropertyAccessExp(property_access));
                }

                // ====================================================================
                // CRITICAL: Check if this is a CTE-sourced variable (marked by CtePrediction)
                // ====================================================================
                // If this alias comes from a CTE (WITH clause export), we should NOT
                // apply schema mapping because CTE columns are already the mapped columns.
                // Example:
                //   MATCH (u:User) WITH u AS person RETURN person.name
                //   - u.name → maps to full_name (User schema)
                //   - CTE exports: u_name (not full_name!)
                //   - person.name should resolve to person.u_name, NOT person.full_name
                //
                // The CtePrediction pass (Step 3.25) runs before FilterTagging and marks
                // all WITH-exported aliases with is_cte_reference() = true.

                if table_ctx.is_cte_reference() {
                    log::info!(
                        "🔧 FilterTagging: Skipping schema mapping for CTE-sourced variable '{}', property='{}'",
                        property_access.table_alias.0,
                        property_access.column.raw()
                    );
                    // Return property as-is for CTE lookup
                    // The render phase will use CTE's exported columns
                    return Ok(LogicalExpr::PropertyAccessExp(property_access));
                }

                // Get the label for this table
                let label = table_ctx.get_label_opt().ok_or_else(|| {
                    // DEBUG: Write debug info to file
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/clickgraph_debug_labels.txt")
                    {
                        writeln!(file, "\n=== FilterTagging Label Lookup Failed ===").ok();
                        writeln!(file, "Looking for: '{}'", property_access.table_alias.0).ok();
                        writeln!(file, "Available aliases in plan_ctx:").ok();
                        for (alias, ctx) in plan_ctx.iter_table_contexts() {
                            writeln!(
                                file,
                                "  - '{}': is_rel={}, label={:?}",
                                alias,
                                ctx.is_relation(),
                                ctx.get_label_opt()
                            )
                            .ok();
                        }
                        writeln!(file, "=== End ===\n").ok();
                    }

                    crate::debug_print!(
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

                // Check if this node uses EmbeddedInEdge strategy (denormalized access)
                let (is_embedded_in_edge, owning_edge_info) = if let Some(plan) = plan {
                    // Also check by traversing the plan to find which edge owns this node
                    let edge_info =
                        Self::find_owning_edge_for_node(plan, &property_access.table_alias.0);

                    // Use NodeAccessStrategy to determine if this is an embedded node
                    let edge_alias = edge_info.as_ref().map(|(alias, _)| alias.as_str());
                    let strategy_embedded = matches!(
                        plan_ctx.get_node_strategy(&property_access.table_alias.0, edge_alias),
                        Some(crate::graph_catalog::pattern_schema::NodeAccessStrategy::EmbeddedInEdge { .. })
                    );

                    // Fallback to schema-level check if strategy lookup fails
                    let schema_embedded = table_ctx
                        .get_label_opt()
                        .and_then(|label| plan_ctx.schema().node_schema_opt(&label))
                        .map(|node_schema| node_schema.is_denormalized)
                        .unwrap_or(false);

                    // Log if strategy and schema disagree (potential bug)
                    if strategy_embedded != schema_embedded {
                        log::debug!(
                            "FilterTagging: Strategy vs schema mismatch for alias='{}': strategy={}, schema={}",
                            property_access.table_alias.0, strategy_embedded, schema_embedded
                        );
                    }

                    let is_embedded = strategy_embedded || schema_embedded;

                    println!(
                        "FilterTagging: Checking EmbeddedInEdge for alias='{}' - from_strategy={}, from_edge={:?}",
                        property_access.table_alias.0, is_embedded, edge_info
                    );
                    (is_embedded, edge_info)
                } else {
                    println!(
                        "FilterTagging: No plan context provided for alias='{}'",
                        property_access.table_alias.0
                    );
                    (false, None)
                };

                println!(
                    "FilterTagging: is_embedded_in_edge={} for alias='{}', property='{}'",
                    is_embedded_in_edge,
                    property_access.table_alias.0,
                    property_access.column.raw()
                );

                let mapped_column = if is_embedded_in_edge {
                    // For embedded nodes, look directly at the ViewScan's properties
                    if let Some(plan) = plan {
                        // First, check if plan_ctx knows which edge this node belongs to
                        // This is critical for multi-hop patterns where a node appears in multiple edges
                        if let Some(crate::graph_catalog::pattern_schema::NodeAccessStrategy::EmbeddedInEdge { edge_alias, is_from_node, .. }) =
                            plan_ctx.get_node_strategy(&property_access.table_alias.0, None) {
                            println!(
                                "FilterTagging: Node '{}' is embedded in edge '{}', is_from={}",
                                property_access.table_alias.0, edge_alias, is_from_node
                            );
                            // Use the owning edge info to find the correct property
                            if let Some(column) = Self::find_property_in_viewscan_with_edge(
                                plan,
                                &property_access.table_alias.0,
                                property_access.column.raw(),
                                &edge_alias,
                                *is_from_node,
                                plan_ctx,
                            ) {
                                println!(
                                    "FilterTagging: Found property '{}' in owning edge '{}' ViewScan -> '{}'",
                                    property_access.column.raw(), edge_alias, column
                                );
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    column,
                                )
                            } else {
                                // Fallback to generic search
                                if let Some(column) = Self::find_property_in_viewscan(
                                    plan,
                                    &property_access.table_alias.0,
                                    property_access.column.raw(),
                                ) {
                                    println!(
                                        "FilterTagging: Found property '{}' via fallback in ViewScan -> '{}'",
                                        property_access.column.raw(), column
                                    );
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        column,
                                    )
                                } else {
                                    // Final fallback to role-aware mapping
                                    let role = if *is_from_node {
                                        Some(crate::render_plan::cte_generation::NodeRole::From)
                                    } else {
                                        Some(crate::render_plan::cte_generation::NodeRole::To)
                                    };
                                    let view_resolver = crate::query_planner::analyzer::view_resolver::ViewResolver::from_schema(graph_schema);
                                    view_resolver.resolve_node_property_with_role(
                                        &label,
                                        &property_access.column.raw(),
                                        role,
                                    )?
                                }
                            }
                        } else {
                            // Node is marked as embedded but no edge info found (standalone node query)
                            // This happens when querying embedded nodes directly without a relationship:
                            //   MATCH (n:IP) RETURN count(DISTINCT n.ip)
                            // Fall back to searching the ViewScan's property mapping directly
                            log::debug!(
                                "FilterTagging: Embedded node '{}' has no edge context - falling back to ViewScan property lookup",
                                property_access.table_alias.0
                            );
                            if let Some(column) = Self::find_property_in_viewscan(
                                plan,
                                &property_access.table_alias.0,
                                property_access.column.raw(),
                            ) {
                                println!(
                                    "FilterTagging: Found property '{}' in standalone ViewScan -> '{}'",
                                    property_access.column.raw(), column
                                );
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    column,
                                )
                            } else {
                                // Still couldn't find it - fall back to view resolver
                                let view_resolver = crate::query_planner::analyzer::view_resolver::ViewResolver::from_schema(graph_schema);
                                view_resolver.resolve_node_property_with_role(
                                    &label,
                                    &property_access.column.raw(),
                                    None, // No role for standalone nodes
                                )?
                            }
                        }
                    } else {
                        // No plan available for embedded node - this should not happen
                        return Err(AnalyzerError::InvalidPlan(format!(
                            "Embedded node '{}' has no plan context. This indicates a bug in query planning.",
                            property_access.table_alias.0
                        )));
                    }
                } else {
                    // Use view resolver to map the property (standard path)
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
                    if table_ctx.is_relation() {
                        // Get connected node labels for polymorphic relationship resolution
                        let from_node = table_ctx.get_from_node_label().map(|s| s.as_str());
                        let to_node = table_ctx.get_to_node_label().map(|s| s.as_str());
                        let result = view_resolver.resolve_relationship_property(
                            &label,
                            &property_access.column.raw(),
                            from_node,
                            to_node,
                        );
                        println!(
                            "FilterTagging: resolve_relationship_property result: {:?}",
                            result
                        );
                        result?
                    } else {
                        let result = view_resolver
                            .resolve_node_property(&label, &property_access.column.raw());
                        println!("FilterTagging: resolve_node_property result: {:?}", result);
                        result?
                    }
                };

                println!(
                    "FilterTagging: Successfully mapped property '{}' to column '{}' (keeping original table alias '{}')",
                    property_access.column.raw(), mapped_column.raw(), property_access.table_alias.0
                );

                // Keep the original table alias - it will be remapped during SQL generation
                Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: property_access.table_alias.clone(),
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
                        plan,
                    )?);
                }
                op.operands = mapped_operands;
                Ok(LogicalExpr::OperatorApplicationExp(op))
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                let fn_name_lower = fn_call.name.to_lowercase();

                // Handle id() function - resolve to actual ID column for WHERE clause usage
                if fn_name_lower == "id" && fn_call.args.len() == 1 {
                    // Extract the alias from the argument
                    if let LogicalExpr::TableAlias(ref alias) = fn_call.args[0] {
                        let alias_str = &alias.0;

                        // Get the table context to determine if it's a node or relationship
                        if let Ok(table_ctx) = plan_ctx.get_table_ctx(alias_str) {
                            if let Some(label) = table_ctx.get_label_opt() {
                                let id_column = if table_ctx.is_relation() {
                                    // For relationships, get the from_id column (or edge_id if defined)
                                    if let Ok(rel_schema) = graph_schema.get_rel_schema(&label) {
                                        if let Some(ref edge_id) = rel_schema.edge_id {
                                            let columns = edge_id.columns();
                                            if columns.len() == 1 {
                                                Some(columns[0].to_string())
                                            } else {
                                                // Composite ID - return first column for simple comparison
                                                // TODO: Handle composite IDs properly in WHERE clause
                                                Some(columns[0].to_string())
                                            }
                                        } else {
                                            Some(rel_schema.from_id.clone())
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    // For nodes, check if it's embedded in edge and get the proper ID column
                                    // Use NodeAccessStrategy to determine access pattern
                                    let is_embedded_in_edge = matches!(
                                        plan_ctx.get_node_strategy(alias_str, None),
                                        Some(crate::graph_catalog::pattern_schema::NodeAccessStrategy::EmbeddedInEdge { .. })
                                    );

                                    if is_embedded_in_edge {
                                        // For denormalized nodes, find the owning edge and get the ID from from_node/to_node properties
                                        if let Some(plan) = plan {
                                            if let Some((owning_edge, is_from_node)) =
                                                Self::find_owning_edge_for_node(plan, alias_str)
                                            {
                                                // Use find_property_in_viewscan_with_edge to get the actual column
                                                // Node id_column in schema is the logical property name (e.g., "id")
                                                let id_property = if let Ok(node_schema) =
                                                    graph_schema.node_schema(&label)
                                                {
                                                    node_schema
                                                        .node_id
                                                        .columns()
                                                        .first()
                                                        .ok_or_else(|| AnalyzerError::SchemaNotFound(
                                                            "Node schema has no ID columns defined".to_string()
                                                        ))?
                                                        .to_string()
                                                } else {
                                                    return Err(AnalyzerError::SchemaNotFound(
                                                        "Node schema not found".to_string(),
                                                    ));
                                                };

                                                Self::find_property_in_viewscan_with_edge(
                                                    plan,
                                                    alias_str,
                                                    &id_property,
                                                    &owning_edge,
                                                    is_from_node,
                                                    plan_ctx,
                                                )
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        // For regular (non-denormalized) nodes, use the node_id column directly
                                        if let Ok(node_schema) = graph_schema.node_schema(&label) {
                                            Some(
                                                node_schema
                                                    .node_id
                                                    .columns()
                                                    .first()
                                                    .ok_or_else(|| AnalyzerError::SchemaNotFound(
                                                        format!("Node schema for label '{}' has no ID columns defined", label)
                                                    ))?
                                                    .to_string(),
                                            )
                                        } else {
                                            return Err(AnalyzerError::SchemaNotFound(format!(
                                                "Node schema not found for label '{}'",
                                                label
                                            )));
                                        }
                                    }
                                };

                                if let Some(column) = id_column {
                                    println!(
                                        "FilterTagging: Resolved id({}) to PropertyAccess with column '{}'",
                                        alias_str, column
                                    );
                                    return Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(alias_str.clone()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(column),
                                    }));
                                }
                            }
                        }
                    }
                    // If we couldn't resolve id(), fall through to pass it unchanged
                    println!(
                        "FilterTagging: Could not resolve id() function, passing through unchanged"
                    );
                }

                // Other graph introspection functions (type, labels, label) are handled by ProjectionTagging
                // for RETURN clauses. In WHERE clauses, pass them through with mapped args.
                if matches!(fn_name_lower.as_str(), "type" | "labels" | "label") {
                    let mut mapped_args = Vec::new();
                    for arg in fn_call.args {
                        mapped_args.push(self.apply_property_mapping(
                            arg,
                            plan_ctx,
                            graph_schema,
                            plan,
                        )?);
                    }
                    return Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                        name: fn_call.name,
                        args: mapped_args,
                    }));
                }

                // For other scalar functions, recursively apply property mapping to arguments
                let mut mapped_args = Vec::new();
                for arg in fn_call.args {
                    mapped_args.push(self.apply_property_mapping(
                        arg,
                        plan_ctx,
                        graph_schema,
                        plan,
                    )?);
                }
                Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: fn_call.name,
                    args: mapped_args,
                }))
            }
            LogicalExpr::AggregateFnCall(mut agg_call) => {
                // Recursively apply property mapping to aggregate function arguments
                let mut mapped_args = Vec::new();
                for arg in agg_call.args {
                    mapped_args.push(self.apply_property_mapping(
                        arg,
                        plan_ctx,
                        graph_schema,
                        plan,
                    )?);
                }
                agg_call.args = mapped_args;
                Ok(LogicalExpr::AggregateFnCall(agg_call))
            }
            LogicalExpr::List(list) => {
                // Recursively apply property mapping to list elements
                let mut mapped_elements = Vec::new();
                for element in list {
                    mapped_elements.push(self.apply_property_mapping(
                        element,
                        plan_ctx,
                        graph_schema,
                        plan,
                    )?);
                }
                Ok(LogicalExpr::List(mapped_elements))
            }
            LogicalExpr::ArraySlicing { array, from, to } => {
                // Recursively apply property mapping to array slicing components
                // This is important for expressions like collect(n.name)[0..10]
                let mapped_array =
                    self.apply_property_mapping(*array, plan_ctx, graph_schema, plan)?;
                let mapped_from = if let Some(f) = from {
                    Some(Box::new(self.apply_property_mapping(
                        *f,
                        plan_ctx,
                        graph_schema,
                        plan,
                    )?))
                } else {
                    None
                };
                let mapped_to = if let Some(t) = to {
                    Some(Box::new(self.apply_property_mapping(
                        *t,
                        plan_ctx,
                        graph_schema,
                        plan,
                    )?))
                } else {
                    None
                };
                Ok(LogicalExpr::ArraySlicing {
                    array: Box::new(mapped_array),
                    from: mapped_from,
                    to: mapped_to,
                })
            }
            LogicalExpr::LabelExpression {
                variable,
                label: check_label,
            } => {
                // Label expression: variable:Label
                // Check if the variable has the specified label
                //
                // For polymorphic tables (with label_column), generate SQL comparison:
                //   m:Comment -> label_column = 'Comment'
                // For non-polymorphic tables, resolve at compile-time to true/false

                if let Ok(table_ctx) = plan_ctx.get_table_ctx(&variable) {
                    if let Some(known_labels) = table_ctx.get_labels() {
                        // Check if this is a polymorphic table with label_column
                        // We need to look up the node schema to see if it has label_column
                        if let Some(first_label) = known_labels.first() {
                            if let Ok(node_schema) = graph_schema.node_schema(first_label) {
                                if let Some(label_col) = &node_schema.label_column {
                                    // Polymorphic table - generate runtime check: label_column = 'check_label'
                                    println!(
                                        "FilterTagging: LabelExpression {}:{} - polymorphic table with label_column='{}', generating runtime check",
                                        variable, check_label, label_col
                                    );
                                    return Ok(LogicalExpr::OperatorApplicationExp(
                                        crate::query_planner::logical_expr::OperatorApplication {
                                            operator: crate::query_planner::logical_expr::Operator::Equal,
                                            operands: vec![
                                                LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                                                    table_alias: crate::query_planner::logical_expr::TableAlias(variable.clone()),
                                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(label_col.clone()),
                                                }),
                                                LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::String(check_label.clone())),
                                            ],
                                        }
                                    ));
                                }
                            }
                        }

                        // Non-polymorphic table - resolve at compile-time
                        // Check if the check_label matches any of the known labels
                        let matches = known_labels
                            .iter()
                            .any(|l| l.eq_ignore_ascii_case(&check_label));
                        println!(
                            "FilterTagging: LabelExpression {} has labels {:?}, checking for '{}' -> {}",
                            variable, known_labels, check_label, matches
                        );
                        return Ok(LogicalExpr::Literal(
                            crate::query_planner::logical_expr::Literal::Boolean(matches),
                        ));
                    }
                }
                // If we can't determine the label statically, keep the expression as-is
                // This will need to be handled at SQL generation time
                println!(
                    "FilterTagging: LabelExpression {}:{} - cannot determine label statically",
                    variable, check_label
                );
                Ok(LogicalExpr::LabelExpression {
                    variable,
                    label: check_label,
                })
            }
            // For other expression types, return as-is
            other => Ok(other),
        }
    }

    // ========================================================================
    // Filter Extraction
    // ========================================================================

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

    /// Recursively collect all PropertyAccess expressions from an expression tree
    /// Used to extract property projections without modifying the expression structure
    fn collect_property_accesses(expr: &LogicalExpr, props: &mut Vec<PropertyAccess>) {
        match expr {
            LogicalExpr::PropertyAccessExp(prop) => {
                props.push(prop.clone());
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                for operand in &op.operands {
                    Self::collect_property_accesses(operand, props);
                }
            }
            LogicalExpr::ScalarFnCall(fc) => {
                for arg in &fc.args {
                    Self::collect_property_accesses(arg, props);
                }
            }
            LogicalExpr::AggregateFnCall(fc) => {
                for arg in &fc.args {
                    Self::collect_property_accesses(arg, props);
                }
            }
            LogicalExpr::Case(case) => {
                if let Some(expr) = &case.expr {
                    Self::collect_property_accesses(expr, props);
                }
                for (cond, result) in &case.when_then {
                    Self::collect_property_accesses(cond, props);
                    Self::collect_property_accesses(result, props);
                }
                if let Some(else_expr) = &case.else_expr {
                    Self::collect_property_accesses(else_expr, props);
                }
            }
            LogicalExpr::List(items) => {
                for item in items {
                    Self::collect_property_accesses(item, props);
                }
            }
            // Other expression types don't contain property accesses
            _ => {}
        }
    }

    fn process_expr(
        expr: LogicalExpr,
        extracted_filters: &mut Vec<OperatorApplication>,
        extracted_projections: &mut Vec<PropertyAccess>,
        in_or: bool,
    ) -> Option<LogicalExpr> {
        log::debug!("process_expr called with: {:?}", expr);
        match expr {
            // When we have an operator application, process it separately.
            LogicalExpr::OperatorApplicationExp(mut op_app) => {
                log::debug!(
                    "process_expr: OperatorApplication with operator={:?}, operands.len()={}",
                    op_app.operator,
                    op_app.operands.len()
                );

                // CRITICAL: Handle NOT operator BEFORE recursing into operands
                // Otherwise the operand (e.g., Equal(p.id, friend.id)) gets processed and extracted first
                if op_app.operator == Operator::Not && op_app.operands.len() == 1 {
                    // Check if the operand (the expression under NOT) is single-table or cross-table
                    let single_table =
                        Self::get_table_alias_if_single_table_condition(&op_app.operands[0], false);

                    if single_table.is_none() {
                        // Cross-table condition under NOT (e.g., NOT (p.id = friend.id))
                        // Keep it intact - don't recurse into operands
                        // Just extract property projections for column selection
                        let mut temp_props = Vec::new();
                        Self::collect_property_accesses(&op_app.operands[0], &mut temp_props);
                        extracted_projections.extend(temp_props);

                        // Return the entire NOT expression as-is for global WHERE clause
                        return Some(LogicalExpr::OperatorApplicationExp(op_app));
                    } else {
                        // Single-table NOT (e.g., NOT p.active)
                        // Extract it as a complete filter to the table
                        // Collect projections from the operand
                        let mut temp_props = Vec::new();
                        Self::collect_property_accesses(&op_app.operands[0], &mut temp_props);
                        extracted_projections.extend(temp_props);

                        // Extract the entire NOT expression as a filter
                        extracted_filters.push(op_app);
                        return None; // Extracted - remove from remaining
                    }
                }

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
                let old_operands_len = op_app.operands.len();
                for (i, operand) in op_app.operands.into_iter().enumerate() {
                    log::debug!("process_expr: Processing operand[{}]: {:?}", i, operand);
                    if let Some(new_operand) = Self::process_expr(
                        operand,
                        extracted_filters,
                        extracted_projections,
                        new_in_or,
                    ) {
                        log::debug!(
                            "process_expr: Operand[{}] returned Some: {:?}",
                            i,
                            new_operand
                        );
                        new_operands.push(new_operand);
                    } else {
                        log::debug!("process_expr: Operand[{}] returned None (extracted)", i);
                    }
                }
                // Update the operator application with the processed operands.
                log::debug!(
                    "process_expr: After processing operands, new_operands.len()={} (was {})",
                    new_operands.len(),
                    old_operands_len
                );
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
                // IMPORTANT: Only extract if the operator is a filter-extractable operator (comparisons, boolean).
                // Arithmetic operators like Addition should NOT be extracted as standalone filters.
                // IMPORTANT: Use get_table_alias_if_single_table_condition to RECURSIVELY check all table aliases,
                // not just direct operands. This handles cases like `r1.x + 100 <= r2.y` where the Addition
                // references r1 but is nested inside the comparison, so condition_belongs_to only saw r2.
                let single_table_alias = Self::get_table_alias_if_single_table_condition(
                    &LogicalExpr::OperatorApplicationExp(op_app.clone()),
                    false, // not checking aggregate functions
                );
                let is_single_table_condition = single_table_alias.is_some();

                if !new_in_or
                    && !agg_operand_found
                    && is_single_table_condition
                    && op_app.operator.is_filter_extractable()
                {
                    extracted_filters.push(op_app);
                    return None;
                } else if new_in_or || !is_single_table_condition {
                    extracted_projections.append(&mut temp_prop_acc);
                }

                // If after processing there is only one operand left and it is not unary then collapse the operator application.
                if op_app.operands.len() == 1 && op_app.operator != Operator::Not {
                    log::warn!(
                        "process_expr: Collapsing operator {:?} with single operand: {:?}",
                        op_app.operator,
                        op_app.operands[0]
                    );
                    return Some(
                        op_app
                            .operands
                            .into_iter()
                            .next()
                            .expect("Vector with len==1 must have element"),
                    );
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

    // ========================================================================
    // Graph Structure Helpers
    // ========================================================================
    // These helper functions examine the logical plan structure to support
    // filter placement decisions:
    // - `references_projection_alias`: Check if filter belongs in HAVING clause
    // - `find_owning_edge_for_node`: Find denormalized edge for node properties
    // - `has_cartesian_product_descendant`: Detect cross-table joins
    // - `is_node_denormalized`: Check if node is embedded in edge table

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

    /// Find which edge "owns" a node alias by looking at GraphRel.left_connection and right_connection
    /// Returns (edge_alias, is_from_node) where is_from_node=true means it's left_connection
    /// Only returns Some if the edge has from_node_properties or to_node_properties defined
    fn find_owning_edge_for_node(plan: &LogicalPlan, node_alias: &str) -> Option<(String, bool)> {
        match plan {
            LogicalPlan::GraphRel(rel) => {
                // Check if this node is the left (from) or right (to) of this relationship
                // AND the relationship has edge-defined node properties
                // IMPORTANT: Only match if the child is a GraphNode (not another GraphRel)
                // This ensures we find the DIRECT owning edge, not an ancestor GraphRel
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    let has_from_props = scan.from_node_properties.is_some();
                    let has_to_props = scan.to_node_properties.is_some();

                    // Check if left child is a GraphNode with this alias
                    if let LogicalPlan::GraphNode(left_node) = rel.left.as_ref() {
                        if left_node.alias == node_alias && has_from_props {
                            return Some((rel.alias.clone(), true)); // is_from_node = true
                        }
                    }

                    // Check if right child is a GraphNode with this alias
                    if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                        if right_node.alias == node_alias && has_to_props {
                            return Some((rel.alias.clone(), false)); // is_from_node = false
                        }
                    }
                }

                // Recurse into branches
                if let Some(result) = Self::find_owning_edge_for_node(&rel.left, node_alias) {
                    return Some(result);
                }
                Self::find_owning_edge_for_node(&rel.right, node_alias)
            }
            LogicalPlan::CartesianProduct(cp) => {
                // Search both branches of the CartesianProduct for the owning edge
                if let Some(result) = Self::find_owning_edge_for_node(&cp.left, node_alias) {
                    return Some(result);
                }
                Self::find_owning_edge_for_node(&cp.right, node_alias)
            }
            LogicalPlan::GraphNode(node) => {
                Self::find_owning_edge_for_node(&node.input, node_alias)
            }
            LogicalPlan::Filter(filter) => {
                Self::find_owning_edge_for_node(&filter.input, node_alias)
            }
            LogicalPlan::Projection(proj) => {
                Self::find_owning_edge_for_node(&proj.input, node_alias)
            }
            LogicalPlan::GroupBy(gb) => Self::find_owning_edge_for_node(&gb.input, node_alias),
            LogicalPlan::OrderBy(ob) => Self::find_owning_edge_for_node(&ob.input, node_alias),
            LogicalPlan::Skip(skip) => Self::find_owning_edge_for_node(&skip.input, node_alias),
            LogicalPlan::Limit(limit) => Self::find_owning_edge_for_node(&limit.input, node_alias),
            LogicalPlan::Cte(cte) => Self::find_owning_edge_for_node(&cte.input, node_alias),
            _ => None,
        }
    }

    /// Check if a plan has a CartesianProduct descendant
    /// Used to identify cross-table join conditions that should not be extracted
    fn has_cartesian_product_descendant(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::CartesianProduct(_) => true,
            LogicalPlan::Projection(proj) => Self::has_cartesian_product_descendant(&proj.input),
            LogicalPlan::Filter(filter) => Self::has_cartesian_product_descendant(&filter.input),
            LogicalPlan::GroupBy(gb) => Self::has_cartesian_product_descendant(&gb.input),
            LogicalPlan::OrderBy(ob) => Self::has_cartesian_product_descendant(&ob.input),
            LogicalPlan::Skip(skip) => Self::has_cartesian_product_descendant(&skip.input),
            LogicalPlan::Limit(limit) => Self::has_cartesian_product_descendant(&limit.input),
            LogicalPlan::Cte(cte) => Self::has_cartesian_product_descendant(&cte.input),
            LogicalPlan::GraphNode(node) => Self::has_cartesian_product_descendant(&node.input),
            LogicalPlan::GraphJoins(joins) => Self::has_cartesian_product_descendant(&joins.input),
            _ => false,
        }
    }

    /// Check if a node is denormalized by using PatternSchemaContext
    fn is_node_denormalized(plan_ctx: &PlanCtx, alias: &str) -> bool {
        // Use PatternSchemaContext to determine if node is embedded in an edge
        matches!(
            plan_ctx.get_node_strategy(alias, None),
            Some(crate::graph_catalog::pattern_schema::NodeAccessStrategy::EmbeddedInEdge { .. })
        )
    }

    // ========================================================================
    // ViewScan Property Resolution
    // ========================================================================
    // These functions search the logical plan tree to find property column
    // mappings from ViewScan nodes. Essential for denormalized schemas where
    // node properties may be stored in edge tables.

    /// Find a property mapping from a specific edge's ViewScan
    /// This is used for multi-hop denormalized patterns where we know which edge owns the node
    fn find_property_in_viewscan_with_edge(
        plan: &LogicalPlan,
        alias: &str,
        property: &str,
        owning_edge: &str,
        is_from_node: bool,
        plan_ctx: &PlanCtx,
    ) -> Option<String> {
        match plan {
            LogicalPlan::GraphRel(rel) => {
                // Only process if this is the owning edge
                if rel.alias == owning_edge {
                    // PRIMARY: Try PatternSchemaContext first - has explicit role information
                    if let Some(pattern_ctx) = plan_ctx.get_pattern_context(&rel.alias) {
                        log::debug!(
                            "find_property_in_viewscan_with_edge: Using PatternSchemaContext for alias='{}', edge='{}', property='{}'",
                            alias, rel.alias, property
                        );

                        if let Some(column) = pattern_ctx.get_node_property(alias, property) {
                            log::debug!(
                                "find_property_in_viewscan_with_edge: Found property '{}' -> '{}' via PatternSchemaContext",
                                property, column
                            );
                            return Some(column);
                        }
                    }
                    // No pattern context = bug in GraphJoinInference, don't hide it with fallback
                }

                // Recurse to find the owning edge
                if let Some(col) = Self::find_property_in_viewscan_with_edge(
                    &rel.left,
                    alias,
                    property,
                    owning_edge,
                    is_from_node,
                    plan_ctx,
                ) {
                    return Some(col);
                }
                Self::find_property_in_viewscan_with_edge(
                    &rel.right,
                    alias,
                    property,
                    owning_edge,
                    is_from_node,
                    plan_ctx,
                )
            }
            LogicalPlan::GraphNode(node) => Self::find_property_in_viewscan_with_edge(
                &node.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Projection(proj) => Self::find_property_in_viewscan_with_edge(
                &proj.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Filter(filter) => Self::find_property_in_viewscan_with_edge(
                &filter.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::GraphJoins(joins) => Self::find_property_in_viewscan_with_edge(
                &joins.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::OrderBy(ob) => Self::find_property_in_viewscan_with_edge(
                &ob.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Skip(skip) => Self::find_property_in_viewscan_with_edge(
                &skip.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Limit(limit) => Self::find_property_in_viewscan_with_edge(
                &limit.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::GroupBy(gb) => Self::find_property_in_viewscan_with_edge(
                &gb.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Unwind(u) => Self::find_property_in_viewscan_with_edge(
                &u.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::Cte(cte) => Self::find_property_in_viewscan_with_edge(
                &cte.input,
                alias,
                property,
                owning_edge,
                is_from_node,
                plan_ctx,
            ),
            LogicalPlan::CartesianProduct(cp) => {
                // Search both branches of the CartesianProduct for the owning edge
                if let Some(col) = Self::find_property_in_viewscan_with_edge(
                    &cp.left,
                    alias,
                    property,
                    owning_edge,
                    is_from_node,
                    plan_ctx,
                ) {
                    return Some(col);
                }
                Self::find_property_in_viewscan_with_edge(
                    &cp.right,
                    alias,
                    property,
                    owning_edge,
                    is_from_node,
                    plan_ctx,
                )
            }
            _ => None,
        }
    }

    /// Find a property mapping directly from a ViewScan in the plan tree
    /// This is the simplest approach - each Union branch's ViewScan has the correct properties
    fn find_property_in_viewscan(
        plan: &LogicalPlan,
        alias: &str,
        property: &str,
    ) -> Option<String> {
        match plan {
            LogicalPlan::GraphNode(node) => {
                if node.alias == alias {
                    if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                        // Check from_node_properties first
                        if let Some(from_props) = &scan.from_node_properties {
                            if let Some(prop_value) = from_props.get(property) {
                                if let crate::graph_catalog::expression_parser::PropertyValue::Column(col) = prop_value {
                                    return Some(col.clone());
                                }
                            }
                        }
                        // Then check to_node_properties
                        if let Some(to_props) = &scan.to_node_properties {
                            if let Some(prop_value) = to_props.get(property) {
                                if let crate::graph_catalog::expression_parser::PropertyValue::Column(col) = prop_value {
                                    return Some(col.clone());
                                }
                            }
                        }
                        // Finally check regular property_mapping
                        if let Some(prop_value) = scan.property_mapping.get(property) {
                            return Some(prop_value.raw().to_string());
                        }
                    }
                }
                Self::find_property_in_viewscan(&node.input, alias, property)
            }
            LogicalPlan::GraphRel(rel) => {
                // IMPORTANT: For multi-hop patterns, recurse into inner GraphRels FIRST
                // This ensures we find a node's properties in the GraphRel where it was DEFINED,
                // not where it's used as a connection point.
                // Example: (a)-[r1]->(b)-[r2]->(c)
                //   - When looking for 'b', we should find it in r1 (where b is right_connection)
                //   - Not in r2 (where b is left_connection, which would give wrong column)

                // First, recurse into left (inner GraphRels)
                if let Some(col) = Self::find_property_in_viewscan(&rel.left, alias, property) {
                    return Some(col);
                }

                // Then check the current GraphRel's connections ONLY if alias wasn't found in inner GraphRels
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    // Check if alias is the left (from) node of THIS relationship
                    if rel.left_connection == alias {
                        if let Some(from_props) = &scan.from_node_properties {
                            if let Some(prop_value) = from_props.get(property) {
                                if let crate::graph_catalog::expression_parser::PropertyValue::Column(col) = prop_value {
                                    println!("FilterTagging: find_property_in_viewscan - found '{}' in from_node_properties -> '{}'", property, col);
                                    return Some(col.clone());
                                }
                            }
                        }
                    }
                    // Check if alias is the right (to) node of THIS relationship
                    if rel.right_connection == alias {
                        if let Some(to_props) = &scan.to_node_properties {
                            if let Some(prop_value) = to_props.get(property) {
                                if let crate::graph_catalog::expression_parser::PropertyValue::Column(col) = prop_value {
                                    println!("FilterTagging: find_property_in_viewscan - found '{}' in to_node_properties -> '{}'", property, col);
                                    return Some(col.clone());
                                }
                            }
                        }
                    }
                }

                // Finally check center and right
                if let Some(col) = Self::find_property_in_viewscan(&rel.center, alias, property) {
                    return Some(col);
                }
                Self::find_property_in_viewscan(&rel.right, alias, property)
            }
            LogicalPlan::Filter(filter) => {
                Self::find_property_in_viewscan(&filter.input, alias, property)
            }
            LogicalPlan::Projection(proj) => {
                Self::find_property_in_viewscan(&proj.input, alias, property)
            }
            LogicalPlan::GroupBy(gb) => Self::find_property_in_viewscan(&gb.input, alias, property),
            LogicalPlan::OrderBy(ob) => Self::find_property_in_viewscan(&ob.input, alias, property),
            LogicalPlan::Skip(skip) => {
                Self::find_property_in_viewscan(&skip.input, alias, property)
            }
            LogicalPlan::Limit(limit) => {
                Self::find_property_in_viewscan(&limit.input, alias, property)
            }
            LogicalPlan::Cte(cte) => Self::find_property_in_viewscan(&cte.input, alias, property),
            LogicalPlan::Union(union) => {
                // For Union, search in all branches (they should all have the same alias)
                for branch in &union.inputs {
                    if let Some(col) = Self::find_property_in_viewscan(branch, alias, property) {
                        return Some(col);
                    }
                }
                None
            }
            LogicalPlan::CartesianProduct(cp) => {
                // Search both branches of the CartesianProduct
                if let Some(col) = Self::find_property_in_viewscan(&cp.left, alias, property) {
                    return Some(col);
                }
                Self::find_property_in_viewscan(&cp.right, alias, property)
            }
            _ => None,
        }
    }

    /// Find denormalized context: relationship type and node role
    /// Returns (relationship_type, node_role) if the alias is a denormalized node in a GraphRel
    fn find_denormalized_context(
        plan: &LogicalPlan,
        alias: &str,
        _label: &str,
    ) -> Option<(
        Option<String>,
        crate::render_plan::cte_generation::NodeRole,
        String,
    )> {
        use crate::render_plan::cte_generation::NodeRole;

        match plan {
            LogicalPlan::GraphRel(rel) => {
                // Check if this alias is the left or right connection of this relationship
                let is_left = rel.left_connection == alias;
                let is_right = rel.right_connection == alias;

                if is_left || is_right {
                    // Check if the node is actually denormalized
                    let node_plan = if is_left { &rel.left } else { &rel.right };
                    if let LogicalPlan::GraphNode(node) = node_plan.as_ref() {
                        if node.is_denormalized {
                            let rel_type = rel
                                .labels
                                .as_ref()
                                .and_then(|labels| labels.first().cloned());
                            let role = if is_left {
                                NodeRole::From
                            } else {
                                NodeRole::To
                            };
                            println!("find_denormalized_context: Returning rel_alias='{}' for node alias='{}'", rel.alias, alias);
                            return Some((rel_type, role, rel.alias.clone()));
                        }
                    }
                }

                // Recursively search in child plans
                if let Some(result) = Self::find_denormalized_context(&rel.left, alias, _label) {
                    return Some(result);
                }
                if let Some(result) = Self::find_denormalized_context(&rel.right, alias, _label) {
                    return Some(result);
                }
                None
            }
            LogicalPlan::GraphNode(node) => {
                // For node-only queries with denormalized nodes, check ViewScan directly
                if node.is_denormalized && node.alias == alias {
                    if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                        // Determine role based on which properties are available
                        if scan.from_node_properties.is_some() && scan.to_node_properties.is_none()
                        {
                            println!(
                                "find_denormalized_context: Node-only FROM for alias='{}'",
                                alias
                            );
                            return Some((None, NodeRole::From, alias.to_string()));
                        } else if scan.to_node_properties.is_some()
                            && scan.from_node_properties.is_none()
                        {
                            println!(
                                "find_denormalized_context: Node-only TO for alias='{}'",
                                alias
                            );
                            return Some((None, NodeRole::To, alias.to_string()));
                        }
                    }
                }
                Self::find_denormalized_context(&node.input, alias, _label)
            }
            LogicalPlan::Filter(filter) => {
                Self::find_denormalized_context(&filter.input, alias, _label)
            }
            LogicalPlan::Projection(proj) => {
                Self::find_denormalized_context(&proj.input, alias, _label)
            }
            LogicalPlan::GroupBy(gb) => Self::find_denormalized_context(&gb.input, alias, _label),
            LogicalPlan::OrderBy(ob) => Self::find_denormalized_context(&ob.input, alias, _label),
            LogicalPlan::Skip(skip) => Self::find_denormalized_context(&skip.input, alias, _label),
            LogicalPlan::Limit(limit) => {
                Self::find_denormalized_context(&limit.input, alias, _label)
            }
            LogicalPlan::Cte(cte) => Self::find_denormalized_context(&cte.input, alias, _label),
            LogicalPlan::CartesianProduct(cp) => {
                // Search both branches of the CartesianProduct
                if let Some(result) = Self::find_denormalized_context(&cp.left, alias, _label) {
                    return Some(result);
                }
                Self::find_denormalized_context(&cp.right, alias, _label)
            }
            _ => None,
        }
    }
}

// ============================================================================
// UNIT TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, PropertyAccess, TableAlias};
    use crate::query_planner::logical_plan::{Filter, GraphNode, LogicalPlan};
    use crate::query_planner::plan_ctx::TableCtx;

    fn create_property_access(table: &str, column: &str) -> LogicalExpr {
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table.to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                column.to_string(),
            ),
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
        person_props.insert(
            "age".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column("age".to_string()),
        );
        person_props.insert(
            "status".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column("status".to_string()),
        );
        person_props.insert(
            "name".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
        );

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
                node_id: NodeIdSchema::single("user_id".to_string(), "UInt32".to_string()),
                property_mappings: person_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
                label_column: None,
                label_value: None,
            },
        );

        // Company node
        let mut company_props = HashMap::new();
        company_props.insert(
            "name".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
        );
        company_props.insert(
            "owner_id".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column("owner_id".to_string()),
        );

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
                node_id: NodeIdSchema::single("company_id".to_string(), "UInt32".to_string()),
                property_mappings: company_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
                label_column: None,
                label_value: None,
            },
        );

        // FOLLOWS relationship
        let mut follows_props = HashMap::new();
        follows_props.insert(
            "since".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column(
                "created_at".to_string(),
            ),
        );

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
                from_node_table: "persons".to_string(),
                to_node_table: "persons".to_string(),
                from_id: "from_node_id".to_string(),
                to_id: "to_node_id".to_string(),
                from_node_id_dtype: "UInt32".to_string(),
                to_node_id_dtype: "UInt32".to_string(),
                property_mappings: follows_props,
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_label_values: None,
                to_label_values: None,
                from_node_properties: None,
                to_node_properties: None,
                is_fk_edge: false,
                constraints: None,
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
        let scan = Arc::new(LogicalPlan::Empty);

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

        // Create complex nested plan: GraphNode -> Filter -> Empty
        let scan = Arc::new(LogicalPlan::Empty);

        let filter = Arc::new(LogicalPlan::Filter(Filter {
            input: scan,
            predicate: create_simple_filter("user", "status", 1),
        }));

        let graph_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: filter,
            alias: "user".to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
        }));

        let result = analyzer
            .analyze_with_graph_schema(graph_node, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should transform the nested structure
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GraphNode(node) => {
                        // The filter should be removed and Empty should be direct input
                        assert_eq!(node.alias, "user");
                        match node.input.as_ref() {
                            LogicalPlan::Empty => {}
                            _ => panic!("Expected Empty as direct input after filter removal"),
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
