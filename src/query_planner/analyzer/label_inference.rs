//! Label Inference Analyzer Pass
//!
//! **Purpose**: Infer missing node labels from relationship schemas.
//!
//! **Problem**: Nodes in Cypher queries may not have explicit labels:
//! ```cypher
//! MATCH (a:Person)-[:KNOWS]->(b)  -- b has no label
//! RETURN b.name
//! ```
//!
//! Without b's label, we can't:
//! - Map properties to columns (FilterTagging fails)
//! - Generate correct SQL table names
//! - Validate queries
//!
//! **Solution**: Use relationship schemas to infer missing labels.
//! If KNOWS relationship connects Person → Person, then b must be Person.
//!
//! **When to run**: Early in analyzer pipeline (position 2, after SchemaInference)
//! This ensures all downstream passes have complete label information.
//!
//! **Scope handling**: Uses existing plan_ctx scope barriers. When a WITH clause
//! creates a scope boundary, get_table_ctx() won't look past it automatically.
//! No additional scope tracking needed.
//!
//! **Example**:
//! ```cypher
//! MATCH (a:Person)-[:KNOWS]->(b), (b)-[:LIVES_IN]->(c)
//! ```
//! - Pattern 1: a=Person (explicit), infer b=Person from KNOWS schema
//! - Pattern 2: b=Person (from pattern 1), infer c=City from LIVES_IN schema
//!
//! **Cross-WITH example**:
//! ```cypher
//! MATCH (a:Person)-[:KNOWS]->(b)
//! WITH b
//! MATCH (b)-[:LIVES_IN]->(c)
//! ```
//! - First MATCH: infer b=Person from KNOWS
//! - WITH exports b with label
//! - Second MATCH: sees b=Person, infers c=City from LIVES_IN

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::AnalyzerError,
        },
        logical_plan::{GraphRel, LogicalPlan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct LabelInference;

impl LabelInference {
    pub fn new() -> Self {
        LabelInference
    }

    /// Recursively walk plan tree and infer missing node labels.
    ///
    /// Strategy:
    /// 1. For each GraphRel, check if connected nodes have labels
    /// 2. If a node has no label, try to infer from relationship schema
    /// 3. Update TableCtx with inferred label
    /// 4. Recurse into child plans
    ///
    /// **Scope handling**: plan_ctx.get_table_ctx() automatically respects
    /// WITH boundaries via is_with_scope flag. No special handling needed.
    fn infer_labels_recursive(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::GraphRel(rel) => {
                log::debug!(
                    "🏷️  LabelInference: Processing GraphRel '{}' (labels: {:?})",
                    rel.alias,
                    rel.labels
                );

                // Check left connection (start node)
                let left_label = self.get_or_infer_label(
                    &rel.left_connection,
                    &rel.labels,
                    true, // is_from_side
                    plan_ctx,
                    graph_schema,
                )?;

                // Check right connection (end node)
                let right_label = self.get_or_infer_label(
                    &rel.right_connection,
                    &rel.labels,
                    false, // is_to_side
                    plan_ctx,
                    graph_schema,
                )?;

                log::debug!(
                    "🏷️  LabelInference: '{}' → [{}] → '{}' (labels: {:?}, {:?})",
                    rel.left_connection,
                    rel.labels.as_ref().map(|v| v.join("|")).unwrap_or_else(|| "None".to_string()),
                    rel.right_connection,
                    left_label,
                    right_label
                );

                // Recurse into children
                let left_transformed = self.infer_labels_recursive(rel.left.clone(), plan_ctx, graph_schema)?;
                let center_transformed = self.infer_labels_recursive(rel.center.clone(), plan_ctx, graph_schema)?;
                let right_transformed = self.infer_labels_recursive(rel.right.clone(), plan_ctx, graph_schema)?;

                // If any child was transformed, rebuild GraphRel
                if left_transformed.is_yes() || center_transformed.is_yes() || right_transformed.is_yes() {
                    let new_rel = GraphRel {
                        left: left_transformed.get_plan().clone(),
                        center: center_transformed.get_plan().clone(),
                        right: right_transformed.get_plan().clone(),
                        alias: rel.alias.clone(),
                        direction: rel.direction.clone(),
                        left_connection: rel.left_connection.clone(),
                        right_connection: rel.right_connection.clone(),
                        is_rel_anchor: rel.is_rel_anchor,
                        variable_length: rel.variable_length.clone(),
                        shortest_path_mode: rel.shortest_path_mode.clone(),
                        path_variable: rel.path_variable.clone(),
                        where_predicate: rel.where_predicate.clone(),
                        labels: rel.labels.clone(),
                        is_optional: rel.is_optional,
                        anchor_connection: rel.anchor_connection.clone(),
                        cte_references: rel.cte_references.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::WithClause(wc) => {
                // Process input (pattern before WITH)
                let input_transformed = self.infer_labels_recursive(wc.input.clone(), plan_ctx, graph_schema)?;
                
                if input_transformed.is_yes() {
                    let new_wc = crate::query_planner::logical_plan::WithClause {
                        input: input_transformed.get_plan().clone(),
                        items: wc.items.clone(),
                        distinct: wc.distinct,
                        order_by: wc.order_by.clone(),
                        skip: wc.skip,
                        limit: wc.limit,
                        exported_aliases: wc.exported_aliases.clone(),
                        where_clause: wc.where_clause.clone(),
                        cte_references: wc.cte_references.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Projection(proj) => {
                let input_transformed = self.infer_labels_recursive(proj.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input_transformed.get_plan().clone(),
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(new_proj))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_transformed = self.infer_labels_recursive(filter.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: input_transformed.get_plan().clone(),
                        predicate: filter.predicate.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::CartesianProduct(cp) => {
                let left_transformed = self.infer_labels_recursive(cp.left.clone(), plan_ctx, graph_schema)?;
                let right_transformed = self.infer_labels_recursive(cp.right.clone(), plan_ctx, graph_schema)?;
                
                if left_transformed.is_yes() || right_transformed.is_yes() {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: left_transformed.get_plan().clone(),
                        right: right_transformed.get_plan().clone(),
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(new_cp))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphNode(_) 
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Scan(_)
            | LogicalPlan::Empty => {
                // Leaf nodes - no recursion needed
                Ok(Transformed::No(plan))
            }

            LogicalPlan::GraphJoins(gj) => {
                let input_transformed = self.infer_labels_recursive(gj.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_gj = crate::query_planner::logical_plan::GraphJoins {
                        input: input_transformed.get_plan().clone(),
                        joins: gj.joins.clone(),
                        optional_aliases: gj.optional_aliases.clone(),
                        anchor_table: gj.anchor_table.clone(),
                        cte_references: gj.cte_references.clone(),
                        correlation_predicates: gj.correlation_predicates.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(new_gj))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GroupBy(gb) => {
                let input_transformed = self.infer_labels_recursive(gb.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_gb = crate::query_planner::logical_plan::GroupBy {
                        input: input_transformed.get_plan().clone(),
                        expressions: gb.expressions.clone(),
                        having_clause: gb.having_clause.clone(),
                        is_materialization_boundary: gb.is_materialization_boundary,
                        exposed_alias: gb.exposed_alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_gb))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::OrderBy(ob) => {
                let input_transformed = self.infer_labels_recursive(ob.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_ob = crate::query_planner::logical_plan::OrderBy {
                        input: input_transformed.get_plan().clone(),
                        items: ob.items.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(new_ob))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Limit(limit) => {
                let input_transformed = self.infer_labels_recursive(limit.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: input_transformed.get_plan().clone(),
                        count: limit.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Union(union) => {
                // Union has Vec<Arc<LogicalPlan>>, need to transform each
                let mut transformed = false;
                let mut new_inputs = Vec::new();
                for input in &union.inputs {
                    let input_tf = self.infer_labels_recursive(input.clone(), plan_ctx, graph_schema)?;
                    if input_tf.is_yes() {
                        transformed = true;
                        new_inputs.push(input_tf.get_plan().clone());
                    } else {
                        new_inputs.push(input.clone());
                    }
                }
                
                if transformed {
                    let new_union = crate::query_planner::logical_plan::Union {
                        inputs: new_inputs,
                        union_type: union.union_type.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(new_union))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::PageRank(_pr) => {
                // PageRank doesn't have an input field - it's a leaf node
                Ok(Transformed::No(plan))
            }

            LogicalPlan::Unwind(unwind) => {
                let input_transformed = self.infer_labels_recursive(unwind.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_unwind = crate::query_planner::logical_plan::Unwind {
                        input: input_transformed.get_plan().clone(),
                        expression: unwind.expression.clone(),
                        alias: unwind.alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_unwind))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Skip(skip) => {
                let input_transformed = self.infer_labels_recursive(skip.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_skip = crate::query_planner::logical_plan::Skip {
                        input: input_transformed.get_plan().clone(),
                        count: skip.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Cte(cte) => {
                let input_transformed = self.infer_labels_recursive(cte.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_cte = crate::query_planner::logical_plan::Cte {
                        input: input_transformed.get_plan().clone(),
                        name: cte.name.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Cte(new_cte))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }
        }
    }

    /// Get existing label or infer from relationship schema.
    ///
    /// Returns: Some(label) if found/inferred, None if couldn't infer.
    /// Side effect: Updates plan_ctx if label was inferred.
    fn get_or_infer_label(
        &self,
        node_alias: &str,
        rel_labels: &Option<Vec<String>>,
        is_from_side: bool,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Option<String>> {
        // Try to get existing label from plan_ctx
        if let Ok(table_ctx) = plan_ctx.get_table_ctx(node_alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                log::debug!("🏷️  LabelInference: '{}' already has label: {}", node_alias, label);
                return Ok(Some(label));
            }
        }

        // No existing label - try to infer from relationship
        let rel_labels = match rel_labels {
            Some(labels) => labels,
            None => {
                log::debug!("🏷️  LabelInference: '{}' has no label and relationship has no types", node_alias);
                return Ok(None);
            }
        };

        if rel_labels.is_empty() {
            log::debug!("🏷️  LabelInference: '{}' has no label and relationship has empty types", node_alias);
            return Ok(None);
        }

        // For multiple relationship types, we can't infer reliably
        // (different types might have different node labels)
        if rel_labels.len() > 1 {
            log::debug!(
                "🏷️  LabelInference: '{}' has no label and relationship has multiple types {:?} - can't infer",
                node_alias,
                rel_labels
            );
            return Ok(None);
        }

        let rel_type = &rel_labels[0];
        
        // Look up relationship schema
        let rel_schema = graph_schema.get_relationships_schema_opt(rel_type)
            .ok_or_else(|| {
                AnalyzerError::InvalidPlan(format!(
                    "Relationship type '{}' not found in schema",
                    rel_type
                ))
            })?;

        // Infer label from schema
        let inferred_label = if is_from_side {
            &rel_schema.from_node
        } else {
            &rel_schema.to_node
        };

        log::info!(
            "🏷️  LabelInference: Inferred '{}' label as '{}' from {} side of '{}' relationship",
            node_alias,
            inferred_label,
            if is_from_side { "from" } else { "to" },
            rel_type
        );

        // Update TableCtx with inferred label
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(node_alias) {
            table_ctx.set_labels(Some(vec![inferred_label.clone()]));
            log::debug!("🏷️  LabelInference: Updated TableCtx for '{}' with label '{}'", node_alias, inferred_label);
        } else {
            log::warn!(
                "🏷️  LabelInference: Could not find TableCtx for '{}' to update with inferred label",
                node_alias
            );
        }

        Ok(Some(inferred_label.clone()))
    }
}

impl AnalyzerPass for LabelInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::info!("🏷️  LabelInference: Starting label inference pass");
        let result = self.infer_labels_recursive(logical_plan, plan_ctx, graph_schema)?;
        log::info!("🏷️  LabelInference: Completed - plan transformed: {}", result.is_yes());
        Ok(result)
    }
}
