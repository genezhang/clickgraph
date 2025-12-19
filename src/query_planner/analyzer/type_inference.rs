//! Type Inference Analyzer Pass
//!
//! **Purpose**: Infer missing node labels AND relationship types from graph schema.
//!
//! **Problem**: Cypher allows omitting types when they can be inferred:
//! ```cypher
//! MATCH (a:Person)-[r]->(b)        -- r has no type, b has no label
//! MATCH ()-[r:KNOWS]->()           -- nodes have no labels
//! MATCH ()-[r]->()                 -- nothing specified!
//! ```
//!
//! **Solution**: Smart inference using graph schema:
//!
//! **Node Label Inference**:
//! 1. From relationship: If KNOWS connects Person → Person, infer node labels
//! 2. From schema: If only one node type exists, use it
//! 3. From connected relationships: Propagate labels through patterns
//!
//! **Edge Type Inference**:
//! 1. From nodes: If Person-?->City and only LIVES_IN connects them, infer LIVES_IN
//! 2. From schema: If only one relationship type exists, use it
//! 3. From pattern: Use relationship properties to disambiguate
//!
//! **When to run**: Early in analyzer pipeline (position 2, after SchemaInference)
//! This ensures all downstream passes have complete type information.
//!
//! **Examples**:
//! ```cypher
//! // Infer node labels from edge type
//! MATCH (a)-[:KNOWS]->(b)           → a:Person, b:Person
//!
//! // Infer edge type from node labels  
//! MATCH (a:Person)-[r]->(b:City)    → r:LIVES_IN
//!
//! // Infer everything (if only one edge type exists)
//! MATCH (a)-[r]->(b)                → a:Person, r:KNOWS, b:Person
//!
//! // Cross-WITH inference
//! MATCH (a:Person)-[:KNOWS]->(b)
//! WITH b
//! MATCH (b)-[:LIVES_IN]->(c)        → b:Person, c:City
//! ```

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::AnalyzerError,
        },
        logical_plan::{GraphNode, GraphRel, LogicalPlan, ViewScan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct TypeInference;

impl TypeInference {
    pub fn new() -> Self {
        TypeInference
    }

    /// Recursively walk plan tree and infer missing types (node labels + edge types).
    ///
    /// Strategy:
    /// 1. For each GraphRel:
    ///    a. Infer missing edge type from node labels (if both known)
    ///    b. Infer missing node labels from edge type (if known)
    ///    c. If still missing, try schema-level defaults
    /// 2. Update plan_ctx with inferred types
    /// 3. Recurse into child plans
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
                    "🔍 TypeInference: Processing GraphRel '{}' (edge_types: {:?})",
                    rel.alias,
                    rel.labels
                );

                // STEP 1: Get or infer edge type(s)
                let edge_types = self.infer_edge_types(
                    &rel.labels,
                    &rel.left_connection,
                    &rel.right_connection,
                    plan_ctx,
                    graph_schema,
                )?;

                // STEP 2: Infer node labels from edge types
                let left_label = self.get_or_infer_node_label(
                    &rel.left_connection,
                    &edge_types,
                    true, // is_from_side
                    plan_ctx,
                    graph_schema,
                )?;

                let right_label = self.get_or_infer_node_label(
                    &rel.right_connection,
                    &edge_types,
                    false, // is_to_side
                    plan_ctx,
                    graph_schema,
                )?;

                log::info!(
                    "🔍 TypeInference: '{}' → [{}] → '{}' (labels: {:?}, {:?})",
                    rel.left_connection,
                    edge_types.as_ref().map(|v| v.join("|")).unwrap_or_else(|| "?".to_string()),
                    rel.right_connection,
                    left_label,
                    right_label
                );

                // Recurse into children
                let left_transformed = self.infer_labels_recursive(rel.left.clone(), plan_ctx, graph_schema)?;
                let center_transformed = self.infer_labels_recursive(rel.center.clone(), plan_ctx, graph_schema)?;
                let right_transformed = self.infer_labels_recursive(rel.right.clone(), plan_ctx, graph_schema)?;

                // Check if we need to rebuild with inferred edge types
                let needs_rebuild = left_transformed.is_yes() 
                    || center_transformed.is_yes() 
                    || right_transformed.is_yes()
                    || (edge_types.is_some() && edge_types != rel.labels);

                if needs_rebuild {
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
                        labels: edge_types.or_else(|| rel.labels.clone()),  // Use inferred types
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

            LogicalPlan::GraphNode(node) => {
                // Check if this node needs ViewScan creation from inferred label
                if node.label.is_none() {
                    // Try to get inferred label from plan_ctx
                    if let Ok(table_ctx) = plan_ctx.get_table_ctx(&node.alias) {
                        if let Some(labels) = &table_ctx.get_labels() {
                            if let Some(label) = labels.first() {
                                log::info!("🏷️ TypeInference: Creating ViewScan for GraphNode '{}' with inferred label '{}'", node.alias, label);
                                
                                // Get node schema to create ViewScan
                                if let Ok(node_schema) = graph_schema.get_node_schema(label) {
                                    let full_table_name = format!("{}.{}", node_schema.database, node_schema.table_name);
                                    let id_column = node_schema
                                        .node_id
                                        .columns()
                                        .first()
                                        .unwrap_or(&"id")
                                        .to_string();
                                    
                                    let view_scan = ViewScan::new(
                                        full_table_name,
                                        None,
                                        node_schema.property_mappings.clone(),
                                        id_column,
                                        vec!["id".to_string()],
                                        vec![],
                                    );
                                    
                                    // Create new GraphNode with ViewScan input and label
                                    let new_node = GraphNode {
                                        input: Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))),
                                        alias: node.alias.clone(),
                                        label: Some(label.clone()),
                                        is_denormalized: false,
                                        projected_columns: None,
                                    };
                                    
                                    return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(new_node))));
                                }
                            }
                        }
                    }
                }
                // No changes needed
                Ok(Transformed::No(plan))
            }
            
            LogicalPlan::ViewScan(_)
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

    /// Infer edge type(s) from node labels or schema.
    ///
    /// Strategy:
    /// 1. If edge_types already specified → use them
    /// 2. If both node labels known → find relationships connecting them
    /// 3. If only one edge type in entire schema → use it
    /// 4. Otherwise → return None (can't infer)
    ///
    /// Returns: Some(vec![edge_type]) if inferred, None if couldn't infer
    fn infer_edge_types(
        &self,
        current_types: &Option<Vec<String>>,
        left_connection: &str,
        right_connection: &str,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Option<Vec<String>>> {
        // If types already specified, use them
        if current_types.is_some() {
            log::debug!("🔗 TypeInference: Edge types already specified: {:?}", current_types);
            return Ok(current_types.clone());
        }

        // Try to get node labels from plan_ctx
        let left_label = plan_ctx
            .get_table_ctx(left_connection)
            .ok()
            .and_then(|ctx| ctx.get_label_opt().map(|s| s.to_string()));
        
        let right_label = plan_ctx
            .get_table_ctx(right_connection)
            .ok()
            .and_then(|ctx| ctx.get_label_opt().map(|s| s.to_string()));

        log::debug!(
            "🔗 TypeInference: Inferring edge type for '{}' ({:?}) -> '{}' ({:?})",
            left_connection, left_label, right_connection, right_label
        );

        // Strategy 2: If both labels known, find relationships connecting them
        if let (Some(ref from_label), Some(ref to_label)) = (left_label, right_label) {
            let connecting_rels: Vec<String> = graph_schema
                .get_relationships_schemas()
                .iter()
                .filter(|(_, rel_schema)| {
                    &rel_schema.from_node == from_label && &rel_schema.to_node == to_label
                })
                .map(|(rel_type, _)| rel_type.clone())
                .collect();

            if !connecting_rels.is_empty() {
                log::info!(
                    "🔗 TypeInference: Inferred edge type(s) from node labels {} -> {}: {:?}",
                    from_label, to_label, connecting_rels
                );
                return Ok(Some(connecting_rels));
            } else {
                log::debug!(
                    "🔗 TypeInference: No relationships found connecting {} -> {}",
                    from_label, to_label
                );
            }
        }

        // Strategy 3: If only one edge type in entire schema, use it
        let all_rel_types: Vec<String> = graph_schema
            .get_relationships_schemas()
            .keys()
            .filter_map(|k| {
                // Extract type name from composite key "TYPE::FROM::TO"
                k.split("::").next().map(|s| s.to_string())
            })
            .collect::<std::collections::HashSet<_>>() // Deduplicate
            .into_iter()
            .collect();

        if all_rel_types.len() == 1 {
            log::info!(
                "🔗 TypeInference: Only one edge type in schema, using: {}",
                all_rel_types[0]
            );
            return Ok(Some(vec![all_rel_types[0].clone()]));
        }

        // Can't infer
        log::debug!("🔗 TypeInference: Could not infer edge type");
        Ok(None)
    }

    /// Get existing node label or infer from edge type(s).
    ///
    /// Returns: Some(label) if found/inferred, None if couldn't infer.
    /// Side effect: Updates plan_ctx if label was inferred.
    fn get_or_infer_node_label(
        &self,
        node_alias: &str,
        edge_types: &Option<Vec<String>>,
        is_from_side: bool,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Option<String>> {
        // Try to get existing label from plan_ctx
        if let Ok(table_ctx) = plan_ctx.get_table_ctx(node_alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                log::debug!("🏷️ TypeInference: '{}' already has label: {}", node_alias, label);
                return Ok(Some(label));
            }
        }

        // No existing label - try to infer from edge type(s)
        let edge_types = match edge_types {
            Some(types) => types,
            None => {
                log::debug!("🏷️ TypeInference: '{}' has no label and relationship has no types", node_alias);
                return Ok(None);
            }
        };

        if edge_types.is_empty() {
            log::debug!("🏷️ TypeInference: '{}' has no label and relationship has empty types", node_alias);
            return Ok(None);
        }

        // For multiple relationship types, we can't infer reliably
        // (different types might have different node labels)
        if edge_types.len() > 1 {
            log::debug!(
                "🏷️ TypeInference: '{}' has no label and relationship has multiple types {:?} - can't infer",
                node_alias,
                edge_types
            );
            return Ok(None);
        }

        let rel_type = &edge_types[0];
        
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
            "🏷️ TypeInference: Inferred '{}' label as '{}' from {} side of '{}' relationship",
            node_alias,
            inferred_label,
            if is_from_side { "from" } else { "to" },
            rel_type
        );

        // Update TableCtx with inferred label
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(node_alias) {
            table_ctx.set_labels(Some(vec![inferred_label.clone()]));
            log::debug!("🏷️ TypeInference: Updated TableCtx for '{}' with label '{}'", node_alias, inferred_label);
        } else {
            log::warn!(
                "🏷️ TypeInference: Could not find TableCtx for '{}' to update with inferred label",
                node_alias
            );
        }

        Ok(Some(inferred_label.clone()))
    }
}

impl AnalyzerPass for TypeInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::info!("🏷️ TypeInference: Starting type inference pass");
        let result = self.infer_labels_recursive(logical_plan, plan_ctx, graph_schema)?;
        log::info!("🏷️ TypeInference: Completed - plan transformed: {}", result.is_yes());
        Ok(result)
    }
}
