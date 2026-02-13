//! Type Inference Analyzer Pass
//!
//! **Purpose**: Infer missing node labels AND relationship types from graph schema.
//!
//! **CRITICAL INVARIANT** ⚠️ **Parser Normalization**:
//! The Cypher parser ALREADY normalizes relationship direction in the logical plan:
//! - `left_connection` ALWAYS means the FROM node (source of relationship)
//! - `right_connection` ALWAYS means the TO node (target of relationship)  
//! - `direction` field only records original syntax (for display/SQL generation)
//!
//! Examples:
//! ```cypher
//! // Outgoing: (a)-[:REL]->(b)
//! // Parser creates: left="a", right="b", direction=Outgoing
//! // a is FROM, b is TO ✓
//!
//! // Incoming: (a)<-[:REL]-(b)  
//! // Parser creates: left="b", right="a", direction=Incoming
//! // b is FROM, a is TO ✓ (parser swapped them!)
//! ```
//!
//! **TypeInference Strategy** (query schema like a database):
//! Use KNOWN facts as filters to find candidates for UNKNOWN labels:
//!
//! **Known Facts**:
//! - Relationship type (if specified): `[:KNOWS]`
//! - Node labels (if specified): `(a:Person)`
//! - Direction (normalized in plan structure, not the field!)
//! - Graph schema (relationship definitions: FROM→TO)
//!
//! **Inference Rules**:
//! 1. If relationship type known → look up schema → infer node labels from from_node/to_node
//! 2. If both node labels known → look up schema → infer relationship type
//! 3. Always use: left_connection → from_node, right_connection → to_node
//!    (DO NOT check direction field - parser already normalized!)
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
        logical_expr::LogicalExpr,
        logical_plan::{GraphNode, GraphRel, LogicalPlan, ViewScan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

// Note: MAX_INFERRED_TYPES constant was removed as dead code.
// The default value (5) is now set directly in plan_builder.rs
// via `max_inferred_types.unwrap_or(5)` and documented in plan_ctx.

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

                // CRITICAL: Process children FIRST (bottom-up) so inner patterns establish node labels
                // For multi-hop (a)-[r1]->(b)-[r2]->(c), we need to:
                // 1. Process r1 first → establishes b's label
                // 2. Then process r2 → can use b's label from step 1
                let left_transformed =
                    self.infer_labels_recursive(rel.left.clone(), plan_ctx, graph_schema)?;
                let center_transformed =
                    self.infer_labels_recursive(rel.center.clone(), plan_ctx, graph_schema)?;
                let right_transformed =
                    self.infer_labels_recursive(rel.right.clone(), plan_ctx, graph_schema)?;

                // Connected Pattern Detection (Optimization - WIP)
                // Detect if left child is GraphRel → means we have connected patterns
                // Example: (a)-[r1]->(b)-[r2]->(c) where b is shared
                //
                // Plan tree structure:
                //   GraphRel(r2, alias="r2")
                //     ├─ left: GraphRel(r1, alias="r1")  ← DETECT THIS!
                //     │    ├─ left_connection: "a"
                //     │    └─ right_connection: "b"
                //     └─ right: GraphNode(c)
                //        ├─ left_connection: "b"  ← SHARED with r1.right_connection!
                //        └─ right_connection: "c"
                //
                // Strategy: If left child is GraphRel, check if its right_connection
                // matches our left_connection → that's the shared variable!
                //
                // Store connection info to process AFTER infer_pattern_types completes
                let connected_pattern_info = match &left_transformed {
                    Transformed::Yes(plan) | Transformed::No(plan) => {
                        if let LogicalPlan::GraphRel(left_rel) = plan.as_ref() {
                            // Check for shared variable
                            if left_rel.right_connection == rel.left_connection {
                                let shared_var = rel.left_connection.clone();
                                log::info!(
                                    "🔗 Connected Pattern Optimization: Detected connected patterns! r1='{}' → {} ← r2='{}' (shared var: '{}')",
                                    left_rel.alias,
                                    shared_var,
                                    rel.alias,
                                    shared_var
                                );
                                Some((
                                    left_rel.alias.clone(),
                                    left_rel.left_connection.clone(),
                                    left_rel.right_connection.clone(),
                                    shared_var,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };

                // NOW infer labels for THIS level using updated plan_ctx from children
                // Use UNIFIED constraint-based inference: gather all known facts, query schema together
                let (edge_types, left_label, mut right_label) = self.infer_pattern_types(
                    &rel.labels,
                    &rel.left_connection,
                    &rel.right_connection,
                    plan_ctx,
                    graph_schema,
                )?;

                // Group Optimization for Connected Patterns (WIP)
                // If we detected a connection earlier, now both patterns have been processed
                // and their combinations are stored in plan_ctx. Time to optimize!
                if let Some((r1_alias, r1_left, r1_right, _shared_var)) = connected_pattern_info {
                    log::info!("🔗 Connected Pattern Optimization: Starting group optimization for r1='{}' + r2='{}'", r1_alias, rel.alias);

                    // Get combinations for both patterns from plan_ctx
                    let r1_combos = plan_ctx.get_pattern_combinations(&r1_left, &r1_right);
                    let r2_combos = plan_ctx
                        .get_pattern_combinations(&rel.left_connection, &rel.right_connection);

                    log::info!(
                        "  📊 r1 has {} combinations, r2 has {} combinations",
                        r1_combos.as_ref().map(|v| v.len()).unwrap_or(0),
                        r2_combos.as_ref().map(|v| v.len()).unwrap_or(0)
                    );

                    // If BOTH patterns have combinations, do the optimization
                    if let (Some(r1_combos), Some(r2_combos)) = (r1_combos, r2_combos) {
                        log::info!("  🎯 Both patterns have combinations! Applying shared variable constraint...");

                        // Filter: Only keep combinations where shared variable has matching type
                        // r1: (a:?)-[r1:?]->(b:?) where b = shared_var (r1.to_label)
                        // r2: (b:?)-[r2:?]->(c:?) where b = shared_var (r2.from_label)
                        // Constraint: r1.to_label must equal r2.from_label

                        use crate::query_planner::plan_ctx::GroupCombination;
                        use std::collections::HashMap;
                        let mut group_combos = Vec::new();

                        for r1_combo in r1_combos {
                            for r2_combo in r2_combos {
                                // Check constraint: shared variable type must match
                                if r1_combo.to_label == r2_combo.from_label {
                                    // Valid combination! Create GroupCombination
                                    let mut pattern_types = HashMap::new();
                                    pattern_types.insert(
                                        (r1_left.clone(), r1_right.clone()),
                                        r1_combo.clone(),
                                    );
                                    pattern_types.insert(
                                        (rel.left_connection.clone(), rel.right_connection.clone()),
                                        r2_combo.clone(),
                                    );

                                    group_combos.push(GroupCombination { pattern_types });
                                }
                            }
                        }

                        log::info!(
                            "  ✅ Group optimization reduced {} × {} = {} combinations to {} valid combinations!",
                            r1_combos.len(),
                            r2_combos.len(),
                            r1_combos.len() * r2_combos.len(),
                            group_combos.len()
                        );

                        // Store group combinations
                        let group_id = format!("{}_{}", r1_alias, rel.alias);
                        plan_ctx.store_group_combinations(group_id.clone(), group_combos.clone());

                        // TODO(future): Distribute back to individual patterns for more efficient queries
                        log::info!(
                            "  📦 Stored {} group combinations with group_id='{}'",
                            group_combos.len(),
                            group_id
                        );
                    } else {
                        log::info!("  ⚠️ Skipping group optimization: one or both patterns have no combinations");
                    }
                }

                // **PART 2A: Multi-Type End Node Inference**
                // For patterns with multiple relationship types (VLP or non-VLP):
                // If end node has no label, infer possible labels from relationship schemas
                //
                // Examples:
                // 1. VLP: (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
                //    FOLLOWS: User → User
                //    AUTHORED: User → Post
                //    Therefore: x can be User OR Post → infer x.labels = [User, Post]
                //
                // 2. Non-VLP multi-type: (u:User)--(x)  [undirected = multiple types]
                //    FOLLOWS: User → User
                //    AUTHORED: User → Post
                //    Therefore: x can be User OR Post → infer x.labels = [User, Post]
                //
                // 3. Non-VLP explicit multi-type: (u:User)-[:FOLLOWS|AUTHORED]->(x)
                //
                // EXTENDED: Now handles both VLP and non-VLP patterns with multiple types
                let inferred_multi_labels = if right_label.is_none()
                    && edge_types.as_ref().is_some_and(|types| types.len() > 1)
                {
                    log::info!(
                        "🎯 TypeInference: Multi-type pattern detected for '{}': VLP={:?}, edge_types={:?}, direction={:?}",
                        rel.right_connection,
                        rel.variable_length,
                        edge_types,
                        rel.direction
                    );

                    // Collect to_node from each relationship type
                    // For bi-directional patterns (--), also include from_node as possibilities
                    let mut to_node_labels = std::collections::HashSet::new();
                    let is_bidirectional = matches!(
                        rel.direction,
                        crate::query_planner::logical_expr::Direction::Either
                    );

                    if let Some(ref types) = edge_types {
                        for rel_type in types {
                            // Use get_all_rel_schemas_by_type to handle composite keys
                            let rel_schemas = graph_schema.rel_schemas_for_type(rel_type);
                            log::debug!(
                                "🎯 TypeInference: Found {} schema(s) for rel_type '{}'",
                                rel_schemas.len(),
                                rel_type
                            );
                            for rel_schema in rel_schemas {
                                // Always include to_node (forward direction)
                                to_node_labels.insert(rel_schema.to_node.clone());

                                // For bi-directional patterns, also include from_node (reverse direction)
                                if is_bidirectional {
                                    to_node_labels.insert(rel_schema.from_node.clone());
                                    log::debug!(
                                        "🎯 TypeInference: Bi-directional pattern - added both from_node='{}' and to_node='{}'",
                                        rel_schema.from_node,
                                        rel_schema.to_node
                                    );
                                }
                            }
                        }
                    }

                    let inferred_labels: Vec<String> = to_node_labels.into_iter().collect();
                    if !inferred_labels.is_empty() {
                        log::info!(
                            "🎯 TypeInference: Multi-type VLP auto-inference for '{}' → labels: {:?}",
                            rel.right_connection,
                            inferred_labels
                        );

                        // Update plan_ctx with inferred labels (plural)
                        if let Some(table_ctx) =
                            plan_ctx.get_mut_table_ctx_opt(&rel.right_connection)
                        {
                            table_ctx.set_labels(Some(inferred_labels.clone()));
                        } else {
                            use crate::query_planner::plan_ctx::TableCtx;
                            plan_ctx.insert_table_ctx(
                                rel.right_connection.clone(),
                                TableCtx::build(
                                    rel.right_connection.clone(),
                                    Some(inferred_labels.clone()),
                                    vec![], // properties: empty for now
                                    false,  // is_rel: false (this is a node)
                                    false,  // explicit_alias: false (inferred)
                                ),
                            );
                        }

                        // Return first label for backward compatibility with single-label logic
                        // The full labels list is now stored in plan_ctx for path enumeration
                        right_label = inferred_labels.first().cloned();
                        Some(inferred_labels)
                    } else {
                        log::debug!(
                            "🎯 TypeInference: No schemas found for edge_types {:?}",
                            edge_types
                        );
                        None
                    }
                } else {
                    log::debug!(
                        "🎯 TypeInference: Multi-type VLP inference skipped for '{}': right_label={:?}, VLP={:?}, edge_types={:?}",
                        rel.right_connection,
                        right_label,
                        rel.variable_length,
                        edge_types
                    );
                    None
                };

                log::info!(
                    "🔍 TypeInference: '{}' → [{}] → '{}' (labels: {:?}, {:?}{})",
                    rel.left_connection,
                    edge_types
                        .as_ref()
                        .map(|v| v.join("|"))
                        .unwrap_or_else(|| "?".to_string()),
                    rel.right_connection,
                    left_label,
                    right_label,
                    if let Some(labels) = &inferred_multi_labels {
                        format!(" [multi-type VLP inferred: {:?}]", labels)
                    } else {
                        String::new()
                    }
                );

                // Check if we need to rebuild with inferred edge types
                let needs_rebuild = left_transformed.is_yes()
                    || center_transformed.is_yes()
                    || right_transformed.is_yes()
                    || (edge_types.is_some() && edge_types != rel.labels)
                    || inferred_multi_labels.is_some();

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
                        labels: edge_types.or_else(|| rel.labels.clone()), // Use inferred types
                        is_optional: rel.is_optional,
                        anchor_connection: rel.anchor_connection.clone(),
                        cte_references: rel.cte_references.clone(),
                        pattern_combinations: None, // TODO(future): Will be set by group optimization
                        was_undirected: rel.was_undirected,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::WithClause(wc) => {
                // Process input (pattern before WITH)
                let input_transformed =
                    self.infer_labels_recursive(wc.input.clone(), plan_ctx, graph_schema)?;

                if input_transformed.is_yes() {
                    let new_wc = crate::query_planner::logical_plan::WithClause {
                        cte_name: None,
                        input: input_transformed.get_plan().clone(),
                        items: wc.items.clone(),
                        distinct: wc.distinct,
                        order_by: wc.order_by.clone(),
                        skip: wc.skip,
                        limit: wc.limit,
                        exported_aliases: wc.exported_aliases.clone(),
                        where_clause: wc.where_clause.clone(),
                        cte_references: wc.cte_references.clone(),
                        pattern_comprehensions: wc.pattern_comprehensions.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Projection(proj) => {
                let input_transformed =
                    self.infer_labels_recursive(proj.input.clone(), plan_ctx, graph_schema)?;
                if input_transformed.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input_transformed.get_plan().clone(),
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                        pattern_comprehensions: proj.pattern_comprehensions.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_transformed =
                    self.infer_labels_recursive(filter.input.clone(), plan_ctx, graph_schema)?;
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
                let left_transformed =
                    self.infer_labels_recursive(cp.left.clone(), plan_ctx, graph_schema)?;
                let right_transformed =
                    self.infer_labels_recursive(cp.right.clone(), plan_ctx, graph_schema)?;

                if left_transformed.is_yes() || right_transformed.is_yes() {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: left_transformed.get_plan().clone(),
                        right: right_transformed.get_plan().clone(),
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(
                        new_cp,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphNode(node) => {
                // Check if this node needs ViewScan creation from inferred label
                if node.label.is_none() {
                    // Try to get inferred label from plan_ctx
                    if let Ok(table_ctx) = plan_ctx.get_table_ctx(&node.alias) {
                        if let Some(labels) = table_ctx.get_labels() {
                            let labels_vec = labels.to_vec(); // Clone to avoid borrow issues

                            // Check if we have multiple labels → multi-type node
                            if labels_vec.len() > 1 {
                                log::info!(
                                    "🎯 TypeInference: GraphNode '{}' has {} possible types: {:?} → Will generate UNION",
                                    node.alias,
                                    labels_vec.len(),
                                    labels_vec
                                );

                                // Store for CTE generation
                                plan_ctx.store_node_combinations(&node.alias, labels_vec.clone());

                                // Use first label for compatibility (downstream analyzers expect single type)
                                let first_label = labels_vec.first().unwrap();

                                if let Ok(node_schema) = graph_schema.node_schema(first_label) {
                                    let full_table_name = format!(
                                        "{}.{}",
                                        node_schema.database, node_schema.table_name
                                    );
                                    let id_column = node_schema
                                        .node_id
                                        .columns()
                                        .first()
                                        .ok_or_else(|| AnalyzerError::SchemaNotFound(
                                            format!("Node schema for label '{}' has no ID columns defined", first_label)
                                        ))?
                                        .to_string();

                                    let mut view_scan = ViewScan::new(
                                        full_table_name,
                                        None,
                                        node_schema.property_mappings.clone(),
                                        id_column,
                                        vec!["id".to_string()],
                                        vec![],
                                    );

                                    // Copy denormalization metadata from node_schema
                                    view_scan.is_denormalized = node_schema.is_denormalized;
                                    view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
                                        props.iter().map(|(k, v)| {
                                            (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                        }).collect()
                                    });
                                    view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
                                        props.iter().map(|(k, v)| {
                                            (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                        }).collect()
                                    });

                                    // Create new GraphNode with ViewScan input and FIRST label
                                    let new_node = GraphNode {
                                        input: Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))),
                                        alias: node.alias.clone(),
                                        label: Some(first_label.clone()),
                                        is_denormalized: node_schema.is_denormalized,
                                        projected_columns: None,
                                        node_types: Some(labels_vec.clone()), // Store all types
                                    };

                                    return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                                        new_node,
                                    ))));
                                }
                            } else if let Some(label) = labels.first() {
                                // Single label - existing logic
                                log::info!("🏷️ TypeInference: Creating ViewScan for GraphNode '{}' with inferred label '{}'", node.alias, label);

                                // Get node schema to create ViewScan
                                if let Ok(node_schema) = graph_schema.node_schema(label) {
                                    let full_table_name = format!(
                                        "{}.{}",
                                        node_schema.database, node_schema.table_name
                                    );
                                    let id_column = node_schema
                                        .node_id
                                        .columns()
                                        .first()
                                        .ok_or_else(|| AnalyzerError::SchemaNotFound(
                                            format!("Node schema for label '{}' has no ID columns defined", label)
                                        ))?
                                        .to_string();

                                    let mut view_scan = ViewScan::new(
                                        full_table_name,
                                        None,
                                        node_schema.property_mappings.clone(),
                                        id_column,
                                        vec!["id".to_string()],
                                        vec![],
                                    );

                                    // Copy denormalization metadata from node_schema
                                    view_scan.is_denormalized = node_schema.is_denormalized;
                                    view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
                                        props.iter().map(|(k, v)| {
                                            (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                        }).collect()
                                    });
                                    view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
                                        props.iter().map(|(k, v)| {
                                            (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                        }).collect()
                                    });

                                    // Create new GraphNode with ViewScan input and label
                                    let new_node = GraphNode {
                                        input: Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))),
                                        alias: node.alias.clone(),
                                        label: Some(label.clone()),
                                        is_denormalized: node_schema.is_denormalized,
                                        projected_columns: None,
                                        node_types: None, // Single type, no multi-type
                                    };

                                    return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                                        new_node,
                                    ))));
                                }
                            }
                        }
                    } else {
                        // ⭐ NEW: No context at all → infer ALL possible node types
                        let node_schemas = graph_schema.all_node_schemas();
                        let all_labels: Vec<String> = node_schemas.keys().cloned().collect();

                        if all_labels.is_empty() {
                            log::warn!(
                                "⚠️ TypeInference: No node schemas found for GraphNode '{}'",
                                node.alias
                            );
                        } else if all_labels.len() > plan_ctx.max_inferred_types {
                            return Err(AnalyzerError::InvalidPlan(format!(
                                "Too many node types ({}) for untyped node '{}'. Max allowed is {}. Please specify explicit node label.",
                                all_labels.len(),
                                node.alias,
                                plan_ctx.max_inferred_types
                            )));
                        } else {
                            log::info!(
                                "🎯 TypeInference: GraphNode '{}' has no context → inferring {} possible types: {:?}",
                                node.alias,
                                all_labels.len(),
                                all_labels
                            );

                            // Store combinations for CTE generation
                            plan_ctx.store_node_combinations(&node.alias, all_labels.clone());

                            // Create TableCtx with all labels
                            use crate::query_planner::plan_ctx::TableCtx;
                            let table_ctx = TableCtx::build(
                                node.alias.clone(),
                                Some(all_labels.clone()),
                                vec![],
                                false, // is_rel
                                false, // explicit_alias
                            );
                            plan_ctx.insert_table_ctx(node.alias.clone(), table_ctx);

                            // Use first label for ViewScan (backward compatibility)
                            let first_label = &all_labels[0];
                            if let Ok(node_schema) = graph_schema.node_schema(first_label) {
                                let full_table_name =
                                    format!("{}.{}", node_schema.database, node_schema.table_name);
                                let id_column = node_schema
                                    .node_id
                                    .columns()
                                    .first()
                                    .ok_or_else(|| {
                                        AnalyzerError::SchemaNotFound(format!(
                                            "Node schema for label '{}' has no ID columns defined",
                                            first_label
                                        ))
                                    })?
                                    .to_string();

                                let mut view_scan = ViewScan::new(
                                    full_table_name,
                                    None,
                                    node_schema.property_mappings.clone(),
                                    id_column,
                                    vec!["id".to_string()],
                                    vec![],
                                );

                                // Copy denormalization metadata
                                view_scan.is_denormalized = node_schema.is_denormalized;
                                view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
                                    props.iter().map(|(k, v)| {
                                        (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                    }).collect()
                                });
                                view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
                                    props.iter().map(|(k, v)| {
                                        (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()))
                                    }).collect()
                                });

                                let new_node = GraphNode {
                                    input: Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))),
                                    alias: node.alias.clone(),
                                    label: Some(first_label.clone()),
                                    is_denormalized: node_schema.is_denormalized,
                                    projected_columns: None,
                                    node_types: Some(all_labels.clone()), // Store all inferred types
                                };

                                return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                                    new_node,
                                ))));
                            }
                        }
                    }
                }
                // No changes needed
                Ok(Transformed::No(plan))
            }

            LogicalPlan::ViewScan(_) | LogicalPlan::Empty => {
                // Leaf nodes - no recursion needed
                Ok(Transformed::No(plan))
            }

            LogicalPlan::GraphJoins(gj) => {
                let input_transformed =
                    self.infer_labels_recursive(gj.input.clone(), plan_ctx, graph_schema)?;
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
                let input_transformed =
                    self.infer_labels_recursive(gb.input.clone(), plan_ctx, graph_schema)?;
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
                let input_transformed =
                    self.infer_labels_recursive(ob.input.clone(), plan_ctx, graph_schema)?;
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
                let input_transformed =
                    self.infer_labels_recursive(limit.input.clone(), plan_ctx, graph_schema)?;
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
                    let input_tf =
                        self.infer_labels_recursive(input.clone(), plan_ctx, graph_schema)?;
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
                let input_transformed =
                    self.infer_labels_recursive(unwind.input.clone(), plan_ctx, graph_schema)?;

                // Try to infer the label from the expression being unwound
                // We look at the plan structure directly since TypeInference runs before FilterTagging
                let label = self.infer_unwind_element_label_from_plan(
                    &unwind.expression,
                    &unwind.input,
                    plan_ctx,
                );

                // Update plan_ctx if we inferred a label
                if let Some(ref label_str) = label {
                    if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&unwind.alias) {
                        table_ctx.set_labels(Some(vec![label_str.clone()]));
                        log::info!(
                            "🏷️ TypeInference: Updated UNWIND alias '{}' with label '{}'",
                            unwind.alias,
                            label_str
                        );
                    }
                }

                // Check if we need to rebuild
                let needs_rebuild =
                    input_transformed.is_yes() || (label.is_some() && label != unwind.label);

                if needs_rebuild {
                    let new_unwind = crate::query_planner::logical_plan::Unwind {
                        input: input_transformed.get_plan().clone(),
                        expression: unwind.expression.clone(),
                        alias: unwind.alias.clone(),
                        label: label.or_else(|| unwind.label.clone()),
                        tuple_properties: unwind.tuple_properties.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_unwind))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Skip(skip) => {
                let input_transformed =
                    self.infer_labels_recursive(skip.input.clone(), plan_ctx, graph_schema)?;
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
                let input_transformed =
                    self.infer_labels_recursive(cte.input.clone(), plan_ctx, graph_schema)?;
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
    /// **Strategy** (combines match_clause and type_inference logic):
    /// 1. If edge_types already specified → use them
    /// 2. If schema has only one relationship → use it
    /// 3. If both node labels known → find relationships connecting them (supports polymorphic)
    /// 4. Otherwise → return None (can't infer)
    ///
    /// **Polymorphic Support**:
    /// - Handles `$any` wildcards in schema (from polymorphic edge tables)
    /// - Checks `from_label_values` and `to_label_values` for runtime discovery
    /// - Applies MAX_INFERRED_TYPES limit to prevent excessive UNION expansion
    ///
    /// Returns: Some(vec![edge_type]) if inferred, None if couldn't infer
    /// Errors: TooManyInferredTypes if more than MAX_INFERRED_TYPES matches
    /// **UNIFIED CONSTRAINT-BASED TYPE INFERENCE**
    ///
    /// Uses ALL known facts together to find matching patterns in schema:
    /// - Known node labels (from explicit labels in query)
    /// - Known edge type (if specified)
    /// - Schema definitions (from_node, to_node, type for each relationship)
    ///
    /// Examples:
    /// - `(a:Airport)-[r:FLIGHT]->(b)`: knows a=Airport, r=FLIGHT → infer b=Airport
    /// - `(a)-[r:FLIGHT]->(b:Airport)`: knows r=FLIGHT, b=Airport → infer a=Airport  
    /// - `(a:IP)-[r]->(b)`: knows a=IP → find all edges from IP, infer r and b
    /// - `(a)-[r]->(b)`: nothing known → can't infer (unless single edge type in schema)
    ///
    /// For polymorphic schemas (LDBC, Zeek), using all constraints together
    /// narrows down possibilities much better than inferring separately.
    fn infer_pattern_types(
        &self,
        current_edge_types: &Option<Vec<String>>,
        left_connection: &str,
        right_connection: &str,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<(Option<Vec<String>>, Option<String>, Option<String>)> {
        // STEP 1: Gather all KNOWN constraints
        let known_left_label = plan_ctx
            .get_table_ctx(left_connection)
            .ok()
            .and_then(|ctx| ctx.get_label_opt());

        let known_right_label = plan_ctx
            .get_table_ctx(right_connection)
            .ok()
            .and_then(|ctx| ctx.get_label_opt());

        let known_edge_types = current_edge_types.clone();

        // Clone for later use (to avoid borrow issues)
        let known_left_clone = known_left_label.clone();
        let known_right_clone = known_right_label.clone();

        log::debug!(
            "🔍 TypeInference: Constraints - left='{}' ({:?}), edge={:?}, right='{}' ({:?})",
            left_connection,
            known_left_label,
            known_edge_types,
            right_connection,
            known_right_label
        );

        // STEP 2: Query schema with all constraints
        let rel_schemas = graph_schema.get_relationships_schemas();

        // Find all relationships that match ALL known constraints
        // IMPORTANT: Iterate ONLY composite keys (those with "::") to avoid duplicates
        // Schema stores both "FOLLOWS" and "FOLLOWS::User::User" for backward compatibility,
        // but we only want to process each unique (type, from_node, to_node) once.
        let matches: Vec<(
            String,
            &crate::graph_catalog::graph_schema::RelationshipSchema,
        )> = rel_schemas
            .iter()
            .filter(|(full_key, _)| {
                // Only process composite keys - they have complete type information
                full_key.contains("::")
            })
            .filter_map(|(full_key, rel_schema)| {
                // Extract base type for matching
                let base_type = full_key.split("::").next().unwrap_or(full_key);
                let type_key = base_type;

                // Check edge type constraint (if known)
                if let Some(ref types) = known_edge_types {
                    // Extract base type name for comparison
                    let base_type = type_key.split("::").next().unwrap_or(type_key);
                    if !types.iter().any(|t| t == base_type || t == type_key) {
                        return None;
                    }
                }

                // Check left node (from_node) constraint (if known)
                if let Some(ref label) = known_left_label {
                    if !self.node_matches_schema(
                        label,
                        &rel_schema.from_node,
                        &rel_schema.from_label_values,
                    ) {
                        return None;
                    }
                }

                // Check right node (to_node) constraint (if known)
                if let Some(ref label) = known_right_label {
                    if !self.node_matches_schema(
                        label,
                        &rel_schema.to_node,
                        &rel_schema.to_label_values,
                    ) {
                        return None;
                    }
                }

                Some((base_type.to_string(), rel_schema))
            })
            .collect();

        log::debug!(
            "🔍 TypeInference: Found {} matching relationship(s) in schema",
            matches.len()
        );

        // STEP 3: Handle results
        if matches.is_empty() {
            // No matches - nothing to infer
            // But if edge type was specified, this is an error
            if known_edge_types.is_some() {
                log::warn!(
                    "🔍 TypeInference: Edge type {:?} specified but no matching schema found for {:?} -> {:?}",
                    known_edge_types, known_left_label, known_right_label
                );
            }
            return Ok((known_edge_types, known_left_label, known_right_label));
        }

        // Check if too many edge types
        let unique_edge_types: Vec<String> = matches
            .iter()
            .map(|(key, _)| key.split("::").next().unwrap_or(key).to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        if unique_edge_types.len() > plan_ctx.max_inferred_types && known_edge_types.is_none() {
            return Err(AnalyzerError::InvalidPlan(format!(
                "Too many matching relationship types ({}) for {:?}->{:?}. Max allowed is {}. Please specify explicit relationship type(s).",
                unique_edge_types.len(), known_left_label, known_right_label, plan_ctx.max_inferred_types
            )));
        }

        // STEP 4: Infer missing values from matches
        let inferred_edge_types = if known_edge_types.is_some() {
            known_edge_types
        } else if unique_edge_types.len() == 1 {
            log::info!(
                "🔍 TypeInference: Inferred edge type '{}'",
                unique_edge_types[0]
            );
            Some(unique_edge_types)
        } else if !unique_edge_types.is_empty() {
            log::info!(
                "🔍 TypeInference: Multiple edge types {:?}, will use UNION",
                unique_edge_types
            );
            Some(unique_edge_types)
        } else {
            None
        };

        // Infer left node label (all matches should agree on from_node for single edge type)
        // NOTE: $any is a polymorphic sentinel, not a real label — treat as ambiguous
        let inferred_left_label = if known_left_label.is_some() {
            known_left_label
        } else if matches.len() == 1 {
            let label = matches[0].1.from_node.clone();
            if label == "$any" {
                None // Polymorphic: node type resolved at runtime
            } else {
                self.update_node_label_in_ctx(
                    left_connection,
                    &label,
                    "from",
                    &matches[0].0,
                    plan_ctx,
                );
                Some(label)
            }
        } else {
            // Multiple matches - check if they all have same from_node
            let from_nodes: std::collections::HashSet<_> =
                matches.iter().map(|(_, s)| &s.from_node).collect();
            if from_nodes.len() == 1 && matches[0].1.from_node != "$any" {
                let label = matches[0].1.from_node.clone();
                self.update_node_label_in_ctx(
                    left_connection,
                    &label,
                    "from",
                    "multiple edges",
                    plan_ctx,
                );
                Some(label)
            } else {
                log::info!(
                    "🎯 TypeInference: Ambiguous from_node for '{}' → {} candidates: {:?}",
                    left_connection,
                    from_nodes.len(),
                    from_nodes
                );
                None // Ambiguous, can't infer single type
            }
        };

        // Infer right node label
        let inferred_right_label = if known_right_label.is_some() {
            known_right_label
        } else if matches.len() == 1 {
            let label = matches[0].1.to_node.clone();
            if label == "$any" {
                None // Polymorphic: node type resolved at runtime
            } else {
                self.update_node_label_in_ctx(
                    right_connection,
                    &label,
                    "to",
                    &matches[0].0,
                    plan_ctx,
                );
                Some(label)
            }
        } else {
            // Multiple matches - check if they all have same to_node
            let to_nodes: std::collections::HashSet<_> =
                matches.iter().map(|(_, s)| &s.to_node).collect();
            if to_nodes.len() == 1 && matches[0].1.to_node != "$any" {
                let label = matches[0].1.to_node.clone();
                self.update_node_label_in_ctx(
                    right_connection,
                    &label,
                    "to",
                    "multiple edges",
                    plan_ctx,
                );
                Some(label)
            } else {
                log::info!(
                    "🎯 TypeInference: Ambiguous to_node for '{}' → {} candidates: {:?}",
                    right_connection,
                    to_nodes.len(),
                    to_nodes
                );
                None // Ambiguous, can't infer single type
            }
        };

        // ⭐ NEW: Generate and store pattern combinations if we have ambiguous nodes
        if (inferred_left_label.is_none() && known_left_clone.is_none())
            || (inferred_right_label.is_none() && known_right_clone.is_none())
        {
            // Collect all possible combinations from matches
            use crate::query_planner::plan_ctx::TypeCombination;
            let mut combinations = Vec::new();

            for (rel_type_key, rel_schema) in &matches {
                // rel_type_key is already a base type from earlier extraction

                combinations.push(TypeCombination {
                    from_label: rel_schema.from_node.clone(),
                    rel_type: rel_type_key.clone(),
                    to_label: rel_schema.to_node.clone(),
                });

                // Apply 38 combination limit
                if combinations.len() >= 38 {
                    log::warn!(
                        "⚠️ Pattern combinations limited to 38 for '{}' -> '{}' (found {} total matches)",
                        left_connection,
                        right_connection,
                        matches.len()
                    );
                    break;
                }
            }

            log::debug!(
                "TypeInference: Before dedup - {} combinations for '{}' -> '{}'",
                combinations.len(),
                left_connection,
                right_connection
            );

            // Debug: print all combinations before dedup
            for (i, combo) in combinations.iter().enumerate() {
                log::debug!(
                    "  Combo {}: {} -[{}]-> {}",
                    i,
                    combo.from_label,
                    combo.rel_type,
                    combo.to_label
                );
            }

            // Deduplicate combinations by (from_label, rel_type, to_label)
            // This prevents duplicates when schema has both simple and composite keys
            if !combinations.is_empty() {
                let mut seen = std::collections::HashSet::new();
                combinations.retain(|combo| {
                    let key = (
                        combo.from_label.clone(),
                        combo.rel_type.clone(),
                        combo.to_label.clone(),
                    );
                    seen.insert(key)
                });

                log::info!(
                    "🎯 TypeInference: Generated {} unique pattern combinations for '{}' -> '{}'",
                    combinations.len(),
                    left_connection,
                    right_connection
                );
                plan_ctx.store_pattern_combinations(
                    left_connection,
                    right_connection,
                    combinations.clone(),
                );

                // Set first combination types in TableCtx for backward compatibility
                if let Some(first_combo) = combinations.first() {
                    if inferred_left_label.is_none() && known_left_clone.is_none() {
                        self.update_node_label_in_ctx(
                            left_connection,
                            &first_combo.from_label,
                            "from",
                            "multi-type pattern",
                            plan_ctx,
                        );
                    }
                    if inferred_right_label.is_none() && known_right_clone.is_none() {
                        self.update_node_label_in_ctx(
                            right_connection,
                            &first_combo.to_label,
                            "to",
                            "multi-type pattern",
                            plan_ctx,
                        );
                    }
                }
            }
        }

        log::info!(
            "🔍 TypeInference: Result - '{}' ({:?}) -[{:?}]-> '{}' ({:?})",
            left_connection,
            inferred_left_label,
            inferred_edge_types.as_ref().map(|v| v.join("|")),
            right_connection,
            inferred_right_label
        );

        Ok((
            inferred_edge_types,
            inferred_left_label,
            inferred_right_label,
        ))
    }

    /// Check if a query node label matches a schema node definition
    /// Supports: direct match, $any wildcard, polymorphic label_values
    fn node_matches_schema(
        &self,
        query_label: &str,
        schema_node: &str,
        label_values: &Option<Vec<String>>,
    ) -> bool {
        // Direct match
        if query_label == schema_node {
            return true;
        }
        // $any wildcard matches everything
        if schema_node == "$any" {
            return true;
        }
        // Polymorphic label_values
        if let Some(values) = label_values {
            if values.iter().any(|v| v == query_label) {
                return true;
            }
        }
        false
    }

    /// Update or create TableCtx with inferred label.
    /// Never stores `$any` — it's a polymorphic sentinel, not a concrete label.
    fn update_node_label_in_ctx(
        &self,
        node_alias: &str,
        label: &str,
        side: &str,
        edge_info: &str,
        plan_ctx: &mut PlanCtx,
    ) {
        if label == "$any" {
            log::debug!(
                "🏷️ TypeInference: Skipping '$any' label for '{}' (polymorphic sentinel)",
                node_alias
            );
            return;
        }
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(node_alias) {
            table_ctx.set_labels(Some(vec![label.to_string()]));
            log::info!(
                "🏷️ TypeInference: UPDATED '{}' = '{}' (from {} side of {})",
                node_alias,
                label,
                side,
                edge_info
            );
        } else {
            use crate::query_planner::plan_ctx::TableCtx;
            plan_ctx.insert_table_ctx(
                node_alias.to_string(),
                TableCtx::build(
                    node_alias.to_string(),
                    Some(vec![label.to_string()]),
                    vec![],
                    false,
                    false,
                ),
            );
            log::info!(
                "🏷️ TypeInference: CREATED '{}' = '{}' (from {} side of {})",
                node_alias,
                label,
                side,
                edge_info
            );
        }
    }
    /// Infer the label/type of elements being unwound from an UNWIND expression.
    ///
    /// Strategy:
    /// 1. If UNWIND input is a WithClause, check its projection items
    /// 2. Find the projection item that matches the UNWIND expression alias
    /// 3. If it's collect(node_alias), extract the node_alias label from plan_ctx
    /// 4. Fall back to checking plan_ctx projection aliases (if registered by earlier passes)
    ///
    /// Examples:
    /// - `WITH collect(u) AS users UNWIND users AS user` → input is WithClause, find "users" item → collect(u) → u:Person
    /// - `UNWIND [1,2,3] AS num` → No label (scalar)
    fn infer_unwind_element_label_from_plan(
        &self,
        expression: &LogicalExpr,
        input_plan: &Arc<LogicalPlan>,
        plan_ctx: &PlanCtx,
    ) -> Option<String> {
        // Extract alias name from expression
        let alias_name = match expression {
            LogicalExpr::TableAlias(table_alias) => &table_alias.0,
            _ => {
                log::debug!("🔍 TypeInference::infer_unwind_element_label_from_plan: Expression type not supported");
                return None;
            }
        };

        // STRATEGY 1: Look at input plan directly (works before FilterTagging registers aliases)
        if let LogicalPlan::WithClause(with_clause) = input_plan.as_ref() {
            log::debug!(
                "🔍 TypeInference::infer_unwind_element_label_from_plan: UNWIND input is WithClause, checking items for '{}'",
                alias_name
            );

            // Find the projection item matching this alias
            for item in &with_clause.items {
                if let Some(col_alias) = &item.col_alias {
                    if col_alias.0 == *alias_name {
                        log::debug!(
                            "🔍 TypeInference::infer_unwind_element_label_from_plan: Found matching item: {} -> {:?}",
                            alias_name,
                            item.expression
                        );

                        // Check if it's collect()
                        if let LogicalExpr::AggregateFnCall(agg_fn) = &item.expression {
                            if agg_fn.name.eq_ignore_ascii_case("collect")
                                && !agg_fn.args.is_empty()
                            {
                                // Extract the first argument (the node being collected)
                                if let LogicalExpr::TableAlias(collected_alias) = &agg_fn.args[0] {
                                    // Look up the label of the collected node
                                    if let Ok(table_ctx) =
                                        plan_ctx.get_table_ctx(&collected_alias.0)
                                    {
                                        if let Some(labels) = table_ctx.get_labels() {
                                            if let Some(first_label) = labels.first() {
                                                log::info!(
                                                    "🔍 TypeInference::infer_unwind_element_label_from_plan: '{}' → collect('{}') → label '{}'",
                                                    alias_name,
                                                    collected_alias.0,
                                                    first_label
                                                );
                                                return Some(first_label.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // STRATEGY 2: Check if it's already registered as projection alias (from earlier passes)
        if let Some(alias_expr) = plan_ctx.get_projection_alias_expr(alias_name) {
            log::debug!(
                "🔍 TypeInference::infer_unwind_element_label_from_plan: '{}' found in projection aliases: {:?}",
                alias_name,
                alias_expr
            );

            // Check if it's collect()
            if let LogicalExpr::AggregateFnCall(agg_fn) = alias_expr {
                if agg_fn.name.eq_ignore_ascii_case("collect") && !agg_fn.args.is_empty() {
                    if let LogicalExpr::TableAlias(collected_alias) = &agg_fn.args[0] {
                        if let Ok(table_ctx) = plan_ctx.get_table_ctx(&collected_alias.0) {
                            if let Some(labels) = table_ctx.get_labels() {
                                if let Some(first_label) = labels.first() {
                                    log::info!(
                                        "🔍 TypeInference::infer_unwind_element_label_from_plan: '{}' → collect('{}') → label '{}' (from projection aliases)",
                                        alias_name,
                                        collected_alias.0,
                                        first_label
                                    );
                                    return Some(first_label.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        // STRATEGY 3: Try direct table context lookup
        if let Ok(table_ctx) = plan_ctx.get_table_ctx(alias_name) {
            if let Some(labels) = table_ctx.get_labels() {
                if let Some(first_label) = labels.first() {
                    log::info!(
                        "🔍 TypeInference::infer_unwind_element_label_from_plan: '{}' → label '{}' (direct lookup)",
                        alias_name,
                        first_label
                    );
                    return Some(first_label.clone());
                }
            }
        }

        log::debug!(
            "🔍 TypeInference::infer_unwind_element_label_from_plan: No label found for '{}'",
            alias_name
        );
        None
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
        log::info!(
            "🏷️ TypeInference: Completed - plan transformed: {}",
            result.is_yes()
        );
        Ok(result)
    }
}
