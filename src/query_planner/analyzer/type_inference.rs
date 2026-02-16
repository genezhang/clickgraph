//! Unified Type Inference Analyzer Pass
//!
//! **Purpose**: Comprehensive type inference system that:
//! 1. Infers missing node labels and relationship types from graph schema
//! 2. Extracts label constraints from WHERE clause id() filters
//! 3. Generates UNION branches for multiple valid type combinations
//! 4. **Validates combinations against schema + direction** ← CRITICAL!
//!
//! This unified pass replaces and merges:
//! - Old TypeInference (incremental, incomplete)
//! - PatternResolver (systematic UNION generation)
//! - Parts of union_pruning (WHERE constraint extraction)
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
//! **Unified TypeInference Algorithm**:
//!
//! **Step 1: Collect ALL Constraints**
//! For each variable in the pattern, gather:
//! - Explicit labels from pattern: `(a:User)`
//! - WHERE id() constraints: `WHERE id(a) IN [...]` → extract labels via ID decoding
//! - Schema relationship constraints: connected edges constrain node types
//! - Direction constraints: pattern direction MUST match schema direction
//!
//! **Step 2: Compute Possible Types**
//! For each variable:
//! - Start with all possible types (all node labels in schema)
//! - Filter by explicit label (if any)
//! - Filter by WHERE id() constraints (if any)
//! - Filter by schema relationships (considering direction!)
//!
//! **Step 3: Generate Valid Combinations**
//! - Cartesian product of all possible types
//! - **Filter by schema validity + direction** ← Prevents invalid branches
//! - For directed patterns (->), validate: schema.has_relationship(from, type, to, direction)
//! - For undirected patterns (--), defer to BidirectionalUnion pass
//!
//! **Step 4: Create UNION Structure**
//! ```rust
//! if valid_combinations.len() == 1 {
//!     // Single valid combination, no Union needed
//!     return single_branch_with_labels(valid_combinations[0])
//! } else {
//!     // Multiple valid combinations, create Union
//!     return LogicalPlan::Union {
//!         inputs: valid_combinations.map(|combo| create_branch_with_labels(combo)),
//!         union_type: UnionType::All
//!     }
//! }
//! ```
//!
//! **Direction Validation** (CRITICAL):
//!
//! For directed patterns (`->`), only schema-valid directions allowed:
//! ```cypher
//! // Schema: AUTHORED from User to Post
//! ✅ Valid:   (User)-[AUTHORED]->(Post)
//! ❌ Invalid: (Post)-[AUTHORED]->(User)  ← MUST BE FILTERED OUT
//! ```
//!
//! **Key Functions**:
//! - `extract_labels_from_where()` - Extract labels from WHERE id() filters
//! - `compute_possible_types()` - Compute valid types for each variable
//! - `is_valid_combination_with_direction()` - Validate against schema + direction
//! - `generate_union_branches()` - Create Union with valid branches
//!
//! **Examples**:
//! ```cypher
//! // Infer node labels from edge type
//! MATCH (a)-[:KNOWS]->(b)           → a:Person, b:Person
//!
//! // WHERE constraint + direction validation
//! MATCH (a)-[r]->(b) WHERE id(a) IN [Post.1, User.2] AND id(b) IN [...]
//! → Generates UNION with ONLY valid branches:
//!    (User)-[AUTHORED|LIKED]->(Post)  ✓
//!    (User)-[FOLLOWS]->(User)          ✓
//!    [NOT: (Post)-[*]->(User) ❌ Invalid direction!]
//!
//! // Infer edge type from node labels  
//! MATCH (a:Person)-[r]->(b:City)    → r:LIVES_IN
//!
//! // Multiple valid combinations → UNION
//! MATCH (a)-[r]->(b) WHERE id(a) IN [User.1, User.2]
//! → UNION of: (User)-[FOLLOWS]->(User), (User)-[AUTHORED]->(Post), (User)-[LIKED]->(Post)
//! ```
//!
//! **When to run**: Early in analyzer pipeline (position 2, after SchemaInference)
//! This ensures all downstream passes have complete type information.

use std::sync::Arc;
use std::collections::{HashMap, HashSet};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    utils::id_encoding::IdEncoding,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::AnalyzerError,
            pattern_resolver_config::get_max_combinations,
        },
        logical_expr::{LogicalExpr, Direction, Literal},
        logical_plan::{GraphNode, GraphRel, LogicalPlan, ViewScan, Union, UnionType},
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
            // CRITICAL: Check for Filter wrapping pattern - extract WHERE constraints for UNION generation
            LogicalPlan::Filter(filter) => {
                // Extract label constraints from WHERE clause
                let where_constraints = extract_labels_from_where(&Some(filter.predicate.clone()));
                
                if !where_constraints.is_empty() {
                    log::info!(
                        "🔍 UnifiedTypeInference: Extracted WHERE constraints: {:?}",
                        where_constraints
                    );
                    
                    // Check if input is GraphRel - if so, try UNION generation
                    if let LogicalPlan::GraphRel(_) = filter.input.as_ref() {
                        // Try to generate UNION with WHERE constraints
                        let result = self.try_generate_union_with_constraints(
                            filter.input.clone(),
                            &where_constraints,
                            plan_ctx,
                            graph_schema,
                        )?;
                        
                        if result.is_yes() {
                            // Union generated - wrap it in Filter
                            let new_filter = crate::query_planner::logical_plan::Filter {
                                input: result.get_plan().clone(),
                                predicate: filter.predicate.clone(),
                            };
                            return Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))));
                        }
                    }
                }
                
                // Default: process input recursively
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
                                // Expand $any to all concrete node labels
                                if rel_schema.to_node == "$any" {
                                    for label in graph_schema.all_node_schemas().keys() {
                                        to_node_labels.insert(label.clone());
                                    }
                                } else {
                                    to_node_labels.insert(rel_schema.to_node.clone());
                                }

                                // For bi-directional patterns, also include from_node (reverse direction)
                                if is_bidirectional {
                                    if rel_schema.from_node == "$any" {
                                        for label in graph_schema.all_node_schemas().keys() {
                                            to_node_labels.insert(label.clone());
                                        }
                                    } else {
                                        to_node_labels.insert(rel_schema.from_node.clone());
                                    }
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
                        // Also update VariableRegistry so Bolt metadata picks up inferred labels
                        plan_ctx.update_node_labels(&rel.right_connection, inferred_labels.clone());

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
                            } else if let Some(label) = labels_vec.first() {
                                // Single label - existing logic
                                log::info!("🏷️ TypeInference: Creating ViewScan for GraphNode '{}' with inferred label '{}'", node.alias, label);

                                // Update VariableRegistry so Bolt metadata picks up inferred labels
                                plan_ctx.update_node_labels(&node.alias, labels_vec.clone());

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

    /// Try to generate UNION branches when WHERE constraints indicate multiple possible types
    ///
    /// This is the core of unified type inference with direction validation.
    ///
    /// # Arguments
    /// * `graph_rel_plan` - The GraphRel plan node
    /// * `where_constraints` - Label constraints extracted from WHERE id() filters
    /// * `plan_ctx` - Planning context
    /// * `graph_schema` - Graph schema
    ///
    /// # Returns
    /// * `Transformed::Yes(Union)` if multiple valid branches generated
    /// * `Transformed::No(plan)` if single branch or generation not applicable
    fn try_generate_union_with_constraints(
        &self,
        graph_rel_plan: Arc<LogicalPlan>,
        where_constraints: &HashMap<String, HashSet<String>>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let rel = match graph_rel_plan.as_ref() {
            LogicalPlan::GraphRel(r) => r,
            _ => return Ok(Transformed::No(graph_rel_plan)),
        };

        // Skip UNION generation for variable-length paths (handled by GraphTraversalPlanning)
        if rel.variable_length.is_some() {
            log::debug!(
                "🔍 UnifiedTypeInference: Skipping UNION generation for VLP pattern '{}'",
                rel.alias
            );
            return self.infer_labels_recursive(graph_rel_plan, plan_ctx, graph_schema);
        }
        
        // NOTE: We DO handle Direction::Either (undirected patterns) here!
        // check_relationship_exists_with_direction() validates bidirectionally for Either
        // This is critical for Neo4j Browser expand: (Post)--(User) should return ONLY
        // valid directions from schema (User→Post), not invalid (Post→User)

        // Get possible types for left and right nodes
        let left_types = self.get_possible_types_for_variable(
            &rel.left_connection,
            where_constraints,
            plan_ctx,
            graph_schema,
        )?;

        let right_types = self.get_possible_types_for_variable(
            &rel.right_connection,
            where_constraints,
            plan_ctx,
            graph_schema,
        )?;

        // Get possible relationship types
        let rel_types = if let Some(labels) = &rel.labels {
            labels.clone()
        } else {
            // No relationship type specified - collect all types from schema
            graph_schema
                .get_relationships_schemas()
                .keys()
                .map(|k| {
                    // Handle composite keys TYPE::FROM::TO
                    if k.contains("::") {
                        k.split("::").next().unwrap_or(k).to_string()
                    } else {
                        k.clone()
                    }
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };

        log::info!(
            "🔍 UnifiedTypeInference: Checking combinations for pattern '{}': left_types={:?}, right_types={:?}, rel_types={:?}, direction={:?}",
            rel.alias,
            left_types,
            right_types,
            rel_types,
            rel.direction
        );

        // For undirected patterns (Direction::Either), check if all valid combinations
        // go in the same direction. If so, convert to that direction to prevent
        // BidirectionalUnion from creating invalid duplicate branches.
        let optimized_direction = if matches!(rel.direction, Direction::Either) {
            self.optimize_undirected_pattern(
                &left_types,
                &right_types,
                &rel_types,
                graph_schema,
            )
        } else {
            rel.direction.clone()
        };

        if optimized_direction != rel.direction {
            log::info!(
                "🎯 UnifiedTypeInference: Optimized undirected pattern: {:?} → {:?} (only one direction valid in schema)",
                rel.direction,
                optimized_direction
            );
        }

        // Generate all combinations and validate with optimized direction
        let mut valid_combinations = Vec::new();

        for left_type in &left_types {
            for right_type in &right_types {
                for rel_type in &rel_types {
                    // CRITICAL: Validate direction matches schema
                    if check_relationship_exists_with_direction(
                        left_type,
                        right_type,
                        rel_type,
                        optimized_direction.clone(),
                        graph_schema,
                    ) {
                        valid_combinations.push((
                            left_type.clone(),
                            rel_type.clone(),
                            right_type.clone(),
                        ));
                    } else {
                        log::debug!(
                            "🚫 UnifiedTypeInference: Invalid combination filtered: ({left_type})-[{rel_type}:{:?}]->({right_type})",
                            optimized_direction
                        );
                    }
                }
            }
        }

        log::info!(
            "🔍 UnifiedTypeInference: Found {} valid combinations after direction validation",
            valid_combinations.len()
        );

        // Check max combinations limit
        let max_combinations = get_max_combinations();
        if valid_combinations.len() > max_combinations {
            log::warn!(
                "⚠️ UnifiedTypeInference: Too many combinations ({} > {}), limiting to first {}",
                valid_combinations.len(),
                max_combinations,
                max_combinations
            );
            valid_combinations.truncate(max_combinations);
        }

        if valid_combinations.is_empty() {
            log::warn!(
                "⚠️ UnifiedTypeInference: No valid combinations found for pattern '{}' - query may return empty results",
                rel.alias
            );
            // Return original plan - query will likely return empty but that's semantically correct
            return self.infer_labels_recursive(graph_rel_plan, plan_ctx, graph_schema);
        }

        if valid_combinations.len() == 1 {
            // Single valid combination - no UNION needed
            let (left_type, rel_type, right_type) = &valid_combinations[0];
            log::info!(
                "✅ UnifiedTypeInference: Single valid combination, no UNION needed: ({:?})-[{:?}:{:?}]->({:?})",
                left_type,
                rel_type,
                optimized_direction,
                right_type
            );
            
            // If direction was optimized, update the GraphRel plan
            if optimized_direction != rel.direction {
                let mut updated_rel = rel.clone();
                updated_rel.direction = optimized_direction;
                let updated_plan = Arc::new(LogicalPlan::GraphRel(updated_rel));
                return self.infer_labels_recursive(updated_plan, plan_ctx, graph_schema);
            }
            
            return self.infer_labels_recursive(graph_rel_plan, plan_ctx, graph_schema);
        }

        // Multiple valid combinations - generate UNION
        log::info!(
            "🔀 UnifiedTypeInference: Generating UNION with {} branches (direction: {:?})",
            valid_combinations.len(),
            optimized_direction
        );

        let mut union_branches = Vec::new();

        for (left_type, rel_type, right_type) in valid_combinations {
            // Create a branch with specific types
            let mut branch = self.create_typed_branch(
                rel,
                &left_type,
                &vec![rel_type],
                &right_type,
                plan_ctx,
                graph_schema,
            )?;
            
            // Update direction if optimized
            if optimized_direction != rel.direction {
                if let LogicalPlan::GraphRel(graph_rel) = Arc::make_mut(&mut branch) {
                    graph_rel.direction = optimized_direction.clone();
                }
            }
            
            union_branches.push(branch);
        }

        // Create Union node
        let union_plan = crate::query_planner::logical_plan::Union {
            inputs: union_branches,
            union_type: crate::query_planner::logical_plan::UnionType::All,
        };

        Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(union_plan))))
    }

    /// Get possible types for a variable considering explicit labels and WHERE constraints
    fn get_possible_types_for_variable(
        &self,
        var_name: &str,
        where_constraints: &HashMap<String, HashSet<String>>,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Vec<String>> {
        // First check: explicit label in plan_ctx
        if let Ok(table_ctx) = plan_ctx.get_table_ctx(var_name) {
            if let Some(labels) = table_ctx.get_labels() {
                if !labels.is_empty() {
                    log::debug!(
                        "🏷️ Variable '{}' has explicit label from plan_ctx: {:?}",
                        var_name,
                        labels
                    );
                    return Ok(labels.clone());
                }
            }
        }

        // Second check: WHERE constraints
        if let Some(constraint_labels) = where_constraints.get(var_name) {
            log::debug!(
                "🏷️ Variable '{}' has WHERE constraint labels: {:?}",
                var_name,
                constraint_labels
            );
            return Ok(constraint_labels.iter().cloned().collect());
        }

        // No constraints - return all possible node types
        let all_types: Vec<String> = graph_schema
            .all_node_schemas()
            .keys()
            .cloned()
            .collect();

        log::debug!(
            "🏷️ Variable '{}' has no constraints, using all node types: {:?}",
            var_name,
            all_types
        );

        Ok(all_types)
    }

    /// Optimize undirected patterns by checking if all valid combinations go in same direction
    /// 
    /// Strategy: Check ALL possible type combinations. If they all go in ONE direction,
    /// convert Direction::Either to that direction. Only keep Either if we have actual
    /// bidirectional relationships (some go forward, some go backward).
    /// 
    /// Example: `(Post)--(User)` where schema only has User→Post
    /// → Convert to Direction::Incoming (Post←User)
    fn optimize_undirected_pattern(
        &self,
        left_types: &[String],
        right_types: &[String],
        rel_types: &[String],
        graph_schema: &GraphSchema,
    ) -> Direction {
        let mut has_forward = false;  // Any combination where left→right exists in schema
        let mut has_backward = false; // Any combination where right→left exists in schema

        for left_type in left_types {
            for right_type in right_types {
                for rel_type in rel_types {
                    if let Some(rel_schema) = graph_schema.get_relationships_schema_opt(rel_type) {
                        // Check forward direction: left→right matches schema from→to
                        if node_type_matches(&rel_schema.from_node, left_type)
                            && node_type_matches(&rel_schema.to_node, right_type)
                        {
                            has_forward = true;
                        }
                        // Check backward direction: right→left matches schema from→to
                        if node_type_matches(&rel_schema.from_node, right_type)
                            && node_type_matches(&rel_schema.to_node, left_type)
                        {
                            has_backward = true;
                        }
                    }
                }
            }
        }

        // Convert to unidirectional only if ALL valid combinations go the same way
        match (has_forward, has_backward) {
            (true, false) => {
                log::info!("🎯 All combinations go forward only (left→right)");
                Direction::Outgoing
            }
            (false, true) => {
                log::info!("🎯 All combinations go backward only (right→left)");
                Direction::Incoming
            }
            (true, true) => {
                log::debug!("↔️ Combinations go both directions, keeping Either (truly bidirectional)");
                Direction::Either
            }
            (false, false) => {
                log::warn!("⚠️ No valid directions found!");
                Direction::Either // Keep original, query will return empty
            }
        }
    }

    /// Create a typed branch for UNION with specific node and relationship types
    fn create_typed_branch(
        &self,
        rel: &GraphRel,
        left_type: &str,
        rel_types: &[String],
        right_type: &str,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Arc<LogicalPlan>> {
        // Update plan_ctx with inferred types for this branch
        self.update_plan_ctx_with_label(&rel.left_connection, left_type, plan_ctx)?;
        self.update_plan_ctx_with_label(&rel.right_connection, right_type, plan_ctx)?;

        // Create GraphRel with specific types
        let typed_rel = GraphRel {
            left: rel.left.clone(),
            center: rel.center.clone(),
            right: rel.right.clone(),
            alias: rel.alias.clone(),
            direction: rel.direction.clone(),
            left_connection: rel.left_connection.clone(),
            right_connection: rel.right_connection.clone(),
            is_rel_anchor: rel.is_rel_anchor,
            variable_length: rel.variable_length.clone(),
            shortest_path_mode: rel.shortest_path_mode.clone(),
            path_variable: rel.path_variable.clone(),
            where_predicate: rel.where_predicate.clone(),
            labels: Some(rel_types.to_vec()),
            is_optional: rel.is_optional,
            anchor_connection: rel.anchor_connection.clone(),
            cte_references: rel.cte_references.clone(),
            pattern_combinations: None,
            was_undirected: rel.was_undirected,
        };

        Ok(Arc::new(LogicalPlan::GraphRel(typed_rel)))
    }

    /// Update plan_ctx with inferred label for a variable
    fn update_plan_ctx_with_label(
        &self,
        var_name: &str,
        label: &str,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<()> {
        if let Ok(mut table_ctx) = plan_ctx.get_table_ctx(var_name).cloned() {
            // Update existing table_ctx with label
            table_ctx.set_labels(Some(vec![label.to_string()]));
            plan_ctx.insert_table_ctx(var_name.to_string(), table_ctx);
        }
        Ok(())
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
        // Find matching relationships using the rel_type_index to avoid duplicates.
        // Standard schemas have composite keys (e.g., "FOLLOWS::User::User"),
        // polymorphic schemas have simple keys (e.g., "FOLLOWS").
        // The rel_type_index maps base_type → [keys], giving us all unique entries.
        let matches: Vec<(
            String,
            &crate::graph_catalog::graph_schema::RelationshipSchema,
        )> = rel_schemas
            .iter()
            .filter(|(full_key, _)| {
                // Use composite keys when they exist; fall back to simple keys for polymorphic
                if full_key.contains("::") {
                    true
                } else {
                    // Include simple key only if no composite key exists for this type
                    !rel_schemas.keys().any(|k| {
                        k.contains("::") && k.split("::").next() == Some(full_key.as_str())
                    })
                }
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

            let max_combos = get_max_combinations();

            for (rel_type_key, rel_schema) in &matches {
                let from_labels = graph_schema.expand_node_type(&rel_schema.from_node);
                let to_labels = graph_schema.expand_node_type(&rel_schema.to_node);

                for from_label in &from_labels {
                    for to_label in &to_labels {
                        combinations.push(TypeCombination {
                            from_label: from_label.clone(),
                            rel_type: rel_type_key.clone(),
                            to_label: to_label.clone(),
                        });

                        if combinations.len() >= max_combos {
                            break;
                        }
                    }
                    if combinations.len() >= max_combos {
                        break;
                    }
                }

                if combinations.len() >= max_combos {
                    log::warn!(
                        "⚠️ Pattern combinations limited to {} for '{}' -> '{}' (found {} total matches). \
                         Set CLICKGRAPH_MAX_TYPE_COMBINATIONS to increase.",
                        max_combos,
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
        // Also update VariableRegistry so Bolt metadata picks up inferred labels
        plan_ctx.update_node_labels(node_alias, vec![label.to_string()]);
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
        log::info!("🏷️ UnifiedTypeInference: Starting type inference pass");
        let result = self.infer_labels_recursive(logical_plan, plan_ctx, graph_schema)?;
        log::info!(
            "🏷️ UnifiedTypeInference: Completed - plan transformed: {}",
            result.is_yes()
        );
        Ok(result)
    }
}

// ================================================================================================
// WHERE Clause Label Extraction
// ================================================================================================

/// Extract node labels from WHERE clause containing `id(var) IN [...]` or `id(var) = X` patterns
///
/// This function decodes ClickGraph's bit-pattern encoded IDs to extract label information,
/// enabling type inference from browser queries like:
/// `MATCH (a)-[r]->(b) WHERE id(a) IN [281474976710657, ...]`
///
/// # Arguments
/// * `where_expr` - The WHERE clause logical expression
///
/// # Returns
/// * Map of variable names to their possible label sets
///
/// # Example
/// ```ignore
/// // WHERE id(a) IN [281474976710657, 281474976710658]  (both User IDs)
/// // Returns: {"a": {"User"}}
///
/// // WHERE id(a) = 281474976710657 AND id(b) = 844424930131969
/// // Returns: {"a": {"User"}, "b": {"Post"}}
/// ```
fn extract_labels_from_where(
    where_expr: &Option<LogicalExpr>,
) -> HashMap<String, HashSet<String>> {
    let mut label_constraints = HashMap::new();
    if let Some(expr) = where_expr {
        extract_labels_from_logical_expr(expr, &mut label_constraints, false);
    }
    label_constraints
}

/// Recursively traverse logical WHERE expression to find `id(var) IN [...]` or `id(var) = X` patterns
fn extract_labels_from_logical_expr(
    expr: &LogicalExpr,
    constraints: &mut HashMap<String, HashSet<String>>,
    negated: bool,
) {
    match expr {
        LogicalExpr::Operator(op_app) => {
            use crate::query_planner::logical_expr::Operator;
            match op_app.operator {
                // NOT operator - flip negation flag and recurse
                Operator::Not => {
                    for operand in &op_app.operands {
                        extract_labels_from_logical_expr(operand, constraints, !negated);
                    }
                }
                Operator::In => {
                    // Skip extraction if we're inside a NOT (e.g., NOT id(a) IN [...])
                    if negated {
                        return;
                    }

                    // Check if first operand is ScalarFnCall("id", [Variable])
                    if let Some(LogicalExpr::ScalarFnCall(func)) = op_app.operands.first() {
                        if func.name == "id" && func.args.len() == 1 {
                            if let LogicalExpr::Column(col) = &func.args[0] {
                                let var_name = &col.0;
                                // Extract IDs from second operand (list)
                                if let Some(LogicalExpr::List(id_list)) = op_app.operands.get(1) {
                                    for item in id_list {
                                        if let LogicalExpr::Literal(Literal::Integer(id_value)) = item {
                                            if let Some((label, _)) = IdEncoding::decode_with_label(*id_value) {
                                                constraints
                                                    .entry(var_name.clone())
                                                    .or_default()
                                                    .insert(label);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Operator::Equal => {
                    // Skip extraction if we're inside a NOT
                    if negated {
                        return;
                    }

                    // Handle: id(var) = X
                    if let Some(LogicalExpr::ScalarFnCall(func)) = op_app.operands.first() {
                        if func.name == "id" && func.args.len() == 1 {
                            if let LogicalExpr::Column(col) = &func.args[0] {
                                let var_name = &col.0;
                                // Extract ID from second operand
                                if let Some(LogicalExpr::Literal(Literal::Integer(id_value))) = op_app.operands.get(1) {
                                    if let Some((label, _)) = IdEncoding::decode_with_label(*id_value) {
                                        constraints
                                            .entry(var_name.clone())
                                            .or_default()
                                            .insert(label);
                                    }
                                }
                            }
                        }
                    }
                }
                // AND, OR - recurse into operands without flipping negation
                Operator::And | Operator::Or => {
                    for operand in &op_app.operands {
                        extract_labels_from_logical_expr(operand, constraints, negated);
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ================================================================================================
// Schema Validation with Direction
// ================================================================================================

/// Check if a specific relationship exists in schema with direction validation
///
/// # Arguments
/// * `from_type` - Source node type
/// * `to_type` - Target node type
/// * `rel_type` - Relationship type
/// * `direction` - Pattern direction (must match schema for directed patterns)
/// * `graph_schema` - Graph schema
///
/// # Returns
/// * `true` if relationship exists in schema with correct direction
fn check_relationship_exists_with_direction(
    from_type: &str,
    to_type: &str,
    rel_type: &str,
    direction: Direction,
    graph_schema: &GraphSchema,
) -> bool {
    match direction {
        Direction::Either => {
            // Undirected pattern - allow either direction
            check_relationship_exists_bidirectional(from_type, to_type, rel_type, graph_schema)
        }
        Direction::Outgoing | Direction::Incoming => {
            // Directed pattern - must match schema direction exactly
            if let Some(rel_schema) = graph_schema.get_relationships_schema_opt(rel_type) {
                node_type_matches(&rel_schema.from_node, from_type)
                    && node_type_matches(&rel_schema.to_node, to_type)
            } else {
                false
            }
        }
    }
}

/// Match node type considering `$any` as wildcard (polymorphic schemas)
fn node_type_matches(schema_node: &str, query_node: &str) -> bool {
    schema_node == "$any" || schema_node == query_node
}

/// Check if a specific relationship exists in either direction
fn check_relationship_exists_bidirectional(
    from_type: &str,
    to_type: &str,
    rel_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    // Iterate all relationships to find matching type with either direction
    graph_schema
        .get_relationships_schemas()
        .iter()
        .any(|(key, rel_schema)| {
            // Match by relationship type (handle composite keys TYPE::FROM::TO)
            let type_matches = if key.contains("::") {
                key.split("::").next() == Some(rel_type)
            } else {
                key == rel_type
            };
            type_matches
                && ((node_type_matches(&rel_schema.from_node, from_type)
                    && node_type_matches(&rel_schema.to_node, to_type))
                    || (node_type_matches(&rel_schema.from_node, to_type)
                        && node_type_matches(&rel_schema.to_node, from_type)))
        })
}

/// Check if ANY relationship exists between two node types with direction validation
fn check_any_relationship_exists_with_direction(
    from_type: &str,
    to_type: &str,
    direction: Direction,
    graph_schema: &GraphSchema,
) -> bool {
    match direction {
        Direction::Either => {
            // Undirected - allow either direction
            check_any_relationship_exists_bidirectional(from_type, to_type, graph_schema)
        }
        Direction::Outgoing | Direction::Incoming => {
            // Directed - must match schema direction
            graph_schema
                .get_relationships_schemas()
                .values()
                .any(|rel_schema| {
                    node_type_matches(&rel_schema.from_node, from_type)
                        && node_type_matches(&rel_schema.to_node, to_type)
                })
        }
    }
}

/// Check if ANY relationship exists between two node types in either direction
fn check_any_relationship_exists_bidirectional(
    from_type: &str,
    to_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    graph_schema
        .get_relationships_schemas()
        .values()
        .any(|rel_schema| {
            (node_type_matches(&rel_schema.from_node, from_type)
                && node_type_matches(&rel_schema.to_node, to_type))
                || (node_type_matches(&rel_schema.from_node, to_type)
                    && node_type_matches(&rel_schema.to_node, from_type))
        })
}
