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
//! ```ignore
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
        plan_ctx::{PlanCtx, TableCtx},  // Added TableCtx
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

    /// Generate UNION for untyped nodes with direction validation
    ///
    /// This is Phase 2 of unified type inference. After initial inference (Phase 1),
    /// we discover any remaining untyped nodes and generate UNION branches for all
    /// valid type combinations, applying direction validation.
    ///
    /// Key difference from old PatternResolver: We apply check_relationship_exists_with_direction()
    /// and optimize_undirected_pattern() to ensure only schema-valid branches are generated.
    fn generate_union_for_untyped_nodes(
        &self,
        plan: Arc<LogicalPlan>,
        untyped_nodes: &HashSet<String>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        relationships: &[RelationshipPattern],
        property_accesses: &HashMap<String, HashSet<String>>,
    ) -> AnalyzerResult<Arc<LogicalPlan>> {
        log::info!(
            "🔀 UnifiedTypeInference: Generating UNION for {} untyped nodes",
            untyped_nodes.len()
        );

        log::debug!(
            "🔍 Phase 2: {} relationships, {} property accesses from original plan",
            relationships.len(),
            property_accesses.len()
        );
        for rel in relationships {
            log::debug!(
                "🔍 Phase 2: Relationship: left={}, right={}, types={:?}, dir={:?}",
                rel.left_alias, rel.right_alias, rel.rel_types, rel.direction
            );
        }
        for (var, props) in property_accesses {
            log::debug!("🔍 Phase 2: PropertyAccess: var={}, props={:?}", var, props);
        }

        // Collect type candidates for each untyped variable, constrained by:
        // 1. Labeled relationships (from schema from_node/to_node)
        // 2. Property accesses (only types that have the accessed properties)
        let mut untyped_vars: Vec<(String, Vec<String>)> = Vec::new();
        for var_name in untyped_nodes {
            let mut candidates: Vec<String> = graph_schema
                .all_node_schemas()
                .keys()
                .cloned()
                .collect();

            // Constraint 1: Filter by labeled relationships involving this variable
            for rel_pattern in relationships {
                if rel_pattern.rel_types.is_empty() {
                    continue;
                }
                if rel_pattern.left_alias == *var_name {
                    let valid_types: HashSet<String> = rel_pattern
                        .rel_types
                        .iter()
                        .filter_map(|rel_type| {
                            graph_schema.get_rel_schema(rel_type).ok().and_then(|rs| {
                                match rel_pattern.direction {
                                    Direction::Outgoing => Some(rs.from_node.clone()),
                                    Direction::Incoming => Some(rs.to_node.clone()),
                                    Direction::Either => None,
                                }
                            })
                        })
                        .collect();
                    if !valid_types.is_empty() {
                        candidates.retain(|c| valid_types.contains(c));
                        log::debug!(
                            "🔍 Constrained '{}' (left) by rel {:?}: valid={:?}",
                            var_name, rel_pattern.rel_types, valid_types
                        );
                    }
                } else if rel_pattern.right_alias == *var_name {
                    let valid_types: HashSet<String> = rel_pattern
                        .rel_types
                        .iter()
                        .filter_map(|rel_type| {
                            graph_schema.get_rel_schema(rel_type).ok().and_then(|rs| {
                                match rel_pattern.direction {
                                    Direction::Outgoing => Some(rs.to_node.clone()),
                                    Direction::Incoming => Some(rs.from_node.clone()),
                                    Direction::Either => None,
                                }
                            })
                        })
                        .collect();
                    if !valid_types.is_empty() {
                        candidates.retain(|c| valid_types.contains(c));
                        log::debug!(
                            "🔍 Constrained '{}' (right) by rel {:?}: valid={:?}",
                            var_name, rel_pattern.rel_types, valid_types
                        );
                    }
                }
            }

            // Constraint 2: Filter by accessed properties
            // Uses property_mappings keys (Cypher names), NOT column_names (ClickHouse names)
            if let Some(props) = property_accesses.get(var_name) {
                candidates.retain(|type_name| {
                    if let Ok(node_schema) = graph_schema.node_schema(type_name) {
                        props.iter().all(|prop| node_schema.has_cypher_property(prop))
                    } else {
                        false
                    }
                });
                log::debug!(
                    "🔍 Constrained '{}' by properties {:?}: {:?}",
                    var_name, props, candidates
                );
            }

            if candidates.is_empty() {
                log::warn!("🔍 No valid types for '{}' after constraints", var_name);
                continue;
            }

            log::debug!(
                "🔍 Variable '{}': {} candidates after constraints: {:?}",
                var_name, candidates.len(), candidates
            );
            untyped_vars.push((var_name.clone(), candidates));
        }

        if untyped_vars.is_empty() {
            return Ok(plan);
        }
        
        // Collect already-typed nodes from plan_ctx
        let typed_nodes: HashMap<String, String> = plan_ctx
            .iter_table_contexts()
            .filter_map(|(var_name, table_ctx)| {
                table_ctx
                    .get_labels()
                    .and_then(|labels| labels.first().map(|l| (var_name.clone(), l.clone())))
            })
            .collect();
        
        log::info!(
            "🔍 Found {} relationship patterns, {} typed nodes",
            relationships.len(),
            typed_nodes.len()
        );

        // Generate type combinations (cartesian product)
        let max_combinations = get_max_combinations();
        let combinations = generate_type_combinations(&untyped_vars, max_combinations);

        log::info!(
            "🔍 Generated {} type combinations (max: {})",
            combinations.len(),
            max_combinations
        );

        // Filter combinations by schema validity + direction
        let valid_combinations: Vec<_> = combinations
            .into_iter()
            .filter(|combo| {
                is_valid_combination_with_direction(
                    combo,
                    &relationships,
                    graph_schema,
                    &typed_nodes,
                )
            })
            .collect();

        log::info!(
            "✅ UnifiedTypeInference: {} valid combinations after direction validation",
            valid_combinations.len()
        );

        if valid_combinations.is_empty() {
            log::warn!("⚠️ No valid combinations found - query may return empty results");
            return Ok(plan);
        }

        if valid_combinations.len() == 1 {
            log::info!("✅ Single valid combination, no UNION needed");
            // Update plan_ctx with the single valid combination
            for (var_name, type_name) in &valid_combinations[0] {
                self.update_plan_ctx_with_label(var_name, type_name, plan_ctx)?;
            }
            return Ok(plan);
        }

        // Multiple valid combinations - generate UNION branches
        log::info!("🔀 Generating UNION with {} branches", valid_combinations.len());

        // Update plan_ctx with all labels for each untyped variable so downstream
        // passes (e.g., ProjectionTagging) can resolve label-dependent logic.
        // Each branch has a specific label in its GraphNode, but plan_ctx is shared.
        let mut all_labels_per_var: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for combo in &valid_combinations {
            for (var_name, type_name) in combo {
                all_labels_per_var
                    .entry(var_name.clone())
                    .or_default()
                    .push(type_name.clone());
            }
        }
        for (var_name, labels) in &all_labels_per_var {
            if let Ok(mut table_ctx) = plan_ctx.get_table_ctx(var_name).cloned() {
                table_ctx.set_labels(Some(labels.clone()));
                plan_ctx.insert_table_ctx(var_name.clone(), table_ctx);
            }
        }

        let mut union_branches = Vec::new();

        // Check if the plan has aggregation (GroupBy or aggregate functions in Projection).
        // If so, inject the UNION *below* the aggregation layer so the aggregate
        // operates on the combined rows from all branches, rather than each branch
        // independently computing its own aggregate.
        let has_aggregation = plan_has_aggregation(&plan);

        if has_aggregation {
            log::info!("🔀 Plan has aggregation: injecting UNION below aggregation layer");
            // Split plan into aggregation wrapper + scan part.
            // Clone only scan parts per combination, then re-wrap.
            for combo in valid_combinations {
                let scan_branch = clone_plan_with_labels(
                    &extract_scan_part(&plan),
                    &combo,
                );
                log::debug!(
                    "✅ Generated scan branch for combination: {:?}",
                    combo
                );
                union_branches.push(Arc::new(scan_branch));
            }

            let union_plan = Union {
                inputs: union_branches,
                union_type: UnionType::All,
            };
            let union_arc = Arc::new(LogicalPlan::Union(union_plan));

            // Re-wrap with the aggregation layers from the original plan
            Ok(rewrap_aggregation(&plan, union_arc))
        } else {
            for combo in valid_combinations {
                let branch_plan = clone_plan_with_labels(&plan, &combo);
                log::debug!(
                    "✅ Generated UNION branch for combination: {:?}",
                    combo
                );
                union_branches.push(Arc::new(branch_plan));
            }

            let union_plan = Union {
                inputs: union_branches,
                union_type: UnionType::All,
            };

            Ok(Arc::new(LogicalPlan::Union(union_plan)))
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

    // ================================================================================================
    // Phase 0: Relationship-Based Label Inference (from SchemaInference)
    // ================================================================================================

    /// Phase 0: Infer missing node/relationship labels from GraphRel patterns
    ///
    /// This mirrors SchemaInference.infer_schema() logic but runs as Phase 0 of
    /// UnifiedTypeInference. Walks the plan tree and infers labels based on the
    /// 8 combinations of known/unknown in (a)-[r]->(b) patterns.
    fn infer_schema_relationships(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                self.infer_schema_relationships(projection.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::GraphNode(graph_node) => {
                self.infer_schema_relationships(graph_node.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_alias = &graph_rel.left_connection;
                let right_alias = &graph_rel.right_connection;

                // Try to get table contexts - may not exist yet
                let left_table_ctx_opt =
                    plan_ctx.get_table_ctx_from_alias_opt(&Some(left_alias.clone()));
                let right_table_ctx_opt =
                    plan_ctx.get_table_ctx_from_alias_opt(&Some(right_alias.clone()));

                // If contexts don't exist yet, skip (will be handled in later phases)
                if let (Ok(left_table_ctx), Ok(right_table_ctx)) =
                    (left_table_ctx_opt, right_table_ctx_opt)
                {
                    if let Ok(rel_table_ctx) = plan_ctx.get_rel_table_ctx(&graph_rel.alias) {
                        // Skip label inference for relationships with multiple types
                        let should_infer_labels = !rel_table_ctx
                            .get_labels()
                            .map(|labels| labels.len() > 1)
                            .unwrap_or(false);

                        if should_infer_labels {
                            // Use the 8-case inference logic from SchemaInference
                            if let Ok((left_label, rel_label, right_label)) = self
                                .infer_missing_labels_from_schema(
                                    graph_schema,
                                    left_table_ctx,
                                    rel_table_ctx,
                                    right_table_ctx,
                                )
                            {
                                // Update plan_ctx with inferred labels
                                for (alias, label) in [
                                    (left_alias, left_label),
                                    (&graph_rel.alias, rel_label),
                                    (right_alias, right_label),
                                ] {
                                    if let Ok(table_ctx) = plan_ctx.get_mut_table_ctx(alias) {
                                        if table_ctx.get_label_opt().is_none() {
                                            log::info!(
                                                "🏷️ Phase 0: Inferred label '{}' for alias '{}'",
                                                label,
                                                alias
                                            );
                                            table_ctx.set_labels(Some(vec![label]));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Recurse into child patterns
                self.infer_schema_relationships(graph_rel.left.clone(), plan_ctx, graph_schema)?;
                self.infer_schema_relationships(graph_rel.right.clone(), plan_ctx, graph_schema)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Infer missing labels using the 8-case logic from SchemaInference
    ///
    /// This is the core label inference algorithm that handles all combinations of
    /// known/unknown labels in (a)-[r]->(b) patterns.
    fn infer_missing_labels_from_schema(
        &self,
        graph_schema: &GraphSchema,
        left_table_ctx: &TableCtx,
        rel_table_ctx: &TableCtx,
        right_table_ctx: &TableCtx,
    ) -> AnalyzerResult<(String, String, String)> {
        use crate::query_planner::analyzer::errors::{AnalyzerError, Pass};

        // Case 1: All labels present - return as-is
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_some()
        {
            return Ok((
                left_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                    pass: Pass::SchemaInference,
                    source: e,
                })?,
                rel_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                    pass: Pass::SchemaInference,
                    source: e,
                })?,
                right_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                    pass: Pass::SchemaInference,
                    source: e,
                })?,
            ));
        }

        // Case 2: Only left missing
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_some()
        {
            let rel_label = rel_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let right_label = right_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;

            let left_label = if right_label == rel_schema.from_node {
                rel_schema.to_node.clone()
            } else {
                rel_schema.from_node.clone()
            };
            return Ok((left_label, rel_label, right_label));
        }

        // Case 3: Only right missing
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_none()
        {
            let left_label = left_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let rel_label = rel_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;

            let right_label = if left_label == rel_schema.from_node {
                rel_schema.to_node.clone()
            } else {
                rel_schema.from_node.clone()
            };
            return Ok((left_label, rel_label, right_label));
        }

        // Case 4: Only relationship missing
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_none()
            && right_table_ctx.get_label_opt().is_some()
        {
            let left_label = left_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let right_label = right_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;

            // Find relationship that connects these nodes
            for (_, rel_schema) in graph_schema.get_relationships_schemas().iter() {
                if (rel_schema.from_node == left_label && rel_schema.to_node == right_label)
                    || (rel_schema.from_node == right_label && rel_schema.to_node == left_label)
                {
                    return Ok((left_label, rel_schema.table_name.clone(), right_label));
                }
            }
            return Err(AnalyzerError::MissingRelationLabel {
                pass: Pass::SchemaInference,
            });
        }

        // Case 5: Both nodes missing, relationship present
        // Infer node types directly from relationship schema's from_node/to_node
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_none()
        {
            let rel_label = rel_table_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::SchemaInference,
                source: e,
            })?;
            let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;
            return Ok((
                rel_schema.from_node.clone(),
                rel_label,
                rel_schema.to_node.clone(),
            ));
        }

        // Cases 6-8: Multiple unknowns without relationship - defer to Phase 2
        Ok((
            left_table_ctx.get_label_opt().unwrap_or_default(),
            rel_table_ctx.get_label_opt().unwrap_or_default(),
            right_table_ctx.get_label_opt().unwrap_or_default(),
        ))
    }

    // ================================================================================================
    // Phase 3: ViewScan Resolution (from SchemaInference)
    // ================================================================================================

    /// Phase 3: Resolve GraphNode(input=Empty) → ViewScan based on inferred labels
    ///
    /// This mirrors SchemaInference.push_inferred_table_names_to_scan() logic.
    /// After phases 0-2 have inferred labels, this phase creates concrete ViewScan
    /// nodes from the inferred label information in TableCtx.
    ///
    /// **NOTE**: Currently a placeholder. SchemaInference still handles ViewScan creation.
    /// This will be fully implemented after Phase B testing confirms Phase 0 works correctly.
    /// Phase 3: ViewScan Resolution
    /// 
    /// Converts GraphNode(label=Some, input=Empty) → GraphNode(label=Some, input=ViewScan)
    /// This is the final step that materializes the type inference into actual table scans.
    /// 
    /// Handles:
    /// - Node ViewScans: GraphNode with inferred label
    /// - Relationship ViewScans: GraphRel center with inferred type
    /// - Denormalized patterns: Node properties in edge tables
    fn push_inferred_table_names_to_scan(
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Arc<LogicalPlan>> {
        use crate::query_planner::transformed::Transformed;
        
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(projection.input.clone(), plan_ctx, _graph_schema)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                // Check if input is Empty - need to resolve to ViewScan
                if matches!(graph_node.input.as_ref(), LogicalPlan::Empty) {
                    // First check if GraphNode already has a label (set by TypeInference Phase 2)
                    let label_to_use = if let Some(ref node_label) = graph_node.label {
                        if node_label != "$any" {
                            Some(node_label.clone())
                        } else {
                            None
                        }
                    } else {
                        // Fallback: Get inferred label from TableCtx
                        match plan_ctx.get_table_ctx(&graph_node.alias) {
                            Ok(table_ctx) => {
                                log::debug!(
                                    "TypeInference Phase 3: Found table_ctx for node '{}' with labels {:?}",
                                    graph_node.alias,
                                    table_ctx.get_labels()
                                );
                                table_ctx.get_labels().and_then(|labels| {
                                    if !labels.is_empty() && labels[0] != "$any" {
                                        Some(labels[0].clone())
                                    } else {
                                        None
                                    }
                                })
                            }
                            Err(e) => {
                                log::debug!(
                                    "TypeInference Phase 3: No table_ctx found for node '{}': {:?}",
                                    graph_node.alias,
                                    e
                                );
                                None
                            }
                        }
                    };

                    if let Some(label) = label_to_use {
                        log::info!(
                            "TypeInference Phase 3: Resolving Empty → ViewScan for node '{}' with label '{}'",
                            graph_node.alias, label
                        );

                        // Create ViewScan using the label
                        match crate::query_planner::logical_plan::match_clause::try_generate_view_scan(
                            &graph_node.alias,
                            &label,
                            plan_ctx,
                        ) {
                            Ok(Some(view_scan)) => {
                                log::info!("TypeInference Phase 3: ✓ Successfully created ViewScan for node '{}' with label '{}'", graph_node.alias, label);
                                // Rebuild GraphNode with ViewScan instead of Empty
                                return Ok(Arc::new(LogicalPlan::GraphNode(
                                    crate::query_planner::logical_plan::GraphNode {
                                        input: view_scan,
                                        alias: graph_node.alias.clone(),
                                        label: Some(label.clone()),
                                        is_denormalized: graph_node.is_denormalized,
                                        projected_columns: graph_node.projected_columns.clone(),
                                        node_types: None,
                                    },
                                )));
                            }
                            Ok(None) => {
                                log::warn!(
                                    "TypeInference Phase 3: Failed to create ViewScan for node '{}' with label '{}' (returned None)",
                                    graph_node.alias, label
                                );
                            }
                            Err(e) => {
                                log::warn!(
                                    "TypeInference Phase 3: Error creating ViewScan for node '{}' with label '{}': {:?}",
                                    graph_node.alias, label, e
                                );
                            }
                        }
                    } else {
                        log::debug!("TypeInference Phase 3: Node '{}' has no valid label, skipping ViewScan creation", graph_node.alias);
                    }
                }

                // Recurse into input (for ViewScan or other plan types)
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_node.input.clone(), plan_ctx, _graph_schema)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // First recurse into left and right nodes
                let left_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.left.clone(), plan_ctx, _graph_schema)?;
                let right_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.right.clone(), plan_ctx, _graph_schema)?;

                // Check if center (relationship) is Empty - need to resolve to ViewScan
                let center_tf = if matches!(graph_rel.center.as_ref(), LogicalPlan::Empty) {
                    // Get inferred relationship type from TableCtx
                    if let Ok(rel_ctx) = plan_ctx.get_rel_table_ctx(&graph_rel.alias) {
                        if let Some(labels) = rel_ctx.get_labels() {
                            if labels.len() == 1 {
                                let rel_type = &labels[0];
                                log::info!(
                                    "TypeInference Phase 3: Resolving Empty → ViewScan for relationship '{}' with inferred type '{}'",
                                    graph_rel.alias, rel_type
                                );

                                // Get left and right node labels for context
                                let left_label = if let LogicalPlan::GraphNode(left_node) =
                                    graph_rel.left.as_ref()
                                {
                                    left_node.label.as_deref()
                                } else {
                                    None
                                };

                                let right_label = if let LogicalPlan::GraphNode(right_node) =
                                    graph_rel.right.as_ref()
                                {
                                    right_node.label.as_deref()
                                } else {
                                    None
                                };

                                // Create ViewScan for the relationship
                                if let Some(view_scan) = crate::query_planner::logical_plan::match_clause::try_generate_relationship_view_scan(
                                    &graph_rel.alias,
                                    rel_type,
                                    left_label,
                                    right_label,
                                    plan_ctx,
                                ) {
                                    Transformed::Yes(view_scan)
                                } else {
                                    log::warn!(
                                        "TypeInference Phase 3: Failed to create ViewScan for relationship '{}' with type '{}'",
                                        graph_rel.alias, rel_type
                                    );
                                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, _graph_schema)?
                                }
                            } else {
                                // Multiple relationship types - keep Empty, will be handled by UNION generation
                                log::debug!(
                                    "TypeInference Phase 3: Relationship '{}' has multiple types {:?}, keeping Empty for UNION generation",
                                    graph_rel.alias, labels
                                );
                                Self::push_inferred_table_names_to_scan_transformed(
                                    graph_rel.center.clone(),
                                    plan_ctx,
                                    _graph_schema,
                                )?
                            }
                        } else {
                            Self::push_inferred_table_names_to_scan_transformed(
                                graph_rel.center.clone(),
                                plan_ctx,
                                _graph_schema,
                            )?
                        }
                    } else {
                        Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, _graph_schema)?
                    }
                } else {
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, _graph_schema)?
                };

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(cte.input.clone(), plan_ctx, _graph_schema)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_joins.input.clone(), plan_ctx, _graph_schema)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(filter.input.clone(), plan_ctx, _graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(group_by.input.clone(), plan_ctx, _graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(order_by.input.clone(), plan_ctx, _graph_schema)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(skip.input.clone(), plan_ctx, _graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(limit.input.clone(), plan_ctx, _graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        Self::push_inferred_table_names_to_scan_transformed(input_plan.clone(), plan_ctx, _graph_schema)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(u.input.clone(), plan_ctx, _graph_schema)?;
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
                let left_tf = Self::push_inferred_table_names_to_scan_transformed(cp.left.clone(), plan_ctx, _graph_schema)?;
                let right_tf = Self::push_inferred_table_names_to_scan_transformed(cp.right.clone(), plan_ctx, _graph_schema)?;
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
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(with_clause.input.clone(), plan_ctx, _graph_schema)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            cte_name: None,
                            input: new_input,
                            items: with_clause.items.clone(),
                            distinct: with_clause.distinct,
                            order_by: with_clause.order_by.clone(),
                            skip: with_clause.skip,
                            limit: with_clause.limit,
                            where_clause: with_clause.where_clause.clone(),
                            exported_aliases: with_clause.exported_aliases.clone(),
                            cte_references: with_clause.cte_references.clone(),
                            pattern_comprehensions: with_clause.pattern_comprehensions.clone(),
                        };
                        Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_with)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
        };
        Ok(transformed_plan.get_plan().clone())
    }
    
    // Helper that returns Transformed for recursive calls
    fn push_inferred_table_names_to_scan_transformed(
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        use crate::query_planner::transformed::Transformed;
        
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(projection.input.clone(), plan_ctx, graph_schema)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                // Check if input is Empty - need to resolve to ViewScan
                if matches!(graph_node.input.as_ref(), LogicalPlan::Empty) {
                    // First check if GraphNode already has a label (set by TypeInference Phase 2)
                    let label_to_use = if let Some(ref node_label) = graph_node.label {
                        if node_label != "$any" {
                            Some(node_label.clone())
                        } else {
                            None
                        }
                    } else {
                        // Fallback: Get inferred label from TableCtx
                        match plan_ctx.get_table_ctx(&graph_node.alias) {
                            Ok(table_ctx) => {
                                table_ctx.get_labels().and_then(|labels| {
                                    if !labels.is_empty() && labels[0] != "$any" {
                                        Some(labels[0].clone())
                                    } else {
                                        None
                                    }
                                })
                            }
                            Err(_) => None
                        }
                    };

                    if let Some(label) = label_to_use {
                        // Create ViewScan using the label
                        match crate::query_planner::logical_plan::match_clause::try_generate_view_scan(
                            &graph_node.alias,
                            &label,
                            plan_ctx,
                        ) {
                            Ok(Some(view_scan)) => {
                                // Rebuild GraphNode with ViewScan instead of Empty
                                return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                                    crate::query_planner::logical_plan::GraphNode {
                                        input: view_scan,
                                        alias: graph_node.alias.clone(),
                                        label: Some(label.clone()),
                                        is_denormalized: graph_node.is_denormalized,
                                        projected_columns: graph_node.projected_columns.clone(),
                                        node_types: None,
                                    },
                                ))));
                            }
                            _ => {}
                        }
                    }
                }

                // Recurse into input
                let child_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_node.input.clone(), plan_ctx, graph_schema)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.left.clone(), plan_ctx, graph_schema)?;
                let right_tf =
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.right.clone(), plan_ctx, graph_schema)?;

                let center_tf = if matches!(graph_rel.center.as_ref(), LogicalPlan::Empty) {
                    if let Ok(rel_ctx) = plan_ctx.get_rel_table_ctx(&graph_rel.alias) {
                        if let Some(labels) = rel_ctx.get_labels() {
                            if labels.len() == 1 {
                                let rel_type = &labels[0];
                                let left_label = if let LogicalPlan::GraphNode(left_node) = graph_rel.left.as_ref() {
                                    left_node.label.as_deref()
                                } else {
                                    None
                                };
                                let right_label = if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                                    right_node.label.as_deref()
                                } else {
                                    None
                                };

                                if let Some(view_scan) = crate::query_planner::logical_plan::match_clause::try_generate_relationship_view_scan(
                                    &graph_rel.alias,
                                    rel_type,
                                    left_label,
                                    right_label,
                                    plan_ctx,
                                ) {
                                    Transformed::Yes(view_scan)
                                } else {
                                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, graph_schema)?
                                }
                            } else {
                                Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, graph_schema)?
                            }
                        } else {
                            Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, graph_schema)?
                        }
                    } else {
                        Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, graph_schema)?
                    }
                } else {
                    Self::push_inferred_table_names_to_scan_transformed(graph_rel.center.clone(), plan_ctx, graph_schema)?
                };

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(cte.input.clone(), plan_ctx, graph_schema)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(graph_joins.input.clone(), plan_ctx, graph_schema)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(filter.input.clone(), plan_ctx, graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(group_by.input.clone(), plan_ctx, graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(order_by.input.clone(), plan_ctx, graph_schema)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(skip.input.clone(), plan_ctx, graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(limit.input.clone(), plan_ctx, graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = Self::push_inferred_table_names_to_scan_transformed(input_plan.clone(), plan_ctx, graph_schema)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(u.input.clone(), plan_ctx, graph_schema)?;
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
                let left_tf = Self::push_inferred_table_names_to_scan_transformed(cp.left.clone(), plan_ctx, graph_schema)?;
                let right_tf = Self::push_inferred_table_names_to_scan_transformed(cp.right.clone(), plan_ctx, graph_schema)?;
                match (&left_tf, &right_tf) {
                    (Transformed::No(_), Transformed::No(_)) => Transformed::No(logical_plan.clone()),
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
                let child_tf = Self::push_inferred_table_names_to_scan_transformed(with_clause.input.clone(), plan_ctx, graph_schema)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            cte_name: None,
                            input: new_input,
                            items: with_clause.items.clone(),
                            distinct: with_clause.distinct,
                            order_by: with_clause.order_by.clone(),
                            skip: with_clause.skip,
                            limit: with_clause.limit,
                            where_clause: with_clause.where_clause.clone(),
                            exported_aliases: with_clause.exported_aliases.clone(),
                            cte_references: with_clause.cte_references.clone(),
                            pattern_comprehensions: with_clause.pattern_comprehensions.clone(),
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

impl AnalyzerPass for TypeInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::info!("🏷️ UnifiedTypeInference: Starting 4-phase type inference pass");
        
        // Phase 0: Relationship-based label inference (from SchemaInference)
        // Walk the plan and infer missing labels from GraphRel patterns
        log::info!("🏷️ UnifiedTypeInference: Phase 0 - Relationship-based label inference");
        self.infer_schema_relationships(logical_plan.clone(), plan_ctx, graph_schema)?;
        
        // Phase 1: Incremental type inference + Filter→GraphRel UNION generation
        log::info!("🏷️ UnifiedTypeInference: Phase 1 - Filter→GraphRel UNION generation");
        
        // Extract relationship patterns and property accesses from the ORIGINAL plan
        // BEFORE Phase 1 transforms it (Phase 1 may wrap/replace GraphRel nodes)
        let original_relationships = extract_relationship_patterns(&logical_plan, plan_ctx);
        let original_property_accesses = extract_property_accesses(&logical_plan);
        
        let result = self.infer_labels_recursive(logical_plan.clone(), plan_ctx, graph_schema)?;
        let phase1_transformed = result.is_yes();
        let mut plan = result.get_plan();
        
        log::info!("🏷️ UnifiedTypeInference: Phase 2 - Checking for untyped nodes");
        
        // Phase 2: Discover remaining untyped nodes and generate UNION
        let untyped_nodes = discover_untyped_nodes(&plan, plan_ctx);
        
        if !untyped_nodes.is_empty() {
            log::info!(
                "🏷️ UnifiedTypeInference: Found {} untyped nodes: {:?}",
                untyped_nodes.len(),
                untyped_nodes
            );
            
            // For top-level Cypher UNION, process each arm independently so that
            // arms without untyped nodes don't get duplicated across combinations.
            if let LogicalPlan::Union(top_union) = plan.as_ref() {
                let mut new_inputs = Vec::new();
                for arm in &top_union.inputs {
                    let arm_untyped = discover_untyped_nodes(arm, plan_ctx);
                    if arm_untyped.is_empty() {
                        // This arm has no untyped nodes — keep as-is
                        new_inputs.push(arm.clone());
                    } else {
                        // Extract relationships/properties scoped to this arm
                        let arm_rels: Vec<_> = original_relationships.iter()
                            .filter(|r| arm_untyped.contains(&r.left_alias) || arm_untyped.contains(&r.right_alias))
                            .cloned()
                            .collect();
                        let arm_props: HashMap<_, _> = original_property_accesses.iter()
                            .filter(|(k, _)| arm_untyped.contains(k.as_str()))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        let expanded = self.generate_union_for_untyped_nodes(
                            arm.clone(),
                            &arm_untyped,
                            plan_ctx,
                            graph_schema,
                            &arm_rels,
                            &arm_props,
                        )?;
                        new_inputs.push(expanded);
                    }
                }
                plan = Arc::new(LogicalPlan::Union(crate::query_planner::logical_plan::Union {
                    inputs: new_inputs,
                    union_type: top_union.union_type.clone(),
                }));
            } else {
                // Not a top-level Union — process normally
                plan = self.generate_union_for_untyped_nodes(
                    plan,
                    &untyped_nodes,
                    plan_ctx,
                    graph_schema,
                    &original_relationships,
                    &original_property_accesses,
                )?;
            }
        } else {
            log::info!("🏷️ UnifiedTypeInference: No untyped nodes found");
        }
        
        // Phase 3: ViewScan resolution (from SchemaInference)  
        // Resolve GraphNode(input=Empty) → ViewScan based on inferred labels
        log::info!("🏷️ UnifiedTypeInference: Phase 3 - ViewScan resolution");
        plan = Self::push_inferred_table_names_to_scan(plan, plan_ctx, graph_schema)?;
        
        log::info!(
            "🏷️ UnifiedTypeInference: Completed - phase0=yes, phase1={}, phase2={}, phase3=yes",
            phase1_transformed,
            !untyped_nodes.is_empty()
        );
        Ok(Transformed::Yes(plan))
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

// ================================================================================================
// Untyped Node Discovery (Phase 2 of Unified TypeInference)
// ================================================================================================

/// Discover all untyped node variables in the logical plan
///
/// Recursively traverses the plan tree to find:
/// - GraphNode without label
/// - GraphRel connections without labels in plan_ctx
///
/// This is Phase 2 of unified type inference, running after Filter→GraphRel handling
fn discover_untyped_nodes(plan: &Arc<LogicalPlan>, plan_ctx: &PlanCtx) -> HashSet<String> {
    log::debug!("🔍 discover_untyped_nodes: Starting discovery on plan type: {:?}", 
        std::mem::discriminant(plan.as_ref()));
    let mut untyped = HashSet::new();
    discover_untyped_recursive(plan, plan_ctx, &mut untyped);
    log::debug!("🔍 discover_untyped_nodes: Found {} untyped nodes: {:?}", untyped.len(), untyped);
    untyped
}

/// Recursive helper for untyped node discovery
fn discover_untyped_recursive(
    plan: &LogicalPlan,
    plan_ctx: &PlanCtx,
    untyped: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check left connection (from node)
            if !has_label_in_ctx(&graph_rel.left_connection, plan_ctx) {
                log::debug!("🔍 Found untyped left connection: {}", graph_rel.left_connection);
                untyped.insert(graph_rel.left_connection.clone());
            }

            // Check right connection (to node)
            if !has_label_in_ctx(&graph_rel.right_connection, plan_ctx) {
                log::debug!("🔍 Found untyped right connection: {}", graph_rel.right_connection);
                untyped.insert(graph_rel.right_connection.clone());
            }

            // Recurse to sub-plans
            discover_untyped_recursive(&graph_rel.left, plan_ctx, untyped);
            discover_untyped_recursive(&graph_rel.center, plan_ctx, untyped);
            discover_untyped_recursive(&graph_rel.right, plan_ctx, untyped);
        }

        LogicalPlan::GraphNode(graph_node) => {
            // Check if node has label
            log::debug!("🔍 discover_untyped: Checking GraphNode alias={}, label={:?}", 
                graph_node.alias, graph_node.label);
            if graph_node.label.is_none() {
                log::debug!("🔍 Found untyped GraphNode: {}", graph_node.alias);
                untyped.insert(graph_node.alias.clone());
            }

            // Recurse to input
            discover_untyped_recursive(&graph_node.input, plan_ctx, untyped);
        }

        // Handle other plan types with inputs
        LogicalPlan::Filter(filter) => {
            discover_untyped_recursive(&filter.input, plan_ctx, untyped);
        }

        LogicalPlan::Projection(proj) => {
            discover_untyped_recursive(&proj.input, plan_ctx, untyped);
        }

        LogicalPlan::Union(union_plan) => {
            for input in &union_plan.inputs {
                discover_untyped_recursive(input, plan_ctx, untyped);
            }
        }

        LogicalPlan::GroupBy(group_by) => {
            discover_untyped_recursive(&group_by.input, plan_ctx, untyped);
        }

        LogicalPlan::OrderBy(order_by) => {
            discover_untyped_recursive(&order_by.input, plan_ctx, untyped);
        }

        LogicalPlan::Limit(limit) => {
            discover_untyped_recursive(&limit.input, plan_ctx, untyped);
        }

        LogicalPlan::Skip(skip) => {
            discover_untyped_recursive(&skip.input, plan_ctx, untyped);
        }

        LogicalPlan::WithClause(wc) => {
            discover_untyped_recursive(&wc.input, plan_ctx, untyped);
        }

        LogicalPlan::CartesianProduct(cp) => {
            discover_untyped_recursive(&cp.left, plan_ctx, untyped);
            discover_untyped_recursive(&cp.right, plan_ctx, untyped);
        }

        // Leaf nodes - no traversal needed
        LogicalPlan::ViewScan(_) | LogicalPlan::Empty => {}

        _ => {
            log::debug!("🔍 discover_untyped: Unhandled plan type: {:?}", plan);
        }
    }
}

/// Check if a variable has labels in plan_ctx
fn has_label_in_ctx(var_name: &str, plan_ctx: &PlanCtx) -> bool {
    plan_ctx
        .get_table_ctx(var_name)
        .ok()
        .and_then(|ctx| ctx.get_labels())
        .is_some()
}

// ================================================================================================
// UNION Generation Helpers (Phase 2 - Untyped Nodes)
// ================================================================================================

/// Relationship pattern extracted from logical plan for validation
#[derive(Debug, Clone)]
struct RelationshipPattern {
    left_alias: String,
    right_alias: String,
    rel_types: Vec<String>, // Empty means any relationship
    direction: Direction,
}

/// Extract all relationship patterns from the plan
fn extract_relationship_patterns(
    plan: &Arc<LogicalPlan>,
    plan_ctx: &PlanCtx,
) -> Vec<RelationshipPattern> {
    let mut patterns = Vec::new();
    extract_patterns_recursive(plan.as_ref(), plan_ctx, &mut patterns);
    patterns
}

/// Recursive helper to extract relationship patterns
fn extract_patterns_recursive(
    plan: &LogicalPlan,
    _plan_ctx: &PlanCtx,
    patterns: &mut Vec<RelationshipPattern>,
) {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            patterns.push(RelationshipPattern {
                left_alias: rel.left_connection.clone(),
                right_alias: rel.right_connection.clone(),
                rel_types: rel.labels.clone().unwrap_or_default(),
                direction: rel.direction.clone(),
            });

            // Recurse to children
            extract_patterns_recursive(&rel.left, _plan_ctx, patterns);
            extract_patterns_recursive(&rel.center, _plan_ctx, patterns);
            extract_patterns_recursive(&rel.right, _plan_ctx, patterns);
        }

        LogicalPlan::Filter(filter) => {
            extract_patterns_recursive(&filter.input, _plan_ctx, patterns);
        }

        LogicalPlan::Projection(proj) => {
            extract_patterns_recursive(&proj.input, _plan_ctx, patterns);
        }

        LogicalPlan::Union(union_plan) => {
            for input in &union_plan.inputs {
                extract_patterns_recursive(input, _plan_ctx, patterns);
            }
        }

        LogicalPlan::WithClause(wc) => {
            extract_patterns_recursive(&wc.input, _plan_ctx, patterns);
        }

        LogicalPlan::CartesianProduct(cp) => {
            extract_patterns_recursive(&cp.left, _plan_ctx, patterns);
            extract_patterns_recursive(&cp.right, _plan_ctx, patterns);
        }

        LogicalPlan::GraphNode(node) => {
            extract_patterns_recursive(&node.input, _plan_ctx, patterns);
        }

        LogicalPlan::Limit(lim) => {
            extract_patterns_recursive(&lim.input, _plan_ctx, patterns);
        }

        LogicalPlan::Skip(s) => {
            extract_patterns_recursive(&s.input, _plan_ctx, patterns);
        }

        LogicalPlan::OrderBy(ob) => {
            extract_patterns_recursive(&ob.input, _plan_ctx, patterns);
        }

        LogicalPlan::GroupBy(gb) => {
            extract_patterns_recursive(&gb.input, _plan_ctx, patterns);
        }

        LogicalPlan::GraphJoins(gj) => {
            extract_patterns_recursive(&gj.input, _plan_ctx, patterns);
        }

        // Leaf nodes
        _ => {}
    }
}

/// Extract property accesses from the plan tree, grouped by variable name.
/// Returns a map: variable_alias → set of Cypher property names accessed.
/// Used to constrain node type candidates to only those types that have the accessed properties.
fn extract_property_accesses(plan: &Arc<LogicalPlan>) -> HashMap<String, HashSet<String>> {
    let mut accesses: HashMap<String, HashSet<String>> = HashMap::new();
    extract_props_from_plan(plan.as_ref(), &mut accesses);
    accesses
}

fn extract_props_from_plan(
    plan: &LogicalPlan,
    accesses: &mut HashMap<String, HashSet<String>>,
) {
    match plan {
        LogicalPlan::Filter(filter) => {
            extract_props_from_expr(&filter.predicate, accesses);
            extract_props_from_plan(&filter.input, accesses);
        }
        LogicalPlan::Projection(proj) => {
            for item in &proj.items {
                extract_props_from_expr(&item.expression, accesses);
            }
            extract_props_from_plan(&proj.input, accesses);
        }
        LogicalPlan::OrderBy(ob) => {
            for item in &ob.items {
                extract_props_from_expr(&item.expression, accesses);
            }
            extract_props_from_plan(&ob.input, accesses);
        }
        LogicalPlan::GroupBy(gb) => {
            for item in &gb.expressions {
                extract_props_from_expr(item, accesses);
            }
            if let Some(ref having) = gb.having_clause {
                extract_props_from_expr(having, accesses);
            }
            extract_props_from_plan(&gb.input, accesses);
        }
        LogicalPlan::GraphRel(rel) => {
            extract_props_from_plan(&rel.left, accesses);
            extract_props_from_plan(&rel.center, accesses);
            extract_props_from_plan(&rel.right, accesses);
            if let Some(ref pred) = rel.where_predicate {
                extract_props_from_expr(pred, accesses);
            }
        }
        LogicalPlan::GraphNode(node) => {
            extract_props_from_plan(&node.input, accesses);
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                extract_props_from_plan(input, accesses);
            }
        }
        LogicalPlan::WithClause(wc) => {
            extract_props_from_plan(&wc.input, accesses);
        }
        LogicalPlan::CartesianProduct(cp) => {
            extract_props_from_plan(&cp.left, accesses);
            extract_props_from_plan(&cp.right, accesses);
        }
        LogicalPlan::Limit(lim) => {
            extract_props_from_plan(&lim.input, accesses);
        }
        LogicalPlan::Skip(s) => {
            extract_props_from_plan(&s.input, accesses);
        }
        _ => {}
    }
}

fn extract_props_from_expr(
    expr: &LogicalExpr,
    accesses: &mut HashMap<String, HashSet<String>>,
) {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            let var = pa.table_alias.0.clone();
            let prop = pa.column.raw().to_string();
            accesses.entry(var).or_default().insert(prop);
        }
        LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                extract_props_from_expr(operand, accesses);
            }
        }
        LogicalExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                extract_props_from_expr(arg, accesses);
            }
        }
        LogicalExpr::AggregateFnCall(f) => {
            for arg in &f.args {
                extract_props_from_expr(arg, accesses);
            }
        }
        LogicalExpr::Case(c) => {
            if let Some(ref e) = c.expr {
                extract_props_from_expr(e, accesses);
            }
            for (w, t) in &c.when_then {
                extract_props_from_expr(w, accesses);
                extract_props_from_expr(t, accesses);
            }
            if let Some(ref e) = c.else_expr {
                extract_props_from_expr(e, accesses);
            }
        }
        LogicalExpr::List(items) => {
            for item in items {
                extract_props_from_expr(item, accesses);
            }
        }
        _ => {}
    }
}

/// Generate all type combinations (cartesian product) for untyped variables
fn generate_type_combinations(
    untyped_vars: &[(String, Vec<String>)],
    max_combinations: usize,
) -> Vec<HashMap<String, String>> {
    if untyped_vars.is_empty() {
        return vec![HashMap::new()];
    }

    let mut combinations = vec![HashMap::new()];

    for (var_name, candidates) in untyped_vars {
        let mut new_combinations = Vec::new();

        for combo in &combinations {
            for candidate in candidates {
                if new_combinations.len() >= max_combinations {
                    log::warn!(
                        "⚠️ Hit max combinations limit ({}), truncating",
                        max_combinations
                    );
                    return new_combinations;
                }

                let mut new_combo = combo.clone();
                new_combo.insert(var_name.clone(), candidate.clone());
                new_combinations.push(new_combo);
            }
        }

        combinations = new_combinations;
    }

    combinations
}

/// Validate a type combination against schema with direction checking
///
/// This is the CRITICAL function that prevents invalid branches.
/// It checks EACH relationship pattern and validates direction against schema.
fn is_valid_combination_with_direction(
    combo: &HashMap<String, String>,
    relationships: &[RelationshipPattern],
    graph_schema: &GraphSchema,
    typed_nodes: &HashMap<String, String>,
) -> bool {
    for rel_pattern in relationships {
        // Get node types from combo (untyped) or typed_nodes (already typed)
        let from_type = match combo.get(&rel_pattern.left_alias) {
            Some(t) => t.as_str(),
            None => match typed_nodes.get(&rel_pattern.left_alias) {
                Some(t) => t.as_str(),
                None => {
                    log::debug!("Unknown alias '{}', skipping validation", rel_pattern.left_alias);
                    continue;
                }
            },
        };

        let to_type = match combo.get(&rel_pattern.right_alias) {
            Some(t) => t.as_str(),
            None => match typed_nodes.get(&rel_pattern.right_alias) {
                Some(t) => t.as_str(),
                None => {
                    log::debug!("Unknown alias '{}', skipping validation", rel_pattern.right_alias);
                    continue;
                }
            },
        };

        // Check if relationship exists with direction validation
        let edge_exists = if rel_pattern.rel_types.is_empty() {
            // Untyped relationship - check if ANY relationship exists
            check_any_relationship_exists_with_direction(
                from_type,
                to_type,
                rel_pattern.direction.clone(),
                graph_schema,
            )
        } else {
            // Typed relationship - check specific type(s) with direction
            rel_pattern.rel_types.iter().any(|rel_type| {
                check_relationship_exists_with_direction(
                    from_type,
                    to_type,
                    rel_type,
                    rel_pattern.direction.clone(),
                    graph_schema,
                )
            })
        };

        if !edge_exists {
            log::debug!(
                "🚫 Invalid combination: {}-[{}:{:?}]->{} (not in schema with direction)",
                from_type,
                if rel_pattern.rel_types.is_empty() {
                    "any".to_string()
                } else {
                    rel_pattern.rel_types.join("|")
                },
                rel_pattern.direction,
                to_type
            );
            return false;
        }
    }

    true
}

/// Check if a plan contains aggregation (GroupBy or aggregate functions in Projection).
fn plan_has_aggregation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GroupBy(_) => true,
        LogicalPlan::Projection(proj) => proj.items.iter().any(|item| {
            matches!(
                &item.expression,
                crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)
            )
        }),
        LogicalPlan::Limit(l) => plan_has_aggregation(&l.input),
        LogicalPlan::Skip(s) => plan_has_aggregation(&s.input),
        LogicalPlan::OrderBy(o) => plan_has_aggregation(&o.input),
        LogicalPlan::GraphJoins(gj) => plan_has_aggregation(&gj.input),
        _ => false,
    }
}

/// Extract the scan part of a plan (everything below aggregation layers).
/// For `Projection(count(n), GroupBy(GJ(GraphNode)))`, returns `GJ(GraphNode)`.
/// For `Projection(count(n), GJ(GraphNode))`, returns `GJ(GraphNode)`.
fn extract_scan_part(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::GroupBy(gb) => (*gb.input).clone(),
        LogicalPlan::Projection(proj) => {
            let has_agg = proj.items.iter().any(|item| {
                matches!(
                    &item.expression,
                    crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)
                )
            });
            if has_agg {
                // Check if input is GroupBy — if so, extract below GroupBy
                if let LogicalPlan::GroupBy(gb) = proj.input.as_ref() {
                    (*gb.input).clone()
                } else {
                    (*proj.input).clone()
                }
            } else {
                (*proj.input).clone()
            }
        }
        LogicalPlan::Limit(l) => extract_scan_part(&l.input),
        LogicalPlan::Skip(s) => extract_scan_part(&s.input),
        LogicalPlan::OrderBy(o) => extract_scan_part(&o.input),
        other => other.clone(),
    }
}

/// Re-wrap aggregation layers from the original plan around a new input (the UNION).
/// For original `Limit(Projection(count(n), GroupBy(GJ(GraphNode))))`:
///   → `Limit(Projection(count(n), GroupBy(union_input)))`
fn rewrap_aggregation(original: &LogicalPlan, new_input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    match original {
        LogicalPlan::Limit(l) => {
            let inner = rewrap_aggregation(&l.input, new_input);
            Arc::new(LogicalPlan::Limit(crate::query_planner::logical_plan::Limit {
                input: inner,
                count: l.count,
            }))
        }
        LogicalPlan::Skip(s) => {
            let inner = rewrap_aggregation(&s.input, new_input);
            Arc::new(LogicalPlan::Skip(crate::query_planner::logical_plan::Skip {
                input: inner,
                count: s.count,
            }))
        }
        LogicalPlan::OrderBy(o) => {
            let inner = rewrap_aggregation(&o.input, new_input);
            Arc::new(LogicalPlan::OrderBy(crate::query_planner::logical_plan::OrderBy {
                input: inner,
                items: o.items.clone(),
            }))
        }
        LogicalPlan::GroupBy(gb) => {
            // GroupBy wraps the new_input directly
            Arc::new(LogicalPlan::GroupBy(crate::query_planner::logical_plan::GroupBy {
                input: new_input,
                expressions: gb.expressions.clone(),
                having_clause: gb.having_clause.clone(),
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            }))
        }
        LogicalPlan::Projection(proj) => {
            let has_agg = proj.items.iter().any(|item| {
                matches!(
                    &item.expression,
                    crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)
                )
            });
            if has_agg {
                // Rewrap inner first (might be GroupBy)
                let inner = rewrap_aggregation(&proj.input, new_input);
                Arc::new(LogicalPlan::Projection(crate::query_planner::logical_plan::Projection {
                    input: inner,
                    items: proj.items.clone(),
                    distinct: proj.distinct,
                    pattern_comprehensions: proj.pattern_comprehensions.clone(),
                }))
            } else {
                // Non-aggregate projection — shouldn't happen but just wrap
                Arc::new(LogicalPlan::Projection(crate::query_planner::logical_plan::Projection {
                    input: new_input,
                    items: proj.items.clone(),
                    distinct: proj.distinct,
                    pattern_comprehensions: proj.pattern_comprehensions.clone(),
                }))
            }
        }
        _ => new_input,
    }
}

/// Clone a LogicalPlan, injecting labels from a type combination into untyped GraphNodes.
///
/// This function recursively traverses the plan tree. When it encounters a GraphNode
/// whose alias appears in the type combination map:
/// - If the node is untyped (label is None), it sets the label from the combination
///   and prunes the Union input to select the matching ViewScan
/// - If already typed, it just recurses
///
/// Used by generate_union_for_untyped_nodes to create properly typed plan branches.
fn clone_plan_with_labels(plan: &LogicalPlan, combo: &HashMap<String, String>) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // If this node variable is in our combination, add the label
            if let Some(label) = combo.get(&node.alias) {
                if node.label.is_none() {
                    // Untyped node - add the label from combination
                    let mut cloned = node.clone();
                    cloned.label = Some(label.clone());
                    // Prune Union input to the ViewScan matching this label
                    cloned.input = Arc::new(prune_union_for_label(&node.input, label, combo));
                    LogicalPlan::GraphNode(cloned)
                } else {
                    // Already typed - just recurse
                    let mut cloned = node.clone();
                    cloned.input = Arc::new(clone_plan_with_labels(&node.input, combo));
                    LogicalPlan::GraphNode(cloned)
                }
            } else {
                // Not in combination - just recurse
                let mut cloned = node.clone();
                cloned.input = Arc::new(clone_plan_with_labels(&node.input, combo));
                LogicalPlan::GraphNode(cloned)
            }
        }

        LogicalPlan::GraphRel(graph_rel) => {
            let mut cloned = graph_rel.clone();
            cloned.left = Arc::new(clone_plan_with_labels(&graph_rel.left, combo));
            cloned.center = Arc::new(clone_plan_with_labels(&graph_rel.center, combo));
            cloned.right = Arc::new(clone_plan_with_labels(&graph_rel.right, combo));
            LogicalPlan::GraphRel(cloned)
        }

        LogicalPlan::Filter(filter) => {
            let mut cloned = filter.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&filter.input, combo));
            LogicalPlan::Filter(cloned)
        }

        LogicalPlan::Projection(proj) => {
            let mut cloned = proj.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&proj.input, combo));
            LogicalPlan::Projection(cloned)
        }

        LogicalPlan::GraphJoins(joins) => {
            let mut cloned = joins.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&joins.input, combo));
            LogicalPlan::GraphJoins(cloned)
        }

        LogicalPlan::Union(union_plan) => {
            let mut cloned = union_plan.clone();
            cloned.inputs = union_plan
                .inputs
                .iter()
                .map(|input| Arc::new(clone_plan_with_labels(input, combo)))
                .collect();
            LogicalPlan::Union(cloned)
        }

        LogicalPlan::GroupBy(group_by) => {
            let mut cloned = group_by.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&group_by.input, combo));
            LogicalPlan::GroupBy(cloned)
        }

        LogicalPlan::OrderBy(order_by) => {
            let mut cloned = order_by.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&order_by.input, combo));
            LogicalPlan::OrderBy(cloned)
        }

        LogicalPlan::Limit(limit) => {
            let mut cloned = limit.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&limit.input, combo));
            LogicalPlan::Limit(cloned)
        }

        LogicalPlan::Skip(skip) => {
            let mut cloned = skip.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&skip.input, combo));
            LogicalPlan::Skip(cloned)
        }

        LogicalPlan::WithClause(with_clause) => {
            let mut cloned = with_clause.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&with_clause.input, combo));
            LogicalPlan::WithClause(cloned)
        }

        LogicalPlan::Unwind(unwind) => {
            let mut cloned = unwind.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&unwind.input, combo));
            LogicalPlan::Unwind(cloned)
        }

        LogicalPlan::CartesianProduct(cart) => {
            let mut cloned = cart.clone();
            cloned.left = Arc::new(clone_plan_with_labels(&cart.left, combo));
            cloned.right = Arc::new(clone_plan_with_labels(&cart.right, combo));
            LogicalPlan::CartesianProduct(cloned)
        }

        LogicalPlan::Cte(cte) => {
            let mut cloned = cte.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&cte.input, combo));
            LogicalPlan::Cte(cloned)
        }

        LogicalPlan::PageRank(pagerank) => {
            // PageRank doesn't have an input field, just clone it
            LogicalPlan::PageRank(pagerank.clone())
        }

        // Base cases that don't need recursion
        LogicalPlan::Empty => LogicalPlan::Empty,
        LogicalPlan::ViewScan(view_scan) => LogicalPlan::ViewScan(view_scan.clone()),
    }
}

/// Prune a Union input to the ViewScan matching the given label.
///
/// When TypeInference assigns a label to an untyped node, the node's input
/// may still be a Union of ViewScans over all node types. This function
/// selects the ViewScan whose source table matches the target label's schema table,
/// effectively "resolving" the polymorphic node to a concrete type.
fn prune_union_for_label(
    input: &LogicalPlan,
    label: &str,
    combo: &HashMap<String, String>,
) -> LogicalPlan {
    if let LogicalPlan::Union(union_plan) = input {
        // Look up the target table for this label
        if let Some(schema) = crate::server::query_context::get_current_schema() {
            if let Some(node_schema) = schema.node_schema_opt(label) {
                let target_table = format!("{}.{}", node_schema.database, node_schema.table_name);
                // Find the ViewScan matching this table
                for vs_input in &union_plan.inputs {
                    if let LogicalPlan::ViewScan(scan) = vs_input.as_ref() {
                        if scan.source_table == target_table {
                            log::debug!(
                                "prune_union_for_label: selected '{}' for label '{}'",
                                target_table,
                                label
                            );
                            return LogicalPlan::ViewScan(scan.clone());
                        }
                    }
                }
            }
        }
        // Fallback: use first ViewScan
        if let Some(first) = union_plan.inputs.first() {
            log::warn!(
                "prune_union_for_label: could not resolve table for label '{}', falling back to first ViewScan",
                label
            );
            return clone_plan_with_labels(first, combo);
        }
    }
    // Not a Union — just recurse
    clone_plan_with_labels(input, combo)
}
