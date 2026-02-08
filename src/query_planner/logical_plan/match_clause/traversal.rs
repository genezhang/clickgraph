use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_expr::{LogicalExpr, Property},
        logical_plan::{
            errors::LogicalPlanError,
            plan_builder::LogicalPlanResult,
            {
                CartesianProduct, GraphNode, GraphRel, LogicalPlan, ShortestPathMode, Union,
                UnionType, VariableLengthSpec,
            },
        },
        plan_ctx::PlanCtx,
    },
};

use crate::query_planner::logical_plan::generate_id;
use std::collections::HashMap;

// Import from sibling modules
use super::helpers::{
    compute_connection_aliases, compute_rel_node_labels, compute_variable_length,
    convert_properties, convert_properties_to_operator_application, determine_optional_anchor,
    generate_denormalization_aware_scan, generate_scan, is_denormalized_scan,
    is_label_denormalized, register_node_in_context, register_path_variable,
    register_relationship_in_context,
};
use super::view_scan::generate_relationship_center;
use crate::query_planner::analyzer::match_type_inference::{
    infer_node_label_from_schema, infer_relationship_type_from_nodes,
};

// Wrapper for backwards compatibility
// Reserved for future use when non-optional traversal needs explicit mode
#[allow(dead_code)]
pub(super) fn traverse_connected_pattern<'a>(
    connected_patterns: &Vec<ast::ConnectedPattern<'a>>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    pathpattern_idx: usize,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    traverse_connected_pattern_with_mode(
        connected_patterns,
        plan,
        plan_ctx,
        pathpattern_idx,
        None,
        None,
        false,
    )
}

fn traverse_connected_pattern_with_mode<'a>(
    connected_patterns: &Vec<ast::ConnectedPattern<'a>>,
    mut plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    pathpattern_idx: usize,
    shortest_path_mode: Option<ShortestPathMode>,
    path_variable: Option<&str>,
    is_optional: bool,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::info!(
        "🔍 TRAVERSE_CONNECTED_PATTERN called with {} patterns, path_variable={:?}",
        connected_patterns.len(),
        path_variable
    );

    crate::debug_print!("\n╔════════════════════════════════════════");
    crate::debug_print!("║ traverse_connected_pattern_with_mode");
    crate::debug_print!("║ connected_patterns.len() = {}", connected_patterns.len());
    crate::debug_print!("║ Current plan type: {:?}", std::mem::discriminant(&*plan));
    crate::debug_print!("╚════════════════════════════════════════\n");

    // === PRE-PROCESS: Assign consistent aliases to shared nodes ===
    // When patterns share nodes via Rc::clone() (e.g., ()-[r1]->()-[r2]->()),
    // we need to ensure the shared node gets the same alias in both patterns.
    // Use pointer equality to detect shared Rc instances.
    // Note: HashMap is already imported at the top of this file.

    // Use usize from Rc::as_ptr() cast as the key for pointer-based identity
    let mut node_alias_map: HashMap<usize, String> = HashMap::new();

    for connected_pattern in connected_patterns.iter() {
        // Check start_node - use address as key
        let start_ptr = connected_pattern.start_node.as_ptr() as usize;
        node_alias_map.entry(start_ptr).or_insert_with(|| {
            let start_node_ref = connected_pattern.start_node.borrow();
            let alias = if let Some(name) = start_node_ref.name {
                name.to_string()
            } else {
                generate_id()
            };
            drop(start_node_ref);
            alias
        });

        // Check end_node - use address as key
        let end_ptr = connected_pattern.end_node.as_ptr() as usize;
        node_alias_map.entry(end_ptr).or_insert_with(|| {
            let end_node_ref = connected_pattern.end_node.borrow();
            let alias = if let Some(name) = end_node_ref.name {
                name.to_string()
            } else {
                generate_id()
            };
            drop(end_node_ref);
            alias
        });
    }

    crate::debug_print!(
        "║ Pre-assigned {} node aliases for shared node detection",
        node_alias_map.len()
    );

    for (pattern_idx, connected_pattern) in connected_patterns.iter().enumerate() {
        crate::debug_print!("┌─ Processing connected_pattern #{}", pattern_idx);

        let start_node_ref = connected_pattern.start_node.borrow();
        let start_node_label_from_ast = start_node_ref.first_label().map(|val| val.to_string());
        // Use pre-assigned alias to ensure shared nodes get the same alias
        let start_node_alias = node_alias_map
            .get(&(connected_pattern.start_node.as_ptr() as usize))
            .cloned()
            .unwrap_or_else(generate_id);

        // CRITICAL FIX: Label resolution order:
        // 1. If AST has explicit label (Some(...)), use it
        // 2. Else if node exists in plan_ctx with label, use that
        // 3. Else None
        // This fixes: MATCH (a)-[:R]->(b:B), (b)-[:S]->(c)
        // where second pattern needs b's label from first pattern (AST returns None after first use)
        let start_node_label = if start_node_label_from_ast.is_some() {
            start_node_label_from_ast
        } else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&start_node_alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                log::info!(
                    ">>> Found existing '{}' in plan_ctx with label: {}",
                    start_node_alias,
                    label
                );
                Some(label)
            } else {
                None
            }
        } else {
            None
        };

        crate::debug_print!(
            "│ Start node: alias='{}', label={:?}",
            start_node_alias,
            start_node_label
        );

        let start_node_props = start_node_ref
            .properties
            .clone()
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e| {
                LogicalPlanError::QueryPlanningError(format!(
                    "Failed to convert start node property: {}",
                    e
                ))
            })?
            .unwrap_or_default();

        // Extract end node info early - needed for filtering anonymous edge types
        let end_node_ref = connected_pattern.end_node.borrow();
        // Use pre-assigned alias to ensure shared nodes get the same alias
        let end_node_alias = node_alias_map
            .get(&(connected_pattern.end_node.as_ptr() as usize))
            .cloned()
            .unwrap_or_else(generate_id);
        let end_node_label_from_ast = end_node_ref.first_label().map(|val| val.to_string());

        // CRITICAL FIX: Same label resolution order as start_node
        let end_node_label = if end_node_label_from_ast.is_some() {
            end_node_label_from_ast
        } else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&end_node_alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                log::info!(
                    ">>> Found existing '{}' in plan_ctx with label: {}",
                    end_node_alias,
                    label
                );
                Some(label)
            } else {
                None
            }
        } else {
            None
        };

        let rel = &connected_pattern.relationship;
        let rel_alias = if let Some(alias) = rel.name {
            alias.to_string()
        } else {
            generate_id()
        };

        // Handle anonymous edge patterns: [] (no type specified)
        // Expand relationship types using composite key index from schema
        // Supports multiple relationships with same type name differentiated by from/to nodes
        let rel_labels = match rel.labels.as_ref() {
            Some(labels) => {
                // Explicit labels provided: [:TYPE1|TYPE2]
                // Look up relationship types using composite key index (O(1) lookup)
                // Filters by node compatibility when node types are known
                let graph_schema = plan_ctx.schema();
                let mut expanded_labels = Vec::new();

                // Get node labels for semantic expansion
                let from_label = start_node_label.as_deref();
                let to_label = end_node_label.as_deref();

                for label in labels.iter() {
                    let variants =
                        graph_schema.expand_generic_relationship_type(label, from_label, to_label);
                    if variants.is_empty() {
                        // No expansion found, use original label (will fail later if truly missing)
                        expanded_labels.push(label.to_string());
                    } else {
                        // Add all expanded variants
                        expanded_labels.extend(variants);
                    }
                }

                // Deduplicate in case of overlapping expansions
                let unique_labels: Vec<String> = {
                    let mut seen = std::collections::HashSet::new();
                    expanded_labels
                        .into_iter()
                        .filter(|l| seen.insert(l.clone()))
                        .collect()
                };

                Some(unique_labels)
            }
            None => {
                // Anonymous edge pattern: [] (no type specified)
                // STEP 1: Check if WHERE clause has property requirements
                let inferred_types = if let Some(required_props) =
                    plan_ctx.get_where_property_requirements(&rel_alias)
                {
                    log::info!(
                        "🔍 Property-based filtering for untyped relationship '{}': required properties {:?}",
                        rel_alias,
                        required_props
                    );

                    // Filter all relationship types by required properties
                    use super::schema_filter::SchemaPropertyFilter;
                    let graph_schema = plan_ctx.schema();
                    let filter = SchemaPropertyFilter::new(graph_schema);
                    let filtered_types = filter.filter_relationship_schemas(required_props);

                    log::info!(
                        "Property-based filtering: {} → {} relationship types for '{}'",
                        graph_schema.get_relationships_schemas().len(),
                        filtered_types.len(),
                        rel_alias
                    );

                    if filtered_types.is_empty() {
                        log::warn!(
                            "No relationship types have required properties {:?}",
                            required_props
                        );
                        None // Will generate empty result
                    } else {
                        Some(filtered_types)
                    }
                } else {
                    // STEP 2: No property requirements - use smart inference
                    // 1. If schema has only one relationship, use it
                    // 2. If nodes are typed, find relationships that match those types
                    // 3. Otherwise, expand to all matching relationship types for UNION
                    let graph_schema = plan_ctx.schema();

                    infer_relationship_type_from_nodes(
                        &start_node_label,
                        &end_node_label,
                        &rel.direction,
                        graph_schema,
                        plan_ctx,
                    )?
                };

                inferred_types
            }
        };

        // === HANDLE NO MATCHING RELATIONSHIP TYPES ===
        // If property filtering removed all relationship types, return Empty plan
        if rel_labels.is_none() {
            log::warn!(
                "🔀 No relationship types found for alias '{}' after property filtering - returning Empty",
                rel_alias
            );
            // Register the relationship alias with empty labels to prevent downstream errors
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                crate::query_planner::plan_ctx::TableCtx::build(
                    rel_alias.clone(),
                    Some(vec![]), // Empty labels
                    vec![],
                    true,
                    false,
                ),
            );
            return Ok(Arc::new(LogicalPlan::Empty));
        }

        // === FULLY UNTYPED MULTI-TYPE UNION EXPANSION ===
        // For patterns like ()-->() where BOTH nodes are untyped AND we have multiple relationship types,
        // generate a UNION where each branch processes as a typed pattern through the normal flow.
        // This ensures each branch gets the exact same treatment as MATCH (a:Type1)-[:REL]->(b:Type2).
        if start_node_label.is_none() && end_node_label.is_none() {
            if let Some(ref types) = rel_labels {
                if types.len() > 1 {
                    log::info!(
                        "🔀 Fully untyped multi-type pattern: {} types, generating UNION",
                        types.len()
                    );

                    // Extract node types for each relationship from schema
                    let mut relationship_node_types: Vec<(String, String, String)> = {
                        let graph_schema = plan_ctx.schema();
                        types
                            .iter()
                            .filter_map(|rel_type| {
                                let rel_schema =
                                    graph_schema.get_relationships_schema_opt(rel_type)?;
                                Some((
                                    rel_type.clone(),
                                    rel_schema.from_node.clone(),
                                    rel_schema.to_node.clone(),
                                ))
                            })
                            .collect()
                    };

                    // === UNION PRUNING OPTIMIZATION ===
                    // If WHERE clause contains id() constraints, use them to prune branches
                    log::info!(
                        "🔍 Checking UNION pruning for start='{}', end='{}'",
                        start_node_alias,
                        end_node_alias
                    );
                    let start_label_constraints =
                        plan_ctx.get_where_label_constraints(&start_node_alias);
                    let end_label_constraints =
                        plan_ctx.get_where_label_constraints(&end_node_alias);
                    log::info!(
                        "  start_constraints: {:?}, end_constraints: {:?}",
                        start_label_constraints,
                        end_label_constraints
                    );

                    if start_label_constraints.is_some() || end_label_constraints.is_some() {
                        let original_count = relationship_node_types.len();

                        relationship_node_types.retain(|(rel_type, from_node, to_node)| {
                            let start_matches = start_label_constraints
                                .map(|labels| labels.contains(from_node))
                                .unwrap_or(true);
                            let end_matches = end_label_constraints
                                .map(|labels| labels.contains(to_node))
                                .unwrap_or(true);

                            let keep = start_matches && end_matches;

                            if !keep {
                                log::info!(
                                    "  ✂️  Pruning branch: {}->{}->{}  (start_match={}, end_match={})",
                                    from_node, rel_type, to_node, start_matches, end_matches
                                );
                            }

                            keep
                        });

                        log::info!(
                            "🎯 UNION pruning: {} → {} branches (removed {})",
                            original_count,
                            relationship_node_types.len(),
                            original_count - relationship_node_types.len()
                        );
                    }

                    if relationship_node_types.is_empty() {
                        return Err(LogicalPlanError::QueryPlanningError(
                            "No valid relationship schemas found for fully untyped pattern"
                                .to_string(),
                        ));
                    }

                    let mut union_branches = Vec::new();

                    // For each relationship type, process as a typed pattern through normal flow
                    for (branch_idx, (rel_type, from_node_type, to_node_type)) in
                        relationship_node_types.iter().enumerate()
                    {
                        log::info!(
                            "  UNION branch: (:{from_node})-[:{rel_type}]->(:{to_node})",
                            from_node = from_node_type,
                            rel_type = rel_type,
                            to_node = to_node_type
                        );

                        // Generate UNIQUE aliases for this branch to avoid conflicts in plan_ctx
                        // Each branch has its own node aliases, but shares the path variable name
                        let branch_start_alias = format!("{}_{}", start_node_alias, branch_idx);
                        let branch_end_alias = format!("{}_{}", end_node_alias, branch_idx);

                        // Temporarily override labels to process through normal flow
                        let branch_start_label = Some(from_node_type.clone());
                        let branch_end_label = Some(to_node_type.clone());
                        let branch_rel_labels = Some(vec![rel_type.clone()]);

                        log::debug!(
                            "🔍 UNION branch {}: start='{}', end='{}', rel='{}', labels=({:?}, {:?})",
                            branch_idx,
                            &branch_start_alias,
                            &branch_end_alias,
                            &rel_alias,
                            &branch_start_label,
                            &branch_end_label
                        );

                        // Process this typed pattern through the STANDARD flow below
                        // by duplicating the "disconnected pattern" logic for each branch

                        // Generate scans for typed nodes
                        let (start_scan, start_is_denorm) =
                            if is_label_denormalized(&branch_start_label, plan_ctx) {
                                (Arc::new(LogicalPlan::Empty), true)
                            } else {
                                let scan = generate_scan(
                                    branch_start_alias.clone(),
                                    branch_start_label.clone(),
                                    plan_ctx,
                                )?;
                                let is_d = is_denormalized_scan(&scan);
                                (scan, is_d)
                            };

                        let start_graph_node = GraphNode {
                            input: start_scan,
                            alias: branch_start_alias.clone(),
                            label: branch_start_label.clone(),
                            is_denormalized: start_is_denorm,
                            projected_columns: None,
                        };

                        let (end_scan, end_is_denorm) = generate_denormalization_aware_scan(
                            &branch_end_alias,
                            &branch_end_label,
                            plan_ctx,
                        )?;

                        let end_graph_node = GraphNode {
                            input: end_scan,
                            alias: branch_end_alias.clone(),
                            label: branch_end_label.clone(),
                            is_denormalized: end_is_denorm,
                            projected_columns: None,
                        };

                        let (left_conn, right_conn) = compute_connection_aliases(
                            &rel.direction,
                            &branch_start_alias,
                            &branch_end_alias,
                        );

                        let (left_node_label_for_rel, right_node_label_for_rel) =
                            compute_rel_node_labels(
                                &rel.direction,
                                &branch_start_label,
                                &branch_end_label,
                            );

                        let (left_node, right_node) = match rel.direction {
                            ast::Direction::Outgoing => (
                                Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                                Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                            ),
                            ast::Direction::Incoming => (
                                Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                                Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                            ),
                            ast::Direction::Either => (
                                Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                                Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                            ),
                        };

                        log::debug!(
                            "🔍 Creating GraphRel for branch {}: rel_alias='{}', rel_type='{}', path_variable={:?}",
                            union_branches.len(),
                            &rel_alias,
                            rel_type,
                            path_variable
                        );

                        let graph_rel_node = GraphRel {
                            left: left_node,
                            center: generate_relationship_center(
                                &rel_alias,
                                &branch_rel_labels,
                                &left_conn,
                                &right_conn,
                                &left_node_label_for_rel,
                                &right_node_label_for_rel,
                                plan_ctx,
                            )?,
                            right: right_node,
                            alias: rel_alias.clone(),
                            direction: rel.direction.clone().into(),
                            left_connection: left_conn.clone(),
                            right_connection: right_conn.clone(),
                            is_rel_anchor: false,
                            variable_length: None, // Single-hop pattern
                            shortest_path_mode: shortest_path_mode.clone(),
                            path_variable: path_variable.map(|s| s.to_string()),
                            where_predicate: None,
                            labels: branch_rel_labels.clone(),
                            is_optional: if is_optional { Some(true) } else { None },
                            anchor_connection: None,
                            cte_references: std::collections::HashMap::new(),
                        };

                        log::warn!(
                            "🔀 Created GraphRel for UNION branch {}: alias='{}', path_variable={:?}",
                            union_branches.len(),
                            &graph_rel_node.alias,
                            &graph_rel_node.path_variable
                        );

                        // Register branch-specific node aliases in plan_ctx
                        // This is critical for property expansion to work!
                        register_node_in_context(
                            plan_ctx,
                            &left_conn,
                            &Some(from_node_type.clone()),
                            vec![], // Properties not used for UNION branches
                            true,   // has_name
                        );
                        register_node_in_context(
                            plan_ctx,
                            &right_conn,
                            &Some(to_node_type.clone()),
                            vec![], // Properties not used for UNION branches
                            true,   // has_name
                        );

                        log::info!(
                            "📝 Registered branch {} node aliases: left='{}' ({}), right='{}' ({})",
                            union_branches.len(),
                            left_conn,
                            from_node_type,
                            right_conn,
                            to_node_type
                        );

                        // SIMPLE: Don't wrap in Projection - let GraphRel handle everything
                        // GraphRel already emits all node/rel properties in its SELECT
                        // We just need it to also emit the path tuple (done in select_builder.rs)
                        union_branches.push(Arc::new(LogicalPlan::GraphRel(graph_rel_node)));
                    }

                    // Create Union of all branches
                    let union_plan = LogicalPlan::Union(Union {
                        inputs: union_branches.clone(), // Clone for registration
                        union_type: UnionType::All,
                    });

                    // Register the ORIGINAL node aliases (for path variable compatibility)
                    // These point to the first branch's nodes
                    register_node_in_context(
                        plan_ctx,
                        &start_node_alias,
                        &None,  // No specific label for the generic alias
                        vec![], // Properties not used for UNION branches
                        start_node_ref.name.is_some(),
                    );
                    register_node_in_context(
                        plan_ctx,
                        &end_node_alias,
                        &None,
                        vec![], // Properties not used for UNION branches
                        end_node_ref.name.is_some(),
                    );

                    // Register EACH branch's node aliases with CORRECT typed labels
                    for (branch_idx, branch) in union_branches.iter().enumerate() {
                        if let LogicalPlan::GraphRel(ref graph_rel) = **branch {
                            // Extract node labels from the branch
                            if let LogicalPlan::GraphNode(ref left_node) = *graph_rel.left {
                                register_node_in_context(
                                    plan_ctx,
                                    &left_node.alias,
                                    &left_node.label,
                                    vec![],
                                    true,
                                );
                            }
                            if let LogicalPlan::GraphNode(ref right_node) = *graph_rel.right {
                                register_node_in_context(
                                    plan_ctx,
                                    &right_node.alias,
                                    &right_node.label,
                                    vec![],
                                    true,
                                );
                            }

                            // Register this branch's relationship alias
                            log::debug!(
                                "Registering UNION branch {}: rel_alias='{}', labels={:?}",
                                branch_idx,
                                graph_rel.alias,
                                graph_rel.labels
                            );
                            plan_ctx.insert_table_ctx(
                                graph_rel.alias.clone(),
                                crate::query_planner::plan_ctx::TableCtx::build(
                                    graph_rel.alias.clone(),
                                    graph_rel.labels.clone(),
                                    vec![],
                                    true,
                                    false,
                                ),
                            );
                        }
                    }

                    // Register the ORIGINAL relationship alias (for path variable)
                    let all_rel_types: Vec<String> = relationship_node_types
                        .iter()
                        .map(|(rel_type, _, _)| rel_type.clone())
                        .collect();

                    plan_ctx.insert_table_ctx(
                        rel_alias.clone(),
                        crate::query_planner::plan_ctx::TableCtx::build(
                            rel_alias.clone(),
                            Some(all_rel_types.clone()),
                            vec![],
                            true,
                            false,
                        ),
                    );

                    // Register path variable if present
                    // For UNION patterns: registered with original aliases, but expansion uses per-branch aliases from GraphRel
                    if let Some(pvar) = path_variable {
                        log::warn!("📍 Registering UNION path variable '{}'", pvar);

                        // Register path - aliases don't matter much since expansion will use GraphRel's actual aliases
                        plan_ctx.define_path(
                            pvar.to_string(),
                            Some(start_node_alias.clone()),
                            Some(end_node_alias.clone()),
                            Some(rel_alias.clone()),
                            None, // No length bounds for single-hop
                            shortest_path_mode.is_some(),
                        );

                        // Register TableCtx for ProjectionTagging
                        plan_ctx.insert_table_ctx(
                            pvar.to_string(),
                            crate::query_planner::plan_ctx::TableCtx::build(
                                pvar.to_string(),
                                None,
                                vec![],
                                false,
                                true,
                            ),
                        );

                        log::warn!(
                            "✅ Registered UNION path variable '{}' (expansion will use per-branch GraphRel aliases)",
                            pvar
                        );
                    }

                    plan = Arc::new(union_plan);

                    log::info!(
                        "✅ Created UNION with {} branches for fully untyped pattern",
                        relationship_node_types.len()
                    );

                    // Skip the normal processing below
                    continue;
                }
            }
        }

        // === LABEL INFERENCE ===
        // NOTE: Label and edge type inference is now handled by the TypeInference analyzer pass
        // which runs after parsing. This provides more robust inference that works across
        // WITH boundaries and handles both node labels AND edge types.
        // The labels in start_node_label/end_node_label come from AST parsing or will be
        // inferred by TypeInference pass.

        log::debug!(
            "Pattern processing: start='{}' ({}), end='{}' ({})",
            start_node_alias,
            start_node_label.as_deref().unwrap_or("None"),
            end_node_alias,
            end_node_label.as_deref().unwrap_or("None")
        );

        // Polymorphic inference removed - TypeInference pass handles this
        // (start_possible_labels and end_possible_labels were used for UNION generation)

        crate::debug_print!(
            "│ Relationship: alias='{}', labels={:?}, direction={:?}",
            rel_alias,
            rel_labels,
            rel.direction
        );
        crate::debug_print!(
            "│ After inference: start_label={:?}, end_label={:?}",
            start_node_label,
            end_node_label
        );

        log::debug!("Parsed relationship labels: {:?}", rel_labels);
        let rel_properties = rel
            .properties
            .clone()
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e| {
                LogicalPlanError::QueryPlanningError(format!(
                    "Failed to convert relationship property: {}",
                    e
                ))
            })?
            .unwrap_or_default();

        crate::debug_print!(
            "│ End node: alias='{}', label={:?}",
            end_node_alias,
            end_node_label
        );

        let end_node_props = end_node_ref
            .properties
            .clone()
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e| {
                LogicalPlanError::QueryPlanningError(format!(
                    "Failed to convert end node property: {}",
                    e
                ))
            })?
            .unwrap_or_default();

        // if start alias already present in ctx map, it means the current nested connected pattern's start node will be connecting at right side plan and end node will be at the left
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&start_node_alias) {
            if start_node_label.is_some() {
                table_ctx.set_labels(start_node_label.clone().map(|l| vec![l]));
            }
            if !start_node_props.is_empty() {
                table_ctx.append_properties(start_node_props);
            }

            register_node_in_context(
                plan_ctx,
                &end_node_alias,
                &end_node_label,
                end_node_props,
                end_node_ref.name.is_some(),
            );

            let (left_conn, right_conn) =
                compute_connection_aliases(&rel.direction, &start_node_alias, &end_node_alias);

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) =
                compute_rel_node_labels(&rel.direction, &start_node_label, &end_node_label);

            // FIX: For multi-hop patterns, use the existing plan as LEFT to create nested structure
            // This ensures (a)-[r1]->(b)-[r2]->(c) becomes GraphRel { left: GraphRel(a-r1-b), center: r2, right: c }
            let (left_node, right_node) = match rel.direction {
                ast::Direction::Outgoing => {
                    // (a)-[:r1]->(b)-[:r2]->(c): existing plan (a-r1-b) on left, new node (c) on right
                    let (scan, is_denorm) = generate_denormalization_aware_scan(
                        &end_node_alias,
                        &end_node_label,
                        plan_ctx,
                    )?;

                    (
                        plan.clone(),
                        Arc::new(LogicalPlan::GraphNode(GraphNode {
                            input: scan,
                            alias: end_node_alias.clone(),
                            label: end_node_label.clone().map(|s| s.to_string()),
                            is_denormalized: is_denorm,
                            projected_columns: None,
                        })),
                    )
                }
                ast::Direction::Incoming => {
                    // (c)<-[:r2]-(b)<-[:r1]-(a): new node (c) on left, existing plan (b-r1-a) on right
                    let (scan, is_denorm) = generate_denormalization_aware_scan(
                        &end_node_alias,
                        &end_node_label,
                        plan_ctx,
                    )?;

                    (
                        Arc::new(LogicalPlan::GraphNode(GraphNode {
                            input: scan,
                            alias: end_node_alias.clone(),
                            label: end_node_label.clone().map(|s| s.to_string()),
                            is_denormalized: is_denorm,
                            projected_columns: None,
                        })),
                        plan.clone(),
                    )
                }
                ast::Direction::Either => {
                    // Either direction: existing plan on left, new node on right
                    let (scan, is_denorm) = generate_denormalization_aware_scan(
                        &end_node_alias,
                        &end_node_label,
                        plan_ctx,
                    )?;

                    (
                        plan.clone(),
                        Arc::new(LogicalPlan::GraphNode(GraphNode {
                            input: scan,
                            alias: end_node_alias.clone(),
                            label: end_node_label.clone().map(|s| s.to_string()),
                            is_denormalized: is_denorm,
                            projected_columns: None,
                        })),
                    )
                }
            };

            // Determine anchor_connection for OPTIONAL MATCH
            let anchor_connection =
                determine_optional_anchor(plan_ctx, is_optional, &left_conn, &right_conn);

            // Handle variable-length patterns and multi-type relationships:
            // - Single-type *1: (a)-[:TYPE*1]->(b) → simplify to regular relationship
            // - Multi-type *1: (a)-[:TYPE1|TYPE2*1]->(b) → keep VLP for polymorphic nodes
            // - Multi-type no VLP: (a)-[:TYPE1|TYPE2]->(b) → ADD implicit *1 for polymorphic handling
            let is_multi_type = rel_labels.as_ref().is_some_and(|labels| labels.len() > 1);

            let variable_length = if let Some(vlp) = rel.variable_length.clone() {
                // Has explicit VLP spec
                let spec: VariableLengthSpec = vlp.into();
                let is_exact_one_hop = spec.min_hops == Some(1) && spec.max_hops == Some(1);

                if is_exact_one_hop && !is_multi_type {
                    log::info!("Simplifying *1 single-type pattern to regular relationship");
                    None // Remove *1 for single-type - treat as regular relationship
                } else {
                    Some(spec) // Keep VLP for multi-type or ranges
                }
            } else if is_multi_type {
                // Multi-type without VLP: add implicit *1 for proper polymorphic handling
                log::info!("Adding implicit *1 for multi-type relationship (polymorphic end node)");
                Some(VariableLengthSpec {
                    min_hops: Some(1),
                    max_hops: Some(1),
                })
            } else {
                None // Single-type, no VLP
            };

            let graph_rel_node = GraphRel {
                left: left_node,
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &left_conn,
                    &right_conn,
                    &left_node_label_for_rel,
                    &right_node_label_for_rel,
                    plan_ctx,
                )?,
                right: right_node,
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: left_conn,
                right_connection: right_conn,
                is_rel_anchor: false,
                variable_length,
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: None, // Will be populated by filter pushdown optimization
                labels: rel_labels.clone(),
                is_optional: if is_optional { Some(true) } else { None },
                anchor_connection,
                cte_references: std::collections::HashMap::new(),
            };

            // Register relationship and path variable in context
            register_relationship_in_context(
                plan_ctx,
                &rel_alias,
                rel_labels,
                rel_properties,
                rel.name.is_some(),
                &left_node_label_for_rel,
                &right_node_label_for_rel,
                &graph_rel_node,
                path_variable,
                shortest_path_mode.as_ref(),
            );

            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

            crate::debug_print!("│ ✓ Created GraphRel (start node already in context)");
            crate::debug_print!("│   Plan is now: GraphRel");
            crate::debug_print!("└─ Pattern #{} complete\n", pattern_idx);
        }
        // if end alias already present in ctx map, it means the current nested connected pattern's end node will be connecting at right side plan and start node will be at the left
        else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&end_node_alias) {
            log::info!(
                ">>> Found existing TableCtx for '{}', updating with label: {:?}",
                end_node_alias,
                end_node_label
            );
            if let Some(ref label) = end_node_label {
                table_ctx.set_labels(end_node_label.clone().map(|l| vec![l]));
                log::info!(">>> Updated '{}' with label: {}", end_node_alias, label);
            } else {
                log::warn!(
                    ">>> end_node_label is None for '{}', cannot update TableCtx!",
                    end_node_alias
                );
            }
            if !end_node_props.is_empty() {
                table_ctx.append_properties(end_node_props);
            }

            let (start_scan, start_is_denorm) = generate_denormalization_aware_scan(
                &start_node_alias,
                &start_node_label,
                plan_ctx,
            )?;

            let start_graph_node = GraphNode {
                input: start_scan,
                alias: start_node_alias.clone(),
                label: start_node_label.clone().map(|s| s.to_string()),
                is_denormalized: start_is_denorm,
                projected_columns: None,
            };
            register_node_in_context(
                plan_ctx,
                &start_node_alias,
                &start_node_label,
                start_node_props,
                start_node_ref.name.is_some(),
            );

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) =
                compute_rel_node_labels(&rel.direction, &start_node_label, &end_node_label);

            let graph_rel_node = GraphRel {
                left: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &start_node_alias,
                    &end_node_alias,
                    &start_node_label,
                    &end_node_label,
                    plan_ctx,
                )?,
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: start_node_alias.clone(),
                right_connection: end_node_alias.clone(),
                is_rel_anchor: false,
                variable_length: compute_variable_length(rel, &rel_labels),
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: None, // Will be populated by filter pushdown optimization
                labels: rel_labels.clone(),
                is_optional: if plan_ctx.is_optional_match_mode() {
                    log::warn!(
                        "CREATING GraphRel with is_optional=Some(true), mode={}",
                        plan_ctx.is_optional_match_mode()
                    );
                    Some(true)
                } else {
                    log::warn!(
                        "CREATING GraphRel with is_optional=None, mode={}",
                        plan_ctx.is_optional_match_mode()
                    );
                    None
                },
                // For anchor traversals, the right connection (end_node) is the anchor from base MATCH
                // The left connection (start_node) is newly introduced
                anchor_connection: if plan_ctx.is_optional_match_mode() {
                    Some(end_node_alias.clone())
                } else {
                    None
                },
                cte_references: std::collections::HashMap::new(),
            };

            // Register relationship and path variable in context
            register_relationship_in_context(
                plan_ctx,
                &rel_alias,
                rel_labels,
                rel_properties,
                rel.name.is_some(),
                &left_node_label_for_rel,
                &right_node_label_for_rel,
                &graph_rel_node,
                path_variable,
                shortest_path_mode.as_ref(),
            );

            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

            crate::debug_print!("│ ✓ Created GraphRel (end node already in context)");
            crate::debug_print!("│   Plan is now: GraphRel");
            crate::debug_print!("└─ Pattern #{} complete\n", pattern_idx);
        }
        // not connected with existing nodes
        else {
            // if two comma separated patterns found and they are not connected to each other i.e. there is no common node alias between them
            // Allow this - it will create a CartesianProduct.
            // If WHERE clause has predicates connecting them (e.g., srcip1.ip = srcip2.ip), those will be processed later
            // and can be converted to proper JOINs by optimizer passes.
            if pathpattern_idx > 0 {
                log::info!(
                    "Disconnected comma pattern detected at index {}. Creating CartesianProduct. WHERE clause may contain connecting predicates.",
                    pathpattern_idx
                );
            }

            crate::debug_print!("=== CHECKING EXISTING PLAN ===");
            crate::debug_print!(
                "=== plan discriminant: {:?} ===",
                std::mem::discriminant(&*plan)
            );

            // Check if we have a non-empty input plan (e.g., from WITH clause or previous MATCH)
            // If so, we need to create a CartesianProduct to join the previous plan with this new pattern
            let has_existing_plan = !matches!(plan.as_ref(), LogicalPlan::Empty);

            crate::debug_print!("=== has_existing_plan: {} ===", has_existing_plan);

            if has_existing_plan {
                crate::debug_print!(
                    "=== DISCONNECTED PATTERN WITH EXISTING PLAN: Creating CartesianProduct ==="
                );
                crate::debug_print!(
                    "=== Existing plan type: {:?} ===",
                    std::mem::discriminant(&*plan)
                );
            }

            // we will keep start graph node at the right side and end at the left side
            crate::debug_print!("=== DISCONNECTED PATTERN: About to create start_graph_node ===");

            let (start_scan, start_is_denorm) =
                if is_label_denormalized(&start_node_label, plan_ctx) {
                    crate::debug_print!(
                        "=== Start node '{}' is DENORMALIZED, creating Empty scan ===",
                        start_node_alias
                    );
                    (Arc::new(LogicalPlan::Empty), true)
                } else {
                    let scan = generate_scan(
                        start_node_alias.clone(),
                        start_node_label.clone(),
                        plan_ctx,
                    )?;
                    crate::debug_print!(
                        "=== DISCONNECTED: start_scan created, calling is_denormalized_scan ==="
                    );
                    let is_d = is_denormalized_scan(&scan);
                    crate::debug_print!("=== DISCONNECTED: start_is_denorm = {} ===", is_d);
                    (scan, is_d)
                };

            let start_graph_node = GraphNode {
                input: start_scan,
                alias: start_node_alias.clone(),
                label: start_node_label.clone().map(|s| s.to_string()),
                is_denormalized: start_is_denorm,
                projected_columns: None,
            };
            crate::debug_print!(
                "=== DISCONNECTED: start_graph_node created with is_denormalized={} ===",
                start_graph_node.is_denormalized
            );
            register_node_in_context(
                plan_ctx,
                &start_node_alias,
                &start_node_label,
                start_node_props,
                start_node_ref.name.is_some(),
            );

            let (end_scan, end_is_denorm) =
                generate_denormalization_aware_scan(&end_node_alias, &end_node_label, plan_ctx)?;

            let end_graph_node = GraphNode {
                input: end_scan,
                alias: end_node_alias.clone(),
                label: end_node_label.clone().map(|s| s.to_string()),
                is_denormalized: end_is_denorm,
                projected_columns: None,
            };
            register_node_in_context(
                plan_ctx,
                &end_node_alias,
                &end_node_label,
                end_node_props,
                end_node_ref.name.is_some(),
            );

            let (left_conn, right_conn) =
                compute_connection_aliases(&rel.direction, &start_node_alias, &end_node_alias);

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) =
                compute_rel_node_labels(&rel.direction, &start_node_label, &end_node_label);

            let (left_node, right_node) = match rel.direction {
                ast::Direction::Outgoing => (
                    Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                    Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                ),
                ast::Direction::Incoming => (
                    Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                    Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                ),
                ast::Direction::Either => (
                    Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                    Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                ),
            };

            // Determine anchor_connection for OPTIONAL MATCH
            let anchor_connection =
                determine_optional_anchor(plan_ctx, is_optional, &left_conn, &right_conn);

            let graph_rel_node = GraphRel {
                left: left_node,
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &left_conn,
                    &right_conn,
                    &left_node_label_for_rel,
                    &right_node_label_for_rel,
                    plan_ctx,
                )?,
                right: right_node,
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: left_conn.clone(), // Left node is the start node (left_conn for Outgoing)
                right_connection: right_conn.clone(), // Right node is the end node (right_conn for Outgoing)
                is_rel_anchor: false,
                variable_length: compute_variable_length(rel, &rel_labels),
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: {
                    // 🔧 FIX: For VLP patterns (including shortestPath), extract filters/properties from bound nodes
                    // When nodes like (p1:Airport {code: 'LAX'}) are used with VLP patterns, their filters
                    // are in plan_ctx but not automatically merged into GraphRel.where_predicate
                    // This is needed for VLP CTE generation to apply correct filters with property mapping
                    if shortest_path_mode.is_some() || rel.variable_length.is_some() {
                        use crate::query_planner::logical_expr::{Operator, OperatorApplication};
                        let mut node_filters = vec![];

                        // Extract filters/properties for left node
                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&left_conn) {
                            // Get both existing filters AND unconverted properties
                            node_filters.extend(table_ctx.get_filters().iter().cloned());

                            // Convert any remaining properties to filters
                            let props = table_ctx.get_and_clear_properties();
                            if !props.is_empty() {
                                match convert_properties(props, &left_conn) {
                                    Ok(mut prop_filters) => {
                                        log::info!(
                                            "🔧 VLP: Converted {} properties to filters for left node '{}'",
                                            prop_filters.len(),
                                            left_conn
                                        );
                                        node_filters.append(&mut prop_filters);
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Failed to convert properties for left node '{}': {:?}",
                                            left_conn,
                                            e
                                        );
                                    }
                                }
                            }
                        }

                        // Extract filters/properties for right node
                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&right_conn) {
                            // Get both existing filters AND unconverted properties
                            node_filters.extend(table_ctx.get_filters().iter().cloned());

                            // Convert any remaining properties to filters
                            let props = table_ctx.get_and_clear_properties();
                            if !props.is_empty() {
                                match convert_properties(props, &right_conn) {
                                    Ok(mut prop_filters) => {
                                        log::info!(
                                            "🔧 VLP: Converted {} properties to filters for right node '{}'",
                                            prop_filters.len(),
                                            right_conn
                                        );
                                        node_filters.append(&mut prop_filters);
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Failed to convert properties for right node '{}': {:?}",
                                            right_conn,
                                            e
                                        );
                                    }
                                }
                            }
                        }

                        // Combine all filters with AND
                        node_filters
                            .into_iter()
                            .reduce(|acc, filter| {
                                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, filter],
                                })
                            })
                            .inspect(|_combined| {
                                log::info!(
                                    "🔧 VLP: Merged {} bound node filters into where_predicate for rel '{}'",
                                    "multiple",
                                    rel_alias
                                );
                            })
                    } else {
                        None // Will be populated by filter pushdown optimization for regular patterns
                    }
                },
                labels: rel_labels.clone(),
                is_optional: if is_optional { Some(true) } else { None },
                anchor_connection,
                cte_references: std::collections::HashMap::new(),
            };

            // Register relationship and path variable in context
            register_relationship_in_context(
                plan_ctx,
                &rel_alias,
                rel_labels,
                rel_properties,
                rel.name.is_some(),
                &left_node_label_for_rel,
                &right_node_label_for_rel,
                &graph_rel_node,
                path_variable,
                shortest_path_mode.as_ref(),
            );

            // Create the GraphRel for this pattern
            let new_pattern = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

            // If we have an existing plan (e.g., from WITH clause), combine with CartesianProduct
            if has_existing_plan {
                // CRITICAL FIX: When existing plan is OPTIONAL and new pattern is REQUIRED,
                // swap them so the required pattern becomes the anchor (FROM clause).
                // This ensures correct SQL generation:
                //   OPTIONAL MATCH ... MATCH x → FROM x LEFT JOIN optional_pattern
                // Instead of wrong:
                //   FROM optional_pattern CROSS JOIN x
                let existing_is_optional = plan.is_optional_pattern();
                let (left, right, cp_is_optional) = if existing_is_optional && !is_optional {
                    // Swap: required pattern becomes left (anchor), optional becomes right
                    log::info!(
                        "🔄 CartesianProduct: Swapping left/right - existing plan is optional, new pattern is required"
                    );
                    (new_pattern.clone(), plan.clone(), true) // is_optional=true means RIGHT is optional
                } else {
                    // Normal case: existing plan is anchor
                    (plan.clone(), new_pattern.clone(), is_optional)
                };

                plan = Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                    left,
                    right,
                    is_optional: cp_is_optional,
                    join_condition: None, // Will be populated by optimizer if WHERE bridges both sides
                }));
                crate::debug_print!(
                    "│ ✓ Created CartesianProduct (combining existing plan with new pattern)"
                );
                crate::debug_print!(
                    "│   Plan is now: CartesianProduct(optional: {})",
                    cp_is_optional
                );
            } else {
                plan = new_pattern;
                crate::debug_print!("│ ✓ Created GraphRel (first pattern - disconnected)");
                crate::debug_print!("│   Plan is now: GraphRel");
            }
            crate::debug_print!("└─ Pattern #{} complete\n", pattern_idx);
        }
    }

    crate::debug_print!("╔════════════════════════════════════════");
    crate::debug_print!("║ traverse_connected_pattern_with_mode COMPLETE");
    crate::debug_print!("║ Final plan type: {:?}", std::mem::discriminant(&*plan));
    crate::debug_print!("╚════════════════════════════════════════\n");

    Ok(plan)
}

pub(super) fn traverse_node_pattern(
    node_pattern: &ast::NodePattern,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    // Generate anonymous alias for nodes without names
    // This supports Neo4j Browser "dot" feature: MATCH () RETURN *
    let node_alias = node_pattern
        .name
        .map(|n| n.to_string())
        .unwrap_or_else(generate_id);
    let mut node_label: Option<String> = node_pattern.first_label().map(|val| val.to_string());

    // === SINGLE-NODE-SCHEMA INFERENCE ===
    // If no label provided and schema has only one node type, use it
    if node_label.is_none() {
        if let Ok(Some(inferred_label)) = infer_node_label_from_schema(plan_ctx.schema(), plan_ctx)
        {
            log::info!(
                "Node '{}' label inferred as '{}' (single node type in schema)",
                node_alias,
                inferred_label
            );
            node_label = Some(inferred_label);
        }
    }

    let node_props: Vec<Property> = node_pattern
        .properties
        .clone()
        .map(|props| {
            props
                .into_iter()
                .map(Property::try_from)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    // if alias already present in ctx map then just add its conditions and do not add it in the logical plan
    if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&node_alias) {
        if node_label.is_some() {
            table_ctx.set_labels(node_label.map(|l| vec![l]));
        }
        if !node_props.is_empty() {
            table_ctx.append_properties(node_props);
        }
        Ok(plan)
    } else {
        // Register the node in the context
        register_node_in_context(
            plan_ctx,
            &node_alias,
            &node_label,
            node_props,
            node_pattern.name.is_some(),
        );

        let scan = generate_scan(node_alias.clone(), node_label.clone(), plan_ctx)?;

        // Check if this is a Union (denormalized node with BOTH positions)
        // In that case, wrap EACH branch in its own GraphNode, then return the Union
        if let LogicalPlan::Union(union) = scan.as_ref() {
            log::info!(
                "✓ Wrapping Union branches in GraphNodes for alias '{}'",
                node_alias
            );
            let wrapped_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .map(|branch| {
                    let is_denorm = is_denormalized_scan(branch);
                    // For UNION branches from untyped patterns (MATCH (n)), extract the label
                    // from the ViewScan source_table to enable property mapping in FilterTagging.
                    // The ViewScan was created by generate_scan with a specific node type.
                    let branch_label = if node_label.is_none() {
                        // Extract label from ViewScan's source_table
                        // The source_table format is "database.table_name" e.g. "brahmand.users_bench"
                        if let LogicalPlan::ViewScan(vs) = branch.as_ref() {
                            // Try to find the node label by looking up which node type uses this table
                            let table_name = vs.source_table.split('.').last().unwrap_or(&vs.source_table);
                            plan_ctx.schema().all_node_schemas()
                                .iter()
                                .find_map(|(label, schema)| {
                                    // Check if this schema's table matches
                                    let schema_table = schema.table_name.split('.').last().unwrap_or(&schema.table_name);
                                    if schema_table == table_name {
                                        Some(label.clone())
                                    } else {
                                        None
                                    }
                                })
                        } else {
                            None
                        }
                    } else {
                        node_label.clone().map(|s| s.to_string())
                    };

                    log::info!(
                        "  ✓ Wrapping branch with alias='{}', label={:?} (original node_label={:?})",
                        node_alias, branch_label, node_label
                    );

                    Arc::new(LogicalPlan::GraphNode(GraphNode {
                        input: branch.clone(),
                        alias: node_alias.clone(),
                        label: branch_label,
                        is_denormalized: is_denorm,
                        projected_columns: None,
                    }))
                })
                .collect();

            let wrapped_union = Union {
                inputs: wrapped_inputs,
                union_type: union.union_type.clone(),
            };
            log::info!(
                "✓✓✓ WRAPPING UNION: {} branches being wrapped in GraphNodes ✓✓✓",
                wrapped_union.inputs.len()
            );
            return Ok(Arc::new(LogicalPlan::Union(wrapped_union)));
        }

        // Normal case: single ViewScan wrapped in GraphNode
        let is_denorm = is_denormalized_scan(&scan);
        let new_node_alias = node_alias.clone(); // Clone for logging
        let graph_node = GraphNode {
            input: scan,
            alias: node_alias,
            label: node_label.map(|s| s.to_string()),
            is_denormalized: is_denorm,
            projected_columns: None,
        };
        let new_node_plan = Arc::new(LogicalPlan::GraphNode(graph_node));

        // Check if we need to create a CartesianProduct
        // For comma patterns like (a:User), (b:User), we need CROSS JOIN
        let has_existing_plan = !matches!(plan.as_ref(), LogicalPlan::Empty);

        if has_existing_plan {
            // CRITICAL FIX: When existing plan is OPTIONAL and new node is from REQUIRED MATCH,
            // swap them so the required node becomes the anchor (FROM clause).
            let existing_is_optional = plan.is_optional_pattern();
            let (left, right, cp_is_optional) = if existing_is_optional {
                // Swap: required node becomes left (anchor), optional becomes right
                log::info!(
                    "🔄 CartesianProduct (node): Swapping - existing plan is optional, node '{}' is required",
                    new_node_alias
                );
                (new_node_plan.clone(), plan.clone(), true) // is_optional=true means RIGHT is optional
            } else {
                // Normal case: existing plan is anchor
                (plan.clone(), new_node_plan.clone(), false)
            };

            log::info!(
                "Creating CartesianProduct for comma pattern: existing plan + node '{}'",
                new_node_alias
            );
            Ok(Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                left,
                right,
                is_optional: cp_is_optional,
                join_condition: None,
            })))
        } else {
            Ok(new_node_plan)
        }
    }
}

pub fn evaluate_match_clause<'a>(
    match_clause: &ast::MatchClause<'a>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    evaluate_match_clause_with_optional(match_clause, plan, plan_ctx, false)
}

/// Internal function that supports optional mode
pub fn evaluate_match_clause_with_optional<'a>(
    match_clause: &ast::MatchClause<'a>,
    mut plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    is_optional: bool,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::info!(
        "🔍 EVALUATE_MATCH_CLAUSE: {} path patterns",
        match_clause.path_patterns.len()
    );

    // Extract property requirements from WHERE clause BEFORE pattern traversal
    // This enables property-based optimization (pruning UNION branches)
    if let Some(where_clause) = &match_clause.where_clause {
        use crate::query_planner::analyzer::where_property_extractor::WherePropertyExtractor;
        let required_properties = WherePropertyExtractor::extract_property_references(where_clause);

        log::debug!(
            "Extracted {} property requirements from WHERE clause: {:?}",
            required_properties.len(),
            required_properties
        );

        // Store in PlanCtx for use during scan generation
        plan_ctx.set_where_property_requirements(required_properties);

        // Extract label constraints from id() patterns for UNION pruning
        use crate::query_planner::optimizer::union_pruning::extract_labels_from_id_where;
        let label_constraints = extract_labels_from_id_where(where_clause);

        log::debug!(
            "Extracted label constraints from WHERE clause: {:?}",
            label_constraints
        );

        // Store in PlanCtx for use during UNION generation
        plan_ctx.set_where_label_constraints(label_constraints);
    }

    for (idx, (path_variable, path_pattern)) in match_clause.path_patterns.iter().enumerate() {
        log::info!(
            "🔍 Pattern #{}: type={:?}, var={:?}",
            idx,
            std::mem::discriminant(path_pattern),
            path_variable
        );

        match path_pattern {
            ast::PathPattern::Node(node_pattern) => {
                log::info!("  → Processing as NODE pattern");
                plan = traverse_node_pattern(node_pattern, plan, plan_ctx)?;
            }
            ast::PathPattern::ConnectedPattern(connected_patterns) => {
                log::info!(
                    "  → Processing as CONNECTED pattern with {} connections",
                    connected_patterns.len()
                );
                plan = traverse_connected_pattern_with_mode(
                    connected_patterns,
                    plan,
                    plan_ctx,
                    idx,
                    None,
                    *path_variable,
                    is_optional,
                )?;
            }
            ast::PathPattern::ShortestPath(inner_pattern) => {
                // Process inner pattern with shortest path mode enabled
                plan = evaluate_single_path_pattern_with_mode(
                    inner_pattern.as_ref(),
                    plan,
                    plan_ctx,
                    idx,
                    Some(ShortestPathMode::Shortest),
                    *path_variable,
                )?;
            }
            ast::PathPattern::AllShortestPaths(inner_pattern) => {
                // Process inner pattern with all shortest paths mode enabled
                plan = evaluate_single_path_pattern_with_mode(
                    inner_pattern.as_ref(),
                    plan,
                    plan_ctx,
                    idx,
                    Some(ShortestPathMode::AllShortest),
                    *path_variable,
                )?;
            }
        }
    }

    convert_properties_to_operator_application(plan_ctx)?;

    // Apply WHERE clause if present (OpenCypher grammar allows WHERE per MATCH)
    if let Some(where_clause) = &match_clause.where_clause {
        use crate::query_planner::logical_plan::where_clause::evaluate_where_clause;
        plan = evaluate_where_clause(where_clause, plan)?;
    }

    Ok(plan)
}

// Helper function to evaluate a single path pattern with shortest path mode
fn evaluate_single_path_pattern_with_mode<'a>(
    path_pattern: &ast::PathPattern<'a>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    idx: usize,
    shortest_path_mode: Option<ShortestPathMode>,
    path_variable: Option<&str>,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    match path_pattern {
        ast::PathPattern::Node(node_pattern) => traverse_node_pattern(node_pattern, plan, plan_ctx),
        ast::PathPattern::ConnectedPattern(connected_patterns) => {
            traverse_connected_pattern_with_mode(
                connected_patterns,
                plan,
                plan_ctx,
                idx,
                shortest_path_mode,
                path_variable,
                false,
            )
        }
        ast::PathPattern::ShortestPath(inner) => {
            // Recursively unwrap with shortest path mode
            evaluate_single_path_pattern_with_mode(
                inner.as_ref(),
                plan,
                plan_ctx,
                idx,
                Some(ShortestPathMode::Shortest),
                path_variable,
            )
        }
        ast::PathPattern::AllShortestPaths(inner) => {
            // Recursively unwrap with all shortest paths mode
            evaluate_single_path_pattern_with_mode(
                inner.as_ref(),
                plan,
                plan_ctx,
                idx,
                Some(ShortestPathMode::AllShortest),
                path_variable,
            )
        }
    }
}
