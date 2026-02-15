//! Type inference for MATCH clause processing.
//!
//! This module handles automatic inference of node labels and relationship types
//! when they are not explicitly specified in Cypher queries.
//!
//! # Inference Strategies
//!
//! 1. **Single-schema inference**: If the schema has only one type, use it
//! 2. **Node-type inference**: Infer relationship types from typed node endpoints
//! 3. **Relationship-type inference**: Infer node labels from relationship type
//!
//! # Examples
//!
//! ```cypher
//! // Single relationship schema - relationship type inferred
//! MATCH ()-[r]->() RETURN r
//!
//! // Typed nodes - relationship type inferred from User→Post combinations
//! MATCH (u:User)-[r]->(p:Post) RETURN r
//! ```

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::graph_catalog::schema_types::SchemaType;
use crate::open_cypher_parser::ast;
use crate::query_planner::logical_plan::errors::LogicalPlanError;
use crate::query_planner::logical_plan::plan_builder::LogicalPlanResult;
use crate::query_planner::plan_ctx::PlanCtx;

/// Infer node label for standalone nodes when label is not specified.
///
/// Handles single-schema inference: If schema has only one node type, use it.
/// - Query: `MATCH (n) RETURN n`
/// - Schema: Only one node type defined (e.g., User)
/// - Result: n inferred as :User
///
/// # Returns
/// - `Ok(Some(label))` - Successfully inferred label
/// - `Ok(None)` - Cannot infer (multiple node types or no nodes in schema)
pub fn infer_node_label_from_schema(
    schema: &GraphSchema,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Option<String>> {
    let node_schemas = schema.all_node_schemas();

    // Case 1: Single node type in schema - use it
    if node_schemas.len() == 1 {
        let node_type = node_schemas
            .keys()
            .next()
            .ok_or_else(|| {
                LogicalPlanError::QueryPlanningError(
                    "Schema has exactly 1 node type but keys().next() returned None".to_string(),
                )
            })?
            .clone();
        log::info!(
            "Node inference: Schema has only one node type '{}', using it",
            node_type
        );
        return Ok(Some(node_type));
    }

    // Case 2: No nodes in schema
    if node_schemas.is_empty() {
        log::debug!("Node inference: Schema has no node types defined, cannot infer");
        return Ok(None);
    }

    // Case 3: Multiple node types - check if within limit for UNION generation
    let node_count = node_schemas.len();
    if node_count <= plan_ctx.max_inferred_types {
        // Could potentially generate UNION of all types, but for now just log info
        log::info!(
            "Node inference: Schema has {} node types ({:?}), would need UNION for all",
            node_count,
            node_schemas.keys().collect::<Vec<_>>()
        );
        // For now, don't auto-generate UNION - require explicit label
        return Ok(None);
    }

    // Case 4: Too many node types
    let types_preview: Vec<_> = node_schemas.keys().take(5).cloned().collect();
    let types_str = if node_count > 5 {
        format!("{}, ...", types_preview.join(", "))
    } else {
        node_schemas.keys().cloned().collect::<Vec<_>>().join(", ")
    };

    log::info!(
        "Node inference: Schema has {} node types [{}], too many for auto-inference",
        node_count,
        types_str
    );

    // Don't error - just return None to indicate no inference possible
    // User should specify an explicit label
    Ok(None)
}

/// Infer relationship type from typed node labels when edge is untyped.
///
/// Handles two cases:
/// 1. **Single-schema inference**: If schema has only one relationship, use it
///    - Query: `()-[r]->()`  →  infer r:ONLY_REL if only one relationship in schema
///
/// 2. **Node-type inference**: If nodes are typed, find relationships that match
///    - Query: `(a:Airport)-[r]->()`  →  infer r:FLIGHT if FLIGHT is the only edge with from_node=Airport
///    - Query: `()-[r]->(a:Airport)`  →  infer r:FLIGHT if FLIGHT is the only edge with to_node=Airport
///    - Query: `(a:User)-[r]->(b:Post)`  →  infer r:LIKES if LIKES is the only User→Post edge
///
/// # Returns
/// - `Ok(Some(types))` - Successfully inferred relationship types
/// - `Ok(None)` - Cannot infer (both nodes untyped with multi-schema, or no matches)
/// - `Err(TooManyInferredTypes)` - Too many matches, user must specify explicit type
pub fn infer_relationship_type_from_nodes(
    start_label: &Option<String>,
    end_label: &Option<String>,
    direction: &ast::Direction,
    schema: &GraphSchema,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Option<Vec<String>>> {
    let rel_schemas = schema.get_relationships_schemas();

    // Case 1: Single relationship in schema - use it regardless of node types
    if rel_schemas.len() == 1 {
        let rel_type = rel_schemas
            .keys()
            .next()
            .ok_or_else(|| {
                LogicalPlanError::QueryPlanningError(
                    "Schema has exactly 1 relationship type but keys().next() returned None"
                        .to_string(),
                )
            })?
            .clone();
        // Extract base type from composite key if needed
        let base_rel_type = if rel_type.contains("::") {
            rel_type.split("::").next().unwrap().to_string()
        } else {
            rel_type
        };
        log::info!(
            "Relationship inference: Schema has only one relationship type '{}', using it",
            base_rel_type
        );
        return Ok(Some(vec![base_rel_type]));
    }

    // Case 2: Both nodes untyped - expand to ALL relationship types (UNION ALL)
    // This enables Neo4j Browser's "dot" feature: MATCH ()-->() RETURN p
    // Each UNION branch becomes a typed query we already support
    if start_label.is_none() && end_label.is_none() {
        let all_rel_types: Vec<String> = rel_schemas
            .keys()
            .map(|key| {
                // Extract base type from composite key
                if key.contains("::") {
                    key.split("::").next().unwrap().to_string()
                } else {
                    key.clone()
                }
            })
            .collect();
        log::info!(
            "Relationship type inference: Both nodes untyped, expanding to all {} relationship types for UNION",
            all_rel_types.len()
        );
        return Ok(Some(all_rel_types));
    }

    // Case 3: At least one node is typed - filter relationships by node type compatibility
    // Helper: check if a label matches a schema node type (handles $any for polymorphic)
    let label_matches_from =
        |l: &String, rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema| -> bool {
            if rel_schema.from_node == "$any" {
                return true;
            }
            if l == &rel_schema.from_node {
                return true;
            }
            if let Some(values) = &rel_schema.from_label_values {
                return values.contains(l);
            }
            false
        };
    let label_matches_to =
        |l: &String, rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema| -> bool {
            if rel_schema.to_node == "$any" {
                return true;
            }
            if l == &rel_schema.to_node {
                return true;
            }
            if let Some(values) = &rel_schema.to_label_values {
                return values.contains(l);
            }
            false
        };

    let matching_types: Vec<String> = rel_schemas
        .iter()
        .filter(|(_, rel_schema)| {
            // Check compatibility based on direction
            match direction {
                ast::Direction::Outgoing => {
                    // start→end: from_node=start, to_node=end
                    let from_ok = start_label
                        .as_ref()
                        .map(|l| label_matches_from(l, rel_schema))
                        .unwrap_or(true);
                    let to_ok = end_label
                        .as_ref()
                        .map(|l| label_matches_to(l, rel_schema))
                        .unwrap_or(true);
                    from_ok && to_ok
                }
                ast::Direction::Incoming => {
                    // start←end: from_node=end, to_node=start
                    let from_ok = end_label
                        .as_ref()
                        .map(|l| label_matches_from(l, rel_schema))
                        .unwrap_or(true);
                    let to_ok = start_label
                        .as_ref()
                        .map(|l| label_matches_to(l, rel_schema))
                        .unwrap_or(true);
                    from_ok && to_ok
                }
                ast::Direction::Either => {
                    // Could match in either direction
                    let outgoing_ok = {
                        let from_ok = start_label
                            .as_ref()
                            .map(|l| label_matches_from(l, rel_schema))
                            .unwrap_or(true);
                        let to_ok = end_label
                            .as_ref()
                            .map(|l| label_matches_to(l, rel_schema))
                            .unwrap_or(true);
                        from_ok && to_ok
                    };
                    let incoming_ok = {
                        let from_ok = end_label
                            .as_ref()
                            .map(|l| label_matches_from(l, rel_schema))
                            .unwrap_or(true);
                        let to_ok = start_label
                            .as_ref()
                            .map(|l| label_matches_to(l, rel_schema))
                            .unwrap_or(true);
                        from_ok && to_ok
                    };
                    outgoing_ok || incoming_ok
                }
            }
        })
        .map(|(type_name, _)| {
            // Extract base type from composite key (TYPE::FROM::TO -> TYPE)
            if type_name.contains("::") {
                type_name.split("::").next().unwrap().to_string()
            } else {
                type_name.clone()
            }
        })
        .collect();

    if matching_types.is_empty() {
        log::warn!(
            "Relationship inference: No relationships match {:?}->{:?}",
            start_label,
            end_label
        );
        return Ok(None);
    }

    // Check if too many types would result in excessive UNION branches
    if matching_types.len() > plan_ctx.max_inferred_types {
        let types_preview: Vec<_> = matching_types.iter().take(5).cloned().collect();
        let types_str = if matching_types.len() > 5 {
            format!("{}, ...", types_preview.join(", "))
        } else {
            matching_types.join(", ")
        };

        log::error!(
            "Relationship inference: Too many matching types ({}) for {:?}->{:?}: [{}]. Max allowed is {}.",
            matching_types.len(), start_label, end_label, types_str, plan_ctx.max_inferred_types
        );

        return Err(LogicalPlanError::TooManyInferredTypes {
            count: matching_types.len(),
            max: plan_ctx.max_inferred_types,
            types: types_str,
        });
    }

    if matching_types.len() == 1 {
        log::info!(
            "Relationship inference: Inferred relationship type '{}' from node types {:?}->{:?}",
            matching_types[0],
            start_label,
            end_label
        );
    } else {
        log::info!(
            "Relationship inference: Multiple matching types {:?} for {:?}->{:?}, will expand to UNION",
            matching_types, start_label, end_label
        );
    }

    Ok(Some(matching_types))
}

/// **NEW (Feb 2026)**: Resolve connected patterns with shared variables
///
/// Detects when multiple patterns share variables and applies cross-pattern constraints
/// to reduce combination explosion.
///
/// # Example
/// ```cypher
/// MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c
/// ```
///
/// Without optimization:
/// - r1 can be 10 types → 10 combinations
/// - r2 can be 10 types → 10 combinations  
/// - Total: 10 × 10 = 100 UNION branches
///
/// With optimization:
/// - Detect `b` is shared: r1.to_node = r2.from_node
/// - Filter: Only combinations where r1.to_label == r2.from_label
/// - Result: ~5-10 valid UNION branches
///
/// # Parameters
/// - `patterns`: List of patterns in order: [(r1_start, r1_end, r1_types), (r2_start, r2_end, r2_types), ...]
/// - `schema`: Graph schema for validation
/// - `max_combinations`: Limit for total combinations (default 38)
///
/// # Returns
/// - `Ok(Some(combinations))` - Optimized list of valid type combinations
/// - `Ok(None)` - No optimization needed (patterns not connected or already typed)
/// - `Err(...)` - Too many combinations even after optimization
pub fn resolve_connected_patterns(
    patterns: Vec<(String, String, Vec<String>)>, // (from_alias, to_alias, rel_types)
    schema: &GraphSchema,
    max_combinations: usize,
) -> LogicalPlanResult<Option<Vec<Vec<(String, String, String)>>>> {
    // patterns: [(r1_from, r1_to, r1_types), (r2_from, r2_to, r2_types), ...]
    // Each entry: (from_node_alias, to_node_alias, possible_rel_types)

    if patterns.is_empty() || patterns.len() == 1 {
        return Ok(None); // No optimization needed
    }

    log::info!(
        "🔗 PatternResolver 2.0: Analyzing {} connected patterns for optimization",
        patterns.len()
    );

    // Find shared variables (nodes that appear in multiple patterns)
    let mut node_appearances: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (idx, (from_alias, to_alias, _)) in patterns.iter().enumerate() {
        node_appearances
            .entry(from_alias.clone())
            .or_default()
            .push(idx);
        node_appearances
            .entry(to_alias.clone())
            .or_default()
            .push(idx);
    }

    // Find shared nodes (appear in 2+ patterns)
    let shared_nodes: Vec<(String, Vec<usize>)> = node_appearances
        .into_iter()
        .filter(|(_, pattern_indices)| pattern_indices.len() >= 2)
        .collect();

    if shared_nodes.is_empty() {
        log::info!("  No shared variables found - patterns are independent");
        return Ok(None);
    }

    log::info!("  Found {} shared variables:", shared_nodes.len());
    for (node, indices) in &shared_nodes {
        log::info!("    '{}' appears in patterns: {:?}", node, indices);
    }

    // Generate all combinations with constraints
    // For prototype: Handle 2-pattern case
    if patterns.len() == 2 {
        let (r1_from, r1_to, r1_types) = &patterns[0];
        let (r2_from, r2_to, r2_types) = &patterns[1];

        // Check if they're connected via shared variable
        let shared_var = if r1_to == r2_from {
            Some(r1_to.clone())
        } else {
            None
        };

        if let Some(shared) = shared_var {
            log::info!("  Patterns connected via shared variable: '{}'", shared);
            log::info!(
                "    Pattern 1: {} -[{} types]-> {}",
                r1_from,
                r1_types.len(),
                r1_to
            );
            log::info!(
                "    Pattern 2: {} -[{} types]-> {}",
                r2_from,
                r2_types.len(),
                r2_to
            );

            let rel_schemas = schema.get_relationships_schemas();
            let mut valid_combos = Vec::new();

            // Generate combinations: for each r1 type, find compatible r2 types
            for r1_type in r1_types {
                // Get from/to labels for r1
                let r1_schema = rel_schemas.get(r1_type);
                if r1_schema.is_none() {
                    continue;
                }
                let r1_schema = r1_schema.unwrap();
                let r1_to_label = &r1_schema.to_node;

                for r2_type in r2_types {
                    // Get from/to labels for r2
                    let r2_schema = rel_schemas.get(r2_type);
                    if r2_schema.is_none() {
                        continue;
                    }
                    let r2_schema = r2_schema.unwrap();
                    let r2_from_label = &r2_schema.from_node;

                    // Constraint: shared variable must have same type
                    if r1_to_label == r2_from_label {
                        // Valid combination!
                        valid_combos.push(vec![
                            (
                                r1_schema.from_node.clone(),
                                r1_type.clone(),
                                r1_schema.to_node.clone(),
                            ),
                            (
                                r2_schema.from_node.clone(),
                                r2_type.clone(),
                                r2_schema.to_node.clone(),
                            ),
                        ]);

                        if valid_combos.len() >= max_combinations {
                            log::warn!(
                                "  ⚠️ Reached combination limit {} (found {} so far, stopping)",
                                max_combinations,
                                valid_combos.len()
                            );
                            break;
                        }
                    }
                }

                if valid_combos.len() >= max_combinations {
                    break;
                }
            }

            log::info!(
                "  ✅ Optimization complete: {} × {} = {} → {} valid combinations ({:.1}% reduction)",
                r1_types.len(),
                r2_types.len(),
                r1_types.len() * r2_types.len(),
                valid_combos.len(),
                100.0 * (1.0 - (valid_combos.len() as f64 / (r1_types.len() * r2_types.len()) as f64))
            );

            if valid_combos.is_empty() {
                log::warn!("  ⚠️ No valid combinations found after applying constraints!");
                return Ok(Some(vec![])); // Empty but not None - we tried optimization
            }

            return Ok(Some(valid_combos));
        }
    }

    // For 3+ patterns or unconnected patterns, fall back to no optimization (for now)
    log::info!(
        "  Skipping optimization: {} patterns (only 2-pattern optimization implemented)",
        patterns.len()
    );
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::graph_schema::{
        GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_schema_with_relationships() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "users".to_string(),
                column_names: vec!["user_id".to_string()],
                primary_keys: "user_id".to_string(),
                node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );
        nodes.insert(
            "Post".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "posts".to_string(),
                column_names: vec!["post_id".to_string()],
                primary_keys: "post_id".to_string(),
                node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );

        let mut rels = HashMap::new();
        rels.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "follows".to_string(),
                column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
                from_node: "User".to_string(),
                to_node: "User".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "users".to_string(),
                from_id: Identifier::from("follower_id"),
                to_id: Identifier::from("followed_id"),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
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
                edge_id_types: None,
            },
        );
        rels.insert(
            "LIKES".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "likes".to_string(),
                column_names: vec!["user_id".to_string(), "post_id".to_string()],
                from_node: "User".to_string(),
                to_node: "Post".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "posts".to_string(),
                from_id: Identifier::from("user_id"),
                to_id: Identifier::from("post_id"),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
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
                edge_id_types: None,
            },
        );

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    fn create_single_relationship_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "Node".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "nodes".to_string(),
                column_names: vec!["id".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );

        let mut rels = HashMap::new();
        rels.insert(
            "ONLY_REL".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "only_rel".to_string(),
                column_names: vec!["from_id".to_string(), "to_id".to_string()],
                from_node: "Node".to_string(),
                to_node: "Node".to_string(),
                from_node_table: "nodes".to_string(),
                to_node_table: "nodes".to_string(),
                from_id: Identifier::from("from_id"),
                to_id: Identifier::from("to_id"),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
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
                edge_id_types: None,
            },
        );

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    #[test]
    fn test_infer_relationship_type_single_schema() {
        let schema = create_single_relationship_schema();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &None,
            &None,
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .unwrap();

        assert_eq!(result, Some(vec!["ONLY_REL".to_string()]));
    }

    #[test]
    fn test_infer_relationship_type_from_both_nodes() {
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // User -> Post should match LIKES
        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &Some("Post".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .unwrap();

        assert_eq!(result, Some(vec!["LIKES".to_string()]));
    }

    #[test]
    fn test_infer_relationship_type_no_matches() {
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // Post -> User has no matching relationship
        let result = infer_relationship_type_from_nodes(
            &Some("Post".to_string()),
            &Some("User".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn test_infer_relationship_type_both_untyped_multi_schema() {
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // Both nodes untyped with multiple relationships - returns all rel types for UNION expansion
        let result = infer_relationship_type_from_nodes(
            &None,
            &None,
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .unwrap();

        // Now returns all relationship types for UNION expansion (changed behavior)
        assert!(result.is_some());
        let rel_types = result.unwrap();
        assert!(rel_types.contains(&"FOLLOWS".to_string()));
        assert!(rel_types.contains(&"LIKES".to_string()));
    }
}
