use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_expr::{Column, LogicalExpr, Operator, OperatorApplication, Property, PropertyAccess, TableAlias},
        logical_plan::{
            errors::LogicalPlanError,
            plan_builder::LogicalPlanResult,
            {CartesianProduct, GraphNode, GraphRel, LogicalPlan, ShortestPathMode, Union, VariableLengthSpec},
        },
        plan_ctx::{PlanCtx, TableCtx},
    },
};
use crate::graph_catalog::expression_parser::PropertyValue;

use super::{generate_id, ViewScan};
use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;

/// Maximum number of inferred types allowed before requiring explicit specification.
/// This prevents accidentally generating huge UNION queries from ambiguous patterns.
/// For example, `()-[r]->()` on a schema with 50 relationship types would need 50 UNION branches.
const MAX_INFERRED_TYPES: usize = 4;

/// Infer node label for standalone nodes when label is not specified.
///
/// Handles single-schema inference: If schema has only one node type, use it.
/// - Query: `MATCH (n) RETURN n`
/// - Schema: Only one node type defined (e.g., User)
/// - Result: n inferred as :User
///
/// Returns:
/// - `Ok(Some(label))` - Successfully inferred label
/// - `Ok(None)` - Cannot infer (multiple node types or no nodes in schema)
/// - `Err(TooManyInferredTypes)` - Too many matches, user must specify explicit type
fn infer_node_label_from_schema(schema: &GraphSchema, plan_ctx: &PlanCtx) -> LogicalPlanResult<Option<String>> {
    let node_schemas = schema.get_nodes_schemas();

    // Case 1: Single node type in schema - use it
    if node_schemas.len() == 1 {
        let node_type = node_schemas.keys().next().unwrap().clone();
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

/// Infer node labels from relationship schema when nodes are unlabeled.
///
/// For example:
/// - Query: `()-[r:FLIGHT]->()`
/// - Schema: FLIGHT has from_node=Airport, to_node=Airport
/// - Result: Both nodes inferred as Airport
///
// REMOVED: Old parsing-time label inference (now handled by TypeInference analyzer pass)
// The TypeInference pass runs after parsing and provides more robust inference
// that works across WITH boundaries and handles both node labels and edge types.

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
/// Returns:
/// - `Ok(Some(types))` - Successfully inferred relationship types
/// - `Ok(None)` - Cannot infer (both nodes untyped with multi-schema, or no matches)
/// - `Err(TooManyInferredTypes)` - Too many matches, user must specify explicit type
fn infer_relationship_type_from_nodes(
    start_label: &Option<String>,
    end_label: &Option<String>,
    direction: &ast::Direction,
    schema: &GraphSchema,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Option<Vec<String>>> {
    let rel_schemas = schema.get_relationships_schemas();

    // Case 1: Single relationship in schema - use it regardless of node types
    if rel_schemas.len() == 1 {
        let rel_type = rel_schemas.keys().next().unwrap().clone();
        log::info!(
            "Relationship inference: Schema has only one relationship type '{}', using it",
            rel_type
        );
        return Ok(Some(vec![rel_type]));
    }

    // Case 2: At least one node is typed - filter relationships by node type compatibility
    if start_label.is_none() && end_label.is_none() {
        log::debug!("Relationship inference: Both nodes untyped and schema has {} relationships, cannot infer",
            rel_schemas.len());
        return Ok(None);
    }

    // Find relationships that match the typed node(s)
    let matching_types: Vec<String> = rel_schemas
        .iter()
        .filter(|(_, rel_schema)| {
            // Check compatibility based on direction
            match direction {
                ast::Direction::Outgoing => {
                    // start→end: from_node=start, to_node=end
                    let from_ok = start_label
                        .as_ref()
                        .map(|l| {
                            // Check both from_node and from_label_values for polymorphic support
                            if l == &rel_schema.from_node {
                                return true;
                            }
                            if let Some(values) = &rel_schema.from_label_values {
                                return values.contains(l);
                            }
                            false
                        })
                        .unwrap_or(true);
                    let to_ok = end_label
                        .as_ref()
                        .map(|l| {
                            if l == &rel_schema.to_node {
                                return true;
                            }
                            if let Some(values) = &rel_schema.to_label_values {
                                return values.contains(l);
                            }
                            false
                        })
                        .unwrap_or(true);
                    from_ok && to_ok
                }
                ast::Direction::Incoming => {
                    // start←end: from_node=end, to_node=start
                    let from_ok = end_label
                        .as_ref()
                        .map(|l| {
                            if l == &rel_schema.from_node {
                                return true;
                            }
                            if let Some(values) = &rel_schema.from_label_values {
                                return values.contains(l);
                            }
                            false
                        })
                        .unwrap_or(true);
                    let to_ok = start_label
                        .as_ref()
                        .map(|l| {
                            if l == &rel_schema.to_node {
                                return true;
                            }
                            if let Some(values) = &rel_schema.to_label_values {
                                return values.contains(l);
                            }
                            false
                        })
                        .unwrap_or(true);
                    from_ok && to_ok
                }
                ast::Direction::Either => {
                    // Could match in either direction
                    let outgoing_ok = {
                        let from_ok = start_label
                            .as_ref()
                            .map(|l| {
                                l == &rel_schema.from_node
                                    || rel_schema
                                        .from_label_values
                                        .as_ref()
                                        .map(|v| v.contains(l))
                                        .unwrap_or(false)
                            })
                            .unwrap_or(true);
                        let to_ok = end_label
                            .as_ref()
                            .map(|l| {
                                l == &rel_schema.to_node
                                    || rel_schema
                                        .to_label_values
                                        .as_ref()
                                        .map(|v| v.contains(l))
                                        .unwrap_or(false)
                            })
                            .unwrap_or(true);
                        from_ok && to_ok
                    };
                    let incoming_ok = {
                        let from_ok = end_label
                            .as_ref()
                            .map(|l| {
                                l == &rel_schema.from_node
                                    || rel_schema
                                        .from_label_values
                                        .as_ref()
                                        .map(|v| v.contains(l))
                                        .unwrap_or(false)
                            })
                            .unwrap_or(true);
                        let to_ok = start_label
                            .as_ref()
                            .map(|l| {
                                l == &rel_schema.to_node
                                    || rel_schema
                                        .to_label_values
                                        .as_ref()
                                        .map(|v| v.contains(l))
                                        .unwrap_or(false)
                            })
                            .unwrap_or(true);
                        from_ok && to_ok
                    };
                    outgoing_ok || incoming_ok
                }
            }
        })
        .map(|(type_name, _)| type_name.clone())
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

/// Generate a scan operation for a node pattern
///
/// This function creates a ViewScan using schema information from plan_ctx.
/// If the schema lookup fails, it returns an error since node labels should be validated
/// against the schema.
fn generate_scan(
    alias: String,
    label: Option<String>,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "generate_scan called with alias='{}', label={:?}",
        alias,
        label
    );

    if let Some(label_str) = &label {
        // Handle $any wildcard for polymorphic edges
        if label_str == "$any" {
            log::debug!("Label is $any (polymorphic wildcard), creating Empty plan");
            return Ok(Arc::new(LogicalPlan::Empty));
        }

        log::debug!("Trying to create ViewScan for label '{}'", label_str);
        if let Some(view_scan) = try_generate_view_scan(&alias, &label_str, plan_ctx) {
            log::info!("✓ Successfully created ViewScan for label '{}'", label_str);
            Ok(view_scan)
        } else {
            // ViewScan creation failed - this is an error (schema not found)
            Err(LogicalPlanError::NodeNotFound(label_str.to_string()))
        }
    } else {
        log::debug!("No label provided - anonymous node, using Empty plan");
        // For anonymous nodes, use Empty plan
        // The node label will be inferred from relationship context during analysis
        Ok(Arc::new(LogicalPlan::Empty))
    }
}

/// Helper function to check if a plan contains a denormalized ViewScan
fn is_denormalized_scan(plan: &Arc<LogicalPlan>) -> bool {
    let result = match plan.as_ref() {
        LogicalPlan::ViewScan(view_scan) => {
            crate::debug_print!(
                "is_denormalized_scan: ViewScan.is_denormalized = {} for table '{}'",
                view_scan.is_denormalized,
                view_scan.source_table
            );
            view_scan.is_denormalized
        }
        _ => {
            crate::debug_print!("is_denormalized_scan: Not a ViewScan, returning false");
            false
        }
    };
    crate::debug_print!("is_denormalized_scan: returning {}", result);
    result
}

/// Check if a node label is denormalized by looking up the schema
/// Returns true if the node is denormalized (exists only in edge context)
fn is_label_denormalized(label: &Option<String>, plan_ctx: &PlanCtx) -> bool {
    if let Some(label_str) = label {
        let schema = plan_ctx.schema();
        if let Ok(node_schema) = schema.get_node_schema(label_str) {
            crate::debug_print!(
                "is_label_denormalized: label '{}' is_denormalized = {}",
                label_str,
                node_schema.is_denormalized
            );
            return node_schema.is_denormalized;
        }
    }
    crate::debug_print!(
        "is_label_denormalized: label {:?} not found or no label, returning false",
        label
    );
    false
}

/// Try to generate a ViewScan for a node by looking up the label in the schema from plan_ctx
/// Returns None if schema is not available or label not found.
pub fn try_generate_view_scan(
    _alias: &str,
    label: &str,
    plan_ctx: &PlanCtx,
) -> Option<Arc<LogicalPlan>> {
    log::debug!("try_generate_view_scan: label='{}'", label);

    // Use plan_ctx.schema() instead of GLOBAL_SCHEMAS
    let schema = plan_ctx.schema();

    // Look up the node schema for this label
    let node_schema = match schema.get_node_schema(label) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("Could not find node schema for label '{}': {:?}", label, e);
            return None;
        }
    };

    // DENORMALIZED NODE-ONLY QUERIES:
    // For denormalized nodes (virtual nodes that exist as columns on edge tables),
    // we need to generate queries from the edge table itself.
    //
    // For nodes that appear in MULTIPLE edge tables (like IP in dns_log and conn_log),
    // we create a UNION ALL of all possible sources.
    //
    // For each relationship where this node appears:
    // - If node is FROM → ViewScan with from_node_properties from that edge table
    // - If node is TO → ViewScan with to_node_properties from that edge table
    if node_schema.is_denormalized {
        log::info!(
            "✓ Denormalized node-only query for label '{}' - checking all tables",
            label
        );

        // Check if this node appears in multiple relationships/tables
        if let Some(metadata) = schema.get_denormalized_node_metadata(label) {
            let rel_types = metadata.get_relationship_types();

            if rel_types.len() > 1 || metadata.id_sources.values().any(|v| v.len() > 1) {
                // MULTI-TABLE CASE: Node appears in multiple tables/positions
                log::info!(
                    "✓ Denormalized node '{}' appears in {} relationship types - creating multi-table UNION",
                    label, rel_types.len()
                );

                let mut union_inputs: Vec<Arc<LogicalPlan>> = Vec::new();

                for rel_type in &rel_types {
                    if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                        let full_table_name = rel_schema.full_table_name();

                        // Check if this node is in FROM position
                        if rel_schema.from_node == label {
                            if let Some(ref from_props) = rel_schema.from_node_properties {
                                log::debug!(
                                    "✓ Adding FROM branch for '{}' from table '{}' (rel: {})",
                                    label,
                                    full_table_name,
                                    rel_type
                                );
                                let mut from_scan = ViewScan::new(
                                    full_table_name.clone(),
                                    None,
                                    HashMap::new(),
                                    String::new(),
                                    vec![],
                                    vec![],
                                );
                                from_scan.is_denormalized = true;
                                from_scan.from_node_properties = Some(
                                    from_props.iter()
                                        .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
                                        .collect()
                                );
                                union_inputs
                                    .push(Arc::new(LogicalPlan::ViewScan(Arc::new(from_scan))));
                            }
                        }

                        // Check if this node is in TO position
                        if rel_schema.to_node == label {
                            if let Some(ref to_props) = rel_schema.to_node_properties {
                                log::debug!(
                                    "✓ Adding TO branch for '{}' from table '{}' (rel: {})",
                                    label,
                                    full_table_name,
                                    rel_type
                                );
                                let mut to_scan = ViewScan::new(
                                    full_table_name.clone(),
                                    None,
                                    HashMap::new(),
                                    String::new(),
                                    vec![],
                                    vec![],
                                );
                                to_scan.is_denormalized = true;
                                to_scan.to_node_properties = Some(
                                    to_props.iter()
                                        .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
                                        .collect()
                                );
                                union_inputs
                                    .push(Arc::new(LogicalPlan::ViewScan(Arc::new(to_scan))));
                            }
                        }
                    }
                }

                if union_inputs.is_empty() {
                    log::error!("No ViewScans generated for denormalized node '{}'", label);
                    return None;
                }

                if union_inputs.len() == 1 {
                    log::info!(
                        "✓ Single ViewScan for denormalized node '{}' (only one source)",
                        label
                    );
                    return Some(union_inputs.pop().unwrap());
                }

                use crate::query_planner::logical_plan::{Union, UnionType};
                let union = Union {
                    inputs: union_inputs,
                    union_type: UnionType::All,
                };

                log::info!(
                    "✓ Created UNION ALL with {} branches for denormalized node '{}'",
                    union.inputs.len(),
                    label
                );
                return Some(Arc::new(LogicalPlan::Union(union)));
            }
        }

        // SINGLE-TABLE CASE: Fall through to existing logic
        let has_from_props = node_schema.from_properties.is_some();
        let has_to_props = node_schema.to_properties.is_some();
        let source_table = node_schema
            .denormalized_source_table
            .as_ref()
            .ok_or_else(|| {
                log::error!("Denormalized node '{}' missing source table", label);
            });

        if source_table.is_err() {
            log::error!("Cannot create ViewScan for denormalized node without source table");
            return None;
        }
        let source_table = source_table.unwrap();

        log::debug!(
            "Denormalized node '{}': has_from_props={}, has_to_props={}, source_table={}",
            label,
            has_from_props,
            has_to_props,
            source_table
        );

        // source_table is already fully qualified (database.table) from config.rs
        let full_table_name = source_table.clone();

        // Case 3: BOTH from and to properties → UNION ALL of two ViewScans
        if has_from_props && has_to_props {
            log::info!(
                "✓ Denormalized node '{}' has BOTH positions - creating UNION ALL",
                label
            );

            // Create FROM position ViewScan
            let mut from_scan = ViewScan::new(
                full_table_name.clone(),
                None,
                HashMap::new(),
                String::new(),
                vec![],
                vec![],
            );
            from_scan.is_denormalized = true;
            from_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
                props
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                v.clone(),
                            ),
                        )
                    })
                    .collect()
            });
            from_scan.schema_filter = node_schema.filter.clone();
            // Note: to_node_properties is None - this is the FROM branch

            // Create TO position ViewScan
            let mut to_scan = ViewScan::new(
                full_table_name,
                None,
                HashMap::new(),
                String::new(),
                vec![],
                vec![],
            );
            to_scan.is_denormalized = true;
            to_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
                props
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            crate::graph_catalog::expression_parser::PropertyValue::Column(
                                v.clone(),
                            ),
                        )
                    })
                    .collect()
            });
            to_scan.schema_filter = node_schema.filter.clone();
            // Note: from_node_properties is None - this is the TO branch

            // Create Union of the two ViewScans
            use crate::query_planner::logical_plan::{Union, UnionType};
            let union = Union {
                inputs: vec![
                    Arc::new(LogicalPlan::ViewScan(Arc::new(from_scan))),
                    Arc::new(LogicalPlan::ViewScan(Arc::new(to_scan))),
                ],
                union_type: UnionType::All,
            };

            log::info!("✓ Created UNION ALL for denormalized node '{}'", label);
            return Some(Arc::new(LogicalPlan::Union(union)));
        }

        // Case 1 or 2: Only one position - single ViewScan
        let mut view_scan = ViewScan::new(
            full_table_name,
            None,
            HashMap::new(),
            String::new(),
            vec![],
            vec![],
        );

        view_scan.is_denormalized = true;
        view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                    )
                })
                .collect()
        });
        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                    )
                })
                .collect()
        });
        view_scan.schema_filter = node_schema.filter.clone();

        log::info!(
            "✓ Created denormalized ViewScan for '{}' (single position)",
            label
        );

        return Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))));
    }

    // Log successful resolution
    log::info!(
        "✓ ViewScan: Resolved label '{}' to table '{}'",
        label,
        node_schema.table_name
    );

    // Use property mapping from schema directly (already PropertyValue)
    let property_mapping = node_schema.property_mappings.clone();

    // Create fully qualified table name (database.table)
    let full_table_name = format!("{}.{}", node_schema.database, node_schema.table_name);
    log::debug!("Using fully qualified table name: {}", full_table_name);

    // Get view parameter names from schema (if this is a parameterized view)
    let view_parameter_names = node_schema.view_parameters.clone();

    // Get view parameter values from PlanCtx (if provided)
    let view_parameter_values = plan_ctx.view_parameter_values().cloned();

    // Log parameter info
    if let Some(ref param_names) = view_parameter_names {
        log::debug!(
            "ViewScan: Table '{}' expects parameters: {:?}",
            node_schema.table_name,
            param_names
        );
        if let Some(ref param_values) = view_parameter_values {
            log::debug!("ViewScan: Will use parameter values: {:?}", param_values);
        } else {
            log::warn!(
                "ViewScan: Table '{}' is parameterized but no values provided!",
                node_schema.table_name
            );
        }
    }

    // Create ViewScan with the actual table name from schema
    let id_column = node_schema
        .node_id
        .columns()
        .first()
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            log::error!("Node schema for '{}' has no ID columns defined", label);
            "id".to_string()
        });
    
    let mut view_scan = ViewScan::new(
        full_table_name,  // Use fully qualified table name (database.table)
        None,             // No filter condition yet
        property_mapping, // Property mappings from schema
        id_column,        // ID column from schema (first for composite)
        vec!["id".to_string()], // Basic output schema
        vec![],           // No projections yet
    );

    // Set view parameters if this is a parameterized view
    view_scan.view_parameter_names = view_parameter_names;
    view_scan.view_parameter_values = view_parameter_values;

    // Set denormalized flag and properties from schema
    view_scan.is_denormalized = node_schema.is_denormalized;

    // Populate denormalized node properties (for role-based mapping)
    if node_schema.is_denormalized {
        // Convert from HashMap<String, String> to HashMap<String, PropertyValue>
        view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                    )
                })
                .collect()
        });

        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                    )
                })
                .collect()
        });

        log::debug!(
            "ViewScan: Populated denormalized properties for label '{}' - from_props={:?}, to_props={:?}",
            label,
            view_scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
            view_scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>())
        );
    }

    log::debug!(
        "ViewScan: Set is_denormalized={} for node label '{}' (table: {})",
        node_schema.is_denormalized,
        label,
        node_schema.table_name
    );

    // Set schema-level filter if defined in schema
    view_scan.schema_filter = node_schema.filter.clone();
    if view_scan.schema_filter.is_some() {
        log::info!(
            "ViewScan: Applied schema filter for label '{}': {:?}",
            label,
            node_schema.filter.as_ref().map(|f| &f.raw)
        );
    }

    Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))))
}

/// Try to generate a ViewScan for a relationship by looking up the relationship type in the schema from plan_ctx
pub fn try_generate_relationship_view_scan(
    _alias: &str,
    rel_type: &str,
    left_node_label: Option<&str>,
    right_node_label: Option<&str>,
    plan_ctx: &PlanCtx,
) -> Option<Arc<LogicalPlan>> {
    log::debug!(
        "try_generate_relationship_view_scan: rel_type='{}', left_node_label={:?}, right_node_label={:?}",
        rel_type,
        left_node_label,
        right_node_label
    );

    // Use plan_ctx.schema() instead of GLOBAL_SCHEMAS
    let schema = plan_ctx.schema();

    // Look up the relationship schema for this type, using node labels for disambiguation
    let rel_schema = match schema.get_rel_schema_with_nodes(rel_type, left_node_label, right_node_label) {
        Ok(s) => s,
        Err(e) => {
            log::warn!(
                "Could not find relationship schema for type '{}' with nodes ({:?}, {:?}): {:?}",
                rel_type,
                left_node_label,
                right_node_label,
                e
            );
            return None;
        }
    };

    // Log successful resolution
    log::info!(
        "✓ Relationship ViewScan: Resolved type '{}' to table '{}'",
        rel_type,
        rel_schema.table_name
    );

    // Copy property mappings from schema so relationships can be expanded in RETURN
    let property_mapping = rel_schema.property_mappings.clone();
    log::debug!(
        "Relationship ViewScan: property_mapping has {} entries",
        property_mapping.len()
    );

    // Create fully qualified table name (database.table)
    let full_table_name = format!("{}.{}", rel_schema.database, rel_schema.table_name);
    log::debug!(
        "Using fully qualified relationship table name: {}",
        full_table_name
    );

    // Get view parameter names from schema (if this is a parameterized view)
    let view_parameter_names = rel_schema.view_parameters.clone();

    // Get view parameter values from PlanCtx (if provided)
    let view_parameter_values = plan_ctx.view_parameter_values().cloned();

    // Log parameter info
    if let Some(ref param_names) = view_parameter_names {
        log::debug!(
            "Relationship ViewScan: Table '{}' expects parameters: {:?}",
            rel_schema.table_name,
            param_names
        );
        if let Some(ref param_values) = view_parameter_values {
            log::debug!(
                "Relationship ViewScan: Will use parameter values: {:?}",
                param_values
            );
        } else {
            log::warn!(
                "Relationship ViewScan: Table '{}' is parameterized but no values provided!",
                rel_schema.table_name
            );
        }
    }

    // Create ViewScan for relationship with from/to columns
    let mut view_scan = ViewScan::new_relationship(
        full_table_name,            // Use fully qualified table name (database.table)
        None,                       // No filter condition yet
        property_mapping,           // Empty for now
        rel_schema.from_id.clone(), // Use from_id as id_column for relationships
        vec!["id".to_string()],     // Output schema - relationships have "id" property
        vec![],                     // No projections yet
        rel_schema.from_id.clone(), // From column from schema
        rel_schema.to_id.clone(),   // To column from schema
    );

    // Set view parameters if this is a parameterized view
    view_scan.view_parameter_names = view_parameter_names;
    view_scan.view_parameter_values = view_parameter_values;

    // Populate polymorphic edge fields from schema
    // Copy label columns even if type_column is None (fixed-endpoint pattern)
    view_scan.type_column = rel_schema.type_column.clone();
    view_scan.from_label_column = rel_schema.from_label_column.clone();
    view_scan.to_label_column = rel_schema.to_label_column.clone();

    if rel_schema.type_column.is_some()
        || rel_schema.from_label_column.is_some()
        || rel_schema.to_label_column.is_some()
    {
        log::debug!(
            "ViewScan: Populated polymorphic fields for rel '{}' - type_column={:?}, from_label={:?}, to_label={:?}",
            rel_type,
            view_scan.type_column,
            view_scan.from_label_column,
            view_scan.to_label_column
        );
    }

    // Set denormalized node properties from schema
    // Convert HashMap<String, String> to HashMap<String, PropertyValue>
    view_scan.from_node_properties = rel_schema.from_node_properties.as_ref().map(|props| {
        props
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                )
            })
            .collect()
    });
    view_scan.to_node_properties = rel_schema.to_node_properties.as_ref().map(|props| {
        props
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone()),
                )
            })
            .collect()
    });

    if view_scan.from_node_properties.is_some() || view_scan.to_node_properties.is_some() {
        log::debug!(
            "ViewScan: Set denormalized node properties for rel '{}' - from_props={:?}, to_props={:?}",
            rel_type,
            view_scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
            view_scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>())
        );
    }

    // Set schema-level filter if defined in schema
    view_scan.schema_filter = rel_schema.filter.clone();
    if view_scan.schema_filter.is_some() {
        log::info!(
            "ViewScan: Applied schema filter for relationship '{}': {:?}",
            rel_type,
            rel_schema.filter.as_ref().map(|f| &f.raw)
        );
    }

    Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))))
}

/// Generate a relationship center (ViewScan if possible, otherwise regular Scan)
fn generate_relationship_center(
    rel_alias: &str,
    rel_labels: &Option<Vec<String>>,
    left_connection: &str,
    right_connection: &str,
    left_node_label: &Option<String>,
    right_node_label: &Option<String>,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "Creating relationship center for alias '{}', labels: {:?}, left_node_label: {:?}, right_node_label: {:?}",
        rel_alias,
        rel_labels,
        left_node_label,
        right_node_label
    );
    // Try to generate a ViewScan for the relationship if we have a single type
    if let Some(labels) = rel_labels {
        log::debug!("Relationship has {} labels: {:?}", labels.len(), labels);

        // Deduplicate labels - [:FOLLOWS|FOLLOWS] should be treated as single type
        let unique_labels: Vec<_> = {
            let mut seen = std::collections::HashSet::new();
            labels.iter().filter(|l| seen.insert(*l)).cloned().collect()
        };
        log::debug!(
            "After deduplication: {} unique labels: {:?}",
            unique_labels.len(),
            unique_labels
        );

        if unique_labels.len() == 1 {
            log::debug!(
                "Trying to create Relationship ViewScan for type '{}'",
                unique_labels[0]
            );
            if let Some(view_scan) =
                try_generate_relationship_view_scan(
                    rel_alias,
                    &unique_labels[0],
                    left_node_label.as_ref().map(|s| s.as_str()),
                    right_node_label.as_ref().map(|s| s.as_str()),
                    plan_ctx
                )
            {
                log::info!(
                    "✓ Successfully created Relationship ViewScan for type '{}'",
                    unique_labels[0]
                );
                return Ok(view_scan);
            } else {
                // ViewScan creation failed - this is an error
                return Err(LogicalPlanError::RelationshipNotFound(
                    unique_labels[0].clone()
                ));
            }
        } else {
            log::debug!(
                "Multiple relationship types ({}), using Empty plan (CTE uses GraphRel.labels)",
                unique_labels.len()
            );
            // For multiple relationships, use Empty plan
            // The actual UNION ALL CTE generation happens in render phase using GraphRel.labels
            // No need for "rel_*" placeholder - it was never actually looked up
            return Ok(Arc::new(LogicalPlan::Empty));
        }
    } else {
        log::debug!("No relationship labels specified, using Empty plan");
        // For relationships without labels, use Empty
        // Type inference pass will fill in the relationship type
        return Ok(Arc::new(LogicalPlan::Empty));
    }
}

fn convert_properties(props: Vec<Property>, node_alias: &str) -> LogicalPlanResult<Vec<LogicalExpr>> {
    let mut extracted_props: Vec<LogicalExpr> = vec![];

    for prop in props {
        match prop {
            Property::PropertyKV(property_kvpair) => {
                let op_app = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(node_alias.to_string()),
                            column: PropertyValue::Column(property_kvpair.key.to_string()),
                        }),
                        property_kvpair.value,
                    ],
                });
                extracted_props.push(op_app);
            }
            Property::Param(_) => return Err(LogicalPlanError::FoundParamInProperties),
        }
    }

    Ok(extracted_props)
}

fn convert_properties_to_operator_application(plan_ctx: &mut PlanCtx) -> LogicalPlanResult<()> {
    for (alias, table_ctx) in plan_ctx.get_mut_alias_table_ctx_map().iter_mut() {
        let mut extracted_props = convert_properties(table_ctx.get_and_clear_properties(), alias)?;
        table_ctx.append_filters(&mut extracted_props);
    }
    Ok(())
}

// Wrapper for backwards compatibility
// Reserved for future use when non-optional traversal needs explicit mode
#[allow(dead_code)]
fn traverse_connected_pattern<'a>(
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
    crate::debug_print!("\n╔════════════════════════════════════════");
    crate::debug_print!("║ traverse_connected_pattern_with_mode");
    crate::debug_print!("║ connected_patterns.len() = {}", connected_patterns.len());
    crate::debug_print!("║ Current plan type: {:?}", std::mem::discriminant(&*plan));
    crate::debug_print!("╚════════════════════════════════════════\n");

    // === PRE-PROCESS: Assign consistent aliases to shared nodes ===
    // When patterns share nodes via Rc::clone() (e.g., ()-[r1]->()-[r2]->()),
    // we need to ensure the shared node gets the same alias in both patterns.
    // Use pointer equality to detect shared Rc instances.
    use std::collections::HashMap;

    // Use usize from Rc::as_ptr() cast as the key for pointer-based identity
    let mut node_alias_map: HashMap<usize, String> = HashMap::new();

    for connected_pattern in connected_patterns.iter() {
        // Check start_node - use address as key
        let start_ptr = connected_pattern.start_node.as_ptr() as usize;
        if !node_alias_map.contains_key(&start_ptr) {
            let start_node_ref = connected_pattern.start_node.borrow();
            let alias = if let Some(name) = start_node_ref.name {
                name.to_string()
            } else {
                generate_id()
            };
            drop(start_node_ref);
            node_alias_map.insert(start_ptr, alias);
        }

        // Check end_node - use address as key
        let end_ptr = connected_pattern.end_node.as_ptr() as usize;
        if !node_alias_map.contains_key(&end_ptr) {
            let end_node_ref = connected_pattern.end_node.borrow();
            let alias = if let Some(name) = end_node_ref.name {
                name.to_string()
            } else {
                generate_id()
            };
            drop(end_node_ref);
            node_alias_map.insert(end_ptr, alias);
        }
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
                log::info!(">>> Found existing '{}' in plan_ctx with label: {}", start_node_alias, label);
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
            .map(|props| props.into_iter().map(Property::from).collect())
            .unwrap_or_else(Vec::new);

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
                log::info!(">>> Found existing '{}' in plan_ctx with label: {}", end_node_alias, label);
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
                    let variants = graph_schema.expand_generic_relationship_type(
                        label,
                        from_label,
                        to_label,
                    );
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
                    expanded_labels.into_iter().filter(|l| seen.insert(l.clone())).collect()
                };
                
                Some(unique_labels)
            }
            None => {
                // Anonymous edge pattern: [] (no type specified)
                // Use smart inference to determine relationship type(s):
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
            }
        };

        // === LABEL INFERENCE ===
        // NOTE: Label and edge type inference is now handled by the TypeInference analyzer pass
        // which runs after parsing. This provides more robust inference that works across
        // WITH boundaries and handles both node labels AND edge types.
        // The labels in start_node_label/end_node_label come from AST parsing or will be
        // inferred by TypeInference pass.
        
        log::debug!(
            "Pattern processing: start='{}' ({}), end='{}' ({})",
            start_node_alias,
            start_node_label.as_ref().map(|s| s.as_str()).unwrap_or("None"),
            end_node_alias,
            end_node_label.as_ref().map(|s| s.as_str()).unwrap_or("None")
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
            .map(|props| props.into_iter().map(Property::from).collect())
            .unwrap_or_else(Vec::new);

        crate::debug_print!(
            "│ End node: alias='{}', label={:?}",
            end_node_alias,
            end_node_label
        );

        let end_node_props = end_node_ref
            .properties
            .clone()
            .map(|props| props.into_iter().map(Property::from).collect())
            .unwrap_or_else(Vec::new);

        // if start alias already present in ctx map, it means the current nested connected pattern's start node will be connecting at right side plan and end node will be at the left
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&start_node_alias) {
            if start_node_label.is_some() {
                table_ctx.set_labels(start_node_label.clone().map(|l| vec![l]));
            }
            if !start_node_props.is_empty() {
                table_ctx.append_properties(start_node_props);
            }

            plan_ctx.insert_table_ctx(
                end_node_alias.clone(),
                TableCtx::build(
                    end_node_alias.clone(),
                    end_node_label.clone().map(|l| vec![l]),
                    end_node_props,
                    false,
                    end_node_ref.name.is_some(),
                ),
            );

            let (left_conn, right_conn) = match rel.direction {
                ast::Direction::Outgoing => (start_node_alias.clone(), end_node_alias.clone()),
                ast::Direction::Incoming => (end_node_alias.clone(), start_node_alias.clone()),
                ast::Direction::Either => (start_node_alias.clone(), end_node_alias.clone()),
            };

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) = match rel.direction {
                ast::Direction::Outgoing => (start_node_label.clone(), end_node_label.clone()),
                ast::Direction::Incoming => (end_node_label.clone(), start_node_label.clone()),
                ast::Direction::Either => (start_node_label.clone(), end_node_label.clone()),
            };

            // FIX: For multi-hop patterns, use the existing plan as LEFT to create nested structure
            // This ensures (a)-[r1]->(b)-[r2]->(c) becomes GraphRel { left: GraphRel(a-r1-b), center: r2, right: c }
            let (left_node, right_node) = match rel.direction {
                ast::Direction::Outgoing => {
                    // (a)-[:r1]->(b)-[:r2]->(c): existing plan (a-r1-b) on left, new node (c) on right

                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        crate::debug_print!(
                            "=== End node '{}' is DENORMALIZED, creating Empty scan ===",
                            end_node_alias
                        );
                        (Arc::new(LogicalPlan::Empty), true)
                    } else {
                        let scan = generate_scan(
                            end_node_alias.clone(),
                            end_node_label.clone(),
                            plan_ctx,
                        )?;
                        let is_d = is_denormalized_scan(&scan);
                        (scan, is_d)
                    };

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

                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        crate::debug_print!(
                            "=== End node '{}' is DENORMALIZED, creating Empty scan ===",
                            end_node_alias
                        );
                        (Arc::new(LogicalPlan::Empty), true)
                    } else {
                        let scan = generate_scan(
                            end_node_alias.clone(),
                            end_node_label.clone(),
                            plan_ctx,
                        )?;
                        let is_d = is_denormalized_scan(&scan);
                        (scan, is_d)
                    };

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

                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        crate::debug_print!(
                            "=== End node '{}' is DENORMALIZED, creating Empty scan ===",
                            end_node_alias
                        );
                        (Arc::new(LogicalPlan::Empty), true)
                    } else {
                        let scan = generate_scan(
                            end_node_alias.clone(),
                            end_node_label.clone(),
                            plan_ctx,
                        )?;
                        let is_d = is_denormalized_scan(&scan);
                        (scan, is_d)
                    };

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
            // The anchor is whichever node was already seen in the base MATCH
            let anchor_connection = if is_optional {
                let alias_map = plan_ctx.get_alias_table_ctx_map();
                if alias_map.contains_key(&left_conn) && !alias_map.contains_key(&right_conn) {
                    // left_conn exists, right_conn is new -> left_conn is anchor
                    Some(left_conn.clone())
                } else if alias_map.contains_key(&right_conn) && !alias_map.contains_key(&left_conn)
                {
                    // right_conn exists, left_conn is new -> right_conn is anchor
                    Some(right_conn.clone())
                } else {
                    // Both exist or neither exists - shouldn't happen in normal OPTIONAL MATCH
                    // Fall back to None
                    crate::debug_print!("WARN: OPTIONAL MATCH could not determine anchor: left_conn={}, right_conn={}", left_conn, right_conn);
                    None
                }
            } else {
                None
            };

            // Handle variable-length patterns and multi-type relationships:
            // - Single-type *1: (a)-[:TYPE*1]->(b) → simplify to regular relationship
            // - Multi-type *1: (a)-[:TYPE1|TYPE2*1]->(b) → keep VLP for polymorphic nodes
            // - Multi-type no VLP: (a)-[:TYPE1|TYPE2]->(b) → ADD implicit *1 for polymorphic handling
            let is_multi_type = rel_labels.as_ref().map_or(false, |labels| labels.len() > 1);
            
            let variable_length = if let Some(vlp) = rel.variable_length.clone() {
                // Has explicit VLP spec
                let spec: VariableLengthSpec = vlp.into();
                let is_exact_one_hop = spec.min_hops == Some(1) && spec.max_hops == Some(1);
                
                if is_exact_one_hop && !is_multi_type {
                    log::info!("Simplifying *1 single-type pattern to regular relationship");
                    None  // Remove *1 for single-type - treat as regular relationship
                } else {
                    Some(spec)  // Keep VLP for multi-type or ranges
                }
            } else if is_multi_type {
                // Multi-type without VLP: add implicit *1 for proper polymorphic handling
                log::info!("Adding implicit *1 for multi-type relationship (polymorphic end node)");
                Some(VariableLengthSpec {
                    min_hops: Some(1),
                    max_hops: Some(1),
                })
            } else {
                None  // Single-type, no VLP
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
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias.clone(),
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

            // Set connected node labels for polymorphic relationship resolution
            if let Some(rel_table_ctx) = plan_ctx.get_mut_table_ctx_opt(&rel_alias) {
                rel_table_ctx.set_connected_nodes(
                    left_node_label_for_rel.clone(),
                    right_node_label_for_rel.clone(),
                );
            }

            // Register path variable in PlanCtx if present
            if let Some(path_var) = path_variable {
                plan_ctx.insert_table_ctx(
                    path_var.to_string(),
                    TableCtx::build(
                        path_var.to_string(),
                        None.map(|l| vec![l]), // Path variables don't have labels
                        vec![],                // Path variables don't have properties
                        false,                 // Not a relationship
                        true,                  // Explicitly named by user
                    ),
                );
            }

            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

            crate::debug_print!("│ ✓ Created GraphRel (start node already in context)");
            crate::debug_print!("│   Plan is now: GraphRel");
            crate::debug_print!("└─ Pattern #{} complete\n", pattern_idx);
        }
        // if end alias already present in ctx map, it means the current nested connected pattern's end node will be connecting at right side plan and start node will be at the left
        else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&end_node_alias) {
            log::info!(">>> Found existing TableCtx for '{}', updating with label: {:?}", end_node_alias, end_node_label);
            if end_node_label.is_some() {
                table_ctx.set_labels(end_node_label.clone().map(|l| vec![l]));
                log::info!(">>> Updated '{}' with label: {}", end_node_alias, end_node_label.as_ref().unwrap());
            } else {
                log::warn!(">>> end_node_label is None for '{}', cannot update TableCtx!", end_node_alias);
            }
            if !end_node_props.is_empty() {
                table_ctx.append_properties(end_node_props);
            }

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
                    let is_d = is_denormalized_scan(&scan);
                    (scan, is_d)
                };

            let start_graph_node = GraphNode {
                input: start_scan,
                alias: start_node_alias.clone(),
                label: start_node_label.clone().map(|s| s.to_string()),
                is_denormalized: start_is_denorm,
            projected_columns: None,
            };
            plan_ctx.insert_table_ctx(
                start_node_alias.clone(),
                TableCtx::build(
                    start_node_alias.clone(),
                    start_node_label.clone().map(|l| vec![l]),
                    start_node_props,
                    false,
                    start_node_ref.name.is_some(),
                ),
            );

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) = match rel.direction {
                ast::Direction::Outgoing => (start_node_label.clone(), end_node_label.clone()),
                ast::Direction::Incoming => (end_node_label.clone(), start_node_label.clone()),
                ast::Direction::Either => (start_node_label.clone(), end_node_label.clone()),
            };

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
                variable_length: {
                    let is_multi_type = rel_labels.as_ref().map_or(false, |labels| labels.len() > 1);
                    if let Some(vlp) = rel.variable_length.clone() {
                        let spec: VariableLengthSpec = vlp.into();
                        let is_exact_one_hop = spec.min_hops == Some(1) && spec.max_hops == Some(1);
                        if is_exact_one_hop && !is_multi_type {
                            None  // *1 single-type is same as regular relationship
                        } else {
                            Some(spec)  // Keep *1 for multi-type or ranges
                        }
                    } else if is_multi_type {
                        // Add implicit *1 for multi-type without VLP (polymorphic end node)
                        Some(VariableLengthSpec { min_hops: Some(1), max_hops: Some(1) })
                    } else {
                        None  // Single-type, no VLP
                    }
                },
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
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias.clone(),
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

            // Set connected node labels for polymorphic relationship resolution
            if let Some(rel_table_ctx) = plan_ctx.get_mut_table_ctx_opt(&rel_alias) {
                rel_table_ctx.set_connected_nodes(
                    left_node_label_for_rel.clone(),
                    right_node_label_for_rel.clone(),
                );
            }

            // Register path variable in PlanCtx if present
            if let Some(path_var) = path_variable {
                plan_ctx.insert_table_ctx(
                    path_var.to_string(),
                    TableCtx::build(
                        path_var.to_string(),
                        None.map(|l| vec![l]), // Path variables don't have labels
                        vec![],                // Path variables don't have properties
                        false,                 // Not a relationship
                        true,                  // Explicitly named by user
                    ),
                );
            }

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
            plan_ctx.insert_table_ctx(
                start_node_alias.clone(),
                TableCtx::build(
                    start_node_alias.clone(),
                    start_node_label.clone().map(|l| vec![l]),
                    start_node_props,
                    false,
                    start_node_ref.name.is_some(),
                ),
            );

            let (end_scan, end_is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                crate::debug_print!(
                    "=== End node '{}' is DENORMALIZED, creating Empty scan ===",
                    end_node_alias
                );
                (Arc::new(LogicalPlan::Empty), true)
            } else {
                let scan = generate_scan(end_node_alias.clone(), end_node_label.clone(), plan_ctx)?;
                let is_d = is_denormalized_scan(&scan);
                (scan, is_d)
            };

            let end_graph_node = GraphNode {
                input: end_scan,
                alias: end_node_alias.clone(),
                label: end_node_label.clone().map(|s| s.to_string()),
                is_denormalized: end_is_denorm,
            projected_columns: None,
            };
            plan_ctx.insert_table_ctx(
                end_node_alias.clone(),
                TableCtx::build(
                    end_node_alias.clone(),
                    end_node_label.clone().map(|l| vec![l]),
                    end_node_props,
                    false,
                    end_node_ref.name.is_some(),
                ),
            );

            let (left_conn, right_conn) = match rel.direction {
                ast::Direction::Outgoing => (start_node_alias.clone(), end_node_alias.clone()),
                ast::Direction::Incoming => (end_node_alias.clone(), start_node_alias.clone()),
                ast::Direction::Either => (start_node_alias.clone(), end_node_alias.clone()),
            };

            // Compute left and right node labels based on direction for relationship lookup
            let (left_node_label_for_rel, right_node_label_for_rel) = match rel.direction {
                ast::Direction::Outgoing => (start_node_label, end_node_label),
                ast::Direction::Incoming => (end_node_label, start_node_label),
                ast::Direction::Either => (start_node_label, end_node_label),
            };

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
            // Check which connection already exists in alias_table_ctx_map
            let anchor_connection = if is_optional {
                let alias_map = plan_ctx.get_alias_table_ctx_map();
                if alias_map.contains_key(&left_conn) && !alias_map.contains_key(&right_conn) {
                    Some(left_conn.clone())
                } else if alias_map.contains_key(&right_conn) && !alias_map.contains_key(&left_conn)
                {
                    Some(right_conn.clone())
                } else {
                    None
                }
            } else {
                None
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
                left_connection: left_conn.clone(), // Left node is the start node (left_conn for Outgoing)
                right_connection: right_conn.clone(), // Right node is the end node (right_conn for Outgoing)
                is_rel_anchor: false,
                variable_length: {
                    let is_multi_type = rel_labels.as_ref().map_or(false, |labels| labels.len() > 1);
                    if let Some(vlp) = rel.variable_length.clone() {
                        let spec: VariableLengthSpec = vlp.into();
                        let is_exact_one_hop = spec.min_hops == Some(1) && spec.max_hops == Some(1);
                        if is_exact_one_hop && !is_multi_type {
                            None  // *1 single-type is same as regular relationship
                        } else {
                            Some(spec)  // Keep *1 for multi-type or ranges
                        }
                    } else if is_multi_type {
                        // Add implicit *1 for multi-type without VLP (polymorphic end node)
                        Some(VariableLengthSpec { min_hops: Some(1), max_hops: Some(1) })
                    } else {
                        None  // Single-type, no VLP
                    }
                },
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: {
                    // 🔧 FIX: For shortestPath with bound nodes, extract filters/properties from left/right nodes
                    // When nodes like (p1:Person {id: 1}) are bound before shortestPath, their filters
                    // are in plan_ctx but not automatically merged into GraphRel.where_predicate
                    if shortest_path_mode.is_some() {
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
                                            "🔧 shortestPath: Converted {} properties to filters for left node '{}'",
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
                                            "🔧 shortestPath: Converted {} properties to filters for right node '{}'",
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
                        if !node_filters.is_empty() {
                            log::info!(
                                "🔧 shortestPath: Merged {} bound node filters into where_predicate for rel '{}'",
                                node_filters.len(),
                                rel_alias
                            );
                            Some(node_filters.into_iter().reduce(|acc, filter| {
                                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, filter],
                                })
                            }).unwrap())
                        } else {
                            None // No filters found
                        }
                    } else {
                        None // Will be populated by filter pushdown optimization for regular patterns
                    }
                },
                labels: rel_labels.clone(),
                is_optional: if is_optional { Some(true) } else { None },
                anchor_connection,
                cte_references: std::collections::HashMap::new(),
            };
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias.clone(),
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

            // Set connected node labels for polymorphic relationship resolution
            if let Some(rel_table_ctx) = plan_ctx.get_mut_table_ctx_opt(&rel_alias) {
                rel_table_ctx.set_connected_nodes(
                    left_node_label_for_rel.clone(),
                    right_node_label_for_rel.clone(),
                );
            }

            // Register path variable in PlanCtx if present
            if let Some(path_var) = path_variable {
                plan_ctx.insert_table_ctx(
                    path_var.to_string(),
                    TableCtx::build(
                        path_var.to_string(),
                        None.map(|l| vec![l]), // Path variables don't have labels
                        vec![],                // Path variables don't have properties
                        false,                 // Not a relationship
                        true,                  // Explicitly named by user
                    ),
                );
            }

            // Create the GraphRel for this pattern
            let new_pattern = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

            // If we have an existing plan (e.g., from WITH clause), combine with CartesianProduct
            if has_existing_plan {
                plan = Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                    left: plan.clone(),   // Previous plan (e.g., Projection from WITH)
                    right: new_pattern,   // New disconnected pattern
                    is_optional,          // Pass through the is_optional flag
                    join_condition: None, // Will be populated by optimizer if WHERE bridges both sides
                }));
                crate::debug_print!(
                    "│ ✓ Created CartesianProduct (combining existing plan with new pattern)"
                );
                crate::debug_print!(
                    "│   Plan is now: CartesianProduct(optional: {})",
                    is_optional
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

fn traverse_node_pattern(
    node_pattern: &ast::NodePattern,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    // For now we are not supporting empty node. standalone node with name is supported.
    let node_alias = node_pattern
        .name
        .ok_or(LogicalPlanError::EmptyNode)?
        .to_string();
    let mut node_label: Option<String> = node_pattern.first_label().map(|val| val.to_string());

    // === SINGLE-NODE-SCHEMA INFERENCE ===
    // If no label provided and schema has only one node type, use it
    if node_label.is_none() {
        if let Ok(Some(inferred_label)) = infer_node_label_from_schema(plan_ctx.schema(), plan_ctx) {
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
        .map(|props| props.into_iter().map(Property::from).collect())
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
        // plan_ctx.alias_table_ctx_map.insert(node_alias.clone(), TableCtx { label: node_label, properties: node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: node_pattern.name.is_some() });
        plan_ctx.insert_table_ctx(
            node_alias.clone(),
            TableCtx::build(
                node_alias.clone(),
                node_label.clone().map(|l| vec![l]), // Clone here so we can use it below
                node_props,
                false,
                node_pattern.name.is_some(),
            ),
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
                    Arc::new(LogicalPlan::GraphNode(GraphNode {
                        input: branch.clone(),
                        alias: node_alias.clone(),
                        label: node_label.clone().map(|s| s.to_string()),
                        is_denormalized: is_denorm,
            projected_columns: None,
                    }))
                })
                .collect();

            let wrapped_union = Union {
                inputs: wrapped_inputs,
                union_type: union.union_type.clone(),
            };
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
        let has_existing_plan = match plan.as_ref() {
            LogicalPlan::Empty => false,
            _ => true,
        };
        
        if has_existing_plan {
            // Create CartesianProduct to combine existing plan with new node
            // This generates: FROM existing_table CROSS JOIN new_node_table
            log::info!(
                "Creating CartesianProduct for comma pattern: existing plan + node '{}'",
                new_node_alias
            );
            Ok(Arc::new(LogicalPlan::CartesianProduct(CartesianProduct {
                left: plan.clone(),
                right: new_node_plan,
                is_optional: false,
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
    for (idx, (path_variable, path_pattern)) in match_clause.path_patterns.iter().enumerate() {
        match path_pattern {
            ast::PathPattern::Node(node_pattern) => {
                plan = traverse_node_pattern(node_pattern, plan, plan_ctx)?;
            }
            ast::PathPattern::ConnectedPattern(connected_patterns) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast;
    use crate::query_planner::logical_expr::{Direction, Literal, LogicalExpr, PropertyKVPair};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_convert_properties_with_kv_pairs() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: LogicalExpr::Literal(Literal::String("John".to_string())),
            }),
            Property::PropertyKV(PropertyKVPair {
                key: "age".to_string(),
                value: LogicalExpr::Literal(Literal::Integer(30)),
            }),
        ];

        let result = convert_properties(properties, "n").unwrap();
        assert_eq!(result.len(), 2);

        // Check first property conversion
        match &result[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                assert_eq!(op_app.operands.len(), 2);
                match &op_app.operands[0] {
                    LogicalExpr::PropertyAccessExp(prop) => {
                        assert_eq!(prop.table_alias.0, "n");
                        match &prop.column {
                            PropertyValue::Column(col) => assert_eq!(col, "name"),
                            _ => panic!("Expected Column property"),
                        }
                    }
                    _ => panic!("Expected PropertyAccessExp"),
                }
                match &op_app.operands[1] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "John"),
                    _ => panic!("Expected String literal"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }

        // Check second property conversion
        match &result[1] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                match &op_app.operands[1] {
                    LogicalExpr::Literal(Literal::Integer(age)) => assert_eq!(*age, 30),
                    _ => panic!("Expected Integer literal"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_convert_properties_with_param_returns_error() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: LogicalExpr::Literal(Literal::String("Alice".to_string())),
            }),
            Property::Param("param1".to_string()),
        ];

        let result = convert_properties(properties, "n");
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::FoundParamInProperties => (), // Expected error
            _ => panic!("Expected FoundParamInProperties error"),
        }
    }

    #[test]
    fn test_convert_properties_empty_list() {
        let properties = vec![];
        let result = convert_properties(properties, "n").unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_generate_id_uniqueness() {
        let id1 = generate_id();
        let id2 = generate_id();

        // IDs should be unique
        assert_ne!(id1, id2);

        // IDs should start with 't' (simple format: t1, t2, t3...)
        assert!(id1.starts_with('t'));
        assert!(id2.starts_with('t'));

        // IDs should be reasonable length (t1 to t999999+)
        assert!(id1.len() >= 2 && id1.len() < 10);
        assert!(id2.len() >= 2 && id2.len() < 10);
    }

    #[test]
    fn test_traverse_node_pattern_new_node() {
        let graph_schema = create_test_schema_with_relationships();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let node_pattern = ast::NodePattern {
            name: Some("customer"),
            labels: Some(vec!["Person"]),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "city",
                value: ast::Expression::Literal(ast::Literal::String("Boston")),
            })]),
        };

        let result =
            traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

        // Should return a GraphNode plan
        match result.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                assert_eq!(graph_node.alias, "customer");
                // Input should be a ViewScan
                match graph_node.input.as_ref() {
                    LogicalPlan::ViewScan(_view_scan) => {
                        // ViewScan created successfully via try_generate_view_scan
                        // This happens when GLOBAL_GRAPH_SCHEMA is available
                    }
                    _ => panic!("Expected ViewScan as input"),
                }
            }
            _ => panic!("Expected GraphNode"),
        }

        // Should have added entry to plan context
        let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
        assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string()));
        // Note: properties get moved to filters after convert_properties_to_operator_application
        assert!(table_ctx.is_explicit_alias());
    }

    #[test]
    fn test_traverse_node_pattern_existing_node() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Pre-populate plan context with existing node
        plan_ctx.insert_table_ctx(
            "customer".to_string(),
            TableCtx::build(
                "customer".to_string(),
                Some("User".to_string()).map(|l| vec![l]),
                vec![],
                false,
                true,
            ),
        );

        let node_pattern = ast::NodePattern {
            name: Some("customer"),
            labels: Some(vec!["Person"]), // Different label
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "age",
                value: ast::Expression::Literal(ast::Literal::Integer(25)),
            })]),
        };

        let result =
            traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

        // Should return the same plan (not create new GraphNode)
        assert_eq!(result, initial_plan);

        // Should have updated the existing table context
        let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
        assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string())); // Label should be updated
                                                                           // Note: properties get moved to filters after convert_properties_to_operator_application
    }

    #[test]
    fn test_traverse_node_pattern_empty_node_error() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let node_pattern = ast::NodePattern {
            name: None, // Empty node
            labels: Some(vec!["Person"]),
            properties: None,
        };

        let result = traverse_node_pattern(&node_pattern, initial_plan, &mut plan_ctx);
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::EmptyNode => (), // Expected error
            _ => panic!("Expected EmptyNode error"),
        }
    }

    #[test]
    fn test_traverse_connected_pattern_new_connection() {
        let graph_schema = create_test_schema_with_relationships();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let start_node = ast::NodePattern {
            name: Some("user"),
            labels: Some(vec!["Person"]),
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("company"),
            labels: Some(vec!["Organization"]),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("works_at"),
            direction: ast::Direction::Outgoing,
            labels: Some(vec!["WORKS_AT"]),
            properties: None,
            variable_length: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        let result =
            traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0)
                .unwrap();

        // Should return a GraphRel plan
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "works_at");
                assert_eq!(graph_rel.direction, Direction::Outgoing);
                assert_eq!(graph_rel.left_connection, "user"); // Left node is the start node (user) for outgoing relationships
                assert_eq!(graph_rel.right_connection, "company"); // Right node is the end node (company) for outgoing relationships
                assert!(!graph_rel.is_rel_anchor);

                // Check left side (start node for outgoing relationships)
                match graph_rel.left.as_ref() {
                    LogicalPlan::GraphNode(left_node) => {
                        assert_eq!(left_node.alias, "user");
                    }
                    _ => panic!("Expected GraphNode on left"),
                }

                // Check right side (end node for outgoing relationships)
                match graph_rel.right.as_ref() {
                    LogicalPlan::GraphNode(right_node) => {
                        assert_eq!(right_node.alias, "company");
                    }
                    _ => panic!("Expected GraphNode on right"),
                }
            }
            _ => panic!("Expected GraphRel"),
        }

        // Should have added entries to plan context
        assert!(plan_ctx.get_table_ctx("user").is_ok());
        assert!(plan_ctx.get_table_ctx("company").is_ok());
        assert!(plan_ctx.get_table_ctx("works_at").is_ok());

        let rel_ctx = plan_ctx.get_table_ctx("works_at").unwrap();
        assert!(rel_ctx.is_relation());
    }

    #[test]
    fn test_traverse_connected_pattern_with_existing_start_node() {
        let graph_schema = create_test_schema_with_relationships();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Pre-populate with existing start node
        plan_ctx.insert_table_ctx(
            "user".to_string(),
            TableCtx::build(
                "user".to_string(),
                Some("Person".to_string()).map(|l| vec![l]),
                vec![],
                false,
                true,
            ),
        );

        let start_node = ast::NodePattern {
            name: Some("user"),      // This exists in plan_ctx
            labels: Some(vec!["Employee"]), // Different label
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("project"),
            labels: Some(vec!["Project"]),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("assigned_to"),
            direction: ast::Direction::Incoming,
            labels: Some(vec!["ASSIGNED_TO"]),
            properties: None,
            variable_length: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        let result =
            traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0)
                .unwrap();

        // Should return a GraphRel plan with different structure
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "assigned_to");
                assert_eq!(graph_rel.direction, Direction::Incoming);
                assert_eq!(graph_rel.left_connection, "project");
                assert_eq!(graph_rel.right_connection, "user");

                // Left should be the new end node
                match graph_rel.left.as_ref() {
                    LogicalPlan::GraphNode(left_node) => {
                        assert_eq!(left_node.alias, "project");
                    }
                    _ => panic!("Expected GraphNode on left"),
                }
            }
            _ => panic!("Expected GraphRel"),
        }

        // Existing start node should have updated label
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_label_opt(), Some("Employee".to_string()));
    }

    // Test removed: DisconnectedPatternFound error no longer exists
    // as of commit b015cf0 which allows disconnected comma patterns
    // with WHERE clause predicates for cross-table correlation

    #[test]
    fn test_evaluate_match_clause_with_node_and_connected_pattern() {
        let graph_schema = create_test_schema_with_relationships();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Create a match clause with both node pattern and connected pattern
        let node_pattern = ast::NodePattern {
            name: Some("admin"),
            labels: Some(vec!["User"]),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "role",
                value: ast::Expression::Literal(ast::Literal::String("administrator")),
            })]),
        };

        let start_node = ast::NodePattern {
            name: Some("admin"), // Same as above - should connect
            labels: None,
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("system"),
            labels: Some(vec!["System"]),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("manages"),
            direction: ast::Direction::Outgoing,
            labels: Some(vec!["MANAGES"]),
            properties: None,
            variable_length: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let match_clause = ast::MatchClause {
            path_patterns: vec![
                (None, ast::PathPattern::Node(node_pattern)),
                (None, ast::PathPattern::ConnectedPattern(vec![connected_pattern])),
            ],
        };

        let result = evaluate_match_clause(&match_clause, initial_plan, &mut plan_ctx).unwrap();

        // Should return a GraphRel plan
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "manages");
                assert_eq!(graph_rel.direction, Direction::Outgoing);
            }
            _ => panic!("Expected GraphRel at top level"),
        }

        // Properties should have been converted to filters
        let admin_ctx = plan_ctx.get_table_ctx("admin").unwrap();
        assert_eq!(admin_ctx.get_filters().len(), 1);
    }

    #[test]
    fn test_convert_properties_to_operator_application() {
        let mut plan_ctx = PlanCtx::default();

        // Add table context with properties
        let properties = vec![Property::PropertyKV(PropertyKVPair {
            key: "status".to_string(),
            value: LogicalExpr::Literal(Literal::String("active".to_string())),
        })];

        let table_ctx = TableCtx::build(
            "user".to_string(),
            Some("Person".to_string()).map(|l| vec![l]),
            properties,
            false,
            true,
        );

        plan_ctx.insert_table_ctx("user".to_string(), table_ctx);

        // Before conversion, table should have no filters
        let table_ctx_before = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(table_ctx_before.get_filters().len(), 0);

        // Convert properties
        let result = convert_properties_to_operator_application(&mut plan_ctx);
        assert!(result.is_ok());

        // After conversion, properties should be moved to filters
        let table_ctx_after = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(table_ctx_after.get_filters().len(), 1); // Filter added

        // Check the filter predicate
        match &table_ctx_after.get_filters()[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                match &op_app.operands[0] {
                    LogicalExpr::PropertyAccessExp(prop_access) => {
                        assert_eq!(prop_access.table_alias.0, "user");
                        assert_eq!(prop_access.column.raw(), "status");
                    }
                    _ => panic!("Expected PropertyAccessExp"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_generate_scan() {
        // Create schema with Customer node
        use crate::graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema};
        use std::collections::HashMap;
        
        let mut nodes = HashMap::new();
        nodes.insert(
            "Customer".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "customers".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            nodes,
            HashMap::new(),
        ));
        let plan_ctx = PlanCtx::new(schema);

        let scan = generate_scan(
            "customers".to_string(),
            Some("Customer".to_string()),
            &plan_ctx,
        )
        .unwrap();

        match scan.as_ref() {
            LogicalPlan::ViewScan(scan_plan) => {
                assert_eq!(scan_plan.source_table, "test_db.customers");
                // The label is "Customer" but ViewScan doesn't store it directly
            }
            _ => panic!("Expected ViewScan plan"),
        }
    }

    // ==========================================
    // Tests for relationship type inference
    // ==========================================

    fn create_test_schema_with_relationships() -> GraphSchema {
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            "Airport".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "airports".to_string(),
                column_names: vec!["id".to_string(), "code".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "users".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "Post".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "posts".to_string(),
                column_names: vec!["id".to_string(), "title".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );

        let mut rels = HashMap::new();
        rels.insert(
            "FLIGHT".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "flights".to_string(),
                column_names: vec!["from_airport".to_string(), "to_airport".to_string()],
                from_node: "Airport".to_string(),
                to_node: "Airport".to_string(),
                from_node_table: "airports".to_string(),
                to_node_table: "airports".to_string(),
                from_id: "from_airport".to_string(),
                to_id: "to_airport".to_string(),
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
            },
        );
        rels.insert(
            "LIKES".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "likes".to_string(),
                column_names: vec!["user_id".to_string(), "post_id".to_string()],
                from_node: "User".to_string(),
                to_node: "Post".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "posts".to_string(),
                from_id: "user_id".to_string(),
                to_id: "post_id".to_string(),
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
            },
        );
        rels.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "follows".to_string(),
                column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
                from_node: "User".to_string(),
                to_node: "User".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "users".to_string(),
                from_id: "follower_id".to_string(),
                to_id: "followed_id".to_string(),
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
            },
        );
        
        // Add missing nodes for tests
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "persons".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "city".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "Organization".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "organizations".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "Employee".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "employees".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "Project".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "projects".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        nodes.insert(
            "System".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "systems".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );
        
        // Add missing relationships for tests
        rels.insert(
            "WORKS_AT".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "works_at".to_string(),
                column_names: vec!["person_id".to_string(), "org_id".to_string()],
                from_node: "Person".to_string(),
                to_node: "Organization".to_string(),
                from_node_table: "persons".to_string(),
                to_node_table: "organizations".to_string(),
                from_id: "person_id".to_string(),
                to_id: "org_id".to_string(),
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
            },
        );
        rels.insert(
            "ASSIGNED_TO".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "assigned_to".to_string(),
                column_names: vec!["emp_id".to_string(), "proj_id".to_string()],
                from_node: "Employee".to_string(),
                to_node: "Project".to_string(),
                from_node_table: "employees".to_string(),
                to_node_table: "projects".to_string(),
                from_id: "emp_id".to_string(),
                to_id: "proj_id".to_string(),
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
            },
        );
        rels.insert(
            "MANAGES".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "manages".to_string(),
                column_names: vec!["user_id".to_string(), "system_id".to_string()],
                from_node: "User".to_string(),
                to_node: "System".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "systems".to_string(),
                from_id: "user_id".to_string(),
                to_id: "system_id".to_string(),
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
            },
        );

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    fn create_single_relationship_schema() -> GraphSchema {
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "persons".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );

        let mut rels = HashMap::new();
        rels.insert(
            "KNOWS".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "knows".to_string(),
                column_names: vec!["person1_id".to_string(), "person2_id".to_string()],
                from_node: "Person".to_string(),
                to_node: "Person".to_string(),
                from_node_table: "persons".to_string(),
                to_node_table: "persons".to_string(),
                from_id: "person1_id".to_string(),
                to_id: "person2_id".to_string(),
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
            },
        );

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    #[test]
    fn test_infer_relationship_type_single_schema() {
        // When schema has only one relationship, use it regardless of node types
        let schema = create_single_relationship_schema();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &None, // untyped start
            &None, // untyped end
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], "KNOWS");
    }

    #[test]
    fn test_infer_relationship_type_from_start_node() {
        // (a:Airport)-[r]->() should infer FLIGHT (only edge from Airport)
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &Some("Airport".to_string()),
            &None,
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], "FLIGHT");
    }

    #[test]
    fn test_infer_relationship_type_from_end_node() {
        // ()-[r]->(p:Post) should infer LIKES (only edge to Post)
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &None,
            &Some("Post".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], "LIKES");
    }

    #[test]
    fn test_infer_relationship_type_from_both_nodes() {
        // (u:User)-[r]->(p:Post) should infer LIKES
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &Some("Post".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], "LIKES");
    }

    #[test]
    fn test_infer_relationship_type_multiple_matches() {
        // (u:User)-[r]->() should return LIKES, FOLLOWS, and MANAGES (multiple edges from User)
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &None,
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 3); // Now 3 relationships: LIKES, FOLLOWS, MANAGES
        assert!(types.contains(&"LIKES".to_string()));
        assert!(types.contains(&"FOLLOWS".to_string()));
        assert!(types.contains(&"MANAGES".to_string()));
    }

    #[test]
    fn test_infer_relationship_type_incoming_direction() {
        // ()<-[r]-(p:Post) should infer LIKES (reversed direction)
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &None,
            &Some("Post".to_string()),
            &ast::Direction::Incoming,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        // Incoming means: from=end(Post), to=start(None)
        // LIKES has from=User, to=Post
        // So we need to check: from_node=Post? No. LIKES doesn't match.
        // Actually for incoming: from=end, to=start
        // So Post is the end node, meaning we're looking for relationships with to_node=Post
        // But incoming flips it: from_matches_end = "Post" == rel.from_node? No for LIKES
        // Hmm, let me reconsider - for incoming, the arrow points to start
        // So the relationship's to_node should be the pattern's start node
        // And the relationship's from_node should be the pattern's end node
        // In this case: ()<-[r]-(p:Post) means Post→anonymous
        // So we want relationships where from_node=Post - but LIKES has from_node=User
        // This should return None/empty
        assert!(result.is_none() || result.as_ref().unwrap().is_empty());
    }

    #[test]
    fn test_infer_relationship_type_incoming_correct() {
        // (u:User)<-[r]-() should infer FOLLOWS (User is the to_node of FOLLOWS)
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &None,
            &ast::Direction::Incoming,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        // Incoming: from=end(None), to=start(User)
        // FOLLOWS: from=User, to=User - matches (to=User checks against start)
        // LIKES: from=User, to=Post - doesn't match (to=Post != User)
        assert!(result.is_some());
        let types = result.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], "FOLLOWS");
    }

    #[test]
    fn test_infer_relationship_type_no_matches() {
        // (a:Airport)-[r]->(u:User) should find no matching relationships
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_relationship_type_from_nodes(
            &Some("Airport".to_string()),
            &Some("User".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("Should not error");

        // FLIGHT: Airport→Airport - doesn't match (to=Airport != User)
        // LIKES: User→Post - doesn't match (from=User != Airport)
        // FOLLOWS: User→User - doesn't match
        assert!(result.is_none());
    }

    #[test]
    fn test_infer_relationship_type_both_untyped_multi_schema() {
        // ()-[r]->() with multiple relationships should return None
        let schema = create_test_schema_with_relationships();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result =
            infer_relationship_type_from_nodes(&None, &None, &ast::Direction::Outgoing, &schema, &plan_ctx)
                .expect("Should not error");

        // Both nodes untyped and schema has 3 relationships - cannot infer
        assert!(result.is_none());
    }

    // Tests for node label inference from relationship type
    // Note: infer_node_labels_from_relationship function was removed
    // These tests are commented out until the feature is reimplemented

    /* #[test]
    fn test_infer_node_labels_from_typed_relationship() {
        // ()-[r:FLIGHT]->() should infer both nodes as Airport
        let schema = create_test_schema_with_relationships();

        let (start, end, _, _) = infer_node_labels_from_relationship(
            None,
            None,
            &Some(vec!["FLIGHT".to_string()]),
            &ast::Direction::Outgoing,
            &schema,
        );

        assert_eq!(start, Some("Airport".to_string()));
        assert_eq!(end, Some("Airport".to_string()));
    }

    #[test]
    fn test_infer_node_labels_partial() {
        // (u:User)-[r:LIKES]->() should infer end node as Post
        let schema = create_test_schema_with_relationships();

        let (start, end, _, _) = infer_node_labels_from_relationship(
            Some("User".to_string()),
            None,
            &Some(vec!["LIKES".to_string()]),
            &ast::Direction::Outgoing,
            &schema,
        );

        // Start was already User, end should be inferred as Post
        assert_eq!(start, Some("User".to_string()));
        assert_eq!(end, Some("Post".to_string()));
    }

    #[test]
    fn test_infer_node_labels_incoming_direction() {
        // ()<-[r:LIKES]-(u:User) should infer start as Post
        let schema = create_test_schema_with_relationships();

        let (start, end, _, _) = infer_node_labels_from_relationship(
            None,
            Some("User".to_string()),
            &Some(vec!["LIKES".to_string()]),
            &ast::Direction::Incoming,
            &schema,
        );

        // For incoming: start is to_node (Post), end is from_node (User)
        assert_eq!(start, Some("Post".to_string()));
        assert_eq!(end, Some("User".to_string()));
    }
    */

    #[test]
    fn test_infer_relationship_type_too_many_matches_error() {
        // Create a schema with many relationship types from User
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "users".to_string(),
                column_names: vec!["id".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );

        let mut rels = HashMap::new();
        // Create 6 relationships from User to User (exceeds MAX_INFERRED_TYPES of 4)
        for i in 1..=6 {
            rels.insert(
                format!("REL_{}", i),
                RelationshipSchema {
                    database: "test_db".to_string(),
                    table_name: format!("rel_{}", i),
                    column_names: vec!["from_id".to_string(), "to_id".to_string()],
                    from_node: "User".to_string(),
                    to_node: "User".to_string(),
                    from_node_table: "users".to_string(),
                    to_node_table: "users".to_string(),
                    from_id: "from_id".to_string(),
                    to_id: "to_id".to_string(),
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
                },
            );
        }

        let schema = GraphSchema::build(1, "test_db".to_string(), nodes, rels);
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // (u:User)-[r]->() should fail with TooManyInferredTypes error
        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &None,
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::TooManyInferredTypes {
                count,
                max,
                types: _,
            } => {
                assert_eq!(count, 6);
                assert_eq!(max, 4); // default max_inferred_types
            }
            other => panic!("Expected TooManyInferredTypes error, got: {:?}", other),
        }
    }

    // ========================================
    // Tests for infer_node_label_from_schema
    // ========================================

    fn create_single_node_schema() -> GraphSchema {
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "persons".to_string(),
                column_names: vec!["id".to_string(), "name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
            },
        );

        // No relationships needed for node-only inference tests
        let rels = HashMap::new();

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    fn create_multi_node_schema() -> GraphSchema {
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        for node_type in &["User", "Post", "Comment"] {
            nodes.insert(
                node_type.to_string(),
                NodeSchema {
                    database: "test_db".to_string(),
                    table_name: format!("{}s", node_type.to_lowercase()),
                    column_names: vec!["id".to_string()],
                    primary_keys: "id".to_string(),
                    node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
                },
            );
        }

        let rels = HashMap::new();

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    fn create_empty_node_schema() -> GraphSchema {
        use std::collections::HashMap;

        let nodes = HashMap::new();
        let rels = HashMap::new();

        GraphSchema::build(1, "test_db".to_string(), nodes, rels)
    }

    #[test]
    fn test_infer_node_label_single_node_schema() {
        // When schema has only one node type, infer it
        let schema = create_single_node_schema();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

        assert_eq!(result, Some("Person".to_string()));
    }

    #[test]
    fn test_infer_node_label_multi_node_schema() {
        // When schema has multiple node types, cannot infer (returns None)
        let schema = create_multi_node_schema();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

        // Should not auto-infer when multiple types exist
        assert_eq!(result, None);
    }

    #[test]
    fn test_infer_node_label_empty_schema() {
        // When schema has no nodes, cannot infer
        let schema = create_empty_node_schema();
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

        assert_eq!(result, None);
    }

    #[test]
    fn test_infer_node_label_many_nodes_no_error() {
        // When schema has many node types, should return None without error
        // (unlike relationships, we don't generate UNION for standalone nodes yet)
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        for i in 1..=10 {
            nodes.insert(
                format!("Type{}", i),
                NodeSchema {
                    database: "test_db".to_string(),
                    table_name: format!("type_{}", i),
                    column_names: vec!["id".to_string()],
                    primary_keys: "id".to_string(),
                    node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
                },
            );
        }

        let schema = GraphSchema::build(1, "test_db".to_string(), nodes, HashMap::new());
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

        // Should not auto-infer when many types exist (just return None, no error)
        assert_eq!(result, None);
    }

    #[test]
    fn test_infer_node_label_denormalized_single_node() {
        // Single denormalized node type should still be inferred
        // The inference works at schema level - denormalized handling is done later
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        nodes.insert(
            "Airport".to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: "flights".to_string(), // Edge table
                column_names: vec!["Origin".to_string(), "Dest".to_string()],
                primary_keys: "Origin".to_string(),
                node_id: NodeIdSchema::single("Origin".to_string(), "String".to_string()),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                is_denormalized: true, // Denormalized node!
                from_properties: Some({
                    let mut m = HashMap::new();
                    m.insert("code".to_string(), "Origin".to_string());
                    m
                }),
                to_properties: Some({
                    let mut m = HashMap::new();
                    m.insert("code".to_string(), "Dest".to_string());
                    m
                }),
                denormalized_source_table: Some("test_db.flights".to_string()),
                label_column: None,
                label_value: None,
            },
        );

        let schema = GraphSchema::build(1, "test_db".to_string(), nodes, HashMap::new());
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // Should still infer the label - denormalized handling happens later
        let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");
        assert_eq!(result, Some("Airport".to_string()));
    }

    #[test]
    fn test_infer_relationship_type_polymorphic_edge() {
        // Polymorphic edge with from_label_values should match typed nodes
        use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        for node_type in &["User", "Group", "Resource"] {
            nodes.insert(
                node_type.to_string(),
                NodeSchema {
                    database: "test_db".to_string(),
                    table_name: format!("{}s", node_type.to_lowercase()),
                    column_names: vec!["id".to_string()],
                    primary_keys: "id".to_string(),
                    node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
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
                },
            );
        }

        let mut rels = HashMap::new();
        // Polymorphic MEMBER_OF: (User|Group) -> Group
        rels.insert(
            "MEMBER_OF".to_string(),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: "memberships".to_string(),
                column_names: vec!["member_id".to_string(), "group_id".to_string()],
                from_node: "$any".to_string(), // Polymorphic
                to_node: "Group".to_string(),
                from_node_table: "$any".to_string(),
                to_node_table: "groups".to_string(),
                from_id: "member_id".to_string(),
                to_id: "group_id".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                edge_id: None,
                type_column: None,
                from_label_column: Some("member_type".to_string()),
                to_label_column: None,
                from_label_values: Some(vec!["User".to_string(), "Group".to_string()]), // Polymorphic!
                to_label_values: None,
                from_node_properties: None,
                to_node_properties: None,
                is_fk_edge: false,
            constraints: None,
            },
        );

        let schema = GraphSchema::build(1, "test_db".to_string(), nodes, rels);
        let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

        // (u:User)-[r]->(g:Group) should infer MEMBER_OF since User is in from_label_values
        let result = infer_relationship_type_from_nodes(
            &Some("User".to_string()),
            &Some("Group".to_string()),
            &ast::Direction::Outgoing,
            &schema,
            &plan_ctx,
        )
        .expect("should not error");

        assert_eq!(result, Some(vec!["MEMBER_OF".to_string()]));
    }
}
