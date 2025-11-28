use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_expr::{Column, LogicalExpr, Operator, OperatorApplication, Property},
        logical_plan::{
            errors::LogicalPlanError,
            plan_builder::LogicalPlanResult,
            {GraphNode, GraphRel, LogicalPlan, Scan, ShortestPathMode, Union, UnionType},
        },
        plan_ctx::{PlanCtx, TableCtx},
    },
};

use super::{ViewScan, generate_id};
use std::collections::HashMap;

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
        log::debug!("Trying to create ViewScan for label '{}'", label_str);
        if let Some(view_scan) = try_generate_view_scan(&alias, &label_str, plan_ctx) {
            log::info!("✓ Successfully created ViewScan for label '{}'", label_str);
            Ok(view_scan)
        } else {
            log::warn!(
                "Schema lookup failed for node label '{}', falling back to regular Scan",
                label_str
            );

            // Even for fallback Scan, try to get the actual table name from schema
            // Use plan_ctx.schema() instead of GLOBAL_GRAPH_SCHEMA
            let table_name = match plan_ctx.schema().get_node_schema(label_str) {
                Ok(node_schema) => {
                    log::info!(
                        "✓ Fallback Scan: Using table '{}' for label '{}'",
                        node_schema.table_name,
                        label_str
                    );
                    Some(node_schema.table_name.clone())
                }
                Err(_) => {
                    log::warn!(
                        "Could not find schema for label '{}', using label as table name",
                        label_str
                    );
                    Some(label_str.clone())
                }
            };

            let scan = Scan {
                table_alias: Some(alias),
                table_name,
            };
            Ok(Arc::new(LogicalPlan::Scan(scan)))
        }
    } else {
        log::debug!("No label provided, creating regular Scan");
        // For nodes without labels, create a regular Scan with no table name
        let scan = Scan {
            table_alias: Some(alias),
            table_name: None,
        };
        Ok(Arc::new(LogicalPlan::Scan(scan)))
    }
}

/// Helper function to check if a plan contains a denormalized ViewScan
fn is_denormalized_scan(plan: &Arc<LogicalPlan>) -> bool {
    let result = match plan.as_ref() {
        LogicalPlan::ViewScan(view_scan) => {
            eprintln!(
                "is_denormalized_scan: ViewScan.is_denormalized = {} for table '{}'",
                view_scan.is_denormalized,
                view_scan.source_table
            );
            view_scan.is_denormalized
        }
        _ => {
            eprintln!("is_denormalized_scan: Not a ViewScan, returning false");
            false
        }
    };
    eprintln!("is_denormalized_scan: returning {}", result);
    result
}

/// Check if a node label is denormalized by looking up the schema
/// Returns true if the node is denormalized (exists only in edge context)
fn is_label_denormalized(label: &Option<String>, plan_ctx: &PlanCtx) -> bool {
    if let Some(label_str) = label {
        let schema = plan_ctx.schema();
        if let Ok(node_schema) = schema.get_node_schema(label_str) {
            eprintln!(
                "is_label_denormalized: label '{}' is_denormalized = {}",
                label_str, node_schema.is_denormalized
            );
            return node_schema.is_denormalized;
        }
    }
    eprintln!("is_label_denormalized: label {:?} not found or no label, returning false", label);
    false
}

/// Try to generate a ViewScan for a node by looking up the label in the schema from plan_ctx
/// Returns None if schema is not available or label not found.
fn try_generate_view_scan(
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
    // Three cases:
    // 1. Node only in FROM position → Single ViewScan with from_node_properties
    // 2. Node only in TO position → Single ViewScan with to_node_properties  
    // 3. Node in BOTH positions → Union(All) of two ViewScans
    if node_schema.is_denormalized {
        log::info!(
            "✓ Denormalized node-only query for label '{}' - will generate from edge table",
            label
        );
        
        let has_from_props = node_schema.from_properties.is_some();
        let has_to_props = node_schema.to_properties.is_some();
        let source_table = node_schema.denormalized_source_table.as_ref()
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
            label, has_from_props, has_to_props, source_table
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
                props.iter()
                    .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
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
                props.iter()
                    .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
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
            props.iter()
                .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
                .collect()
        });
        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props.iter()
                .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
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
    let mut view_scan = ViewScan::new(
        full_table_name,                    // Use fully qualified table name (database.table)
        None,                               // No filter condition yet
        property_mapping,                   // Property mappings from schema
        node_schema.node_id.column.clone(), // ID column from schema
        vec!["id".to_string()],             // Basic output schema
        vec![],                             // No projections yet
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
            props.iter()
                .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
                .collect()
        });
        
        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props.iter()
                .map(|(k, v)| (k.clone(), crate::graph_catalog::expression_parser::PropertyValue::Column(v.clone())))
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
fn try_generate_relationship_view_scan(
    _alias: &str,
    rel_type: &str,
    plan_ctx: &PlanCtx,
) -> Option<Arc<LogicalPlan>> {
    log::debug!(
        "try_generate_relationship_view_scan: rel_type='{}'",
        rel_type
    );

    // Use plan_ctx.schema() instead of GLOBAL_SCHEMAS
    let schema = plan_ctx.schema();

    // Look up the relationship schema for this type
    let rel_schema = match schema.get_rel_schema(rel_type) {
        Ok(s) => s,
        Err(e) => {
            log::warn!(
                "Could not find relationship schema for type '{}': {:?}",
                rel_type,
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

    // Create property mapping (initially empty - will be populated during projection planning)
    let property_mapping = HashMap::new();

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
    if rel_schema.type_column.is_some() {
        view_scan.type_column = rel_schema.type_column.clone();
        view_scan.from_label_column = rel_schema.from_label_column.clone();
        view_scan.to_label_column = rel_schema.to_label_column.clone();
        
        log::debug!(
            "ViewScan: Populated polymorphic fields for rel '{}' - type_column={:?}",
            rel_type,
            view_scan.type_column
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
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "Creating relationship center for alias '{}', labels: {:?}",
        rel_alias,
        rel_labels
    );
    // Try to generate a ViewScan for the relationship if we have a single type
    if let Some(labels) = rel_labels {
        log::debug!("Relationship has {} labels: {:?}", labels.len(), labels);
        if labels.len() == 1 {
            log::debug!(
                "Trying to create Relationship ViewScan for type '{}'",
                labels[0]
            );
            if let Some(view_scan) =
                try_generate_relationship_view_scan(rel_alias, &labels[0], plan_ctx)
            {
                log::info!(
                    "✓ Successfully created Relationship ViewScan for type '{}'",
                    labels[0]
                );
                return Ok(view_scan);
            } else {
                log::warn!(
                    "Relationship ViewScan creation failed for type '{}', falling back to regular Scan",
                    labels[0]
                );
                // Fallback to regular Scan when schema is not available (e.g., in tests)
                let scan = Scan {
                    table_alias: Some(rel_alias.to_string()),
                    table_name: Some(labels[0].clone()), // Use the relationship type as table name
                };
                return Ok(Arc::new(LogicalPlan::Scan(scan)));
            }
        } else {
            log::debug!(
                "Multiple relationship types ({}), will be handled by CTE generation",
                labels.len()
            );
            // For multiple relationships, create a placeholder scan that will be replaced by CTE generation
            // Use the CTE name as the table name so the plan builder knows to use the CTE
            let cte_name = format!("rel_{}_{}", left_connection, right_connection);
            let placeholder_scan = Scan {
                table_alias: Some(rel_alias.to_string()),
                table_name: Some(cte_name),
            };
            return Ok(Arc::new(LogicalPlan::Scan(placeholder_scan)));
        }
    } else {
        log::debug!("No relationship labels specified, creating regular scan");
        // For relationships without labels, create a regular Scan
        let scan = Scan {
            table_alias: Some(rel_alias.to_string()),
            table_name: None,
        };
        return Ok(Arc::new(LogicalPlan::Scan(scan)));
    }
}

fn convert_properties(props: Vec<Property>) -> LogicalPlanResult<Vec<LogicalExpr>> {
    let mut extracted_props: Vec<LogicalExpr> = vec![];

    for prop in props {
        match prop {
            Property::PropertyKV(property_kvpair) => {
                let op_app = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::Column(Column(property_kvpair.key)),
                        LogicalExpr::Literal(property_kvpair.value),
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
    for (_, table_ctx) in plan_ctx.get_mut_alias_table_ctx_map().iter_mut() {
        let mut extracted_props = convert_properties(table_ctx.get_and_clear_properties())?;
        table_ctx.append_filters(&mut extracted_props);
    }
    Ok(())
}

// Wrapper for backwards compatibility
fn traverse_connected_pattern<'a>(
    connected_patterns: &Vec<ast::ConnectedPattern<'a>>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    path_pattern_idx: usize,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    traverse_connected_pattern_with_mode(
        connected_patterns,
        plan,
        plan_ctx,
        path_pattern_idx,
        None,
        None,
        false,
    )
}

fn traverse_connected_pattern_with_mode<'a>(
    connected_patterns: &Vec<ast::ConnectedPattern<'a>>,
    mut plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    path_pattern_idx: usize,
    shortest_path_mode: Option<ShortestPathMode>,
    path_variable: Option<&str>,
    is_optional: bool,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    eprintln!("\n╔════════════════════════════════════════");
    eprintln!("║ traverse_connected_pattern_with_mode");
    eprintln!("║ connected_patterns.len() = {}", connected_patterns.len());
    eprintln!("║ Current plan type: {:?}", std::mem::discriminant(&*plan));
    eprintln!("╚════════════════════════════════════════\n");

    for (pattern_idx, connected_pattern) in connected_patterns.iter().enumerate() {
        eprintln!("┌─ Processing connected_pattern #{}", pattern_idx);

        let start_node_ref = connected_pattern.start_node.borrow();
        let start_node_label = start_node_ref.label.map(|val| val.to_string());
        let start_node_alias = if let Some(alias) = start_node_ref.name {
            alias.to_string()
        } else {
            generate_id()
        };

        eprintln!(
            "│ Start node: alias='{}', label={:?}",
            start_node_alias, start_node_label
        );

        let start_node_props = start_node_ref
            .properties
            .clone()
            .map(|props| props.into_iter().map(Property::from).collect())
            .unwrap_or_else(Vec::new);

        let rel = &connected_pattern.relationship;
        let rel_alias = if let Some(alias) = rel.name {
            alias.to_string()
        } else {
            generate_id()
        };

        // Handle anonymous edge patterns: [] (no type specified)
        // Automatically expand to UNION of all relationship types from schema
        let rel_labels = match rel.labels.as_ref() {
            Some(labels) => {
                // Explicit labels provided: [:TYPE1|TYPE2]
                Some(labels.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            }
            None => {
                // Anonymous edge pattern: []
                // Expand to all relationship types from schema
                let graph_schema = plan_ctx.schema();
                let all_rel_types: Vec<String> = graph_schema
                    .get_relationships_schemas()
                    .keys()
                    .map(|k| k.to_string())
                    .collect();

                if all_rel_types.is_empty() {
                    log::warn!("Anonymous edge pattern [] but no relationship types in schema");
                    None
                } else {
                    log::info!(
                        "Anonymous edge pattern [] expanded to UNION of {} types: {:?}",
                        all_rel_types.len(),
                        all_rel_types
                    );
                    Some(all_rel_types)
                }
            }
        };

        eprintln!(
            "│ Relationship: alias='{}', labels={:?}, direction={:?}",
            rel_alias, rel_labels, rel.direction
        );

        log::debug!("Parsed relationship labels: {:?}", rel_labels);
        let rel_properties = rel
            .properties
            .clone()
            .map(|props| props.into_iter().map(Property::from).collect())
            .unwrap_or_else(Vec::new);

        let end_node_ref = connected_pattern.end_node.borrow();
        let end_node_alias = if let Some(alias) = end_node_ref.name {
            alias.to_string()
        } else {
            generate_id()
        };
        let end_node_label = end_node_ref.label.map(|val| val.to_string());

        eprintln!(
            "│ End node: alias='{}', label={:?}",
            end_node_alias, end_node_label
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

            // FIX: For multi-hop patterns, use the existing plan as LEFT to create nested structure
            // This ensures (a)-[r1]->(b)-[r2]->(c) becomes GraphRel { left: GraphRel(a-r1-b), center: r2, right: c }
            let (left_node, right_node) = match rel.direction {
                ast::Direction::Outgoing => {
                    // (a)-[:r1]->(b)-[:r2]->(c): existing plan (a-r1-b) on left, new node (c) on right
                    
                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        eprintln!("=== End node '{}' is DENORMALIZED, creating Empty scan ===", end_node_alias);
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
                        })),
                    )
                }
                ast::Direction::Incoming => {
                    // (c)<-[:r2]-(b)<-[:r1]-(a): new node (c) on left, existing plan (b-r1-a) on right
                    
                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        eprintln!("=== End node '{}' is DENORMALIZED, creating Empty scan ===", end_node_alias);
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
                        })),
                        plan.clone(),
                    )
                }
                ast::Direction::Either => {
                    // Either direction: existing plan on left, new node on right
                    
                    // Check if end_node is denormalized - if so, don't create a separate scan
                    let (scan, is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                        eprintln!("=== End node '{}' is DENORMALIZED, creating Empty scan ===", end_node_alias);
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
                        })),
                    )
                }
            };

            let graph_rel_node = GraphRel {
                left: left_node,
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &left_conn,
                    &right_conn,
                    plan_ctx,
                )?,
                right: right_node,
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: left_conn,
                right_connection: right_conn,
                is_rel_anchor: false,
                variable_length: rel.variable_length.clone().map(|v| v.into()),
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: None, // Will be populated by filter pushdown optimization
                labels: rel_labels.clone(),
                is_optional: if is_optional { Some(true) } else { None },
            };
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias,
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

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

            eprintln!("│ ✓ Created GraphRel (start node already in context)");
            eprintln!("│   Plan is now: GraphRel");
            eprintln!("└─ Pattern #{} complete\n", pattern_idx);
        }
        // if end alias already present in ctx map, it means the current nested connected pattern's end node will be connecting at right side plan and start node will be at the left
        else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&end_node_alias) {
            if end_node_label.is_some() {
                table_ctx.set_labels(end_node_label.map(|l| vec![l]));
            }
            if !end_node_props.is_empty() {
                table_ctx.append_properties(end_node_props);
            }

            let (start_scan, start_is_denorm) = if is_label_denormalized(&start_node_label, plan_ctx) {
                eprintln!("=== Start node '{}' is DENORMALIZED, creating Empty scan ===", start_node_alias);
                (Arc::new(LogicalPlan::Empty), true)
            } else {
                let scan = generate_scan(start_node_alias.clone(), start_node_label.clone(), plan_ctx)?;
                let is_d = is_denormalized_scan(&scan);
                (scan, is_d)
            };
            
            let start_graph_node = GraphNode {
                input: start_scan,
                alias: start_node_alias.clone(),
                label: start_node_label.clone().map(|s| s.to_string()),
                is_denormalized: start_is_denorm,
            };
            plan_ctx.insert_table_ctx(
                start_node_alias.clone(),
                TableCtx::build(
                    start_node_alias.clone(),
                    start_node_label.map(|l| vec![l]),
                    start_node_props,
                    false,
                    start_node_ref.name.is_some(),
                ),
            );

            let graph_rel_node = GraphRel {
                left: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &start_node_alias,
                    &end_node_alias,
                    plan_ctx,
                )?,
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: start_node_alias,
                right_connection: end_node_alias,
                is_rel_anchor: false,
                variable_length: rel.variable_length.clone().map(|v| v.into()),
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
            };
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias,
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

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

            eprintln!("│ ✓ Created GraphRel (end node already in context)");
            eprintln!("│   Plan is now: GraphRel");
            eprintln!("└─ Pattern #{} complete\n", pattern_idx);
        }
        // not connected with existing nodes
        else {
            // if two comma separated patterns found and they are not connected to each other i.e. there is no common node alias between them then throw error.
            if path_pattern_idx > 0 {
                // throw error
                return Err(LogicalPlanError::DisconnectedPatternFound);
            }

            // we will keep start graph node at the right side and end at the left side
            eprintln!("=== DISCONNECTED PATTERN: About to create start_graph_node ===");
            
            let (start_scan, start_is_denorm) = if is_label_denormalized(&start_node_label, plan_ctx) {
                eprintln!("=== Start node '{}' is DENORMALIZED, creating Empty scan ===", start_node_alias);
                (Arc::new(LogicalPlan::Empty), true)
            } else {
                let scan = generate_scan(start_node_alias.clone(), start_node_label.clone(), plan_ctx)?;
                eprintln!("=== DISCONNECTED: start_scan created, calling is_denormalized_scan ===");
                let is_d = is_denormalized_scan(&scan);
                eprintln!("=== DISCONNECTED: start_is_denorm = {} ===", is_d);
                (scan, is_d)
            };
            
            let start_graph_node = GraphNode {
                input: start_scan,
                alias: start_node_alias.clone(),
                label: start_node_label.clone().map(|s| s.to_string()),
                is_denormalized: start_is_denorm,
            };
            eprintln!("=== DISCONNECTED: start_graph_node created with is_denormalized={} ===", start_graph_node.is_denormalized);
            plan_ctx.insert_table_ctx(
                start_node_alias.clone(),
                TableCtx::build(
                    start_node_alias.clone(),
                    start_node_label.map(|l| vec![l]),
                    start_node_props,
                    false,
                    start_node_ref.name.is_some(),
                ),
            );

            let (end_scan, end_is_denorm) = if is_label_denormalized(&end_node_label, plan_ctx) {
                eprintln!("=== End node '{}' is DENORMALIZED, creating Empty scan ===", end_node_alias);
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
            };
            plan_ctx.insert_table_ctx(
                end_node_alias.clone(),
                TableCtx::build(
                    end_node_alias.clone(),
                    end_node_label.map(|l| vec![l]),
                    end_node_props,
                    false,
                    end_node_ref.name.is_some(),
                ),
            );

            let (left_conn, right_conn) = match rel.direction {
                ast::Direction::Outgoing => (start_node_alias, end_node_alias),
                ast::Direction::Incoming => (end_node_alias, start_node_alias),
                ast::Direction::Either => (start_node_alias, end_node_alias),
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

            let graph_rel_node = GraphRel {
                left: left_node,
                center: generate_relationship_center(
                    &rel_alias,
                    &rel_labels,
                    &left_conn,
                    &right_conn,
                    plan_ctx,
                )?,
                right: right_node,
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: left_conn.clone(), // Left node is the start node (left_conn for Outgoing)
                right_connection: right_conn.clone(), // Right node is the end node (right_conn for Outgoing)
                is_rel_anchor: false,
                variable_length: rel.variable_length.clone().map(|v| v.into()),
                shortest_path_mode: shortest_path_mode.clone(),
                path_variable: path_variable.map(|s| s.to_string()),
                where_predicate: None, // Will be populated by filter pushdown optimization
                labels: rel_labels.clone(),
                is_optional: if is_optional { Some(true) } else { None },
            };
            plan_ctx.insert_table_ctx(
                rel_alias.clone(),
                TableCtx::build(
                    rel_alias,
                    rel_labels,
                    rel_properties,
                    true,
                    rel.name.is_some(),
                ),
            );

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

            eprintln!("│ ✓ Created GraphRel (first pattern - disconnected)");
            eprintln!("│   Plan is now: GraphRel");
            eprintln!("└─ Pattern #{} complete\n", pattern_idx);
        }
    }

    eprintln!("╔════════════════════════════════════════");
    eprintln!("║ traverse_connected_pattern_with_mode COMPLETE");
    eprintln!("║ Final plan type: {:?}", std::mem::discriminant(&*plan));
    eprintln!("╚════════════════════════════════════════\n");

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
    let node_label: Option<String> = node_pattern.label.map(|val| val.to_string());
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
            log::info!("✓ Wrapping Union branches in GraphNodes for alias '{}'", node_alias);
            let wrapped_inputs: Vec<Arc<LogicalPlan>> = union.inputs.iter().map(|branch| {
                let is_denorm = is_denormalized_scan(branch);
                Arc::new(LogicalPlan::GraphNode(GraphNode {
                    input: branch.clone(),
                    alias: node_alias.clone(),
                    label: node_label.clone().map(|s| s.to_string()),
                    is_denormalized: is_denorm,
                }))
            }).collect();
            
            let wrapped_union = Union {
                inputs: wrapped_inputs,
                union_type: union.union_type.clone(),
            };
            return Ok(Arc::new(LogicalPlan::Union(wrapped_union)));
        }
        
        // Normal case: single ViewScan wrapped in GraphNode
        let is_denorm = is_denormalized_scan(&scan);
        let graph_node = GraphNode {
            input: scan,
            alias: node_alias,
            label: node_label.map(|s| s.to_string()),
            is_denormalized: is_denorm,
        };
        Ok(Arc::new(LogicalPlan::GraphNode(graph_node)))
    }
}

pub fn evaluate_match_clause<'a>(
    match_clause: &ast::MatchClause<'a>,
    mut plan: Arc<LogicalPlan>,
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
    for (idx, path_pattern) in match_clause.path_patterns.iter().enumerate() {
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
                    match_clause.path_variable,
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
                    match_clause.path_variable,
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
                    match_clause.path_variable,
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
    use crate::query_planner::logical_expr::{Direction, Literal, PropertyKVPair};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_convert_properties_with_kv_pairs() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: Literal::String("John".to_string()),
            }),
            Property::PropertyKV(PropertyKVPair {
                key: "age".to_string(),
                value: Literal::Integer(30),
            }),
        ];

        let result = convert_properties(properties).unwrap();
        assert_eq!(result.len(), 2);

        // Check first property conversion
        match &result[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                assert_eq!(op_app.operands.len(), 2);
                match &op_app.operands[0] {
                    LogicalExpr::Column(col) => assert_eq!(col.0, "name"),
                    _ => panic!("Expected Column"),
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
                value: Literal::String("Alice".to_string()),
            }),
            Property::Param("param1".to_string()),
        ];

        let result = convert_properties(properties);
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::FoundParamInProperties => (), // Expected error
            _ => panic!("Expected FoundParamInProperties error"),
        }
    }

    #[test]
    fn test_convert_properties_empty_list() {
        let properties = vec![];
        let result = convert_properties(properties).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_generate_id_uniqueness() {
        let id1 = generate_id();
        let id2 = generate_id();

        // IDs should be unique
        assert_ne!(id1, id2);

        // IDs should start with 'a'
        assert!(id1.starts_with('a'));
        assert!(id2.starts_with('a'));

        // IDs should be reasonable length (not too short or too long)
        assert!(id1.len() > 1 && id1.len() < 20);
        assert!(id2.len() > 1 && id2.len() < 20);
    }

    #[test]
    fn test_traverse_node_pattern_new_node() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let node_pattern = ast::NodePattern {
            name: Some("customer"),
            label: Some("Person"),
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
                // Input should be a ViewScan or Scan
                match graph_node.input.as_ref() {
                    LogicalPlan::ViewScan(_view_scan) => {
                        // ViewScan created successfully via try_generate_view_scan
                        // This happens when GLOBAL_GRAPH_SCHEMA is available
                    }
                    LogicalPlan::Scan(scan) => {
                        // Fallback Scan when ViewScan creation fails or schema not available
                        assert_eq!(scan.table_alias, Some("customer".to_string()));
                        assert_eq!(scan.table_name, Some("Person".to_string())); // Now we pass the label!
                    }
                    _ => panic!("Expected ViewScan or Scan as input"),
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
            label: Some("Person"), // Different label
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
            label: Some("Person"),
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
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let start_node = ast::NodePattern {
            name: Some("user"),
            label: Some("Person"),
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("company"),
            label: Some("Organization"),
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
        let mut plan_ctx = PlanCtx::default();
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
            label: Some("Employee"), // Different label
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("project"),
            label: Some("Project"),
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

    #[test]
    fn test_traverse_connected_pattern_disconnected_error() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let start_node = ast::NodePattern {
            name: Some("user1"),
            label: Some("Person"),
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("user2"),
            label: Some("Person"),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("knows"),
            direction: ast::Direction::Either,
            labels: Some(vec!["KNOWS"]),
            properties: None,
            variable_length: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        // Pass path_pattern_idx > 0 to simulate second pattern that's disconnected
        let result =
            traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 1);

        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::DisconnectedPatternFound => (), // Expected error
            _ => panic!("Expected DisconnectedPatternFound error"),
        }
    }

    #[test]
    fn test_evaluate_match_clause_with_node_and_connected_pattern() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Create a match clause with both node pattern and connected pattern
        let node_pattern = ast::NodePattern {
            name: Some("admin"),
            label: Some("User"),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "role",
                value: ast::Expression::Literal(ast::Literal::String("administrator")),
            })]),
        };

        let start_node = ast::NodePattern {
            name: Some("admin"), // Same as above - should connect
            label: None,
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("system"),
            label: Some("System"),
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
            path_variable: None,
            path_patterns: vec![
                ast::PathPattern::Node(node_pattern),
                ast::PathPattern::ConnectedPattern(vec![connected_pattern]),
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
            value: Literal::String("active".to_string()),
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
                    LogicalExpr::Column(col) => assert_eq!(col.0, "status"),
                    _ => panic!("Expected Column"),
                }
            }
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_generate_scan() {
        // Create empty schema for test
        use crate::graph_catalog::graph_schema::GraphSchema;
        use std::collections::HashMap;
        let schema = Arc::new(GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
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
            LogicalPlan::Scan(scan_plan) => {
                assert_eq!(scan_plan.table_alias, Some("customers".to_string()));
                assert_eq!(scan_plan.table_name, Some("Customer".to_string()));
            }
            _ => panic!("Expected Scan plan"),
        }
    }
}
