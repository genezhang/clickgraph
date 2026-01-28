//! ViewScan generation for MATCH clause processing.
//!
//! This module handles the creation of ViewScan logical plan nodes for both
//! node patterns and relationship patterns. It encapsulates the complex logic
//! for:
//! - Regular (non-denormalized) node ViewScans
//! - Denormalized node ViewScans (nodes stored as columns in edge tables)
//! - Multi-table UNION ALL for nodes appearing in multiple tables
//! - Relationship ViewScans with polymorphic edge support
//! - Schema filter propagation
//! - Parameterized view support

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::errors::LogicalPlanError;
use crate::query_planner::logical_plan::plan_builder::LogicalPlanResult;
use crate::query_planner::logical_plan::{LogicalPlan, Union, UnionType, ViewScan};
use crate::query_planner::plan_ctx::PlanCtx;

/// Try to generate a ViewScan for a node by looking up the label in the schema from plan_ctx.
///
/// This function handles several complex cases:
/// 1. **Denormalized nodes in multiple tables**: Creates UNION ALL of ViewScans
/// 2. **Denormalized nodes with both positions**: Creates UNION ALL of FROM and TO branches
/// 3. **Multi-table labels**: Same label in different tables → UNION ALL
/// 4. **Standard nodes**: Single ViewScan from node table
///
/// # Returns
/// - `Ok(Some(plan))` - Successfully created ViewScan or Union plan
/// - `Ok(None)` - Label not found in schema (caller should handle)
/// - `Err(...)` - Invalid schema configuration
pub fn try_generate_view_scan(
    _alias: &str,
    label: &str,
    plan_ctx: &PlanCtx,
) -> Result<Option<Arc<LogicalPlan>>, LogicalPlanError> {
    log::debug!("try_generate_view_scan: label='{}'", label);

    // Use plan_ctx.schema() instead of GLOBAL_SCHEMAS
    let schema = plan_ctx.schema();

    // Look up the node schema for this label
    let node_schema = match schema.node_schema(label) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("Could not find node schema for label '{}': {:?}", label, e);
            return Ok(None);
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
                    "✓ Denormalized node '{}' appears in {} relationship type(s) - creating multi-table UNION",
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
                                log::debug!(
                                    "Adding FROM branch. union_inputs before push: len={}",
                                    union_inputs.len()
                                );

                                // Populate property_mapping from from_props so full node expansion works
                                let property_mapping: HashMap<String, PropertyValue> = from_props
                                    .iter()
                                    .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                                    .collect();

                                // Get the actual ID column name from node_id property
                                let id_prop_name = node_schema
                                    .node_id
                                    .columns()
                                    .first()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| "id".to_string());
                                let id_column = from_props
                                    .get(&id_prop_name)
                                    .cloned()
                                    .unwrap_or_else(|| id_prop_name.clone());

                                log::info!(
                                    "✓ FROM branch for '{}': id_prop='{}', id_column='{}', {} properties",
                                    label, id_prop_name, id_column, property_mapping.len()
                                );

                                let mut from_scan = ViewScan::new(
                                    full_table_name.clone(),
                                    None,
                                    property_mapping.clone(),
                                    id_column,
                                    vec![],
                                    vec![],
                                );
                                from_scan.is_denormalized = true;
                                from_scan.from_node_properties = Some(property_mapping);
                                log::debug!(
                                    "FROM ViewScan properties: from={:?}, to={:?}",
                                    from_scan
                                        .from_node_properties
                                        .as_ref()
                                        .map(|p| p.keys().collect::<Vec<_>>()),
                                    from_scan
                                        .to_node_properties
                                        .as_ref()
                                        .map(|p| p.keys().collect::<Vec<_>>())
                                );
                                union_inputs
                                    .push(Arc::new(LogicalPlan::ViewScan(Arc::new(from_scan))));
                                log::debug!(
                                    "Added FROM branch. union_inputs after push: len={}",
                                    union_inputs.len()
                                );
                            }
                        }

                        // Check if this node is in TO position
                        if rel_schema.to_node == label {
                            log::debug!(
                                "Checking TO position. to_node='{}', label='{}', has to_node_properties: {}",
                                rel_schema.to_node, label, rel_schema.to_node_properties.is_some()
                            );
                            if let Some(ref to_props) = rel_schema.to_node_properties {
                                log::debug!("TO props: {:?}", to_props.keys().collect::<Vec<_>>());
                                log::debug!(
                                    "✓ Adding TO branch for '{}' from table '{}' (rel: {})",
                                    label,
                                    full_table_name,
                                    rel_type
                                );
                                log::debug!(
                                    "Adding TO branch. union_inputs before push: len={}",
                                    union_inputs.len()
                                );

                                // Populate property_mapping from to_props so full node expansion works
                                let property_mapping: HashMap<String, PropertyValue> = to_props
                                    .iter()
                                    .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                                    .collect();

                                // Get the actual ID column name from node_id property
                                let id_prop_name = node_schema
                                    .node_id
                                    .columns()
                                    .first()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| "id".to_string());
                                let id_column = to_props
                                    .get(&id_prop_name)
                                    .cloned()
                                    .unwrap_or_else(|| id_prop_name.clone());

                                log::info!(
                                    "✓ TO branch for '{}': id_prop='{}', id_column='{}', {} properties",
                                    label, id_prop_name, id_column, property_mapping.len()
                                );

                                let mut to_scan = ViewScan::new(
                                    full_table_name.clone(),
                                    None,
                                    property_mapping.clone(),
                                    id_column,
                                    vec![],
                                    vec![],
                                );
                                to_scan.is_denormalized = true;
                                to_scan.to_node_properties = Some(property_mapping);
                                log::debug!(
                                    "TO ViewScan properties: from={:?}, to={:?}",
                                    to_scan
                                        .from_node_properties
                                        .as_ref()
                                        .map(|p| p.keys().collect::<Vec<_>>()),
                                    to_scan
                                        .to_node_properties
                                        .as_ref()
                                        .map(|p| p.keys().collect::<Vec<_>>())
                                );
                                union_inputs
                                    .push(Arc::new(LogicalPlan::ViewScan(Arc::new(to_scan))));
                                log::debug!(
                                    "Added TO branch. union_inputs after push: len={}",
                                    union_inputs.len()
                                );
                            }
                        }
                    }
                }

                if union_inputs.is_empty() {
                    log::error!("No ViewScans generated for denormalized node '{}'", label);
                    return Ok(None);
                }

                if union_inputs.len() == 1 {
                    log::info!(
                        "✓ Single ViewScan for denormalized node '{}' (only one source)",
                        label
                    );
                    // Safe: we just checked that union_inputs.len() == 1
                    if let Some(plan) = union_inputs.pop() {
                        return Ok(Some(plan));
                    }
                }

                let union = Union {
                    inputs: union_inputs,
                    union_type: UnionType::All,
                };

                log::info!(
                    "✓ Created UNION ALL with {} branches for denormalized node '{}'",
                    union.inputs.len(),
                    label
                );
                return Ok(Some(Arc::new(LogicalPlan::Union(union))));
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
                LogicalPlanError::InvalidSchema {
                    label: label.to_string(),
                    reason: "Denormalized node missing source table".to_string(),
                }
            })?;

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
                "✓✓✓ SINGLE-TABLE CASE: Denormalized node '{}' has BOTH positions - creating UNION ALL ✓✓✓",
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
                    .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
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
                    .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                    .collect()
            });
            to_scan.schema_filter = node_schema.filter.clone();
            // Note: from_node_properties is None - this is the TO branch

            // Create Union of the two ViewScans
            let union = Union {
                inputs: vec![
                    Arc::new(LogicalPlan::ViewScan(Arc::new(from_scan))),
                    Arc::new(LogicalPlan::ViewScan(Arc::new(to_scan))),
                ],
                union_type: UnionType::All,
            };

            log::info!(
                ">>>SINGLE-TABLE CASE: Created UNION with 2 branches for '{}' <<<",
                label
            );
            return Ok(Some(Arc::new(LogicalPlan::Union(union))));
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
                .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                .collect()
        });
        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                .collect()
        });
        view_scan.schema_filter = node_schema.filter.clone();

        log::info!(
            "✓ Created denormalized ViewScan for '{}' (single position)",
            label
        );

        return Ok(Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan)))));
    }

    // MULTI_TABLE_LABEL CHECK: Non-denormalized nodes with same label in multiple tables
    // This happens when the config has multiple node definitions with the same label but different tables
    let all_schemas_for_label = schema.get_all_node_schemas_for_label(label);
    if all_schemas_for_label.len() > 1 {
        log::info!(
            "✓ MULTI_TABLE_LABEL: Found '{}' in {} different tables - creating UNION ALL",
            label,
            all_schemas_for_label.len()
        );

        let mut union_inputs: Vec<Arc<LogicalPlan>> = Vec::new();

        for (_composite_key, other_schema) in all_schemas_for_label {
            let full_table_name = format!("{}.{}", other_schema.database, other_schema.table_name);
            let id_column = other_schema
                .node_id
                .columns()
                .first()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "id".to_string());

            let mut view_scan = ViewScan::new(
                full_table_name,
                None,
                other_schema.property_mappings.clone(),
                id_column,
                vec![],
                vec![],
            );

            view_scan.schema_filter = other_schema.filter.clone();
            log::debug!(
                "Added ViewScan for '{}' from table '{}.{}'",
                label,
                other_schema.database,
                other_schema.table_name
            );

            union_inputs.push(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))));
        }

        if union_inputs.len() > 1 {
            let union = Union {
                inputs: union_inputs,
                union_type: UnionType::All,
            };

            log::info!(
                "✓ Created MULTI_TABLE_LABEL UNION with {} branches for '{}'",
                union.inputs.len(),
                label
            );
            return Ok(Some(Arc::new(LogicalPlan::Union(union))));
        }
    }

    // SINGLE-TABLE CASE OR NON-DENORMALIZED: Use standard ViewScan logic
    log::info!(
        "✓ ViewScan: Resolved label '{}' to table '{}'",
        label,
        node_schema.table_name
    );

    // Use property mapping from schema directly (already PropertyValue)
    // For denormalized nodes, property_mappings is often empty because properties
    // are stored in from_properties/to_properties. Merge them into property_mapping
    // so that full node expansion (RETURN n) works correctly for MULTI_TABLE_LABEL schemas.
    let mut property_mapping = node_schema.property_mappings.clone();

    if node_schema.is_denormalized && property_mapping.is_empty() {
        // Merge from_properties and to_properties into property_mapping
        // This enables full node expansion to find the actual column names
        if let Some(ref from_props) = node_schema.from_properties {
            for (prop_name, col_name) in from_props.iter() {
                property_mapping.insert(prop_name.clone(), PropertyValue::Column(col_name.clone()));
            }
        }
        if let Some(ref to_props) = node_schema.to_properties {
            for (prop_name, col_name) in to_props.iter() {
                // Only add if not already present (from_properties takes precedence)
                property_mapping
                    .entry(prop_name.clone())
                    .or_insert_with(|| PropertyValue::Column(col_name.clone()));
            }
        }

        if !property_mapping.is_empty() {
            log::info!(
                "✓ Populated property_mapping for denormalized node '{}' with {} properties: {:?}",
                label,
                property_mapping.len(),
                property_mapping.keys().collect::<Vec<_>>()
            );
        }
    }

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
    // For denormalized nodes, node_id refers to the property name (e.g., "ip"),
    // but we need the actual column name (e.g., "id.orig_h") for SQL generation.
    // Look it up from from_properties/to_properties for denormalized schemas.
    let id_column = if node_schema.is_denormalized {
        // Get the node_id property name first
        let id_prop_name = node_schema
            .node_id
            .columns()
            .first()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "id".to_string());

        // Look up the actual column name from from_properties or to_properties
        let actual_column = node_schema
            .from_properties
            .as_ref()
            .and_then(|props| props.get(&id_prop_name))
            .or_else(|| {
                node_schema
                    .to_properties
                    .as_ref()
                    .and_then(|props| props.get(&id_prop_name))
            })
            .cloned()
            .unwrap_or_else(|| {
                log::warn!(
                    "Denormalized node '{}' ID property '{}' not found in from/to_properties, using as-is",
                    label,
                    id_prop_name
                );
                id_prop_name.clone()
            });

        log::info!(
            "✓ Resolved denormalized node '{}' ID column: '{}' (property) → '{}' (column)",
            label,
            id_prop_name,
            actual_column
        );
        actual_column
    } else {
        // For non-denormalized nodes, node_id IS the actual column name
        node_schema
            .node_id
            .columns()
            .first()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                log::error!("Node schema for '{}' has no ID columns defined", label);
                // Don't hardcode "id" - this causes bugs with auto_discover_columns
                // where the actual column might be user_id, object_id, etc.
                // This should never happen in valid schemas.
                LogicalPlanError::InvalidSchema {
                    label: label.to_string(),
                    reason: "No ID columns defined in node schema".to_string(),
                }
            })?
    };

    let mut view_scan = ViewScan::new(
        full_table_name,        // Use fully qualified table name (database.table)
        None,                   // No filter condition yet
        property_mapping,       // Property mappings from schema
        id_column,              // ID column from schema (first for composite)
        vec!["id".to_string()], // Basic output schema
        vec![],                 // No projections yet
    );

    // Set view parameters if this is a parameterized view
    view_scan.view_parameter_names = view_parameter_names.clone();
    view_scan.view_parameter_values = view_parameter_values.clone();
    log::debug!(
        "ViewScan created for '{}': param_names={:?}, param_values={:?}",
        label,
        view_parameter_names,
        view_parameter_values
    );

    // Set denormalized flag and properties from schema
    view_scan.is_denormalized = node_schema.is_denormalized;

    // Populate denormalized node properties (for role-based mapping)
    if node_schema.is_denormalized {
        // Convert from HashMap<String, String> to HashMap<String, PropertyValue>
        view_scan.from_node_properties = node_schema.from_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
                .collect()
        });

        view_scan.to_node_properties = node_schema.to_properties.as_ref().map(|props| {
            props
                .iter()
                .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
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

    Ok(Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan)))))
}

/// Try to generate a ViewScan for a relationship by looking up the relationship type in the schema.
///
/// This function handles:
/// - Single relationship type lookups with node context for disambiguation
/// - Property mapping propagation from schema
/// - Polymorphic edge field population (type_column, label columns)
/// - Denormalized node property propagation
/// - Schema filter application
/// - Parameterized view support
///
/// # Arguments
/// - `_alias` - The relationship alias (currently unused, reserved for future use)
/// - `rel_type` - The relationship type name to look up
/// - `left_node_label` - Optional left node label for disambiguation
/// - `right_node_label` - Optional right node label for disambiguation
/// - `plan_ctx` - Planning context containing schema information
///
/// # Returns
/// - `Some(plan)` - Successfully created relationship ViewScan
/// - `None` - Relationship type not found in schema
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
    let rel_schema =
        match schema.get_rel_schema_with_nodes(rel_type, left_node_label, right_node_label) {
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
        full_table_name,
        None,
        property_mapping,
        rel_schema.from_id.clone(),
        vec!["id".to_string()],
        vec![],
        rel_schema.from_id.clone(),
        rel_schema.to_id.clone(),
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
            .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
            .collect()
    });
    view_scan.to_node_properties = rel_schema.to_node_properties.as_ref().map(|props| {
        props
            .iter()
            .map(|(k, v)| (k.clone(), PropertyValue::Column(v.clone())))
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

/// Generate a relationship center (ViewScan if possible, otherwise Empty plan).
///
/// This function is used internally during pattern processing to create the
/// logical plan node for a relationship. It handles:
/// - Single relationship types: Creates ViewScan via `try_generate_relationship_view_scan`
/// - Multiple relationship types (e.g., `[:TYPE1|TYPE2]`): Returns Empty plan
///   (actual UNION ALL CTE generation happens in render phase using GraphRel.labels)
/// - No specified type: Returns Empty plan (type inference will fill in later)
///
/// # Arguments
/// - `rel_alias` - Alias for the relationship variable
/// - `rel_labels` - Optional list of relationship type names
/// - `_left_connection` - Left node connection (reserved)
/// - `_right_connection` - Right node connection (reserved)
/// - `left_node_label` - Optional left node label for disambiguation
/// - `right_node_label` - Optional right node label for disambiguation
/// - `plan_ctx` - Planning context
///
/// # Returns
/// - `Ok(plan)` - ViewScan or Empty plan
/// - `Err(...)` - Relationship not found when single type specified
pub fn generate_relationship_center(
    rel_alias: &str,
    rel_labels: &Option<Vec<String>>,
    _left_connection: &str,
    _right_connection: &str,
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
            if let Some(view_scan) = try_generate_relationship_view_scan(
                rel_alias,
                &unique_labels[0],
                left_node_label.as_ref().map(|s| s.as_str()),
                right_node_label.as_ref().map(|s| s.as_str()),
                plan_ctx,
            ) {
                log::info!(
                    "✓ Successfully created Relationship ViewScan for type '{}'",
                    unique_labels[0]
                );
                Ok(view_scan)
            } else {
                // ViewScan creation failed - this is an error
                Err(LogicalPlanError::RelationshipNotFound(
                    unique_labels[0].clone(),
                ))
            }
        } else {
            log::debug!(
                "Multiple relationship types ({}), using Empty plan (CTE uses GraphRel.labels)",
                unique_labels.len()
            );
            // For multiple relationships, use Empty plan
            // The actual UNION ALL CTE generation happens in render phase using GraphRel.labels
            // No need for "rel_*" placeholder - it was never actually looked up
            Ok(Arc::new(LogicalPlan::Empty))
        }
    } else {
        log::debug!("No relationship labels specified, using Empty plan");
        // For relationships without labels, use Empty
        // Type inference pass will fill in the relationship type
        Ok(Arc::new(LogicalPlan::Empty))
    }
}

#[cfg(test)]
mod tests {
    // Tests would go here - currently these functions are tested via integration tests
    // since they require a full GraphSchema setup
}
