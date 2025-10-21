//! Query planning for view-based queries

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph_catalog::GraphViewDefinition;
use crate::query_planner::{
    logical_expr::{ColumnAlias, LogicalExpr},
    logical_plan::{LogicalPlan, ViewScan},
    plan_ctx::PlanCtx,
};

use super::LogicalPlanResult;

/// Plan a view-based node scan
#[allow(dead_code)]
pub fn plan_view_node_scan(
    view: &GraphViewDefinition,
    label: &str,
    properties: &[String],
    _plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    // Get node mapping from view
    let node_mapping = view.nodes.get(label)
        .ok_or_else(|| super::errors::LogicalPlanError::NodeNotFound(label.to_string()))?;

    // Create property mapping
    let mut property_mapping = HashMap::new();
    let mut projections = Vec::new();
    let mut output_schema = Vec::new();

    // Map requested properties
    for prop in properties {
        if let Some(source_col) = node_mapping.property_mappings.get(prop) {
            property_mapping.insert(prop.clone(), source_col.clone());
            projections.push(LogicalExpr::ColumnAlias(ColumnAlias(format!("{}.{}", node_mapping.source_table.clone(), source_col.clone()))));
            output_schema.push(prop.clone());
        }
    }

    // Always include ID column
    property_mapping.insert("id".to_string(), node_mapping.id_column.clone());
    projections.push(LogicalExpr::ColumnAlias(ColumnAlias(format!("{}.{}", node_mapping.source_table, node_mapping.id_column))));
    output_schema.push("id".to_string());

    // Create view scan
    let scan = ViewScan::new(
        node_mapping.source_table.clone(),
        node_mapping.filter_condition.as_ref().map(|f| LogicalExpr::Raw(f.clone())),
        property_mapping,
        node_mapping.id_column.clone(),
        output_schema,
        projections,
    );

    Ok(Arc::new(LogicalPlan::ViewScan(Arc::new(scan))))
}

/// Plan a view-based relationship scan
#[allow(dead_code)]
pub fn plan_view_relationship_scan(
    view: &GraphViewDefinition,
    type_name: &str,
    properties: &[String],
    from_plan: Arc<LogicalPlan>,
    _plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    // Get relationship mapping from view
    let rel_mapping = view.relationships.get(type_name)
        .ok_or_else(|| super::errors::LogicalPlanError::RelationshipNotFound(type_name.to_string()))?;

    // Create property mapping
    let mut property_mapping = HashMap::new();
    let mut projections = Vec::new();
    let mut output_schema = Vec::new();

    // Map requested properties
    for prop in properties {
        if let Some(source_col) = rel_mapping.property_mappings.get(prop) {
            property_mapping.insert(prop.clone(), source_col.clone());
            projections.push(LogicalExpr::ColumnAlias(ColumnAlias(format!("{}.{}", rel_mapping.source_table, source_col))));
            output_schema.push(prop.clone());
        }
    }

    // Include source and target ID columns
    property_mapping.insert("from_id".to_string(), rel_mapping.from_column.clone());
    property_mapping.insert("to_id".to_string(), rel_mapping.to_column.clone());
    
    projections.push(LogicalExpr::ColumnAlias(ColumnAlias(format!("{}.{}", rel_mapping.source_table, rel_mapping.from_column))));
    projections.push(LogicalExpr::ColumnAlias(ColumnAlias(format!("{}.{}", rel_mapping.source_table, rel_mapping.to_column))));
    
    output_schema.push("from_id".to_string());
    output_schema.push("to_id".to_string());

    // Create view scan with input plan - use relationship constructor to include column info
    let scan = ViewScan::relationship_with_input(
        rel_mapping.source_table.clone(),
        rel_mapping.filter_condition.as_ref().map(|f| LogicalExpr::Raw(f.clone())),
        property_mapping,
        rel_mapping.from_column.clone(), // Use from_column as primary ID
        output_schema,
        projections,
        rel_mapping.from_column.clone(), // Source node column
        rel_mapping.to_column.clone(),   // Target node column
        from_plan,
    );

    Ok(Arc::new(LogicalPlan::ViewScan(Arc::new(scan))))
}
