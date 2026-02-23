//! CTE extraction utilities for variable-length path handling
//!
//! Some functions in this module are reserved for future use or used only in specific code paths.
// Note: Helper functions for VLP CTE generation are kept for complex path patterns
#![allow(dead_code)]

use crate::clickhouse_query_generator::variable_length_cte::NodeProperty;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::graph_catalog::pattern_schema::{JoinStrategy, PatternSchemaContext};
use crate::query_planner::join_context::{
    VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
};
use crate::query_planner::logical_expr::expression_rewriter::{
    rewrite_projection_items_with_property_mapping, ExpressionRewriteContext,
};
use crate::query_planner::logical_expr::ColumnAlias as LogicalColumnAlias;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::cte_generation::CteGenerationContext;
use crate::render_plan::cte_manager::CteManager;
use crate::render_plan::expression_utils::{flatten_addition_operands, has_string_operand};
use crate::utils::cte_column_naming::cte_column_name;
use crate::utils::cte_naming::generate_cte_name;

use super::errors::RenderBuildError;
use super::filter_pipeline::{categorize_filters, CategorizedFilters};
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr};
use super::{Cte, CteContent, Join, JoinType};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

// ============================================================================
// Pattern Schema Context Recreation for CTE Generation
// ============================================================================

/// Recreate PatternSchemaContext from a GraphRel during CTE extraction.
///
/// During the analyzer phase, PatternSchemaContext is created and stored in PlanCtx.
/// However, the render phase (where CTE extraction happens) doesn't have access to PlanCtx.
/// This helper recreates the context from the GraphRel's information + GraphSchema.
///
/// This is part of Phase 2 of schema consolidation (replacing scattered is_denormalized/is_fk_edge checks).
fn recreate_pattern_schema_context(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    schema: &GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> Result<PatternSchemaContext, RenderBuildError> {
    // Get relationship types first — needed for label inference when nodes are unlabeled
    let rel_types = graph_rel
        .labels
        .clone()
        .unwrap_or_else(|| vec!["UNKNOWN".to_string()]);

    // Resolve node labels using a 3-tier strategy:
    // 1. Explicit label from plan tree (GraphNode.label — set by parser)
    // 2. Inferred label from PlanCtx (TableCtx.labels — set by type inference)
    // 3. Inferred from relationship schema's from_node/to_node fields
    let explicit_left = extract_node_labels(&graph_rel.left).and_then(|l| l.first().cloned());
    let explicit_right = extract_node_labels(&graph_rel.right).and_then(|l| l.first().cloned());

    // Tier 2: Check PlanCtx for inferred labels (type inference results)
    let left_from_ctx = if explicit_left.is_none() {
        plan_ctx
            .and_then(|ctx| ctx.get_table_ctx(&graph_rel.left_connection).ok())
            .and_then(|tc| tc.get_labels().and_then(|l| l.first().cloned()))
    } else {
        None
    };
    let right_from_ctx = if explicit_right.is_none() {
        plan_ctx
            .and_then(|ctx| ctx.get_table_ctx(&graph_rel.right_connection).ok())
            .and_then(|tc| tc.get_labels().and_then(|l| l.first().cloned()))
    } else {
        None
    };

    let resolved_left = explicit_left.or(left_from_ctx);
    let resolved_right = explicit_right.or(right_from_ctx);

    // Tier 3: Infer from relationship schema when still missing
    // GraphRel convention: left = source (from_node), right = target (to_node).
    let (left_label, right_label) =
        infer_node_labels_from_rel(resolved_left, resolved_right, &rel_types, schema)?;

    log::debug!(
        "recreate_pattern_schema_context: left='{}', right='{}', rel={:?}",
        left_label,
        right_label,
        rel_types
    );

    // Get node schemas
    let left_node_schema = schema.node_schema(&left_label).map_err(|e| {
        RenderBuildError::MissingTableInfo(format!("Could not get left node schema: {}", e))
    })?;

    let right_node_schema = schema.node_schema(&right_label).map_err(|e| {
        RenderBuildError::MissingTableInfo(format!("Could not get right node schema: {}", e))
    })?;

    // Get relationship schema with node context for precise matching
    let rel_schema = schema
        .get_rel_schema_with_nodes(
            rel_types.first().unwrap(),
            Some(&left_label),
            Some(&right_label),
        )
        .map_err(|e| {
            RenderBuildError::MissingTableInfo(format!("Could not get relationship schema: {}", e))
        })?;

    // Recreate PatternSchemaContext using the same analysis logic
    PatternSchemaContext::analyze(
        &graph_rel.left_connection,  // left_node_alias
        &graph_rel.right_connection, // right_node_alias
        left_node_schema,
        right_node_schema,
        rel_schema,
        schema,
        &graph_rel.alias, // rel_alias
        rel_types,
        None, // prev_edge_info - not needed for CTE generation
    )
    .map_err(|e| {
        RenderBuildError::MissingTableInfo(format!("PatternSchemaContext analysis failed: {}", e))
    })
}

/// Infer missing node labels from the relationship schema's from_node/to_node fields.
///
/// When a node has no explicit label (common after WITH→CTE replacement where the
/// original typed node becomes an untyped CTE reference), we can infer the label
/// from the relationship schema since it declares what node types it connects.
///
/// GraphRel convention: left = source (from_node), right = target (to_node).
fn infer_node_labels_from_rel(
    explicit_left: Option<String>,
    explicit_right: Option<String>,
    rel_types: &[String],
    schema: &GraphSchema,
) -> Result<(String, String), RenderBuildError> {
    // If both labels are already known, no inference needed
    if let (Some(left), Some(right)) = (&explicit_left, &explicit_right) {
        return Ok((left.clone(), right.clone()));
    }

    let left_explicit = explicit_left.is_some();
    let right_explicit = explicit_right.is_some();

    let rel_type = rel_types.first().ok_or_else(|| {
        RenderBuildError::MissingTableInfo(
            "No relationship type available for label inference".to_string(),
        )
    })?;

    // rel_type may be a simple name ("REPLY_OF") or composite key ("REPLY_OF::Comment::Post").
    // Extract simple type name for rel_type_index lookup.
    let simple_type = if let Some(idx) = rel_type.find("::") {
        &rel_type[..idx]
    } else {
        rel_type.as_str()
    };

    let rel_schemas = schema.rel_schemas_for_type(simple_type);
    if rel_schemas.is_empty() {
        return Err(RenderBuildError::MissingTableInfo(format!(
            "No relationship schemas found for type '{}' during label inference",
            simple_type
        )));
    }

    // Filter rel schemas by the known label to narrow candidates
    let left_label = if let Some(left) = explicit_left {
        left
    } else {
        // Infer left (from_node) — filter by right label if known
        let candidates: Vec<&str> = if let Some(ref right) = explicit_right {
            rel_schemas
                .iter()
                .filter(|rs| rs.to_node == *right)
                .map(|rs| rs.from_node.as_str())
                .collect()
        } else {
            rel_schemas.iter().map(|rs| rs.from_node.as_str()).collect()
        };

        candidates
            .first()
            .copied()
            .ok_or_else(|| {
                RenderBuildError::MissingTableInfo(format!(
                    "Could not infer left node label from relationship '{}' (right='{:?}')",
                    simple_type, explicit_right
                ))
            })?
            .to_string()
    };

    let right_label = if let Some(right) = explicit_right {
        right
    } else {
        // Infer right (to_node) — filter by left label
        let candidates: Vec<&str> = rel_schemas
            .iter()
            .filter(|rs| rs.from_node == left_label)
            .map(|rs| rs.to_node.as_str())
            .collect();

        if let Some(label) = candidates.first().copied() {
            label.to_string()
        } else {
            // Fallback: try without filtering by left label
            rel_schemas
                .first()
                .map(|rs| rs.to_node.as_str())
                .ok_or_else(|| {
                    RenderBuildError::MissingTableInfo(format!(
                        "Could not infer right node label from relationship '{}' (left='{}')",
                        simple_type, left_label
                    ))
                })?
                .to_string()
        }
    };

    log::debug!(
        "infer_node_labels_from_rel: inferred labels - left='{}' (explicit={}), right='{}' (explicit={})",
        left_label, left_explicit, right_label, right_explicit
    );

    Ok((left_label, right_label))
}

// ============================================================================
// CteManager-based VLP Generation (Phase 2 Integration)
// ============================================================================

/// Generate a variable-length path CTE using the unified CteManager approach.
///
/// This function provides a cleaner interface to VLP CTE generation by:
/// 1. Using PatternSchemaContext to determine the appropriate strategy
/// 2. Building CteGenerationContext with all VLP-specific fields
/// 3. Delegating to CteManager.generate_vlp_cte()
///
/// # Arguments
/// * `pattern_ctx` - The pattern schema context (from recreate_pattern_schema_context)
/// * `schema` - The graph schema
/// * `spec` - Variable-length specification (min/max hops)
/// * `properties` - Node properties to include in CTE projection
/// * `filters` - Pre-rendered SQL filters for nodes and relationships
/// * `vlp_options` - Additional VLP options (path_variable, shortest_path_mode, etc.)
///
/// # Returns
/// A Cte ready to be added to the CTE list, or an error
pub fn generate_vlp_cte_via_manager(
    pattern_ctx: &PatternSchemaContext,
    schema: &GraphSchema,
    spec: crate::query_planner::logical_plan::VariableLengthSpec,
    properties: Vec<NodeProperty>,
    start_filters_sql: Option<String>,
    end_filters_sql: Option<String>,
    rel_filters_sql: Option<String>,
    path_variable: Option<String>,
    shortest_path_mode: Option<crate::query_planner::logical_plan::ShortestPathMode>,
    relationship_types: Option<Vec<String>>,
    edge_id: Option<crate::graph_catalog::config::Identifier>,
    relationship_cypher_alias: Option<String>,
    start_label: Option<String>,
    end_label: Option<String>,
    is_optional: Option<bool>,
) -> Result<Cte, RenderBuildError> {
    use std::sync::Arc;

    log::debug!(
        "generate_vlp_cte_via_manager: {} -[*]-> {}",
        pattern_ctx.left_node_alias,
        pattern_ctx.right_node_alias
    );

    // Build CteGenerationContext with all VLP-specific fields
    // Clone the schema to own it for the context
    let context = CteGenerationContext::new()
        .with_spec(spec)
        .with_schema_owned(schema.clone())
        .with_path_variable(path_variable.clone())
        .with_shortest_path_mode(shortest_path_mode)
        .with_relationship_types(relationship_types.clone())
        .with_edge_id(edge_id)
        .with_relationship_cypher_alias(relationship_cypher_alias)
        .with_node_labels(start_label.clone(), end_label.clone())
        .with_is_optional(is_optional.unwrap_or(false));

    // Convert filters to CategorizedFilters format
    let filters = super::filter_pipeline::CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
        path_function_filters: None,
        start_sql: start_filters_sql,
        end_sql: end_filters_sql,
        relationship_sql: rel_filters_sql,
    };

    // Create CteManager and generate VLP CTE
    // Note: We clone the schema to create an Arc since we have a reference
    let schema_arc = Arc::new(schema.clone());
    let manager = CteManager::with_context(schema_arc, context);
    let result = manager.generate_vlp_cte(pattern_ctx, &properties, &filters)
        .map_err(|e| RenderBuildError::UnsupportedFeature(format!(
            "CteManager VLP generation failed: {}. Falling back to direct generator may be needed.", e
        )))?;

    // Convert CteGenerationResult to Cte, preserving column metadata for deterministic lookups
    let cte = Cte {
        cte_name: result.cte_name.clone(),
        content: CteContent::RawSql(result.sql),
        is_recursive: result.recursive,
        vlp_start_alias: result.vlp_endpoint.as_ref().map(|e| e.start_alias.clone()),
        vlp_end_alias: result.vlp_endpoint.as_ref().map(|e| e.end_alias.clone()),
        vlp_start_table: result.vlp_endpoint.as_ref().map(|e| e.start_table.clone()),
        vlp_end_table: result.vlp_endpoint.as_ref().map(|e| e.end_table.clone()),
        vlp_cypher_start_alias: result
            .vlp_endpoint
            .as_ref()
            .map(|e| e.cypher_start_alias.clone()),
        vlp_cypher_end_alias: result
            .vlp_endpoint
            .as_ref()
            .map(|e| e.cypher_end_alias.clone()),
        vlp_start_id_col: result.vlp_endpoint.as_ref().map(|e| e.start_id_col.clone()),
        vlp_end_id_col: result.vlp_endpoint.as_ref().map(|e| e.end_id_col.clone()),
        vlp_path_variable: path_variable,
        // Preserve column metadata from CteGenerationResult for deterministic column lookups
        columns: result.columns.clone(),
        from_alias: Some(result.from_alias.clone()),
        // Pass through outer_where_filters for denormalized VLP end node filtering
        outer_where_filters: result.outer_where_filters.clone(),
        with_exported_aliases: Vec::new(),
        variable_registry: None,
    };

    log::info!(
        "✅ Generated VLP CTE '{}' via CteManager (recursive={}, {} columns, from_alias='{}')",
        cte.cte_name,
        cte.is_recursive,
        cte.columns.len(),
        result.from_alias
    );

    Ok(cte)
}

/// Collect all parameter names from a RenderExpr tree
/// Recursively traverses the expression tree to find all Parameter variants
fn collect_parameters_from_expr(expr: &RenderExpr) -> Vec<String> {
    let mut params = Vec::new();
    collect_parameters_recursive(expr, &mut params);
    params
}

/// Collect all parameter names from CategorizedFilters
pub fn collect_parameters_from_filters(filters: &CategorizedFilters) -> Vec<String> {
    let mut params = Vec::new();

    if let Some(ref expr) = filters.start_node_filters {
        params.extend(collect_parameters_from_expr(expr));
    }
    if let Some(ref expr) = filters.end_node_filters {
        params.extend(collect_parameters_from_expr(expr));
    }
    if let Some(ref expr) = filters.relationship_filters {
        params.extend(collect_parameters_from_expr(expr));
    }
    if let Some(ref expr) = filters.path_function_filters {
        params.extend(collect_parameters_from_expr(expr));
    }

    // Remove duplicates while preserving order
    let mut unique_params = Vec::new();
    for param in params {
        if !unique_params.contains(&param) {
            unique_params.push(param);
        }
    }
    unique_params
}

/// Recursive helper to collect parameters from RenderExpr
fn collect_parameters_recursive(expr: &RenderExpr, params: &mut Vec<String>) {
    match expr {
        RenderExpr::Parameter(param_name) => {
            if !params.contains(param_name) {
                params.push(param_name.clone());
            }
        }
        RenderExpr::List(exprs) => {
            for expr in exprs {
                collect_parameters_recursive(expr, params);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_parameters_recursive(operand, params);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_parameters_recursive(arg, params);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_parameters_recursive(arg, params);
            }
        }
        RenderExpr::PropertyAccessExp(_) => {}
        RenderExpr::Case(case) => {
            if let Some(ref expr) = case.expr {
                collect_parameters_recursive(expr, params);
            }
            for (when_expr, then_expr) in &case.when_then {
                collect_parameters_recursive(when_expr, params);
                collect_parameters_recursive(then_expr, params);
            }
            if let Some(ref else_expr) = case.else_expr {
                collect_parameters_recursive(else_expr, params);
            }
        }
        RenderExpr::InSubquery(subq) => {
            collect_parameters_recursive(&subq.expr, params);
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_parameters_recursive(array, params);
            collect_parameters_recursive(index, params);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_parameters_recursive(array, params);
            if let Some(ref from_expr) = from {
                collect_parameters_recursive(from_expr, params);
            }
            if let Some(ref to_expr) = to {
                collect_parameters_recursive(to_expr, params);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            collect_parameters_recursive(&reduce.initial_value, params);
            collect_parameters_recursive(&reduce.list, params);
            collect_parameters_recursive(&reduce.expression, params);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, value_expr) in entries {
                collect_parameters_recursive(value_expr, params);
            }
        }
        // These variants don't contain expressions
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_) => {}
    }
}

/// Helper function to extract the node alias from a GraphNode
fn extract_node_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::Filter(filter) => extract_node_alias(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_alias(&proj.input),
        LogicalPlan::WithClause(wc) => extract_node_alias(&wc.input),
        _ => None,
    }
}

/// Extract schema filter from a node's ViewScan (for CTE generation)
/// Returns the raw filter SQL with table alias replaced to match CTE convention
fn extract_schema_filter_from_node(plan: &LogicalPlan, cte_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => extract_schema_filter_from_node(&node.input, cte_alias),
        LogicalPlan::ViewScan(view_scan) => {
            if let Some(ref schema_filter) = view_scan.schema_filter {
                // Convert schema filter to SQL with the CTE alias
                schema_filter.to_sql(cte_alias).ok()
            } else {
                None
            }
        }
        LogicalPlan::Filter(filter) => extract_schema_filter_from_node(&filter.input, cte_alias),
        LogicalPlan::Projection(proj) => extract_schema_filter_from_node(&proj.input, cte_alias),
        LogicalPlan::WithClause(wc) => extract_schema_filter_from_node(&wc.input, cte_alias),
        _ => None,
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
/// Extract filters from a bound node (Filter → GraphNode structure)
/// Returns the filter expression in SQL format suitable for CTE WHERE clauses
fn extract_bound_node_filter(
    plan: &LogicalPlan,
    node_alias: &str,
    cte_alias: &str,
    relationship_type: Option<&str>,
    node_role: Option<crate::render_plan::cte_generation::NodeRole>,
) -> Option<String> {
    match plan {
        LogicalPlan::Filter(filter) => {
            // Found a filter - convert to RenderExpr and then to SQL
            if let Ok(mut render_expr) = RenderExpr::try_from(filter.predicate.clone()) {
                // Apply property mapping to the filter expression with relationship context
                apply_property_mapping_to_expr_with_context(
                    &mut render_expr,
                    plan,
                    relationship_type,
                    node_role,
                );

                // Create alias mapping: node_alias → cte_alias (e.g., "p1" → "start_node")
                let alias_mapping = [(node_alias.to_string(), cte_alias.to_string())];
                let filter_sql = render_expr_to_sql_string(&render_expr, &alias_mapping);

                log::info!(
                    "🔍 Extracted bound node filter: {} → {}",
                    node_alias,
                    filter_sql
                );
                return Some(filter_sql);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Recurse into the node's input in case there's a filter there
            extract_bound_node_filter(
                &node.input,
                node_alias,
                cte_alias,
                relationship_type,
                node_role,
            )
        }
        LogicalPlan::CartesianProduct(cp) => {
            // For CartesianProduct, the filter might be on either side
            // Try right first (most recent pattern), then left
            if let Some(filter) = extract_bound_node_filter(
                &cp.right,
                node_alias,
                cte_alias,
                relationship_type,
                node_role,
            ) {
                Some(filter)
            } else {
                extract_bound_node_filter(
                    &cp.left,
                    node_alias,
                    cte_alias,
                    relationship_type,
                    node_role,
                )
            }
        }
        _ => None,
    }
}

/// Extract node labels from a GraphNode plan (supports multi-label nodes)
/// Returns Vec of labels, or None if no labels found
pub(crate) fn extract_node_labels(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // When TypeInference inferred multiple possible types (polymorphic endpoint),
            // node_types holds the full set. Return all of them so path enumeration
            // considers all valid combinations (e.g. Post← via LIKED ← User).
            if let Some(ref types) = node.node_types {
                if types.len() > 1 {
                    return Some(types.clone());
                }
            }
            if let Some(ref label) = node.label {
                Some(vec![label.clone()])
            } else {
                None
            }
        }
        LogicalPlan::Filter(filter) => extract_node_labels(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_labels(&proj.input),
        LogicalPlan::WithClause(wc) => extract_node_labels(&wc.input),
        _ => None,
    }
}

/// Check if variable-length path should use JOIN expansion (for multi-type nodes)
/// instead of recursive CTE
///
/// Returns true if:
/// 1. End node has multiple explicit labels (e.g., (x:User|Post))
/// 2. Multiple relationship types lead to different end node types
///
/// Examples:
/// - `MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)` where FOLLOWS→User, AUTHORED→Post
/// - `MATCH (u:User)-[:FOLLOWS*1..2]->(x:User|Post)`
fn should_use_join_expansion(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    rel_types: &[String],
    _schema: &GraphSchema,
) -> bool {
    log::info!(
        "🔍 VLP: should_use_join_expansion called with {} rel_types: {:?}",
        rel_types.len(),
        rel_types
    );

    // Extract end node labels from right node
    let end_node_labels = extract_node_labels(&graph_rel.right);
    log::info!("🔍 VLP: end_node_labels = {:?}", end_node_labels);

    // Case 1: End node has multiple explicit labels (x:User|Post)
    if let Some(ref labels) = end_node_labels {
        if labels.len() > 1 {
            log::info!(
                "🎯 CTE: Multi-type VLP detected - end node has {} labels: {:?}",
                labels.len(),
                labels
            );
            return true;
        }
    }

    // Case 2: Multiple relationship types ALWAYS require UNION ALL CTE
    // Even if they connect to the same node type, they may be in different tables
    if rel_types.len() > 1 {
        log::info!(
            "🎯 CTE: Multi-relationship-type VLP detected - {} relationship types require UNION ALL CTE",
            rel_types.len()
        );
        return true;
    }

    log::info!(
        "🎯 CTE: Single-type VLP - using recursive CTE (end_labels={:?}, rel_types={:?})",
        end_node_labels,
        rel_types
    );
    false
}

/// Public wrapper for should_use_join_expansion for use in plan_builder.rs
pub(crate) fn should_use_join_expansion_public(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    rel_types: &[String],
    schema: &GraphSchema,
) -> bool {
    should_use_join_expansion(graph_rel, rel_types, schema)
}

fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    let result = match plan {
        LogicalPlan::ViewScan(view_scan) => {
            log::debug!(
                "extract_table_name: ViewScan, table={}",
                view_scan.source_table
            );
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!(
                "extract_table_name: GraphNode, alias={}, label={:?}",
                node.alias,
                node.label
            );
            // First try to extract from the input (ViewScan/Scan)
            if let Some(table) = extract_table_name(&node.input) {
                return Some(table);
            }
            // 🔧 FIX: Fallback to label-based lookup for bound nodes
            // When a node is bound from an earlier pattern (e.g., MATCH (person1:Person {id: 1}), ...),
            // its input is an empty Scan with no table name. Use the node's label to look up the table.
            log::debug!(
                "🔍 extract_table_name: GraphNode alias='{}', label={:?}",
                node.alias,
                node.label
            );
            if let Some(label) = &node.label {
                let table = label_to_table_name(label);
                log::info!(
                    "🔧 extract_table_name: Using label '{}' → table '{}'",
                    label,
                    table
                );
                return Some(table);
            }
            log::warn!(
                "⚠️  extract_table_name: GraphNode '{}' has no label and no table in input",
                node.alias
            );
            None
        }
        LogicalPlan::GraphRel(rel) => {
            log::debug!("extract_table_name: GraphRel, recursing to left");
            // For nested GraphRel (e.g., when VLP connects to another relationship),
            // extract the node table from the LEFT side, not the relationship table from CENTER
            // Example: (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post)
            // When processing REPLY_OF, left is HAS_CREATOR GraphRel, need message node table
            extract_table_name(&rel.left)
        }
        LogicalPlan::Filter(filter) => {
            log::debug!("extract_table_name: Filter, recursing to input");
            extract_table_name(&filter.input)
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("extract_table_name: Projection, recursing to input");
            extract_table_name(&proj.input)
        }
        LogicalPlan::CartesianProduct(cp) => {
            log::debug!("extract_table_name: CartesianProduct, checking right side first");
            // For CartesianProduct from comma-separated patterns like:
            // MATCH (p1:Person {id: 1}), (p2:Person {id: 2}), path = shortestPath((p1)-[*]-(p2))
            // The right side contains the most recent pattern (p2), left contains earlier patterns (p1)
            // When extracting table for a bound node, try right first (most likely to be the target)
            if let Some(table) = extract_table_name(&cp.right) {
                return Some(table);
            }
            // If right doesn't work, try left
            extract_table_name(&cp.left)
        }
        other => {
            let plan_type = match other {
                LogicalPlan::Empty => "Empty",
                LogicalPlan::Union(_) => "Union",
                LogicalPlan::PageRank(_) => "PageRank",
                LogicalPlan::Unwind(_) => "Unwind",
                LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                LogicalPlan::WithClause(_) => "WithClause",
                LogicalPlan::GroupBy(_) => "GroupBy",
                LogicalPlan::OrderBy(_) => "OrderBy",
                LogicalPlan::Skip(_) => "Skip",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::Cte(_) => "Cte",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                _ => "Unknown",
            };
            log::debug!("extract_table_name: Unhandled plan type: {}", plan_type);
            None
        }
    };
    log::debug!("extract_table_name: result={:?}", result);
    result
}

/// Extract table name with parameterized view syntax if applicable.
/// For parameterized views, returns `table(param1='value1', param2='value2')`.
/// For regular tables, returns just the table name.
///
/// This is essential for VLP CTE generation where both node tables and relationship
/// tables may be parameterized views (e.g., multi-tenant GraphRAG schemas).
fn extract_parameterized_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = (
                &view_scan.view_parameter_names,
                &view_scan.view_parameter_values,
            ) {
                if !param_names.is_empty() {
                    // Generate parameterized view call: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                // Escape single quotes in value for SQL safety
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();

                    if !param_pairs.is_empty() {
                        let result =
                            format!("{}({})", view_scan.source_table, param_pairs.join(", "));
                        log::debug!(
                            "extract_parameterized_table_name: ViewScan '{}' → '{}'",
                            view_scan.source_table,
                            result
                        );
                        return Some(result);
                    }
                }
            }
            // No parameters - return plain table name
            log::debug!(
                "extract_parameterized_table_name: ViewScan '{}' (no params)",
                view_scan.source_table
            );
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!(
                "extract_parameterized_table_name: GraphNode alias='{}', label={:?}",
                node.alias,
                node.label
            );
            // First try to extract from the input (ViewScan/Scan)
            if let Some(table) = extract_parameterized_table_name(&node.input) {
                return Some(table);
            }
            // Fallback: use plain table name if label-based lookup needed
            if let Some(label) = &node.label {
                let table = label_to_table_name(label);
                log::info!(
                    "extract_parameterized_table_name: Using label '{}' → table '{}' (no params)",
                    label,
                    table
                );
                return Some(table);
            }
            None
        }
        LogicalPlan::GraphRel(rel) => {
            log::debug!("extract_parameterized_table_name: GraphRel, recursing to left");
            extract_parameterized_table_name(&rel.left)
        }
        LogicalPlan::Filter(filter) => extract_parameterized_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_parameterized_table_name(&proj.input),
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(table) = extract_parameterized_table_name(&cp.right) {
                return Some(table);
            }
            extract_parameterized_table_name(&cp.left)
        }
        _ => None,
    }
}

/// Extract parameterized table name specifically from a ViewScan (used for relationship center)
fn extract_parameterized_rel_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = (
                &view_scan.view_parameter_names,
                &view_scan.view_parameter_values,
            ) {
                if !param_names.is_empty() {
                    // Generate parameterized view call: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();

                    if !param_pairs.is_empty() {
                        let result =
                            format!("{}({})", view_scan.source_table, param_pairs.join(", "));
                        log::info!(
                            "extract_parameterized_rel_table: Relationship '{}' → '{}'",
                            view_scan.source_table,
                            result
                        );
                        return Some(result);
                    }
                }
            }
            log::debug!(
                "extract_parameterized_rel_table: '{}' (no params)",
                view_scan.source_table
            );
            Some(view_scan.source_table.clone())
        }
        _ => None,
    }
}

/// Extract view_parameter_values from a LogicalPlan (traverses to find ViewScan)
/// This is used for multi-type VLP to get the parameter values from the query context
fn extract_view_parameter_values(
    plan: &LogicalPlan,
) -> Option<std::collections::HashMap<String, String>> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            let params = view_scan.view_parameter_values.clone().unwrap_or_default();
            log::debug!(
                "🔍 extract_view_parameter_values: ViewScan '{}' → params {:?}",
                view_scan.source_table,
                params
            );
            if params.is_empty() {
                None
            } else {
                Some(params)
            }
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!(
                "🔍 extract_view_parameter_values: GraphNode '{}' → recursing into input",
                node.alias
            );
            extract_view_parameter_values(&node.input)
        }
        LogicalPlan::GraphRel(rel) => {
            // Try left first, then center, then right
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying left");
            if let Some(left_params) = extract_view_parameter_values(&rel.left) {
                return Some(left_params);
            }
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying center");
            if let Some(center_params) = extract_view_parameter_values(&rel.center) {
                return Some(center_params);
            }
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying right");
            extract_view_parameter_values(&rel.right)
        }
        LogicalPlan::Filter(filter) => extract_view_parameter_values(&filter.input),
        LogicalPlan::Projection(proj) => extract_view_parameter_values(&proj.input),
        _ => {
            log::debug!("🔍 extract_view_parameter_values: Unhandled plan type → None");
            None
        }
    }
}

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
pub fn render_expr_to_sql_string(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> String {
    match expr {
        RenderExpr::Column(col) => col.raw().to_string(),
        RenderExpr::TableAlias(alias) => alias.0.clone(),
        RenderExpr::ColumnAlias(alias) => alias.0.clone(),
        RenderExpr::Literal(lit) => match lit {
            super::render_expr::Literal::String(s) => format!("'{}'", s.replace("'", "''")),
            super::render_expr::Literal::Integer(i) => i.to_string(),
            super::render_expr::Literal::Float(f) => f.to_string(),
            super::render_expr::Literal::Boolean(b) => b.to_string(),
            super::render_expr::Literal::Null => "NULL".to_string(),
        },
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            let table_alias = alias_mapping
                .iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
            // Quote column name if it contains dots or special characters
            let quoted_column =
                crate::clickhouse_query_generator::quote_identifier(prop.column.raw());
            format!("{}.{}", table_alias, quoted_column)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Special handling for IS NULL / IS NOT NULL with wildcard property access (e.g., r.*)
            // Convert r.* to r.from_id for null checks (LEFT JOIN produces NULL for all columns)
            let operands: Vec<String> = if matches!(
                op.operator,
                Operator::IsNull | Operator::IsNotNull
            ) && op.operands.len() == 1
                && matches!(&op.operands[0], RenderExpr::PropertyAccessExp(prop) if prop.column.raw() == "*")
            {
                // Extract the relationship alias and use from_id column instead of wildcard
                if let RenderExpr::PropertyAccessExp(prop) = &op.operands[0] {
                    let table_alias = alias_mapping
                        .iter()
                        .find(|(cypher, _)| *cypher == prop.table_alias.0)
                        .map(|(_, cte)| cte.clone())
                        .unwrap_or_else(|| prop.table_alias.0.clone());

                    // Use from_id as the representative column for null check
                    // (LEFT JOIN makes all columns NULL together, so checking one is sufficient)
                    // Quote column name in case it contains dots
                    vec![format!("{}.`from_id`", table_alias)]
                } else {
                    op.operands
                        .iter()
                        .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                        .collect()
                }
            } else {
                op.operands
                    .iter()
                    .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                    .collect()
            };

            match op.operator {
                Operator::Equal => format!("{} = {}", operands[0], operands[1]),
                Operator::NotEqual => format!("{} != {}", operands[0], operands[1]),
                Operator::LessThan => format!("{} < {}", operands[0], operands[1]),
                Operator::GreaterThan => format!("{} > {}", operands[0], operands[1]),
                Operator::LessThanEqual => format!("{} <= {}", operands[0], operands[1]),
                Operator::GreaterThanEqual => format!("{} >= {}", operands[0], operands[1]),
                Operator::And => format!("({})", operands.join(" AND ")),
                Operator::Or => format!("({})", operands.join(" OR ")),
                Operator::Not => format!("NOT ({})", operands[0]),
                Operator::Addition => {
                    // Use concat() for string concatenation
                    // Flatten nested + operations for cases like: a + ' - ' + b
                    if has_string_operand(&op.operands) {
                        let flattened: Vec<String> = op
                            .operands
                            .iter()
                            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
                            .collect();
                        format!("concat({})", flattened.join(", "))
                    } else {
                        format!("{} + {}", operands[0], operands[1])
                    }
                }
                Operator::Subtraction => format!("{} - {}", operands[0], operands[1]),
                Operator::Multiplication => format!("{} * {}", operands[0], operands[1]),
                Operator::Division => format!("{} / {}", operands[0], operands[1]),
                Operator::ModuloDivision => format!("{} % {}", operands[0], operands[1]),
                Operator::Exponentiation => format!("POWER({}, {})", operands[0], operands[1]),
                Operator::In => {
                    // Check if right operand is a property access (array column)
                    // Cypher: x IN array_property → ClickHouse: has(array, x)
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} IN {}", operands[0], operands[1])
                    }
                }
                Operator::NotIn => {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("NOT has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} NOT IN {}", operands[0], operands[1])
                    }
                }
                Operator::StartsWith => format!("startsWith({}, {})", operands[0], operands[1]),
                Operator::EndsWith => format!("endsWith({}, {})", operands[0], operands[1]),
                Operator::Contains => format!("(position({}, {}) > 0)", operands[0], operands[1]),
                Operator::IsNull => format!("{} IS NULL", operands[0]),
                Operator::IsNotNull => format!("{} IS NOT NULL", operands[0]),
                Operator::Distinct => format!("{} IS DISTINCT FROM {}", operands[0], operands[1]),
                Operator::RegexMatch => format!("match({}, {})", operands[0], operands[1]),
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", func.name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        RenderExpr::List(list) => {
            let items: Vec<String> = list
                .iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            format!("({})", items.join(", "))
        }
        RenderExpr::InSubquery(subq) => {
            format!(
                "{} IN ({})",
                render_expr_to_sql_string(&subq.expr, alias_mapping),
                "/* subquery */"
            )
        }
        RenderExpr::Case(case) => {
            let when_clauses: Vec<String> = case
                .when_then
                .iter()
                .map(|(condition, result)| {
                    format!(
                        "WHEN {} THEN {}",
                        render_expr_to_sql_string(condition, alias_mapping),
                        render_expr_to_sql_string(result, alias_mapping)
                    )
                })
                .collect();
            let else_clause = case
                .else_expr
                .as_ref()
                .map(|expr| format!(" ELSE {}", render_expr_to_sql_string(expr, alias_mapping)))
                .unwrap_or_default();
            format!(
                "CASE {} {} END",
                case.expr
                    .as_ref()
                    .map(|e| render_expr_to_sql_string(e, alias_mapping))
                    .unwrap_or_default(),
                when_clauses.join(" ") + &else_clause
            )
        }
        RenderExpr::ExistsSubquery(exists) => {
            // Use the pre-generated SQL from ExistsSubquery
            format!("EXISTS ({})", exists.sql)
        }
        RenderExpr::ReduceExpr(reduce) => {
            // Convert to ClickHouse arrayFold((acc, x) -> expr, list, init)
            // Cast numeric init to Int64 to prevent type mismatch issues
            let init_sql = render_expr_to_sql_string(&reduce.initial_value, alias_mapping);
            let list_sql = render_expr_to_sql_string(&reduce.list, alias_mapping);
            let expr_sql = render_expr_to_sql_string(&reduce.expression, alias_mapping);

            // Wrap numeric init values in toInt64() to prevent type mismatch
            let init_cast = if matches!(
                *reduce.initial_value,
                RenderExpr::Literal(Literal::Integer(_))
            ) {
                format!("toInt64({})", init_sql)
            } else {
                init_sql
            };

            format!(
                "arrayFold({}, {} -> {}, {}, {})",
                reduce.variable, reduce.accumulator, expr_sql, list_sql, init_cast
            )
        }
        RenderExpr::PatternCount(pc) => {
            // Use the pre-generated SQL from PatternCount
            pc.sql.clone()
        }
        RenderExpr::ArraySubscript { array, index } => {
            let array_sql = render_expr_to_sql_string(array, alias_mapping);
            let index_sql = render_expr_to_sql_string(index, alias_mapping);
            format!("{}[{}]", array_sql, index_sql)
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            // Use to_sql method which handles ClickHouse arraySlice generation
            // This is a temporary delegation - ideally we'd use alias_mapping here too
            // but to_sql() doesn't support it. For now, array slicing shouldn't have
            // complex alias references that need mapping.
            let array_sql = render_expr_to_sql_string(array, alias_mapping);
            match (from, to) {
                (Some(from_expr), Some(to_expr)) => {
                    let from_sql = render_expr_to_sql_string(from_expr, alias_mapping);
                    let to_sql = render_expr_to_sql_string(to_expr, alias_mapping);
                    format!(
                        "arraySlice({}, {} + 1, {} - {} + 1)",
                        array_sql, from_sql, to_sql, from_sql
                    )
                }
                (Some(from_expr), None) => {
                    let from_sql = render_expr_to_sql_string(from_expr, alias_mapping);
                    format!("arraySlice({}, {} + 1)", array_sql, from_sql)
                }
                (None, Some(to_expr)) => {
                    let to_sql = render_expr_to_sql_string(to_expr, alias_mapping);
                    format!("arraySlice({}, 1, {} + 1)", array_sql, to_sql)
                }
                (None, None) => array_sql,
            }
        }
        RenderExpr::Star => "*".to_string(),
        RenderExpr::Parameter(param) => format!("${}", param),
        RenderExpr::MapLiteral(entries) => {
            // Map literals handled specially for duration(), point(), etc.
            let pairs: Vec<String> = entries
                .iter()
                .map(|(k, v)| {
                    let val_sql = render_expr_to_sql_string(v, alias_mapping);
                    format!("'{}': {}", k, val_sql)
                })
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        RenderExpr::CteEntityRef(cte_ref) => {
            // CTE entity reference - expand to prefixed column references
            // For now, return the alias as placeholder - full expansion happens in select_builder
            log::debug!(
                "render_expr_to_sql_string: CteEntityRef '{}' from CTE '{}' - should be expanded by select_builder",
                cte_ref.alias, cte_ref.cte_name
            );
            cte_ref.alias.clone()
        }
    }
}

/// Relationship column information
#[derive(Debug, Clone)]
pub struct RelationshipColumns {
    pub from_id: Identifier,
    pub to_id: Identifier,
}

/// Convert a label to its corresponding table name using provided schema
pub fn label_to_table_name_with_schema(
    label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.node_schema(label) {
        Ok(node_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", node_schema.database, node_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error!
            log::error!(
                "❌ SCHEMA ERROR: Node label '{}' not found in schema.",
                label
            );
            format!("ERROR_NODE_SCHEMA_MISSING_{}", label)
        }
    }
}

/// Convert a label to its corresponding table name.
/// Uses the task-local query schema.
pub fn label_to_table_name(label: &str) -> String {
    use crate::server::query_context::get_current_schema;

    if let Some(schema) = get_current_schema() {
        return label_to_table_name_with_schema(label, &schema);
    }

    log::error!(
        "❌ SCHEMA ERROR: No schema in query context. Cannot resolve label '{}'.",
        label
    );
    format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", label)
}

/// Convert a relationship type to its corresponding table name using provided schema
/// For polymorphic relationships (multiple tables for same relationship type), specify node types
pub fn rel_type_to_table_name_with_nodes(
    rel_type: &str,
    from_node: Option<&str>,
    to_node: Option<&str>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.get_rel_schema_with_nodes(rel_type, from_node, to_node) {
        Ok(rel_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", rel_schema.database, rel_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error and return marker that will fail in ClickHouse
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' (from_node={:?}, to_node={:?}) not found in schema. This should have been caught during query planning.",
                rel_type, from_node, to_node
            );
            format!(
                "ERROR_SCHEMA_MISSING_{}_FROM_{:?}_TO_{:?}",
                rel_type, from_node, to_node
            )
        }
    }
}

/// Convert a relationship type to its corresponding table name with parameterized view support
/// This is used when the relationship doesn't come from a ViewScan (e.g., inferred from schema)
/// but we still need to apply parameterized view syntax if the rel schema defines view_parameters.
pub fn rel_type_to_table_name_with_nodes_and_params(
    rel_type: &str,
    from_node: Option<&str>,
    to_node: Option<&str>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    view_parameter_values: &std::collections::HashMap<String, String>,
) -> String {
    match schema.get_rel_schema_with_nodes(rel_type, from_node, to_node) {
        Ok(rel_schema) => {
            let base_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);

            // Check if rel_schema has view_parameters and if we have values for them
            if let Some(ref view_params) = rel_schema.view_parameters {
                if !view_params.is_empty() && !view_parameter_values.is_empty() {
                    // Build parameterized view syntax: `db.table`(param1 = 'value1', param2 = 'value2')
                    let params: Vec<String> = view_params
                        .iter()
                        .filter_map(|param| {
                            view_parameter_values
                                .get(param)
                                .map(|value| format!("{} = '{}'", param, value))
                        })
                        .collect();

                    if !params.is_empty() {
                        let param_str = params.join(", ");
                        log::info!(
                            "🔧 Applying parameterized view syntax to rel table: `{}`({})",
                            base_table,
                            param_str
                        );
                        return format!("`{}`({})", base_table, param_str);
                    }
                }
            }

            // No parameterized view or no matching values - return plain table name
            base_table
        }
        Err(_) => {
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' (from_node={:?}, to_node={:?}) not found in schema.",
                rel_type, from_node, to_node
            );
            format!(
                "ERROR_SCHEMA_MISSING_{}_FROM_{:?}_TO_{:?}",
                rel_type, from_node, to_node
            )
        }
    }
}

/// Convert a relationship type to its corresponding table name using provided schema
pub fn rel_type_to_table_name_with_schema(
    rel_type: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.get_rel_schema(rel_type) {
        Ok(rel_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", rel_schema.database, rel_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error and return marker that will fail in ClickHouse
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' not found in schema. For polymorphic relationships with multiple tables, use get_rel_schema_with_nodes() to specify node types.",
                rel_type
            );
            format!("ERROR_SCHEMA_MISSING_{}", rel_type)
        }
    }
}

/// Convert a relationship type to its corresponding table name.
/// Uses the task-local query schema.
pub fn rel_type_to_table_name(rel_type: &str) -> String {
    use crate::server::query_context::get_current_schema;

    if let Some(schema) = get_current_schema() {
        return rel_type_to_table_name_with_schema(rel_type, &schema);
    }

    log::error!(
        "❌ SCHEMA ERROR: No schema in query context. Cannot resolve relationship type '{}'.",
        rel_type
    );
    format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", rel_type)
}

/// Convert multiple relationship types to table names
pub fn rel_types_to_table_names(rel_types: &[String]) -> Vec<String> {
    rel_types
        .iter()
        .map(|rt| rel_type_to_table_name(rt))
        .collect()
}

/// Extract relationship columns from a table name using provided schema
pub fn extract_relationship_columns_from_table_with_schema(
    table_name: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RelationshipColumns {
    // Extract just the table name without database prefix for matching
    let table_only = table_name.rsplit('.').next().unwrap_or(table_name);

    // Find relationship schema by table name (sorted for determinism)
    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());
    for (_, rel_schema) in sorted_rels {
        // Match both with full name (db.table) or just table name
        if rel_schema.table_name == table_name
            || rel_schema.table_name == table_only
            || table_name.ends_with(&format!(".{}", rel_schema.table_name))
        {
            return RelationshipColumns {
                from_id: rel_schema.from_id.clone(),
                to_id: rel_schema.to_id.clone(),
            };
        }
    }

    // NO FALLBACK - log error and return generic columns that will cause SQL error
    log::error!("\u{274c} SCHEMA ERROR: Relationship table '{}' not found in schema. Using generic from_id/to_id columns which will likely fail.", table_name);
    RelationshipColumns {
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
    }
}

/// Extract relationship columns from a table name.
/// Uses the task-local query schema.
pub fn extract_relationship_columns_from_table(table_name: &str) -> RelationshipColumns {
    use crate::server::query_context::get_current_schema;

    if let Some(schema) = get_current_schema() {
        return extract_relationship_columns_from_table_with_schema(table_name, &schema);
    }

    log::error!(
        "❌ SCHEMA ERROR: No schema in query context. Using generic from_id/to_id for table '{}'.",
        table_name
    );
    RelationshipColumns {
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
    }
}

/// Extract relationship columns from a LogicalPlan
pub fn extract_relationship_columns(plan: &LogicalPlan) -> Option<RelationshipColumns> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if ViewScan already has relationship columns configured
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                Some(RelationshipColumns {
                    from_id: from_col.clone(),
                    to_id: to_col.clone(),
                })
            } else {
                // Fallback to table-based lookup
                Some(extract_relationship_columns_from_table(
                    &view_scan.source_table,
                ))
            }
        }
        LogicalPlan::Cte(cte) => extract_relationship_columns(&cte.input),
        LogicalPlan::GraphRel(rel) => extract_relationship_columns(&rel.center),
        LogicalPlan::Filter(filter) => extract_relationship_columns(&filter.input),
        LogicalPlan::Projection(proj) => extract_relationship_columns(&proj.input),
        _ => None,
    }
}

/// Extract ID column from a LogicalPlan
fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        // For WithClause, recurse into input to get ID column from underlying node
        LogicalPlan::WithClause(wc) => extract_id_column(&wc.input),
        _ => None,
    }
}

/// Get ID column for a table using provided schema
pub fn table_to_id_column_with_schema(
    table: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<String, String> {
    // Find node schema by table name
    // Handle both fully qualified (database.table) and simple (table) names
    for node_schema in schema.all_node_schemas().values() {
        let fully_qualified = format!("{}.{}", node_schema.database, node_schema.table_name);
        if node_schema.table_name == table || fully_qualified == table {
            return Ok(node_schema
                .node_id
                .columns()
                .first()
                .ok_or_else(|| {
                    format!(
                        "Node schema for table '{}' has no ID columns defined",
                        table
                    )
                })?
                .to_string());
        }
    }

    // Node table not found in schema - this is an error
    Err(format!("Node table '{}' not found in schema", table))
}

/// Get ID column for a table.
/// Uses the task-local query schema.
pub fn table_to_id_column(table: &str) -> String {
    use crate::server::query_context::get_current_schema;

    if let Some(schema) = get_current_schema() {
        match table_to_id_column_with_schema(table, &schema) {
            Ok(col) => return col,
            Err(e) => {
                log::error!("❌ SCHEMA ERROR: {}", e);
                return "id".to_string();
            }
        }
    }

    log::error!(
        "❌ SCHEMA ERROR: No schema in query context. Using generic 'id' column for table '{}'.",
        table
    );
    "id".to_string()
}

/// Get ID column for a label
fn table_to_id_column_for_label(label: &str) -> String {
    table_to_id_column(&label_to_table_name(label))
}

/// Get relationship columns from schema
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    let table = rel_type_to_table_name(rel_type);
    let cols = extract_relationship_columns_from_table(&table);
    Some((cols.from_id.to_string(), cols.to_id.to_string()))
}

/// Get relationship columns by table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    let cols = extract_relationship_columns_from_table(table_name);
    Some((cols.from_id.to_string(), cols.to_id.to_string()))
}

/// Get node info from schema
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    let table = label_to_table_name(node_label);
    let id_col = table_to_id_column(&table);
    Some((table, id_col))
}

/// Apply property mapping to an expression
fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    apply_property_mapping_to_expr_with_context(expr, plan, None, None);
}

fn apply_property_mapping_to_expr_with_context(
    expr: &mut RenderExpr,
    plan: &LogicalPlan,
    relationship_type: Option<&str>,
    node_role: Option<crate::render_plan::cte_generation::NodeRole>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Get the node label for this table alias
            if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                // Map the property to the correct column with relationship context
                let mapped_column =
                    crate::render_plan::cte_generation::map_property_to_column_with_relationship_context(
                        prop.column.raw(),
                        &node_label,
                        relationship_type,
                        node_role,
                        None, // schema_name will be resolved from task-local
                    ).unwrap_or_else(|_| prop.column.raw().to_string());
                prop.column = PropertyValue::Column(mapped_column);
            }
        }
        RenderExpr::Column(col) => {
            // Check if this column name is actually an alias
            if let Some(node_label) = get_node_label_for_alias(col.raw(), plan) {
                // Convert Column(alias) to PropertyAccess(alias, "id")
                let id_column = table_to_id_column(&label_to_table_name(&node_label));
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: super::render_expr::TableAlias(col.raw().to_string()),
                    column: PropertyValue::Column(id_column),
                });
            }
        }
        RenderExpr::TableAlias(alias) => {
            // For denormalized nodes, convert TableAlias to PropertyAccess with the ID column
            // This is especially important for GROUP BY expressions
            //
            // CRITICAL: Use task-local context (populated during planning) instead of
            // traversing the plan tree. This ensures coupled edges get the unified_alias.
            if let Some(rel_alias) = crate::render_plan::get_denormalized_alias_mapping(&alias.0) {
                // We have the correct edge alias from task-local context
                // Now find the ID column from the plan (still need to traverse)
                if let Some((_, id_column)) = get_denormalized_node_id_reference(&alias.0, plan) {
                    *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: super::render_expr::TableAlias(rel_alias),
                        column: PropertyValue::Column(id_column),
                    });
                } else {
                    // Fallback: use "id" as ID column
                    *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: super::render_expr::TableAlias(rel_alias),
                        column: PropertyValue::Column("id".to_string()),
                    });
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                apply_property_mapping_to_expr_with_context(
                    operand,
                    plan,
                    relationship_type,
                    node_role,
                );
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                apply_property_mapping_to_expr_with_context(
                    arg,
                    plan,
                    relationship_type,
                    node_role,
                );
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                apply_property_mapping_to_expr_with_context(
                    arg,
                    plan,
                    relationship_type,
                    node_role,
                );
            }
        }
        RenderExpr::List(list) => {
            for item in list {
                apply_property_mapping_to_expr_with_context(
                    item,
                    plan,
                    relationship_type,
                    node_role,
                );
            }
        }
        RenderExpr::InSubquery(subq) => {
            apply_property_mapping_to_expr_with_context(
                &mut subq.expr,
                plan,
                relationship_type,
                node_role,
            );
        }
        // Other expression types don't contain nested expressions
        _ => {}
    }
}

/// Get the node label for a given Cypher alias by searching the logical plan tree.
///
/// This function traverses the plan tree recursively to find a GraphNode with the specified alias,
/// then extracts its label. For denormalized schemas, it first checks node.label; for normal schemas,
/// it extracts from ViewScan.
///
/// Used by property resolution logic in both cte_extraction and plan_builder_helpers.
pub(crate) fn get_node_label_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            // For denormalized nodes, the label is stored directly on GraphNode
            // For normal nodes, we need to extract from ViewScan input
            node.label
                .clone()
                .or_else(|| extract_node_label_from_viewscan(&node.input))
        }
        LogicalPlan::GraphNode(node) => get_node_label_for_alias(alias, &node.input),
        LogicalPlan::GraphRel(rel) => get_node_label_for_alias(alias, &rel.left)
            .or_else(|| get_node_label_for_alias(alias, &rel.center))
            .or_else(|| get_node_label_for_alias(alias, &rel.right)),
        LogicalPlan::Filter(filter) => get_node_label_for_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_node_label_for_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_node_label_for_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => get_node_label_for_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => get_node_label_for_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_node_label_for_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => get_node_label_for_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => get_node_label_for_alias(alias, &cte.input),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(label) = get_node_label_for_alias(alias, input) {
                    return Some(label);
                }
            }
            None
        }
        _ => None,
    }
}

/// Get the relationship type for a given relationship alias by traversing the plan tree.
/// Returns the first relationship type from GraphRel.labels if found.
///
/// IMPORTANT: For UNION queries, callers should invoke this function on the specific
/// branch plan they are working with, not the full UNION plan. Each UNION branch
/// should be processed independently for proper per-branch property mapping.
pub(crate) fn get_relationship_type_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.alias == alias => {
            // Found matching GraphRel - return the first label
            // Labels are stored as "TYPE::FromNode::ToNode", extract just the type
            rel.labels.as_ref().and_then(|labels| {
                labels.first().map(|label| {
                    let rel_type = label.split("::").next().unwrap_or(label);
                    if !label.contains("::") {
                        // Log a warning to help diagnose schema inconsistencies where
                        // a composite relationship label was expected but a simple one was found.
                        log::debug!(
                            "Expected composite relationship label 'TYPE::FromNode::ToNode' \
                             for alias '{}', but got '{}'; using full label as relationship type",
                            alias,
                            label
                        );
                    }
                    rel_type.to_string()
                })
            })
        }
        LogicalPlan::GraphRel(rel) => get_relationship_type_for_alias(alias, &rel.left)
            .or_else(|| get_relationship_type_for_alias(alias, &rel.center))
            .or_else(|| get_relationship_type_for_alias(alias, &rel.right)),
        LogicalPlan::GraphNode(node) => get_relationship_type_for_alias(alias, &node.input),
        LogicalPlan::Filter(filter) => get_relationship_type_for_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_relationship_type_for_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_relationship_type_for_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => get_relationship_type_for_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => get_relationship_type_for_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_relationship_type_for_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => get_relationship_type_for_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => get_relationship_type_for_alias(alias, &cte.input),
        LogicalPlan::Union(_union) => {
            // IMPORTANT: Do not resolve relationship aliases across UNION branches here.
            // Each UNION input/branch must be processed independently, so callers should
            // invoke this function on the specific branch plan they are working with.
            // Returning first match here is only for backward compatibility with simple cases.
            for input in &_union.inputs {
                if let Some(rel_type) = get_relationship_type_for_alias(alias, input) {
                    return Some(rel_type);
                }
            }
            None
        }
        _ => None,
    }
}

/// For denormalized schemas: get the relationship alias and ID column for a node alias
/// Returns (rel_alias, id_column) if the node is denormalized, None otherwise
fn get_denormalized_node_id_reference(alias: &str, plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this node alias matches left or right connection
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // Check if node is the "from" node (left_connection)
                if alias == rel.left_connection {
                    if let Some(from_id) = &scan.from_id {
                        return Some((rel.alias.clone(), from_id.to_string()));
                    }
                }
                // Check if node is the "to" node (right_connection)
                if alias == rel.right_connection {
                    if let Some(to_id) = &scan.to_id {
                        return Some((rel.alias.clone(), to_id.to_string()));
                    }
                }
            }

            // Recursively check left and right branches
            // Check right branch first (more recent relationships take precedence for multi-hop)
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.right) {
                return Some(result);
            }
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.left) {
                return Some(result);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Check if this is a denormalized node
            if node.is_denormalized && node.alias == alias {
                // For standalone denormalized nodes, check their input ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if let Some(from_id) = &scan.from_id {
                        // Use a placeholder alias since standalone nodes don't have a rel alias
                        return Some((alias.to_string(), from_id.to_string()));
                    }
                }
            }
            get_denormalized_node_id_reference(alias, &node.input)
        }
        LogicalPlan::Filter(filter) => get_denormalized_node_id_reference(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_denormalized_node_id_reference(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_denormalized_node_id_reference(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => {
            get_denormalized_node_id_reference(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => get_denormalized_node_id_reference(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_denormalized_node_id_reference(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => {
            get_denormalized_node_id_reference(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => get_denormalized_node_id_reference(alias, &cte.input),
        LogicalPlan::CartesianProduct(cp) => get_denormalized_node_id_reference(alias, &cp.left)
            .or_else(|| get_denormalized_node_id_reference(alias, &cp.right)),
        _ => None,
    }
}

/// Extract CTEs with context - the main CTE extraction function
pub fn extract_ctes_with_context(
    plan: &LogicalPlan,
    last_node_alias: &str,
    context: &mut super::cte_generation::CteGenerationContext,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> RenderPlanBuilderResult<Vec<Cte>> {
    // Debug: Log the plan type being processed
    let plan_type = match plan {
        LogicalPlan::Empty => "Empty",
        LogicalPlan::ViewScan(_) => "ViewScan",
        LogicalPlan::GraphNode(_) => "GraphNode",
        LogicalPlan::GraphRel(_) => "GraphRel",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::GraphJoins(_) => "GraphJoins",
        LogicalPlan::CartesianProduct(_) => "CartesianProduct",
        LogicalPlan::WithClause(_) => "WithClause",
        _ => "Other",
    };

    log::debug!(
        "extract_ctes_with_context: Processing {} node, plan_ctx available: {}",
        plan_type,
        plan_ctx.is_some()
    );

    match plan {
        LogicalPlan::Empty => Ok(vec![]),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a relationship ViewScan (has from_id/to_id)
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                // This is a relationship ViewScan - create a CTE that selects the relationship columns
                let cte_name = format!(
                    "rel_{}",
                    view_scan.source_table.replace([' ', '-', '_'], "")
                );
                let sql = format!(
                    "SELECT {}, {} FROM {}",
                    from_col, to_col, view_scan.source_table
                );
                let formatted_sql = format!("{} AS (\n{}\n)", cte_name, sql);

                Ok(vec![Cte::new(
                    cte_name,
                    super::CteContent::RawSql(formatted_sql),
                    false, // is_recursive
                )])
            } else {
                // This is a node ViewScan - no CTE needed
                Ok(vec![])
            }
        }
        LogicalPlan::GraphNode(graph_node) => {
            // Skip CTE creation for denormalized nodes - their properties are on the relationship table
            if graph_node.is_denormalized {
                log::debug!(
                    "Skipping CTE for denormalized node '{}' (properties stored on relationship table)",
                    graph_node.alias
                );
                return Ok(vec![]);
            }
            extract_ctes_with_context(
                &graph_node.input,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )
        }
        LogicalPlan::GraphRel(graph_rel) => {
            log::debug!(
                "🔍 CTE extraction: GraphRel alias='{}' left='{}' right='{}' vlp={:?}",
                graph_rel.alias,
                graph_rel.left_connection,
                graph_rel.right_connection,
                graph_rel.variable_length.is_some(),
            );
            // Handle variable-length paths with context
            if let Some(spec) = &graph_rel.variable_length {
                log::debug!(
                    "🔧 VLP ENTRY: alias='{}' left='{}' right='{}' spec={:?}",
                    graph_rel.alias,
                    graph_rel.left_connection,
                    graph_rel.right_connection,
                    spec
                );
                log::debug!("🔧 VLP: Entering variable-length path handling");
                // Extract actual table names directly from ViewScan - with fallback to label lookup
                let left_plan_desc = match graph_rel.left.as_ref() {
                    LogicalPlan::Empty => "Empty".to_string(),
                    LogicalPlan::ViewScan(_) => "ViewScan".to_string(),
                    LogicalPlan::GraphNode(n) => format!("GraphNode({})", n.alias),
                    LogicalPlan::GraphRel(_) => "GraphRel".to_string(),
                    LogicalPlan::Filter(_) => "Filter".to_string(),
                    LogicalPlan::Projection(_) => "Projection".to_string(),
                    _ => "Other".to_string(),
                };
                log::info!("🔍 VLP: Left plan = {}", left_plan_desc);
                // Extract start table name if available (used for validation only)
                let _start_table = extract_parameterized_table_name(&graph_rel.left);
                if _start_table.is_none() {
                    log::info!("🔍 VLP: No explicit table for start node (may be CTE reference), will infer from schema");
                }

                // 🎯 CHECK: Is this multi-type VLP? (end node has unknown type)
                // If so, end_table will be determined by schema expansion, not from the plan
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type_vlp = should_use_join_expansion(graph_rel, &rel_types, schema);

                let _end_table = if is_multi_type_vlp {
                    // For multi-type VLP, end_table isn't in the plan (it's polymorphic)
                    // We'll determine end types from schema later
                    log::info!(
                        "🎯 VLP: Multi-type detected, end_table will be determined from schema"
                    );
                    None
                } else {
                    // Regular VLP: try to extract from plan, but don't fail if absent
                    let table = extract_parameterized_table_name(&graph_rel.right);
                    if table.is_none() {
                        log::info!("🔍 VLP: No explicit table for end node (may be CTE reference), will infer from schema");
                    }
                    table
                };

                // Also extract labels for filter categorization and property extraction
                // These are optional - not all nodes have labels (e.g., CTEs)
                // ✅ FIX: Use schema-aware label extraction to support multi-schema queries
                let start_label =
                    extract_node_label_from_viewscan_with_schema(&graph_rel.left, schema)
                        .unwrap_or_default();
                let end_label =
                    extract_node_label_from_viewscan_with_schema(&graph_rel.right, schema)
                        .unwrap_or_default();

                // 🔧 PARAMETERIZED VIEW FIX: Get rel_table with parameterized view syntax if applicable
                // First try to extract parameterized table from ViewScan, fallback to schema lookup
                let center_plan_desc = match graph_rel.center.as_ref() {
                    LogicalPlan::Empty => "Empty".to_string(),
                    LogicalPlan::ViewScan(vs) => format!("ViewScan({})", vs.source_table),
                    LogicalPlan::GraphNode(n) => format!("GraphNode({})", n.alias),
                    LogicalPlan::GraphRel(_) => "GraphRel".to_string(),
                    LogicalPlan::Filter(_) => "Filter".to_string(),
                    LogicalPlan::Projection(_) => "Projection".to_string(),
                    _ => "Other".to_string(),
                };
                log::info!("🔍 VLP: Center plan = {}", center_plan_desc);

                let rel_table = match graph_rel.center.as_ref() {
                    LogicalPlan::ViewScan(_) => {
                        // Use extract_parameterized_rel_table for parameterized view support
                        let result = extract_parameterized_rel_table(graph_rel.center.as_ref());
                        log::info!(
                            "🔍 VLP: extract_parameterized_rel_table returned: {:?}",
                            result
                        );
                        result.unwrap_or_else(|| {
                            log::debug!("Failed to extract parameterized rel table from ViewScan");
                            "unknown_rel_table".to_string()
                        })
                    }
                    _ => {
                        // Schema-based lookup with node types for polymorphic relationships
                        let rel_type = if let Some(labels) = &graph_rel.labels {
                            labels.first().unwrap_or(&graph_rel.alias)
                        } else {
                            &graph_rel.alias
                        };

                        // For VLP with different start/end labels (e.g., Message→Post),
                        // the recursive traversal should use start→start relationship (Message→Message)
                        // Only the initial base case needs start→end
                        let (lookup_from, lookup_to) = if !start_label.is_empty()
                            && !end_label.is_empty()
                            && start_label != end_label
                        {
                            // Different labels: use start→start for recursive traversal
                            log::info!("🔍 VLP with different labels: {}→{}. Using {}→{} for recursive traversal",
                                start_label, end_label, start_label, start_label);
                            (Some(start_label.as_str()), Some(start_label.as_str()))
                        } else {
                            // Same label or missing: use as-is
                            (Some(start_label.as_str()), Some(end_label.as_str()))
                        };

                        // 🔧 PARAMETERIZED VIEW FIX: Extract view_parameter_values from node ViewScans
                        // The node ViewScans have the parameter values; use them for the relationship table too
                        let view_params = extract_view_parameter_values(&graph_rel.left)
                            .or_else(|| extract_view_parameter_values(&graph_rel.right))
                            .unwrap_or_default();

                        // Use schema lookup with node types and parameterized view support
                        // 🔧 FIX: Use the schema parameter directly instead of context.schema()
                        if !view_params.is_empty() {
                            log::info!(
                                "🔧 VLP: Using parameterized view lookup with params: {:?}",
                                view_params
                            );
                            rel_type_to_table_name_with_nodes_and_params(
                                rel_type,
                                lookup_from,
                                lookup_to,
                                schema,
                                &view_params,
                            )
                        } else {
                            rel_type_to_table_name_with_nodes(
                                rel_type,
                                lookup_from,
                                lookup_to,
                                schema,
                            )
                        }
                    }
                };

                // For relationship column lookup, we need the plain table name (without parameters or backticks)
                // Extract plain table name for schema lookups:
                // 1. Remove parameterized suffix: `db.table`(param = 'value') → `db.table`
                // 2. Remove backticks: `db.table` → db.table
                let rel_table_plain = {
                    let without_params = if rel_table.contains('(') {
                        rel_table
                            .split('(')
                            .next()
                            .unwrap_or(&rel_table)
                            .to_string()
                    } else {
                        rel_table.clone()
                    };
                    // Remove backticks that may be present from parameterized view syntax
                    without_params.trim_matches('`').to_string()
                };

                // Extract relationship columns from schema using the plain table name
                log::debug!(
                    "🔧 VLP: Extract rel columns for table: {} (plain: {})",
                    rel_table,
                    rel_table_plain
                );
                // Use schema lookup for the relationship table columns
                let rel_cols =
                    extract_relationship_columns_from_table_with_schema(&rel_table_plain, schema);
                let from_col = rel_cols.from_id;
                let to_col = rel_cols.to_id;
                log::debug!(
                    "🔧 VLP: Final columns: from_col='{}', to_col='{}' for table '{}'",
                    from_col,
                    to_col,
                    rel_table
                );

                // ⚠️ CRITICAL: Node ID Column Selection (Multi-Schema Support)
                // ========================================================
                // ClickGraph supports TWO fundamentally different schema patterns:
                //
                // 1. TRADITIONAL SCHEMA (separate node & edge tables):
                //    - Node table exists: users, posts, etc.
                //    - node_schema.node_id points to PHYSICAL column in node table
                //    - Example: User.node_id="user_id" → users.user_id
                //    - VLP Strategy: Use node_schema.node_id.column()
                //
                // 2. DENORMALIZED SCHEMA (virtual nodes in edge table):
                //    - Node table is VIRTUAL (points to edge table)
                //    - node_schema.node_id is LOGICAL property name
                //    - Physical ID is in relationship columns (from_id/to_id)
                //    - Example: Airport.node_id="code" but physical is flights.Origin
                //    - VLP Strategy: Use relationship columns (from_col/to_col)
                //
                // 🚨 BREAKING HISTORY:
                // - Dec 22, 2025: Changed to use node_schema.node_id without checking is_denormalized
                // - Result: Denormalized VLP broke (3 tests marked xfail)
                // - Dec 25, 2025: Fixed by checking is_denormalized flag
                //
                // 🧪 TESTING REQUIREMENT:
                // ANY change to this logic MUST test BOTH schema types:
                // - tests/integration/test_variable_paths.py (traditional)
                // - tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths (denormalized)
                //
                // See: docs/development/schema-testing-requirements.md
                let start_id_col = if !start_label.is_empty() {
                    if let Ok(node_schema) = schema.node_schema(&start_label) {
                        if node_schema.is_denormalized {
                            // For denormalized nodes, use relationship column
                            from_col.to_string()
                        } else {
                            // For traditional nodes, use node schema's node_id (first column for composite)
                            node_schema
                                .node_id
                                .id
                                .columns()
                                .first()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| from_col.to_string())
                        }
                    } else {
                        // Fallback: use relationship's from_id
                        log::warn!("⚠️ VLP: Could not find node schema for '{}', using relationship from_id '{}'", start_label, from_col);
                        from_col.to_string()
                    }
                } else {
                    // No label available, use relationship columns
                    from_col.to_string()
                };

                let end_id_col = if !end_label.is_empty() {
                    if let Ok(node_schema) = schema.node_schema(&end_label) {
                        if node_schema.is_denormalized {
                            // For denormalized nodes, use relationship column
                            to_col.to_string()
                        } else {
                            // For traditional nodes, use node schema's node_id (first column for composite)
                            node_schema
                                .node_id
                                .id
                                .columns()
                                .first()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| to_col.to_string())
                        }
                    } else {
                        // Fallback: use relationship's to_id
                        log::warn!("⚠️ VLP: Could not find node schema for '{}', using relationship to_id '{}'", end_label, to_col);
                        to_col.to_string()
                    }
                } else {
                    // No label available, use relationship columns
                    to_col.to_string()
                };

                log::debug!(
                    "🔧 VLP: Node ID columns: start_id_col='{}', end_id_col='{}'",
                    start_id_col,
                    end_id_col
                );

                // Define aliases for traversal
                // Note: GraphRel.left_connection and right_connection are ALREADY swapped based on direction
                // in match_clause.rs (lines 1088-1092), so we always use them directly:
                // - left_connection = traversal start node alias
                // - right_connection = traversal end node alias
                let start_alias = graph_rel.left_connection.clone();
                let end_alias = graph_rel.right_connection.clone();
                // Relationship alias for property filters (e.g., WHERE r.property = value)
                let rel_alias = graph_rel.alias.clone();

                // 🔧 HOLISTIC FIX: Early detection of FK-edge pattern for proper alias mapping
                // In FK-edge patterns, relationship properties are on the start_node table (not a separate rel table)
                let is_fk_edge_early = if let Some(labels) = &graph_rel.labels {
                    if let Some(first_label) = labels.first() {
                        schema
                            .get_rel_schema(first_label)
                            .map(|rel_schema| rel_schema.is_fk_edge)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                };

                if is_fk_edge_early {
                    log::info!("🔧 VLP: Detected FK-edge pattern early - relationship properties map to start_node alias");
                }

                // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                let (
                    mut start_filters_sql,
                    mut end_filters_sql,
                    rel_filters_sql,
                    categorized_filters_opt,
                ) = if let Some(where_predicate) = &graph_rel.where_predicate {
                    log::info!("🔍 GraphRel has where_predicate: {:?}", where_predicate);

                    // 🔧 CRITICAL FIX: Strip LabelExpression from where_predicate BEFORE conversion
                    // LabelExpression becomes `false` in RenderExpr, causing WHERE ... AND false
                    // For multi-type VLP, type info is encoded in UNION structure, not WHERE
                    use crate::query_planner::logical_expr::LogicalExpr;
                    fn strip_label_expr_from_logical(expr: &LogicalExpr) -> Option<LogicalExpr> {
                        match expr {
                            LogicalExpr::LabelExpression { .. } => {
                                log::info!("🔧 Stripping LabelExpression from WHERE predicate");
                                None // Remove label expressions
                            }
                            LogicalExpr::Operator(op) => {
                                use crate::query_planner::logical_expr::Operator as LogicalOperator;
                                if matches!(op.operator, LogicalOperator::And | LogicalOperator::Or)
                                {
                                    // Recursively strip from operands
                                    let stripped: Vec<LogicalExpr> = op
                                        .operands
                                        .iter()
                                        .filter_map(strip_label_expr_from_logical)
                                        .collect();
                                    match stripped.len() {
                                        0 => None, // All were labels
                                        1 => Some(stripped[0].clone()), // Unwrap single operand
                                        _ => Some(LogicalExpr::Operator(
                                            crate::query_planner::logical_expr::OperatorApplication {
                                                operator: op.operator,
                                                operands: stripped,
                                            }
                                        ))
                                    }
                                } else {
                                    Some(expr.clone()) // Keep other operators
                                }
                            }
                            _ => Some(expr.clone()), // Keep everything else
                        }
                    }

                    let cleaned_predicate = strip_label_expr_from_logical(where_predicate);

                    if cleaned_predicate.is_none() {
                        log::info!("🔧 WHERE predicate contained only label expressions - cleared");
                        (None, None, None, None)
                    } else {
                        let cleaned_predicate = cleaned_predicate.unwrap();

                        // Convert LogicalExpr to RenderExpr
                        let render_expr = RenderExpr::try_from(cleaned_predicate).map_err(|e| {
                            RenderBuildError::UnsupportedFeature(format!(
                                "Failed to convert LogicalExpr to RenderExpr: {}",
                                e
                            ))
                        })?;

                        // ⚠️ CRITICAL FIX (Jan 10, 2026): Schema-aware categorization for ALL schema variations!
                        //
                        // Problem: After property mapping, denormalized filters ALL have rel_alias:
                        //   origin.code → f.Origin (f = relationship alias)
                        //   dest.code → f.Dest (f = relationship alias)
                        //
                        // We can't categorize by table alias alone! Must check COLUMN NAME against schema.
                        //
                        // Solution: Pass schema and rel_labels so categorize_filters can check:
                        //   - Is column in from_node_properties? → start_node_filter
                        //   - Is column in to_node_properties? → end_node_filter
                        //   - Is column in property_mappings? → relationship_filter
                        let rel_labels = graph_rel.labels.clone().unwrap_or_default();
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            &rel_alias,
                            schema,
                            &rel_labels,
                        );
                        log::info!("🔍 Categorized filters (BEFORE property mapping):");
                        log::info!(
                            "  start_alias: {}, end_alias: {}, rel_alias: {}",
                            start_alias,
                            end_alias,
                            rel_alias
                        );
                        log::info!("  start_node_filters: {:?}", categorized.start_node_filters);
                        log::info!("  end_node_filters: {:?}", categorized.end_node_filters);
                        log::info!(
                            "  relationship_filters: {:?}",
                            categorized.relationship_filters
                        );

                        // Now apply property mapping to each categorized filter separately with relationship context
                        let rel_type = rel_labels.first().map(|s| s.as_str());
                        let mut mapped_start = categorized.start_node_filters.clone();
                        let mut mapped_end = categorized.end_node_filters.clone();
                        let mut mapped_rel = categorized.relationship_filters.clone();
                        let mut mapped_path = categorized.path_function_filters.clone();

                        if let Some(ref mut expr) = mapped_start {
                            apply_property_mapping_to_expr_with_context(
                                expr,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                                rel_type,
                                Some(crate::render_plan::cte_generation::NodeRole::From),
                            );
                        }
                        if let Some(ref mut expr) = mapped_end {
                            apply_property_mapping_to_expr_with_context(
                                expr,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                                rel_type,
                                Some(crate::render_plan::cte_generation::NodeRole::To),
                            );
                        }
                        if let Some(ref mut expr) = mapped_rel {
                            apply_property_mapping_to_expr(
                                expr,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                            );
                        }
                        if let Some(ref mut expr) = mapped_path {
                            apply_property_mapping_to_expr(
                                expr,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                            );
                        }

                        // 🔧 FIX: Create alias mapping based on schema type
                        // For denormalized (SingleTableScan): both nodes map to relationship alias
                        // For traditional: nodes map to start_node/end_node
                        let pattern_ctx_for_mapping =
                            recreate_pattern_schema_context(graph_rel, schema, plan_ctx).ok();
                        let (start_target_alias, end_target_alias) = if let Some(ref ctx) =
                            pattern_ctx_for_mapping
                        {
                            match &ctx.join_strategy {
                                JoinStrategy::SingleTableScan { .. } => {
                                    // Denormalized: both nodes accessed via relationship table alias
                                    log::info!("🔧 VLP Denormalized: Mapping start/end aliases to relationship table");
                                    (rel_alias.clone(), rel_alias.clone())
                                }
                                _ => {
                                    // Traditional/FK-Edge/etc: use start_node/end_node
                                    ("start_node".to_string(), "end_node".to_string())
                                }
                            }
                        } else {
                            // Fallback if pattern_ctx recreation fails
                            ("start_node".to_string(), "end_node".to_string())
                        };

                        // Create alias mapping for node aliases
                        let alias_mapping = [
                            (start_alias.clone(), start_target_alias),
                            (end_alias.clone(), end_target_alias),
                        ];

                        // 🔧 HOLISTIC FIX: Create alias mapping for relationship based on schema pattern
                        // - Standard pattern (3-way join): Maps to "rel" alias (separate edge table)
                        // - FK-edge pattern (2-way join): Maps to "start_node" alias (edge IS start node table)
                        let rel_target_alias = if is_fk_edge_early {
                            log::info!("🔧 VLP FK-edge: Mapping relationship properties to 'start_node' (no separate rel table)");
                            "start_node".to_string()
                        } else {
                            "rel".to_string()
                        };
                        let rel_alias_mapping = [(rel_alias.clone(), rel_target_alias.clone())];

                        // ⚠️ CRITICAL FIX (Jan 10, 2026): Choose correct alias mapping based on expression content!
                        //
                        // For STANDARD schemas: Filter uses node alias (e.g., `a.name = 'Alice'`)
                        //   → Use alias_mapping: a → start_node
                        //
                        // For DENORMALIZED schemas: Filter uses rel alias (e.g., `f.Origin = 'LAX'`)
                        //   → Use rel_alias_mapping: f → rel
                        //
                        // We detect by checking if the filter expression references the rel_alias or node alias.

                        // Helper to check if expression uses a specific table alias
                        fn expr_uses_alias(expr: &RenderExpr, alias: &str) -> bool {
                            match expr {
                                RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
                                RenderExpr::OperatorApplicationExp(op) => {
                                    op.operands.iter().any(|o| expr_uses_alias(o, alias))
                                }
                                _ => false,
                            }
                        }

                        // Choose mapping based on which alias the filter uses
                        let start_sql = mapped_start.as_ref().map(|expr| {
                            if expr_uses_alias(expr, &rel_alias) {
                                // Denormalized: filter uses rel_alias after property mapping
                                render_expr_to_sql_string(expr, &rel_alias_mapping)
                            } else {
                                // Standard: filter uses node alias
                                render_expr_to_sql_string(expr, &alias_mapping)
                            }
                        });
                        let end_sql = mapped_end.as_ref().map(|expr| {
                            if expr_uses_alias(expr, &rel_alias) {
                                // Denormalized: filter uses rel_alias after property mapping
                                render_expr_to_sql_string(expr, &rel_alias_mapping)
                            } else {
                                // Standard: filter uses node alias
                                render_expr_to_sql_string(expr, &alias_mapping)
                            }
                        });
                        // ✅ Relationship filters always use rel_alias_mapping
                        let rel_sql_rendered = mapped_rel
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &rel_alias_mapping));

                        // Build CategorizedFilters with both RenderExpr and pre-rendered SQL
                        // The pre-rendered SQL is used by VariableLengthCteGenerator (backward compat)
                        // while RenderExpr is used by CteManager (new path)
                        let mapped_categorized = CategorizedFilters {
                            start_node_filters: mapped_start,
                            end_node_filters: mapped_end,
                            relationship_filters: mapped_rel,
                            path_function_filters: mapped_path,
                            start_sql: start_sql.clone(),
                            end_sql: end_sql.clone(),
                            relationship_sql: rel_sql_rendered.clone(),
                        };

                        // Note: End node filters are placed INSIDE the CTE (via end_sql above).
                        // Chained pattern filters (nodes outside VLP) are handled separately in
                        // plan_builder.rs via references_only_vlp_aliases check.

                        (
                            start_sql,
                            end_sql,
                            rel_sql_rendered,
                            Some(mapped_categorized),
                        )
                    } // End of else block for cleaned_predicate.is_none()
                } else {
                    (None, None, None, None)
                };

                log::info!("🔍 After categorization and mapping:");
                log::info!("  start_filters_sql: {:?}", start_filters_sql);
                log::info!("  end_filters_sql: {:?}", end_filters_sql);
                log::info!("  rel_filters_sql: {:?}", rel_filters_sql);

                // 🔧 BOUND NODE FIX: Extract filters from bound nodes (Filter → GraphNode)
                // For ALL VLP queries, inline property predicates like {code: 'LAX'} are in Filter nodes
                // wrapping the GraphNodes, not in where_predicate.
                // Examples:
                //   - MATCH path = (p1:Person {id: 1})-[:KNOWS*]-(p2:Person {id: 2})
                //   - MATCH (origin:Airport {code: 'LAX'})-[:FLIGHT*1..2]->(dest:Airport {code: 'ATL'})
                log::debug!("🔍 VLP BOUND NODE FIX: Extracting inline property predicates from bound nodes...");
                log::debug!("  Start alias: {}, End alias: {}", start_alias, end_alias);
                log::debug!("  Current start_filters_sql: {:?}", start_filters_sql);
                log::debug!("  Current end_filters_sql: {:?}", end_filters_sql);
                log::debug!(
                    "  graph_rel.left type: {:?}",
                    std::mem::discriminant(graph_rel.left.as_ref())
                );
                log::debug!(
                    "  graph_rel.right type: {:?}",
                    std::mem::discriminant(graph_rel.right.as_ref())
                );

                // 🔧 FIX: Determine correct CTE aliases based on schema type
                // For denormalized (SingleTableScan), use relationship alias
                // For traditional, use "start_node"/"end_node"
                let pattern_ctx_for_filters =
                    recreate_pattern_schema_context(graph_rel, schema, plan_ctx).ok();
                let (start_cte_alias, end_cte_alias) =
                    if let Some(ref ctx) = pattern_ctx_for_filters {
                        match &ctx.join_strategy {
                            JoinStrategy::SingleTableScan { .. } => {
                                // Denormalized: both nodes accessed via relationship table alias
                                (rel_alias.clone(), rel_alias.clone())
                            }
                            _ => {
                                // Traditional/FK-Edge/etc: use start_node/end_node
                                ("start_node".to_string(), "end_node".to_string())
                            }
                        }
                    } else {
                        // Fallback if pattern_ctx recreation fails
                        ("start_node".to_string(), "end_node".to_string())
                    };

                log::info!(
                    "🔧 Filter CTE aliases: start='{}', end='{}'",
                    start_cte_alias,
                    end_cte_alias
                );

                // Extract start node filter (from left side) with relationship context for denormalized schemas
                let rel_type = graph_rel
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.first())
                    .map(|s| s.as_str());
                if let Some(bound_start_filter) = extract_bound_node_filter(
                    &graph_rel.left,
                    &start_alias,
                    &start_cte_alias,
                    rel_type,
                    Some(crate::render_plan::cte_generation::NodeRole::From),
                ) {
                    log::info!("🔧 Adding bound start node filter: {}", bound_start_filter);
                    start_filters_sql = Some(match start_filters_sql {
                        Some(existing) => {
                            format!("({}) AND ({})", existing, bound_start_filter)
                        }
                        None => bound_start_filter,
                    });
                } else {
                    log::info!("⚠️  No bound start node filter found");
                }

                // Extract end node filter (from right side)
                if let Some(bound_end_filter) = extract_bound_node_filter(
                    &graph_rel.right,
                    &end_alias,
                    &end_cte_alias,
                    rel_type,
                    Some(crate::render_plan::cte_generation::NodeRole::To),
                ) {
                    log::info!("🔧 Adding bound end node filter: {}", bound_end_filter);
                    end_filters_sql = Some(match end_filters_sql {
                        Some(existing) => format!("({}) AND ({})", existing, bound_end_filter),
                        None => bound_end_filter,
                    });
                } else {
                    log::info!("⚠️  No bound end node filter found");
                }

                log::info!("  Final start_filters_sql: {:?}", start_filters_sql);
                log::info!("  Final end_filters_sql: {:?}", end_filters_sql);

                // For optional VLP, don't include start node filters in CTE
                // The filters should remain on the base table in the final query
                if graph_rel.is_optional.unwrap_or(false) {
                    log::info!("🔧 Optional VLP: Removing start_filters_sql from CTE (will be applied to final FROM)");
                    start_filters_sql = None;
                }

                // Extract properties from filter expressions for shortest path queries
                // Even in SQL_ONLY mode, we need properties that appear in filters
                let filter_properties = if graph_rel.shortest_path_mode.is_some() {
                    use crate::render_plan::cte_generation::extract_properties_from_filter;

                    let mut props = Vec::new();

                    if let Some(categorized) = categorized_filters_opt {
                        // Extract from start filters
                        if let Some(ref filter_expr) = categorized.start_node_filters {
                            let start_props = extract_properties_from_filter(
                                filter_expr,
                                &start_alias,
                                &start_label,
                            );
                            props.extend(start_props);
                        }

                        // Extract from end filters
                        if let Some(ref filter_expr) = categorized.end_node_filters {
                            let end_props =
                                extract_properties_from_filter(filter_expr, &end_alias, &end_label);
                            props.extend(end_props);
                        }
                    }

                    props
                } else {
                    // ✨ BUG #7 FIX: For regular VLP queries, include ALL node properties
                    // This handles queries like MATCH (a)-[*]->(b) RETURN a, b
                    // where both nodes need all their properties in the CTE for the final SELECT
                    log::debug!(
                        "🔧 BUG #7: Extracting all properties for VLP query ({}-{})",
                        start_label,
                        end_label
                    );
                    let mut props = Vec::new();

                    // Get all properties for start node using the schema parameter (which is already in scope)
                    if !start_label.is_empty() {
                        if let Ok(start_node_schema) = schema.node_schema(&start_label) {
                            log::debug!(
                                "🔧 BUG #7: Found start node schema with {} properties",
                                start_node_schema.property_mappings.len()
                            );
                            // Get the node's ID columns to skip them from properties
                            // Use columns() for composite ID support
                            let start_id_columns = start_node_schema.node_id.columns();
                            // Sort keys for deterministic column ordering
                            let mut sorted_props: Vec<_> =
                                start_node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (prop_name, prop_value) in sorted_props {
                                // Skip ID property - it's already added as start_id/end_id in CTE
                                // Check DB column name (not Cypher property name) for schema-independence
                                // For composite IDs, skip all ID columns
                                if start_id_columns.contains(&prop_value.raw()) {
                                    log::debug!(
                                        "Skipping ID property '{}' → '{}' (already added as start_id)",
                                        prop_name, prop_value.raw()
                                    );
                                    continue;
                                }
                                props.push(NodeProperty {
                                    cypher_alias: start_alias.clone(),
                                    column_name: prop_value.raw().to_string(),
                                    alias: prop_name.clone(),
                                });
                            }
                        } else {
                            log::debug!(
                                "🔧 BUG #7: No schema found for start node {}",
                                start_label
                            );
                        }
                    }

                    // Get all properties for end node
                    if !end_label.is_empty() {
                        if let Ok(end_node_schema) = schema.node_schema(&end_label) {
                            log::debug!(
                                "🔧 BUG #7: Found end node schema with {} properties",
                                end_node_schema.property_mappings.len()
                            );
                            // Get the node's ID columns to skip them from properties
                            // Use columns() for composite ID support
                            let end_id_columns = end_node_schema.node_id.columns();
                            // Sort keys for deterministic column ordering
                            let mut sorted_props: Vec<_> =
                                end_node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (prop_name, prop_value) in sorted_props {
                                // Skip ID property - it's already added as start_id/end_id in CTE
                                // Check DB column name (not Cypher property name) for schema-independence
                                // For composite IDs, skip all ID columns
                                if end_id_columns.contains(&prop_value.raw()) {
                                    log::debug!(
                                        "Skipping ID property '{}' → '{}' (already added as end_id)",
                                        prop_name, prop_value.raw()
                                    );
                                    continue;
                                }
                                props.push(NodeProperty {
                                    cypher_alias: end_alias.clone(),
                                    column_name: prop_value.raw().to_string(),
                                    alias: prop_name.clone(),
                                });
                            }
                        } else {
                            log::debug!("🔧 BUG #7: No schema found for end node {}", end_label);
                        }
                    }

                    log::debug!("🔧 BUG #7: Total properties extracted: {}", props.len());
                    props
                };

                // Generate CTE with filters
                // For shortest path queries, always use recursive CTE (even for exact hops)
                // because we need proper filtering and shortest path selection logic

                // 🎯 DECISION POINT: CTE or inline JOINs?
                // BUT FIRST: Check if this is multi-type VLP (requires UNION ALL, not chained JOINs)
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type = should_use_join_expansion(graph_rel, &rel_types, schema);

                let use_chained_join = spec.exact_hop_count().is_some()
                    && graph_rel.shortest_path_mode.is_none()
                    && !is_multi_type; // Don't use chained JOINs for multi-type VLP

                if use_chained_join {
                    // 🚀 OPTIMIZATION: Fixed-length, non-shortest-path → NO CTE!
                    // Generate inline JOINs instead of recursive CTE
                    let exact_hops = spec.exact_hop_count().unwrap();
                    println!(
                        "CTE BRANCH: Fixed-length pattern (*{}) detected - generating inline JOINs",
                        exact_hops
                    );

                    // Build VlpContext with all necessary information
                    if let Some(vlp_ctx) = build_vlp_context(graph_rel, schema) {
                        // Generate inline JOINs using expand_fixed_length_joins_with_context
                        let (from_table, from_alias, joins) =
                            expand_fixed_length_joins_with_context(&vlp_ctx);

                        // Store the generated JOINs in context for later retrieval
                        context.set_fixed_length_joins(
                            &vlp_ctx.start_alias,
                            &vlp_ctx.end_alias,
                            from_table,
                            from_alias,
                            joins,
                        );

                        println!(
                            "CTE BRANCH: Stored fixed-length JOINs for {}-{} pattern",
                            vlp_ctx.start_alias, vlp_ctx.end_alias
                        );
                    } else {
                        log::debug!(
                            "Failed to build VlpContext for fixed-length pattern - falling back to CTE"
                        );
                        // Fall through to CTE generation below
                    }

                    // Extract CTEs from BOTH child branches (left may contain other VLPs)
                    let mut child_ctes = extract_ctes_with_context(
                        &graph_rel.left,
                        last_node_alias,
                        context,
                        schema,
                        plan_ctx,
                    )?;
                    child_ctes.extend(extract_ctes_with_context(
                        &graph_rel.right,
                        last_node_alias,
                        context,
                        schema,
                        plan_ctx,
                    )?);

                    return Ok(child_ctes);
                } else {
                    // ✅ Truly variable-length or shortest path → Check if multi-type
                    println!("CTE BRANCH: Variable-length pattern detected");
                    log::info!("🔍 VLP: Variable-length or shortest path detected (not using chained JOINs)");

                    // 🎯 CHECK FOR MULTI-TYPE VLP (Part 1D implementation)
                    let mut rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                    log::info!("🔍 VLP: rel_types={:?}", rel_types);

                    let is_multi_type_check =
                        should_use_join_expansion(graph_rel, &rel_types, schema);
                    log::info!(
                        "🔍 VLP: should_use_join_expansion returned: {}",
                        is_multi_type_check
                    );

                    if should_use_join_expansion(graph_rel, &rel_types, schema) {
                        // Multi-type VLP: Use JOIN expansion with UNION ALL
                        log::info!("🎯 CTE: Using JOIN expansion for multi-type VLP");

                        // Extract start labels from graph pattern
                        let start_labels =
                            extract_node_labels(&graph_rel.left).unwrap_or_else(|| {
                                // Fallback: extract from ViewScan
                                vec![extract_node_label_from_viewscan_with_schema(
                                    &graph_rel.left,
                                    schema,
                                )
                                .unwrap_or_else(|| "UnknownStartType".to_string())]
                            });

                        // For multi-type VLP, we need ALL possible end types from the relationship schema
                        // The GraphNode label might only have one type (from type inference),
                        // but the actual query could reach multiple types
                        let mut end_labels: Vec<String> = Vec::new();

                        // First, try to get explicit labels from the graph pattern
                        if let Some(labels) = extract_node_labels(&graph_rel.right) {
                            end_labels = labels;
                        }

                        // If no labels set, derive all possible end types from relationships.
                        // When end_labels is already set (by PatternResolver or explicit typing
                        // like `(b:Post)`), respect the constraint — don't re-expand.
                        if end_labels.is_empty() {
                            let mut possible_end_types = std::collections::HashSet::new();

                            // 🔧 FIX: If rel_types is empty (unlabeled relationship pattern),
                            // query schema for ALL relationships from start node
                            if rel_types.is_empty() {
                                log::info!("🔍 CTE: No relationship types specified, inferring from schema");
                                // Get all relationships that involve the start node type(s)
                                let mut all_rel_types = std::collections::HashSet::new();

                                // Use unique relationship types to avoid duplicates
                                let unique_rel_types = schema.get_unique_relationship_types();
                                for rel_type in unique_rel_types {
                                    if let Ok(rel_schema) = schema.get_rel_schema(&rel_type) {
                                        // Check if this relationship connects from or to any start node
                                        for start_label in &start_labels {
                                            if rel_schema.from_node == *start_label
                                                || rel_schema.to_node == *start_label
                                            {
                                                all_rel_types.insert(rel_type.clone());
                                                break;
                                            }
                                        }
                                    }
                                }

                                rel_types = all_rel_types.into_iter().collect();
                                log::info!(
                                    "🎯 CTE: Inferred {} relationship types: {:?}",
                                    rel_types.len(),
                                    rel_types
                                );
                            }

                            for rel_type in &rel_types {
                                if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                                    // For undirected patterns, we need to consider both directions
                                    // But we should add the OTHER node in each relationship, not both blindly
                                    for start_label in &start_labels {
                                        let from_matches = rel_schema.from_node == *start_label
                                            || rel_schema.from_node == "$any";
                                        let to_matches = rel_schema.to_node == *start_label
                                            || rel_schema.to_node == "$any";

                                        // If relationship goes FROM start_label, add to_node as possible end
                                        if from_matches {
                                            if rel_schema.to_node == "$any" {
                                                // Polymorphic: expand to all concrete node types
                                                for nt in schema.all_node_schemas().keys() {
                                                    possible_end_types.insert(nt.clone());
                                                }
                                            } else {
                                                possible_end_types
                                                    .insert(rel_schema.to_node.clone());
                                            }
                                        }
                                        // If relationship goes TO start_label, add from_node as possible end
                                        if to_matches {
                                            if rel_schema.from_node == "$any" {
                                                for nt in schema.all_node_schemas().keys() {
                                                    possible_end_types.insert(nt.clone());
                                                }
                                            } else {
                                                possible_end_types
                                                    .insert(rel_schema.from_node.clone());
                                            }
                                        }
                                    }
                                }
                            }

                            if possible_end_types.len() > 1 {
                                // Multiple possible end types - use all of them
                                end_labels = possible_end_types.into_iter().collect();
                                log::info!(
                                    "🎯 CTE: Expanded end_labels from relationships: {:?}",
                                    end_labels
                                );
                            } else if end_labels.is_empty() && possible_end_types.len() == 1 {
                                // Single possible type
                                end_labels = possible_end_types.into_iter().collect();
                                log::info!("🎯 CTE: Found single end_label: {:?}", end_labels);
                            } else if end_labels.is_empty() {
                                // Last resort fallback: extract from ViewScan
                                end_labels = vec![extract_node_label_from_viewscan_with_schema(
                                    &graph_rel.right,
                                    schema,
                                )
                                .unwrap_or_else(|| "UnknownEndType".to_string())];
                            }
                        }

                        log::info!(
                            "🎯 CTE Multi-type VLP: start_labels={:?}, rel_types={:?}, end_labels={:?}",
                            start_labels, rel_types, end_labels
                        );

                        // 🔧 PARAMETERIZED VIEW FIX: Extract view parameters from the graph pattern
                        // Try to get from left (start node) first, then try right (end node)
                        let view_parameter_values = extract_view_parameter_values(&graph_rel.left)
                            .or_else(|| extract_view_parameter_values(&graph_rel.right))
                            .unwrap_or_default();
                        log::debug!(
                            "🔧 Multi-type VLP: Extracted view_parameter_values: {:?}",
                            view_parameter_values
                        );

                        // Create the generator
                        use crate::clickhouse_query_generator::MultiTypeVlpJoinGenerator;

                        // For multi-type VLP, we use start_filters_sql and end_filters_sql directly
                        // The schema filters are handled differently in JOIN expansion
                        let is_undirected = graph_rel.was_undirected.unwrap_or(false);
                        let generator = MultiTypeVlpJoinGenerator::new(
                            schema,
                            start_labels,
                            rel_types,
                            end_labels,
                            spec.clone(),
                            start_alias.clone(),
                            end_alias.clone(),
                            start_filters_sql.clone(),
                            end_filters_sql.clone(),
                            rel_filters_sql.clone(),
                            view_parameter_values,
                            plan_ctx.map(|ctx| std::sync::Arc::new(ctx.clone())),
                            is_undirected,
                        );

                        // TODO: Add property projections based on what's needed in RETURN clause
                        // For now, we'll generate without specific property projections
                        // Properties will be handled by the analyzer in Phase 5

                        // Generate CTE name - use vlp_ prefix for proper detection
                        // The plan_builder.rs looks for CTEs starting with "vlp_" or "chained_path_"
                        // Use start_alias_end_alias order as-is — undirected patterns create
                        // separate CTEs for each direction (a→o vs o→a) with different content
                        let cte_name = format!("vlp_multi_type_{}_{}", start_alias, end_alias);

                        // Generate SQL
                        match generator.generate_cte_sql(&cte_name) {
                            Ok(cte_sql) => {
                                log::info!("🎯 CTE Multi-type VLP SQL generated successfully");
                                log::debug!("Generated SQL:\n{}", cte_sql);

                                // Create CTE wrapper with all required fields
                                let cte = Cte {
                                    cte_name: cte_name.clone(),
                                    content: CteContent::RawSql(cte_sql),
                                    is_recursive: false, // Multi-type VLP uses UNION ALL, not recursive
                                    vlp_start_alias: Some("start_node".to_string()),
                                    vlp_end_alias: Some("end_node".to_string()),
                                    vlp_start_table: None, // Will be filled by generator if needed
                                    vlp_end_table: None,
                                    vlp_cypher_start_alias: Some(start_alias.clone()),
                                    vlp_cypher_end_alias: Some(end_alias.clone()),
                                    vlp_start_id_col: None,
                                    vlp_end_id_col: None,
                                    vlp_path_variable: graph_rel.path_variable.clone(),
                                    // Multi-type VLP: basic column metadata (start_id and end_id for ID lookups)
                                    columns: vec![
                                        crate::render_plan::CteColumnMetadata {
                                            cte_column_name: VLP_START_ID_COLUMN.to_string(),
                                            cypher_alias: start_alias.clone(),
                                            cypher_property: "id".to_string(),
                                            db_column: "id".to_string(),
                                            is_id_column: true,
                                            vlp_position: Some(crate::render_plan::cte_manager::VlpColumnPosition::Start),
                                        },
                                        crate::render_plan::CteColumnMetadata {
                                            cte_column_name: VLP_END_ID_COLUMN.to_string(),
                                            cypher_alias: end_alias.clone(),
                                            cypher_property: "id".to_string(),
                                            db_column: "id".to_string(),
                                            is_id_column: true,
                                            vlp_position: Some(crate::render_plan::cte_manager::VlpColumnPosition::End),
                                        },
                                    ],
                                    from_alias: Some(VLP_CTE_FROM_ALIAS.to_string()),
                                    outer_where_filters: None, // Multi-type VLP doesn't need outer filters
                                    with_exported_aliases: Vec::new(),
                                    variable_registry: None,
                                };

                                // Register CTE name in context for deterministic FROM/JOIN references
                                // Register CTE name in task-local QueryContext for lookup during nested rendering
                                crate::server::query_context::register_relationship_cte_name(
                                    &graph_rel.alias,
                                    &cte_name,
                                );
                                eprintln!(
                                    "🔥🔥🔥 REGISTRATION: alias='{}' → cte_name='{}'",
                                    graph_rel.alias, cte_name
                                );
                                log::debug!(
                                    "📝 Registered multi-type VLP CTE: alias='{}' → cte_name='{}'",
                                    graph_rel.alias,
                                    cte_name
                                );

                                // Extract CTEs from BOTH child branches (left may contain other VLPs)
                                let mut child_ctes = extract_ctes_with_context(
                                    &graph_rel.left,
                                    last_node_alias,
                                    context,
                                    schema,
                                    plan_ctx,
                                )?;
                                child_ctes.extend(extract_ctes_with_context(
                                    &graph_rel.right,
                                    last_node_alias,
                                    context,
                                    schema,
                                    plan_ctx,
                                )?);
                                child_ctes.push(cte);

                                return Ok(child_ctes);
                            }
                            Err(e) => {
                                return Err(RenderBuildError::UnsupportedFeature(format!(
                                    "Failed to generate multi-type VLP SQL: {}. \
                                     This may indicate missing schema information or unsupported path combination.",
                                    e
                                )));
                            }
                        }
                    }

                    // Single-type VLP: Use traditional recursive CTE
                    println!("CTE BRANCH: Single-type VLP - using recursive CTE");

                    // ✨ PHASE 2 REFACTORING: Use PatternSchemaContext instead of scattered is_denormalized checks
                    // Recreate the pattern schema context to determine JOIN strategy and node access patterns
                    let pattern_ctx = match recreate_pattern_schema_context(
                        graph_rel, schema, plan_ctx,
                    ) {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            log::warn!("⚠️ Failed to recreate PatternSchemaContext, falling back to denormalized flag checks: {}", e);
                            // Fallback: use old logic if context recreation fails
                            let start_is_denormalized = match graph_rel.left.as_ref() {
                                LogicalPlan::GraphNode(node) => node.is_denormalized,
                                _ => false,
                            };
                            let end_is_denormalized = match graph_rel.right.as_ref() {
                                LogicalPlan::GraphNode(node) => node.is_denormalized,
                                _ => false,
                            };
                            let both_denormalized = start_is_denormalized && end_is_denormalized;
                            let is_mixed = start_is_denormalized != end_is_denormalized;

                            // Continue with old logic...
                            log::debug!("CTE: Using fallback - start_denormalized={}, end_denormalized={}, both={}, mixed={}",
                                start_is_denormalized, end_is_denormalized, both_denormalized, is_mixed);

                            // Create a minimal pattern for continuation
                            return Err(RenderBuildError::UnsupportedFeature(format!(
                                "Failed to recreate PatternSchemaContext: {}. Consider updating schema configuration.", e
                            )));
                        }
                    };

                    // Determine node access strategies from PatternSchemaContext
                    let both_denormalized = matches!(
                        pattern_ctx.join_strategy,
                        JoinStrategy::SingleTableScan { .. }
                    );

                    let is_mixed =
                        matches!(pattern_ctx.join_strategy, JoinStrategy::MixedAccess { .. });

                    // Determine FK-edge pattern from JoinStrategy
                    let is_fk_edge =
                        matches!(pattern_ctx.join_strategy, JoinStrategy::FkEdgeJoin { .. });

                    // Extract individual denormalized flags for old generators that still need them
                    // TODO: Phase 2 continuation - refactor generators to use PatternSchemaContext directly
                    let start_is_denormalized = pattern_ctx.left_node.is_embedded();
                    let end_is_denormalized = pattern_ctx.right_node.is_embedded();

                    log::debug!("CTE: Using PatternSchemaContext - both_denormalized={}, is_mixed={}, is_fk_edge={}, strategy={:?}",
                        both_denormalized, is_mixed, is_fk_edge, pattern_ctx.join_strategy);
                    log::debug!(
                        "CTE: Individual flags - start_is_denormalized={}, end_is_denormalized={}",
                        start_is_denormalized,
                        end_is_denormalized
                    );

                    // Get edge properties from PatternSchemaContext
                    // Note: edge_id is not in PatternSchemaContext yet, so get it from schema directly
                    let edge_id = if let Some(labels) = &graph_rel.labels {
                        labels
                            .first()
                            .and_then(|label| schema.get_rel_schema(label).ok())
                            .and_then(|rel_schema| rel_schema.edge_id.clone())
                    } else {
                        None
                    };
                    let _type_column = pattern_ctx.edge.type_column();
                    let _from_label_column = pattern_ctx.edge.from_label_column();
                    let _to_label_column = pattern_ctx.edge.to_label_column();

                    // 🎯 Extract schema filters from start and end nodes
                    // Schema filters are defined in YAML and should be applied to CTE base/recursive cases
                    let start_schema_filter =
                        extract_schema_filter_from_node(&graph_rel.left, "start_node");
                    let end_schema_filter =
                        extract_schema_filter_from_node(&graph_rel.right, "end_node");

                    // Combine user filters with schema filters using AND
                    let combined_start_filters = match (&start_filters_sql, &start_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };

                    let combined_end_filters = match (&end_filters_sql, &end_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };

                    if start_schema_filter.is_some() || end_schema_filter.is_some() {
                        log::info!(
                            "CTE: Applying schema filters - start: {:?}, end: {:?}",
                            start_schema_filter,
                            end_schema_filter
                        );
                    }

                    // ========================================================================
                    // 🚀 CteManager Integration (Phase 2): Use unified API for VLP generation
                    // ========================================================================

                    // Determine properties based on pattern type
                    let vlp_properties = if both_denormalized {
                        log::debug!("🔧 CTE: Using denormalized generator for variable-length path (both nodes virtual)");
                        log::debug!(
                            "🔧 CTE: rel_table={}, filter_properties count={}",
                            rel_table,
                            filter_properties.len()
                        );

                        // For denormalized nodes, extract ALL properties from the node schema
                        // (not just filter properties, since properties come from the edge table)
                        let mut all_denorm_properties = filter_properties.clone();

                        // Get node schema to extract all from_properties and to_properties
                        // Handle both "table" and "database.table" formats
                        let rel_table_name = rel_table.rsplit('.').next().unwrap_or(&rel_table);

                        if let Some(node_schema) = schema.all_node_schemas().values().find(|n| {
                            let schema_table =
                                n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
                            schema_table == rel_table_name
                        }) {
                            log::debug!("🔧 CTE: Found node schema for table {}", rel_table);

                            // Add all from_node properties
                            if let Some(ref from_props) = node_schema.from_properties {
                                log::debug!(
                                    "🔧 CTE: Adding {} from_node properties",
                                    from_props.len()
                                );
                                for logical_prop in from_props.keys() {
                                    if !all_denorm_properties.iter().any(|p| {
                                        p.cypher_alias == graph_rel.left_connection
                                            && p.alias == *logical_prop
                                    }) {
                                        log::trace!(
                                            "🔧 CTE: Adding from property: {}",
                                            logical_prop
                                        );
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: graph_rel.left_connection.clone(),
                                            column_name: logical_prop.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }

                            // Add all to_node properties
                            if let Some(ref to_props) = node_schema.to_properties {
                                log::debug!("🔧 CTE: Adding {} to_node properties", to_props.len());
                                for logical_prop in to_props.keys() {
                                    if !all_denorm_properties.iter().any(|p| {
                                        p.cypher_alias == graph_rel.right_connection
                                            && p.alias == *logical_prop
                                    }) {
                                        log::trace!("🔧 CTE: Adding to property: {}", logical_prop);
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: graph_rel.right_connection.clone(),
                                            column_name: logical_prop.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }
                        } else {
                            log::debug!("❌ CTE: No node schema found for table {}", rel_table);
                        }

                        log::debug!(
                            "🔧 CTE: Final all_denorm_properties count: {}",
                            all_denorm_properties.len()
                        );
                        all_denorm_properties
                    } else {
                        // Non-denormalized patterns use filter_properties directly
                        filter_properties.clone()
                    };

                    log::info!("🔍 VLP CTE: Using CteManager with filters:");
                    log::info!("  combined_start_filters: {:?}", combined_start_filters);
                    log::info!("  combined_end_filters: {:?}", combined_end_filters);
                    log::info!("  rel_filters_sql: {:?}", rel_filters_sql);

                    // Generate VLP CTE via unified CteManager API
                    let var_len_cte = generate_vlp_cte_via_manager(
                        &pattern_ctx,
                        schema,
                        spec.clone(),
                        vlp_properties,
                        combined_start_filters,
                        combined_end_filters,
                        rel_filters_sql,
                        graph_rel.path_variable.clone(),
                        graph_rel.shortest_path_mode.clone(),
                        graph_rel.labels.clone(),
                        edge_id,
                        Some(rel_alias.clone()),
                        Some(start_label.clone()),
                        Some(end_label.clone()),
                        graph_rel.is_optional,
                    )?;

                    // TODO(multi-vlp): Per-VLP unique aliases (vt0, vt1) are used in
                    // inference-phase join conditions, but the render phase (VLPExprRewriter,
                    // select_builder, to_sql_query, from_builder) still uses VLP_CTE_FROM_ALIAS
                    // ("t") for FROM alias and expression rendering. Until all render-phase
                    // code is updated to use per-VLP aliases, we keep from_alias as "t" and
                    // don't call register_vlp_cte_outer_alias(). Wiring it up prematurely
                    // would break t.start_id/t.end_id references in SELECT/JOIN clauses.

                    // Extract CTEs from BOTH child branches (left may contain other VLPs)
                    let mut child_ctes = extract_ctes_with_context(
                        &graph_rel.left,
                        last_node_alias,
                        context,
                        schema,
                        plan_ctx,
                    )?;
                    child_ctes.extend(extract_ctes_with_context(
                        &graph_rel.right,
                        last_node_alias,
                        context,
                        schema,
                        plan_ctx,
                    )?);
                    child_ctes.push(var_len_cte);

                    return Ok(child_ctes);
                }
            }

            // Handle multiple relationship types for regular single-hop relationships
            let mut relationship_ctes = vec![];

            // === PATTERNRESOLVER 2.0: HANDLE PATTERN COMBINATIONS ===
            // Check for deferred UNION - pattern_combinations contains (from_label, rel_type, to_label)
            // This enables ()-[r]->() patterns where BOTH nodes AND relationships vary
            if let Some(ref combinations) = graph_rel.pattern_combinations {
                log::info!(
                    "🔀 PatternResolver 2.0: Found {} pattern combinations in GraphRel, generating UNION CTE",
                    combinations.len()
                );

                // Collect all unique relationship property names across all combinations
                // so they can be exposed as direct CTE columns (with NULL for missing ones)
                let all_rel_props: Vec<String> = {
                    let mut props = std::collections::BTreeSet::new();
                    for combo in combinations {
                        if let Some(rs) = schema.get_relationships_schema_opt(&combo.rel_type) {
                            for cypher_name in rs.property_mappings.keys() {
                                props.insert(cypher_name.clone());
                            }
                        }
                    }
                    props.into_iter().collect()
                };

                // Generate a SELECT for each combination: full pattern JOIN
                // Each branch: (from_node_table JOIN rel_table JOIN to_node_table)
                let union_branches: Result<Vec<String>, RenderBuildError> = combinations
                    .iter()
                    .map(|combo| {
                        // Get schemas for this combination
                        let from_node_schema =
                            schema.node_schema(&combo.from_label).map_err(|_| {
                                RenderBuildError::NodeSchemaNotFound(format!(
                                    "Node schema not found for '{}'",
                                    combo.from_label
                                ))
                            })?;

                        let rel_schema = schema
                            .get_rel_schema_with_nodes(
                                &combo.rel_type,
                                Some(&combo.from_label),
                                Some(&combo.to_label),
                            )
                            .map_err(|_| {
                                RenderBuildError::MissingTableInfo(format!(
                                    "Relationship schema not found for '{}' between '{}' and '{}'",
                                    combo.rel_type, combo.from_label, combo.to_label
                                ))
                            })?;

                        let to_node_schema = schema.node_schema(&combo.to_label).map_err(|_| {
                            RenderBuildError::NodeSchemaNotFound(format!(
                                "Node schema not found for '{}'",
                                combo.to_label
                            ))
                        })?;

                        // Table names
                        let raw_from_table = format!(
                            "{}.{}",
                            from_node_schema.database, from_node_schema.table_name
                        );
                        let rel_table =
                            format!("{}.{}", rel_schema.database, rel_schema.table_name);
                        let raw_to_table =
                            format!("{}.{}", to_node_schema.database, to_node_schema.table_name);

                        // Self-join detection: when both endpoints are the same table,
                        // we need distinct aliases so ClickHouse can distinguish them.
                        let is_self_join = raw_from_table == raw_to_table;
                        let (from_table, from_join_expr, to_table, to_join_expr) = if is_self_join {
                            let from_alias = "from_node".to_string();
                            let to_alias = "to_node".to_string();
                            (
                                from_alias.clone(),
                                format!("{} AS {}", raw_from_table, from_alias),
                                to_alias.clone(),
                                format!("{} AS {}", raw_to_table, to_alias),
                            )
                        } else {
                            (
                                raw_from_table.clone(),
                                raw_from_table.clone(),
                                raw_to_table.clone(),
                                raw_to_table.clone(),
                            )
                        };

                        // ID columns (as Identifier for composite support)
                        let from_node_id = &from_node_schema.node_id.id;
                        let to_node_id = &to_node_schema.node_id.id;
                        let rel_from_col = &rel_schema.from_id;
                        let rel_to_col = &rel_schema.to_id;

                        // Generate SELECT for this branch
                        // Output columns matching VLP/path pattern expectations:
                        // - start_id, end_id (for ID lookups)
                        // - path_relationships (array with relationship type)
                        // - rel_properties (array with relationship properties as JSON)

                        // Collect relationship properties for formatRowNoNewline
                        let rel_prop_cols: Vec<String> = rel_schema
                            .property_mappings
                            .iter()
                            .map(|(cypher_name, prop_val)| {
                                let col_name = match prop_val {
                                    PropertyValue::Column(c) => c.clone(),
                                    PropertyValue::Expression(e) => e.clone(), // Use expression as-is
                                };
                                format!("{rel_table}.{col_name} AS {cypher_name}")
                            })
                            .collect();

                        let rel_properties_json = if rel_prop_cols.is_empty() {
                            "'{}'".to_string() // Empty JSON object
                        } else {
                            format!(
                                "formatRowNoNewline('JSONEachRow', {})",
                                rel_prop_cols.join(", ")
                            )
                        };

                        // Collect node properties for start_properties and end_properties
                        // This enables path queries: MATCH p=()-->() RETURN p
                        let start_prop_cols: Vec<String> = from_node_schema
                            .property_mappings
                            .values()
                            .map(|prop_val| {
                                let col_name = match prop_val {
                                    PropertyValue::Column(c) => c.clone(),
                                    PropertyValue::Expression(e) => e.clone(),
                                };
                                // Include both the column and its alias for proper JSON formatting
                                format!("{from_table}.{col_name}")
                            })
                            .collect();

                        let end_prop_cols: Vec<String> = to_node_schema
                            .property_mappings
                            .values()
                            .map(|prop_val| {
                                let col_name = match prop_val {
                                    PropertyValue::Column(c) => c.clone(),
                                    PropertyValue::Expression(e) => e.clone(),
                                };
                                format!("{to_table}.{col_name}")
                            })
                            .collect();

                        let start_properties_json = if start_prop_cols.is_empty() {
                            "'{}'".to_string()
                        } else {
                            format!(
                                "formatRowNoNewline('JSONEachRow', {})",
                                start_prop_cols.join(", ")
                            )
                        };

                        let end_properties_json = if end_prop_cols.is_empty() {
                            "'{}'".to_string()
                        } else {
                            format!(
                                "formatRowNoNewline('JSONEachRow', {})",
                                end_prop_cols.join(", ")
                            )
                        };

                        // Extract base relationship type (strip ::FromLabel::ToLabel suffix)
                        let base_rel_type =
                            combo.rel_type.split("::").next().unwrap_or(&combo.rel_type);

                        // Build WHERE clauses for polymorphic type filtering
                        let mut where_clauses = Vec::new();
                        if let Some(ref type_col) = rel_schema.type_column {
                            where_clauses
                                .push(format!("{rel_table}.{type_col} = '{base_rel_type}'"));
                        }
                        if let Some(ref from_lbl_col) = rel_schema.from_label_column {
                            where_clauses.push(format!(
                                "{rel_table}.{from_lbl_col} = '{}'",
                                combo.from_label
                            ));
                        }
                        if let Some(ref to_lbl_col) = rel_schema.to_label_column {
                            where_clauses
                                .push(format!("{rel_table}.{to_lbl_col} = '{}'", combo.to_label));
                        }
                        let where_clause = if where_clauses.is_empty() {
                            String::new()
                        } else {
                            format!(" WHERE {}", where_clauses.join(" AND "))
                        };

                        // Generate start_id and end_id expressions for composite support
                        let start_id_expr = match from_node_id {
                            Identifier::Single(col) => format!("toString({from_table}.{col})"),
                            Identifier::Composite(cols) => {
                                let parts: Vec<String> = cols
                                    .iter()
                                    .map(|c| format!("toString({from_table}.{c})"))
                                    .collect();
                                format!("concat({})", parts.join(", '|', "))
                            }
                        };
                        let end_id_expr = match to_node_id {
                            Identifier::Single(col) => format!("toString({to_table}.{col})"),
                            Identifier::Composite(cols) => {
                                let parts: Vec<String> = cols
                                    .iter()
                                    .map(|c| format!("toString({to_table}.{c})"))
                                    .collect();
                                format!("concat({})", parts.join(", '|', "))
                            }
                        };

                        // Generate JOIN conditions for composite support
                        let from_join_parts: Vec<String> = from_node_id
                            .columns()
                            .iter()
                            .zip(rel_from_col.columns().iter())
                            .map(|(n, r)| format!("{from_table}.{n} = {rel_table}.{r}"))
                            .collect();
                        let from_join_cond = from_join_parts.join(" AND ");

                        let to_join_parts: Vec<String> = to_node_id
                            .columns()
                            .iter()
                            .zip(rel_to_col.columns().iter())
                            .map(|(n, r)| format!("{to_table}.{n} = {rel_table}.{r}"))
                            .collect();
                        let to_join_cond = to_join_parts.join(" AND ");

                        // Build direct relationship property columns for outer query access
                        let direct_rel_cols: String = all_rel_props
                            .iter()
                            .map(|prop_name| {
                                if let Some(prop_val) = rel_schema.property_mappings.get(prop_name)
                                {
                                    let col_name = match prop_val {
                                        PropertyValue::Column(c) => c.clone(),
                                        PropertyValue::Expression(e) => e.clone(),
                                    };
                                    format!(", {rel_table}.{col_name} AS {prop_name}")
                                } else {
                                    format!(", NULL AS {prop_name}")
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("");

                        let branch_sql = format!(
                            "SELECT \
                                '{from_label}' AS start_type, \
                                {start_id_expr} as start_id, \
                                {end_id_expr} as end_id, \
                                '{to_label}' AS end_type, \
                                ['{}'] as path_relationships, \
                                [{}] as rel_properties, \
                                {} as start_properties, \
                                {} as end_properties{direct_rel_cols} \
                            FROM {rel_table} \
                            INNER JOIN {from_join_expr} ON {from_join_cond} \
                            INNER JOIN {to_join_expr} ON {to_join_cond}{where_clause} \
                            LIMIT 1000",
                            base_rel_type,
                            rel_properties_json,
                            start_properties_json,
                            end_properties_json,
                            from_label = combo.from_label,
                            to_label = combo.to_label,
                        );

                        log::debug!(
                            "  Branch: (:{from_label})-[:{rel_type}]->(:{to_label})",
                            from_label = combo.from_label,
                            rel_type = combo.rel_type,
                            to_label = combo.to_label
                        );

                        Ok(branch_sql)
                    })
                    .collect();

                let union_branches = union_branches?;
                let union_sql = union_branches.join("\nUNION ALL\n");

                // Create CTE name based on relationship alias
                let cte_name = format!("pattern_union_{}", graph_rel.alias);
                log::info!(
                    "✅ Generated UNION CTE '{}' with {} branches",
                    cte_name,
                    combinations.len()
                );

                relationship_ctes.push(Cte::new(
                    cte_name.clone(),
                    super::CteContent::RawSql(format!("{} AS (\n{}\n)", cte_name, union_sql)),
                    false,
                ));

                // Register CTE name in task-local QueryContext for lookup during nested rendering
                crate::server::query_context::register_relationship_cte_name(
                    &graph_rel.alias,
                    &cte_name,
                );
                log::info!(
                    "📝 Registered PatternResolver 2.0 CTE: alias='{}' → cte_name='{}'",
                    graph_rel.alias,
                    cte_name
                );

                // Extract CTEs from child plans
                let mut child_ctes = extract_ctes_with_context(
                    &graph_rel.left,
                    last_node_alias,
                    context,
                    schema,
                    plan_ctx,
                )?;
                let right_ctes = extract_ctes_with_context(
                    &graph_rel.right,
                    last_node_alias,
                    context,
                    schema,
                    plan_ctx,
                )?;
                child_ctes.extend(right_ctes);
                child_ctes.extend(relationship_ctes);

                // Deduplicate CTEs by name (same pattern can appear in multiple plan branches)
                let mut seen_cte_names = std::collections::HashSet::new();
                child_ctes.retain(|cte| seen_cte_names.insert(cte.cte_name.clone()));

                return Ok(child_ctes);
            }

            if let Some(labels) = &graph_rel.labels {
                crate::debug_print!(
                    "DEBUG cte_extraction: GraphRel labels: {:?} (len={})",
                    labels,
                    labels.len()
                );

                // Deduplicate labels to handle cases like [:FOLLOWS|FOLLOWS]
                let unique_labels: Vec<String> = {
                    let mut seen = std::collections::HashSet::new();
                    labels
                        .iter()
                        .filter(|l| seen.insert((*l).clone()))
                        .cloned()
                        .collect()
                };

                if unique_labels.len() > 1 {
                    // Multiple distinct relationship types: create a UNION CTE
                    // 🔧 FIX: Use the schema parameter directly instead of context.schema()
                    let rel_tables: Vec<String> = unique_labels
                        .iter()
                        .map(|label| {
                            if let Ok(rel_schema) = schema.get_rel_schema(label) {
                                format!("{}.{}", rel_schema.database, rel_schema.table_name)
                            } else {
                                log::error!(
                                    "❌ SCHEMA ERROR: Relationship type '{}' not found in schema",
                                    label
                                );
                                format!("ERROR_SCHEMA_MISSING_{}", label)
                            }
                        })
                        .collect();
                    crate::debug_print!(
                        "DEBUG cte_extraction: Resolved tables for labels {:?}: {:?}",
                        unique_labels,
                        rel_tables
                    );

                    // Check if this is a polymorphic edge (all types map to same table with type_column)
                    // 🔧 FIX: Use the schema parameter directly
                    let is_polymorphic =
                        if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                            rel_schema.type_column.is_some()
                        } else {
                            false
                        };

                    let union_queries: Vec<String> = if is_polymorphic {
                        // Polymorphic edge: all types share the same table, need type filters
                        // 🔧 FIX: Use the schema parameter directly
                        if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                            let table_name =
                                format!("{}.{}", rel_schema.database, rel_schema.table_name);
                            let from_col = &rel_schema.from_id;
                            let to_col = &rel_schema.to_id;
                            let type_col = rel_schema
                                .type_column
                                .as_ref()
                                .expect("polymorphic edge must have type_column");

                            // For polymorphic edges, use a single query with IN clause
                            // This is more efficient than UNION of identical table scans
                            // Include type_column for relationship property access
                            let type_values: Vec<String> =
                                unique_labels.iter().map(|l| format!("'{}'", l)).collect();
                            let type_in_clause = type_values.join(", ");

                            vec![format!(
                                "SELECT {from_col} as from_node_id, {to_col} as to_node_id, {type_col} as interaction_type FROM {table_name} WHERE {type_col} IN ({type_in_clause})"
                            )]
                        } else {
                            // Fallback if schema lookup fails
                            rel_tables
                                .iter()
                                .map(|table| {
                                    let (from_col, to_col) =
                                        get_relationship_columns_by_table(table).unwrap_or((
                                            "from_node_id".to_string(),
                                            "to_node_id".to_string(),
                                        ));
                                    format!(
                                        "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                        from_col, to_col, table
                                    )
                                })
                                .collect()
                        }
                    } else {
                        // Regular multiple relationship types: UNION of different tables
                        // 🔧 FIX: Use schema parameter to get the correct column names for each relationship type
                        unique_labels
                            .iter()
                            .zip(rel_tables.iter())
                            .map(|(label, table)| {
                                if let Ok(rel_schema) = schema.get_rel_schema(label) {
                                    let from_col = &rel_schema.from_id;
                                    let to_col = &rel_schema.to_id;
                                    format!(
                                        "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                        from_col, to_col, table
                                    )
                                } else {
                                    // Fallback if schema lookup fails
                                    format!(
                                        "SELECT from_id as from_node_id, to_id as to_node_id FROM {}",
                                        table
                                    )
                                }
                            })
                                .collect()
                    };

                    let union_sql = union_queries.join(" UNION ALL ");
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );

                    // Format as proper CTE: cte_name AS (union_sql)
                    let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);

                    crate::debug_println!(
                        "DEBUG cte_extraction: Generated UNION CTE: {}",
                        cte_name
                    );

                    relationship_ctes.push(Cte::new(
                        cte_name.clone(),
                        super::CteContent::RawSql(formatted_union_sql),
                        false,
                    ));

                    // Register CTE name in task-local QueryContext for lookup during nested rendering
                    crate::server::query_context::register_relationship_cte_name(
                        &graph_rel.alias,
                        &cte_name,
                    );
                    log::info!(
                        "📝 Registered multi-type CTE: alias='{}' → cte_name='{}'",
                        graph_rel.alias,
                        cte_name
                    );
                } else {
                    crate::debug_println!(
                        "DEBUG cte_extraction: Single relationship type, no UNION needed"
                    );
                }
            } else {
                crate::debug_println!("DEBUG cte_extraction: No labels on GraphRel!");
            }

            // IMPORTANT: Recurse into left and right branches to collect CTEs from nested GraphRels
            // This is needed for multi-hop polymorphic patterns like (u)-[r1]->(m)-[r2]->(t)
            // where both r1 and r2 are wildcard edges needing their own CTEs
            let left_ctes = extract_ctes_with_context(
                &graph_rel.left,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )?;
            let mut right_ctes = extract_ctes_with_context(
                &graph_rel.right,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )?;

            // Combine all CTEs from left, right, and current relationship
            let mut all_ctes = left_ctes;
            all_ctes.append(&mut right_ctes);
            all_ctes.append(&mut relationship_ctes);

            Ok(all_ctes)
        }
        LogicalPlan::Filter(filter) => {
            // Store the filter in context so GraphRel nodes can access it
            log::trace!(
                "Filter node detected, storing filter predicate in context: {:?}",
                filter.predicate
            );

            // 🆕 IMMUTABLE PATTERN: Create new context with filter instead of mutating
            let filter_expr: RenderExpr = filter.predicate.clone().try_into()?;
            log::trace!("Converted to RenderExpr: {:?}", filter_expr);
            let new_context = context.clone().with_filter(filter_expr);

            // Extract CTEs with the new context
            let ctes = extract_ctes_with_context(
                &filter.input,
                last_node_alias,
                &mut new_context.clone(),
                schema,
                plan_ctx,
            )?;

            Ok(ctes)
        }
        LogicalPlan::Projection(projection) => {
            log::trace!(
                "Projection node detected, recursing into input type: {}",
                match &*projection.input {
                    LogicalPlan::Empty => "Empty",
                    LogicalPlan::ViewScan(_) => "ViewScan",
                    LogicalPlan::GraphNode(_) => "GraphNode",
                    LogicalPlan::GraphRel(_) => "GraphRel",
                    LogicalPlan::Filter(_) => "Filter",
                    LogicalPlan::Projection(_) => "Projection",
                    LogicalPlan::GraphJoins(_) => "GraphJoins",
                    LogicalPlan::GroupBy(_) => "GroupBy",
                    LogicalPlan::OrderBy(_) => "OrderBy",
                    LogicalPlan::Skip(_) => "Skip",
                    LogicalPlan::Limit(_) => "Limit",
                    LogicalPlan::Cte(_) => "Cte",
                    LogicalPlan::Union(_) => "Union",
                    LogicalPlan::PageRank(_) => "PageRank",
                    LogicalPlan::Unwind(_) => "Unwind",
                    LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                    LogicalPlan::WithClause(_) => "WithClause",
                }
            );
            extract_ctes_with_context(
                &projection.input,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )
        }
        LogicalPlan::GraphJoins(graph_joins) => {
            log::debug!(
                "🔍 CTE extraction: GraphJoins input type: {:?}",
                std::mem::discriminant(graph_joins.input.as_ref())
            );
            extract_ctes_with_context(
                &graph_joins.input,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )
        }
        LogicalPlan::GroupBy(group_by) => {
            log::info!("🔍 CTE extraction: Delegating from GroupBy to input plan");
            extract_ctes_with_context(&group_by.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::OrderBy(order_by) => {
            extract_ctes_with_context(&order_by.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::Skip(skip) => {
            extract_ctes_with_context(&skip.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::Limit(limit) => {
            extract_ctes_with_context(&limit.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::Cte(logical_cte) => {
            // 🔧 FIX: Use the schema parameter directly instead of context.schema()
            // The context.schema() was sometimes None, causing an empty schema fallback
            // which led to node lookups failing in VLP queries
            Ok(vec![Cte::new(
                logical_cte.name.clone(),
                super::CteContent::Structured(Box::new(logical_cte.input.to_render_plan(schema)?)),
                false,
            )])
        }
        LogicalPlan::Union(union) => {
            let mut ctes = vec![];
            for input_plan in union.inputs.iter() {
                ctes.append(&mut extract_ctes_with_context(
                    input_plan,
                    last_node_alias,
                    context,
                    schema,
                    plan_ctx,
                )?);
            }
            Ok(ctes)
        }
        LogicalPlan::PageRank(_) => Ok(vec![]),
        LogicalPlan::Unwind(u) => {
            extract_ctes_with_context(&u.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::CartesianProduct(cp) => {
            log::debug!(
                "🔍 CTE extraction: CartesianProduct - left={:?}, right={:?}",
                std::mem::discriminant(cp.left.as_ref()),
                std::mem::discriminant(cp.right.as_ref()),
            );
            let mut ctes =
                extract_ctes_with_context(&cp.left, last_node_alias, context, schema, plan_ctx)?;
            log::debug!(
                "🔍 CTE extraction: CartesianProduct left returned {} CTEs",
                ctes.len()
            );
            ctes.append(&mut extract_ctes_with_context(
                &cp.right,
                last_node_alias,
                context,
                schema,
                plan_ctx,
            )?);
            log::debug!(
                "🔍 CTE extraction: CartesianProduct total {} CTEs",
                ctes.len()
            );
            Ok(ctes)
        }
        LogicalPlan::WithClause(wc) => {
            println!(
                "DEBUG CTE Extraction: Processing WithClause with {} exported aliases",
                wc.exported_aliases.len()
            );
            // WITH clause should generate a CTE!
            // The CTE contains the SQL from the input plan with the WITH projection

            // First, extract any CTEs from the input
            let mut ctes =
                extract_ctes_with_context(&wc.input, last_node_alias, context, schema, plan_ctx)?;

            // CRITICAL FIX: Use CTE name from analyzer's cte_references if available
            // The VariableResolver already assigned CTE names and stored them in cte_references.
            // Using those names ensures consistency with expressions that reference the CTE.
            let cte_name = wc.exported_aliases
                .first()
                .and_then(|alias| wc.cte_references.get(alias))
                .cloned()
                .unwrap_or_else(|| {
                    // Fallback: Generate unique CTE name with sequence number 1
                    // Format: with_<sorted_aliases>_cte_<seq>
                    let name = generate_cte_name(&wc.exported_aliases, 1);
                    log::debug!("🔧 CTE Extraction: Fallback - Generated CTE name '{}' (no analyzer reference)", name);
                    name
                });

            log::info!(
                "🔧 CTE Extraction: Using CTE name '{}' for WITH clause with {} exported aliases",
                cte_name,
                wc.exported_aliases.len()
            );

            // Build the CTE content by rendering the input plan as a RenderPlan
            // 🔧 FIX: Use the schema parameter directly instead of context.schema()
            // The schema parameter is always passed and is the correct schema for this query

            // CRITICAL: Expand collect(node) to groupArray(tuple(...)) BEFORE creating Projection
            // This ensures the CTE has the proper aggregation structure
            use crate::query_planner::logical_expr::LogicalExpr;

            // 🔧 SHARED EXPRESSION PROCESSING: Rewrite property access expressions
            // This maps Cypher property names (e.g., u.name) to DB column names (e.g., full_name)
            // using the schema configuration. This is the SAME processing RETURN clause does.
            let rewrite_ctx = ExpressionRewriteContext::new(&wc.input);
            let rewritten_items =
                rewrite_projection_items_with_property_mapping(&wc.items, &rewrite_ctx);
            log::info!(
                "🔧 CTE Extraction: Rewrote {} WITH items with property mapping",
                rewritten_items.len()
            );

            // First pass: Check if we have any aggregations
            let has_aggregation_in_items = rewritten_items
                .iter()
                .any(|item| matches!(&item.expression, LogicalExpr::AggregateFnCall(_)));

            let expanded_items: Vec<_> = rewritten_items.iter().flat_map(|item| {
                if let LogicalExpr::AggregateFnCall(ref agg) = item.expression {
                    if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                        if let LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                            log::info!("🔧 CTE Extraction: Expanding collect({}) to groupArray(tuple(...))", alias.0);

                            // Get properties for this alias from the input plan
                            // We need to construct a temporary PlanBuilder to access get_properties_with_table_alias
                            // For now, log and keep as-is (will be handled by plan_builder path)
                            // TODO: Refactor to access schema properties directly
                            log::warn!("⚠️  CTE Extraction path for collect() - need schema access to expand");
                            vec![crate::query_planner::logical_plan::ProjectionItem {
                                expression: item.expression.clone(),
                                col_alias: item.col_alias.clone(),
                            }]
                        } else {
                            vec![crate::query_planner::logical_plan::ProjectionItem {
                                expression: item.expression.clone(),
                                col_alias: item.col_alias.clone(),
                            }]
                        }
                    } else {
                        vec![crate::query_planner::logical_plan::ProjectionItem {
                            expression: item.expression.clone(),
                            col_alias: item.col_alias.clone(),
                        }]
                    }
                } else if let LogicalExpr::TableAlias(ref alias) = item.expression {
                    // 🔧 FIX: Expand TableAlias to individual properties with underscore aliases
                    // This matches the behavior expected by the underscore convention test
                    log::info!("🔧 CTE Extraction: Expanding TableAlias '{}' to individual properties", alias.0);

                    // Get properties for this alias from the input plan
                    match wc.input.get_properties_with_table_alias(&alias.0) {
                        Ok((properties, _actual_table_alias)) => {
                            if !properties.is_empty() {
                                log::info!("🔧 CTE Extraction: Found {} properties for alias '{}'", properties.len(), alias.0);

                                // Convert properties to ProjectionItems with underscore aliases
                                properties.into_iter().map(|(cypher_prop, db_col)| {
                                    let underscore_alias = cte_column_name(&alias.0, &cypher_prop);
                                    crate::query_planner::logical_plan::ProjectionItem {
                                        expression: LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: alias.clone(),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(db_col),
                                            }
                                        ),
                                        col_alias: Some(LogicalColumnAlias(underscore_alias)),
                                    }
                                }).collect()
                            } else {
                                log::warn!("⚠️ CTE Extraction: No properties found for alias '{}', keeping as TableAlias", alias.0);
                                vec![crate::query_planner::logical_plan::ProjectionItem {
                                    expression: item.expression.clone(),
                                    col_alias: item.col_alias.clone(),
                                }]
                            }
                        }
                        Err(e) => {
                            log::warn!("⚠️ CTE Extraction: Error getting properties for alias '{}': {:?}, keeping as TableAlias", alias.0, e);
                            vec![crate::query_planner::logical_plan::ProjectionItem {
                                expression: item.expression.clone(),
                                col_alias: item.col_alias.clone(),
                            }]
                        }
                    }
                } else if has_aggregation_in_items {
                    // If we have aggregations in the items, wrap non-aggregate TableAlias with anyLast()
                    // for all non-ID columns (ID columns will be in GROUP BY)
                    vec![crate::query_planner::logical_plan::ProjectionItem {
                        expression: item.expression.clone(),
                        col_alias: item.col_alias.clone(),
                    }]
                } else {
                    vec![crate::query_planner::logical_plan::ProjectionItem {
                        expression: item.expression.clone(),
                        col_alias: item.col_alias.clone(),
                    }]
                }
            }).collect();

            // Detect if any items contain aggregation functions
            // If so, we need to wrap in GroupBy to generate proper SQL
            let has_aggregation = expanded_items.iter().any(|item| {
                use crate::query_planner::logical_expr::LogicalExpr;
                matches!(&item.expression, LogicalExpr::AggregateFnCall(_))
            });

            log::info!("🔧 CTE Extraction: has_aggregation={}", has_aggregation);

            // If we have aggregations, we need to:
            // 1. Wrap TableAlias non-ID columns with anyLast()
            // 2. Create GroupBy node with ID columns only
            // Use the same logic as build_chained_with_match_cte_plan (lines 1745-1900)

            let final_items = if has_aggregation {
                // Wrap non-ID columns of TableAlias with anyLast()
                expanded_items.into_iter().map(|item| {
                    use crate::query_planner::logical_expr::LogicalExpr;

                    // Only wrap TableAlias, not aggregate functions
                    if let LogicalExpr::TableAlias(ref alias) = item.expression {
                        // Find the ID column for this alias
                        if let Ok(_id_col) = wc.input.find_id_column_for_alias(&alias.0) {
                            log::info!("🔧 CTE Extraction: Wrapping non-ID columns of '{}' with anyLast()", alias.0);

                            // Expand TableAlias to all properties and wrap non-ID with anyLast()
                            // For now, keep as TableAlias - it will be expanded in plan_builder
                            // where we have access to the schema and can determine which columns are IDs
                            // The anyLast wrapping happens in plan_builder.rs around line 1745-1780
                            item
                        } else {
                            log::warn!("⚠️ CTE Extraction: Could not find ID column for alias '{}', keeping as-is", alias.0);
                            item
                        }
                    } else {
                        item
                    }
                }).collect()
            } else {
                expanded_items
            };

            // Create a Projection wrapping the input with the WITH items
            // This ensures the rendered SQL has proper SELECT items
            use crate::query_planner::logical_plan::Projection;

            let projection_with_with_items = Projection {
                input: wc.input.clone(),
                items: final_items.clone(),
                distinct: wc.distinct,
                pattern_comprehensions: vec![],
            };

            // If we have aggregations, wrap in GroupBy node with proper ID column lookup
            // Use the same logic as build_chained_with_match_cte_plan
            let plan_to_render = if has_aggregation {
                // Build GROUP BY expressions using TableAlias → ID column lookup
                // Only group by non-aggregate items
                let group_by_exprs: Vec<_> = final_items.iter()
                    .filter(|item| !matches!(&item.expression, crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)))
                    .flat_map(|item| {
                        use crate::query_planner::logical_expr::LogicalExpr;
                        match &item.expression {
                            LogicalExpr::TableAlias(alias) => {
                                // For TableAlias, find and use ID column only
                                // Try to find the ID column from the input plan
                                if let Ok(id_col) = wc.input.find_id_column_for_alias(&alias.0) {
                                    log::info!("🔧 CTE Extraction: Found ID column '{}' for alias '{}' via find_id_column_for_alias", id_col, alias.0);
                                    vec![LogicalExpr::PropertyAccessExp(
                                        crate::query_planner::logical_expr::PropertyAccess {
                                            table_alias: alias.clone(),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_col),
                                        }
                                    )]
                                } else {
                                    log::warn!("⚠️ CTE Extraction: Could not find ID column for alias '{}', skipping from GROUP BY", alias.0);
                                    vec![]
                                }
                            }
                            LogicalExpr::ArraySubscript { array, .. } => {
                                // For array subscripts (e.g., labels(x)[1]), only GROUP BY the array part
                                // ClickHouse can't GROUP BY an array element, only the array itself
                                vec![(**array).clone()]
                            }
                            _ => {
                                // For other expressions, use as-is in GROUP BY
                                vec![item.expression.clone()]
                            }
                        }
                    })
                    .collect();

                if !group_by_exprs.is_empty() {
                    log::info!(
                        "🔧 CTE Extraction: Creating GroupBy with {} expressions",
                        group_by_exprs.len()
                    );
                    use crate::query_planner::logical_plan::GroupBy;
                    use std::sync::Arc;

                    LogicalPlan::GroupBy(GroupBy {
                        input: Arc::new(LogicalPlan::Projection(projection_with_with_items)),
                        expressions: group_by_exprs,
                        having_clause: None,
                        is_materialization_boundary: false,
                        exposed_alias: wc.exported_aliases.first().cloned(),
                    })
                } else {
                    log::warn!("⚠️ CTE Extraction: has_aggregation but no valid GROUP BY expressions, using Projection only");
                    LogicalPlan::Projection(projection_with_with_items)
                }
            } else {
                LogicalPlan::Projection(projection_with_with_items)
            };

            let mut cte_render_plan = plan_to_render.to_render_plan(schema)?;

            // 🔧 CRITICAL FIX: Override FROM with context-aware extraction
            // The regular to_render_plan() can't access CTE context, so it may compute wrong CTE names.
            // We re-extract FROM here with the context to get the correct registered CTE names.
            use crate::render_plan::from_builder::FromBuilder;
            log::debug!("🔍 CTE Extraction: About to extract FROM for WITH clause");
            match plan_to_render.extract_from() {
                Ok(Some(correct_from)) => {
                    log::debug!(
                        "✅ CTE Extraction: Extracted FROM, new FROM={:?}",
                        correct_from.table
                    );
                    cte_render_plan.from = crate::render_plan::FromTableItem(correct_from.table);
                }
                Ok(None) => {
                    log::warn!("⚠️  CTE Extraction: extract_from returned None");
                }
                Err(e) => {
                    log::error!("❌ CTE Extraction: extract_from failed: {:?}", e);
                }
            }

            // Add WHERE clause from WITH to the CTE render plan
            if let Some(where_clause) = &wc.where_clause {
                let render_where = where_clause.clone().try_into().map_err(|_| {
                    RenderBuildError::InvalidRenderPlan(
                        "Failed to convert where clause".to_string(),
                    )
                })?;
                if cte_render_plan.group_by.0.is_empty() {
                    // Non-aggregation, add to filters
                    if let Some(existing) = cte_render_plan.filters.0 {
                        cte_render_plan.filters.0 =
                            Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing, render_where],
                            }));
                    } else {
                        cte_render_plan.filters.0 = Some(render_where);
                    }
                } else {
                    // Aggregation, add to having
                    if let Some(existing) = cte_render_plan.having_clause {
                        cte_render_plan.having_clause =
                            Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing, render_where],
                            }));
                    } else {
                        cte_render_plan.having_clause = Some(render_where);
                    }
                }
            }

            // Create the CTE
            let with_cte = Cte::new(
                cte_name.clone(),
                CteContent::Structured(Box::new(cte_render_plan.clone())),
                false,
            );

            // 🔧 FIX: Populate task-local CTE column mappings for SQL rendering
            // Extract column mappings from the CTE SELECT items
            // Format: (from_alias, property) → cte_column_name
            // Example: ("a_follows", "name") → "a_name"
            //
            // CRITICAL: The FROM clause uses a simplified alias, not the full CTE name
            // CTE name: "with_a_follows_cte_1" → FROM alias: "a_follows"
            // We strip "with_" prefix and "_cte_X" suffix to get the FROM alias
            let from_alias = cte_name
                .strip_prefix("with_")
                .unwrap_or(&cte_name)
                .strip_suffix("_cte")
                .or_else(|| {
                    cte_name
                        .strip_prefix("with_")
                        .and_then(|s| s.strip_suffix("_cte_1"))
                })
                .or_else(|| {
                    cte_name
                        .strip_prefix("with_")
                        .and_then(|s| s.strip_suffix("_cte_2"))
                })
                .or_else(|| {
                    cte_name
                        .strip_prefix("with_")
                        .and_then(|s| s.strip_suffix("_cte_3"))
                })
                .unwrap_or(&cte_name);

            let mut cte_mappings: std::collections::HashMap<
                String,
                std::collections::HashMap<String, String>,
            > = std::collections::HashMap::new();
            let mut alias_mapping: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();

            for select_item in &cte_render_plan.select.items {
                if let Some(col_alias) = &select_item.col_alias {
                    let col_name = col_alias.0.clone();

                    // CTE columns can be:
                    // 1. Prefixed: "a_name", "a_user_id" → property is after underscore
                    // 2. Unprefixed: "follows" (aggregate result) → property is the column name itself

                    if let Some(underscore_pos) = col_name.find('_') {
                        // Case 1: Prefixed column like "a_name"
                        let property = &col_name[underscore_pos + 1..];
                        log::debug!(
                            "🔧 CTE mapping (prefixed): {}.{} → {}",
                            from_alias,
                            property,
                            col_name
                        );
                        alias_mapping.insert(property.to_string(), col_name.clone());
                    } else {
                        // Case 2: Unprefixed column like "follows" (aggregate or scalar)
                        // Map the column name to itself
                        log::debug!(
                            "🔧 CTE mapping (unprefixed): {}.{} → {}",
                            from_alias,
                            &col_name,
                            &col_name
                        );
                        alias_mapping.insert(col_name.clone(), col_name.clone());
                    }
                }
            }

            // Map the FROM alias (e.g., "a_follows") to the property mappings
            cte_mappings.insert(from_alias.to_string(), alias_mapping);

            // Log before moving cte_mappings
            let num_properties = cte_mappings.get(from_alias).map(|m| m.len()).unwrap_or(0);
            log::info!(
                "🔧 Populated CTE property mappings: CTE '{}' → FROM alias '{}' with {} properties",
                cte_name,
                from_alias,
                num_properties
            );

            // Store in task-local context for SQL rendering
            crate::server::query_context::set_cte_property_mappings(cte_mappings);

            // CRITICAL: Insert WITH clause CTE at the BEGINNING of the list
            // This ensures it's in the first CTE group and doesn't get nested
            // inside subsequent recursive CTE groups (which would make it inaccessible)
            ctes.insert(0, with_cte);

            log::info!(
                "🔧 CTE Extraction: Added WITH CTE '{}' at beginning of CTE list",
                cte_name
            );
            Ok(ctes)
        }
    }
}

/// Check if a variable-length relationship is optional (for OPTIONAL MATCH semantics)
/// Returns true if the VLP should use LEFT JOIN instead of INNER JOIN
pub fn is_variable_length_optional(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            rel.is_optional.unwrap_or(false)
        }
        LogicalPlan::GraphNode(node) => is_variable_length_optional(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_optional(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_optional(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_optional(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_optional(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_optional(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_optional(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_optional(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_optional(&cte.input),
        _ => false,
    }
}

/// Check if the plan contains a variable-length relationship and return node aliases
/// Returns (left_alias, right_alias) if found
pub fn has_variable_length_rel(plan: &LogicalPlan) -> Option<(String, String)> {
    log::debug!(
        "🔍 has_variable_length_rel: Checking plan type: {:?}",
        std::mem::discriminant(plan)
    );
    let result = match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            log::debug!(
                "  ✅ Found VLP in GraphRel: {} -> {}",
                rel.left_connection,
                rel.right_connection
            );
            Some((rel.left_connection.clone(), rel.right_connection.clone()))
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            log::debug!("  → GraphRel without VLP, checking left branch");
            has_variable_length_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!("  → GraphNode, checking input");
            has_variable_length_rel(&node.input)
        }
        LogicalPlan::Filter(filter) => {
            log::debug!("  → Filter, checking input");
            has_variable_length_rel(&filter.input)
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("  → Projection, checking input");
            has_variable_length_rel(&proj.input)
        }
        LogicalPlan::GraphJoins(joins) => {
            log::debug!("  → GraphJoins, checking input");
            has_variable_length_rel(&joins.input)
        }
        LogicalPlan::GroupBy(gb) => has_variable_length_rel(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_rel(&ob.input),
        LogicalPlan::Skip(skip) => has_variable_length_rel(&skip.input),
        LogicalPlan::Limit(limit) => has_variable_length_rel(&limit.input),
        LogicalPlan::Cte(cte) => {
            log::debug!("  → Cte, checking input");
            has_variable_length_rel(&cte.input)
        }
        _ => {
            log::debug!("  ✗ No VLP found in this branch");
            None
        }
    };
    log::debug!("  Result: {:?}", result);
    result
}

/// Get all VLP-related aliases: (start_node_alias, end_node_alias, relationship_alias)
/// Used to determine if filters should be handled by CTE vs outer query
pub fn get_variable_length_aliases(plan: &LogicalPlan) -> Option<(String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => Some((
            rel.left_connection.clone(),
            rel.right_connection.clone(),
            rel.alias.clone(),
        )),
        LogicalPlan::GraphRel(rel) => get_variable_length_aliases(&rel.left),
        LogicalPlan::GraphNode(node) => get_variable_length_aliases(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_aliases(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_aliases(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_aliases(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_aliases(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_aliases(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_aliases(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_aliases(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_aliases(&cte.input),
        _ => None,
    }
}

/// Check if a variable-length pattern uses denormalized edges
/// Returns true if EITHER node is virtual (embedded in edge table)
/// For checking if BOTH are denormalized, use get_variable_length_denorm_info
pub fn is_variable_length_denormalized(plan: &LogicalPlan) -> bool {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }

    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Check if either left or right node is denormalized
            check_node_denormalized(&rel.left) || check_node_denormalized(&rel.right)
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        LogicalPlan::GraphRel(rel) => is_variable_length_denormalized(&rel.left),
        LogicalPlan::GraphNode(node) => is_variable_length_denormalized(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_denormalized(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_denormalized(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_denormalized(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_denormalized(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_denormalized(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_denormalized(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_denormalized(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_denormalized(&cte.input),
        _ => false,
    }
}

/// Detailed denormalization info for a variable-length pattern
#[derive(Debug, Clone)]
pub struct VariableLengthDenormInfo {
    pub start_is_denormalized: bool,
    pub end_is_denormalized: bool,
    // Node table information extracted from the plan (fully qualified)
    pub start_table: Option<String>,
    pub start_id_col: Option<String>,
    pub end_table: Option<String>,
    pub end_id_col: Option<String>,
}

impl VariableLengthDenormInfo {
    pub fn is_fully_denormalized(&self) -> bool {
        self.start_is_denormalized && self.end_is_denormalized
    }

    pub fn is_mixed(&self) -> bool {
        self.start_is_denormalized != self.end_is_denormalized
    }

    pub fn is_any_denormalized(&self) -> bool {
        self.start_is_denormalized || self.end_is_denormalized
    }
}

/// Get detailed denormalization info for a variable-length pattern
///
/// ✅ PHASE 2: Uses GraphNode.is_denormalized as fallback (VLP nodes don't always have PatternSchemaContext)
/// For VLP, the denormalized flag is set during analyzer passes and is reliable.
pub fn get_variable_length_denorm_info(plan: &LogicalPlan) -> Option<VariableLengthDenormInfo> {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }

    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // 🔧 PARAMETERIZED VIEW FIX: Extract table names with parameterized view syntax
            // This ensures outer JOINs also use parameterized views (tenant_id = 'value')
            let start_table = extract_parameterized_table_name(&rel.left);
            let end_table = extract_parameterized_table_name(&rel.right);
            let start_id_col = extract_id_column(&rel.left);
            let end_id_col = extract_id_column(&rel.right);

            Some(VariableLengthDenormInfo {
                start_is_denormalized: check_node_denormalized(&rel.left),
                end_is_denormalized: check_node_denormalized(&rel.right),
                start_table,
                start_id_col,
                end_table,
                end_id_col,
            })
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            // Recurse into left branch to find nested VLP
            get_variable_length_denorm_info(&rel.left)
        }
        LogicalPlan::GraphNode(node) => get_variable_length_denorm_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_denorm_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_denorm_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_denorm_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_denorm_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_denorm_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_denorm_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_denorm_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_denorm_info(&cte.input),
        _ => None,
    }
}

/// Info about the relationship in a variable-length pattern
/// Used for SELECT rewriting to map f.Origin → t.start_id, f.Dest → t.end_id
#[derive(Debug, Clone)]
pub struct VariableLengthRelInfo {
    pub rel_alias: String, // e.g., "f"
    pub from_col: String,  // e.g., "Origin"
    pub to_col: String,    // e.g., "Dest"
}

/// Extract relationship info (alias, from_col, to_col) from a variable-length path
pub fn get_variable_length_rel_info(plan: &LogicalPlan) -> Option<VariableLengthRelInfo> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Get the from/to columns from the ViewScan in the center
            let cols = extract_relationship_columns(&rel.center)?;
            Some(VariableLengthRelInfo {
                rel_alias: rel.alias.clone(),
                from_col: cols.from_id.to_string(),
                to_col: cols.to_id.to_string(),
            })
        }
        LogicalPlan::GraphNode(node) => get_variable_length_rel_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_rel_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_rel_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_rel_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_rel_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_rel_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_rel_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_rel_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_rel_info(&cte.input),
        _ => None,
    }
}

/// Extract path variable from the plan (variable-length paths only, for CTE generation)
pub fn get_path_variable(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => rel.path_variable.clone(),
        LogicalPlan::GraphNode(node) => get_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_path_variable(&u.input),
        LogicalPlan::WithClause(wc) => get_path_variable(&wc.input),
        LogicalPlan::Union(union_plan) => {
            // Check first branch for path variable
            // All branches should have the same path variable if any
            if !union_plan.inputs.is_empty() {
                get_path_variable(&union_plan.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract path variable from fixed multi-hop patterns (no variable_length)
/// Returns (path_variable_name, hop_count) if found
pub fn get_fixed_path_variable(plan: &LogicalPlan) -> Option<(String, u32)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Only handle fixed patterns (no variable_length)
            if rel.variable_length.is_some() {
                return None;
            }

            if let Some(ref path_var) = rel.path_variable {
                // Count hops by traversing the GraphRel chain
                let hop_count = count_hops_in_graph_rel(plan);
                return Some((path_var.clone(), hop_count));
            }

            // Check nested GraphRels
            if let LogicalPlan::GraphRel(_) = rel.left.as_ref() {
                return get_fixed_path_variable(&rel.left);
            }
            None
        }
        LogicalPlan::GraphNode(node) => get_fixed_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_fixed_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_fixed_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_fixed_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_fixed_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_fixed_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_fixed_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_fixed_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_fixed_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_fixed_path_variable(&u.input),
        _ => None,
    }
}

/// Count the number of hops (relationships) in a GraphRel chain
fn count_hops_in_graph_rel(plan: &LogicalPlan) -> u32 {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Count this relationship + any nested ones
            1 + count_hops_in_graph_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => count_hops_in_graph_rel(&node.input),
        _ => 0,
    }
}

/// Complete information about a fixed path pattern
/// For `p = (a)-[r1]->(b)-[r2]->(c)`:
/// - path_var_name: "p"
/// - node_aliases: ["a", "b", "c"]
/// - rel_aliases: ["r1", "r2"]
/// - hop_count: 2
/// - node_id_columns: mapping from node alias to (rel_alias, id_column)
///   e.g., {"a" -> ("r1", "Origin"), "b" -> ("r1", "Dest"), "c" -> ("r2", "Dest")}
#[derive(Debug, Clone)]
pub struct FixedPathInfo {
    pub path_var_name: String,
    pub node_aliases: Vec<String>,
    pub rel_aliases: Vec<String>,
    pub hop_count: u32,
    /// Maps node alias to (relationship_alias, id_column) for denormalized schemas
    /// e.g., "a" -> ("r", "Origin"), "b" -> ("r", "Dest")
    pub node_id_columns: std::collections::HashMap<String, (String, String)>,
}

/// Extract complete path information from fixed multi-hop patterns
/// Returns FixedPathInfo with all node and relationship aliases
pub fn get_fixed_path_info(
    plan: &LogicalPlan,
) -> Result<Option<FixedPathInfo>, super::errors::RenderBuildError> {
    // First find the path variable and hop count
    let (path_var_name, hop_count) = match get_fixed_path_variable(plan) {
        Some(info) => info,
        None => return Ok(None),
    };

    // Then extract all aliases and node ID mappings
    let (node_aliases, rel_aliases, node_id_columns) = collect_path_aliases_with_ids(plan)?;

    Ok(Some(FixedPathInfo {
        path_var_name,
        node_aliases,
        rel_aliases,
        hop_count,
        node_id_columns,
    }))
}

/// Collect node and relationship aliases plus ID column mappings
fn collect_path_aliases_with_ids(
    plan: &LogicalPlan,
) -> Result<
    (
        Vec<String>,
        Vec<String>,
        std::collections::HashMap<String, (String, String)>,
    ),
    super::errors::RenderBuildError,
> {
    let mut node_aliases = Vec::new();
    let mut rel_aliases = Vec::new();
    let mut node_id_columns = std::collections::HashMap::new();

    collect_path_aliases_with_ids_recursive(
        plan,
        &mut node_aliases,
        &mut rel_aliases,
        &mut node_id_columns,
    )?;

    Ok((node_aliases, rel_aliases, node_id_columns))
}

/// Recursive helper to collect aliases and ID column mappings
fn collect_path_aliases_with_ids_recursive(
    plan: &LogicalPlan,
    node_aliases: &mut Vec<String>,
    rel_aliases: &mut Vec<String>,
    node_id_columns: &mut std::collections::HashMap<String, (String, String)>,
) -> Result<(), super::errors::RenderBuildError> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Process left side first (may be another GraphRel or the start node)
            collect_path_aliases_with_ids_recursive(
                &rel.left,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;

            // Get the from_id and to_id columns from the ViewScan
            if let LogicalPlan::ViewScan(view_scan) = rel.center.as_ref() {
                // ViewScan should ALWAYS have from_id and to_id for relationship scans
                // If missing, this is a query planner bug, not a user error
                let from_id = view_scan.from_id.clone().ok_or_else(|| {
                    super::errors::RenderBuildError::ViewScanMissingRelationshipColumn(
                        "from_id".to_string(),
                    )
                })?;
                let to_id = view_scan.to_id.clone().ok_or_else(|| {
                    super::errors::RenderBuildError::ViewScanMissingRelationshipColumn(
                        "to_id".to_string(),
                    )
                })?;

                // Map left node to this relationship's from_id (if not already mapped)
                if !node_id_columns.contains_key(&rel.left_connection) {
                    node_id_columns.insert(
                        rel.left_connection.clone(),
                        (rel.alias.clone(), from_id.to_string()),
                    );
                }

                // Map right node to this relationship's to_id
                node_id_columns.insert(
                    rel.right_connection.clone(),
                    (rel.alias.clone(), to_id.to_string()),
                );
            }

            // Add this relationship
            rel_aliases.push(rel.alias.clone());

            // Add the right node
            if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                if !node_aliases.contains(&right_node.alias) {
                    node_aliases.push(right_node.alias.clone());
                }
            }
        }
        LogicalPlan::GraphNode(node) => {
            // Start node - add it if not already present
            if !node_aliases.contains(&node.alias) {
                node_aliases.push(node.alias.clone());
            }
            // Recurse into input
            collect_path_aliases_with_ids_recursive(
                &node.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Filter(filter) => {
            collect_path_aliases_with_ids_recursive(
                &filter.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Projection(proj) => {
            collect_path_aliases_with_ids_recursive(
                &proj.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::GraphJoins(joins) => {
            collect_path_aliases_with_ids_recursive(
                &joins.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::GroupBy(gb) => {
            collect_path_aliases_with_ids_recursive(
                &gb.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::OrderBy(ob) => {
            collect_path_aliases_with_ids_recursive(
                &ob.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Skip(skip) => {
            collect_path_aliases_with_ids_recursive(
                &skip.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Limit(limit) => {
            collect_path_aliases_with_ids_recursive(
                &limit.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        _ => {}
    }
    Ok(())
}

// ============================================================================
// VLP (Variable-Length Path) Schema Types and Consolidated Info
// ============================================================================

/// Schema type classification for VLP query generation
///
/// Different schema types require different SQL generation strategies:
/// - Normal: Separate node and edge tables, standard JOIN patterns
/// - Polymorphic: Single edge table with type_column, nodes still separate
/// - Denormalized: Nodes embedded in edge table (no separate node tables)
/// - FkEdge: FK column on node table represents edge (no separate edge table)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VlpSchemaType {
    /// Standard schema: separate tables for nodes and edges
    /// Example: users table + follows table
    Normal,

    /// Polymorphic edge: single edge table with type_column to distinguish edge types
    /// Example: interactions table with interaction_type column
    /// Nodes still have separate tables
    Polymorphic,

    /// Denormalized: node properties embedded in edge table
    /// Example: flights table with Origin/Dest as node IDs and OriginCity/DestCity as properties
    /// No separate node tables exist
    Denormalized,

    /// FK-Edge: edge is represented by a FK column on the node table
    /// Example: fs_objects table with parent_id FK column
    /// Edge table == Node table (self-referencing)
    FkEdge,
}

/// Consolidated VLP context containing all information needed for SQL generation
///
/// This struct gathers all the scattered VLP-related info into one place,
/// making it easier to reason about and pass through the code.
#[derive(Debug, Clone)]
pub struct VlpContext {
    /// Schema type determines SQL generation strategy
    pub schema_type: VlpSchemaType,

    /// True if exact hop count (e.g., *2, *3), false if range/unbounded
    pub is_fixed_length: bool,

    /// Exact hop count if fixed-length, None otherwise
    pub exact_hops: Option<u32>,

    /// Min/max hops for range patterns
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,

    /// Start node information
    pub start_alias: String,
    pub start_table: String,
    pub start_id_col: String,
    /// Parameterized table reference for start node (e.g., `db.table`(param='value'))
    pub start_table_parameterized: Option<String>,

    /// End node information
    pub end_alias: String,
    pub end_table: String,
    pub end_id_col: String,
    /// Parameterized table reference for end node (e.g., `db.table`(param='value'))
    pub end_table_parameterized: Option<String>,

    /// Relationship information
    pub rel_alias: String,
    pub rel_table: String,
    pub rel_from_col: String,
    pub rel_to_col: String,
    /// Parameterized table reference for relationship (e.g., `db.table`(param='value'))
    pub rel_table_parameterized: Option<String>,

    /// For polymorphic edges: type column and value
    pub type_column: Option<String>,
    pub type_value: Option<String>,

    /// For denormalized edges: property mappings (logical_name -> ClickHouse column/expression)
    pub from_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,
    pub to_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,

    /// For FK-edge patterns: true if edge is represented by FK on node table
    pub is_fk_edge: bool,
}

impl VlpContext {
    /// Check if this VLP needs a recursive CTE (true for range/unbounded patterns)
    pub fn needs_cte(&self) -> bool {
        !self.is_fixed_length
    }

    /// Check if nodes have separate tables (not denormalized)
    pub fn has_separate_node_tables(&self) -> bool {
        self.schema_type != VlpSchemaType::Denormalized && self.schema_type != VlpSchemaType::FkEdge
    }

    /// Check if this is an FK-edge pattern
    pub fn is_fk_edge(&self) -> bool {
        self.schema_type == VlpSchemaType::FkEdge || self.is_fk_edge
    }
}

/// Detect VLP schema type from a GraphRel
pub fn detect_vlp_schema_type(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
) -> VlpSchemaType {
    // Check if nodes are denormalized
    let left_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.left);
    let right_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.right);

    if left_is_denorm && right_is_denorm {
        return VlpSchemaType::Denormalized;
    }

    // Check for FK-edge pattern: edge table == node table (self-referencing FK)
    // This is detected by checking if rel_table == start_table == end_table
    let rel_table = extract_table_name(&graph_rel.center);
    let start_table = extract_node_table(&graph_rel.left);
    let end_table = extract_node_table(&graph_rel.right);

    if let (Some(rt), Some(st), Some(et)) = (rel_table, start_table, end_table) {
        if rt == st && rt == et {
            return VlpSchemaType::FkEdge;
        }
    }

    // Check for polymorphic edge (has type_column)
    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        if scan.type_column.is_some() {
            return VlpSchemaType::Polymorphic;
        }
    }

    VlpSchemaType::Normal
}

/// Extract table name from a node plan
fn extract_node_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                Some(scan.source_table.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper to check if a GraphNode is denormalized
fn is_node_denormalized_from_graph_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => node.is_denormalized,
        _ => false,
    }
}

/// Build a complete VlpContext from a GraphRel
///
/// This gathers all VLP-related information into a single struct
pub fn build_vlp_context(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    schema: &GraphSchema,
) -> Option<VlpContext> {
    let spec = graph_rel.variable_length.as_ref()?;

    let schema_type = detect_vlp_schema_type(graph_rel);
    let is_fixed_length =
        spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();
    let exact_hops = spec.exact_hop_count();

    // Extract start node info
    let (start_alias, start_table, start_id_col) =
        extract_node_info(&graph_rel.left, schema_type, &graph_rel.center)?;

    // Extract end node info
    let (end_alias, end_table, end_id_col) =
        extract_node_info(&graph_rel.right, schema_type, &graph_rel.center)?;

    // Extract relationship info
    let rel_alias = graph_rel.alias.clone();

    // 🔧 FIX: For VLP patterns, graph_rel.center might be Empty instead of ViewScan
    // Fall back to looking up the relationship table from schema using the relationship type
    let rel_table = extract_table_name(&graph_rel.center).or_else(|| {
        // Get relationship type from graph_rel.labels
        let rel_type = graph_rel.labels.as_ref()?.first()?;
        log::info!(
            "🔍 VLP: center is not ViewScan, looking up relationship type '{}' in schema",
            rel_type
        );

        // Look up relationship schema by type
        let rel_schemas = schema.rel_schemas_for_type(rel_type);
        if rel_schemas.is_empty() {
            log::warn!(
                "⚠️  VLP: No relationship schema found for type '{}'",
                rel_type
            );
            return None;
        }

        if rel_schemas.len() > 1 {
            log::warn!(
                "⚠️  VLP: Multiple relationship schemas found for type '{}', using first one",
                rel_type
            );
        }

        let rel_schema = rel_schemas[0];
        let full_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        log::info!(
            "✓ VLP: Resolved relationship type '{}' to table '{}'",
            rel_type,
            full_table
        );
        Some(full_table)
    })?;

    // 🔧 FIX: For VLP patterns, also fall back to schema for relationship columns
    let rel_cols = extract_relationship_columns(&graph_rel.center).or_else(|| {
        // Get relationship type from graph_rel.labels
        let rel_type = graph_rel.labels.as_ref()?.first()?;
        log::info!(
            "🔍 VLP: center has no columns, looking up relationship type '{}' in schema",
            rel_type
        );

        // Look up relationship schema by type
        let rel_schemas = schema.rel_schemas_for_type(rel_type);
        if rel_schemas.is_empty() {
            log::warn!(
                "⚠️  VLP: No relationship schema found for type '{}'",
                rel_type
            );
            return None;
        }

        let rel_schema = rel_schemas[0];
        Some(RelationshipColumns {
            from_id: rel_schema.from_id.clone(),
            to_id: rel_schema.to_id.clone(),
        })
    })?;

    // 🔧 PARAMETERIZED VIEW FIX: Extract parameterized table names for chained join optimization
    let start_table_parameterized = extract_parameterized_table_name(&graph_rel.left);
    let end_table_parameterized = extract_parameterized_table_name(&graph_rel.right);
    let rel_table_parameterized = extract_parameterized_rel_table(&graph_rel.center);

    log::debug!(
        "build_vlp_context: start_table='{}' parameterized={:?}, end_table='{}' parameterized={:?}, rel_table='{}' parameterized={:?}",
        start_table, start_table_parameterized, end_table, end_table_parameterized, rel_table, rel_table_parameterized
    );

    // Extract polymorphic type info
    let (type_column, type_value) = if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        (
            scan.type_column.clone(),
            graph_rel.labels.as_ref().and_then(|l| l.first().cloned()),
        )
    } else {
        (None, None)
    };

    // Extract denormalized property mappings
    let (from_node_properties, to_node_properties) =
        if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
            (
                scan.from_node_properties.clone(),
                scan.to_node_properties.clone(),
            )
        } else {
            (None, None)
        };

    // Detect FK-edge pattern
    let is_fk_edge = schema_type == VlpSchemaType::FkEdge;

    Some(VlpContext {
        schema_type,
        is_fixed_length,
        exact_hops,
        min_hops: spec.min_hops,
        max_hops: spec.max_hops,
        start_alias,
        start_table,
        start_id_col,
        start_table_parameterized,
        end_alias,
        end_table,
        end_id_col,
        end_table_parameterized,
        rel_alias,
        rel_table,
        rel_from_col: rel_cols.from_id.to_string(),
        rel_to_col: rel_cols.to_id.to_string(),
        rel_table_parameterized,
        type_column,
        type_value,
        from_node_properties,
        to_node_properties,
        is_fk_edge,
    })
}

/// Extract node info (alias, table, id_col) handling different schema types
fn extract_node_info(
    node_plan: &LogicalPlan,
    schema_type: VlpSchemaType,
    rel_center: &LogicalPlan,
) -> Option<(String, String, String)> {
    match node_plan {
        LogicalPlan::GraphNode(node) => {
            let alias = node.alias.clone();

            match schema_type {
                VlpSchemaType::Denormalized => {
                    // For denormalized, table comes from relationship
                    let table = extract_table_name(rel_center)?;
                    // ID column is from relationship's from_id or to_id
                    let rel_cols = extract_relationship_columns(rel_center)?;
                    // Determine if this is start or end node by checking if it's the left or right
                    // For now, use from_id - caller should determine correct column
                    Some((alias, table, rel_cols.from_id.to_string()))
                }
                _ => {
                    // Normal/Polymorphic: get from node's ViewScan
                    if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                        let table = scan.source_table.clone();
                        let id_col = scan.id_column.clone();
                        Some((alias, table, id_col))
                    } else if let Some(label) = &node.label {
                        // Fallback: derive from label
                        let table = label_to_table_name(label);
                        let id_col = table_to_id_column(&table);
                        Some((alias, table, id_col))
                    } else {
                        None
                    }
                }
            }
        }
        LogicalPlan::GraphRel(rel) => {
            // Handle case where node_plan is a GraphRel (nested relationship pattern)
            // Extract the boundary node from the GraphRel's RIGHT side
            // For patterns like: (forum)-[:CONTAINER_OF]->(post)<-[:REPLY_OF*0..]-(message)
            //   Inner GraphRel: left=forum, right=post → we want post (right side)
            // For patterns like: (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post)
            //   Inner GraphRel: left=person, right=message → we want message (right side)
            // The shared/boundary node is always at the right end of the inner chain
            extract_node_info(&rel.right, schema_type, rel_center)
        }
        _ => None,
    }
}

/// Extract variable length spec from the plan
pub fn get_variable_length_spec(
    plan: &LogicalPlan,
) -> Option<crate::query_planner::logical_plan::VariableLengthSpec> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this GraphRel has variable_length
            if rel.variable_length.is_some() {
                return rel.variable_length.clone();
            }
            // Recursively check nested GraphRels (for chained patterns like (a)-[*]->(b)-[:R]->(c))
            get_variable_length_spec(&rel.left).or_else(|| get_variable_length_spec(&rel.right))
        }
        LogicalPlan::GraphNode(node) => get_variable_length_spec(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_spec(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_spec(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_spec(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_spec(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_spec(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_spec(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_spec(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_spec(&cte.input),
        LogicalPlan::Unwind(u) => get_variable_length_spec(&u.input),
        _ => None,
    }
}

/// Extract shortest path mode from the plan
pub fn get_shortest_path_mode(
    plan: &LogicalPlan,
) -> Option<crate::query_planner::logical_plan::ShortestPathMode> {
    match plan {
        LogicalPlan::GraphRel(rel) => rel.shortest_path_mode.clone(),
        LogicalPlan::GraphNode(node) => get_shortest_path_mode(&node.input),
        LogicalPlan::Filter(filter) => get_shortest_path_mode(&filter.input),
        LogicalPlan::Projection(proj) => get_shortest_path_mode(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_shortest_path_mode(&joins.input),
        LogicalPlan::GroupBy(gb) => get_shortest_path_mode(&gb.input),
        LogicalPlan::OrderBy(ob) => get_shortest_path_mode(&ob.input),
        LogicalPlan::Skip(skip) => get_shortest_path_mode(&skip.input),
        LogicalPlan::Limit(limit) => get_shortest_path_mode(&limit.input),
        LogicalPlan::Cte(cte) => get_shortest_path_mode(&cte.input),
        LogicalPlan::Unwind(u) => get_shortest_path_mode(&u.input),
        _ => None,
    }
}

/// Extract node label from ViewScan in the plan
/// Uses the provided schema for node label lookup
pub fn extract_node_label_from_viewscan_with_schema(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // First check if ViewScan has explicit node_label (for denormalized nodes)
            if let Some(label) = &view_scan.node_label {
                return Some(label.clone());
            }
            // Otherwise, look up node label from the provided schema using table name
            if let Some((label, _)) = get_node_schema_by_table(schema, &view_scan.source_table) {
                return Some(label.to_string());
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan_with_schema(&node.input, schema)
        }
        LogicalPlan::Filter(filter) => {
            extract_node_label_from_viewscan_with_schema(&filter.input, schema)
        }
        LogicalPlan::Projection(proj) => {
            extract_node_label_from_viewscan_with_schema(&proj.input, schema)
        }
        LogicalPlan::GraphJoins(gj) => {
            extract_node_label_from_viewscan_with_schema(&gj.input, schema)
        }
        LogicalPlan::GraphRel(gr) => {
            // For GraphRel, prefer start node (left) label but fall back to end node (right)
            if let Some(label) = extract_node_label_from_viewscan_with_schema(&gr.left, schema) {
                return Some(label);
            }
            extract_node_label_from_viewscan_with_schema(&gr.right, schema)
        }
        LogicalPlan::Union(u) => {
            // For UNION of denormalized nodes, try to get label from first input
            if let Some(first) = u.inputs.first() {
                return extract_node_label_from_viewscan_with_schema(first, schema);
            }
            None
        }
        LogicalPlan::Limit(l) => extract_node_label_from_viewscan_with_schema(&l.input, schema),
        LogicalPlan::Skip(s) => extract_node_label_from_viewscan_with_schema(&s.input, schema),
        LogicalPlan::WithClause(wc) => {
            // WITH clause wraps the original node plan — recurse to find the label
            extract_node_label_from_viewscan_with_schema(&wc.input, schema)
        }
        _ => None,
    }
}

/// Extract node label from ViewScan in the plan.
/// Uses the task-local query schema.
pub fn extract_node_label_from_viewscan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            if let Some(schema) = crate::server::query_context::get_current_schema() {
                if let Some((label, _)) = get_node_schema_by_table(&schema, &view_scan.source_table)
                {
                    return Some(label.to_string());
                }
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_label_from_viewscan(&proj.input),
        LogicalPlan::WithClause(wc) => extract_node_label_from_viewscan(&wc.input),
        _ => None,
    }
}

/// Extract the relationship type from a plan containing a GraphRel.
/// Returns the first relationship type found (for UNION branches, each branch has one type).
pub fn extract_relationship_type_from_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Labels are stored as "TYPE::FromNode::ToNode", extract just the type
            rel.labels.as_ref().and_then(|labels| {
                labels.first().map(|label| {
                    // Split by "::" and take first part (the relationship type)
                    label.split("::").next().unwrap_or(label).to_string()
                })
            })
        }
        LogicalPlan::GraphNode(node) => extract_relationship_type_from_plan(&node.input),
        LogicalPlan::Filter(filter) => extract_relationship_type_from_plan(&filter.input),
        LogicalPlan::Projection(proj) => extract_relationship_type_from_plan(&proj.input),
        LogicalPlan::GraphJoins(joins) => extract_relationship_type_from_plan(&joins.input),
        LogicalPlan::Limit(limit) => extract_relationship_type_from_plan(&limit.input),
        LogicalPlan::Skip(skip) => extract_relationship_type_from_plan(&skip.input),
        _ => None,
    }
}

/// Extract start and end node labels from a path plan containing GraphRel.
/// Gets node labels from the left (start) and right (end) nodes in the GraphRel.
pub fn extract_path_node_labels_from_plan(plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // First try: composite label format "TYPE::FromNode::ToNode"
            if let Some(labels) = &rel.labels {
                if let Some(label) = labels.first() {
                    let parts: Vec<&str> = label.split("::").collect();
                    if parts.len() >= 3 {
                        return Some((parts[1].to_string(), parts[2].to_string()));
                    }
                }
            }

            // Second try: extract from left/right node ViewScans
            let start_label = extract_node_label_from_plan(&rel.left);
            let end_label = extract_node_label_from_plan(&rel.right);

            if let (Some(sl), Some(el)) = (start_label, end_label) {
                return Some((sl, el));
            }

            None
        }
        LogicalPlan::GraphNode(node) => extract_path_node_labels_from_plan(&node.input),
        LogicalPlan::Filter(filter) => extract_path_node_labels_from_plan(&filter.input),
        LogicalPlan::Projection(proj) => extract_path_node_labels_from_plan(&proj.input),
        LogicalPlan::GraphJoins(joins) => extract_path_node_labels_from_plan(&joins.input),
        LogicalPlan::Limit(limit) => extract_path_node_labels_from_plan(&limit.input),
        LogicalPlan::Skip(skip) => extract_path_node_labels_from_plan(&skip.input),
        _ => None,
    }
}

/// Extract node label from a plan (typically a GraphNode or ViewScan)
fn extract_node_label_from_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // GraphNode.label has the node type
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise recurse into input
            extract_node_label_from_plan(&node.input)
        }
        LogicalPlan::ViewScan(scan) => {
            // Try to get from table alias via task-local schema
            if let Some(schema) = crate::server::query_context::get_current_schema() {
                if let Some((label, _)) = get_node_schema_by_table(&schema, &scan.source_table) {
                    return Some(label.to_string());
                }
            }
            None
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_plan(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_label_from_plan(&proj.input),
        _ => None,
    }
}

/// Get node schema information by table name
pub fn get_node_schema_by_table<'a>(
    schema: &'a GraphSchema,
    table_name: &str,
) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.all_node_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}

/// Expand fixed-length path patterns into inline JOINs
///
/// This function generates JOIN clauses for exact hop-count patterns (*2, *3, etc.)
/// without using CTEs. It directly chains relationship and node JOINs.
///
/// # Arguments
/// * `exact_hops` - Number of hops (e.g., 2 for *2)
/// * `start_table` - Table name for start node
/// * `start_id_col` - ID column for start node
/// * `rel_table` - Table name for relationship
/// * `from_col` - From-node ID column in relationship table
/// * `to_col` - To-node ID column in relationship table
/// * `end_table` - Table name for end node
/// * `end_id_col` - ID column for end node
/// * `start_alias` - Cypher alias for start node
/// * `end_alias` - Cypher alias for end node
///
/// # Returns
/// Vector of JOIN items to be added to the FROM clause
pub fn expand_fixed_length_joins(
    exact_hops: u32,
    _start_table: &str,
    start_id_col: &str,
    rel_table: &str,
    from_col: &str,
    to_col: &str,
    end_table: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Vec<Join> {
    use super::render_expr::{
        Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    let mut joins = Vec::new();

    println!(
        "expand_fixed_length_joins: Generating {} hops from {} to {}",
        exact_hops, start_alias, end_alias
    );

    for hop in 1..=exact_hops {
        let rel_alias = format!("r{}", hop);

        // Determine previous node/relationship alias
        let prev_alias = if hop == 1 {
            start_alias.to_string()
        } else {
            // Bridge directly through previous relationship's to_id
            format!("r{}", hop - 1)
        };

        let prev_id_col = if hop == 1 {
            start_id_col.to_string()
        } else {
            to_col.to_string() // Bridge through previous relationship's to_id
        };

        // Add relationship JOIN
        joins.push(Join {
            table_name: rel_table.to_string(),
            table_alias: rel_alias.clone(),
            joining_on: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(prev_alias),
                        column: PropertyValue::Column(prev_id_col),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(rel_alias.clone()),
                        column: PropertyValue::Column(from_col.to_string()),
                    }),
                ],
            }],
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        });

        // TODO: Add intermediate node JOIN only if properties referenced
        // For now, always bridge directly through relationship IDs (optimization!)
    }

    // Always add final node JOIN (the endpoint)
    let last_rel = format!("r{}", exact_hops);
    joins.push(Join {
        table_name: end_table.to_string(),
        table_alias: end_alias.to_string(),
        joining_on: vec![OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(last_rel),
                    column: PropertyValue::Column(to_col.to_string()),
                }),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(end_alias.to_string()),
                    column: PropertyValue::Column(end_id_col.to_string()),
                }),
            ],
        }],
        join_type: JoinType::Inner,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
        graph_rel: None,
    });

    println!(
        "expand_fixed_length_joins: Generated {} JOINs (no intermediate nodes)",
        joins.len()
    );

    joins
}

/// Schema-aware fixed-length VLP JOIN generation using VlpContext
///
/// This is the consolidated version that handles all schema types correctly:
/// - Normal: FROM start_node, JOINs through r1...rN, final JOIN to end_node
/// - Polymorphic: Same as Normal (nodes have separate tables)
/// - Denormalized: FROM r1 (first edge), JOINs through r2...rN only (no node JOINs)
///
/// # Returns
/// (from_table, from_alias, joins) - The FROM table info and JOIN clauses
pub fn expand_fixed_length_joins_with_context(ctx: &VlpContext) -> (String, String, Vec<Join>) {
    use super::render_expr::{
        Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    let exact_hops = ctx.exact_hops.unwrap_or(1);
    let mut joins = Vec::new();

    // 🔧 PARAMETERIZED VIEW FIX: Use parameterized table names if available, else fallback to plain names
    let start_table_ref = ctx
        .start_table_parameterized
        .as_ref()
        .unwrap_or(&ctx.start_table);
    let end_table_ref = ctx
        .end_table_parameterized
        .as_ref()
        .unwrap_or(&ctx.end_table);
    let rel_table_ref = ctx
        .rel_table_parameterized
        .as_ref()
        .unwrap_or(&ctx.rel_table);

    println!(
        "expand_fixed_length_joins_with_context: schema_type={:?}, {} hops from {} to {}",
        ctx.schema_type, exact_hops, ctx.start_alias, ctx.end_alias
    );
    log::debug!(
        "expand_fixed_length_joins_with_context: start_table='{}', end_table='{}', rel_table='{}'",
        start_table_ref,
        end_table_ref,
        rel_table_ref
    );

    match ctx.schema_type {
        VlpSchemaType::Denormalized => {
            // DENORMALIZED: No separate node tables
            // FROM: edge_table AS r1 (the first hop becomes FROM)
            // JOINs: r2 ON r1.to_id = r2.from_id, ..., rN ON r(N-1).to_id = rN.from_id
            // No final node JOIN needed - end node properties come from rN.to_node_properties

            // First hop is the FROM table, not a JOIN
            let from_table = rel_table_ref.clone();
            let from_alias = "r1".to_string();

            // Generate JOINs for hops 2..N
            for hop in 2..=exact_hops {
                let rel_alias = format!("r{}", hop);
                let prev_alias = format!("r{}", hop - 1);

                joins.push(Join {
                    table_name: rel_table_ref.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(ctx.rel_to_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                });
            }

            println!(
                "expand_fixed_length_joins_with_context [DENORMALIZED]: FROM {} AS {}, {} JOINs",
                from_table,
                from_alias,
                joins.len()
            );

            (from_table, from_alias, joins)
        }

        VlpSchemaType::Normal | VlpSchemaType::Polymorphic => {
            // NORMAL/POLYMORPHIC: Separate node tables exist
            // FROM: start_node_table AS start_alias
            // JOINs: r1 ON start.id = r1.from_id, r2 ON r1.to_id = r2.from_id, ..., end ON rN.to_id = end.id

            let from_table = start_table_ref.clone();
            let from_alias = ctx.start_alias.clone();

            for hop in 1..=exact_hops {
                let rel_alias = format!("r{}", hop);

                let (prev_alias, prev_id_col) = if hop == 1 {
                    (ctx.start_alias.clone(), ctx.start_id_col.clone())
                } else {
                    (format!("r{}", hop - 1), ctx.rel_to_col.clone())
                };

                // Add relationship JOIN
                joins.push(Join {
                    table_name: rel_table_ref.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(prev_id_col),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                });
            }

            // Add final node JOIN - use parameterized end table
            let last_rel = format!("r{}", exact_hops);
            joins.push(Join {
                table_name: end_table_ref.clone(),
                table_alias: ctx.end_alias.clone(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(last_rel),
                            column: PropertyValue::Column(ctx.rel_to_col.clone()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(ctx.end_alias.clone()),
                            column: PropertyValue::Column(ctx.end_id_col.clone()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            });

            println!(
                "expand_fixed_length_joins_with_context [NORMAL/POLYMORPHIC]: FROM {} AS {}, {} JOINs",
                from_table, from_alias, joins.len()
            );

            (from_table, from_alias, joins)
        }

        VlpSchemaType::FkEdge => {
            // FK-EDGE: Edge is FK column on node table, no separate edge table
            // FROM: start_node_table AS start_alias
            // JOINs: m1 ON start.fk_col = m1.id_col, m2 ON m1.fk_col = m2.id_col, ..., end ON mN-1.fk_col = end.id_col
            //
            // Example for *2 with parent_id FK:
            // FROM fs_objects AS child
            // JOIN fs_objects AS m1 ON child.parent_id = m1.object_id  -- hop 1
            // JOIN fs_objects AS parent ON m1.parent_id = parent.object_id  -- hop 2

            let from_table = start_table_ref.clone();
            let from_alias = ctx.start_alias.clone();

            for hop in 1..=exact_hops {
                let is_last_hop = hop == exact_hops;
                let current_alias = if is_last_hop {
                    ctx.end_alias.clone()
                } else {
                    format!("m{}", hop)
                };

                let prev_alias = if hop == 1 {
                    ctx.start_alias.clone()
                } else {
                    format!("m{}", hop - 1)
                };

                // FK-edge: prev_node.fk_col = current_node.id_col
                // Example: child.parent_id = m1.object_id
                // 🔧 PARAMETERIZED VIEW FIX: Use start_table_ref for FK-edge (self-referencing table)
                joins.push(Join {
                    table_name: start_table_ref.clone(), // Same table as start (self-referencing)
                    table_alias: current_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()), // FK column (parent_id)
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(current_alias),
                                column: PropertyValue::Column(ctx.rel_to_col.clone()), // ID column (object_id)
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                });
            }

            println!(
                "expand_fixed_length_joins_with_context [FK-EDGE]: FROM {} AS {}, {} JOINs",
                from_table,
                from_alias,
                joins.len()
            );

            (from_table, from_alias, joins)
        }
    }
}

/// Generate cycle prevention filters for fixed-length paths
///
/// Prevents nodes from being revisited in a path by ensuring:
/// 1. Start node != End node
/// 2. All intermediate relationship endpoints are unique
///
/// For *2: `a.user_id != c.user_id AND r1.followed_id != r2.follower_id`
/// For *3: `a.user_id != d.user_id AND r1.followed_id != r2.follower_id AND r2.followed_id != r3.follower_id`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_col` - ID column name for start node
/// * `to_col` - "to" ID column name for relationships
/// * `from_col` - "from" ID column name for relationships
/// * `end_id_col` - ID column name for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters(
    exact_hops: u32,
    start_id_col: &str,
    to_col: &str,
    from_col: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    // Delegate to composite version with single-column IDs
    generate_cycle_prevention_filters_composite(
        exact_hops,
        &[start_id_col],
        &[to_col],
        &[from_col],
        &[end_id_col],
        start_alias,
        end_alias,
    )
}

/// Generate cycle prevention filters for fixed-length paths with composite IDs
///
/// Supports both simple and composite primary keys. For composite keys, generates
/// NOT (col1=col1 AND col2=col2 AND ...) conditions.
///
/// # Examples
///
/// Simple ID: `a.user_id != c.user_id`
///
/// Composite ID: `NOT (a.flight_date = c.flight_date AND a.flight_num = c.flight_num)`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_cols` - ID column names for start node
/// * `to_cols` - "to" ID column names for relationships
/// * `from_cols` - "from" ID column names for relationships
/// * `end_id_cols` - ID column names for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters_composite(
    exact_hops: u32,
    start_id_cols: &[&str],
    _to_cols: &[&str],
    _from_cols: &[&str],
    end_id_cols: &[&str],
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    use super::render_expr::{
        Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    if exact_hops == 0 {
        return None;
    }

    let mut filters = Vec::new();

    // Helper to generate composite equality check: NOT (col1=col1 AND col2=col2 AND ...)
    let generate_composite_not_equal = |left_alias: &str,
                                        left_cols: &[&str],
                                        right_alias: &str,
                                        right_cols: &[&str]|
     -> RenderExpr {
        if left_cols.len() == 1 {
            // Simple ID: a.user_id != c.user_id
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::NotEqual,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(left_alias.to_string()),
                        column: PropertyValue::Column(left_cols[0].to_string()),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(right_alias.to_string()),
                        column: PropertyValue::Column(right_cols[0].to_string()),
                    }),
                ],
            })
        } else {
            // Composite ID: NOT (a.col1 = c.col1 AND a.col2 = c.col2 AND ...)
            let equality_checks: Vec<RenderExpr> = left_cols
                .iter()
                .zip(right_cols.iter())
                .map(|(left_col, right_col)| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: PropertyValue::Column(left_col.to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: PropertyValue::Column(right_col.to_string()),
                            }),
                        ],
                    })
                })
                .collect();

            // Combine equality checks with AND
            let combined_equality = if equality_checks.len() == 1 {
                equality_checks.into_iter().next().unwrap()
            } else {
                equality_checks
                    .into_iter()
                    .reduce(|acc, expr| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![acc, expr],
                        })
                    })
                    .unwrap()
            };

            // Wrap in NOT
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![combined_equality],
            })
        }
    };

    // 1. Start node != End node (prevents returning to the starting point)
    filters.push(generate_composite_not_equal(
        start_alias,
        start_id_cols,
        end_alias,
        end_id_cols,
    ));

    // NOTE: We previously had cycle prevention for intermediate nodes, but it was WRONG.
    // The condition `r1.to_id != r2.from_id` blocks VALID paths because that's exactly
    // how paths connect (r1.to_id = r2.from_id is the JOIN condition).
    //
    // For proper cycle prevention (no node visited twice), we would need to track all
    // intermediate nodes and ensure they're all different from each other. This is
    // complex for inline JOINs (easy in recursive CTEs with path arrays).
    //
    // For now, we only prevent returning to the start node, which is the most common
    // cycle prevention requirement. Full cycle detection can be added later if needed.

    // Combine all filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        // Combine with AND
        Some(
            filters
                .into_iter()
                .reduce(|acc, filter| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![acc, filter],
                    })
                })
                .unwrap(),
        )
    }
}
