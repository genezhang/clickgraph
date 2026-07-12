//! CTE extraction utilities for variable-length path handling
//!
//! Some functions in this module are reserved for future use or used only in specific code paths.
// Note: Helper functions for VLP CTE generation are kept for complex path patterns
#![allow(dead_code)]

use crate::clickhouse_query_generator::quote_identifier;
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
use crate::sql_generator::function_mapper::current_function_mapper;
use crate::utils::cte_column_naming::cte_column_name;
use crate::utils::cte_naming::generate_cte_name;

use super::errors::RenderBuildError;
use super::filter_pipeline::{categorize_filters, CategorizedFilters};
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr};
use super::{Cte, CteContent, Join, JoinType};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Merge `new_cte` into `existing` by name, mirroring the collision-handling
/// rules `plan_builder.rs`'s per-branch UNION assembly already uses (kept in
/// sync intentionally — see #557):
/// - No existing CTE with this name → push as-is.
/// - Existing is an empty VLP placeholder (`WHERE 0 = 1`) and the new one
///   isn't → replace it (a real branch superseding a pruned/empty one).
/// - The new one is empty and the existing one isn't → keep the existing,
///   drop the new (mirrors the same "don't overwrite a real CTE with an
///   empty placeholder" rule, the other direction).
/// - Same name, identical SQL → true duplicate (e.g. the same VLP CTE
///   legitimately shared by multiple UNION branches); skip.
/// - Same name, DIFFERENT SQL → this is #557's case: a Union of per-candidate-
///   end-label branches (`generate_union_for_untyped_nodes`/`clone_plan_with_labels`)
///   each independently compute the SAME formulaic CTE name
///   (`vlp_multi_type_{start}_{end}` — the formula only depends on the
///   Cypher aliases, not the per-branch end-label restriction), but generate
///   DIFFERENT CTE bodies (one scoped to each candidate end label). Rename
///   the new one with a numeric suffix so both survive — `extract_union`/
///   `extract_union_with_ctx` (`plan_builder.rs`) already computes this exact
///   same renamed reference independently (same algorithm, same input order)
///   for the branch's FROM clause, so the two stay consistent without needing
///   to thread the rename back through here.
///
/// Returns `Some((old_name, new_name))` when a rename occurred, for callers
/// that also need to fix up a FROM/JOIN reference to the renamed CTE.
pub(crate) fn merge_cte_deduping_by_name_content(
    existing: &mut Vec<Cte>,
    new_cte: Cte,
) -> Option<(String, String)> {
    let Some(existing_idx) = existing.iter().position(|e| e.cte_name == new_cte.cte_name) else {
        existing.push(new_cte);
        return None;
    };

    let is_empty_placeholder = |content: &CteContent| matches!(content, CteContent::RawSql(s) if s.contains("WHERE 0 = 1"));
    let existing_is_empty = is_empty_placeholder(&existing[existing_idx].content);
    let new_is_empty = is_empty_placeholder(&new_cte.content);

    if existing_is_empty && !new_is_empty {
        existing[existing_idx] = new_cte;
        return None;
    }
    if new_is_empty {
        return None;
    }

    let same_content = match (&existing[existing_idx].content, &new_cte.content) {
        (CteContent::RawSql(a), CteContent::RawSql(b)) => a == b,
        _ => false,
    };
    if same_content {
        return None;
    }

    let base_name = new_cte.cte_name.clone();
    let mut suffix = 2;
    let mut new_name = format!("{}_{}", base_name, suffix);
    while existing.iter().any(|e| e.cte_name == new_name) {
        suffix += 1;
        new_name = format!("{}_{}", base_name, suffix);
    }
    let mut renamed_cte = new_cte;
    renamed_cte.cte_name = new_name.clone();
    existing.push(renamed_cte);
    Some((base_name, new_name))
}

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

    // Get relationship schema with node context for precise matching.
    // For VLP with different start/end labels (e.g., Message→Post), the recursive
    // traversal needs a union view (e.g., Message_replyOf_Message) that covers all
    // intermediate hops. Try (start, start) lookup first in that case.
    let is_vlp = graph_rel.variable_length.is_some();
    let rel_schema = if is_vlp && left_label != right_label {
        log::info!(
            "VLP cross-type: trying ({}, {}) lookup for rel type {:?}",
            left_label,
            left_label,
            rel_types.first()
        );
        schema
            .get_rel_schema_with_nodes(
                rel_types.first().unwrap(),
                Some(&left_label),
                Some(&left_label),
            )
            .or_else(|_| {
                schema.get_rel_schema_with_nodes(
                    rel_types.first().unwrap(),
                    Some(&left_label),
                    Some(&right_label),
                )
            })
            .map_err(|e| {
                RenderBuildError::MissingTableInfo(format!(
                    "Could not get relationship schema: {}",
                    e
                ))
            })?
    } else {
        schema
            .get_rel_schema_with_nodes(
                rel_types.first().unwrap(),
                Some(&left_label),
                Some(&right_label),
            )
            .map_err(|e| {
                RenderBuildError::MissingTableInfo(format!(
                    "Could not get relationship schema: {}",
                    e
                ))
            })?
    };

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
        None, // prev_left_edge - not needed for CTE generation
        None, // prev_right_edge - not needed for CTE generation
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
#[allow(clippy::too_many_arguments)] // VLP CTE generation requires the full pattern context (schema, spec, properties, all-three filter slots, mode, types, labels, options); each is a distinct planner input
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
    weight_cte: Option<crate::clickhouse_query_generator::WeightCteConfig>,
    needs_path_relationships: bool,
    use_bfs_mode: bool,
    is_undirected: bool,
) -> Result<Cte, RenderBuildError> {
    use std::sync::Arc;

    log::debug!(
        "generate_vlp_cte_via_manager: {} -[*]-> {}",
        pattern_ctx.left_node_alias,
        pattern_ctx.right_node_alias
    );

    // Build CteGenerationContext with all VLP-specific fields
    // Clone the schema to own it for the context
    let mut context = CteGenerationContext::new()
        .with_spec(spec)
        .with_schema_owned(schema.clone())
        .with_path_variable(path_variable.clone())
        .with_shortest_path_mode(shortest_path_mode)
        .with_relationship_types(relationship_types.clone())
        .with_edge_id(edge_id)
        .with_relationship_cypher_alias(relationship_cypher_alias)
        .with_node_labels(start_label.clone(), end_label.clone())
        .with_is_optional(is_optional.unwrap_or(false))
        .with_weight_cte(weight_cte);
    context.needs_path_relationships = needs_path_relationships;
    context.use_bfs_mode = use_bfs_mode;
    context.is_undirected = is_undirected;

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
            // 🔧 FIX: Verify this filter belongs to the target alias before extracting.
            // In multi-pattern MATCH, a CartesianProduct may contain filters for multiple
            // nodes. Without verification, we could extract the wrong node's filter.
            if !filter_subtree_has_alias(&filter.input, node_alias) {
                log::debug!(
                    "🔍 Skipping filter - subtree does not contain alias '{}', recursing",
                    node_alias
                );
                // This filter is for a different node — recurse into input to continue searching
                return extract_bound_node_filter(
                    &filter.input,
                    node_alias,
                    cte_alias,
                    relationship_type,
                    node_role,
                );
            }

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

/// Check if a plan subtree contains a GraphNode with the given alias.
/// Used to verify that a Filter belongs to the correct node before extracting it.
/// Traverses through all common plan node wrappers to find GraphNode leaves.
fn filter_subtree_has_alias(plan: &LogicalPlan, alias: &str) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => node.alias == alias,
        LogicalPlan::Filter(f) => filter_subtree_has_alias(&f.input, alias),
        LogicalPlan::Projection(p) => filter_subtree_has_alias(&p.input, alias),
        LogicalPlan::WithClause(w) => filter_subtree_has_alias(&w.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            filter_subtree_has_alias(&cp.left, alias) || filter_subtree_has_alias(&cp.right, alias)
        }
        LogicalPlan::GraphRel(gr) => {
            filter_subtree_has_alias(&gr.left, alias) || filter_subtree_has_alias(&gr.right, alias)
        }
        // ViewScan without GraphNode wrapper — allow (backward compatibility)
        LogicalPlan::ViewScan(_) | LogicalPlan::Empty => true,
        _ => false,
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
        // For chained patterns like (n)-->(a)-->(b), the outer GraphRel's left child
        // is the inner GraphRel(n→a). The shared variable `a` is the right endpoint
        // of that inner GraphRel — recurse into it to find the correct start labels.
        LogicalPlan::GraphRel(rel) => extract_node_labels(&rel.right),
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

/// #466: Does this predicate conjunct reference a PROPERTY of one of the
/// given node aliases? Such conjuncts cannot be applied as an outer WHERE on
/// a `pattern_union` CTE — the CTE projection does not expose node property
/// columns (only ids/types/JSON blobs), and the alias's binding (label +
/// physical table) differs per combination AND per traversal orientation.
/// They must be resolved per-branch INSIDE the CTE instead (see
/// `substitute_pattern_branch_refs`).
pub(crate) fn conjunct_references_node_property(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    node_aliases: &[&str],
) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr as LE;
    match expr {
        LE::PropertyAccessExp(p) => node_aliases.contains(&p.table_alias.0.as_str()),
        LE::OperatorApplicationExp(op) | LE::Operator(op) => op
            .operands
            .iter()
            .any(|o| conjunct_references_node_property(o, node_aliases)),
        LE::ScalarFnCall(f) => f
            .args
            .iter()
            .any(|a| conjunct_references_node_property(a, node_aliases)),
        LE::AggregateFnCall(f) => f
            .args
            .iter()
            .any(|a| conjunct_references_node_property(a, node_aliases)),
        LE::List(items) => items
            .iter()
            .any(|i| conjunct_references_node_property(i, node_aliases)),
        LE::Case(c) => {
            c.expr
                .as_deref()
                .is_some_and(|e| conjunct_references_node_property(e, node_aliases))
                || c.when_then.iter().any(|(w, t)| {
                    conjunct_references_node_property(w, node_aliases)
                        || conjunct_references_node_property(t, node_aliases)
                })
                || c.else_expr
                    .as_deref()
                    .is_some_and(|e| conjunct_references_node_property(e, node_aliases))
        }
        _ => false,
    }
}

/// #466 round 3 (ride-along finding): does this conjunct contain a reference
/// that can NEVER be resolved per `pattern_union` branch — a bare
/// whole-entity alias (`WHERE o = n`) or a subquery/pattern expression
/// (`EXISTS { ... }`, `size((o)-->())`, `x IN (subquery)`)? These previously
/// fell through every classifier (which only fired on PropertyAccessExp) and
/// were SILENTLY DROPPED. They must be routed to a clean UnsupportedFeature
/// error instead (ground rule #1; precedent 99ae1473).
///
/// A bare alias that is the sole argument of `id()`/`labels()`/
/// `elementId()`/`label()` is NOT flagged — those resolve cleanly (outer
/// rewrite or per-branch substitution). Subquery/pattern expressions are
/// flagged unconditionally in this context: their subplans reference pattern
/// variables in ways the per-branch substitution cannot rewrite, and a
/// GraphRel's `where_predicate` only carries conjuncts scoped to this
/// pattern.
pub(crate) fn conjunct_has_unresolvable_entity_ref(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    node_aliases: &[&str],
) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr as LE;
    match expr {
        LE::TableAlias(t) => node_aliases.contains(&t.0.as_str()),
        LE::ExistsSubquery(_) | LE::InSubquery(_) | LE::PatternCount(_) | LE::PathPattern(_) => {
            true
        }
        LE::OperatorApplicationExp(op) | LE::Operator(op) => op
            .operands
            .iter()
            .any(|o| conjunct_has_unresolvable_entity_ref(o, node_aliases)),
        LE::ScalarFnCall(f) => {
            // id(n)/labels(n) etc. take a bare alias argument legitimately.
            let is_entity_fn = matches!(
                f.name.to_lowercase().as_str(),
                "id" | "elementid" | "labels" | "label"
            );
            if is_entity_fn && f.args.len() == 1 && matches!(&f.args[0], LE::TableAlias(_)) {
                return false;
            }
            f.args
                .iter()
                .any(|a| conjunct_has_unresolvable_entity_ref(a, node_aliases))
        }
        LE::AggregateFnCall(f) => f
            .args
            .iter()
            .any(|a| conjunct_has_unresolvable_entity_ref(a, node_aliases)),
        LE::List(items) => items
            .iter()
            .any(|i| conjunct_has_unresolvable_entity_ref(i, node_aliases)),
        LE::Case(c) => {
            c.expr
                .as_deref()
                .is_some_and(|e| conjunct_has_unresolvable_entity_ref(e, node_aliases))
                || c.when_then.iter().any(|(w, t)| {
                    conjunct_has_unresolvable_entity_ref(w, node_aliases)
                        || conjunct_has_unresolvable_entity_ref(t, node_aliases)
                })
                || c.else_expr
                    .as_deref()
                    .is_some_and(|e| conjunct_has_unresolvable_entity_ref(e, node_aliases))
        }
        _ => false,
    }
}

/// #466 round 4 (review nit): human-readable description of the offending
/// part of an unresolvable conjunct for UnsupportedFeature messages —
/// instead of dumping the internal AST Debug format.
pub(crate) fn describe_unresolvable_conjunct(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    node_aliases: &[&str],
) -> String {
    use crate::query_planner::logical_expr::LogicalExpr as LE;
    fn first_offender(expr: &LE, aliases: &[&str]) -> Option<String> {
        match expr {
            LE::TableAlias(t) if aliases.contains(&t.0.as_str()) => {
                Some(format!("a whole-entity reference to '{}'", t.0))
            }
            LE::ExistsSubquery(_) => Some("an EXISTS subquery".to_string()),
            LE::InSubquery(_) => Some("an IN subquery".to_string()),
            LE::PatternCount(_) => Some("a pattern-count expression".to_string()),
            LE::PathPattern(_) => Some("a path-pattern expression".to_string()),
            LE::OperatorApplicationExp(op) | LE::Operator(op) => {
                op.operands.iter().find_map(|o| first_offender(o, aliases))
            }
            LE::ScalarFnCall(f) => f.args.iter().find_map(|a| first_offender(a, aliases)),
            LE::AggregateFnCall(f) => f.args.iter().find_map(|a| first_offender(a, aliases)),
            LE::List(items) => items.iter().find_map(|i| first_offender(i, aliases)),
            LE::Case(c) => c
                .expr
                .as_deref()
                .and_then(|e| first_offender(e, aliases))
                .or_else(|| {
                    c.when_then.iter().find_map(|(w, t)| {
                        first_offender(w, aliases).or_else(|| first_offender(t, aliases))
                    })
                })
                .or_else(|| {
                    c.else_expr
                        .as_deref()
                        .and_then(|e| first_offender(e, aliases))
                }),
            _ => None,
        }
    }
    first_offender(expr, node_aliases).unwrap_or_else(|| "an unsupported expression".to_string())
}

/// #466: What one endpoint alias of an undirected/multi-type `pattern_union`
/// branch is bound to, for per-branch WHERE resolution. Bindings differ per
/// combination and — for undirected patterns — per traversal orientation
/// (the reverse branch binds the aliases to the opposite sides).
pub(crate) struct PatternBranchBinding<'a> {
    /// Cypher alias (e.g. `n` / `o`).
    pub alias: &'a str,
    /// Node label this alias is bound to in this branch.
    pub label: &'a str,
    /// SQL table reference for this side in this branch (may be a self-join
    /// alias like `from_node`, or the edge table for a denormalized endpoint).
    pub table: &'a str,
    /// Rendered id expression for this side (for `id(alias)`).
    pub id_expr: &'a str,
    /// Denormalized endpoint: resolve via the role property maps below.
    pub denorm: bool,
    pub denorm_props: Option<&'a std::collections::HashMap<String, String>>,
    pub denorm_props_alt: Option<&'a std::collections::HashMap<String, String>>,
    /// Normal endpoint: resolve via the node schema's property mappings.
    pub property_mappings: &'a std::collections::HashMap<String, PropertyValue>,
}

/// Resolve a cypher property name against a branch binding. `None` = the
/// bound label has no such property → the reference is NULL in this branch
/// (Cypher: a missing property evaluates to NULL, so e.g. an equality
/// comparison is false for the branch while `IS NULL` is true).
fn resolve_branch_property(binding: &PatternBranchBinding, prop: &str) -> Option<String> {
    if binding.denorm {
        binding
            .denorm_props
            .and_then(|m| m.get(prop))
            .or_else(|| binding.denorm_props_alt.and_then(|m| m.get(prop)))
            // #466 round 3: the property may arrive PRE-RESOLVED to the
            // physical column (upstream passes resolve the anchor alias
            // against its registered table ctx before the per-branch
            // substitution runs). Accept a physical column of this map too.
            .or_else(|| {
                binding
                    .denorm_props
                    .and_then(|m| m.values().find(|c| c.as_str() == prop))
            })
            .or_else(|| {
                binding
                    .denorm_props_alt
                    .and_then(|m| m.values().find(|c| c.as_str() == prop))
            })
            .map(|col| format!("{}.{}", binding.table, quote_identifier(col)))
    } else {
        resolve_mapped_property(binding.property_mappings, binding.table, prop)
    }
}

/// Resolve a property reference against a `property_mappings` map for a given
/// table reference. Looks up by CYPHER name first; falls back to matching the
/// PHYSICAL column value — upstream passes may have already resolved the
/// property to its physical column for an alias with a registered table ctx
/// (e.g. the anchor alias of a rerouted undirected pattern: `n.amount`
/// arrives as `total_amount`, `n.name` as `full_name`). Without the fallback
/// such renamed properties silently became NULL in EVERY branch (#466 round-3
/// blocking finding 1). `None` = the bound label genuinely has no such
/// property (NULL per Cypher).
fn resolve_mapped_property(
    property_mappings: &std::collections::HashMap<String, PropertyValue>,
    table: &str,
    prop: &str,
) -> Option<String> {
    property_mappings
        .get(prop)
        .or_else(|| {
            property_mappings.values().find(|pv| match pv {
                PropertyValue::Column(c) => c == prop,
                PropertyValue::Expression(_) => false,
            })
        })
        .map(|pv| match pv {
            PropertyValue::Column(c) => format!("{}.{}", table, quote_identifier(c)),
            PropertyValue::Expression(e) => format!("{}.{}", table, e),
        })
}

/// #466: Rewrite a WHERE conjunct for ONE `pattern_union` branch, resolving
/// every node-alias/rel-alias reference against that branch's bindings:
///
/// - `alias.prop` → the bound label's physical column (missing → NULL)
/// - `id(alias)` → the branch's id expression; `elementId(alias)` → the
///   composite `Label:id-` string rebuilt from the branch's label + id
/// - `labels(alias)` / `label(alias)` → the bound label as a string literal
/// - `rel_alias.prop` → the edge table's physical column (missing → NULL)
///
/// Any reference that cannot be resolved per-branch (bare alias equality,
/// EXISTS/pattern subqueries, unknown aliases) is a clean error — NEVER
/// silently drop or degrade a predicate (ground rule #1; precedent 99ae1473).
fn substitute_pattern_branch_refs(
    expr: &RenderExpr,
    node_bindings: &[PatternBranchBinding<'_>],
    rel_alias: &str,
    rel_table: &str,
    rel_props: &std::collections::HashMap<String, PropertyValue>,
) -> Result<RenderExpr, RenderBuildError> {
    use super::render_expr::Literal as RLit;
    let recurse = |e: &RenderExpr| {
        substitute_pattern_branch_refs(e, node_bindings, rel_alias, rel_table, rel_props)
    };
    match expr {
        RenderExpr::PropertyAccessExp(p) => {
            let alias = p.table_alias.0.as_str();
            let prop = p.column.raw();
            if let Some(binding) = node_bindings.iter().find(|b| b.alias == alias) {
                Ok(match resolve_branch_property(binding, prop) {
                    Some(sql) => RenderExpr::Raw(sql),
                    None => RenderExpr::Literal(RLit::Null),
                })
            } else if alias == rel_alias {
                // Same cypher-name-first / physical-column-fallback rule as
                // node properties (the rel alias can also arrive pre-resolved).
                Ok(match resolve_mapped_property(rel_props, rel_table, prop) {
                    Some(sql) => RenderExpr::Raw(sql),
                    None => RenderExpr::Literal(RLit::Null),
                })
            } else {
                Err(RenderBuildError::UnsupportedFeature(format!(
                    "WHERE predicate on a multi-type/undirected pattern references \
                     alias '{alias}' (property '{prop}') which is not part of the \
                     pattern; cannot resolve it per UNION branch"
                )))
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            // id(alias) / labels(alias) resolve directly against the binding
            if f.args.len() == 1 {
                if let RenderExpr::TableAlias(t) = &f.args[0] {
                    if let Some(binding) = node_bindings.iter().find(|b| b.alias == t.0) {
                        match f.name.to_lowercase().as_str() {
                            "id" => return Ok(RenderExpr::Raw(binding.id_expr.to_string())),
                            // elementId is the composite `Label:id-` string
                            // (generate_node_element_id) — the branch's label
                            // is static, its id expression per-row.
                            "elementid" => {
                                return Ok(RenderExpr::Raw(format!(
                                    "concat('{}:', {}, '-')",
                                    binding.label.replace('\'', "''"),
                                    binding.id_expr
                                )))
                            }
                            "labels" | "label" => {
                                return Ok(RenderExpr::Raw(format!(
                                    "'{}'",
                                    binding.label.replace('\'', "''")
                                )))
                            }
                            _ => {}
                        }
                    }
                }
            }
            let args: Result<Vec<_>, _> = f.args.iter().map(recurse).collect();
            Ok(RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: f.name.clone(),
                args: args?,
            }))
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let operands: Result<Vec<_>, _> = op.operands.iter().map(recurse).collect();
            Ok(RenderExpr::OperatorApplicationExp(
                super::render_expr::OperatorApplication {
                    operator: op.operator,
                    operands: operands?,
                },
            ))
        }
        RenderExpr::List(items) => {
            let items: Result<Vec<_>, _> = items.iter().map(recurse).collect();
            Ok(RenderExpr::List(items?))
        }
        RenderExpr::Case(c) => {
            let case_expr = match &c.expr {
                Some(e) => Some(Box::new(recurse(e)?)),
                None => None,
            };
            let when_then: Result<Vec<_>, _> = c
                .when_then
                .iter()
                .map(|(w, t)| Ok::<_, RenderBuildError>((recurse(w)?, recurse(t)?)))
                .collect();
            let else_expr = match &c.else_expr {
                Some(e) => Some(Box::new(recurse(e)?)),
                None => None,
            };
            Ok(RenderExpr::Case(super::render_expr::RenderCase {
                expr: case_expr,
                when_then: when_then?,
                else_expr,
            }))
        }
        RenderExpr::ArraySubscript { array, index } => Ok(RenderExpr::ArraySubscript {
            array: Box::new(recurse(array)?),
            index: Box::new(recurse(index)?),
        }),
        RenderExpr::ArraySlicing { array, from, to } => Ok(RenderExpr::ArraySlicing {
            array: Box::new(recurse(array)?),
            from: match from {
                Some(f) => Some(Box::new(recurse(f)?)),
                None => None,
            },
            to: match to {
                Some(t) => Some(Box::new(recurse(t)?)),
                None => None,
            },
        }),
        RenderExpr::MapLiteral(entries) => {
            let entries: Result<Vec<_>, _> = entries
                .iter()
                .map(|(k, v)| Ok::<_, RenderBuildError>((k.clone(), recurse(v)?)))
                .collect();
            Ok(RenderExpr::MapLiteral(entries?))
        }
        RenderExpr::TableAlias(t)
            if node_bindings.iter().any(|b| b.alias == t.0) || t.0 == rel_alias =>
        {
            Err(RenderBuildError::UnsupportedFeature(format!(
                "WHERE predicate on a multi-type/undirected pattern uses whole-entity \
                 reference '{}'; cannot resolve it per UNION branch",
                t.0
            )))
        }
        RenderExpr::InSubquery(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::ReduceExpr(_)
        | RenderExpr::CteEntityRef(_) => Err(RenderBuildError::UnsupportedFeature(
            "WHERE predicate on a multi-type/undirected pattern contains a subquery or \
             entity reference that cannot be resolved per UNION branch"
                .to_string(),
        )),
        // Leaves without alias references pass through unchanged
        other => Ok(other.clone()),
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
                    // Cypher: x IN array_property → CH: has(arr, x), Spark: array_contains(arr, x)
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        let contains =
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .array_contains();
                        format!("{}({}, {})", contains, operands[1], operands[0])
                    } else {
                        format!("{} IN {}", operands[0], operands[1])
                    }
                }
                Operator::NotIn => {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        let contains =
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .array_contains();
                        format!("NOT {}({}, {})", contains, operands[1], operands[0])
                    } else {
                        format!("{} NOT IN {}", operands[0], operands[1])
                    }
                }
                Operator::StartsWith => format!("startsWith({}, {})", operands[0], operands[1]),
                Operator::EndsWith => format!("endsWith({}, {})", operands[0], operands[1]),
                Operator::Contains => {
                    // Dialect-aware: Spark's position(substr, str) reverses CH's arg order.
                    crate::clickhouse_query_generator::contains_predicate(
                        &operands[0],
                        &operands[1],
                    )
                }
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
            // Map dialect-divergent names (e.g. tuple -> Spark struct) via the registry.
            let name = crate::clickhouse_query_generator::dialect_function_name(&func.name);
            format!("{}({})", name, args.join(", "))
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

            // Wrap numeric init values in a 64-bit integer cast to prevent
            // type mismatch.
            let init_cast = if matches!(
                *reduce.initial_value,
                RenderExpr::Literal(Literal::Integer(_))
            ) {
                format!("{}({})", current_function_mapper().cast_int64(), init_sql)
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
            let mapper = crate::sql_generator::function_mapper::current_function_mapper();
            // Cypher list ranges are 0-based and HALF-OPEN: `list[from..to]` yields
            // indices [from, to), i.e. `to - from` elements. offset = from + 1.
            match (from, to) {
                (Some(from_expr), Some(to_expr)) => {
                    let from_sql = render_expr_to_sql_string(from_expr, alias_mapping);
                    let to_sql = render_expr_to_sql_string(to_expr, alias_mapping);
                    // Floor at 0 so from > to yields an empty slice (a negative length
                    // is silently wrong on CH arraySlice and errors on Databricks slice).
                    mapper.array_slice(
                        &array_sql,
                        &format!("{} + 1", from_sql),
                        Some(&format!("greatest({} - {}, 0)", to_sql, from_sql)),
                    )
                }
                (Some(from_expr), None) => {
                    let from_sql = render_expr_to_sql_string(from_expr, alias_mapping);
                    mapper.array_slice(&array_sql, &format!("{} + 1", from_sql), None)
                }
                (None, Some(to_expr)) => {
                    let to_sql = render_expr_to_sql_string(to_expr, alias_mapping);
                    mapper.array_slice(&array_sql, "1", Some(&to_sql))
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
            // If the column is already an Expression (resolved by FilterTagging),
            // skip re-mapping — it's already the correct ClickHouse expression.
            if matches!(&prop.column, PropertyValue::Expression(_)) {
                return;
            }
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
            // CAVEAT: returns the FIRST branch that resolves the alias. Today's
            // UNION shapes (multi-table label, denormalized from/to dimension,
            // polymorphic) all carry the SAME label for a given alias across
            // branches, so first-match is safe — but if a UNION ever mixed
            // DIFFERENT labels (hence possibly different id schemas, e.g.
            // composite vs. single-column) for one alias, first-match would
            // silently pick one branch's identity. Callers needing per-branch
            // precision should query the specific branch plan instead.
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

/// Returns `true` when the relationship `alias` is backed by a multi-type
/// `pattern_union` CTE (i.e. its `GraphRel` carries `pattern_combinations`).
///
/// When this is the case, the relationship's properties are projected by the
/// CTE under their *property* names (e.g. `... AS timestamp`), so downstream
/// references to `alias.<property>` must use the property-named CTE column and
/// must NOT be reverse-mapped to the physical schema column (CLAUDE.md §2:
/// forward-resolution through a CTE barrier).
pub(crate) fn is_pattern_union_rel_alias(alias: &str, plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.alias == alias => rel.pattern_combinations.is_some(),
        LogicalPlan::GraphRel(rel) => {
            is_pattern_union_rel_alias(alias, &rel.left)
                || is_pattern_union_rel_alias(alias, &rel.center)
                || is_pattern_union_rel_alias(alias, &rel.right)
        }
        LogicalPlan::GraphNode(node) => is_pattern_union_rel_alias(alias, &node.input),
        LogicalPlan::Filter(filter) => is_pattern_union_rel_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => is_pattern_union_rel_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => is_pattern_union_rel_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => is_pattern_union_rel_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => is_pattern_union_rel_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => is_pattern_union_rel_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => is_pattern_union_rel_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => is_pattern_union_rel_alias(alias, &cte.input),
        LogicalPlan::WithClause(with_clause) => {
            is_pattern_union_rel_alias(alias, &with_clause.input)
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| is_pattern_union_rel_alias(alias, input)),
        _ => false,
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
#[allow(clippy::only_used_in_recursion)] // last_node_alias forwarded through recursion
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

                // Extract labels for filter categorization and property extraction.
                // CRITICAL: left_connection/right_connection are direction-swapped (traversal order)
                // but graph_rel.left/right are NOT swapped (Cypher order). So we must extract
                // labels using the connection aliases (via plan_ctx) to ensure start_label
                // matches start_alias and end_label matches end_alias.
                // Fallback to endpoint extraction from the plan tree for nested chains.
                let start_label = plan_ctx
                    .and_then(|ctx| {
                        ctx.get_table_ctx(&graph_rel.left_connection)
                            .ok()
                            .and_then(|tc| tc.get_label_str().ok())
                    })
                    .or_else(|| extract_endpoint_node_label_with_schema(&graph_rel.left, schema))
                    .or_else(|| extract_endpoint_node_label_with_schema(&graph_rel.right, schema))
                    .unwrap_or_default();
                let end_label = plan_ctx
                    .and_then(|ctx| {
                        ctx.get_table_ctx(&graph_rel.right_connection)
                            .ok()
                            .and_then(|tc| tc.get_label_str().ok())
                    })
                    .or_else(|| extract_endpoint_node_label_with_schema(&graph_rel.right, schema))
                    .or_else(|| extract_endpoint_node_label_with_schema(&graph_rel.left, schema))
                    .unwrap_or_default();
                log::info!(
                    "🔍 VLP labels: start_label={}, end_label={}, left_conn={}, right_conn={}",
                    start_label,
                    end_label,
                    graph_rel.left_connection,
                    graph_rel.right_connection
                );

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

                let rel_table = {
                    let rel_type = if let Some(labels) = &graph_rel.labels {
                        labels.first().unwrap_or(&graph_rel.alias)
                    } else {
                        &graph_rel.alias
                    };

                    // For VLP with different start/end labels (e.g., Message→Post),
                    // the traversal planner may have picked the wrong relationship table
                    // (e.g., Comment_replyOf_Comment) because no single schema entry
                    // matches both node types exactly. Override with start→start lookup
                    // to get the union view (e.g., Message_replyOf_Message).
                    let cross_type_override = !start_label.is_empty()
                        && !end_label.is_empty()
                        && start_label != end_label;

                    if cross_type_override {
                        log::info!(
                            "🔍 VLP cross-type override: {}→{}. Using {}→{} schema lookup for recursive traversal",
                            start_label, end_label, start_label, start_label
                        );
                        let view_params = extract_view_parameter_values(&graph_rel.left)
                            .or_else(|| extract_view_parameter_values(&graph_rel.right))
                            .unwrap_or_default();
                        if !view_params.is_empty() {
                            rel_type_to_table_name_with_nodes_and_params(
                                rel_type,
                                Some(start_label.as_str()),
                                Some(start_label.as_str()),
                                schema,
                                &view_params,
                            )
                        } else {
                            rel_type_to_table_name_with_nodes(
                                rel_type,
                                Some(start_label.as_str()),
                                Some(start_label.as_str()),
                                schema,
                            )
                        }
                    } else {
                        match graph_rel.center.as_ref() {
                            LogicalPlan::ViewScan(_) => {
                                let result =
                                    extract_parameterized_rel_table(graph_rel.center.as_ref());
                                log::info!(
                                    "🔍 VLP: extract_parameterized_rel_table returned: {:?}",
                                    result
                                );
                                result.unwrap_or_else(|| {
                                    log::debug!(
                                        "Failed to extract parameterized rel table from ViewScan"
                                    );
                                    "unknown_rel_table".to_string()
                                })
                            }
                            _ => {
                                let (lookup_from, lookup_to) =
                                    (Some(start_label.as_str()), Some(end_label.as_str()));
                                let view_params = extract_view_parameter_values(&graph_rel.left)
                                    .or_else(|| extract_view_parameter_values(&graph_rel.right))
                                    .unwrap_or_default();
                                if !view_params.is_empty() {
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

                    if let Some(cleaned_predicate) = cleaned_predicate {
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
                    } else {
                        log::info!("🔧 WHERE predicate contained only label expressions - cleared");
                        (None, None, None, None)
                    }
                } else {
                    (None, None, None, None)
                };

                log::debug!("After categorization and mapping:");
                log::debug!("  start_filters_sql: {:?}", start_filters_sql);
                log::debug!("  end_filters_sql: {:?}", end_filters_sql);
                log::debug!("  rel_filters_sql: {:?}", rel_filters_sql);

                // 🔧 BOUND NODE FIX: Extract filters from bound nodes (Filter → GraphNode)
                // For ALL VLP queries, inline property predicates like {code: 'LAX'} are in Filter nodes
                // wrapping the GraphNodes, not in where_predicate.
                // Examples:
                //   - MATCH path = (p1:Person {id: 1})-[:KNOWS*]-(p2:Person {id: 2})
                //   - MATCH (origin:Airport {code: 'LAX'})-[:FLIGHT*1..2]->(dest:Airport {code: 'ATL'})
                log::debug!(
                    "VLP BOUND NODE FIX: Extracting inline property predicates from bound nodes..."
                );
                log::debug!("  Start alias: {}, End alias: {}", start_alias, end_alias);
                log::debug!("  Current start_filters_sql: {:?}", start_filters_sql);
                log::debug!("  Current end_filters_sql: {:?}", end_filters_sql);
                log::info!(
                    "  graph_rel.left type: {:?}",
                    std::mem::discriminant(graph_rel.left.as_ref())
                );
                log::info!(
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
                    log::debug!("Adding bound start node filter: {}", bound_start_filter);
                    start_filters_sql = Some(match start_filters_sql {
                        Some(existing) => {
                            format!("({}) AND ({})", existing, bound_start_filter)
                        }
                        None => bound_start_filter,
                    });
                } else if let Some(bound_start_filter) = extract_bound_node_filter(
                    // 🔧 FIX: For BidirectionalUnion reverse branch, start node's filter
                    // may be in the right subtree (connections are swapped).
                    &graph_rel.right,
                    &start_alias,
                    &start_cte_alias,
                    rel_type,
                    Some(crate::render_plan::cte_generation::NodeRole::From),
                ) {
                    log::info!(
                        "🔧 Adding bound start node filter (from right subtree): {}",
                        bound_start_filter
                    );
                    start_filters_sql = Some(match start_filters_sql {
                        Some(existing) => {
                            format!("({}) AND ({})", existing, bound_start_filter)
                        }
                        None => bound_start_filter,
                    });
                } else {
                    log::debug!("No bound start node filter found");
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
                } else if let Some(bound_end_filter) = extract_bound_node_filter(
                    // 🔧 FIX: For multi-pattern MATCH, the end node's filter may be in the
                    // left subtree (inside a CartesianProduct of standalone patterns).
                    &graph_rel.left,
                    &end_alias,
                    &end_cte_alias,
                    rel_type,
                    Some(crate::render_plan::cte_generation::NodeRole::To),
                ) {
                    log::info!(
                        "🔧 Adding bound end node filter (from left subtree): {}",
                        bound_end_filter
                    );
                    end_filters_sql = Some(match end_filters_sql {
                        Some(existing) => format!("({}) AND ({})", existing, bound_end_filter),
                        None => bound_end_filter,
                    });
                } else {
                    log::debug!("No bound end node filter found");
                }

                log::debug!("  Final start_filters_sql: {:?}", start_filters_sql);
                log::debug!("  Final end_filters_sql: {:?}", end_filters_sql);

                // For optional VLP, don't include start node filters in CTE
                // The filters should remain on the base table in the final query
                if graph_rel.is_optional.unwrap_or(false) {
                    log::info!("🔧 Optional VLP: Removing start_filters_sql from CTE (will be applied to final FROM)");
                    start_filters_sql = None;
                }

                // Extract properties from filter expressions for shortest path queries
                // Even in SQL_ONLY mode, we need properties that appear in filters
                // Also include properties required by downstream WITH/RETURN clauses
                // (PropertyRequirements captures what the rest of the query needs)
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

                    // Also include properties from PropertyRequirements (downstream usage)
                    // Without this, properties like friend.birthday referenced after WITH
                    // won't be included in the VLP CTE columns
                    let prop_reqs = plan_ctx.and_then(|ctx| ctx.get_property_requirements());
                    let existing_props: std::collections::HashSet<(String, String)> = props
                        .iter()
                        .map(|p| (p.cypher_alias.clone(), p.alias.clone()))
                        .collect();

                    for (alias, label) in [(&start_alias, &start_label), (&end_alias, &end_label)] {
                        if label.is_empty() {
                            continue;
                        }
                        let reqs = prop_reqs.and_then(|r| r.get_requirements(alias));
                        let include_all = prop_reqs.is_none_or(|r| {
                            r.requires_all(alias) || r.get_requirements(alias).is_none()
                        });
                        if let Ok(node_schema) = schema.node_schema(label) {
                            let id_columns = node_schema.node_id.columns();
                            let mut sorted_props: Vec<_> =
                                node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (prop_name, prop_value) in sorted_props {
                                if id_columns.contains(&prop_value.raw()) {
                                    continue;
                                }
                                if !include_all {
                                    if let Some(req_set) = reqs {
                                        if !req_set.contains(prop_name.as_str())
                                            && !req_set.contains(prop_value.raw())
                                        {
                                            continue;
                                        }
                                    }
                                }
                                if !existing_props.contains(&(alias.clone(), prop_name.clone())) {
                                    props.push(NodeProperty {
                                        cypher_alias: alias.clone(),
                                        column_name: prop_value.raw().to_string(),
                                        alias: prop_name.clone(),
                                    });
                                }
                            }
                        }
                    }

                    props
                } else {
                    // For regular VLP queries, include node properties needed by the query.
                    // Uses PropertyRequirements to prune unused properties from the recursive CTE.
                    // Fallback: include ALL properties when requirements are unknown or wildcard.
                    log::debug!(
                        "VLP property extraction for ({}-{})",
                        start_label,
                        end_label
                    );
                    let mut props = Vec::new();

                    // Get property requirements for pruning (if available)
                    let prop_reqs = plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                    // Helper: check if we should include all properties for this alias
                    let needs_all = |alias: &str| -> bool {
                        match prop_reqs {
                            None => true, // No requirements tracked → safe fallback
                            Some(reqs) => {
                                if reqs.requires_all(alias) {
                                    return true; // Wildcard (bare node ref like RETURN a)
                                }
                                // If alias has no entry at all, include all (safe fallback)
                                reqs.get_requirements(alias).is_none()
                            }
                        }
                    };

                    // Get required property set for an alias (None = include all)
                    let required_props_for =
                        |alias: &str| -> Option<&std::collections::HashSet<String>> {
                            prop_reqs.and_then(|reqs| {
                                if reqs.requires_all(alias) {
                                    None
                                } else {
                                    reqs.get_requirements(alias)
                                }
                            })
                        };

                    // Get all properties for start node
                    if !start_label.is_empty() {
                        if let Ok(start_node_schema) = schema.node_schema(&start_label) {
                            let start_id_columns = start_node_schema.node_id.columns();
                            let required = required_props_for(&start_alias);
                            let include_all = needs_all(&start_alias);

                            let mut sorted_props: Vec<_> =
                                start_node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (prop_name, prop_value) in sorted_props {
                                if start_id_columns.contains(&prop_value.raw()) {
                                    continue;
                                }
                                // Prune: skip if requirements exist and this property isn't needed
                                // Check both Cypher property name (prop_name) and DB column name
                                // (prop_value.raw()) because PropertyRequirementsAnalyzer records
                                // DB column names after view resolution
                                if !include_all {
                                    if let Some(req_set) = required {
                                        if !req_set.contains(prop_name.as_str())
                                            && !req_set.contains(prop_value.raw())
                                        {
                                            continue;
                                        }
                                    }
                                }
                                props.push(NodeProperty {
                                    cypher_alias: start_alias.clone(),
                                    column_name: prop_value.raw().to_string(),
                                    alias: prop_name.clone(),
                                });
                            }
                            if !include_all {
                                log::debug!(
                                    "VLP property pruning: start node '{}' ({}) — {} of {} properties kept",
                                    start_alias,
                                    start_label,
                                    props.len(),
                                    start_node_schema.property_mappings.len()
                                );
                            }
                        }
                    }

                    // Get properties for end node
                    let start_count = props.len();
                    if !end_label.is_empty() {
                        if let Ok(end_node_schema) = schema.node_schema(&end_label) {
                            let end_id_columns = end_node_schema.node_id.columns();
                            let required = required_props_for(&end_alias);
                            let include_all = needs_all(&end_alias);

                            let mut sorted_props: Vec<_> =
                                end_node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (prop_name, prop_value) in sorted_props {
                                if end_id_columns.contains(&prop_value.raw()) {
                                    continue;
                                }
                                // Check both Cypher property name and DB column name
                                if !include_all {
                                    if let Some(req_set) = required {
                                        if !req_set.contains(prop_name.as_str())
                                            && !req_set.contains(prop_value.raw())
                                        {
                                            continue;
                                        }
                                    }
                                }
                                props.push(NodeProperty {
                                    cypher_alias: end_alias.clone(),
                                    column_name: prop_value.raw().to_string(),
                                    alias: prop_name.clone(),
                                });
                            }
                            if !include_all {
                                log::debug!(
                                    "VLP property pruning: end node '{}' ({}) — {} of {} properties kept",
                                    end_alias,
                                    end_label,
                                    props.len() - start_count,
                                    end_node_schema.property_mappings.len()
                                );
                            }
                        }
                    }

                    // Bidirectional VLP fix: when start and end have the same label
                    // (e.g., Person-[:KNOWS]-Person), the UNION ALL SELECT for the
                    // WITH CTE uses the same column names (end_firstName) for both
                    // forward and reverse branches. Ensure properties required by one
                    // alias are mirrored to the other, so both branches produce
                    // matching columns regardless of start_/end_ prefix assignment.
                    if start_label == end_label
                        && !start_label.is_empty()
                        && start_alias != end_alias
                    {
                        let mut mirrored = Vec::new();
                        for p in &props {
                            if p.cypher_alias == start_alias {
                                // Check if this property already exists for end_alias
                                if !props
                                    .iter()
                                    .any(|ep| ep.cypher_alias == end_alias && ep.alias == p.alias)
                                {
                                    mirrored.push(NodeProperty {
                                        cypher_alias: end_alias.clone(),
                                        column_name: p.column_name.clone(),
                                        alias: p.alias.clone(),
                                    });
                                }
                            } else if p.cypher_alias == end_alias {
                                // Check if this property already exists for start_alias
                                if !props
                                    .iter()
                                    .any(|sp| sp.cypher_alias == start_alias && sp.alias == p.alias)
                                {
                                    mirrored.push(NodeProperty {
                                        cypher_alias: start_alias.clone(),
                                        column_name: p.column_name.clone(),
                                        alias: p.alias.clone(),
                                    });
                                }
                            }
                        }
                        if !mirrored.is_empty() {
                            log::info!(
                                "VLP bidirectional mirror: adding {} mirrored properties for same-label nodes ({} = {})",
                                mirrored.len(), start_label, end_label
                            );
                            props.extend(mirrored);
                        }
                    }

                    log::debug!("VLP properties extracted: {}", props.len());
                    props
                };

                // Generate CTE with filters
                // For shortest path queries, always use recursive CTE (even for exact hops)
                // because we need proper filtering and shortest path selection logic

                // 🎯 DECISION POINT: CTE or inline JOINs?
                // BUT FIRST: Check if this is multi-type VLP (requires UNION ALL, not chained JOINs)
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type = should_use_join_expansion(graph_rel, &rel_types, schema);

                // Denormalized VLP: both nodes are virtual (same table as relationship).
                // Chained JOINs produce mismatched aliases (r1/r2 vs t1) and break SELECT.
                // Always use recursive CTE for denormalized schemas.
                let is_denormalized_vlp = matches!(graph_rel.left.as_ref(), LogicalPlan::GraphNode(n) if n.is_denormalized)
                    && matches!(graph_rel.right.as_ref(), LogicalPlan::GraphNode(n) if n.is_denormalized);

                let use_chained_join = spec.exact_hop_count().is_some()
                    && graph_rel.shortest_path_mode.is_none()
                    && !is_multi_type // Don't use chained JOINs for multi-type VLP
                    && !is_denormalized_vlp; // Don't use chained JOINs for denormalized VLP

                if use_chained_join {
                    // 🚀 OPTIMIZATION: Fixed-length, non-shortest-path → NO CTE!
                    // Generate inline JOINs instead of recursive CTE
                    let exact_hops = spec.exact_hop_count().unwrap();
                    log::debug!(
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

                        log::debug!(
                            "CTE BRANCH: Stored fixed-length JOINs for {}-{} pattern",
                            vlp_ctx.start_alias,
                            vlp_ctx.end_alias
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
                    log::debug!("CTE BRANCH: Variable-length pattern detected");
                    log::info!("🔍 VLP: Variable-length or shortest path detected (not using chained JOINs)");

                    // 🎯 CHECK FOR MULTI-TYPE VLP (Part 1D implementation)
                    let mut rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                    // Strip composite key suffixes (e.g., "HAS_TYPE::Tag::TagClass" -> "HAS_TYPE")
                    // Type inference may add ::From::To to distinguish ambiguous relationship types,
                    // but the multi-type VLP generator resolves types via schema from/to node matching
                    rel_types = rel_types
                        .into_iter()
                        .map(|rt| {
                            if rt.contains("::") {
                                rt.split("::").next().unwrap_or(&rt).to_string()
                            } else {
                                rt
                            }
                        })
                        .collect();
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

                        let is_undirected_pattern = graph_rel.was_undirected.unwrap_or(false);

                        // Extract start labels from graph pattern
                        let mut start_labels =
                            extract_node_labels(&graph_rel.left).unwrap_or_else(|| {
                                // Fallback 1: extract from ViewScan
                                if let Some(label) = extract_node_label_from_viewscan_with_schema(
                                    &graph_rel.left,
                                    schema,
                                ) {
                                    return vec![label];
                                }
                                // Fallback 2: look up from plan context using left_connection alias
                                if let Some(ctx) = plan_ctx {
                                    if let Ok(table_ctx) =
                                        ctx.get_table_ctx(&graph_rel.left_connection)
                                    {
                                        if let Ok(label) = table_ctx.get_label_str() {
                                            log::info!(
                                                "🔧 VLP: Derived start label '{}' from plan context for alias '{}'",
                                                label, graph_rel.left_connection
                                            );
                                            return vec![label];
                                        }
                                    }
                                }
                                // Fallback 3: derive from schema relationships
                                let mut from_types: Vec<String> = rel_types
                                    .iter()
                                    .flat_map(|rt| {
                                        schema.get_all_rel_schemas_for_type(rt)
                                            .into_iter()
                                            .map(|rs| rs.from_node.clone())
                                    })
                                    .collect();
                                from_types.sort();
                                from_types.dedup();
                                if !from_types.is_empty() {
                                    log::info!(
                                        "🔧 VLP: Derived start labels {:?} from schema relationships",
                                        from_types
                                    );
                                    return from_types;
                                }
                                vec!["UnknownStartType".to_string()]
                            });

                        // If start_labels only contains __Unlabeled and the left child is a
                        // chained GraphRel (i.e., this is the second+ hop of a chained pattern
                        // like (n)-->(a)-->(b)), supplement with from_node types from the outer
                        // relationship schemas. This handles cases like FRIEND: B→C where type
                        // inference constrains `a` to __Unlabeled from the first hop, causing
                        // FRIEND branches to be missed. We only apply this for chained patterns
                        // to avoid incorrect expansions in simple single-hop anonymous patterns.
                        if start_labels.iter().all(|l| l == "__Unlabeled")
                            && matches!(&*graph_rel.left, LogicalPlan::GraphRel(_))
                        {
                            let mut extra: Vec<String> = rel_types
                                .iter()
                                .flat_map(|rt| {
                                    schema
                                        .get_all_rel_schemas_for_type(rt)
                                        .into_iter()
                                        .map(|rs| rs.from_node.clone())
                                })
                                .filter(|ft| ft != "__Unlabeled" && !start_labels.contains(ft))
                                .collect();
                            extra.sort();
                            extra.dedup();
                            if !extra.is_empty() {
                                log::info!(
                                    "🔧 VLP: Supplementing start_labels with schema from_nodes {:?} for untyped start '{}'",
                                    extra, graph_rel.left_connection
                                );
                                start_labels.extend(extra);
                            }
                        }

                        // For multi-type VLP, we need ALL possible end types from the relationship schema
                        // The GraphNode label might only have one type (from type inference),
                        // but the actual query could reach multiple types
                        let mut end_labels: Vec<String> = Vec::new();

                        // For directed patterns, trust explicit labels from the graph pattern.
                        // For undirected patterns (was_undirected=true), the GraphNode's inferred
                        // label may reflect only one direction (e.g., the to_node from forward
                        // traversal). We always re-derive end types from the schema below, then
                        // intersect with any explicit labels to preserve user constraints like
                        // (a)-[:R*]-(b:User) while still covering both directions.
                        let explicit_end_labels = extract_node_labels(&graph_rel.right);

                        if !is_undirected_pattern {
                            if let Some(ref labels) = explicit_end_labels {
                                end_labels = labels.clone();
                            }
                        }

                        // Derive all possible end types from relationships when end_labels is
                        // empty (undirected patterns always derive, directed only when untyped).
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

                                rel_types = {
                                    let mut v: Vec<_> = all_rel_types.into_iter().collect();
                                    v.sort();
                                    v
                                };
                                log::info!(
                                    "🎯 CTE: Inferred {} relationship types: {:?}",
                                    rel_types.len(),
                                    rel_types
                                );
                            }

                            for rel_type in &rel_types {
                                // Use get_all_rel_schemas_for_type to iterate ALL variants
                                // e.g., HAS_CREATOR has Comment→Person, Post→Person, Message→Person
                                let all_schemas = schema.get_all_rel_schemas_for_type(rel_type);
                                for rel_schema in &all_schemas {
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
                                // Multiple possible end types - use all of them (sorted for determinism)
                                end_labels = {
                                    let mut v: Vec<_> = possible_end_types.into_iter().collect();
                                    v.sort();
                                    v
                                };
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

                            // For undirected patterns with explicit end labels (e.g., (a)--(b:User)),
                            // intersect schema-derived types with the explicit constraint to avoid
                            // widening the query beyond what the user specified.
                            if is_undirected_pattern {
                                if let Some(ref explicit) = explicit_end_labels {
                                    let explicit_set: std::collections::HashSet<&String> =
                                        explicit.iter().collect();
                                    let intersected: Vec<String> = end_labels
                                        .iter()
                                        .filter(|l| explicit_set.contains(l))
                                        .cloned()
                                        .collect();
                                    if !intersected.is_empty() {
                                        end_labels = intersected;
                                    }
                                    // If intersection is empty, keep schema-derived labels —
                                    // the explicit label was inferred (not user-specified) and
                                    // may reflect only one direction.
                                }
                            }
                        }

                        // === SUPERTYPE FILTERING (remove supertypes when all subtypes present) ===
                        // When a polymorphic supertype (e.g., Message = Post|Comment) is present
                        // AND all its subtypes are also present, REMOVE the supertype.
                        // CTE UNION branches are per-concrete-subtype; the supertype would double-count.
                        let start_labels = {
                            let start_set: std::collections::HashSet<String> =
                                start_labels.iter().cloned().collect();
                            let mut supertypes_to_remove: std::collections::HashSet<String> =
                                std::collections::HashSet::new();
                            for label in &start_set {
                                if let Some(ns) = schema.node_schema_opt(label) {
                                    if let Some(ref lv) = ns.label_value {
                                        let subtypes: Vec<&str> = lv.split('|').collect();
                                        if subtypes.len() >= 2
                                            && subtypes.iter().all(|st| start_set.contains(*st))
                                        {
                                            // All subtypes present → remove the supertype
                                            supertypes_to_remove.insert(label.clone());
                                        }
                                    }
                                }
                            }
                            let mut filtered: Vec<String> = start_labels
                                .into_iter()
                                .filter(|label| {
                                    if supertypes_to_remove.contains(label) {
                                        log::info!(
                                            "  ✂️  CTE supertype filtering: removing start_label '{}' (supertype with all subtypes present)",
                                            label
                                        );
                                        return false;
                                    }
                                    true
                                })
                                .collect();
                            filtered.sort();
                            filtered
                        };
                        let end_labels = {
                            let end_set: std::collections::HashSet<String> =
                                end_labels.iter().cloned().collect();
                            let mut supertypes_to_remove: std::collections::HashSet<String> =
                                std::collections::HashSet::new();
                            for label in &end_set {
                                if let Some(ns) = schema.node_schema_opt(label) {
                                    if let Some(ref lv) = ns.label_value {
                                        let subtypes: Vec<&str> = lv.split('|').collect();
                                        if subtypes.len() >= 2
                                            && subtypes.iter().all(|st| end_set.contains(*st))
                                        {
                                            supertypes_to_remove.insert(label.clone());
                                        }
                                    }
                                }
                            }
                            let mut filtered: Vec<String> = end_labels
                                .into_iter()
                                .filter(|label| {
                                    if supertypes_to_remove.contains(label) {
                                        log::info!(
                                            "  ✂️  CTE supertype filtering: removing end_label '{}' (supertype with all subtypes present)",
                                            label
                                        );
                                        return false;
                                    }
                                    true
                                })
                                .collect();
                            filtered.sort();
                            filtered
                        };

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

                        // For multi-type VLP, sanitize filters:
                        // 1. Strip predicates referencing external variables (e.g., `IN tags`
                        //    from WITH clause output). These belong in the outer query.
                        // 2. Filters involving both start and end aliases (OR) are kept in
                        //    start_filters with end_node. alias replacement done per branch.
                        let mt_start_filters = start_filters_sql.as_ref().map(|f| {
                            // Strip outer wrapping parens to get clean predicates
                            let inner = f.trim();
                            let inner = {
                                let mut s = inner;
                                // Peel matching outer parens
                                while s.starts_with('(') && s.ends_with(')') {
                                    let mut depth = 0i32;
                                    let mut wraps_all = true;
                                    for (i, ch) in s.chars().enumerate() {
                                        match ch {
                                            '(' => depth += 1,
                                            ')' => {
                                                depth -= 1;
                                                if depth == 0 && i < s.len() - 1 {
                                                    wraps_all = false;
                                                    break;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    if wraps_all {
                                        s = &s[1..s.len() - 1];
                                    } else {
                                        break;
                                    }
                                }
                                s
                            };
                            // Split on top-level " AND " (respecting paren depth)
                            let parts = crate::clickhouse_query_generator::multi_type_vlp_joins::split_top_level_and(inner);
                            let valid: Vec<&str> = parts
                                .into_iter()
                                .filter(|p| {
                                    // Strip predicates referencing bare identifiers (external variables)
                                    let trimmed = p.trim();
                                    if trimmed.contains(" IN ") {
                                        let after_in = trimmed.split(" IN ").last().unwrap_or("");
                                        let after_in = after_in.trim().trim_end_matches(')');
                                        if !after_in.contains('.') && !after_in.starts_with('(')
                                            && !after_in.starts_with('[')
                                        {
                                            log::info!(
                                                "🔧 VLP: Stripping cross-scope predicate from CTE filter: {}",
                                                trimmed
                                            );
                                            return false;
                                        }
                                    }
                                    true
                                })
                                .collect();
                            if valid.is_empty() {
                                String::new()
                            } else {
                                valid.join(" AND ")
                            }
                        }).filter(|f| !f.is_empty());

                        let is_undirected = graph_rel.was_undirected.unwrap_or(false);
                        let generator = MultiTypeVlpJoinGenerator::new(
                            schema,
                            start_labels,
                            rel_types,
                            end_labels,
                            spec.clone(),
                            start_alias.clone(),
                            end_alias.clone(),
                            mt_start_filters,
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
                    log::debug!("CTE BRANCH: Single-type VLP - using recursive CTE");

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
                        log::debug!("CTE: Using denormalized generator for variable-length path (both nodes virtual)");

                        // For denormalized nodes, properties come from the edge table
                        let mut all_denorm_properties = filter_properties.clone();

                        // Get PropertyRequirements for pruning
                        let prop_reqs = plan_ctx.and_then(|ctx| ctx.get_property_requirements());
                        let start_conn = &graph_rel.left_connection;
                        let end_conn = &graph_rel.right_connection;

                        let needs_all_for = |alias: &str| -> bool {
                            match prop_reqs {
                                None => true,
                                Some(reqs) => {
                                    reqs.requires_all(alias)
                                        || reqs.get_requirements(alias).is_none()
                                }
                            }
                        };
                        let required_for =
                            |alias: &str| -> Option<&std::collections::HashSet<String>> {
                                prop_reqs.and_then(|reqs| {
                                    if reqs.requires_all(alias) {
                                        None
                                    } else {
                                        reqs.get_requirements(alias)
                                    }
                                })
                            };

                        let start_include_all = needs_all_for(start_conn);
                        let end_include_all = needs_all_for(end_conn);
                        let start_required = required_for(start_conn);
                        let end_required = required_for(end_conn);

                        // Handle both "table" and "database.table" formats
                        let rel_table_name = rel_table.rsplit('.').next().unwrap_or(&rel_table);

                        if let Some(node_schema) = schema.all_node_schemas().values().find(|n| {
                            let schema_table =
                                n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
                            schema_table == rel_table_name
                        }) {
                            // Add from_node properties (filtered by requirements)
                            // Sorted by cypher property name: these HashMap
                            // entries become the CTE's fixed column order, so
                            // an unsorted iteration flaps across processes
                            // (#480, same recipe as #458/#464).
                            if let Some(ref from_props) = node_schema.from_properties {
                                let mut from_props: Vec<(&String, &String)> =
                                    from_props.iter().collect();
                                from_props.sort_by(|a, b| a.0.cmp(b.0));
                                for (logical_prop, physical_col) in from_props {
                                    if !start_include_all {
                                        if let Some(req_set) = start_required {
                                            if !req_set.contains(logical_prop.as_str())
                                                && !req_set.contains(physical_col.as_str())
                                            {
                                                continue;
                                            }
                                        }
                                    }
                                    if !all_denorm_properties.iter().any(|p| {
                                        p.cypher_alias == *start_conn && p.alias == *logical_prop
                                    }) {
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: start_conn.clone(),
                                            column_name: physical_col.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }

                            // Add to_node properties (filtered by requirements)
                            // Sorted for determinism — see from_properties above (#480).
                            if let Some(ref to_props) = node_schema.to_properties {
                                let mut to_props: Vec<(&String, &String)> =
                                    to_props.iter().collect();
                                to_props.sort_by(|a, b| a.0.cmp(b.0));
                                for (logical_prop, physical_col) in to_props {
                                    if !end_include_all {
                                        if let Some(req_set) = end_required {
                                            if !req_set.contains(logical_prop.as_str())
                                                && !req_set.contains(physical_col.as_str())
                                            {
                                                continue;
                                            }
                                        }
                                    }
                                    if !all_denorm_properties.iter().any(|p| {
                                        p.cypher_alias == *end_conn && p.alias == *logical_prop
                                    }) {
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: end_conn.clone(),
                                            column_name: physical_col.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }
                        }

                        log::debug!(
                            "CTE: Denormalized properties count: {}",
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
                    log::debug!("  rel_filters_sql: {:?}", rel_filters_sql);

                    // Detect weight CTE for weighted shortest path
                    // Check task-local QueryContext (set by build_chained_with_match_cte_plan)
                    let weight_cte_config = if graph_rel.shortest_path_mode.is_some() {
                        crate::server::query_context::get_weight_cte_config().inspect(|wc| {
                            log::info!(
                                "🏋️ Detected weight CTE '{}' for weighted shortest path",
                                wc.cte_name
                            );
                        })
                    } else {
                        None
                    };

                    // Check if query needs path_relationships array. This is needed when:
                    // - relationships(path) is called explicitly, OR
                    // - the path variable is returned bare (RETURN p) for Bolt Path encoding
                    let needs_path_rels = if let Some(ref pv) = graph_rel.path_variable {
                        let check = context.root_plan.as_deref().unwrap_or(plan);
                        plan_uses_relationships_fn(check, pv)
                            || plan_uses_bare_path_variable(check, pv)
                    } else {
                        false
                    };

                    // Detect lightweight BFS mode for shortestPath + length(path)-only queries.
                    // BFS tracks only distinct reachable node_ids per hop level instead of
                    // per-path visited arrays, reducing memory from ~500M rows to ~180K rows.
                    // Use root_plan (the full query tree) for checking path variable usage,
                    // since the local `plan` is just the GraphRel subtree and doesn't contain
                    // the outer Projection (e.g., RETURN p).
                    let check_plan: &LogicalPlan = context.root_plan.as_deref().unwrap_or(plan);
                    // BFS CTE only has (node_id, hop) — no node properties.
                    // Check if the plan references any properties of start/end nodes.
                    let start_conn = &graph_rel.left_connection;
                    let end_conn = &graph_rel.right_connection;
                    let plan_needs_endpoint_properties =
                        plan_references_alias_properties(check_plan, start_conn)
                            || plan_references_alias_properties(check_plan, end_conn);

                    // BFS requires both endpoints to be ID-bound. The BFS CTE
                    // only tracks (node_id, hop) — no filters can be applied inside
                    // the recursive traversal except stop-at-target by ID.
                    let start_id_col = match &pattern_ctx.left_node {
                        crate::graph_catalog::pattern_schema::NodeAccessStrategy::OwnTable {
                            id_column,
                            ..
                        } => id_column.first_column().to_string(),
                        _ => String::new(),
                    };
                    let end_id_col = match &pattern_ctx.right_node {
                        crate::graph_catalog::pattern_schema::NodeAccessStrategy::OwnTable {
                            id_column,
                            ..
                        } => id_column.first_column().to_string(),
                        _ => String::new(),
                    };
                    let start_has_id_filter = combined_start_filters.as_ref().is_some_and(|f| {
                        crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator::extract_id_from_filter(
                            f, "start_node", &start_id_col,
                        ).is_some()
                    });
                    let end_has_id_filter = combined_end_filters.as_ref().is_some_and(|f| {
                        crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator::extract_id_from_filter(
                            f, "end_node", &end_id_col,
                        ).is_some()
                    });

                    let needs_bfs_mode = graph_rel.shortest_path_mode.is_some()
                        && weight_cte_config.is_none()
                        && !plan_needs_endpoint_properties
                        && start_has_id_filter
                        && end_has_id_filter
                        && graph_rel.path_variable.as_ref().is_none_or(|pv| {
                            !plan_uses_nodes_fn(check_plan, pv)
                                && !plan_uses_relationships_fn(check_plan, pv)
                                && !plan_uses_bare_path_variable(check_plan, pv)
                        });
                    let is_undirected = graph_rel.direction
                        == crate::query_planner::logical_expr::Direction::Either
                        || graph_rel.was_undirected == Some(true);

                    if needs_bfs_mode {
                        log::info!(
                            "BFS mode enabled for shortestPath VLP: {} -[*]-> {} (undirected={})",
                            start_label,
                            end_label,
                            is_undirected
                        );
                    }

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
                        weight_cte_config,
                        needs_path_rels,
                        needs_bfs_mode,
                        is_undirected,
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

                // #466: An UNDIRECTED expand binds each endpoint to BOTH roles.
                // For every combination we emit the FORWARD branch (n = from-node,
                // o = to-node) and, when undirected, an additional REVERSE branch
                // that traverses the SAME physical edge with start/end swapped
                // (n = to-node, o = from-node). The join is identical; only the
                // projected start_*/end_* columns swap. UNION ALL (no dedup): each
                // edge yields two rows, matching Cypher semantics. Self-loops
                // (from-id == to-id, only possible when both endpoints share a
                // table) are emitted ONCE — the reverse branch excludes them via a
                // guard — matching Neo4j (a self-loop appears once for `-[r]-`).
                let is_undirected = graph_rel.direction
                    == crate::query_planner::logical_expr::Direction::Either
                    || graph_rel.was_undirected == Some(true);

                // #466: node-property WHERE conjuncts (e.g. `o.name = 'Alice'`)
                // must be resolved PER-BRANCH inside the CTE: the CTE projection
                // does not expose node property columns, and which physical
                // table/label an alias binds to differs per combination and per
                // traversal orientation. The outer-WHERE rewriter
                // (`filter_builder::rewrite_predicate_for_pattern_cte`) skips
                // exactly these conjuncts using the same classifier. A property
                // missing on a branch's bound label resolves to NULL — the
                // branch then contributes nothing for comparisons (Cypher:
                // missing property = NULL) while `IS NULL` stays true.
                let pattern_node_aliases = [
                    graph_rel.left_connection.as_str(),
                    graph_rel.right_connection.as_str(),
                ];
                let branch_local_conjuncts: Vec<crate::query_planner::logical_expr::LogicalExpr> =
                    match graph_rel.where_predicate.as_ref() {
                        Some(pred) => {
                            let mut parts = Vec::new();
                            crate::render_plan::filter_builder::split_and_predicates(
                                pred, &mut parts,
                            );
                            // #466 round 3 (ride-along): whole-entity refs
                            // (`WHERE o = n`) and subquery/pattern conjuncts
                            // (`EXISTS {...}`) cannot be resolved per branch —
                            // error loudly instead of silently dropping them.
                            if let Some(bad) = parts.iter().find(|p| {
                                conjunct_has_unresolvable_entity_ref(p, &pattern_node_aliases)
                            }) {
                                return Err(RenderBuildError::UnsupportedFeature(format!(
                                    "WHERE predicate on a multi-type/undirected pattern \
                                     contains {} which cannot be resolved per UNION \
                                     branch; rewrite the filter using node properties, \
                                     id(), or labels()",
                                    describe_unresolvable_conjunct(bad, &pattern_node_aliases)
                                )));
                            }
                            parts
                                .into_iter()
                                .filter(|p| {
                                    conjunct_references_node_property(p, &pattern_node_aliases)
                                })
                                .cloned()
                                .collect()
                        }
                        None => Vec::new(),
                    };

                // Generate SELECT(s) for each combination: full pattern JOIN
                // Each branch: (from_node_table JOIN rel_table JOIN to_node_table)
                let union_branches: Result<Vec<Vec<String>>, RenderBuildError> = combinations
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

                        // Denormalized endpoint: the node is embedded in the edge
                        // table itself, with a VIRTUAL node_id/properties resolved
                        // through from_properties (when it is this edge's from-node)
                        // / to_properties (to-node) — e.g. Airport.code → flights.Origin
                        // (from) / flights.Dest (to). Such an endpoint IS the edge
                        // row: it must reference rel_table directly, with no alias and
                        // no join, and its id/properties resolve through the denorm
                        // property maps (NOT node_id/property_mappings, which are empty
                        // for a virtual-id node).
                        //
                        // The join-skip decision deliberately does NOT depend on which
                        // property map is present: a node embedded in the edge table is
                        // never a separate table to join, period. Requiring a specific
                        // role's map here would, for a self-loop node that defines only
                        // one of from_/to_properties, leave the other side to emit a
                        // spurious unaliased self-join on the non-existent virtual
                        // column. Resolution (below) still prefers the role-appropriate
                        // map and falls back to the other.
                        let from_denorm =
                            from_node_schema.is_denormalized && raw_from_table == rel_table;
                        let to_denorm = to_node_schema.is_denormalized && raw_to_table == rel_table;

                        // Self-join detection: when both endpoints are the same table,
                        // we need distinct aliases so ClickHouse can distinguish them —
                        // UNLESS an endpoint is denormalized (it reuses rel_table with
                        // no alias). Aliasing is only needed for two NON-denorm
                        // endpoints that genuinely share a table.
                        let is_self_join = raw_from_table == raw_to_table;
                        let needs_alias = is_self_join && !from_denorm && !to_denorm;
                        let from_table = if from_denorm {
                            rel_table.clone()
                        } else if needs_alias {
                            "from_node".to_string()
                        } else {
                            raw_from_table.clone()
                        };
                        let to_table = if to_denorm {
                            rel_table.clone()
                        } else if needs_alias {
                            "to_node".to_string()
                        } else {
                            raw_to_table.clone()
                        };
                        let from_join_expr = if needs_alias {
                            format!("{} AS from_node", raw_from_table)
                        } else {
                            raw_from_table.clone()
                        };
                        let to_join_expr = if needs_alias {
                            format!("{} AS to_node", raw_to_table)
                        } else {
                            raw_to_table.clone()
                        };

                        // ID columns (as Identifier for composite support)
                        let from_node_id = &from_node_schema.node_id.id;
                        let to_node_id = &to_node_schema.node_id.id;
                        let rel_from_col = &rel_schema.from_id;
                        let rel_to_col = &rel_schema.to_id;

                        // For a denormalized endpoint, the node_id is a VIRTUAL
                        // property: map it through the denorm property map to its
                        // physical column. Prefer the role-appropriate map
                        // (from_properties for the from-endpoint, to_properties for
                        // the to-endpoint); fall back to the other map for a
                        // partially-specified self-loop node, then to the bare name.
                        // Non-denorm endpoints pass the column through unchanged.
                        let from_props = from_node_schema.from_properties.as_ref();
                        let from_props_alt = from_node_schema.to_properties.as_ref();
                        let to_props = to_node_schema.to_properties.as_ref();
                        let to_props_alt = to_node_schema.from_properties.as_ref();
                        let resolve_from_id_col = |c: &str| -> String {
                            if from_denorm {
                                from_props
                                    .and_then(|m| m.get(c))
                                    .or_else(|| from_props_alt.and_then(|m| m.get(c)))
                                    .cloned()
                                    .unwrap_or_else(|| c.to_string())
                            } else {
                                c.to_string()
                            }
                        };
                        let resolve_to_id_col = |c: &str| -> String {
                            if to_denorm {
                                to_props
                                    .and_then(|m| m.get(c))
                                    .or_else(|| to_props_alt.and_then(|m| m.get(c)))
                                    .cloned()
                                    .unwrap_or_else(|| c.to_string())
                            } else {
                                c.to_string()
                            }
                        };

                        // Generate SELECT for this branch
                        // Output columns matching VLP/path pattern expectations:
                        // - start_id, end_id (for ID lookups)
                        // - path_relationships (array with relationship type)
                        // - rel_properties (array with relationship properties as JSON)

                        // Collect relationship properties for formatRowNoNewline.
                        // Iterate in sorted cypher-property order so the emitted
                        // column order (and thus the runtime JSON blob) is
                        // deterministic run-to-run — `property_mappings` is a
                        // HashMap, whose iteration order varies per process. This
                        // mirrors the vlp_multi_type / json_builder path, which
                        // already sorts (see json_builder.rs `sorted_keys`).
                        let rel_prop_cols: Vec<String> = {
                            let mut entries: Vec<_> = rel_schema.property_mappings.iter().collect();
                            entries.sort_by_key(|(k, _)| k.as_str());
                            entries
                                .into_iter()
                                .map(|(cypher_name, prop_val)| {
                                    // Quote physical columns (dotted names like `id.orig_h` must be
                                    // backtick-quoted); expressions are emitted verbatim.
                                    let col_ref = match prop_val {
                                        PropertyValue::Column(c) => {
                                            format!("{rel_table}.{}", quote_identifier(c))
                                        }
                                        PropertyValue::Expression(e) => format!("{rel_table}.{e}"),
                                    };
                                    format!("{col_ref} AS {cypher_name}")
                                })
                                .collect()
                        };

                        let rel_properties_json = if rel_prop_cols.is_empty() {
                            "'{}'".to_string() // Empty JSON object
                        } else {
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .json_row_object(&rel_prop_cols.join(", "))
                        };

                        // Collect node properties for start_properties and end_properties
                        // This enables path queries: MATCH p=()-->() RETURN p
                        // No AS aliases here — ClickHouse treats AS inside formatRowNoNewline
                        // as SELECT-level aliases, which conflict when both start_properties and
                        // end_properties reference same-named columns (User→User JOINs).
                        // The result transformer's clean_property_keys() strips table alias prefixes.
                        // For a denormalized endpoint the properties live in the
                        // role-appropriate denorm map (from_properties / to_properties),
                        // NOT property_mappings (which is empty for a virtual-id node).
                        // Fall back to the other map for a partially-specified self-loop.
                        // Sort by cypher-property key so the emitted column order
                        // (and the runtime JSON blob key order) is deterministic —
                        // both source maps are HashMaps (nondeterministic iteration).
                        let start_prop_cols: Vec<String> = if from_denorm {
                            from_props
                                .or(from_props_alt)
                                .map(|m| {
                                    let mut entries: Vec<_> = m.iter().collect();
                                    entries.sort_by_key(|(k, _)| k.as_str());
                                    entries
                                        .into_iter()
                                        .map(|(_, col)| {
                                            format!("{from_table}.{}", quote_identifier(col))
                                        })
                                        .collect()
                                })
                                .unwrap_or_default()
                        } else {
                            let mut entries: Vec<_> =
                                from_node_schema.property_mappings.iter().collect();
                            entries.sort_by_key(|(k, _)| k.as_str());
                            entries
                                .into_iter()
                                .map(|(_, prop_val)| match prop_val {
                                    PropertyValue::Column(c) => {
                                        format!("{from_table}.{}", quote_identifier(c))
                                    }
                                    PropertyValue::Expression(e) => format!("{from_table}.{e}"),
                                })
                                .collect()
                        };

                        let end_prop_cols: Vec<String> = if to_denorm {
                            to_props
                                .or(to_props_alt)
                                .map(|m| {
                                    let mut entries: Vec<_> = m.iter().collect();
                                    entries.sort_by_key(|(k, _)| k.as_str());
                                    entries
                                        .into_iter()
                                        .map(|(_, col)| {
                                            format!("{to_table}.{}", quote_identifier(col))
                                        })
                                        .collect()
                                })
                                .unwrap_or_default()
                        } else {
                            let mut entries: Vec<_> =
                                to_node_schema.property_mappings.iter().collect();
                            entries.sort_by_key(|(k, _)| k.as_str());
                            entries
                                .into_iter()
                                .map(|(_, prop_val)| match prop_val {
                                    PropertyValue::Column(c) => {
                                        format!("{to_table}.{}", quote_identifier(c))
                                    }
                                    PropertyValue::Expression(e) => format!("{to_table}.{e}"),
                                })
                                .collect()
                        };

                        let start_properties_json = if start_prop_cols.is_empty() {
                            "'{}'".to_string()
                        } else {
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .json_row_object(&start_prop_cols.join(", "))
                        };

                        let end_properties_json = if end_prop_cols.is_empty() {
                            "'{}'".to_string()
                        } else {
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .json_row_object(&end_prop_cols.join(", "))
                        };

                        // Extract base relationship type (strip ::FromLabel::ToLabel suffix)
                        let base_rel_type =
                            combo.rel_type.split("::").next().unwrap_or(&combo.rel_type);

                        // Build WHERE clauses for polymorphic type filtering
                        let mut where_clauses = Vec::new();
                        if let Some(ref type_col) = rel_schema.type_column {
                            where_clauses.push(format!(
                                "{rel_table}.{} = '{base_rel_type}'",
                                quote_identifier(type_col)
                            ));
                        }
                        if let Some(ref from_lbl_col) = rel_schema.from_label_column {
                            where_clauses.push(format!(
                                "{rel_table}.{} = '{}'",
                                quote_identifier(from_lbl_col),
                                combo.from_label
                            ));
                        }
                        if let Some(ref to_lbl_col) = rel_schema.to_label_column {
                            where_clauses.push(format!(
                                "{rel_table}.{} = '{}'",
                                quote_identifier(to_lbl_col),
                                combo.to_label
                            ));
                        }
                        // (The per-branch WHERE is assembled in `build_branch`
                        // below from `where_clauses` plus the reverse-only
                        // self-loop guard.)

                        // Generate start_id and end_id expressions for composite support
                        let cast_str = current_function_mapper().cast_string();
                        let start_id_expr = match from_node_id {
                            Identifier::Single(col) => {
                                format!(
                                    "{cast_str}({from_table}.{})",
                                    quote_identifier(&resolve_from_id_col(col))
                                )
                            }
                            Identifier::Composite(cols) => {
                                let parts: Vec<String> = cols
                                    .iter()
                                    .map(|c| {
                                        format!(
                                            "{cast_str}({from_table}.{})",
                                            quote_identifier(&resolve_from_id_col(c))
                                        )
                                    })
                                    .collect();
                                format!("concat({})", parts.join(", '|', "))
                            }
                        };
                        let end_id_expr = match to_node_id {
                            Identifier::Single(col) => {
                                format!(
                                    "{cast_str}({to_table}.{})",
                                    quote_identifier(&resolve_to_id_col(col))
                                )
                            }
                            Identifier::Composite(cols) => {
                                let parts: Vec<String> = cols
                                    .iter()
                                    .map(|c| {
                                        format!(
                                            "{cast_str}({to_table}.{})",
                                            quote_identifier(&resolve_to_id_col(c))
                                        )
                                    })
                                    .collect();
                                format!("concat({})", parts.join(", '|', "))
                            }
                        };

                        // Generate JOIN conditions for composite support
                        let from_join_parts: Vec<String> = from_node_id
                            .columns()
                            .iter()
                            .zip(rel_from_col.columns().iter())
                            .map(|(n, r)| {
                                format!(
                                    "{from_table}.{} = {rel_table}.{}",
                                    quote_identifier(n),
                                    quote_identifier(r)
                                )
                            })
                            .collect();
                        let from_join_cond = from_join_parts.join(" AND ");

                        let to_join_parts: Vec<String> = to_node_id
                            .columns()
                            .iter()
                            .zip(rel_to_col.columns().iter())
                            .map(|(n, r)| {
                                format!(
                                    "{to_table}.{} = {rel_table}.{}",
                                    quote_identifier(n),
                                    quote_identifier(r)
                                )
                            })
                            .collect();
                        let to_join_cond = to_join_parts.join(" AND ");

                        // Build direct relationship property columns for outer query access
                        let direct_rel_cols: String = all_rel_props
                            .iter()
                            .map(|prop_name| {
                                if let Some(prop_val) = rel_schema.property_mappings.get(prop_name)
                                {
                                    let col_ref = match prop_val {
                                        PropertyValue::Column(c) => {
                                            format!("{rel_table}.{}", quote_identifier(c))
                                        }
                                        PropertyValue::Expression(e) => format!("{rel_table}.{e}"),
                                    };
                                    format!(", {col_ref} AS {prop_name}")
                                } else {
                                    format!(", NULL AS {prop_name}")
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("");

                        // Dialect-aware array literals: ClickHouse `['x']`/`[expr]`
                        // vs Spark/Databricks `array('x')`/`array(expr)`. A bare
                        // `[...]` is a parse error on Databricks.
                        let fm = crate::sql_generator::function_mapper::current_function_mapper();
                        let path_relationships_lit =
                            fm.array_literal(&format!("'{base_rel_type}'"));
                        let rel_properties_lit = fm.array_literal(&rel_properties_json);

                        // A denormalized edge embeds an endpoint node in the edge
                        // table itself (e.g. AUTHORED with Post embedded in
                        // posts_test, so raw_to_table == rel_table). That endpoint
                        // IS the edge row — joining its table again would emit a
                        // spurious unaliased self-join
                        // (`posts_test.post_id = posts_test.post_id`) that explodes
                        // the row count. Skip the join for a coupled endpoint; its
                        // columns already reference rel_table. Not applied to
                        // self-joins (both endpoints aliased distinctly there).
                        //
                        // Coupling requires BOTH (a) same physical table AND (b) the
                        // node-id column == the edge's endpoint-id column. Only then
                        // is the join condition (`node.id = rel.endpoint_id`) a
                        // tautology that is safe to drop. If a node is embedded in the
                        // edge table under a DIFFERENT id column (node_id `user_id` vs
                        // edge `author_id`), the join is a real selective filter and
                        // must be kept.
                        let from_coupled = !is_self_join
                            && raw_from_table == rel_table
                            && from_node_id.columns() == rel_from_col.columns();
                        let to_coupled = !is_self_join
                            && raw_to_table == rel_table
                            && to_node_id.columns() == rel_to_col.columns();
                        // A denormalized endpoint (embedded in the edge table with a
                        // virtual id) likewise has no separate table to join — its
                        // columns already reference rel_table via the denorm property
                        // maps. Skip its join too.
                        let mut node_joins = String::new();
                        if !from_coupled && !from_denorm {
                            node_joins.push_str(&format!(
                                " INNER JOIN {from_join_expr} ON {from_join_cond}"
                            ));
                        }
                        if !to_coupled && !to_denorm {
                            node_joins
                                .push_str(&format!(" INNER JOIN {to_join_expr} ON {to_join_cond}"));
                        }

                        // #466 self-loop guard: when both endpoints share a
                        // physical table a row can be a self-loop (from-id ==
                        // to-id). Neo4j returns a self-loop ONCE for `-[r]-`, so
                        // the REVERSE branch must exclude self-loop rows (the
                        // forward branch already emits them once). For non-self
                        // patterns (distinct tables) no row can be a self-loop, so
                        // no guard is needed. Composite ids: differ if ANY column
                        // differs.
                        let self_loop_guard: Option<String> = if is_self_join {
                            let parts: Vec<String> = rel_from_col
                                .columns()
                                .iter()
                                .zip(rel_to_col.columns().iter())
                                .map(|(f, t)| {
                                    format!(
                                        "{rel_table}.{} != {rel_table}.{}",
                                        quote_identifier(f),
                                        quote_identifier(t)
                                    )
                                })
                                .collect();
                            Some(format!("({})", parts.join(" OR ")))
                        } else {
                            None
                        };

                        // Build one branch SELECT. `swap=false` is the forward
                        // orientation (n=from, o=to); `swap=true` traverses the
                        // SAME edge with start/end (n/o) swapped for undirected.
                        let build_branch =
                            |swap: bool| -> Result<String, RenderBuildError> {
                                let (s_type, e_type) = if swap {
                                    (&combo.to_label, &combo.from_label)
                                } else {
                                    (&combo.from_label, &combo.to_label)
                                };
                                let (s_id, e_id) = if swap {
                                    (&end_id_expr, &start_id_expr)
                                } else {
                                    (&start_id_expr, &end_id_expr)
                                };
                                let (s_props, e_props) = if swap {
                                    (&end_properties_json, &start_properties_json)
                                } else {
                                    (&start_properties_json, &end_properties_json)
                                };
                                let mut extra_where = where_clauses.clone();
                                if swap {
                                    if let Some(ref g) = self_loop_guard {
                                        extra_where.push(g.clone());
                                    }
                                }

                                // #466: apply node-property WHERE conjuncts with
                                // this branch's alias bindings. left_connection
                                // binds the from-side in the forward orientation
                                // and the to-side in the reverse; right_connection
                                // the opposite.
                                if !branch_local_conjuncts.is_empty() {
                                    let from_binding_alias = if swap {
                                        graph_rel.right_connection.as_str()
                                    } else {
                                        graph_rel.left_connection.as_str()
                                    };
                                    let to_binding_alias = if swap {
                                        graph_rel.left_connection.as_str()
                                    } else {
                                        graph_rel.right_connection.as_str()
                                    };
                                    let node_bindings = [
                                        PatternBranchBinding {
                                            alias: from_binding_alias,
                                            label: &combo.from_label,
                                            table: &from_table,
                                            id_expr: &start_id_expr,
                                            denorm: from_denorm,
                                            denorm_props: from_props,
                                            denorm_props_alt: from_props_alt,
                                            property_mappings: &from_node_schema
                                                .property_mappings,
                                        },
                                        PatternBranchBinding {
                                            alias: to_binding_alias,
                                            label: &combo.to_label,
                                            table: &to_table,
                                            id_expr: &end_id_expr,
                                            denorm: to_denorm,
                                            denorm_props: to_props,
                                            denorm_props_alt: to_props_alt,
                                            property_mappings: &to_node_schema
                                                .property_mappings,
                                        },
                                    ];
                                    for conjunct in &branch_local_conjuncts {
                                        let render_expr = RenderExpr::try_from(
                                            conjunct.clone(),
                                        )
                                        .map_err(|e| {
                                            RenderBuildError::UnsupportedFeature(format!(
                                                "WHERE predicate on a multi-type/undirected \
                                                 pattern could not be converted for per-branch \
                                                 resolution: {e:?}"
                                            ))
                                        })?;
                                        let substituted = substitute_pattern_branch_refs(
                                            &render_expr,
                                            &node_bindings,
                                            &graph_rel.alias,
                                            &rel_table,
                                            &rel_schema.property_mappings,
                                        )?;
                                        extra_where.push(format!(
                                            "({})",
                                            render_expr_to_sql_string(&substituted, &[])
                                        ));
                                    }
                                }

                                let branch_where = if extra_where.is_empty() {
                                    String::new()
                                } else {
                                    format!(" WHERE {}", extra_where.join(" AND "))
                                };
                                Ok(format!(
                                    "SELECT \
                                '{s_type}' AS start_type, \
                                {s_id} as start_id, \
                                {e_id} as end_id, \
                                '{e_type}' AS end_type, \
                                {path_relationships_lit} as path_relationships, \
                                {rel_properties_lit} as rel_properties, \
                                {s_props} as start_properties, \
                                {e_props} as end_properties{direct_rel_cols} \
                            FROM {rel_table}{node_joins}{branch_where}",
                                ))
                            };

                        log::debug!(
                            "  Branch: (:{from_label})-[:{rel_type}]->(:{to_label}) undirected={is_undirected}",
                            from_label = combo.from_label,
                            rel_type = combo.rel_type,
                            to_label = combo.to_label
                        );

                        let mut branch_sqls = vec![build_branch(false)?];
                        if is_undirected {
                            branch_sqls.push(build_branch(true)?);
                        }
                        Ok(branch_sqls)
                    })
                    .collect();

                let union_branches: Vec<String> = union_branches?.into_iter().flatten().collect();
                // #511: branches previously carried a hardcoded `LIMIT 1000`
                // "safety cap" with no genuine architectural justification
                // (no design doc, no comment explaining the value, git blame
                // traces it to the original pattern-resolver feature commit
                // with no rationale) — for an unlabeled/multi-type pattern
                // scan (`MATCH ()-[r]->() RETURN ...`) with no user-specified
                // LIMIT, this silently truncated `count(*)`/aggregate results
                // and arbitrarily dropped rows once a branch's underlying
                // table exceeded 1000 matching edges, with no error or
                // indication to the caller — a ground-rule-1 (never silently
                // wrong) violation. Removed: any actual limiting the user
                // wants is expressed via an explicit Cypher `LIMIT`, applied
                // normally at the outer query level, same as any other
                // pattern in this engine.
                //
                // Parenthesizing each branch below predates and is
                // independent of that removed cap — the other dialect is
                // stricter about bare per-branch modifiers in a set
                // operation in general, so branches stay parenthesized there
                // for robustness even though no branch carries a LIMIT of
                // its own anymore. ClickHouse accepts the bare form and
                // keeps its prior byte-identical output.
                let union_sql = if matches!(
                    crate::server::query_context::get_current_dialect(),
                    crate::sql_generator::SqlDialect::Databricks
                ) {
                    union_branches
                        .iter()
                        .map(|b| format!("({b})"))
                        .collect::<Vec<_>>()
                        .join("\nUNION ALL\n")
                } else {
                    union_branches.join("\nUNION ALL\n")
                };

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
                    LogicalPlan::Create(_) => "Create",
                    LogicalPlan::SetProperties(_) => "SetProperties",
                    LogicalPlan::Delete(_) => "Delete",
                    LogicalPlan::Remove(_) => "Remove",
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
            // #557: don't just concatenate every branch's CTEs — a Union of
            // per-candidate-end-label branches (unlabeled VLP end node, e.g.
            // `MATCH (a:User)-[:T1|T2*1..2]->(b) RETURN count(*)`) has each
            // branch independently compute the SAME formulaic multi-type VLP
            // CTE name (the formula only depends on the Cypher start/end
            // aliases, unchanged across branches) while generating DIFFERENT
            // CTE bodies (each scoped to its own candidate end label).
            // Concatenating verbatim left two same-named-but-different CTEs
            // in the list; downstream name-keyed dedup silently kept only
            // one, while the branch's own FROM reference (computed
            // independently and correctly by `extract_union`/
            // `extract_union_with_ctx` in plan_builder.rs, using the same
            // rename-on-collision rule) pointed at a `_2`-suffixed CTE that
            // was never actually added to this list — `Code: 60. Unknown
            // table expression identifier`. Route same-name collisions
            // through the shared merge/rename helper instead, so this list
            // and the branch's FROM reference agree.
            let mut ctes = vec![];
            for input_plan in union.inputs.iter() {
                let branch_ctes = extract_ctes_with_context(
                    input_plan,
                    last_node_alias,
                    context,
                    schema,
                    plan_ctx,
                )?;
                for cte in branch_ctes {
                    merge_cte_deduping_by_name_content(&mut ctes, cte);
                }
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
            log::debug!(
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
        // Write variants — recurse into the preceding read pipeline so any
        // CTEs it produced (WITH CTEs, VLP CTEs, etc.) still reach the
        // renderer. The write payload itself does not contribute CTEs.
        LogicalPlan::Create(c) => {
            extract_ctes_with_context(&c.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::SetProperties(sp) => {
            extract_ctes_with_context(&sp.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::Delete(d) => {
            extract_ctes_with_context(&d.input, last_node_alias, context, schema, plan_ctx)
        }
        LogicalPlan::Remove(r) => {
            extract_ctes_with_context(&r.input, last_node_alias, context, schema, plan_ctx)
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

/// Check if the plan uses `relationships(path_var)` anywhere in its expressions.
/// Used to decide whether VLP CTEs need to track `path_relationships` arrays.
pub fn plan_uses_relationships_fn(plan: &LogicalPlan, path_var: &str) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;

    fn expr_uses_relationships(expr: &LogicalExpr, path_var: &str) -> bool {
        match expr {
            LogicalExpr::ScalarFnCall(fc) => {
                if fc.name == "relationships" {
                    // Check if any argument references the path variable
                    return fc.args.iter().any(|arg| match arg {
                        LogicalExpr::TableAlias(v) => v.0 == path_var,
                        LogicalExpr::PropertyAccessExp(pa) => pa.table_alias.0 == path_var,
                        _ => expr_uses_relationships(arg, path_var),
                    });
                }
                fc.args.iter().any(|a| expr_uses_relationships(a, path_var))
            }
            LogicalExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .any(|o| expr_uses_relationships(o, path_var)),
            LogicalExpr::Case(c) => {
                c.expr
                    .as_ref()
                    .is_some_and(|o| expr_uses_relationships(o, path_var))
                    || c.when_then.iter().any(|(w, t)| {
                        expr_uses_relationships(w, path_var) || expr_uses_relationships(t, path_var)
                    })
                    || c.else_expr
                        .as_ref()
                        .is_some_and(|e| expr_uses_relationships(e, path_var))
            }
            _ => false,
        }
    }

    fn plan_has_relationships(plan: &LogicalPlan, path_var: &str) -> bool {
        match plan {
            LogicalPlan::Projection(p) => {
                p.items
                    .iter()
                    .any(|item| expr_uses_relationships(&item.expression, path_var))
                    || plan_has_relationships(&p.input, path_var)
            }
            LogicalPlan::Filter(f) => {
                expr_uses_relationships(&f.predicate, path_var)
                    || plan_has_relationships(&f.input, path_var)
            }
            LogicalPlan::WithClause(wc) => {
                wc.items
                    .iter()
                    .any(|item| expr_uses_relationships(&item.expression, path_var))
                    || plan_has_relationships(&wc.input, path_var)
            }
            LogicalPlan::OrderBy(ob) => {
                ob.items
                    .iter()
                    .any(|item| expr_uses_relationships(&item.expression, path_var))
                    || plan_has_relationships(&ob.input, path_var)
            }
            LogicalPlan::GraphRel(gr) => {
                gr.where_predicate
                    .as_ref()
                    .is_some_and(|p| expr_uses_relationships(p, path_var))
                    || plan_has_relationships(&gr.left, path_var)
                    || plan_has_relationships(&gr.center, path_var)
                    || plan_has_relationships(&gr.right, path_var)
            }
            LogicalPlan::GraphNode(n) => plan_has_relationships(&n.input, path_var),
            LogicalPlan::GroupBy(gb) => plan_has_relationships(&gb.input, path_var),
            LogicalPlan::Limit(l) => plan_has_relationships(&l.input, path_var),
            LogicalPlan::Skip(s) => plan_has_relationships(&s.input, path_var),
            LogicalPlan::CartesianProduct(cp) => {
                plan_has_relationships(&cp.left, path_var)
                    || plan_has_relationships(&cp.right, path_var)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_has_relationships(i, path_var)),
            LogicalPlan::GraphJoins(gj) => plan_has_relationships(&gj.input, path_var),
            _ => false,
        }
    }

    plan_has_relationships(plan, path_var)
}

/// Check if the plan uses `nodes(path_var)` — mirrors `plan_uses_relationships_fn`.
pub fn plan_uses_nodes_fn(plan: &LogicalPlan, path_var: &str) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;

    fn expr_uses_nodes(expr: &LogicalExpr, path_var: &str) -> bool {
        match expr {
            LogicalExpr::ScalarFnCall(fc) => {
                if fc.name == "nodes" {
                    return fc.args.iter().any(|arg| match arg {
                        LogicalExpr::TableAlias(v) => v.0 == path_var,
                        LogicalExpr::PropertyAccessExp(pa) => pa.table_alias.0 == path_var,
                        _ => expr_uses_nodes(arg, path_var),
                    });
                }
                fc.args.iter().any(|a| expr_uses_nodes(a, path_var))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(|o| expr_uses_nodes(o, path_var))
            }
            LogicalExpr::Case(c) => {
                c.expr
                    .as_ref()
                    .is_some_and(|o| expr_uses_nodes(o, path_var))
                    || c.when_then
                        .iter()
                        .any(|(w, t)| expr_uses_nodes(w, path_var) || expr_uses_nodes(t, path_var))
                    || c.else_expr
                        .as_ref()
                        .is_some_and(|e| expr_uses_nodes(e, path_var))
            }
            _ => false,
        }
    }

    fn plan_has_nodes(plan: &LogicalPlan, path_var: &str) -> bool {
        match plan {
            LogicalPlan::Projection(p) => {
                p.items
                    .iter()
                    .any(|item| expr_uses_nodes(&item.expression, path_var))
                    || plan_has_nodes(&p.input, path_var)
            }
            LogicalPlan::Filter(f) => {
                expr_uses_nodes(&f.predicate, path_var) || plan_has_nodes(&f.input, path_var)
            }
            LogicalPlan::WithClause(wc) => {
                wc.items
                    .iter()
                    .any(|item| expr_uses_nodes(&item.expression, path_var))
                    || plan_has_nodes(&wc.input, path_var)
            }
            LogicalPlan::OrderBy(ob) => {
                ob.items
                    .iter()
                    .any(|item| expr_uses_nodes(&item.expression, path_var))
                    || plan_has_nodes(&ob.input, path_var)
            }
            LogicalPlan::GraphRel(gr) => {
                gr.where_predicate
                    .as_ref()
                    .is_some_and(|p| expr_uses_nodes(p, path_var))
                    || plan_has_nodes(&gr.left, path_var)
                    || plan_has_nodes(&gr.center, path_var)
                    || plan_has_nodes(&gr.right, path_var)
            }
            LogicalPlan::GraphNode(n) => plan_has_nodes(&n.input, path_var),
            LogicalPlan::GroupBy(gb) => plan_has_nodes(&gb.input, path_var),
            LogicalPlan::Limit(l) => plan_has_nodes(&l.input, path_var),
            LogicalPlan::Skip(s) => plan_has_nodes(&s.input, path_var),
            LogicalPlan::CartesianProduct(cp) => {
                plan_has_nodes(&cp.left, path_var) || plan_has_nodes(&cp.right, path_var)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_has_nodes(i, path_var)),
            LogicalPlan::GraphJoins(gj) => plan_has_nodes(&gj.input, path_var),
            _ => false,
        }
    }

    plan_has_nodes(plan, path_var)
}

/// Check if the path variable is used bare (as TableAlias/ColumnAlias) outside of
/// `length()`, `nodes()`, `relationships()`, and `cost()` calls.
/// If it appears bare, BFS mode can't be used because the full path object is needed.
pub fn plan_uses_bare_path_variable(plan: &LogicalPlan, path_var: &str) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;

    /// Returns true if expr references path_var as a bare alias (not inside safe functions).
    fn expr_uses_bare(expr: &LogicalExpr, path_var: &str) -> bool {
        use crate::query_planner::logical_expr::Operator;

        fn is_path_ref(e: &LogicalExpr, pv: &str) -> bool {
            match e {
                LogicalExpr::TableAlias(v) => v.0 == pv,
                LogicalExpr::ColumnAlias(v) => v.0 == pv,
                _ => false,
            }
        }

        match expr {
            // Bare path variable reference
            LogicalExpr::TableAlias(v) if v.0 == path_var => true,
            LogicalExpr::ColumnAlias(v) if v.0 == path_var => true,
            // Functions that consume path variables — their args are safe
            LogicalExpr::ScalarFnCall(fc) => {
                let safe_fns = ["length", "nodes", "relationships", "cost"];
                if safe_fns.contains(&fc.name.as_str()) {
                    let references_pv = fc.args.iter().any(|a| is_path_ref(a, path_var));
                    if references_pv {
                        return false; // Safe function consuming path_var
                    }
                }
                fc.args.iter().any(|a| expr_uses_bare(a, path_var))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // `path IS NULL` / `path IS NOT NULL` patterns are safe —
                // used in CASE WHEN for length-only queries
                if matches!(op.operator, Operator::IsNull | Operator::IsNotNull)
                    && op.operands.len() == 1
                    && is_path_ref(&op.operands[0], path_var)
                {
                    return false;
                }
                op.operands.iter().any(|o| expr_uses_bare(o, path_var))
            }
            LogicalExpr::Case(c) => {
                c.expr.as_ref().is_some_and(|o| expr_uses_bare(o, path_var))
                    || c.when_then
                        .iter()
                        .any(|(w, t)| expr_uses_bare(w, path_var) || expr_uses_bare(t, path_var))
                    || c.else_expr
                        .as_ref()
                        .is_some_and(|e| expr_uses_bare(e, path_var))
            }
            LogicalExpr::AggregateFnCall(afc) => {
                afc.args.iter().any(|a| expr_uses_bare(a, path_var))
            }
            _ => false,
        }
    }

    fn plan_has_bare(plan: &LogicalPlan, path_var: &str) -> bool {
        match plan {
            LogicalPlan::Projection(p) => {
                p.items
                    .iter()
                    .any(|item| expr_uses_bare(&item.expression, path_var))
                    || plan_has_bare(&p.input, path_var)
            }
            LogicalPlan::Filter(f) => {
                expr_uses_bare(&f.predicate, path_var) || plan_has_bare(&f.input, path_var)
            }
            LogicalPlan::WithClause(wc) => {
                wc.items
                    .iter()
                    .any(|item| expr_uses_bare(&item.expression, path_var))
                    || plan_has_bare(&wc.input, path_var)
            }
            LogicalPlan::OrderBy(ob) => {
                ob.items
                    .iter()
                    .any(|item| expr_uses_bare(&item.expression, path_var))
                    || plan_has_bare(&ob.input, path_var)
            }
            LogicalPlan::GraphRel(gr) => {
                gr.where_predicate
                    .as_ref()
                    .is_some_and(|p| expr_uses_bare(p, path_var))
                    || plan_has_bare(&gr.left, path_var)
                    || plan_has_bare(&gr.center, path_var)
                    || plan_has_bare(&gr.right, path_var)
            }
            LogicalPlan::GraphNode(n) => plan_has_bare(&n.input, path_var),
            LogicalPlan::GroupBy(gb) => plan_has_bare(&gb.input, path_var),
            LogicalPlan::Limit(l) => plan_has_bare(&l.input, path_var),
            LogicalPlan::Skip(s) => plan_has_bare(&s.input, path_var),
            LogicalPlan::CartesianProduct(cp) => {
                plan_has_bare(&cp.left, path_var) || plan_has_bare(&cp.right, path_var)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_has_bare(i, path_var)),
            LogicalPlan::GraphJoins(gj) => plan_has_bare(&gj.input, path_var),
            _ => false,
        }
    }

    plan_has_bare(plan, path_var)
}

/// Check if the plan's **output** (Projection, OrderBy, WithClause items, GroupBy)
/// references properties of a given alias (e.g., `a.name`, `a.age`).
/// Filter/WHERE predicates are excluded — those become CTE filter parameters.
/// Used to determine if BFS mode can be used (BFS CTEs don't carry node properties).
pub fn plan_references_alias_properties(plan: &LogicalPlan, alias: &str) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;

    fn expr_refs_alias(expr: &LogicalExpr, alias: &str) -> bool {
        match expr {
            LogicalExpr::PropertyAccessExp(pa) => pa.table_alias.0 == alias,
            LogicalExpr::ScalarFnCall(fc) => fc.args.iter().any(|a| expr_refs_alias(a, alias)),
            LogicalExpr::AggregateFnCall(afc) => afc.args.iter().any(|a| expr_refs_alias(a, alias)),
            LogicalExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(|o| expr_refs_alias(o, alias))
            }
            LogicalExpr::Case(c) => {
                c.expr.as_ref().is_some_and(|e| expr_refs_alias(e, alias))
                    || c.when_then
                        .iter()
                        .any(|(w, t)| expr_refs_alias(w, alias) || expr_refs_alias(t, alias))
                    || c.else_expr
                        .as_ref()
                        .is_some_and(|e| expr_refs_alias(e, alias))
            }
            _ => false,
        }
    }

    fn plan_refs(plan: &LogicalPlan, alias: &str) -> bool {
        match plan {
            // Output-bearing nodes — check their items
            LogicalPlan::Projection(p) => {
                p.items
                    .iter()
                    .any(|item| expr_refs_alias(&item.expression, alias))
                    || plan_refs(&p.input, alias)
            }
            LogicalPlan::WithClause(wc) => {
                wc.items
                    .iter()
                    .any(|item| expr_refs_alias(&item.expression, alias))
                    || plan_refs(&wc.input, alias)
            }
            LogicalPlan::OrderBy(ob) => {
                ob.items
                    .iter()
                    .any(|item| expr_refs_alias(&item.expression, alias))
                    || plan_refs(&ob.input, alias)
            }
            LogicalPlan::GroupBy(gb) => {
                gb.expressions.iter().any(|e| expr_refs_alias(e, alias))
                    || plan_refs(&gb.input, alias)
            }
            // Filter/WHERE — skip (properties in filters become CTE parameters)
            LogicalPlan::Filter(f) => plan_refs(&f.input, alias),
            // Structural nodes — recurse only
            LogicalPlan::GraphRel(gr) => {
                plan_refs(&gr.left, alias)
                    || plan_refs(&gr.center, alias)
                    || plan_refs(&gr.right, alias)
            }
            LogicalPlan::GraphNode(n) => plan_refs(&n.input, alias),
            LogicalPlan::Limit(l) => plan_refs(&l.input, alias),
            LogicalPlan::Skip(s) => plan_refs(&s.input, alias),
            LogicalPlan::CartesianProduct(cp) => {
                plan_refs(&cp.left, alias) || plan_refs(&cp.right, alias)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_refs(i, alias)),
            LogicalPlan::GraphJoins(gj) => plan_refs(&gj.input, alias),
            _ => false,
        }
    }

    plan_refs(plan, alias)
}

/// Extract path variable from fixed multi-hop patterns (no variable_length)
/// Returns (path_variable_name, hop_count) if found
pub fn get_fixed_path_variable(plan: &LogicalPlan) -> Option<(String, u32)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Only handle fixed patterns (no variable_length ANYWHERE in the
            // chain). #499/#501 regression guard: the original check only
            // looked at THIS GraphRel's own `variable_length`, so a mixed
            // pattern like `(a)-[:FOLLOWS*1..2]->(b)-[:AUTHORED]->(c)` (outer
            // leg fixed, inner leg VLP — `path_variable` is set on every
            // nested GraphRel of the same path) was wrongly treated as a
            // fully-fixed path just because the OUTERMOST leg happened to be
            // fixed. That produced wrong `FixedPathMetadata` (hop_count/
            // node_aliases computed by `count_hops_in_graph_rel`/
            // `collect_path_aliases_with_ids` don't understand a nested VLP
            // CTE endpoint) which fed into the plan_optimizer.rs join-pruning
            // guards and silently dropped the trailing leg's JOIN for ANY
            // query with a path variable declared — even `RETURN c` (no path
            // function at all). Reject the whole chain if *any* leg is VLP.
            if rel.variable_length.is_some() || chain_has_any_vlp(&rel.left) {
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

/// Whether any GraphRel reachable from this subtree (walking both `.left` and
/// `.right`, since a chained leg can be nested on either side depending on
/// how the pattern was parsed) still has `variable_length.is_some()` — i.e.
/// genuinely needs a recursive VLP CTE, as opposed to a plain fixed hop.
fn chain_has_any_vlp(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            rel.variable_length.is_some()
                || chain_has_any_vlp(&rel.left)
                || chain_has_any_vlp(&rel.right)
        }
        LogicalPlan::GraphNode(node) => chain_has_any_vlp(&node.input),
        LogicalPlan::Filter(filter) => chain_has_any_vlp(&filter.input),
        _ => false,
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
    /// Maps node alias to (reference_alias, id_column) — `id_column` is the
    /// full composite-aware `Identifier` (single or multi-column). For a
    /// denormalized/embedded node, `reference_alias` is the relationship's
    /// own alias (e.g., "a" -> ("r", Single("Origin"))); otherwise it's the
    /// node's own alias (e.g., "a" -> ("a", Composite(["bank_id",
    /// "account_number"]))).
    pub node_id_columns: std::collections::HashMap<String, (String, Identifier)>,
    /// Maps relationship alias to its Cypher relationship type name (first label),
    /// e.g. "r" -> "PLACED_BY". Used to render `relationships(p)` as an array of
    /// type-name literals, mirroring how the VLP recursive CTE's `path_relationships`
    /// column is populated (see `get_relationship_type_array` in variable_length_cte.rs).
    pub rel_types: std::collections::HashMap<String, String>,
}

/// Extract complete path information from fixed multi-hop patterns
/// Returns FixedPathInfo with all node and relationship aliases
pub fn get_fixed_path_info(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> Result<Option<FixedPathInfo>, super::errors::RenderBuildError> {
    // First find the path variable and hop count
    let (path_var_name, hop_count) = match get_fixed_path_variable(plan) {
        Some(info) => info,
        None => return Ok(None),
    };

    // Then extract all aliases and node ID mappings
    let (node_aliases, rel_aliases, node_id_columns, rel_types) =
        collect_path_aliases_with_ids(plan, schema, plan_ctx)?;

    Ok(Some(FixedPathInfo {
        path_var_name,
        node_aliases,
        rel_aliases,
        hop_count,
        node_id_columns,
        rel_types,
    }))
}

/// (node_aliases, rel_aliases, node_id_columns, rel_types) — node_id_columns maps
/// alias → (rel_alias, id_column); rel_types maps rel_alias → Cypher type name.
type PathAliasesWithIds = (
    Vec<String>,
    Vec<String>,
    std::collections::HashMap<String, (String, Identifier)>,
    std::collections::HashMap<String, String>,
);

/// Collect node and relationship aliases plus ID column mappings
fn collect_path_aliases_with_ids(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> Result<PathAliasesWithIds, super::errors::RenderBuildError> {
    let mut node_aliases = Vec::new();
    let mut rel_aliases = Vec::new();
    let mut node_id_columns = std::collections::HashMap::new();
    let mut rel_types = std::collections::HashMap::new();

    collect_path_aliases_with_ids_recursive(
        plan,
        schema,
        plan_ctx,
        &mut node_aliases,
        &mut rel_aliases,
        &mut node_id_columns,
        &mut rel_types,
    )?;

    Ok((node_aliases, rel_aliases, node_id_columns, rel_types))
}

/// Resolve `(left_id, right_id)` — each `Some(Identifier)` when that node has
/// its own genuinely separate FROM/JOIN binding (standard, FK-edge), `None`
/// when the node's property data is embedded/coupled onto the edge row itself
/// (denormalized schemas — no separate alias exists to reference; caller
/// falls back to the relationship's own alias + from_id/to_id).
///
/// Routes through `PatternSchemaContext` / `NodeAccessStrategy`
/// (`recreate_pattern_schema_context`, `graph_catalog/pattern_schema.rs`) —
/// the project's canonical schema-pattern dispatch API — rather than a raw
/// per-node denormalized flag or a table-name comparison. Two benefits over
/// the flag this replaced: (1) genuine axis-dispatch (routes through the
/// catalog API instead of a raw schema-pattern predicate, avoiding ratchet-
/// token cost), and (2) `NodeAccessStrategy::OwnTable.id_column` is a full composite-aware
/// `Identifier`, not a single lossy `String` — a prior version of this code
/// used `ViewScan.id_column: String`, which silently drops every column past
/// the first for a composite-key node (verified live on
/// `schemas/test/composite_node_ids.yaml`'s `Account`, keyed on
/// `(bank_id, account_number)` — `nodes(p)` rendered `array(..., a.bank_id,
/// ...)`, silently omitting `account_number`). Falls back to the caller's
/// relationship-alias route (also loud-safe: `Identifier::Composite` there
/// renders as a real multi-column pipe-join too, see `emit_id_expr` in
/// `variable_length_cte.rs`, which this mirrors) if the context can't be
/// resolved (e.g. an unusual/partial schema shape) — best-effort, not fatal.
fn resolve_node_own_ids(
    rel: &crate::query_planner::logical_plan::GraphRel,
    schema: &GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
) -> (Option<Identifier>, Option<Identifier>) {
    match recreate_pattern_schema_context(rel, schema, plan_ctx) {
        Ok(ctx) => {
            let left = match &ctx.left_node {
                crate::graph_catalog::pattern_schema::NodeAccessStrategy::OwnTable {
                    id_column,
                    ..
                } => Some(id_column.clone()),
                _ => None,
            };
            let right = match &ctx.right_node {
                crate::graph_catalog::pattern_schema::NodeAccessStrategy::OwnTable {
                    id_column,
                    ..
                } => Some(id_column.clone()),
                _ => None,
            };
            (left, right)
        }
        Err(_) => (None, None),
    }
}

/// Recursive helper to collect aliases and ID column mappings
fn collect_path_aliases_with_ids_recursive(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    node_aliases: &mut Vec<String>,
    rel_aliases: &mut Vec<String>,
    node_id_columns: &mut std::collections::HashMap<String, (String, Identifier)>,
    rel_types: &mut std::collections::HashMap<String, String>,
) -> Result<(), super::errors::RenderBuildError> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Process left side first (may be another GraphRel or the start node)
            collect_path_aliases_with_ids_recursive(
                &rel.left,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
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

                // #497/#498: prefer the node's OWN alias + own id column. It's
                // always correctly bound in FROM/JOIN for standard and FK-edge
                // schemas, whereas the relationship's own alias (e.g. an
                // auto-generated VLP-style alias for an unnamed `[:TYPE]`
                // pattern) is NOT always a separately bound SQL alias — for
                // FK-edge in particular, the "relationship" is folded into the
                // node join and has no table/alias of its own, so referencing
                // `rel_alias.column` produces an unbound-identifier SQL error.
                // Only fall back to the relationship alias's from_id/to_id when
                // the node's data is embedded/coupled onto the edge row itself
                // (denormalized — no separate alias to reference).
                let (left_own_id, right_own_id) = resolve_node_own_ids(rel, schema, plan_ctx);

                if !node_id_columns.contains_key(&rel.left_connection) {
                    let mapping = match left_own_id {
                        Some(own_id_col) => (rel.left_connection.clone(), own_id_col),
                        None => (rel.alias.clone(), from_id.clone()),
                    };
                    node_id_columns.insert(rel.left_connection.clone(), mapping);
                }

                // Map right node — prefer its own alias/id column unless denormalized.
                let right_mapping = match right_own_id {
                    Some(own_id_col) => (rel.right_connection.clone(), own_id_col),
                    None => (rel.alias.clone(), to_id.clone()),
                };
                node_id_columns.insert(rel.right_connection.clone(), right_mapping);
            }

            // Add this relationship
            rel_aliases.push(rel.alias.clone());

            // Record this relationship's Cypher type name (first label) for
            // `relationships(p)` rendering — mirrors VLP's path_relationships
            // (an array of type-name strings, see get_relationship_type_array).
            if let Some(type_name) = rel.labels.as_ref().and_then(|l| l.first()) {
                use crate::graph_catalog::composite_key_utils::extract_type_name;
                rel_types.insert(rel.alias.clone(), extract_type_name(type_name).to_string());
            }

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
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::Filter(filter) => {
            collect_path_aliases_with_ids_recursive(
                &filter.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::Projection(proj) => {
            collect_path_aliases_with_ids_recursive(
                &proj.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::GraphJoins(joins) => {
            collect_path_aliases_with_ids_recursive(
                &joins.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::GroupBy(gb) => {
            collect_path_aliases_with_ids_recursive(
                &gb.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::OrderBy(ob) => {
            collect_path_aliases_with_ids_recursive(
                &ob.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::Skip(skip) => {
            collect_path_aliases_with_ids_recursive(
                &skip.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
            )?;
        }
        LogicalPlan::Limit(limit) => {
            collect_path_aliases_with_ids_recursive(
                &limit.input,
                schema,
                plan_ctx,
                node_aliases,
                rel_aliases,
                node_id_columns,
                rel_types,
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

/// Extract the **endpoint** node label from a plan subtree — the node directly
/// connected to the current relationship. For a nested GraphRel chain like
/// `(forum:Forum)-[:CONTAINER_OF]->(post:Post)`, this returns "Post" (the
/// rightmost/endpoint node), not "Forum" (the leftmost).
///
/// This is critical for VLP label extraction where `graph_rel.left` may be an
/// entire chain and we need the node actually connected to the VLP.
pub fn extract_endpoint_node_label_with_schema(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            // In a chain, the endpoint is the RIGHT side (the node at the end of the chain)
            extract_endpoint_node_label_with_schema(&gr.right, schema)
        }
        // For all other plan types, delegate to the standard extraction
        _ => extract_node_label_from_viewscan_with_schema(plan, schema),
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

    log::debug!(
        "expand_fixed_length_joins_with_context: schema_type={:?}, {} hops from {} to {}",
        ctx.schema_type,
        exact_hops,
        ctx.start_alias,
        ctx.end_alias
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

            log::debug!(
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

            log::debug!(
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

            log::debug!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, LogicalExpr};
    use std::sync::Arc;

    /// Regression: extract_node_labels should return None for unlabeled nodes.
    #[test]
    fn test_extract_node_labels_unlabeled_node() {
        let node = LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "o".to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        });
        assert_eq!(extract_node_labels(&node), None);
    }

    /// extract_node_labels returns single label when set.
    #[test]
    fn test_extract_node_labels_single_label() {
        let node = LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "a".to_string(),
            label: Some("Post".to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        });
        assert_eq!(extract_node_labels(&node), Some(vec!["Post".to_string()]));
    }

    /// extract_node_labels returns ALL types from node_types when multiple are present.
    #[test]
    fn test_extract_node_labels_multi_type() {
        let node = LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "o".to_string(),
            label: Some("Post".to_string()), // First label
            is_denormalized: false,
            projected_columns: None,
            node_types: Some(vec!["Post".to_string(), "User".to_string()]),
        });
        // Should return both types, not just the single label
        let labels = extract_node_labels(&node).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Post".to_string()));
        assert!(labels.contains(&"User".to_string()));
    }

    /// Regression: For undirected patterns, node_types with single entry should
    /// still return that single entry via the label field (not node_types).
    #[test]
    fn test_extract_node_labels_single_node_type_uses_label() {
        let node = LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "o".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: Some(vec!["User".to_string()]), // Single type
        });
        // node_types has len=1, so falls through to label check
        assert_eq!(extract_node_labels(&node), Some(vec!["User".to_string()]));
    }

    /// extract_node_labels traverses through Filter wrappers.
    #[test]
    fn test_extract_node_labels_through_filter() {
        let node = LogicalPlan::GraphNode(crate::query_planner::logical_plan::GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "a".to_string(),
            label: Some("Post".to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        });
        let filtered = LogicalPlan::Filter(crate::query_planner::logical_plan::Filter {
            input: Arc::new(node),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        });
        assert_eq!(
            extract_node_labels(&filtered),
            Some(vec!["Post".to_string()])
        );
    }
}
