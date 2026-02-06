//! Join Builder Module
//!
//! This module handles the extraction and building of JOIN clauses for graph queries.
//! It processes logical plans to generate appropriate JOIN structures for relationships,
//! cartesian products, and array joins (UNWIND clauses).
//!
//! Key responsibilities:
//! - Extract JOIN clauses from GraphRel and GraphJoins nodes
//! - Handle different relationship types (standard, FK-edge, polymorphic)
//! - Generate cartesian product JOINs for multiple standalone nodes
//! - Process UNWIND clauses as ARRAY JOINs
//! - Build simple relationship render plans using direct JOINs

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::from_builder::FromBuilder;
use crate::render_plan::plan_builder::RenderPlanBuilderResult;
use crate::render_plan::render_expr::{
    Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::{ArrayJoin, Join, JoinType};
use std::sync::Arc;

// Helper function imports from plan_builder_helpers
use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
use crate::render_plan::cte_extraction::{
    build_vlp_context, expand_fixed_length_joins_with_context, extract_node_label_from_viewscan,
    extract_relationship_columns, get_variable_length_spec, table_to_id_column,
};
use crate::render_plan::plan_builder_helpers::{
    combine_optional_filters_with_and, extract_end_node_id_column, extract_end_node_table_name,
    extract_id_column, extract_parameterized_table_ref, extract_predicates_for_alias_logical,
    extract_rel_and_node_tables, extract_table_name, get_polymorphic_edge_filter_for_join,
    get_schema_filter_for_node, is_node_denormalized,
};
use crate::render_plan::plan_builder_utils::generate_swapped_joins_for_optional_match;
use crate::render_plan::utils::alias_utils::get_anchor_alias_from_plan;

// Additional types
use crate::render_plan::cte_extraction::RelationshipColumns;

/// Helper function to find multi-type relationship patterns in a logical plan
/// Returns the GraphRel with multiple relationship types if found
/// IMPORTANT: Excludes true VLP patterns, but includes implicit *1 for multi-type
fn find_multi_type_in_plan(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::GraphRel> {
    use crate::query_planner::logical_plan::*;
    match plan {
        LogicalPlan::GraphRel(gr) => {
            log::debug!("🔍 find_multi_type_in_plan: Found GraphRel alias={}, labels={:?}, variable_length={:?}", 
                gr.alias, gr.labels, gr.variable_length.is_some());
            // Check if this is a multi-type pattern
            // Note: Query planner adds implicit *1 for multi-type, so check for exact 1-hop
            let is_implicit_one_hop = gr
                .variable_length
                .as_ref()
                .map(|spec| spec.min_hops == Some(1) && spec.max_hops == Some(1))
                .unwrap_or(false);
            let is_no_vlp_or_implicit = gr.variable_length.is_none() || is_implicit_one_hop;

            if is_no_vlp_or_implicit {
                if let Some(ref labels) = gr.labels {
                    log::debug!("  → No VLP or implicit *1, labels.len() = {}", labels.len());
                    if labels.len() > 1 {
                        log::info!("  ✅ MULTI-TYPE MATCH: {:?}", labels);
                        return Some(gr);
                    }
                }
            }
            // Check recursively in left and right
            if let Some(multi) = find_multi_type_in_plan(&gr.left) {
                return Some(multi);
            }
            find_multi_type_in_plan(&gr.right)
        }
        LogicalPlan::Projection(proj) => find_multi_type_in_plan(&proj.input),
        LogicalPlan::Filter(filter) => find_multi_type_in_plan(&filter.input),
        LogicalPlan::GroupBy(group_by) => find_multi_type_in_plan(&group_by.input),
        LogicalPlan::GraphNode(gn) => find_multi_type_in_plan(&gn.input),
        _ => None,
    }
}

/// Join Builder trait for extracting JOIN-related information from logical plans
pub trait JoinBuilder {
    /// Extract JOIN clauses from the logical plan
    fn extract_joins(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Vec<Join>>;

    /// Extract UNWIND clauses as ARRAY JOIN items
    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<ArrayJoin>>;
}

/// Default implementation of JoinBuilder for LogicalPlan
impl JoinBuilder for LogicalPlan {
    /// Extract UNWIND clauses as ARRAY JOIN items
    /// Traverses the logical plan tree to find ALL Unwind nodes for cartesian product
    /// Multiple UNWIND clauses generate multiple ARRAY JOIN clauses in sequence
    fn extract_array_join(&self) -> RenderPlanBuilderResult<Vec<ArrayJoin>> {
        let mut array_joins = Vec::new();

        match self {
            LogicalPlan::Unwind(u) => {
                // Convert LogicalExpr to RenderExpr for this UNWIND
                let render_expr = RenderExpr::try_from(u.expression.clone())?;
                array_joins.push(ArrayJoin {
                    expression: render_expr,
                    alias: u.alias.clone(),
                });
                // Recursively collect UNWIND nodes from input
                let mut inner_joins = <LogicalPlan as JoinBuilder>::extract_array_join(&u.input)?;
                array_joins.append(&mut inner_joins);
                Ok(array_joins)
            }
            // Recursively check children for more UNWIND nodes
            LogicalPlan::Projection(p) => {
                <LogicalPlan as JoinBuilder>::extract_array_join(&p.input)
            }
            LogicalPlan::Filter(f) => <LogicalPlan as JoinBuilder>::extract_array_join(&f.input),
            LogicalPlan::GroupBy(g) => <LogicalPlan as JoinBuilder>::extract_array_join(&g.input),
            LogicalPlan::OrderBy(o) => <LogicalPlan as JoinBuilder>::extract_array_join(&o.input),
            LogicalPlan::Limit(l) => <LogicalPlan as JoinBuilder>::extract_array_join(&l.input),
            LogicalPlan::Skip(s) => <LogicalPlan as JoinBuilder>::extract_array_join(&s.input),
            LogicalPlan::GraphJoins(gj) => {
                <LogicalPlan as JoinBuilder>::extract_array_join(&gj.input)
            }
            LogicalPlan::GraphNode(gn) => {
                <LogicalPlan as JoinBuilder>::extract_array_join(&gn.input)
            }
            LogicalPlan::GraphRel(gr) => {
                // Check all branches for UNWIND nodes
                let mut joins = <LogicalPlan as JoinBuilder>::extract_array_join(&gr.center)?;
                joins.append(&mut <LogicalPlan as JoinBuilder>::extract_array_join(
                    &gr.left,
                )?);
                joins.append(&mut <LogicalPlan as JoinBuilder>::extract_array_join(
                    &gr.right,
                )?);
                Ok(joins)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn extract_joins(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<Vec<Join>> {
        println!(
            "🔧 DEBUG: extract_joins called on plan type: {:?}",
            std::mem::discriminant(self)
        );
        // Helper functions for edge constraint compilation

        // Extract relationship type and node labels from GraphRel in the plan
        fn extract_relationship_context(
            plan: &LogicalPlan,
            rel_alias: &str,
        ) -> Option<(String, String, String)> {
            match plan {
                LogicalPlan::Projection(proj) => {
                    extract_relationship_context(&proj.input, rel_alias)
                }
                LogicalPlan::Filter(filter) => {
                    extract_relationship_context(&filter.input, rel_alias)
                }
                LogicalPlan::GraphRel(gr) if gr.alias == rel_alias => {
                    let rel_type = gr.labels.as_ref()?.first()?.clone();
                    let from_label = extract_node_label(&gr.left)?;
                    let to_label = extract_node_label(&gr.right)?;
                    Some((rel_type, from_label, to_label))
                }
                LogicalPlan::GraphRel(gr) => extract_relationship_context(&gr.left, rel_alias)
                    .or_else(|| extract_relationship_context(&gr.center, rel_alias))
                    .or_else(|| extract_relationship_context(&gr.right, rel_alias)),
                LogicalPlan::GraphNode(gn) => extract_relationship_context(&gn.input, rel_alias),
                _ => None,
            }
        }

        // Extract node label from GraphNode
        fn extract_node_label(plan: &LogicalPlan) -> Option<String> {
            match plan {
                LogicalPlan::GraphNode(gn) => gn.label.clone(),
                _ => None,
            }
        }

        // Extract relationship context for FK-edge patterns
        // For FK-edge, the JOIN uses the to_node alias, so we search for GraphRel nodes
        // that connect to this alias
        fn extract_fk_edge_relationship_context(
            plan: &LogicalPlan,
            node_alias: &str,
        ) -> Option<(String, String, String)> {
            match plan {
                LogicalPlan::Projection(proj) => {
                    extract_fk_edge_relationship_context(&proj.input, node_alias)
                }
                LogicalPlan::Filter(filter) => {
                    extract_fk_edge_relationship_context(&filter.input, node_alias)
                }
                LogicalPlan::GraphRel(gr) => {
                    // Check if this GraphRel's right node (to_node) matches the alias
                    if let LogicalPlan::GraphNode(to_node) = &*gr.right {
                        if to_node.alias == node_alias {
                            let rel_type = gr.labels.as_ref()?.first()?.clone();
                            let from_label = extract_node_label(&gr.left)?;
                            let to_label = to_node.label.clone()?;
                            return Some((rel_type, from_label, to_label));
                        }
                    }

                    // Recurse into nested patterns
                    extract_fk_edge_relationship_context(&gr.left, node_alias)
                        .or_else(|| extract_fk_edge_relationship_context(&gr.center, node_alias))
                        .or_else(|| extract_fk_edge_relationship_context(&gr.right, node_alias))
                }
                LogicalPlan::GraphNode(gn) => {
                    extract_fk_edge_relationship_context(&gn.input, node_alias)
                }
                _ => None,
            }
        }

        // Extract from/to node aliases for a relationship alias from JOIN list and anchor
        fn extract_node_aliases_from_joins(
            joins: &[crate::query_planner::logical_plan::Join],
            rel_alias: &str,
        ) -> Option<(String, String)> {
            // From alias: look for FROM marker (JOIN with no conditions = anchor node)
            let from_alias = joins
                .iter()
                .find(|j| j.joining_on.is_empty())
                .map(|j| j.table_alias.clone());

            // To alias: find the OTHER node (not the relationship, not the anchor, has conditions)
            let to_alias = joins
                .iter()
                .find(|j| {
                    j.table_alias != rel_alias
                        && !j.joining_on.is_empty()
                        && j.table_alias != from_alias.as_deref().unwrap_or("")
                })
                .map(|j| j.table_alias.clone());

            match (from_alias, to_alias) {
                (Some(from), Some(to)) => {
                    log::info!("🔍 Extracted node aliases: from={}, to={}", from, to);
                    Some((from, to))
                }
                _ => {
                    log::warn!(
                        "⚠️  Could not extract node aliases for relationship {}",
                        rel_alias
                    );
                    None
                }
            }
        }

        // Main extract_joins implementation

        // Use helper functions from plan_builder_helpers module
        // get_schema_filter_for_node() - extracts schema filter from LogicalPlan
        // get_polymorphic_edge_filter_for_join() - generates polymorphic edge type filter
        // extract_predicates_for_alias_logical() - extracts predicates for specific alias
        // combine_render_exprs_with_and() - combines filters with AND

        let joins = match &self {
            LogicalPlan::Limit(limit) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&limit.input, schema)?
            }
            LogicalPlan::Skip(skip) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&skip.input, schema)?
            }
            LogicalPlan::OrderBy(order_by) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&order_by.input, schema)?
            }
            LogicalPlan::GroupBy(group_by) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&group_by.input, schema)?
            }
            LogicalPlan::Filter(filter) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&filter.input, schema)?
            }
            LogicalPlan::Projection(projection) => {
                <LogicalPlan as JoinBuilder>::extract_joins(&projection.input, schema)?
            }
            LogicalPlan::GraphNode(graph_node) => {
                // For nested GraphNodes (multiple standalone nodes), create CROSS JOINs
                let mut joins = vec![];

                // If this GraphNode has another GraphNode as input, create a CROSS JOIN for the inner node
                if let LogicalPlan::GraphNode(inner_node) = graph_node.input.as_ref() {
                    if let Some(table_name) = extract_table_name(&graph_node.input) {
                        joins.push(Join {
                            table_name,
                            table_alias: inner_node.alias.clone(), // Use the inner GraphNode's alias
                            joining_on: vec![],                    // Empty for CROSS JOIN
                            join_type: JoinType::Join,             // CROSS JOIN
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        });
                    }
                }

                // Recursively get joins from the input
                let mut inner_joins =
                    <LogicalPlan as JoinBuilder>::extract_joins(&graph_node.input, schema)?;
                joins.append(&mut inner_joins);

                joins
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // � MULTI-TYPE FIX: Check for multi-type relationship patterns FIRST
                // Multi-type patterns like [:FOLLOWS|AUTHORED] don't use the deprecated joins field
                // They generate a CTE (vlp_multi_type_a_b) that must be used as FROM, not JOINs
                // Delegate to input.extract_joins() which will return empty (see GraphRel handler)
                // IMPORTANT: Only for non-VLP patterns (VLP multi-type is handled below)
                if let Some(graph_rel) = find_multi_type_in_plan(&graph_joins.input) {
                    let is_implicit_one_hop = graph_rel
                        .variable_length
                        .as_ref()
                        .map(|spec| spec.min_hops == Some(1) && spec.max_hops == Some(1))
                        .unwrap_or(false);
                    let is_no_vlp_or_implicit =
                        graph_rel.variable_length.is_none() || is_implicit_one_hop;

                    if is_no_vlp_or_implicit {
                        log::info!(
                            "✓ MULTI-TYPE (non-VLP or implicit *1) detected in GraphJoins input: {:?} - delegating to input.extract_joins()",
                            graph_rel.labels
                        );
                        return <LogicalPlan as JoinBuilder>::extract_joins(
                            &graph_joins.input,
                            schema,
                        );
                    }
                }

                // 🔧 FIX: For GraphJoins with CTE references, check if we have pre-computed joins.
                // The analyzer populates graph_joins.joins with CTE-aware join conditions.
                // Only delegate to input.extract_joins() if graph_joins.joins is empty.
                if !graph_joins.cte_references.is_empty() && !graph_joins.joins.is_empty() {
                    log::warn!(
                        "🔧 GraphJoins has {} CTE references AND {} pre-computed joins - using pre-computed joins",
                        graph_joins.cte_references.len(),
                        graph_joins.joins.len()
                    );
                    for (alias, cte_name) in &graph_joins.cte_references {
                        log::warn!("  CTE ref: {} → {}", alias, cte_name);
                    }
                    // Fall through to use the pre-computed joins
                } else if !graph_joins.cte_references.is_empty() {
                    log::warn!(
                        "🔧 GraphJoins has {} CTE references but NO pre-computed joins - delegating to input",
                        graph_joins.cte_references.len()
                    );
                    // Delegate to input to get the joins with CTE references
                    return <LogicalPlan as JoinBuilder>::extract_joins(&graph_joins.input, schema);
                }

                // Check if input has a fixed-length variable-length pattern with >1 hops
                // For those, we need to use the expanded JOINs from extract_joins on the input
                // (which will call GraphRel.extract_joins -> expand_fixed_length_joins)
                if let Some(spec) = get_variable_length_spec(&graph_joins.input) {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        if exact_hops > 1 {
                            println!(
                                "DEBUG: GraphJoins has fixed-length *{} input - delegating to input.extract_joins()",
                                exact_hops
                            );
                            // Delegate to input to get the expanded multi-hop JOINs
                            return <LogicalPlan as JoinBuilder>::extract_joins(
                                &graph_joins.input,
                                schema,
                            );
                        }
                    }
                }

                // 🔧 FIX: If graph_joins.joins is empty but input has CartesianProduct,
                // delegate to input.extract_joins() to get the CROSS JOIN
                // This handles patterns like: MATCH (a:User) MATCH (b:User)
                if graph_joins.joins.is_empty() {
                    log::info!("🔧 GraphJoins has 0 joins - delegating to input.extract_joins()");
                    return <LogicalPlan as JoinBuilder>::extract_joins(&graph_joins.input, schema);
                }

                // FIX: Use ViewScan source_table instead of deprecated joins field table_name
                // The deprecated joins field has incorrect table names for polymorphic relationships
                // Extract alias → parameterized table reference mapping from GraphRel/GraphNode nodes
                // This uses the centralized helper that handles parameterized views correctly

                // Use the centralized helper from plan_builder_helpers.rs
                let rel_tables = extract_rel_and_node_tables(graph_joins.input.as_ref());

                // Collect edge constraints to apply to the final node JOIN
                // Edge constraints reference both from/to nodes, so must be applied after both are joined
                let mut edge_constraints: Vec<(String, RenderExpr)> = Vec::new();

                // 🔧 FIX: When anchor_table is None, from_builder will use the first join as FROM.
                // We need to skip that join here to avoid duplicate aliases.
                let first_join_alias_to_skip: Option<String> = if graph_joins.anchor_table.is_none()
                    && graph_joins.cte_references.is_empty()
                {
                    graph_joins
                        .joins
                        .first()
                        .filter(|j| !j.joining_on.is_empty()) // Only non-FROM-marker joins
                        .map(|j| j.table_alias.clone())
                } else {
                    None
                };

                if let Some(ref skip_alias) = first_join_alias_to_skip {
                    log::info!("🔧 Will skip first join '{}' as it will be used as FROM (anchor_table is None)", skip_alias);
                }

                // Convert joins
                // FROM markers (joins with empty joining_on and Inner type) are used by extract_from(), not extract_joins()
                // But optional entry points (joins with empty joining_on and Left type) need to be rendered as LEFT JOIN ... ON 1=1
                let mut joins: Vec<Join> = Vec::new();
                let mut skipped_first = false;
                let from_alias = graph_joins.anchor_table.as_ref().cloned();

                // Import logical JoinType for comparison
                use crate::query_planner::logical_plan::JoinType as LogicalJoinType;
                use crate::render_plan::render_expr::Literal as RenderLiteral;

                for logical_join in &graph_joins.joins {
                    // SKIP the FROM table marker - it has empty joining_on AND is the anchor
                    if logical_join.joining_on.is_empty() {
                        let is_from_table = from_alias
                            .as_ref()
                            .map(|a| a == &logical_join.table_alias)
                            .unwrap_or(false);

                        if is_from_table {
                            // This is the FROM table, skip it (will be rendered by extract_from)
                            log::debug!("🔧 Skipping FROM marker '{}'", logical_join.table_alias);
                            continue;
                        }

                        // This is an entry point with empty joining_on (not the FROM table)
                        // Render as JOIN ON 1=1 (cross-join semantics) with appropriate join type
                        let join_type = if logical_join.join_type == LogicalJoinType::Left {
                            log::info!(
                                "🔧 Optional entry point '{}' will be LEFT JOIN ON 1=1",
                                logical_join.table_alias
                            );
                            super::JoinType::Left
                        } else {
                            // Required entry point (Inner) - render as CROSS JOIN (inner join on 1=1)
                            log::info!(
                                "🔧 Required entry point '{}' will be JOIN ON 1=1 (cross-join)",
                                logical_join.table_alias
                            );
                            super::JoinType::Join
                        };

                        let cross_join = Join {
                            join_type,
                            table_name: logical_join.table_name.clone(),
                            table_alias: logical_join.table_alias.clone(),
                            joining_on: vec![
                                // ON 1=1 condition for cross-product
                                OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        RenderExpr::Literal(RenderLiteral::Integer(1)),
                                        RenderExpr::Literal(RenderLiteral::Integer(1)),
                                    ],
                                },
                            ],
                            pre_filter: None,
                            from_id_column: logical_join.from_id_column.clone(),
                            to_id_column: logical_join.to_id_column.clone(),
                            graph_rel: None,
                        };
                        joins.push(cross_join);
                        continue;
                    }

                    // 🔧 FIX: Skip the first join that will be used as FROM when anchor_table is None
                    if !skipped_first {
                        if let Some(ref skip_alias) = first_join_alias_to_skip {
                            if &logical_join.table_alias == skip_alias {
                                log::info!("🔧 Skipping join '{}' as FROM source", skip_alias);
                                skipped_first = true;
                                continue;
                            }
                        }
                    }

                    let mut render_join: Join = logical_join.clone().try_into()?;

                    // 🔧 CTE COLUMN REWRITING: When join conditions reference a CTE alias,
                    // we need to rewrite the column names to use the CTE column naming convention.
                    // E.g., o.user_id → o.o_user_id (because CTE exports columns as alias_column)
                    if !graph_joins.cte_references.is_empty() && !render_join.joining_on.is_empty() {
                        render_join.joining_on = render_join
                            .joining_on
                            .into_iter()
                            .map(|op_app| {
                                use crate::render_plan::plan_builder_utils::rewrite_operator_application_for_cte;
                                rewrite_operator_application_for_cte(
                                    &op_app,
                                    &graph_joins.cte_references,
                                )
                            })
                            .collect();
                    }

                    // Compile edge constraints for relationship JOINs (if constraints defined in schema)
                    // Store them to apply to the final node JOIN (where both from/to tables are available)
                    if render_join.from_id_column.is_some() && render_join.to_id_column.is_some() {
                        log::debug!(
                            "🔍 JOIN {} has from_id/to_id columns - checking for constraints",
                            render_join.table_alias
                        );

                        // This is a relationship JOIN (has from/to ID columns)
                        // Try two patterns:
                        // 1. Standard edge: table_alias matches GraphRel rel_alias (e.g., "c" for COPIED_BY)
                        // 2. FK-edge: table_alias matches GraphRel to_node alias (e.g., "folder" for IN_FOLDER)
                        let rel_context = extract_relationship_context(
                            &graph_joins.input,
                            &render_join.table_alias,
                        )
                        .or_else(|| {
                            log::debug!(
                                "Standard rel context not found, trying FK-edge pattern..."
                            );
                            extract_fk_edge_relationship_context(
                                &graph_joins.input,
                                &render_join.table_alias,
                            )
                        });

                        if let Some((rel_type, from_label, to_label)) = rel_context {
                            log::debug!(
                                "✓ Found relationship context: type={}, from={}, to={}",
                                rel_type,
                                from_label,
                                to_label
                            );
                            if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type)
                            {
                                log::debug!("✓ Found relationship schema for {}", rel_type);
                                if let Some(ref constraint_expr) = rel_schema.constraints {
                                    log::debug!(
                                        "✓ Found constraint expression: {}",
                                        constraint_expr
                                    );
                                    if let (Some(from_schema), Some(to_schema)) = (
                                        schema.node_schema_opt(&from_label),
                                        schema.node_schema_opt(&to_label),
                                    ) {
                                        // Find from/to aliases from JOIN conditions
                                        // For standard edge, use extract_node_aliases_from_joins
                                        // For FK-edge, extract directly from JOIN (from and to are the joined nodes)
                                        let node_aliases = extract_node_aliases_from_joins(
                                            &graph_joins.joins,
                                            &render_join.table_alias,
                                        )
                                        .or_else(|| {
                                            // FK-edge fallback: infer from the JOIN itself
                                            // The JOIN connects from_node to to_node, alias is to_node
                                            // For FK-edge with 1 JOIN, the anchor is the from_node
                                            let from_alias_opt = graph_joins
                                                .joins
                                                .iter()
                                                .find(|j| {
                                                    !j.joining_on.is_empty()
                                                        && j.table_alias != render_join.table_alias
                                                })
                                                .map(|j| j.table_alias.clone())
                                                .or_else(|| {
                                                    // If no other JOIN, from_node is the anchor (FROM table)
                                                    // Use graph_joins.anchor_table
                                                    graph_joins.anchor_table.clone()
                                                });

                                            from_alias_opt.map(|from_alias| {
                                                log::debug!(
                                                    "🔍 FK-edge alias extraction: from={}, to={}",
                                                    from_alias,
                                                    render_join.table_alias
                                                );
                                                (from_alias, render_join.table_alias.clone())
                                            })
                                        });

                                        if let Some((from_alias, to_alias)) = node_aliases {
                                            match crate::graph_catalog::constraint_compiler::compile_constraint(
                                                constraint_expr,
                                                from_schema,
                                                to_schema,
                                                &from_alias,
                                                &to_alias,
                                            ) {
                                                Ok(compiled_sql) => {
                                                    log::info!("✓ Compiled edge constraint for {} ({}): {} → {}",
                                                        render_join.table_alias, rel_type, constraint_expr, compiled_sql);

                                                    // Store constraint to apply to this JOIN directly
                                                    // (for FK-edge, this IS the to_node JOIN)
                                                    let constraint_expr = RenderExpr::Raw(compiled_sql);
                                                    edge_constraints.push((to_alias, constraint_expr));
                                                }
                                                Err(e) => {
                                                    log::warn!("Failed to compile edge constraint for {}: {}", render_join.table_alias, e);
                                                }
                                            }
                                        } else {
                                            log::debug!("Could not extract node aliases for FK-edge JOIN: {}", render_join.table_alias);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Fix table_name if we have a resolved table for this alias
                    if let Some(resolved_table) = rel_tables.get(&render_join.table_alias) {
                        render_join.table_name = resolved_table.clone();
                    }

                    joins.push(render_join);
                }

                // Apply collected edge constraints to the appropriate node JOINs
                for (to_alias, constraint) in edge_constraints {
                    if let Some(join) = joins.iter_mut().find(|j| j.table_alias == to_alias) {
                        join.pre_filter = if let Some(existing) = join.pre_filter.clone() {
                            Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing, constraint],
                            }))
                        } else {
                            Some(constraint)
                        };
                        log::debug!("Applied edge constraint to node JOIN: {}", to_alias);
                    }
                }

                // 🔧 FIX: Also extract joins from input plan (for CartesianProduct with non-GraphRel right side)
                // The GraphJoins' joins array only covers the OPTIONAL MATCH joins, but if there's a
                // CartesianProduct in the input with additional nodes (from required MATCH), we need those too.
                // Collect the aliases we already have to avoid duplicates.
                // Include: processed joins, FROM markers (skipped), anchor_table, and first_join if used as FROM
                let mut existing_aliases: std::collections::HashSet<String> =
                    joins.iter().map(|j| j.table_alias.clone()).collect();

                // Also add FROM marker aliases (joins with empty joining_on that were skipped)
                for logical_join in &graph_joins.joins {
                    if logical_join.joining_on.is_empty() {
                        existing_aliases.insert(logical_join.table_alias.clone());
                    }
                }

                // Also add anchor_table if set
                if let Some(ref anchor) = graph_joins.anchor_table {
                    existing_aliases.insert(anchor.clone());
                }

                // Also add first_join_alias_to_skip if set
                if let Some(ref skip_alias) = first_join_alias_to_skip {
                    existing_aliases.insert(skip_alias.clone());
                }

                log::debug!(
                    "🔍 existing_aliases before input extraction: {:?}",
                    existing_aliases
                );

                let input_joins =
                    <LogicalPlan as JoinBuilder>::extract_joins(&graph_joins.input, schema)?;

                // CRITICAL FIX: For FK-edge patterns, detect duplicate table joins
                // Build a map of alias -> table_name from joins and FROM markers
                let mut alias_to_table: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();

                // Add joins
                for j in &graph_joins.joins {
                    alias_to_table.insert(j.table_alias.clone(), j.table_name.clone());
                }

                // Add FROM markers (joins with empty joining_on)
                for j in &graph_joins.joins {
                    if j.joining_on.is_empty() {
                        alias_to_table.insert(j.table_alias.clone(), j.table_name.clone());
                    }
                }

                // Get anchor table name if set
                let anchor_table_name =
                    graph_joins.anchor_table.as_ref().and_then(|anchor_alias| {
                        // First check our map
                        if let Some(name) = alias_to_table.get(anchor_alias) {
                            return Some(name.clone());
                        }
                        // Otherwise try to extract from GraphRel right node (which is typically the anchor for FK-edge)
                        if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                            if let LogicalPlan::GraphRel(graph_rel) = proj.input.as_ref() {
                                if &graph_rel.right_connection == anchor_alias {
                                    if let LogicalPlan::GraphNode(gn) = graph_rel.right.as_ref() {
                                        if let LogicalPlan::ViewScan(scan) = gn.input.as_ref() {
                                            return Some(scan.source_table.clone());
                                        }
                                    }
                                }
                            }
                        }
                        None
                    });

                log::debug!(
                    "🔍 anchor_table_name for FK-edge check: {:?}",
                    anchor_table_name
                );

                for input_join in input_joins {
                    // Skip if alias already exists
                    if existing_aliases.contains(&input_join.table_alias) {
                        continue;
                    }

                    // CRITICAL FIX: For FK-edge patterns, the relationship alias points to the same
                    // table as one of the nodes (anchor). We should NOT create a duplicate JOIN.
                    // Example: (u:User)-[r:AUTHORED]->(po:Post)
                    //   - FK-edge: AUTHORED is stored ON posts_bench table
                    //   - anchor_table = "po" (posts_bench)
                    //   - relationship alias "r" also points to posts_bench
                    //   - We should NOT add: JOIN posts_bench AS r
                    //   - Instead, "r" properties should be accessed via "po" alias
                    if let Some(ref anchor_name) = anchor_table_name {
                        if &input_join.table_name == anchor_name {
                            log::info!(
                                "🔑 Skipping duplicate JOIN for FK-edge: {} AS {} (same table as anchor '{}')",
                                input_join.table_name,
                                input_join.table_alias,
                                graph_joins.anchor_table.as_ref().unwrap()
                            );
                            continue;
                        }
                    }

                    log::info!(
                        "🔧 Adding missing JOIN from input: {} (alias={})",
                        input_join.table_name,
                        input_join.table_alias
                    );
                    joins.push(input_join);
                }

                joins
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate JOINs for the relationship traversal
                // This fixes OPTIONAL MATCH queries by creating proper JOIN clauses
                log::info!(
                    "🔧 DEBUG: GraphRel.extract_joins called for alias='{}', left='{}', right='{}', labels={:?}",
                    graph_rel.alias, graph_rel.left_connection, graph_rel.right_connection, graph_rel.labels
                );

                // 🚀 MULTI-TYPE RELATIONSHIPS: Check if this is a multi-type pattern (e.g., [:FOLLOWS|AUTHORED])
                // If it has multiple relationship types, a VLP CTE is generated for the UNION ALL
                // Return empty joins - the CTE will be used as FROM clause
                // IMPORTANT: Handles implicit *1 added by query planner for multi-type
                let is_implicit_one_hop = graph_rel
                    .variable_length
                    .as_ref()
                    .map(|spec| spec.min_hops == Some(1) && spec.max_hops == Some(1))
                    .unwrap_or(false);
                let is_no_vlp_or_implicit =
                    graph_rel.variable_length.is_none() || is_implicit_one_hop;

                if is_no_vlp_or_implicit {
                    if let Some(ref labels) = graph_rel.labels {
                        if labels.len() > 1 {
                            log::info!(
                                "✓ Multi-type relationship (non-VLP or implicit *1) {:?} - using CTE vlp_multi_type_{}_{}, no joins needed (RETURNING EMPTY)",
                                labels,
                                graph_rel.left_connection,
                                graph_rel.right_connection
                            );
                            return Ok(Vec::new());
                        }
                    }
                }

                // 🚀 FIXED-LENGTH VLP: Use consolidated VlpContext for all schema types
                if let Some(vlp_ctx) = build_vlp_context(graph_rel, schema) {
                    let exact_hops = vlp_ctx.exact_hops.unwrap_or(1);

                    // Special case: *0 pattern (zero hops = same node)
                    // Return empty joins - both a and b reference the same node
                    if vlp_ctx.is_fixed_length && exact_hops == 0 {
                        crate::debug_println!(
                            "DEBUG: extract_joins - Zero-hop pattern (*0) - returning empty joins"
                        );
                        return Ok(Vec::new());
                    }

                    if vlp_ctx.is_fixed_length && exact_hops > 0 {
                        println!(
                            "DEBUG: extract_joins - Fixed-length VLP (*{}) with {:?} schema",
                            exact_hops, vlp_ctx.schema_type
                        );

                        // Use the consolidated function that handles all schema types
                        let (_from_table, _from_alias, joins) =
                            expand_fixed_length_joins_with_context(&vlp_ctx);

                        // Store the VLP context for later use by FROM clause and property resolution
                        // (This is done via the existing pattern of passing info through the plan)

                        return Ok(joins);
                    }

                    // VARIABLE-LENGTH VLP (recursive CTE): Handle based on optionality
                    // - Optional VLP: Create LEFT JOIN from anchor to CTE
                    // - Required VLP: Return empty joins (CTE used as FROM)
                    if !vlp_ctx.is_fixed_length {
                        let is_optional = graph_rel.is_optional.unwrap_or(false);
                        log::info!(
                            "🔍 VLP OPTIONAL CHECK: is_optional={}, graph_rel.is_optional={:?}",
                            is_optional,
                            graph_rel.is_optional
                        );

                        if is_optional {
                            // OPTIONAL VLP: Need LEFT JOIN from anchor node to VLP CTE
                            // SQL pattern: FROM users AS a LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
                            crate::debug_println!(
                                "DEBUG: extract_joins - OPTIONAL VLP - creating LEFT JOIN to CTE"
                            );
                            log::info!("✓ OPTIONAL VLP: Creating LEFT JOIN from anchor to CTE");

                            let cte_name = format!(
                                "vlp_{}_{}",
                                &graph_rel.left_connection, &graph_rel.right_connection
                            );

                            // Get the start node's ID column
                            let start_id_col = extract_id_column(&graph_rel.left)
                                .unwrap_or_else(|| "user_id".to_string());

                            // Create LEFT JOIN condition: a.user_id = t.start_id
                            let join_condition = vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.left_connection.clone()),
                                        column: PropertyValue::Column(start_id_col.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(VLP_CTE_FROM_ALIAS.to_string()),
                                        column: PropertyValue::Column("start_id".to_string()),
                                    }),
                                ],
                            }];

                            let vlp_join = Join {
                                join_type: JoinType::Left,
                                table_name: cte_name.clone(),
                                table_alias: VLP_CTE_FROM_ALIAS.to_string(),
                                joining_on: join_condition,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: Some(Arc::new(graph_rel.clone())),
                            };

                            crate::debug_println!(
                                "DEBUG: Created OPTIONAL VLP LEFT JOIN: {} AS {}",
                                cte_name,
                                VLP_CTE_FROM_ALIAS
                            );
                            log::info!(
                                "✓ Created LEFT JOIN: {} AS {}",
                                cte_name,
                                VLP_CTE_FROM_ALIAS
                            );
                            return Ok(vec![vlp_join]);
                        } else {
                            // Required VLP: CTE used as FROM, no joins needed
                            crate::debug_println!("DEBUG: extract_joins - Variable-length VLP (recursive CTE) - returning empty joins");
                            log::info!("✓ Required VLP: CTE as FROM, no joins needed");
                            return Ok(Vec::new());
                        }
                    }
                }

                // MULTI-HOP FIX: If left side is another GraphRel, recursively extract its joins first
                // This handles patterns like (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
                let mut joins = vec![];

                // DENORMALIZED EDGE TABLE CHECK
                // For denormalized patterns, nodes are virtual (stored on edge table)
                // We need to JOIN edge tables directly, not node tables
                let left_is_denormalized = is_node_denormalized(&graph_rel.left);
                let right_is_denormalized = is_node_denormalized(&graph_rel.right);

                println!(
                    "DEBUG: extract_joins - left_is_denormalized={}, right_is_denormalized={}",
                    left_is_denormalized, right_is_denormalized
                );

                // For denormalized patterns, handle specially
                if left_is_denormalized && right_is_denormalized {
                    crate::debug_println!("DEBUG: DENORMALIZED multi-hop pattern detected");

                    // Get the relationship table with parameterized view syntax if applicable
                    let rel_table = extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone());

                    // Get relationship columns (from_id and to_id)
                    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                        RelationshipColumns {
                            from_id: "from_node_id".to_string(),
                            to_id: "to_node_id".to_string(),
                        },
                    );

                    // Check if this is a chained hop (left side is another GraphRel)
                    if let LogicalPlan::GraphRel(left_rel) = graph_rel.left.as_ref() {
                        println!(
                            "DEBUG: DENORMALIZED multi-hop - chaining {} -> {}",
                            left_rel.alias, graph_rel.alias
                        );

                        // First, recursively get joins from the left GraphRel
                        let mut left_joins =
                            <LogicalPlan as JoinBuilder>::extract_joins(&graph_rel.left, schema)?;
                        joins.append(&mut left_joins);

                        // Get the left relationship's to_id column for joining
                        let left_rel_cols = extract_relationship_columns(&left_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // =========================================================
                        // COUPLED EDGE DETECTION
                        // =========================================================
                        // Check if the left and current edges are coupled (same table, coupling node)
                        // If so, they exist in the same row - NO JOIN needed!
                        let current_rel_type = graph_rel
                            .labels
                            .as_ref()
                            .and_then(|l: &Vec<String>| l.first().cloned());
                        let left_rel_type = left_rel
                            .labels
                            .as_ref()
                            .and_then(|l: &Vec<String>| l.first().cloned());

                        if let (Some(curr_type), Some(left_type)) =
                            (current_rel_type, left_rel_type)
                        {
                            // Try to get coupling info from schema
                            if let Some(schema_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                if let Ok(schemas) = schema_lock.try_read() {
                                    // Try different schema names
                                    for schema_name in ["default", ""] {
                                        if let Some(schema) = schemas.get(schema_name) {
                                            if let Some(coupling_info) =
                                                schema.get_coupled_edge_info(&left_type, &curr_type)
                                            {
                                                println!(
                                                    "DEBUG: COUPLED EDGES DETECTED! {} and {} share coupling node {} in table {}",
                                                    left_type, curr_type, coupling_info.coupling_node, coupling_info.table_name
                                                );

                                                // Skip the JOIN - edges are in the same row!
                                                // If arrays need expansion, user should use UNWIND clause
                                                return Ok(joins);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Not coupled - add the JOIN as usual
                        // JOIN this relationship table to the previous one
                        // e.g., INNER JOIN flights AS f2 ON f2.Origin = f1.Dest
                        joins.push(Join {
                            table_name: rel_table.clone(),
                            table_alias: graph_rel.alias.clone(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_rel.alias.clone()),
                                        column: PropertyValue::Column(left_rel_cols.to_id.clone()),
                                    }),
                                ],
                            }],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(rel_cols.from_id.clone()), // Preserve for NULL checks
                            to_id_column: Some(rel_cols.to_id.clone()), // Preserve for NULL checks
                            graph_rel: None,
                        });
                    }
                    // For single-hop denormalized, no JOINs needed - relationship table IS the data
                    // Just return empty joins, the FROM clause will use the relationship table

                    return Ok(joins);
                }

                // STANDARD (non-denormalized) multi-hop handling
                // MULTI-HOP FIX: Check BOTH left and right sides for nested GraphRel patterns
                if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    println!(
                        "🔍 DEBUG: Multi-hop pattern detected on LEFT side - recursively extracting left GraphRel joins (alias={})",
                        graph_rel.alias
                    );
                    let mut left_joins =
                        <LogicalPlan as JoinBuilder>::extract_joins(&graph_rel.left, schema)?;
                    println!("  ↳ Got {} joins from left GraphRel", left_joins.len());
                    joins.append(&mut left_joins);
                }

                // Also check right side for nested GraphRel (e.g., (a)-[r1]->(b)-[r2]->(c))
                // In this case, right side contains (b)-[r2]->(c) which needs its own joins
                if let LogicalPlan::GraphRel(inner_rel) = graph_rel.right.as_ref() {
                    println!(
                        "🔍 DEBUG: Multi-hop pattern detected on RIGHT side - recursively extracting right GraphRel joins (alias={})",
                        graph_rel.alias
                    );

                    // NESTED PATTERN JOIN ORDERING FIX
                    // ================================
                    // For pattern like (post)<-[:HAS_CREATOR]-(f)-[:KNOWS]-(p):
                    // - Outer: left=post, right=inner, right_connection="f" (shared node)
                    // - Inner: left=p, right=f, left_connection="p", right_connection="f"
                    //
                    // The inner extract_joins assumes left_connection (p) is the anchor/FROM,
                    // generating: t1 ON t1.from_id = p.id (WRONG - p not available yet!)
                    //
                    // In nested context, the SHARED node (f) is the anchor, so we need:
                    // - t1 should connect to f (shared): t1.to_id = f.id
                    // - p should connect to t1: p.id = t1.from_id
                    //
                    // SOLUTION: Don't use inner extract_joins which has wrong anchor assumption.
                    // Instead, manually build the correct JOINs for nested patterns.

                    let shared_node_alias = &graph_rel.right_connection;
                    let inner_left_alias = &inner_rel.left_connection;
                    let inner_right_alias = &inner_rel.right_connection;

                    // Determine which inner node is the shared node
                    let shared_is_inner_left = inner_left_alias == shared_node_alias;
                    let shared_is_inner_right = inner_right_alias == shared_node_alias;

                    println!("🔍 DEBUG: Nested pattern - shared='{}', inner_left='{}', inner_right='{}', shared_is_left={}, shared_is_right={}",
                             shared_node_alias, inner_left_alias, inner_right_alias, shared_is_inner_left, shared_is_inner_right);

                    if shared_is_inner_right {
                        // Shared node is inner's right_connection (e.g., f)
                        // Non-shared node is inner's left_connection (e.g., p)
                        // We need:
                        // 1. t1 (relationship) connecting to shared node (f): t1.to_id = f.id
                        // 2. p (non-shared) connecting to t1: p.id = t1.from_id

                        let non_shared_alias = inner_left_alias;

                        let inner_rel_cols = extract_relationship_columns(&inner_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // Get shared node's ID column
                        let shared_id_col = extract_end_node_id_column(&inner_rel.right)
                            .unwrap_or_else(|| "id".to_string());

                        // JOIN 1: Relationship table connecting to shared node
                        // t1.to_id = f.id (since f = right_connection → to_id per GraphRel convention)
                        let rel_table = extract_parameterized_table_ref(&inner_rel.center)
                            .unwrap_or_else(|| inner_rel.alias.clone());

                        let rel_join_condition = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(inner_rel.alias.clone()),
                                    column: PropertyValue::Column(inner_rel_cols.to_id.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(shared_node_alias.clone()),
                                    column: PropertyValue::Column(shared_id_col),
                                }),
                            ],
                        };

                        joins.push(Join {
                            table_name: rel_table,
                            table_alias: inner_rel.alias.clone(),
                            joining_on: vec![rel_join_condition],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(inner_rel_cols.from_id.clone()),
                            to_id_column: Some(inner_rel_cols.to_id.clone()),
                            graph_rel: None,
                        });

                        // JOIN 2: Non-shared node connecting to relationship
                        // p.id = t1.from_id (since p = left_connection → from_id)
                        if let Some(non_shared_table) = extract_table_name(&inner_rel.left) {
                            let non_shared_id_col = extract_id_column(&inner_rel.left)
                                .unwrap_or_else(|| "id".to_string());

                            let non_shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(non_shared_alias.clone()),
                                        column: PropertyValue::Column(non_shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(inner_rel.alias.clone()),
                                        column: PropertyValue::Column(
                                            inner_rel_cols.from_id.clone(),
                                        ),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name: non_shared_table,
                                table_alias: non_shared_alias.clone(),
                                joining_on: vec![non_shared_join_condition],
                                join_type: JoinType::Inner,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });
                        }

                        println!(
                            "  ✅ Built nested pattern JOINs: {} → {}",
                            inner_rel.alias, non_shared_alias
                        );
                    } else if shared_is_inner_left {
                        // Shared node is inner's left_connection
                        // This case should work with normal extract_joins since left is anchor
                        // But let's still use the manual approach for consistency

                        let non_shared_alias = inner_right_alias;

                        let inner_rel_cols = extract_relationship_columns(&inner_rel.center)
                            .unwrap_or(RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            });

                        // Get shared node's ID column
                        let shared_id_col =
                            extract_id_column(&inner_rel.left).unwrap_or_else(|| "id".to_string());

                        // JOIN 1: Relationship connecting to shared node (left)
                        // t1.from_id = f.id (since f = left_connection → from_id)
                        let rel_table = extract_parameterized_table_ref(&inner_rel.center)
                            .unwrap_or_else(|| inner_rel.alias.clone());

                        let rel_join_condition = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(inner_rel.alias.clone()),
                                    column: PropertyValue::Column(inner_rel_cols.from_id.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(shared_node_alias.clone()),
                                    column: PropertyValue::Column(shared_id_col),
                                }),
                            ],
                        };

                        joins.push(Join {
                            table_name: rel_table,
                            table_alias: inner_rel.alias.clone(),
                            joining_on: vec![rel_join_condition],
                            join_type: JoinType::Inner,
                            pre_filter: None,
                            from_id_column: Some(inner_rel_cols.from_id.clone()),
                            to_id_column: Some(inner_rel_cols.to_id.clone()),
                            graph_rel: None,
                        });

                        // JOIN 2: Non-shared node (right) connecting to relationship
                        // p.id = t1.to_id (since p = right_connection → to_id)
                        if let Some(non_shared_table) =
                            extract_end_node_table_name(&inner_rel.right)
                        {
                            let non_shared_id_col = extract_end_node_id_column(&inner_rel.right)
                                .unwrap_or_else(|| "id".to_string());

                            let non_shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(non_shared_alias.clone()),
                                        column: PropertyValue::Column(non_shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(inner_rel.alias.clone()),
                                        column: PropertyValue::Column(inner_rel_cols.to_id.clone()),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name: non_shared_table,
                                table_alias: non_shared_alias.clone(),
                                joining_on: vec![non_shared_join_condition],
                                join_type: JoinType::Inner,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });
                        }

                        println!(
                            "  ✅ Built nested pattern JOINs (left shared): {} → {}",
                            inner_rel.alias, non_shared_alias
                        );
                    } else {
                        // Shared node doesn't match either inner connection - fallback to old behavior
                        println!("⚠️ DEBUG: Shared node '{}' doesn't match inner connections - using fallback", shared_node_alias);
                        let mut right_joins =
                            <LogicalPlan as JoinBuilder>::extract_joins(&graph_rel.right, schema)?;
                        joins.append(&mut right_joins);
                    }
                }

                // CTE REFERENCE CHECK: If right side is GraphJoins with pre-computed joins,
                // use those instead of generating new joins. This handles chained WITH clauses
                // where the right node is a CTE reference.
                if let LogicalPlan::GraphJoins(right_joins) = graph_rel.right.as_ref() {
                    println!(
                        "DEBUG: GraphRel.right is GraphJoins with {} pre-computed joins - using them",
                        right_joins.joins.len()
                    );
                    // The GraphJoins contains pre-computed joins that reference the CTE correctly.
                    // However, some joins may have stale conditions referencing tables from
                    // previous WITH clause scopes. Filter those out.

                    // First, add the relationship table join (center -> left node)
                    // Use extract_parameterized_table_ref to handle parameterized views correctly
                    let rel_table = extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| {
                            println!("WARNING: extract_parameterized_table_ref returned None for relationship alias '{}', falling back to alias", graph_rel.alias);
                            graph_rel.alias.clone()
                        });

                    println!("DEBUG extract_joins GraphRel: alias='{}', rel_table from extract_parameterized_table_ref='{}'", graph_rel.alias, rel_table);

                    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                        RelationshipColumns {
                            from_id: "from_node_id".to_string(),
                            to_id: "to_node_id".to_string(),
                        },
                    );

                    // Get left side ID column from the FROM table
                    let left_id_col = extract_id_column(&graph_rel.left).ok_or_else(|| {
                        RenderBuildError::InvalidRenderPlan(format!(
                            "Cannot determine ID column for left node '{}' in relationship '{}'. \
                             Node schema must define id_column in YAML, or node might have invalid plan structure.",
                            graph_rel.left_connection, graph_rel.alias
                        ))
                    })?;

                    // Determine join condition based on direction
                    let is_optional = graph_rel.is_optional.unwrap_or(false);
                    let join_type = if is_optional {
                        JoinType::Left
                    } else {
                        JoinType::Inner
                    };

                    // For relationship joins, the columns are determined by the edge definition:
                    // - from_id connects to the SOURCE node (where edge originates)
                    // - to_id connects to the TARGET node (where edge points)
                    //
                    // Due to how left_connection/right_connection are computed in match_clause.rs:
                    // - Outgoing (a)-[r]->(b): left_conn=a, right_conn=b -> a is source, b is target
                    // - Incoming (a)<-[r]-(b): left_conn=b, right_conn=a -> b is source, a is target
                    //
                    // In both cases: left_connection is the SOURCE, right_connection is the TARGET
                    // So we always use: left_conn.id = rel.from_id, right_conn.id = rel.to_id
                    let rel_col_start = &rel_cols.from_id; // for left_connection (SOURCE)
                    let rel_col_end = &rel_cols.to_id; // for right_connection (TARGET)

                    // JOIN 1: Relationship table -> FROM (left) node
                    joins.push(Join {
                        table_name: rel_table,
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: PropertyValue::Column(rel_col_start.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: PropertyValue::Column(left_id_col),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                        pre_filter: None,
                        from_id_column: Some(rel_col_start.clone()), // Preserve for NULL checks
                        to_id_column: Some(rel_col_end.clone()),     // Preserve for NULL checks
                        graph_rel: None,
                    });

                    // JOIN 2: CTE (right node) -> Relationship table
                    // Get the CTE table name from the GraphJoins input
                    if let LogicalPlan::GraphJoins(gn) = right_joins.input.as_ref() {
                        if let Some(cte_table) = extract_table_name(&gn.input) {
                            // Get the right node's ID column
                            let right_id_col = extract_id_column(&right_joins.input).ok_or_else(|| {
                                RenderBuildError::InvalidRenderPlan(format!(
                                    "Cannot determine ID column for right node '{}' in relationship '{}'. \
                                     Node schema must define id_column in YAML, or node might have invalid plan structure.",
                                    graph_rel.right_connection, graph_rel.alias
                                ))
                            })?;

                            joins.push(Join {
                                table_name: cte_table,
                                table_alias: graph_rel.right_connection.clone(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(
                                                graph_rel.right_connection.clone(),
                                            ),
                                            column: PropertyValue::Column(right_id_col),
                                        }),
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(graph_rel.alias.clone()),
                                            column: PropertyValue::Column(rel_col_end.clone()),
                                        }),
                                    ],
                                }],
                                join_type,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });
                        }
                    }

                    // Skip the pre-computed joins from GraphJoins - they have stale conditions
                    // We've generated fresh joins above with correct conditions
                    return Ok(joins);
                }

                // First, check if the plan_ctx marks this relationship as optional
                // This is set by OPTIONAL MATCH clause processing
                let is_optional = graph_rel.is_optional.unwrap_or(false);
                let join_type = if is_optional {
                    JoinType::Left
                } else {
                    JoinType::Inner
                };

                // Extract table names and columns
                // IMPORTANT: For CTE references, use the source_table directly from ViewScan
                // because CTEs don't have labels in the schema

                /// Get table name for START node (left side of GraphRel)
                /// Uses standard extract_table_name which returns relationship table for GraphRel
                fn get_start_table_name_or_cte(
                    plan: &LogicalPlan,
                ) -> Result<String, RenderBuildError> {
                    // First, try to get source_table directly from ViewScan (handles CTE references)
                    if let Some(table_name) = extract_table_name(plan) {
                        // Check if this looks like a CTE (starts with "with_")
                        if table_name.starts_with("with_") {
                            return Ok(table_name);
                        }
                    }
                    // Extract table name from ViewScan - no fallback
                    extract_table_name(plan).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "start node table in extract_joins".to_string(),
                        )
                    })
                }

                /// Get table name for END node (right side of GraphRel)
                /// CRITICAL: For nested GraphRel patterns (multi-hop), uses extract_end_node_table_name
                /// which correctly traverses to the rightmost node instead of returning the relationship table
                fn get_end_table_name_or_cte(
                    plan: &LogicalPlan,
                ) -> Result<String, RenderBuildError> {
                    // First, try to get source_table directly from ViewScan (handles CTE references)
                    if let Some(table_name) = extract_end_node_table_name(plan) {
                        // Check if this looks like a CTE (starts with "with_")
                        if table_name.starts_with("with_") {
                            return Ok(table_name);
                        }
                    }
                    // Extract END NODE table name - handles nested GraphRel correctly
                    extract_end_node_table_name(plan).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "end node table in extract_joins".to_string(),
                        )
                    })
                }

                // Helper function to get table name from relationship schema
                // Used when target node doesn't have a specific label (e.g., in multi-relationship queries)
                fn get_table_from_rel_schema(
                    labels: &Option<Vec<String>>,
                    is_from_node: bool,
                ) -> Option<String> {
                    if let Some(label_list) = labels {
                        if !label_list.is_empty() {
                            // Use the first relationship type to get the table name
                            // For multi-relationship queries, all relationships should connect to the same table
                            // (or the query should use denormalized edges)
                            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                if let Ok(schemas) = schemas_lock.try_read() {
                                    // Try "default" schema first, then empty string
                                    for schema_name in ["default", ""] {
                                        if let Some(schema) = schemas.get(schema_name) {
                                            if let Ok(rel_schema) =
                                                schema.get_rel_schema(&label_list[0])
                                            {
                                                let table_name = if is_from_node {
                                                    &rel_schema.from_node_table
                                                } else {
                                                    &rel_schema.to_node_table
                                                };
                                                return Some(format!(
                                                    "{}.{}",
                                                    rel_schema.database, table_name
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None
                }

                let start_table = get_start_table_name_or_cte(&graph_rel.left).or_else(|_| {
                    // Fallback: try to get from relationship schema
                    get_table_from_rel_schema(&graph_rel.labels, true).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "start node table in extract_joins".to_string(),
                        )
                    })
                })?;

                // CRITICAL FIX: Use get_end_table_name_or_cte for the right side
                // This correctly handles nested GraphRel patterns (multi-hop traversals)
                // where graph_rel.right is itself a GraphRel, not a simple GraphNode
                let end_table = get_end_table_name_or_cte(&graph_rel.right).or_else(|_| {
                    // Fallback: try to get from relationship schema
                    get_table_from_rel_schema(&graph_rel.labels, false).ok_or_else(|| {
                        RenderBuildError::MissingTableInfo(
                            "end node table in extract_joins".to_string(),
                        )
                    })
                })?;

                // Also extract labels for schema filter generation (optional for CTEs)
                let start_label = extract_node_label_from_viewscan(&graph_rel.left);
                let end_label = extract_node_label_from_viewscan(&graph_rel.right);

                // Get relationship table with parameterized view syntax if applicable
                // POLYMORPHIC FIX: Always use extract from ViewScan source_table
                // which contains the correctly resolved polymorphic table name (e.g., "ldbc.Person_likes_Message")
                // NOT rel_type_to_table_name which doesn't know about node labels
                let rel_table = if matches!(&*graph_rel.center, LogicalPlan::Cte(_)) {
                    // CTEs don't have parameterized views
                    extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
                } else {
                    // Use extract_parameterized_table_ref for ViewScan (handles parameterized views)
                    extract_parameterized_table_ref(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone())
                };

                println!(
                    "DEBUG: GraphRel extract_joins - rel_table='{}' for alias='{}'",
                    rel_table, graph_rel.alias
                );

                // MULTI-HOP FIX: For ID columns, use proper extraction based on plan structure
                // - Single hop (left is GraphNode): use extract_id_column on left
                // - Multi-hop (left is GraphRel): use extract_end_node_id_column which follows rel.right chain
                // The left_connection in multi-hop points to the right_connection of the inner GraphRel
                let start_id_col = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    // Multi-hop: left side is another GraphRel
                    // The shared node is left_connection, which is the inner GraphRel's right node
                    // Use extract_end_node_id_column to get ID from the inner GraphRel's right side
                    println!(
                        "DEBUG: Multi-hop - left_connection={}, extracting ID from inner GraphRel's right node",
                        graph_rel.left_connection
                    );
                    extract_end_node_id_column(&graph_rel.left)
                        .unwrap_or_else(|| {
                            log::warn!("⚠️ extract_end_node_id_column failed for multi-hop left_connection='{}', using 'id' fallback", graph_rel.left_connection);
                            "id".to_string()
                        })
                } else {
                    // Single hop: extract ID column from the node ViewScan
                    extract_id_column(&graph_rel.left)
                        .unwrap_or_else(|| table_to_id_column(&start_table))
                };
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));

                // Get relationship columns
                let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                    RelationshipColumns {
                        from_id: "from_node_id".to_string(),
                        to_id: "to_node_id".to_string(),
                    },
                );

                // JOIN ORDER: For standard patterns like (a)-[:R]->(b), we join:
                // 1. Relationship table (can reference anchor `a` from FROM clause)
                // 2. End node `b` (can reference relationship)
                //
                // The `is_optional` flag determines JOIN TYPE (LEFT vs INNER), not order.
                // The FROM clause is always the left/anchor node, so normal order works.

                // For LEFT JOINs, we need to extract:
                // 1. Schema filters from YAML config (ViewScan.schema_filter)
                // 2. User WHERE predicates that reference ONLY optional aliases
                // Both go into pre_filter (subquery form) for correct LEFT JOIN semantics
                //
                // IMPORTANT: In OPTIONAL MATCH (a)-[r]->(b):
                // - left_connection (a) is the REQUIRED anchor - do NOT extract its predicates!
                // - alias (r) is optional - extract its predicates
                // - right_connection (b) is optional - extract its predicates

                // Extract user predicates ONLY for optional aliases (rel and right)
                // DO NOT extract for left_connection - it's the required anchor!
                let (rel_user_pred, remaining_after_rel) = if is_optional {
                    extract_predicates_for_alias_logical(
                        &graph_rel.where_predicate,
                        &graph_rel.alias,
                    )
                } else {
                    (None, graph_rel.where_predicate.clone())
                };

                let (right_user_pred, _remaining) = if is_optional {
                    extract_predicates_for_alias_logical(
                        &remaining_after_rel,
                        &graph_rel.right_connection,
                    )
                } else {
                    (None, remaining_after_rel)
                };

                // Get schema filters from YAML config
                // Note: left_connection is the anchor node, but it might still have a schema filter
                let left_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.left, &graph_rel.left_connection)
                } else {
                    None
                };
                let rel_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.center, &graph_rel.alias)
                } else {
                    None
                };
                let right_schema_filter = if is_optional {
                    get_schema_filter_for_node(&graph_rel.right, &graph_rel.right_connection)
                } else {
                    None
                };

                // Generate polymorphic edge filter (type_column IN ('TYPE1', 'TYPE2') AND from_label = 'X' AND to_label = 'Y')
                // This applies regardless of whether the JOIN is optional or required
                let rel_types_for_filter: Vec<String> =
                    graph_rel.labels.clone().unwrap_or_default();
                let polymorphic_filter = get_polymorphic_edge_filter_for_join(
                    &graph_rel.center,
                    &graph_rel.alias,
                    &rel_types_for_filter,
                    &start_label,
                    &end_label,
                );

                // Combine schema filter + user predicates for each alias's pre_filter
                // Note: left_connection is anchor, so we only use schema filter (no user predicate extraction)
                // Using combine_optional_filters_with_and from plan_builder_helpers

                // left_node uses ONLY schema filter (no user predicates - anchor node predicates stay in WHERE)
                let _left_node_pre_filter = left_schema_filter;
                // Relationship pre_filter combines: schema filter + polymorphic filter + user predicates
                let rel_pre_filter = combine_optional_filters_with_and(vec![
                    rel_schema_filter,
                    polymorphic_filter,
                    rel_user_pred,
                ]);
                let right_node_pre_filter =
                    combine_optional_filters_with_and(vec![right_schema_filter, right_user_pred]);

                // Standard join order: relationship first, then end node
                // The FROM clause is always the left/anchor node.

                // DEBUG: Log CTE references at start of extract_joins
                log::info!("🔍 extract_joins: left_connection='{}', right_connection='{}', cte_references={:?}",
                           graph_rel.left_connection, graph_rel.right_connection, graph_rel.cte_references);

                // Helper: Resolve table alias and column for CTE references
                // When a node connection (e.g., "b") references a CTE, we need to use
                // the CTE alias (e.g., "a_b") instead of the node alias ("b")
                let resolve_cte_reference = |node_alias: &str, column: &str| -> (String, String) {
                    if let Some(cte_name) = graph_rel.cte_references.get(node_alias) {
                        // The node alias IS the table alias for the CTE
                        // e.g., CTE is: with_o_cte_0 AS (...) and FROM uses: FROM with_o_cte_0 AS o
                        // So we reference columns as: o.o_user_id
                        let cte_alias = node_alias;

                        // Column name in CTE: node_alias_column (e.g., "b_user_id")
                        let cte_column = format!("{}_{}", node_alias, column);

                        log::info!(
                            "🔧 Resolved CTE reference: {} -> CTE '{}' (alias '{}'), column '{}'",
                            node_alias,
                            cte_name,
                            cte_alias,
                            cte_column
                        );

                        (cte_alias.to_string(), cte_column)
                    } else {
                        // Not a CTE reference, use as-is
                        (node_alias.to_string(), column.to_string())
                    }
                };

                // Import Direction for bidirectional pattern support
                use crate::query_planner::logical_expr::Direction;

                // Determine if this is an undirected pattern (Direction::Either)
                let is_bidirectional = graph_rel.direction == Direction::Either;

                // JOIN 1: Start node -> Relationship table
                //   For outgoing: r.from_id = a.user_id
                //   For incoming: r.to_id = a.user_id
                //   For either: (r.from_id = a.user_id) OR (r.to_id = a.user_id)
                let rel_join_condition = if is_bidirectional {
                    // Bidirectional: create OR condition for both directions
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let outgoing_cond = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_cols.from_id.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias.clone()),
                                column: PropertyValue::Column(left_column.clone()),
                            }),
                        ],
                    };
                    let incoming_cond = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_cols.to_id.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias.clone()),
                                column: PropertyValue::Column(left_column.clone()),
                            }),
                        ],
                    };
                    OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(outgoing_cond),
                            RenderExpr::OperatorApplicationExp(incoming_cond),
                        ],
                    }
                } else {
                    // Directional: left is always source (from), right is always target (to)
                    // The GraphRel representation normalizes this - direction only affects
                    // how nodes are assigned to left/right during parsing.
                    // JOIN 1: relationship.from_id = left_node.id
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let rel_col = &rel_cols.from_id;
                    OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_table_alias),
                                column: PropertyValue::Column(left_column),
                            }),
                        ],
                    }
                };

                println!(
                    "🔧 DEBUG: About to push JOIN 1 (relationship): {} AS {}",
                    rel_table, graph_rel.alias
                );

                // Compile edge constraints if present
                // Look up relationship schema and check for constraints field
                let mut combined_pre_filter = rel_pre_filter.clone();

                log::info!(
                    "🔍 Edge constraint check: is_bidirectional={}",
                    is_bidirectional
                );

                if !is_bidirectional {
                    // Only compile constraints for directional edges (bidirectional is complex OR condition)
                    // Use the passed schema parameter instead of accessing GLOBAL_SCHEMAS
                    log::info!("🔍 Using passed schema: {}", schema.database());

                    // Get the first relationship type (for multi-type like [:TYPE1|TYPE2], constraints not supported)
                    if let Some(labels_vec) = &graph_rel.labels {
                        log::info!("🔍 Relationship labels: {:?}", labels_vec);
                        if let Some(rel_type) = labels_vec.first() {
                            log::info!("🔍 Looking up relationship type: {}", rel_type);
                            // Look up relationship schema by type using passed schema
                            if let Some(rel_schema) = schema.get_relationships_schema_opt(rel_type)
                            {
                                log::info!(
                                    "🔍 Found relationship schema for {}, constraints={:?}",
                                    rel_type,
                                    rel_schema.constraints
                                );
                                // Check if constraints are defined
                                if let Some(ref constraint_expr) = rel_schema.constraints {
                                    log::info!(
                                        "🔍 Found constraint expression: {}",
                                        constraint_expr
                                    );
                                    // Get node schemas for from/to nodes
                                    log::info!(
                                        "🔍 Node labels: start={:?}, end={:?}",
                                        start_label,
                                        end_label
                                    );
                                    if let (Some(start_label), Some(end_label)) =
                                        (&start_label, &end_label)
                                    {
                                        log::info!(
                                            "🔍 Looking up node schemas: start={}, end={}",
                                            start_label,
                                            end_label
                                        );
                                        if let (Some(from_node_schema), Some(to_node_schema)) = (
                                            schema.node_schema_opt(start_label),
                                            schema.node_schema_opt(end_label),
                                        ) {
                                            log::info!("🔍 Found both node schemas, compiling constraint...");
                                            // Compile the constraint expression
                                            match crate::graph_catalog::constraint_compiler::compile_constraint(
                                                                constraint_expr,
                                                                from_node_schema,
                                                                to_node_schema,
                                                                &graph_rel.left_connection,
                                                                &graph_rel.right_connection,
                                                            ) {
                                                                Ok(compiled_sql) => {
                                                                    log::info!(
                                                                        "✅ Compiled edge constraint for {} (schema={}): {} → {}",
                                                                        graph_rel.alias, schema.database(), constraint_expr, compiled_sql
                                                                    );
                                                                    // Add compiled constraint to pre_filter (will be added to ON clause)
                                                                    let constraint_render_expr = RenderExpr::Raw(compiled_sql);
                                                                    combined_pre_filter = if let Some(existing) = combined_pre_filter {
                                                                        // Combine with existing pre_filter using AND
                                                                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                                                                            operator: Operator::And,
                                                                            operands: vec![existing, constraint_render_expr],
                                                                        }))
                                                                    } else {
                                                                        Some(constraint_render_expr)
                                                                    };
                                                                }
                                                                Err(e) => {
                                                                    log::warn!(
                                                                        "⚠️  Failed to compile edge constraint for {} (schema={}): {}",
                                                                        graph_rel.alias, schema.database(), e
                                                                    );
                                                                }
                                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                joins.push(Join {
                    table_name: rel_table.clone(),
                    table_alias: graph_rel.alias.clone(),
                    joining_on: vec![rel_join_condition],
                    join_type: join_type.clone(),
                    pre_filter: combined_pre_filter,
                    from_id_column: Some(rel_cols.from_id.clone()),
                    to_id_column: Some(rel_cols.to_id.clone()),
                    graph_rel: None,
                });

                // CRITICAL FIX: Handle nested GraphRel patterns differently
                // In nested multi-hop patterns like (post)<-[:HAS_CREATOR]-(f)-[:KNOWS]-(p):
                // - The outer GraphRel (HAS_CREATOR) has right = inner GraphRel (KNOWS)
                // - We need to add a JOIN for the SHARED node (f) connecting to outer rel (t2)
                // - The inner pattern JOINs were already added earlier in this function
                // - Then we skip the normal "JOIN 2" code which would try to add a duplicate
                let right_is_nested_graph_rel =
                    matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_));

                if right_is_nested_graph_rel {
                    println!(
                        "🔍 DEBUG: Nested GraphRel detected for {} - adding shared node JOIN",
                        graph_rel.alias
                    );

                    // The shared node (right_connection, e.g., 'f') needs to be JOINed to OUTER rel (t2)
                    // According to GraphRel convention: right_connection connects to to_id
                    // So: f.id = t2.to_id

                    // Get table info for the shared node
                    // The shared node is embedded inside the inner GraphRel
                    // For inner pattern (p)-[:KNOWS]-(f), if f is right_connection of inner,
                    // then f's table info is in inner_rel.right
                    if let LogicalPlan::GraphRel(inner_rel) = graph_rel.right.as_ref() {
                        let shared_alias = &graph_rel.right_connection;

                        // Determine which side of inner pattern has the shared node
                        let shared_is_inner_right = &inner_rel.right_connection == shared_alias;

                        let (shared_table, shared_id_col) = if shared_is_inner_right {
                            (
                                extract_end_node_table_name(&inner_rel.right),
                                extract_end_node_id_column(&inner_rel.right)
                                    .unwrap_or_else(|| "id".to_string()),
                            )
                        } else {
                            (
                                extract_table_name(&inner_rel.left),
                                extract_id_column(&inner_rel.left)
                                    .unwrap_or_else(|| "id".to_string()),
                            )
                        };

                        if let Some(table_name) = shared_table {
                            // Create JOIN for shared node: f.id = t2.to_id
                            let shared_join_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(shared_alias.clone()),
                                        column: PropertyValue::Column(shared_id_col),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                ],
                            };

                            joins.push(Join {
                                table_name,
                                table_alias: shared_alias.clone(),
                                joining_on: vec![shared_join_condition],
                                join_type: join_type.clone(),
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });

                            println!(
                                "  ✅ Added JOIN for shared node '{}' connecting to outer rel '{}'",
                                shared_alias, graph_rel.alias
                            );
                        }
                    }

                    println!(
                        "📤 DEBUG: GraphRel (alias={}) returning {} total joins (nested pattern)",
                        graph_rel.alias,
                        joins.len()
                    );
                    return Ok(joins);
                }

                // JOIN 2: Relationship table -> End node
                //   For outgoing: b.user_id = r.to_id
                //   For incoming: b.user_id = r.from_id
                //   For either: (b.user_id = r.to_id AND r.from_id = a.user_id) OR (b.user_id = r.from_id AND r.to_id = a.user_id)
                //   Simplified for bidirectional: b.user_id = CASE WHEN r.from_id = a.user_id THEN r.to_id ELSE r.from_id END
                //   Actually simpler: just check b connects to whichever end of r that's NOT a
                let end_join_condition = if is_bidirectional {
                    // For bidirectional, the end node connects to whichever side of r that ISN'T the start node
                    // This is expressed as: (b.id = r.to_id AND r.from_id = a.id) OR (b.id = r.from_id AND r.to_id = a.id)
                    let (left_table_alias, left_column) =
                        resolve_cte_reference(&graph_rel.left_connection, &start_id_col);
                    let (right_table_alias, right_column) =
                        resolve_cte_reference(&graph_rel.right_connection, &end_id_col);
                    let outgoing_side = OperatorApplication {
                        operator: Operator::And,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_table_alias.clone()),
                                        column: PropertyValue::Column(right_column.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                ],
                            }),
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_table_alias.clone()),
                                        column: PropertyValue::Column(left_column.clone()),
                                    }),
                                ],
                            }),
                        ],
                    };
                    let incoming_side = OperatorApplication {
                        operator: Operator::And,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_table_alias.clone()),
                                        column: PropertyValue::Column(right_column.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.from_id.clone()),
                                    }),
                                ],
                            }),
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: PropertyValue::Column(rel_cols.to_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_table_alias.clone()),
                                        column: PropertyValue::Column(left_column.clone()),
                                    }),
                                ],
                            }),
                        ],
                    };
                    OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            RenderExpr::OperatorApplicationExp(outgoing_side),
                            RenderExpr::OperatorApplicationExp(incoming_side),
                        ],
                    }
                } else {
                    // Directional: right is always target (to)
                    // JOIN 2: right_node.id = relationship.to_id
                    let (right_table_alias, right_column) =
                        resolve_cte_reference(&graph_rel.right_connection, &end_id_col);
                    let rel_col = &rel_cols.to_id;
                    OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_table_alias),
                                column: PropertyValue::Column(right_column),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(graph_rel.alias.clone()),
                                column: PropertyValue::Column(rel_col.clone()),
                            }),
                        ],
                    }
                };

                println!(
                    "🔧 DEBUG: About to push JOIN 2 (end node): {} AS {}",
                    end_table, graph_rel.right_connection
                );

                // DENORMALIZED EDGE CHECK: Handle denormalized relationships where end node table == relationship table
                // For denormalized edges (e.g., AUTHORED with posts_bench as both edge and end node),
                // we still need JOIN 2, but it's a self-join on the relationship table
                // Example: AUTHORED relationship uses posts_bench for both relationship and Post node
                // - Relationship join: posts_bench AS r2 ON r2.author_id = a.user_id
                // - End node join: posts_bench AS d ON d.post_id = r2.post_id
                println!(
                    "🔧 DEBUG: Checking denormalized: end_table='{}', rel_table='{}', equal={}",
                    end_table,
                    rel_table,
                    end_table == rel_table
                );
                if end_table != rel_table {
                    // Standard case: different tables for relationship and end node
                    joins.push(Join {
                        table_name: end_table,
                        table_alias: graph_rel.right_connection.clone(),
                        joining_on: vec![end_join_condition],
                        join_type,
                        pre_filter: right_node_pre_filter.clone(),
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    });
                } else {
                    // Denormalized case: end_table == rel_table (same physical table)
                    // Still need JOIN 2 as a self-join on the relationship table
                    // The end node gets its own alias pointing to the same table
                    println!("🔧 DEBUG: Adding denormalized end node JOIN: {} AS {} (same table as relationship {})",
                             rel_table, graph_rel.right_connection, graph_rel.alias);
                    joins.push(Join {
                        table_name: rel_table.clone(), // Same table as relationship
                        table_alias: graph_rel.right_connection.clone(), // End node alias
                        joining_on: vec![end_join_condition], // Connects end node to relationship
                        join_type,
                        pre_filter: right_node_pre_filter.clone(),
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    });
                    println!(
                        "✓ Denormalized relationship: added self-join for end node {} on table '{}'",
                        graph_rel.right_connection, rel_table
                    );
                    log::info!(
                        "✓ Denormalized relationship for {}: table '{}' serves as both edge and end node, added self-join",
                        graph_rel.alias, rel_table
                    );
                }

                println!(
                    "📤 DEBUG: GraphRel (alias={}) returning {} total joins",
                    graph_rel.alias,
                    joins.len()
                );
                joins
            }
            LogicalPlan::CartesianProduct(cp) => {
                // For CartesianProduct, generate JOIN with ON clause if join_condition exists
                // or CROSS JOIN semantics if no join_condition
                let mut joins = <LogicalPlan as JoinBuilder>::extract_joins(&cp.left, schema)?;

                // Check if right side is a GraphRel - OPTIONAL MATCH case needs special handling
                if let LogicalPlan::GraphRel(graph_rel) = cp.right.as_ref() {
                    // OPTIONAL MATCH with GraphRel pattern
                    // Need to determine which connection is the anchor (already defined in cp.left)
                    // and generate joins in the correct order

                    // Get the anchor alias from cp.left (the base pattern)
                    let anchor_alias = get_anchor_alias_from_plan(&cp.left);
                    crate::debug_print!(
                        "CartesianProduct with GraphRel: anchor_alias={:?}",
                        anchor_alias
                    );
                    crate::debug_print!(
                        "  left_connection={}, right_connection={}",
                        graph_rel.left_connection,
                        graph_rel.right_connection
                    );

                    // Determine if anchor is on left or right
                    let anchor_is_right = anchor_alias
                        .as_ref()
                        .map(|a| a == &graph_rel.right_connection)
                        .unwrap_or(false);

                    if cp.is_optional && anchor_is_right {
                        // OPTIONAL MATCH where anchor is on right side
                        // e.g., MATCH (post:Post) OPTIONAL MATCH (liker:Person)-[:LIKES]->(post)
                        // Anchor is 'post' (right_connection), new node is 'liker' (left_connection)
                        crate::debug_print!("  -> Anchor is on RIGHT, generating swapped joins");

                        let swapped_joins = generate_swapped_joins_for_optional_match(graph_rel)?;
                        joins.extend(swapped_joins);
                    } else {
                        // Normal case: anchor is on left, or non-optional

                        // CRITICAL FIX: For cross-table WITH patterns where the GraphRel's left_connection
                        // is NOT the anchor (i.e., it's a NEW node that needs to be joined), we must add
                        // a join for the left_connection node BEFORE adding the relationship and end node joins.
                        //
                        // Example: MATCH (a)-[:FOLLOWS]->(b) WITH a, b MATCH (c)-[:AUTHORED]->(d) WHERE a.id = c.id
                        // - anchor_alias is None or "a_b" (from CTE)
                        // - graph_rel.left_connection is "c" (NOT the anchor)
                        // - We need: JOIN users_bench AS c, then JOIN posts_bench AS r2, then JOIN posts_bench AS d
                        // - But standard extract_joins only adds r2 and d joins, not c

                        let anchor_is_left = anchor_alias
                            .as_ref()
                            .map(|a| a == &graph_rel.left_connection)
                            .unwrap_or(false);

                        crate::debug_print!(
                            "  anchor_is_left={}, anchor_alias={:?}, left_connection={}",
                            anchor_is_left,
                            anchor_alias,
                            graph_rel.left_connection
                        );

                        if !anchor_is_left {
                            // The GraphRel's left_connection is a NEW node that needs its own JOIN
                            // Get the table info for this node
                            if let LogicalPlan::GraphNode(left_node) = graph_rel.left.as_ref() {
                                if let LogicalPlan::ViewScan(vs) = left_node.input.as_ref() {
                                    let left_table_name = vs.source_table.clone();
                                    let left_table_alias = graph_rel.left_connection.clone();

                                    // Build the join condition from cp.join_condition
                                    // This is the correlation predicate (e.g., a.user_id = c.user_id)
                                    let join_conditions =
                                        if let Some(ref join_cond) = cp.join_condition {
                                            if let Ok(RenderExpr::OperatorApplicationExp(op)) =
                                                RenderExpr::try_from(join_cond.clone())
                                            {
                                                vec![op]
                                            } else {
                                                vec![]
                                            }
                                        } else {
                                            vec![]
                                        };

                                    crate::debug_print!(
                                        "  Adding JOIN for left_connection '{}': {} with {} conditions",
                                        left_table_alias, left_table_name, join_conditions.len()
                                    );

                                    let join_type = if cp.is_optional {
                                        JoinType::Left
                                    } else {
                                        JoinType::Inner
                                    };

                                    joins.push(super::Join {
                                        table_name: left_table_name,
                                        table_alias: left_table_alias,
                                        joining_on: join_conditions,
                                        join_type,
                                        pre_filter: None,
                                        from_id_column: None,
                                        to_id_column: None,
                                        graph_rel: None,
                                    });
                                }
                            }
                        }

                        // Now add the standard joins from the GraphRel (relationship and end node)
                        joins.extend(<LogicalPlan as JoinBuilder>::extract_joins(
                            &cp.right, schema,
                        )?);
                    }
                } else {
                    // Non-GraphRel right side (e.g., simple node patterns)
                    // Get the right side's FROM table to create a JOIN
                    if let Some(right_from) = cp.right.as_ref().extract_from()? {
                        if let Some(right_table) = right_from.table {
                            // Convert join_condition to OperatorApplication for the ON clause
                            let joining_on = if let Some(ref join_cond) = cp.join_condition {
                                // Convert LogicalExpr to RenderExpr, then extract OperatorApplication
                                let render_expr: Result<RenderExpr, _> =
                                    join_cond.clone().try_into();
                                match render_expr {
                                    Ok(RenderExpr::OperatorApplicationExp(op)) => vec![op],
                                    Ok(_other) => {
                                        // Wrap non-operator expressions in equality check
                                        crate::debug_print!("CartesianProduct: join_condition is not OperatorApplication: {:?}", _other);
                                        vec![]
                                    }
                                    Err(_e) => {
                                        crate::debug_print!("CartesianProduct: Failed to convert join_condition: {:?}", _e);
                                        vec![]
                                    }
                                }
                            } else {
                                vec![] // No join condition - pure CROSS JOIN semantics
                            };

                            // Determine join type:
                            // - OPTIONAL: LEFT JOIN
                            // - With join condition: INNER JOIN (has ON clause)
                            // - No join condition: JoinType::Join renders as CROSS JOIN
                            let join_type = if cp.is_optional {
                                JoinType::Left
                            } else if joining_on.is_empty() {
                                JoinType::Join // Renders as CROSS JOIN when joining_on is empty
                            } else {
                                JoinType::Inner
                            };

                            crate::debug_print!("CartesianProduct extract_joins: table={}, alias={}, joining_on={:?}",
                                right_table.name, right_table.alias.as_deref().unwrap_or(""), joining_on);

                            joins.push(super::Join {
                                table_name: right_table.name.clone(),
                                table_alias: right_table.alias.clone().unwrap_or_default(),
                                joining_on,
                                join_type,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            });
                        }
                    }

                    // Include any joins from the right side
                    joins.extend(<LogicalPlan as JoinBuilder>::extract_joins(
                        &cp.right, schema,
                    )?);
                }

                joins
            }
            _ => vec![],
        };
        Ok(joins)
    }
}
