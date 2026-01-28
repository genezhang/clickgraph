//! FROM clause builder for RenderPlanBuilder
//!
//! This module contains the `FromBuilder` trait and its implementation for extracting
//! FROM clauses from logical plans. The FROM clause determines the primary data source
//! for a query and is essential for all SQL queries.
//!
//! ## Architecture
//!
//! The `FromBuilder` trait provides a single method `extract_from()` that:
//! - Traverses the logical plan tree
//! - Identifies the primary table/CTE to use in FROM clause
//! - Handles special cases:
//!   - ViewScan nodes (direct table references)
//!   - GraphNode/GraphRel (graph pattern traversal)
//!   - Variable-length paths (VLP CTEs)
//!   - Denormalized edge tables
//!   - Optional matches (LEFT JOIN anchors)
//!   - GraphJoins (FROM marker detection)
//!   - CartesianProduct (WITH...MATCH patterns)
//!
//! ## Design Patterns
//!
//! 1. **Recursive Traversal**: Most LogicalPlan variants delegate to their input plan
//! 2. **Pattern Matching**: Different plan types require different FROM resolution logic
//! 3. **CTE Naming**: Variable-length paths and WITH clauses create CTEs that become FROM sources
//! 4. **Anchor Selection**: For optional matches and joins, find the non-optional anchor node
//!
//! ## Integration
//!
//! This module is part of Phase 2 modularization:
//! - Week 3: join_builder.rs ✅
//! - Week 4: select_builder.rs ✅
//! - **Week 5: from_builder.rs** ← Current
//! - Week 6: group_by_builder.rs (planned)

use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::utils::cte_naming::{extract_cte_base_name, is_generated_cte_name};
use log::debug;
use std::sync::Arc;

use super::errors::RenderBuildError;
use super::plan_builder_helpers::{
    extract_rel_and_node_tables, extract_table_name, find_anchor_node, find_table_name_for_alias,
    get_all_relationship_connections, is_node_denormalized,
};
use super::view_table_ref::{from_table_to_view_ref, view_ref_to_from_table};
use super::{FromTable, ViewTableRef};

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting FROM clauses from logical plans
///
/// This trait provides the `extract_from()` method which analyzes a logical plan
/// and determines the appropriate FROM clause for the rendered SQL query.
///
/// # Examples
///
/// ```ignore
/// use clickgraph::render_plan::from_builder::FromBuilder;
///
/// let from_table = logical_plan.extract_from()?;
/// ```
pub trait FromBuilder {
    /// Extract FROM clause from this logical plan
    ///
    /// Returns an optional FromTable representing the primary data source.
    /// Returns None for plans that don't have a direct table source (e.g., Union, PageRank).
    ///
    /// # Errors
    ///
    /// Returns RenderBuildError if:
    /// - Table name cannot be resolved for a required alias
    /// - GraphJoins has no FROM marker and no valid anchor
    /// - Plan structure is invalid or unsupported
    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>>;
}

impl FromBuilder for LogicalPlan {
    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>> {
        log::debug!(
            "🔍 extract_from START: plan type={:?}",
            std::mem::discriminant(self)
        );

        let from_ref = match &self {
            LogicalPlan::Empty => None,

            LogicalPlan::ViewScan(scan) => {
                // Check if this is a relationship ViewScan (has from_id/to_id)
                if scan.from_id.is_some() && scan.to_id.is_some() {
                    // For denormalized edges, use the actual table name directly
                    // CTE references (rel_*) are only needed for standard edges with separate node tables
                    // Denormalized ViewScans have from_node_properties/to_node_properties indicating
                    // node data is stored on the edge table itself
                    let use_actual_table =
                        scan.from_node_properties.is_some() && scan.to_node_properties.is_some();

                    debug!("📊 extract_from ViewScan: source_table={}, from_props={:?}, to_props={:?}, use_actual_table={}",
                        scan.source_table,
                        scan.from_node_properties.as_ref().map(|p| p.len()),
                        scan.to_node_properties.as_ref().map(|p| p.len()),
                        use_actual_table);

                    if use_actual_table {
                        // Denormalized: use actual table name
                        debug!("✅ Using actual table name: {}", scan.source_table);
                        Some(ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            scan.source_table.clone(),
                        ))
                    } else {
                        // Standard edge: use CTE reference
                        let cte_name =
                            format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""));
                        debug!("🔄 Using CTE reference: {}", cte_name);
                        Some(ViewTableRef::new_table(scan.as_ref().clone(), cte_name))
                    }
                } else {
                    // For node ViewScans, use the table name
                    Some(ViewTableRef::new_table(
                        scan.as_ref().clone(),
                        scan.source_table.clone(),
                    ))
                }
            }

            LogicalPlan::GraphNode(graph_node) => {
                // For GraphNode, extract FROM from the input but use this GraphNode's alias
                // CROSS JOINs for multiple standalone nodes are handled in extract_joins
                log::debug!(
                    "GraphNode.extract_from() - alias: {}, input: {:?}",
                    graph_node.alias,
                    graph_node.input
                );
                match &*graph_node.input {
                    LogicalPlan::ViewScan(scan) => {
                        log::debug!(
                            "GraphNode.extract_from() - matched ViewScan, table: {}",
                            scan.source_table
                        );
                        // Check if this is a relationship ViewScan (has from_id/to_id)
                        let table_or_cte_name = if scan.from_id.is_some() && scan.to_id.is_some() {
                            // For denormalized edges, use actual table; for standard edges, use CTE
                            let use_actual_table = scan.from_node_properties.is_some()
                                && scan.to_node_properties.is_some();
                            if use_actual_table {
                                scan.source_table.clone()
                            } else {
                                format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""))
                            }
                        } else {
                            // For node ViewScans, use the table name
                            scan.source_table.clone()
                        };
                        // ViewScan already returns ViewTableRef, just update the alias
                        let mut view_ref =
                            ViewTableRef::new_table(scan.as_ref().clone(), table_or_cte_name);
                        view_ref.alias = Some(graph_node.alias.clone());
                        log::debug!(
                            "GraphNode.extract_from() - created ViewTableRef: {:?}",
                            view_ref
                        );
                        Some(view_ref)
                    }
                    _ => {
                        log::debug!(
                            "GraphNode.extract_from() - not a ViewScan, input type: {:?}",
                            graph_node.input
                        );
                        // For other input types, extract FROM and convert
                        let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                        // Use this GraphNode's alias
                        if let Some(ref mut view_ref) = from_ref {
                            view_ref.alias = Some(graph_node.alias.clone());
                        }
                        from_ref
                    }
                }
            }

            LogicalPlan::GraphRel(graph_rel) => self.extract_from_graph_rel(graph_rel)?,

            LogicalPlan::Filter(filter) => {
                log::debug!(
                    "  → Filter, recursing to input type={:?}",
                    std::mem::discriminant(filter.input.as_ref())
                );
                from_table_to_view_ref(filter.input.extract_from()?)
            }

            LogicalPlan::Projection(projection) => {
                log::debug!(
                    "  → Projection, recursing to input type={:?}",
                    std::mem::discriminant(projection.input.as_ref())
                );
                from_table_to_view_ref(projection.input.extract_from()?)
            }

            LogicalPlan::GraphJoins(graph_joins) => self.extract_from_graph_joins(graph_joins)?,

            LogicalPlan::GroupBy(group_by) => {
                from_table_to_view_ref(group_by.input.extract_from()?)
            }

            LogicalPlan::OrderBy(order_by) => {
                from_table_to_view_ref(order_by.input.extract_from()?)
            }

            LogicalPlan::Skip(skip) => from_table_to_view_ref(skip.input.extract_from()?),

            LogicalPlan::Limit(limit) => from_table_to_view_ref(limit.input.extract_from()?),

            LogicalPlan::Cte(cte) => from_table_to_view_ref(cte.input.extract_from()?),

            LogicalPlan::Union(_) => None,

            LogicalPlan::PageRank(_) => None,

            LogicalPlan::Unwind(u) => from_table_to_view_ref(u.input.extract_from()?),

            LogicalPlan::CartesianProduct(cp) => {
                // Try left side first (for most queries)
                let left_from = cp.left.extract_from()?;
                if left_from.is_some() {
                    // Left has a table, use it (normal case)
                    from_table_to_view_ref(left_from)
                } else {
                    // Left has no FROM (e.g., WITH clause creating a CTE)
                    // Use right side as FROM source (e.g., new MATCH after WITH)
                    log::info!(
                        "CartesianProduct: Left side has no FROM (likely CTE), using right side"
                    );
                    from_table_to_view_ref(cp.right.extract_from()?)
                }
            }

            LogicalPlan::WithClause(wc) => from_table_to_view_ref(wc.input.extract_from()?),
        };

        Ok(view_ref_to_from_table(from_ref))
    }
}

impl LogicalPlan {
    /// Extract FROM clause for GraphRel patterns
    ///
    /// Handles:
    /// - Variable-length paths (VLP CTEs)
    /// - Denormalized edge tables
    /// - Anonymous edge patterns
    /// - Optional matches with anchor selection
    fn extract_from_graph_rel(
        &self,
        graph_rel: &crate::query_planner::logical_plan::GraphRel,
    ) -> RenderPlanBuilderResult<Option<ViewTableRef>> {
        use crate::query_planner::logical_plan::GraphRel;

        // DENORMALIZED EDGE TABLE CHECK
        // For denormalized patterns, both nodes are virtual - use relationship table as FROM
        let left_is_denormalized = is_node_denormalized(&graph_rel.left);
        let right_is_denormalized = is_node_denormalized(&graph_rel.right);

        log::debug!(
            "🔍 extract_from GraphRel: alias='{}', left_is_denorm={}, right_is_denorm={}, is_optional={:?}",
            graph_rel.alias,
            left_is_denormalized,
            right_is_denormalized,
            graph_rel.is_optional
        );

        // VARIABLE-LENGTH PATH CHECK
        // For variable-length paths, use the CTE name as FROM UNLESS it's optional
        if graph_rel.variable_length.is_some() {
            let is_optional = graph_rel.is_optional.unwrap_or(false);

            if is_optional {
                // OPTIONAL VLP: Don't use VLP CTE as FROM. Instead, let the anchor node (from required MATCH)
                // become FROM, and the VLP CTE will be added as a LEFT JOIN later.
                // This ensures that rows where the start node has no outgoing paths still appear in results.
                log::debug!("✓ OPTIONAL VLP: Skipping CTE as FROM, will use anchor node instead");
                return Ok(None); // Return None to let GraphJoins use its anchor_table
            }

            // Non-optional VLP: Use CTE as FROM
            log::debug!("✓ VARIABLE-LENGTH pattern: using CTE as FROM");

            // TODO: Extract CTE naming logic to shared utility function
            // Current duplication: plan_builder.rs (here and lines 1285-1304),
            // plan_builder_utils.rs (lines 1152-1167 with multi-type support)
            // Note: This handles single-type VLP only. Multi-type VLP uses
            // vlp_multi_type_{start}_{end} format (see plan_builder_utils.rs)
            let start_alias = &graph_rel.left_connection;
            let end_alias = &graph_rel.right_connection;
            let cte_name = format!("vlp_{}_{}", start_alias, end_alias);

            log::debug!(
                "✓ Using CTE '{}' as FROM for variable-length path",
                cte_name
            );

            return Ok(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: cte_name,
                alias: graph_rel.path_variable.clone(),
                use_final: false,
            }));
        }

        if left_is_denormalized && right_is_denormalized {
            log::debug!(
                "✓ DENORMALIZED pattern: both nodes on edge table, using edge table as FROM"
            );

            // For multi-hop denormalized, find the first (leftmost) relationship
            fn find_first_graph_rel(graph_rel: &GraphRel) -> &GraphRel {
                match graph_rel.left.as_ref() {
                    LogicalPlan::GraphRel(left_rel) => find_first_graph_rel(left_rel),
                    _ => graph_rel,
                }
            }

            let first_graph_rel = find_first_graph_rel(graph_rel);

            // Try ViewScan first (normal case)
            if let LogicalPlan::ViewScan(scan) = first_graph_rel.center.as_ref() {
                log::debug!(
                    "✓ Using ViewScan edge table '{}' AS '{}'",
                    scan.source_table,
                    first_graph_rel.alias
                );
                return Ok(Some(ViewTableRef {
                    source: first_graph_rel.center.clone(),
                    name: scan.source_table.clone(),
                    alias: Some(first_graph_rel.alias.clone()),
                    use_final: scan.use_final,
                }));
            }

            log::debug!(
                "⚠️  Could not extract edge table from center (type: {:?})",
                std::mem::discriminant(first_graph_rel.center.as_ref())
            );
        }

        // Check if both nodes are anonymous (edge-driven query)
        let left_table_name = extract_table_name(&graph_rel.left);
        let right_table_name = extract_table_name(&graph_rel.right);

        // If both nodes are anonymous, use the relationship table as FROM
        if left_table_name.is_none() && right_table_name.is_none() {
            // Edge-driven query: use relationship table directly (not as CTE)
            // Extract table name from the relationship ViewScan
            if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                // Use actual table name, not CTE name
                return Ok(Some(ViewTableRef::new_table(
                    scan.as_ref().clone(),
                    scan.source_table.clone(),
                )));
            }
            // Fallback to normal extraction if not a ViewScan
            return Ok(None);
        }

        // For GraphRel with labeled nodes, we need to include the start node in the FROM clause
        // This handles simple relationship queries where the start node should be FROM

        // ALWAYS use left node as FROM for relationship patterns.
        // The is_optional flag determines JOIN type (INNER vs LEFT), not FROM table selection.
        //
        // For `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`:
        //   - a is the left connection (required, already defined)
        //   - b is the right connection (optional, newly introduced)
        //   - FROM should be `a`, with LEFT JOIN to relationship and `b`
        //
        // For `MATCH (a) OPTIONAL MATCH (b)-[:R]->(a)`:
        //   - b is the left connection (optional, newly introduced)
        //   - a is the right connection (required, already defined)
        //   - FROM should be `a` (the required one), but the pattern structure has `b` on left
        //   - This case needs special handling: find which connection is NOT optional

        log::debug!("graph_rel.is_optional = {:?}", graph_rel.is_optional);

        // Use left as primary, right as fallback
        let (primary_from, fallback_from) = (
            graph_rel.left.extract_from(),
            graph_rel.right.extract_from(),
        );

        crate::debug_println!("DEBUG: primary_from = {:?}", primary_from);
        crate::debug_println!("DEBUG: fallback_from = {:?}", fallback_from);

        if let Ok(Some(from_table)) = primary_from {
            Ok(from_table_to_view_ref(Some(from_table)))
        } else {
            // If primary node doesn't have FROM, try fallback
            let right_from = fallback_from;
            crate::debug_println!("DEBUG: Using fallback FROM");
            crate::debug_println!("DEBUG: right_from = {:?}", right_from);

            if let Ok(Some(from_table)) = right_from {
                Ok(from_table_to_view_ref(Some(from_table)))
            } else {
                // If right also doesn't have FROM, check if right contains a nested GraphRel
                if let LogicalPlan::GraphRel(nested_graph_rel) = graph_rel.right.as_ref() {
                    // Extract FROM from the nested GraphRel's left node
                    let nested_left_from = nested_graph_rel.left.extract_from();
                    crate::debug_println!(
                        "DEBUG: nested_graph_rel.left = {:?}",
                        nested_graph_rel.left
                    );
                    crate::debug_println!("DEBUG: nested_left_from = {:?}", nested_left_from);

                    if let Ok(Some(nested_from_table)) = nested_left_from {
                        Ok(from_table_to_view_ref(Some(nested_from_table)))
                    } else {
                        // If nested left also doesn't have FROM, create one from the nested left_connection alias
                        let table_name =
                            extract_table_name(&nested_graph_rel.left).ok_or_else(|| {
                                RenderBuildError::TableNameNotFound(format!(
                                    "Could not resolve table name for alias '{}', plan: {:?}",
                                    nested_graph_rel.left_connection, nested_graph_rel.left
                                ))
                            })?;

                        Ok(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::Empty),
                            name: table_name,
                            alias: Some(nested_graph_rel.left_connection.clone()),
                            use_final: false,
                        }))
                    }
                } else {
                    // If right doesn't have FROM, we need to determine which node should be the anchor
                    // Use find_anchor_node logic to choose the correct anchor
                    let all_connections = get_all_relationship_connections(self);
                    let optional_aliases = std::collections::HashSet::new();
                    let denormalized_aliases = std::collections::HashSet::new();

                    if let Some(anchor_alias) =
                        find_anchor_node(&all_connections, &optional_aliases, &denormalized_aliases)
                    {
                        // Determine which node (left or right) the anchor corresponds to
                        let (table_plan, connection_alias) =
                            if anchor_alias == graph_rel.left_connection {
                                (&graph_rel.left, &graph_rel.left_connection)
                            } else {
                                (&graph_rel.right, &graph_rel.right_connection)
                            };

                        let table_name = extract_table_name(table_plan).ok_or_else(|| {
                            RenderBuildError::TableNameNotFound(format!(
                                "Could not resolve table name for anchor alias '{}', plan: {:?}",
                                connection_alias, table_plan
                            ))
                        })?;

                        Ok(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::Empty),
                            name: table_name,
                            alias: Some(connection_alias.clone()),
                            use_final: false,
                        }))
                    } else {
                        // Fallback: use left_connection as anchor (traditional behavior)
                        let table_name = extract_table_name(&graph_rel.left).ok_or_else(|| {
                            RenderBuildError::TableNameNotFound(format!(
                                "Could not resolve table name for alias '{}', plan: {:?}",
                                graph_rel.left_connection, graph_rel.left
                            ))
                        })?;

                        Ok(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::Empty),
                            name: table_name,
                            alias: Some(graph_rel.left_connection.clone()),
                            use_final: false,
                        }))
                    }
                }
            }
        }
    }

    /// Extract FROM clause for GraphJoins patterns
    ///
    /// GraphJoins represent the result of graph pattern analysis where all tables
    /// and their join conditions have been identified. The FROM clause is determined by:
    ///
    /// 1. **FROM Marker**: A Join with empty `joining_on` (no join conditions)
    /// 2. **Special Cases**: Variable-length paths, denormalized edges, node-only queries
    /// 3. **Anchor Table**: For optional matches, use the non-optional anchor
    /// 4. **CTE References**: For WITH clauses, use the CTE as FROM
    fn extract_from_graph_joins(
        &self,
        graph_joins: &crate::query_planner::logical_plan::GraphJoins,
    ) -> RenderPlanBuilderResult<Option<ViewTableRef>> {
        use crate::query_planner::logical_plan::{CartesianProduct, GraphNode, GraphRel};

        // ============================================================================
        // CLEAN DESIGN: FROM table determination for GraphJoins
        // ============================================================================
        //
        // The logical model is simple:
        // 1. Every table in a graph query is represented as a Join in graph_joins.joins
        // 2. A Join with EMPTY joining_on is a FROM marker (no join conditions = base table)
        // 3. A Join with NON-EMPTY joining_on is a real JOIN
        // 4. There should be exactly ONE FROM marker per GraphJoins
        //
        // This function finds that FROM marker and returns it.
        // NO FALLBACKS. If there's no FROM marker, something is wrong upstream.
        // ============================================================================

        log::debug!(
            "🔍 GraphJoins.extract_from: {} joins, anchor_table={:?}",
            graph_joins.joins.len(),
            graph_joins.anchor_table
        );

        // 🔧 PARAMETERIZED VIEW FIX: Get parameterized table references from input plan
        let parameterized_tables = extract_rel_and_node_tables(&graph_joins.input);

        // STEP 1: Find FROM marker (Join with empty joining_on)
        // This is the authoritative source - it was set by graph_join_inference
        for join in &graph_joins.joins {
            if join.joining_on.is_empty() {
                // 🔧 PARAMETERIZED VIEW FIX: Use parameterized table reference if available
                let table_name = parameterized_tables
                    .get(&join.table_alias)
                    .cloned()
                    .unwrap_or_else(|| join.table_name.clone());

                log::info!(
                    "✅ Found FROM marker: table='{}' (original='{}') alias='{}'",
                    table_name,
                    join.table_name,
                    join.table_alias
                );
                return Ok(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table_name,
                    alias: Some(join.table_alias.clone()),
                    use_final: false,
                }));
            }
        }

        // STEP 2: No FROM marker found - check special cases that don't use joins

        // Helper to find GraphRel through wrappers
        fn find_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
            match plan {
                LogicalPlan::GraphRel(gr) => Some(gr),
                LogicalPlan::Projection(proj) => find_graph_rel(&proj.input),
                LogicalPlan::Filter(filter) => find_graph_rel(&filter.input),
                LogicalPlan::GroupBy(group_by) => find_graph_rel(&group_by.input),
                LogicalPlan::Unwind(u) => find_graph_rel(&u.input),
                LogicalPlan::GraphJoins(gj) => find_graph_rel(&gj.input),
                _ => None,
            }
        }

        // Helper to find VLP GraphRel specifically (for chained patterns)
        // This traverses the entire plan tree to find a GraphRel with variable_length
        fn find_vlp_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
            match plan {
                LogicalPlan::GraphRel(gr) => {
                    // Check this GraphRel first
                    if gr.variable_length.is_some() {
                        return Some(gr);
                    }
                    // Check left side (for chained patterns like (a)-[*]->(b)-[:REL]->(c))
                    if let Some(vlp) = find_vlp_graph_rel(&gr.left) {
                        return Some(vlp);
                    }
                    // Check right side
                    find_vlp_graph_rel(&gr.right)
                }
                LogicalPlan::Projection(proj) => find_vlp_graph_rel(&proj.input),
                LogicalPlan::Filter(filter) => find_vlp_graph_rel(&filter.input),
                LogicalPlan::GroupBy(group_by) => find_vlp_graph_rel(&group_by.input),
                LogicalPlan::Unwind(u) => find_vlp_graph_rel(&u.input),
                LogicalPlan::GraphJoins(gj) => find_vlp_graph_rel(&gj.input),
                LogicalPlan::GraphNode(gn) => find_vlp_graph_rel(&gn.input),
                _ => None,
            }
        }

        // Helper to find GraphNode for node-only queries
        fn find_graph_node(plan: &LogicalPlan) -> Option<&GraphNode> {
            match plan {
                LogicalPlan::GraphNode(gn) => Some(gn),
                LogicalPlan::Projection(proj) => find_graph_node(&proj.input),
                LogicalPlan::Filter(filter) => find_graph_node(&filter.input),
                LogicalPlan::GroupBy(group_by) => find_graph_node(&group_by.input),
                LogicalPlan::Unwind(u) => find_graph_node(&u.input),
                LogicalPlan::GraphJoins(gj) => find_graph_node(&gj.input),
                _ => None,
            }
        }

        // Helper to find CartesianProduct
        fn find_cartesian_product(plan: &LogicalPlan) -> Option<&CartesianProduct> {
            match plan {
                LogicalPlan::CartesianProduct(cp) => Some(cp),
                LogicalPlan::Filter(f) => find_cartesian_product(&f.input),
                LogicalPlan::Projection(p) => find_cartesian_product(&p.input),
                _ => None,
            }
        }

        fn is_cte_reference(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::WithClause(_) => true,
                LogicalPlan::ViewScan(vs) => is_generated_cte_name(&vs.source_table),
                LogicalPlan::GraphNode(gn) => is_cte_reference(&gn.input),
                LogicalPlan::Projection(p) => is_cte_reference(&p.input),
                LogicalPlan::Filter(f) => is_cte_reference(&f.input),
                _ => false,
            }
        }

        // CASE A: Empty joins - check for denormalized edge or node-only patterns
        if graph_joins.joins.is_empty() {
            log::debug!("📋 No joins - checking for special patterns");

            // A.1: Check for variable-length path FIRST (before other checks)
            // Use find_vlp_graph_rel to search entire plan tree (handles chained patterns)
            if let Some(graph_rel) = find_vlp_graph_rel(&graph_joins.input) {
                let is_optional = graph_rel.is_optional.unwrap_or(false);

                if is_optional {
                    // OPTIONAL VLP: Don't use VLP CTE as FROM. Instead, find the anchor node
                    // (from required MATCH) and use it as FROM. The VLP CTE will be added
                    // as a LEFT JOIN later. This ensures rows where start node has no paths
                    // still appear in results.
                    log::info!(
                        "🎯 OPTIONAL VLP: Not using CTE as FROM, finding anchor node instead"
                    );

                    // Find the start node (GraphNode) in the VLP pattern - it should be FROM
                    if let LogicalPlan::GraphNode(start_node) = graph_rel.left.as_ref() {
                        if let LogicalPlan::ViewScan(scan) = start_node.input.as_ref() {
                            log::info!(
                                "✓ OPTIONAL VLP: Using anchor node '{}' from table '{}' as FROM",
                                start_node.alias,
                                scan.source_table
                            );
                            return Ok(Some(ViewTableRef {
                                source: Arc::new(LogicalPlan::GraphNode(start_node.clone())),
                                name: scan.source_table.clone(),
                                alias: Some(start_node.alias.clone()),
                                use_final: scan.use_final,
                            }));
                        }
                    }

                    log::warn!("⚠️ OPTIONAL VLP: Could not find anchor node, falling through");
                    // Fall through to try other patterns
                } else {
                    // Non-optional VLP: Use CTE as FROM
                    log::info!(
                        "🎯 VARIABLE-LENGTH: Using CTE as FROM for path '{}'",
                        graph_rel.alias
                    );

                    // TODO: Extract CTE naming logic to shared utility function
                    // Current duplication: plan_builder.rs (here and lines 965-986),
                    // plan_builder_utils.rs (lines 1152-1167 with multi-type support)
                    // Note: This handles single-type VLP only. Multi-type VLP uses
                    // vlp_multi_type_{start}_{end} format (see plan_builder_utils.rs)
                    let start_alias = &graph_rel.left_connection;
                    let end_alias = &graph_rel.right_connection;
                    let cte_name = format!("vlp_{}_{}", start_alias, end_alias);

                    return Ok(Some(ViewTableRef {
                        source: Arc::new(LogicalPlan::Empty),
                        name: cte_name,
                        alias: Some(VLP_CTE_FROM_ALIAS.to_string()), // Standard VLP alias
                        use_final: false,
                    }));
                }
            }

            // A.2: Denormalized edge pattern - use edge table directly
            if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                if let LogicalPlan::ViewScan(rel_scan) = graph_rel.center.as_ref() {
                    if rel_scan.from_node_properties.is_some()
                        || rel_scan.to_node_properties.is_some()
                    {
                        log::info!(
                            "🎯 DENORMALIZED: Using edge table '{}' as FROM",
                            rel_scan.source_table
                        );
                        return Ok(Some(ViewTableRef {
                            source: graph_rel.center.clone(),
                            name: rel_scan.source_table.clone(),
                            alias: Some(graph_rel.alias.clone()),
                            use_final: rel_scan.use_final,
                        }));
                    }
                }

                // A.3: Polymorphic edge - use the labeled node
                if let LogicalPlan::GraphNode(left_node) = graph_rel.left.as_ref() {
                    if let LogicalPlan::ViewScan(scan) = left_node.input.as_ref() {
                        log::info!(
                            "🎯 POLYMORPHIC: Using left node '{}' as FROM",
                            left_node.alias
                        );
                        return Ok(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::GraphNode(left_node.clone())),
                            name: scan.source_table.clone(),
                            alias: Some(left_node.alias.clone()),
                            use_final: scan.use_final,
                        }));
                    }
                }
                if let LogicalPlan::GraphNode(right_node) = graph_rel.right.as_ref() {
                    if let LogicalPlan::ViewScan(scan) = right_node.input.as_ref() {
                        log::info!(
                            "🎯 POLYMORPHIC: Using right node '{}' as FROM",
                            right_node.alias
                        );
                        return Ok(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::GraphNode(right_node.clone())),
                            name: scan.source_table.clone(),
                            alias: Some(right_node.alias.clone()),
                            use_final: scan.use_final,
                        }));
                    }
                }
            }

            // A.4: Node-only query (MATCH (n:Label) RETURN n)
            if let Some(graph_node) = find_graph_node(&graph_joins.input) {
                if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                    log::info!("🎯 NODE-ONLY: Using node '{}' as FROM", graph_node.alias);
                    let view_ref = ViewTableRef::new_table_with_alias(
                        scan.as_ref().clone(),
                        scan.source_table.clone(),
                        graph_node.alias.clone(),
                    );
                    return Ok(Some(view_ref));
                }
            }

            // A.5: CartesianProduct (WITH...MATCH or comma patterns)
            if let Some(cp) = find_cartesian_product(&graph_joins.input) {
                if is_cte_reference(&cp.left) {
                    log::info!("🎯 WITH...MATCH: FROM comes from right side");
                    return Ok(from_table_to_view_ref(cp.right.as_ref().extract_from()?));
                } else {
                    log::info!("🎯 COMMA PATTERN: FROM comes from left side");
                    return Ok(from_table_to_view_ref(cp.left.as_ref().extract_from()?));
                }
            }

            // No valid FROM found for empty joins - this is unexpected
            log::warn!(
                "⚠️ GraphJoins has empty joins and no recognizable pattern - returning None"
            );
            return Ok(None);
        }

        // CASE B: Has joins but no FROM marker
        // This happens for OPTIONAL MATCH where the anchor comes from a prior MATCH
        // The anchor_table is set but the anchor table info is in the input plan, not in joins
        //
        // ALSO: After WITH scope barriers, anchor_table may be None if the original anchor
        // was not exported by the WITH. In this case, pick the first join as anchor.

        // B.0: CRITICAL - Check for VLP in input FIRST
        // When VLP is followed by additional relationships (chained pattern like
        // (a)-[*]->(b)-[:REL]->(c)), the anchor might be set to 'b', but we need
        // the VLP CTE as FROM, with 'b' being accessed via t.end_* columns.
        // IMPORTANT: Use find_vlp_graph_rel to traverse nested GraphRels and find VLP
        // inside chained patterns. find_graph_rel only finds the outermost GraphRel.
        if let Some(graph_rel) = find_vlp_graph_rel(&graph_joins.input) {
            // find_vlp_graph_rel already checks variable_length.is_some()
            let start_alias = &graph_rel.left_connection;
            let end_alias = &graph_rel.right_connection;
            let cte_name = format!("vlp_{}_{}", start_alias, end_alias);

            log::info!(
                "🎯 VLP + CHAINED: Using CTE '{}' as FROM (anchor was '{:?}' but is part of VLP)",
                cte_name,
                graph_joins.anchor_table
            );

            return Ok(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: cte_name,
                alias: Some(VLP_CTE_FROM_ALIAS.to_string()), // Standard VLP alias
                use_final: false,
            }));
        }

        if let Some(anchor_alias) = &graph_joins.anchor_table {
            log::info!(
                "🔍 No FROM marker in joins, looking for anchor '{}' in input plan",
                anchor_alias
            );

            // Try to find the anchor table in the input plan tree
            // For OPTIONAL MATCH, the anchor is from the first MATCH (which is in input)
            let rel_tables = extract_rel_and_node_tables(&graph_joins.input);
            if let Some(table_name) = rel_tables.get(anchor_alias) {
                log::info!(
                    "✅ Found anchor '{}' table '{}' in input plan",
                    anchor_alias,
                    table_name
                );
                return Ok(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table_name.clone(),
                    alias: Some(anchor_alias.clone()),
                    use_final: false,
                }));
            }

            // Also check CTE references
            if let Some(cte_name) = graph_joins.cte_references.get(anchor_alias) {
                log::info!(
                    "✅ Anchor '{}' has CTE reference: '{}'",
                    anchor_alias,
                    cte_name
                );
                return Ok(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: cte_name.clone(),
                    alias: Some(anchor_alias.clone()),
                    use_final: false,
                }));
            }

            // Try find_table_name_for_alias as last resort
            if let Some(table_name) = find_table_name_for_alias(&graph_joins.input, anchor_alias) {
                log::info!(
                    "✅ Found anchor '{}' via find_table_name_for_alias: '{}'",
                    anchor_alias,
                    table_name
                );
                return Ok(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table_name,
                    alias: Some(anchor_alias.clone()),
                    use_final: false,
                }));
            }
        } else {
            // No anchor_table - likely cleared due to scope barrier
            // PRIORITY: If we have CTE references, use the LATEST CTE as FROM
            // The CTE references represent variables that are in scope after WITH clauses
            // We want the LAST CTE (highest sequence number) as it represents the final scope

            if !graph_joins.cte_references.is_empty() {
                log::warn!(
                    "🔍 anchor_table is None, but have {} CTE references - finding latest CTE as FROM",
                    graph_joins.cte_references.len()
                );

                // Find the CTE with the highest sequence number (format: with_*_cte_N)
                // This is the most recent WITH clause's output
                let mut best_cte: Option<(&String, &String, usize)> = None;
                for (alias, cte_name) in &graph_joins.cte_references {
                    // Extract sequence number from CTE name using centralized utility
                    // Format: "with_tag_cte_1" or "with_inValidPostCount_postCount_tag_cte_1"
                    // Or base name without counter: "with_tag_cte" or "with_inValidPostCount_postCount_tag_cte"
                    let seq_num = if let Some(base_name) = extract_cte_base_name(cte_name) {
                        // Counter is everything after base_name
                        let counter_str = &cte_name[base_name.len()..];
                        if counter_str.starts_with('_') {
                            counter_str[1..].parse::<usize>().unwrap_or(0)
                        } else {
                            0 // Base name without counter
                        }
                    } else {
                        0
                    };

                    // Keep the CTE with highest sequence number (latest in the chain)
                    // Tie-breaker: prefer longer CTE names (more aliases = more complete)
                    match &best_cte {
                        None => best_cte = Some((alias, cte_name, seq_num)),
                        Some((_, current_name, current_seq)) => {
                            if seq_num > *current_seq
                                || (seq_num == *current_seq && cte_name.len() > current_name.len())
                            {
                                best_cte = Some((alias, cte_name, seq_num));
                            }
                        }
                    }
                }

                if let Some((alias, cte_name, _)) = best_cte {
                    log::info!(
                        "✅ Using latest CTE '{}' AS '{}' as FROM (from cte_references)",
                        cte_name,
                        alias
                    );
                    return Ok(Some(ViewTableRef {
                        source: Arc::new(LogicalPlan::Empty),
                        name: cte_name.clone(),
                        alias: Some(alias.clone()),
                        use_final: false,
                    }));
                }
            }

            // SECONDARY FALLBACK: Pick first join as FROM table
            log::warn!("🔍 anchor_table is None and no CTE references, using first join as FROM");
            if let Some(first_join) = graph_joins.joins.first() {
                // Check if this join has a CTE reference
                if let Some(cte_name) = graph_joins.cte_references.get(&first_join.table_alias) {
                    log::info!(
                        "✅ Using first join '{}' → CTE '{}' as FROM",
                        first_join.table_alias,
                        cte_name
                    );
                    return Ok(Some(ViewTableRef {
                        source: Arc::new(LogicalPlan::Empty),
                        name: cte_name.clone(),
                        alias: Some(first_join.table_alias.clone()),
                        use_final: false,
                    }));
                } else {
                    log::info!(
                        "✅ Using first join '{}' (table '{}') as FROM",
                        first_join.table_alias,
                        first_join.table_name
                    );
                    return Ok(Some(ViewTableRef {
                        source: Arc::new(LogicalPlan::Empty),
                        name: first_join.table_name.clone(),
                        alias: Some(first_join.table_alias.clone()),
                        use_final: false,
                    }));
                }
            }
        }

        // If we still can't find FROM, this is a real bug
        log::error!("❌ BUG: GraphJoins has {} joins but NO FROM marker and couldn't resolve anchor! anchor_table={:?}",
            graph_joins.joins.len(), graph_joins.anchor_table);
        for (i, join) in graph_joins.joins.iter().enumerate() {
            log::error!(
                "  join[{}]: table='{}' alias='{}' conditions={}",
                i,
                join.table_name,
                join.table_alias,
                join.joining_on.len()
            );
        }

        // Return None to surface the bug
        Ok(None)
    }
}
